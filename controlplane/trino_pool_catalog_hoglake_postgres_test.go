//go:build kubernetes

package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// scopedCatalogWriter builds the production writer against a real PostgreSQL,
// as the scoped publisher role infra provisions.
func scopedCatalogWriter(t *testing.T, store trinoPoolRevisionStore, lease configstore.TrinoPoolLease) *trinoPoolCatalogWriter {
	t.Helper()
	adminDSN := adminPostgresURL(t)
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	suffix := randomSuffix(t)
	schema := "trino_cell_" + suffix
	role := "dgpub_" + suffix
	password := "pw_" + randomSuffix(t)

	mustExec(t, admin, fmt.Sprintf(`CREATE SCHEMA %s`, schema))
	mustExec(t, admin, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, password))
	t.Cleanup(func() {
		_, _ = admin.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema))
		_, _ = admin.Exec(fmt.Sprintf(`REASSIGN OWNED BY %s TO CURRENT_USER`, role))
		_, _ = admin.Exec(fmt.Sprintf(`DROP OWNED BY %s`, role))
		_, _ = admin.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	mustExec(t, admin, fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA %s TO %s`, schema, role))
	mustExec(t, admin, fmt.Sprintf(`REVOKE ALL ON SCHEMA public FROM %s`, role))

	dsnFile := filepath.Join(t.TempDir(), "publisher.dsn")
	if err := os.WriteFile(dsnFile, []byte(scopedDSN(t, adminDSN, role, password)), 0o600); err != nil {
		t.Fatalf("write dsn file: %v", err)
	}
	t.Setenv(envTrinoPoolCatalogWriter, "true")
	t.Setenv(envTrinoPoolCatalogBootstrap, "true")
	t.Setenv(envTrinoPoolCatalogDSNFile, dsnFile)
	t.Setenv(envTrinoPoolCatalogSchema, schema)

	writer, err := buildTrinoPoolCatalogWriter("cell-001", store,
		func() (configstore.TrinoPoolLease, bool) { return lease, true }, nil)
	if err != nil || writer == nil {
		t.Fatalf("build the catalog writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.db.Close() })
	return writer
}

// recordingRevisionStore is the pool row's checkpoint, with the failure the
// watermark recovery exists for.
type recordingRevisionStore struct {
	revision int64
	refuse   bool
}

func (s *recordingRevisionStore) RecordTrinoPoolPublicationRevision(_ context.Context, _ configstore.TrinoPoolLease, _ string, revision int64) error {
	if s.refuse {
		return errors.New("checkpoint refused")
	}
	s.revision = revision
	return nil
}

// Managed Hoglake refuses to adopt a catalog whose connector it cannot inspect,
// because silently re-pointing an existing DuckLake catalog at Hoglake metadata
// would be a migration nobody asked for. On a coordinator-mediated cell that
// inspection is a `system.metadata.catalogs` query; a pooled cell has no fixed
// coordinator, so the same question is answered from the store the coordinators
// reconcile from.
func TestPooledCatalogWriterReportsPublishedConnectors(t *testing.T) {
	lease := configstore.TrinoPoolLease{PoolID: "registered:cell-001", Owner: "cp-test", Epoch: 1}
	writer := scopedCatalogWriter(t, &recordingRevisionStore{}, lease)
	ctx := context.Background()
	if err := writer.ClaimWriter(ctx); err != nil {
		t.Fatalf("claim the writer fence: %v", err)
	}

	// Nothing published yet: an empty inventory, not an error. A first Hoglake
	// catalog has no existing connector to disagree with.
	connectors, err := writer.CatalogConnectors(ctx)
	if err != nil {
		t.Fatalf("read connectors on an empty cell: %v", err)
	}
	if len(connectors) != 0 {
		t.Fatalf("connectors = %v, want none before anything is published", connectors)
	}

	if err := writer.CreateCatalog(ctx, "org_legacy", map[string]string{
		"connector.name": "ducklake", "ducklake.data-path": "s3://bucket/legacy/",
	}); err != nil {
		t.Fatalf("publish the DuckLake catalog: %v", err)
	}
	if err := writer.CreateCatalog(ctx, "org_new", map[string]string{
		"connector.name": "hoglake", "hoglake.catalog": "org_new",
	}); err != nil {
		t.Fatalf("publish the Hoglake catalog: %v", err)
	}

	connectors, err = writer.CatalogConnectors(ctx)
	if err != nil {
		t.Fatalf("read connectors: %v", err)
	}
	// The mismatched one is reported as what it IS, which is what makes the
	// adoption check refuse it, and the Hoglake one as hoglake, which is what
	// lets an already-published tenant reconcile without being recreated.
	if connectors["org_legacy"] != "ducklake" {
		t.Fatalf("org_legacy connector = %q, want the existing DuckLake connector to be visible",
			connectors["org_legacy"])
	}
	if connectors["org_new"] != "hoglake" {
		t.Fatalf("org_new connector = %q, want hoglake", connectors["org_new"])
	}
}

// The watermark the admission gate certifies against comes from the catalog
// store, so a checkpoint that failed after a committed catalog is recoverable.
//
// Without this the number is only ever written as a side effect of a
// publication, and the catalog that failed to record its revision is never
// published again - it already exists. The gate would keep certifying members
// against a revision that predates the tenant.
func TestPooledCatalogWriterRecoversTheWatermarkFromTheStore(t *testing.T) {
	lease := configstore.TrinoPoolLease{PoolID: "registered:cell-001", Owner: "cp-test", Epoch: 1}
	store := &recordingRevisionStore{}
	writer := scopedCatalogWriter(t, store, lease)
	ctx := context.Background()
	if err := writer.ClaimWriter(ctx); err != nil {
		t.Fatalf("claim the writer fence: %v", err)
	}

	// The catalog commits; the checkpoint of its revision does not.
	store.refuse = true
	err := writer.CreateCatalog(ctx, "org_acme", map[string]string{
		"connector.name": "hoglake", "hoglake.catalog": "org_acme",
	})
	if err == nil {
		t.Fatal("a failed revision checkpoint was reported as success")
	}
	if store.revision != 0 {
		t.Fatalf("checkpointed revision = %d, want none recorded", store.revision)
	}

	// The catalog IS published - the publication itself committed.
	names, err := writer.ListCatalogs(ctx)
	if err != nil || len(names) != 1 || names[0] != "org_acme" {
		t.Fatalf("published catalogs = %v (err %v), want the committed catalog", names, err)
	}

	// Nothing will republish it, so the number has to be recoverable from the
	// store's own writer state. That is what the admission gate reads.
	published, err := writer.PublishedRevision(ctx)
	if err != nil {
		t.Fatalf("read the published revision: %v", err)
	}
	if published < 1 {
		t.Fatalf("published revision = %d, want the committed catalog's revision", published)
	}

	// And a later claim checkpoints it without any new catalog mutation.
	store.refuse = false
	if err := writer.ClaimWriter(ctx); err != nil {
		t.Fatalf("re-claim the writer fence: %v", err)
	}
	if store.revision != published {
		t.Fatalf("checkpointed revision = %d, want %d recovered at the claim", store.revision, published)
	}
}
