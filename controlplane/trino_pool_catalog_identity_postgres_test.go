//go:build kubernetes

package controlplane

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// assertingRevisionStore is the pool row's checkpoint with the real store's
// precondition: RecordTrinoPoolPublicationRevision refuses a pool id that is not
// the one the held lease names. The catalog store's partition must therefore
// never be handed to it, however the deployment spells that partition.
type assertingRevisionStore struct {
	t        *testing.T
	poolID   string
	revision int64
}

func (s *assertingRevisionStore) RecordTrinoPoolPublicationRevision(_ context.Context, lease configstore.TrinoPoolLease, poolID string, revision int64) error {
	s.t.Helper()
	if poolID != lease.PoolID {
		s.t.Fatalf("checkpoint addressed pool %q, the held lease is for %q", poolID, lease.PoolID)
	}
	s.poolID, s.revision = poolID, revision
	return nil
}

// The catalog store's partition is a TRINO-side identity: it is the value each
// coordinator of the cell reads with `catalog-store.cell-id`, and the rows the
// publisher writes must carry exactly it or every coordinator reconciles an
// empty catalog set.
//
// It is deliberately not derived from the pool's own id. That id carries
// duckgres's reserved `registered:` prefix and doubles as the warehouse
// ASSIGNMENT key, so deriving one from the other would force a cluster to spell
// its store partition `registered:<cell>` and would silently re-key every
// coordinator's read the day a pool's logical id changes. The two identities are
// configured separately and this asserts they stay separate: the rows land under
// the configured partition while the pool row's checkpoint still addresses the
// pool.
func TestCatalogWriterPublishesUnderTheConfiguredStorePartition(t *testing.T) {
	lease := configstore.TrinoPoolLease{PoolID: "registered:example-pool", Owner: "cp-test", Epoch: 1}
	store := &assertingRevisionStore{t: t}
	// The helper states the partition, which is a different value from the pool
	// id this writer is built for.
	writer := scopedCatalogWriter(t, lease.PoolID, store, lease)

	ctx := context.Background()
	if err := writer.ClaimWriter(ctx); err != nil {
		t.Fatalf("claim the writer fence: %v", err)
	}
	if err := writer.CreateCatalog(ctx, "org_acme", map[string]string{
		"connector.name":     "ducklake",
		"ducklake.data-path": "s3://bucket/prefix/",
	}); err != nil {
		t.Fatalf("publish a catalog: %v", err)
	}

	// The definition and the writer state both belong to the partition the
	// coordinators read, not to the pool's id.
	var cellID string
	if err := writer.db.QueryRow(`SELECT cell_id FROM trino_catalogs WHERE catalog_name = 'org_acme'`).Scan(&cellID); err != nil {
		t.Fatalf("read the published catalog: %v", err)
	}
	if cellID != testCatalogPartition {
		t.Fatalf("catalog published under cell_id %q, the coordinators read %q", cellID, testCatalogPartition)
	}
	if err := writer.db.QueryRow(`SELECT cell_id FROM trino_catalog_writer_state`).Scan(&cellID); err != nil {
		t.Fatalf("read the writer state: %v", err)
	}
	if cellID != testCatalogPartition {
		t.Fatalf("writer state recorded under cell_id %q, want %q", cellID, testCatalogPartition)
	}

	// And the checkpoint reached the pool row, which is a different identity.
	if store.poolID != lease.PoolID || store.revision < 1 {
		t.Fatalf("checkpoint recorded pool %q at revision %d", store.poolID, store.revision)
	}
}

// The partition is REQUIRED and fails closed, and the three ways it can be
// absent are one answer: each means nobody can say which partition the
// publisher would write to. There is deliberately no fallback to the pool id -
// that would let a missing setting publish a full catalog set under a partition
// no coordinator reads, which looks exactly like a store nothing has been
// published to yet.
func TestCatalogWriterRequiresAStorePartition(t *testing.T) {
	dsnFile := filepath.Join(t.TempDir(), "publisher.dsn")
	if err := os.WriteFile(dsnFile, []byte("postgres://user:pw@example.invalid:5432/duckgres"), 0o600); err != nil {
		t.Fatalf("write dsn file: %v", err)
	}

	for name, partition := range map[string]string{
		"unset":    "",
		"blank":    "   ",
		"unusable": "not a cell id",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(envTrinoPoolCatalogWriter, "true")
			t.Setenv(envTrinoPoolCatalogBootstrap, "false")
			t.Setenv(envTrinoPoolCatalogDSNFile, dsnFile)
			t.Setenv(envTrinoPoolCatalogSchema, "trino_pool")
			t.Setenv(envTrinoPoolCatalogCellID, partition)

			writer, err := buildTrinoPoolCatalogWriter("registered:example-pool", nil,
				func() (configstore.TrinoPoolLease, bool) { return configstore.TrinoPoolLease{}, false }, nil)
			if err == nil {
				t.Fatal("the writer was built with no usable store partition")
			}
			if writer != nil {
				t.Fatal("a writer was returned alongside the refusal")
			}
			// The refusal names the setting, because the operator's next move is
			// to render it from whatever the coordinators already carry.
			if !strings.Contains(err.Error(), envTrinoPoolCatalogCellID) {
				t.Fatalf("the refusal does not name %s: %v", envTrinoPoolCatalogCellID, err)
			}
		})
	}
}
