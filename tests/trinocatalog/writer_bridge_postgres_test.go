//go:build linux || darwin

package trinocatalog_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinocatalog"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// The provisioner publishes a catalog as a property map that includes
// connector.name. The store keeps the connector in its own column and hashes
// only the remaining properties, exactly as the coordinator does - so the
// version it writes has to equal the version the coordinator computes for the
// same catalog.
func TestPublishedCatalogVersionMatchesTheCoordinatorsOwn(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()

	properties := map[string]string{
		"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
		"ducklake.data-path":               "s3://bucket/prefix/",
	}
	if _, err := publisher.Apply(ctx, trinocatalog.Mutation{
		OperationID: "catalog.org_acme.first", Operation: trinocatalog.OperationAddOrReplace,
		CatalogName: "org_acme", ConnectorName: "ducklake", Properties: properties,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var version, connector string
	if err := db.QueryRow(
		`SELECT catalog_version, connector_name FROM trino_catalogs WHERE cell_id=$1 AND catalog_name='org_acme'`,
		testCell).Scan(&version, &connector); err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	if connector != "ducklake" {
		t.Fatalf("connector = %q", connector)
	}
	if want := trinopool.CatalogVersion("org_acme", "ducklake", properties); version != want {
		t.Fatalf("catalog_version = %q, want the coordinator's own hash %q", version, want)
	}
}

// Republishing an unchanged catalog must not advance the revision: coordinators
// poll that number, and a revision that moves on every reconcile tick would
// make every coordinator refetch the whole snapshot forever.
func TestRepublishingAnUnchangedCatalogIsAReplay(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()

	mutation := trinocatalog.Mutation{
		Operation: trinocatalog.OperationAddOrReplace, CatalogName: "org_acme",
		ConnectorName: "ducklake",
		Properties:    map[string]string{"ducklake.data-path": "s3://bucket/prefix/"},
	}
	// The bridge derives the operation id from the intent, which is what makes
	// an unchanged republication a replay.
	mutation.OperationID = "catalog.org_acme." + mutation.PayloadHash()[:32]

	first, err := publisher.Apply(ctx, mutation)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	second, err := publisher.Apply(ctx, mutation)
	if err != nil {
		t.Fatalf("republish: %v", err)
	}
	if !second.Replayed || second.Revision != first.Revision {
		t.Fatalf("republication advanced the revision: %+v -> %+v", first, second)
	}

	// A CHANGED intent is a different operation and does advance it.
	changed := mutation
	changed.Properties = map[string]string{"ducklake.data-path": "s3://other/prefix/"}
	changed.OperationID = "catalog.org_acme." + changed.PayloadHash()[:32]
	third, err := publisher.Apply(ctx, changed)
	if err != nil {
		t.Fatalf("publish changed: %v", err)
	}
	if third.Revision <= first.Revision {
		t.Fatalf("a changed catalog did not advance the revision: %+v", third)
	}

	var revision int64
	if err := db.QueryRow(`SELECT revision FROM trino_catalog_writer_state WHERE cell_id=$1`, testCell).Scan(&revision); err != nil {
		t.Fatalf("read revision: %v", err)
	}
	if revision != third.Revision {
		t.Fatalf("writer state revision = %d, want %d", revision, third.Revision)
	}
}

// The reader's completeness check compares catalog_count with the rows it read,
// so a drop has to leave both consistent within one transaction.
func TestDropLeavesTheStoreConsistent(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()

	for _, name := range []string{"org_a", "org_b"} {
		if _, err := publisher.Apply(ctx, addCatalog(name)); err != nil {
			t.Fatalf("publish %s: %v", name, err)
		}
	}
	if _, err := publisher.Apply(ctx, trinocatalog.Mutation{
		OperationID: "catalog.org_a.drop", Operation: trinocatalog.OperationRemove, CatalogName: "org_a",
	}); err != nil {
		t.Fatalf("drop: %v", err)
	}

	var count int
	var stored int
	if err := db.QueryRow(`SELECT count(*) FROM trino_catalogs WHERE cell_id=$1`, testCell).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if err := db.QueryRow(`SELECT catalog_count FROM trino_catalog_writer_state WHERE cell_id=$1`, testCell).Scan(&stored); err != nil {
		t.Fatalf("read catalog_count: %v", err)
	}
	if count != 1 || stored != count {
		t.Fatalf("rows=%d catalog_count=%d, want both 1", count, stored)
	}
}

// A reader that polls the revision must see a consistent (revision, rows,
// count) triple. This is the read the coordinator performs.
func TestReaderSnapshotIsComplete(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	for _, name := range []string{"org_a", "org_b", "org_c"} {
		if _, err := publisher.Apply(ctx, addCatalog(name)); err != nil {
			t.Fatalf("publish %s: %v", name, err)
		}
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatalf("begin snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var revision int64
	var expected int
	if err := tx.QueryRow(`SELECT revision, catalog_count FROM trino_catalog_writer_state WHERE cell_id=$1`, testCell).
		Scan(&revision, &expected); err != nil {
		t.Fatalf("read writer state: %v", err)
	}
	var actual int
	if err := tx.QueryRow(`SELECT count(*) FROM trino_catalogs WHERE cell_id=$1`, testCell).Scan(&actual); err != nil {
		t.Fatalf("count catalogs: %v", err)
	}
	if actual != expected {
		t.Fatalf("snapshot is incomplete: %d rows, catalog_count %d", actual, expected)
	}
	if revision != 3 {
		t.Fatalf("revision = %d, want 3", revision)
	}
}

// Create, drop, then create again with IDENTICAL properties. With a
// content-only operation id the third call hit the journal and returned the
// first create's revision without writing anything: the catalog was dropped and
// never republished, so no coordinator ever saw it again.
func TestRecreateAfterDropIsRepublished(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()

	create := func() trinocatalog.Mutation {
		return trinocatalog.Mutation{
			Operation: trinocatalog.OperationAddOrReplace, CatalogName: "org_acme",
			ConnectorName: "ducklake",
			Properties:    map[string]string{"ducklake.data-path": "s3://bucket/prefix/"},
		}
	}
	// The bridge's identity scheme: the store's current revision plus the
	// intent, so an intent repeated after other commits is a NEW operation.
	apply := func(mutation trinocatalog.Mutation) trinocatalog.Result {
		t.Helper()
		state, err := publisher.State(ctx)
		if err != nil {
			t.Fatalf("state: %v", err)
		}
		mutation.OperationID = fmt.Sprintf("catalog.%s.r%d.%s", mutation.CatalogName, state.Revision, mutation.PayloadHash()[:16])
		result, err := publisher.Apply(ctx, mutation)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		return result
	}

	first := apply(create())
	apply(trinocatalog.Mutation{Operation: trinocatalog.OperationRemove, CatalogName: "org_acme"})
	third := apply(create())

	if third.Replayed {
		t.Fatal("the recreate was treated as a replay of the original create")
	}
	if third.Revision <= first.Revision {
		t.Fatalf("recreate revision %d did not advance past %d", third.Revision, first.Revision)
	}
	var present bool
	if err := db.QueryRow(`SELECT count(*) = 1 FROM trino_catalogs WHERE cell_id=$1 AND catalog_name='org_acme'`, testCell).Scan(&present); err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	if !present {
		t.Fatal("the recreated catalog is not in the store")
	}
}
