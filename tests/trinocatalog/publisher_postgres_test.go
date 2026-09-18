//go:build linux || darwin

// Real-PostgreSQL tests for the fenced catalog publisher. The publisher writes
// the catalog store that Trino coordinators read, so its transaction shape,
// fence and journal semantics cannot be established against a fake: they are
// row locks, unique indexes and serialization behavior.
package trinocatalog_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/controlplane/trinocatalog"
	integrationtest "github.com/posthog/duckgres/tests/integration"
)

var ensurePostgresOnce sync.Once

const testCell = "cell-001"

func newCatalogStore(t *testing.T) (*sql.DB, string) {
	t.Helper()

	dsn := os.Getenv("DUCKGRES_TEST_PG_DSN")
	if dsn == "" {
		ensurePostgres(t)
		dsn = "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable"
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	schema := fmt.Sprintf("catalog_store_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`) })

	db, err := sql.Open("postgres", dsn+" search_path="+schema)
	if err != nil {
		t.Fatalf("open schema connection: %v", err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	return db, schema
}

func ensurePostgres(t *testing.T) {
	t.Helper()
	var err error
	ensurePostgresOnce.Do(func() {
		if integrationtest.IsPostgresRunning(35432) {
			return
		}
		err = integrationtest.StartPostgresContainer()
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
}

func newPublisher(t *testing.T, db *sql.DB, identity string, epoch int64) *trinocatalog.Publisher {
	t.Helper()
	publisher, err := trinocatalog.NewPublisher(db, testCell, identity, epoch)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return publisher
}

// unclaimed returns a store whose schema exists but whose cell has no writer
// yet. Epoch 0 with an empty identity is "nobody has ever written here".
func unclaimed(t *testing.T) (*sql.DB, *trinocatalog.Publisher) {
	t.Helper()
	db, _ := newCatalogStore(t)
	publisher := newPublisher(t, db, "duckgres-test", 1)
	if err := publisher.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return db, publisher
}

// bootstrapped additionally claims the cell. Claiming is always explicit: a
// publisher never becomes the writer as a side effect of a mutation.
func bootstrapped(t *testing.T) (*sql.DB, *trinocatalog.Publisher) {
	t.Helper()
	db, publisher := unclaimed(t)
	if _, err := publisher.Takeover(context.Background()); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	return db, publisher
}

// A publisher that has not claimed the cell must not be able to write, even
// when nobody else holds it.
func TestUnclaimedCellRejectsMutations(t *testing.T) {
	_, publisher := unclaimed(t)
	if _, err := publisher.Apply(context.Background(), addCatalog("org_a")); !errors.Is(err, trinocatalog.ErrNotWriter) {
		t.Fatalf("unclaimed apply error = %v, want ErrNotWriter", err)
	}
}

func addCatalog(name string) trinocatalog.Mutation {
	return trinocatalog.Mutation{
		OperationID:   "op-" + name,
		Operation:     trinocatalog.OperationAddOrReplace,
		CatalogName:   name,
		ConnectorName: "ducklake",
		Properties: map[string]string{
			"ducklake.metadata.connection-url": "jdbc:postgresql://db:5432/lake",
			"ducklake.data-path":               "s3://bucket/prefix/",
		},
	}
}

func readState(t *testing.T, db *sql.DB) (revision int64, epoch int64, identity string, count int) {
	t.Helper()
	row := db.QueryRow(`SELECT revision, writer_epoch, writer_identity, catalog_count FROM trino_catalog_writer_state WHERE cell_id = $1`, testCell)
	if err := row.Scan(&revision, &epoch, &identity, &count); err != nil {
		t.Fatalf("read writer state: %v", err)
	}
	return revision, epoch, identity, count
}

// The schema is created by the publisher because a managed-reader coordinator
// runs no DDL at all.
func TestEnsureSchemaIsIdempotent(t *testing.T) {
	db, publisher := bootstrapped(t)
	if err := publisher.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("second ensure schema: %v", err)
	}
	for _, table := range []string{"trino_catalogs", "trino_catalog_writer_state", "trino_catalog_journal"} {
		var exists bool
		if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil || !exists {
			t.Fatalf("table %s missing (err=%v)", table, err)
		}
	}
}

// Seeding a writer-state row at catalog_count 0 against a store that already
// holds catalogs makes every reader's completeness check fail, which freezes the
// whole fleet on last-good state. The seed must count the existing rows.
func TestSeedsWriterStateFromExistingCatalogRows(t *testing.T) {
	db, publisher := unclaimed(t)
	for _, name := range []string{"org_a", "org_b", "org_c"} {
		if _, err := db.Exec(`INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties) VALUES ($1,$2,'ducklake','v','{}')`, testCell, name); err != nil {
			t.Fatalf("seed catalog row: %v", err)
		}
	}
	// A row for a different cell must not be counted.
	if _, err := db.Exec(`INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties) VALUES ('other-cell','org_z','ducklake','v','{}')`); err != nil {
		t.Fatalf("seed foreign catalog row: %v", err)
	}

	if _, err := publisher.State(context.Background()); err != nil {
		t.Fatalf("state: %v", err)
	}
	revision, _, _, count := readState(t, db)
	if revision != 0 {
		t.Fatalf("seeded revision = %d, want 0", revision)
	}
	if count != 3 {
		t.Fatalf("seeded catalog_count = %d, want 3", count)
	}
}

func TestApplyAdvancesRevisionAndRecomputesCount(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()

	first, err := publisher.Apply(ctx, addCatalog("org_a"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if first.Revision != 1 || first.Replayed {
		t.Fatalf("first apply = %+v, want revision 1", first)
	}
	second, err := publisher.Apply(ctx, addCatalog("org_b"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if second.Revision != 2 {
		t.Fatalf("second apply revision = %d, want 2", second.Revision)
	}

	revision, epoch, identity, count := readState(t, db)
	if revision != 2 || epoch != 1 || identity != "duckgres-test" || count != 2 {
		t.Fatalf("writer state = (%d,%d,%q,%d)", revision, epoch, identity, count)
	}

	var version string
	if err := db.QueryRow(`SELECT catalog_version FROM trino_catalogs WHERE cell_id=$1 AND catalog_name='org_a'`, testCell).Scan(&version); err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	// The version has to be the Trino content hash, not an invented value.
	if want := trinocatalog.Mutation(addCatalog("org_a")).CatalogVersion(); version != want {
		t.Fatalf("catalog_version = %q, want %q", version, want)
	}
}

func TestRemoveDeletesTheRowAndJournalsANullVersion(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	removal := trinocatalog.Mutation{OperationID: "op-remove", Operation: trinocatalog.OperationRemove, CatalogName: "org_a"}
	result, err := publisher.Apply(ctx, removal)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if result.Revision != 2 || result.CatalogCount != 0 {
		t.Fatalf("remove result = %+v", result)
	}
	var version sql.NullString
	if err := db.QueryRow(`SELECT catalog_version FROM trino_catalog_journal WHERE cell_id=$1 AND operation_id='op-remove'`, testCell).Scan(&version); err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if version.Valid {
		t.Fatalf("REMOVE journalled a catalog_version %q", version.String)
	}
}

// A lost COMMIT response must be resolved from the journal, never by applying
// the mutation a second time or compensating with a DROP.
func TestReplayReturnsTheRecordedRevision(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	first, err := publisher.Apply(ctx, addCatalog("org_a"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	replay, err := publisher.Apply(ctx, addCatalog("org_a"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.Revision != first.Revision {
		t.Fatalf("replay = %+v, want the recorded revision %d", replay, first.Revision)
	}
	revision, _, _, _ := readState(t, db)
	if revision != first.Revision {
		t.Fatalf("replay advanced the revision to %d", revision)
	}

	resolved, err := publisher.ResolveOperation(ctx, "op-org_a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved == nil || resolved.Revision != first.Revision {
		t.Fatalf("resolve = %+v, want revision %d", resolved, first.Revision)
	}
	missing, err := publisher.ResolveOperation(ctx, "op-never-happened")
	if err != nil {
		t.Fatalf("resolve missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("resolve of an unknown operation returned %+v", missing)
	}
}

// Same operation id with different content is a bug in the caller, not a
// replay: silently applying it would publish an unintended definition.
func TestChangedIntentUnderTheSameOperationIDIsAConflict(t *testing.T) {
	_, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	changed := addCatalog("org_a")
	changed.Properties["ducklake.data-path"] = "s3://other-bucket/prefix/"
	if _, err := publisher.Apply(ctx, changed); !errors.Is(err, trinocatalog.ErrIntentChanged) {
		t.Fatalf("changed intent error = %v, want ErrIntentChanged", err)
	}
}

func TestFencedWriterCannotWrite(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}

	successor := newPublisher(t, db, "duckgres-successor", 2)
	if _, err := successor.Takeover(ctx); err != nil {
		t.Fatalf("takeover: %v", err)
	}

	// The old publisher is now fenced and must not write anything at all.
	if _, err := publisher.Apply(ctx, addCatalog("org_b")); !errors.Is(err, trinocatalog.ErrFenced) {
		t.Fatalf("stale writer error = %v, want ErrFenced", err)
	}
	revision, epoch, identity, count := readState(t, db)
	if revision != 1 || epoch != 2 || identity != "duckgres-successor" || count != 1 {
		t.Fatalf("stale writer changed state: (%d,%d,%q,%d)", revision, epoch, identity, count)
	}
	if _, err := successor.Apply(ctx, addCatalog("org_b")); err != nil {
		t.Fatalf("successor apply: %v", err)
	}
}

// Root integration decision 2: a mutation verifies the exact epoch AND identity.
// It never claims a higher epoch as a side effect, because that would let a
// process that merely believes it is newer seize the cell mid-write.
func TestMutationNeverClaimsAHigherEpochImplicitly(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	ambitious := newPublisher(t, db, "duckgres-ambitious", 5)
	if _, err := ambitious.Apply(ctx, addCatalog("org_b")); !errors.Is(err, trinocatalog.ErrNotWriter) {
		t.Fatalf("implicit takeover error = %v, want ErrNotWriter", err)
	}
	_, epoch, identity, _ := readState(t, db)
	if epoch != 1 || identity != "duckgres-test" {
		t.Fatalf("an unapproved mutation moved the fence to (%d,%q)", epoch, identity)
	}
}

// Same epoch, different process: exactly one writer identity owns a cell.
func TestSameEpochDifferentIdentityIsRejected(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	impostor := newPublisher(t, db, "duckgres-impostor", 1)
	if _, err := impostor.Apply(ctx, addCatalog("org_b")); !errors.Is(err, trinocatalog.ErrNotWriter) {
		t.Fatalf("same-epoch impostor error = %v, want ErrNotWriter", err)
	}
}

func TestTakeoverIsMonotonic(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := newPublisher(t, db, "duckgres-successor", 2).Takeover(ctx); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	// An older epoch can never take the cell back.
	if _, err := newPublisher(t, db, "duckgres-test", 1).Takeover(ctx); !errors.Is(err, trinocatalog.ErrFenced) {
		t.Fatalf("regressing takeover error = %v, want ErrFenced", err)
	}
	_, epoch, identity, _ := readState(t, db)
	if epoch != 2 || identity != "duckgres-successor" {
		t.Fatalf("writer fence regressed to (%d,%q)", epoch, identity)
	}
}

// The writer-state row lock is what serializes concurrent publishers. Revisions
// must come out contiguous with no gaps and no duplicates, because the reader
// polls that single number to decide whether to fetch a snapshot.
func TestConcurrentPublishersProduceContiguousRevisions(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.State(ctx); err != nil {
		t.Fatalf("state: %v", err)
	}

	const concurrency = 8
	revisions := make([]int64, concurrency)
	errs := make([]error, concurrency)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < concurrency; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			result, err := publisher.Apply(ctx, addCatalog(fmt.Sprintf("org_%d", index)))
			revisions[index], errs[index] = result.Revision, err
		}(index)
	}
	close(start)
	wg.Wait()

	seen := map[int64]bool{}
	for index, err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply %d: %v", index, err)
		}
		if seen[revisions[index]] {
			t.Fatalf("revision %d was handed out twice", revisions[index])
		}
		seen[revisions[index]] = true
	}
	for expected := int64(1); expected <= concurrency; expected++ {
		if !seen[expected] {
			t.Fatalf("revision %d is missing; revisions are not contiguous", expected)
		}
	}
	revision, _, _, count := readState(t, db)
	if revision != concurrency || count != concurrency {
		t.Fatalf("final state revision=%d count=%d, want %d/%d", revision, count, concurrency, concurrency)
	}
}

// The reader's completeness check compares catalog_count against the rows it
// read, so the count has to be recomputed inside the mutation transaction
// rather than incremented optimistically.
func TestCatalogCountIsRecomputedInsideTheTransaction(t *testing.T) {
	db, publisher := bootstrapped(t)
	ctx := context.Background()
	if _, err := publisher.Apply(ctx, addCatalog("org_a")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Something outside the publisher (a legacy writer during the migration
	// bridge) adds a row. The next mutation must reconcile the count, not
	// carry the stale one forward.
	if _, err := db.Exec(`INSERT INTO trino_catalogs (cell_id, catalog_name, connector_name, catalog_version, properties) VALUES ($1,'org_legacy','ducklake','v','{}')`, testCell); err != nil {
		t.Fatalf("legacy insert: %v", err)
	}
	if _, err := publisher.Apply(ctx, addCatalog("org_b")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	_, _, _, count := readState(t, db)
	if count != 3 {
		t.Fatalf("catalog_count = %d, want 3", count)
	}
}
