//go:build k8s_integration

package k8s_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestK8sDuckLakeRoundTrip exercises the full DuckLake write/read path
// against MinIO: CREATE TABLE writes parquet to the org's S3 prefix,
// metadata to the DuckLake-metadata Postgres, then SELECT reads it back.
// Existing k8s tests stop at "the catalog is attached"; this is the first
// test that proves the catalog actually serves writes and reads through
// the real object store.
func TestK8sDuckLakeRoundTrip(t *testing.T) {
	tableName := fmt.Sprintf("ducklake.dl_roundtrip_%d", time.Now().UnixNano())
	const rows = 500

	prefixBefore, err := minioPrefixFileCount("orgs/local")
	if err != nil {
		t.Fatalf("count orgs/local prefix before write: %v", err)
	}

	if err := retryDBOperationWithReconnect(45*time.Second, "create ducklake table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE OR REPLACE TABLE %s AS SELECT i AS id, repeat('x', 256) AS payload FROM generate_series(1, %d) t(i)",
			tableName, rows,
		))
		return err
	}); err != nil {
		t.Fatalf("create ducklake table: %v", err)
	}
	t.Cleanup(func() {
		_ = retryDBOperationWithReconnect(15*time.Second, "drop ducklake table", func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
			return err
		})
	})

	if err := waitForMinioPrefixFileCountAtLeast("orgs/local", prefixBefore+1, 60*time.Second); err != nil {
		t.Fatalf("expected DuckLake to write at least one new file under orgs/local: %v", err)
	}

	var count int
	if err := retryScanIntWithReconnect("SELECT COUNT(*) FROM "+tableName, 30*time.Second, &count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != rows {
		t.Fatalf("ducklake table row count = %d, want %d", count, rows)
	}
}

// TestK8sDuckLakeDurabilityAcrossWorkerRestart proves that data committed
// to DuckLake survives a worker pod restart — i.e. the parquet in MinIO
// and the snapshot in the metadata Postgres are the source of truth, not
// any worker-local state. This is the closest thing to a "did checkpoint
// actually durably commit" assertion we can make without inspecting the
// metadata DB directly.
//
// The sequence:
//  1. Write a table through the active worker.
//  2. Kill that worker pod.
//  3. Wait for a replacement to come up.
//  4. Reconnect and SELECT — data must still be there.
//
// Without this test, a regression that buffers DuckLake commits in-memory
// (or skips the metadata commit) would silently slip through: the data
// would survive single-session reads but vanish after a restart.
func TestK8sDuckLakeDurabilityAcrossWorkerRestart(t *testing.T) {
	tableName := fmt.Sprintf("ducklake.dl_durable_%d", time.Now().UnixNano())
	const rows = 200

	if err := retryDBOperationWithReconnect(45*time.Second, "create ducklake table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			"CREATE OR REPLACE TABLE %s AS SELECT i AS id FROM generate_series(1, %d) t(i)",
			tableName, rows,
		))
		return err
	}); err != nil {
		t.Fatalf("create ducklake table: %v", err)
	}

	// Pick the latest worker (the one that just served the CREATE), then
	// delete it. The post-restart query will land on a replacement.
	worker := latestWorkerPod(t)
	t.Logf("Killing worker pod %s to force a fresh activation", worker.Name)
	if err := clientset.CoreV1().Pods(namespace).Delete(context.Background(), worker.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete worker pod %s: %v", worker.Name, err)
	}
	if _, err := waitForWorkerReplacement(worker.Name, 90*time.Second); err != nil {
		t.Fatalf("worker pod was not replaced: %v", err)
	}

	t.Cleanup(func() {
		_ = retryDBOperationWithReconnect(15*time.Second, "drop ducklake table", func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
			return err
		})
	})

	var count int
	// Generous timeout: the fresh worker has to spawn, activate, attach
	// DuckLake, and respond. This is the exact path that breaks if the
	// metadata commit was a no-op or the worker's view of the catalog is
	// stale, so we want a clean failure rather than a flake.
	if err := retryScanIntWithReconnect("SELECT COUNT(*) FROM "+tableName, 120*time.Second, &count); err != nil {
		t.Fatalf("post-restart count rows: %v", err)
	}
	if count != rows {
		t.Fatalf("post-restart row count = %d, want %d (data did not survive worker restart)", count, rows)
	}
}

// TestK8sDuckLakeConcurrentWriters exercises the PostHog DuckLake fork's
// conflict-retry path in the real k8s setup: N goroutines, each on its
// own connection, INSERT into the same table. With the fork's retry
// semantics every commit should eventually land; without retries (or with
// a regression that suppresses retries on certain SQLSTATEs) the test
// would either fail with a conflict error or end up with fewer rows than
// expected.
//
// We deliberately don't assert no-conflicts-occurred: that's the wrong
// invariant. The invariant is no-rows-lost — conflicts are fine as long
// as the retry layer makes every writer eventually succeed.
func TestK8sDuckLakeConcurrentWriters(t *testing.T) {
	tableName := fmt.Sprintf("ducklake.dl_concurrent_%d", time.Now().UnixNano())
	const writers = 4
	const rowsPerWriter = 25

	if err := retryDBOperationWithReconnect(30*time.Second, "create concurrent table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s (writer INTEGER, id INTEGER)", tableName))
		return err
	}); err != nil {
		t.Fatalf("create concurrent table: %v", err)
	}
	t.Cleanup(func() {
		_ = retryDBOperationWithReconnect(15*time.Second, "drop concurrent table", func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
			return err
		})
	})

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			// Single multi-row INSERT keeps the test focused on the
			// commit-conflict path rather than transaction overhead.
			values := ""
			for r := 0; r < rowsPerWriter; r++ {
				if r > 0 {
					values += ","
				}
				values += fmt.Sprintf("(%d, %d)", writerID, writerID*rowsPerWriter+r)
			}
			err := retryDBOperationWithReconnect(120*time.Second, fmt.Sprintf("writer %d INSERT", writerID), func(ctx context.Context, db *sql.DB) error {
				_, err := db.ExecContext(ctx, "INSERT INTO "+tableName+" VALUES "+values)
				return err
			})
			if err != nil {
				errs <- fmt.Errorf("writer %d: %w", writerID, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	var count int
	if err := retryScanIntWithReconnect("SELECT COUNT(*) FROM "+tableName, 30*time.Second, &count); err != nil {
		t.Fatalf("count concurrent rows: %v", err)
	}
	want := writers * rowsPerWriter
	if count != want {
		t.Fatalf("concurrent row count = %d, want %d — DuckLake fork's conflict retry did not preserve all writes", count, want)
	}
}
