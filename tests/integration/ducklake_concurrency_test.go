package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concurrencyMetric captures the result of a single concurrency sub-test.
// When DUCKGRES_BENCH_OUT is set, all metrics are written as JSON to that
// file so the matrix runner can compare across DuckLake versions.
type concurrencyMetric struct {
	Test         string  `json:"test"`
	Successes    int64   `json:"successes"`
	Conflicts    int64   `json:"conflicts"`
	Errors       int64   `json:"errors,omitempty"`
	ConflictRate float64 `json:"conflict_rate_pct"`
	Duration     float64 `json:"duration_sec"`
	Throughput   float64 `json:"throughput_ops_sec,omitempty"`
}

// concurrencyReport is the top-level JSON output for the matrix runner.
type concurrencyReport struct {
	DuckDBVersion   string              `json:"duckdb_version"`
	DuckLakeVersion string              `json:"ducklake_version"`
	Timestamp       string              `json:"timestamp"`
	Metrics         []concurrencyMetric `json:"metrics"`
}

// metricsCollector gathers metrics across sub-tests for the final report.
var metricsCollector struct {
	mu      sync.Mutex
	metrics []concurrencyMetric
}

func recordMetric(m concurrencyMetric) {
	total := float64(m.Successes + m.Conflicts)
	if total > 0 {
		m.ConflictRate = float64(m.Conflicts) / total * 100
	}
	metricsCollector.mu.Lock()
	metricsCollector.metrics = append(metricsCollector.metrics, m)
	metricsCollector.mu.Unlock()
}

// benchSub is a helper to run a sub-test with timing and automatic metric recording.
// The body function receives a *metric pointer to fill in successes/conflicts/errors.
// Duration, throughput, and conflict rate are computed automatically.
type metric = concurrencyMetric

func benchSub(t *testing.T, name string, body func(t *testing.T, m *metric)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		start := time.Now()
		var m metric
		m.Test = name
		body(t, &m)
		m.Duration = time.Since(start).Seconds()
		if m.Duration > 0 && m.Successes > 0 {
			m.Throughput = float64(m.Successes) / m.Duration
		}
		recordMetric(m)
	})
}

// TestDuckLakeConcurrentTransactions is an extensive concurrency test suite for
// DuckLake transactions. It exercises concurrent inserts, updates, deletes,
// mixed DDL/DML, and transaction conflicts across multiple connections to
// detect regressions across DuckLake versions.
//
// DuckLake uses global snapshot IDs in its PostgreSQL metadata store, so even
// writes to unrelated tables can conflict under concurrency. These tests
// stress that behavior.
//
// Set DUCKGRES_BENCH_OUT=path.json to write structured metrics for comparison.
func TestDuckLakeConcurrentTransactions(t *testing.T) {
	if !testHarness.useDuckLake {
		t.Skip("DuckLake mode not enabled (set DUCKGRES_TEST_NO_DUCKLAKE= to enable)")
	}

	// Log version info for traceability.
	// DuckDB version functions are transpiled to PG-compat strings, so we query
	// the extension table for DuckLake and use the library_version pragma for DuckDB.
	conn := openDuckgresConn(t)
	var duckdbVer, ducklakeVer string
	// SHOW duckdb.library_version is not transpiled and returns the actual engine version
	if err := conn.QueryRow("SELECT library_version FROM pragma_version()").Scan(&duckdbVer); err != nil {
		duckdbVer = "unknown"
	}
	rows, err := conn.Query("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake' AND loaded")
	if err == nil {
		if rows.Next() {
			_ = rows.Scan(&ducklakeVer)
		}
		rows.Close()
	}
	_ = conn.Close()
	t.Logf("DuckDB %s, DuckLake extension %s", duckdbVer, ducklakeVer)

	// Write report on cleanup
	t.Cleanup(func() {
		outPath := os.Getenv("DUCKGRES_BENCH_OUT")
		if outPath == "" {
			return
		}
		metricsCollector.mu.Lock()
		report := concurrencyReport{
			DuckDBVersion:   duckdbVer,
			DuckLakeVersion: ducklakeVer,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
			Metrics:         metricsCollector.metrics,
		}
		metricsCollector.mu.Unlock()

		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Logf("failed to marshal metrics: %v", err)
			return
		}
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			t.Logf("failed to write metrics to %s: %v", outPath, err)
			return
		}
		t.Logf("metrics written to %s", outPath)
	})

	benchSub(t, "concurrent_inserts_same_table", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_insert (id INTEGER, worker INTEGER, val TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_insert") }()

		const numWorkers = 8
		const rowsPerWorker = 50
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var successes atomic.Int64
		errs := make(chan error, numWorkers*rowsPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range rowsPerWorker {
					_, err := wconn.Exec(
						"INSERT INTO dl_conc_insert (id, worker, val) VALUES ($1, $2, $3)",
						workerID*rowsPerWorker+i, workerID, fmt.Sprintf("w%d-r%d", workerID, i),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d row %d: %w", workerID, i, err)
						return
					}
					successes.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = successes.Load(), conflicts.Load()
		t.Logf("concurrent inserts: %d succeeded, %d conflicts", m.Successes, m.Conflicts)

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_insert").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if int64(count) != successes.Load() {
			t.Errorf("row count mismatch: table has %d rows but %d inserts succeeded", count, successes.Load())
		}
	})

	benchSub(t, "concurrent_inserts_with_transactions", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_tx_insert (id INTEGER, batch INTEGER, val TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_tx_insert") }()

		const numWorkers = 6
		const batchesPerWorker = 10
		const rowsPerBatch = 5
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var committedBatches atomic.Int64
		errs := make(chan error, numWorkers*batchesPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for b := range batchesPerWorker {
					tx, err := wconn.Begin()
					if err != nil {
						errs <- fmt.Errorf("worker %d batch %d begin: %w", workerID, b, err)
						return
					}

					batchOK := true
					for r := range rowsPerBatch {
						id := workerID*batchesPerWorker*rowsPerBatch + b*rowsPerBatch + r
						_, err := tx.Exec(
							"INSERT INTO dl_conc_tx_insert (id, batch, val) VALUES ($1, $2, $3)",
							id, b, fmt.Sprintf("w%d-b%d-r%d", workerID, b, r),
						)
						if err != nil {
							if isTransactionConflict(err) {
								conflicts.Add(1)
								_ = tx.Rollback()
								batchOK = false
								break
							}
							_ = tx.Rollback()
							errs <- fmt.Errorf("worker %d batch %d row %d: %w", workerID, b, r, err)
							return
						}
					}
					if !batchOK {
						continue
					}

					if err := tx.Commit(); err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							continue
						}
						errs <- fmt.Errorf("worker %d batch %d commit: %w", workerID, b, err)
						return
					}
					committedBatches.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = committedBatches.Load(), conflicts.Load()
		t.Logf("batched inserts: %d batches committed, %d conflicts", m.Successes, m.Conflicts)

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_tx_insert").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		expected := int(committedBatches.Load()) * rowsPerBatch
		if count != expected {
			t.Errorf("row count mismatch: table has %d rows, expected %d (%d batches * %d rows)",
				count, expected, committedBatches.Load(), rowsPerBatch)
		}
	})

	benchSub(t, "concurrent_updates_same_rows", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_update (id INTEGER, counter INTEGER, last_writer INTEGER)")
		// Seed rows
		for i := range 20 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_conc_update VALUES (%d, 0, -1)", i))
		}
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_update") }()

		const numWorkers = 6
		const updatesPerWorker = 30
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var applied atomic.Int64
		errs := make(chan error, numWorkers*updatesPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range updatesPerWorker {
					rowID := (workerID + i) % 20
					_, err := wconn.Exec(
						"UPDATE dl_conc_update SET counter = counter + 1, last_writer = $1 WHERE id = $2",
						workerID, rowID,
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d update %d: %w", workerID, i, err)
						return
					}
					applied.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = applied.Load(), conflicts.Load()
		t.Logf("concurrent updates: %d applied, %d conflicts", m.Successes, m.Conflicts)

		// Verify sum of counters equals applied updates
		var total int
		if err := conn.QueryRow("SELECT SUM(counter) FROM dl_conc_update").Scan(&total); err != nil {
			t.Fatalf("sum query failed: %v", err)
		}
		if int64(total) != applied.Load() {
			t.Errorf("counter sum mismatch: SUM(counter)=%d but %d updates applied", total, applied.Load())
		}
	})

	benchSub(t, "concurrent_deletes_and_inserts", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_del (id INTEGER, val TEXT)")
		// Pre-populate
		for i := range 100 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_conc_del VALUES (%d, 'seed')", i))
		}
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_del") }()

		const numDeleters = 3
		const numInserters = 3
		const opsPerWorker = 40
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var deletes atomic.Int64
		var inserts atomic.Int64
		errs := make(chan error, (numDeleters+numInserters)*opsPerWorker)

		// Deleters: delete rows from the existing set
		for w := range numDeleters {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range opsPerWorker {
					targetID := workerID*opsPerWorker + i
					res, err := wconn.Exec("DELETE FROM dl_conc_del WHERE id = $1", targetID)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("deleter %d op %d: %w", workerID, i, err)
						return
					}
					n, _ := res.RowsAffected()
					deletes.Add(n)
				}
			}(w)
		}

		// Inserters: insert new rows with IDs above the seed range
		for w := range numInserters {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range opsPerWorker {
					newID := 1000 + workerID*opsPerWorker + i
					_, err := wconn.Exec(
						"INSERT INTO dl_conc_del VALUES ($1, $2)",
						newID, fmt.Sprintf("inserted-%d-%d", workerID, i),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("inserter %d op %d: %w", workerID, i, err)
						return
					}
					inserts.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = deletes.Load()+inserts.Load(), conflicts.Load()
		t.Logf("deletes+inserts: %d deleted, %d inserted, %d conflicts",
			deletes.Load(), inserts.Load(), m.Conflicts)

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_del").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		expected := 100 - int(deletes.Load()) + int(inserts.Load())
		if count != expected {
			t.Errorf("row count mismatch: got %d, expected %d (100 - %d deleted + %d inserted)",
				count, expected, deletes.Load(), inserts.Load())
		}
	})

	benchSub(t, "concurrent_multi_table_transactions", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_orders (id INTEGER, customer TEXT, total DOUBLE)")
		mustExec(t, conn, "CREATE TABLE dl_conc_items (order_id INTEGER, product TEXT, qty INTEGER)")
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_items")
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_orders")
		}()

		const numWorkers = 6
		const ordersPerWorker = 15
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var committed atomic.Int64
		errs := make(chan error, numWorkers*ordersPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for o := range ordersPerWorker {
					orderID := workerID*ordersPerWorker + o
					tx, err := wconn.Begin()
					if err != nil {
						errs <- fmt.Errorf("worker %d order %d begin: %w", workerID, o, err)
						return
					}

					// Insert order
					_, err = tx.Exec(
						"INSERT INTO dl_conc_orders VALUES ($1, $2, $3)",
						orderID, fmt.Sprintf("customer-%d", workerID), float64(o)*10.5,
					)
					if err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							_ = tx.Rollback()
							continue
						}
						_ = tx.Rollback()
						errs <- fmt.Errorf("worker %d order %d insert order: %w", workerID, o, err)
						return
					}

					// Insert 2-3 items per order
					itemCount := 2 + (o % 2)
					txFailed := false
					for i := range itemCount {
						_, err = tx.Exec(
							"INSERT INTO dl_conc_items VALUES ($1, $2, $3)",
							orderID, fmt.Sprintf("product-%d", i), i+1,
						)
						if err != nil {
							if isTransactionConflict(err) {
								conflicts.Add(1)
								_ = tx.Rollback()
								txFailed = true
								break
							}
							_ = tx.Rollback()
							errs <- fmt.Errorf("worker %d order %d item %d: %w", workerID, o, i, err)
							return
						}
					}
					if txFailed {
						continue
					}

					if err := tx.Commit(); err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							continue
						}
						errs <- fmt.Errorf("worker %d order %d commit: %w", workerID, o, err)
						return
					}
					committed.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = committed.Load(), conflicts.Load()
		t.Logf("multi-table tx: %d committed, %d conflicts", m.Successes, m.Conflicts)

		// Verify referential integrity: every item should have a matching order
		var orphans int
		if err := conn.QueryRow(`
			SELECT COUNT(*) FROM dl_conc_items i
			WHERE NOT EXISTS (SELECT 1 FROM dl_conc_orders o WHERE o.id = i.order_id)
		`).Scan(&orphans); err != nil {
			t.Fatalf("orphan check failed: %v", err)
		}
		if orphans != 0 {
			t.Errorf("found %d orphaned items without matching orders (transaction atomicity violation)", orphans)
		}
	})

	benchSub(t, "concurrent_read_write_isolation", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_rw (id INTEGER, val INTEGER)")
		for i := range 50 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_conc_rw VALUES (%d, %d)", i, i*10))
		}
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_rw") }()

		const numReaders = 5
		const numWriters = 3
		const opsPerWorker = 40
		var wg sync.WaitGroup
		var readErrors atomic.Int64
		var writeConflicts atomic.Int64
		errs := make(chan error, (numReaders+numWriters)*opsPerWorker)

		// Writers: continuously update rows
		for w := range numWriters {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range opsPerWorker {
					rowID := (writerID*7 + i) % 50
					_, err := wconn.Exec("UPDATE dl_conc_rw SET val = val + 1 WHERE id = $1", rowID)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							writeConflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("writer %d op %d: %w", writerID, i, err)
						return
					}
				}
			}(w)
		}

		// Readers: continuously read and verify consistency
		for r := range numReaders {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()
				rconn := openDuckgresConn(t)
				defer func() { _ = rconn.Close() }()

				for i := range opsPerWorker {
					// Each read should see a consistent snapshot
					var count int
					if err := rconn.QueryRow("SELECT COUNT(*) FROM dl_conc_rw").Scan(&count); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							readErrors.Add(1)
							recoverConnection(rconn)
							continue
						}
						errs <- fmt.Errorf("reader %d op %d: %w", readerID, i, err)
						return
					}
					if count != 50 {
						errs <- fmt.Errorf("reader %d op %d: expected 50 rows, got %d", readerID, i, count)
						return
					}

					// Aggregates should be consistent within a snapshot
					var sum sql.NullInt64
					if err := rconn.QueryRow("SELECT SUM(val) FROM dl_conc_rw").Scan(&sum); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							readErrors.Add(1)
							recoverConnection(rconn)
							continue
						}
						errs <- fmt.Errorf("reader %d op %d sum: %w", readerID, i, err)
						return
					}
					if !sum.Valid {
						errs <- fmt.Errorf("reader %d op %d: SUM(val) is NULL", readerID, i)
						return
					}
				}
			}(r)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = int64(numReaders*opsPerWorker)-readErrors.Load(), writeConflicts.Load()
		t.Logf("read/write isolation: %d write conflicts, %d read errors", writeConflicts.Load(), readErrors.Load())
	})

	benchSub(t, "concurrent_upsert_storm", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// DuckLake rewrites ON CONFLICT to MERGE — test it under concurrency
		mustExec(t, conn, "CREATE TABLE dl_conc_upsert (id INTEGER, counter INTEGER, last_ts BIGINT)")
		// Seed some rows so upserts hit the update path
		for i := range 20 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_conc_upsert VALUES (%d, 0, 0)", i))
		}
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_upsert") }()

		const numWorkers = 8
		const opsPerWorker = 30
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var successes atomic.Int64
		errs := make(chan error, numWorkers*opsPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range opsPerWorker {
					// Target overlapping rows across workers
					targetID := (workerID + i*3) % 30 // some IDs don't exist yet -> insert path
					ts := time.Now().UnixMicro()
					// Use literal values: ON CONFLICT → MERGE rewriting breaks $N placeholders
					_, err := wconn.Exec(fmt.Sprintf(
						`INSERT INTO dl_conc_upsert (id, counter, last_ts) VALUES (%d, 1, %d)
						 ON CONFLICT (id) DO UPDATE SET counter = dl_conc_upsert.counter + 1, last_ts = %d`,
						targetID, ts, ts,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						// ON CONFLICT may not be supported in DuckLake — log and skip
						if strings.Contains(err.Error(), "Not implemented") ||
							strings.Contains(err.Error(), "not supported") {
							t.Logf("upsert not supported: %v", err)
							return
						}
						errs <- fmt.Errorf("worker %d op %d: %w", workerID, i, err)
						return
					}
					successes.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = successes.Load(), conflicts.Load()
		t.Logf("upsert storm: %d succeeded, %d conflicts", m.Successes, m.Conflicts)
	})

	benchSub(t, "concurrent_ddl_while_writing", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_ddl_write (id INTEGER, val TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_ddl_write") }()

		var wg sync.WaitGroup
		var conflicts atomic.Int64
		errs := make(chan error, 200)

		// Writer: insert rows while DDL is happening
		wg.Add(1)
		go func() {
			defer wg.Done()
			wconn := openDuckgresConn(t)
			defer func() { _ = wconn.Close() }()

			for i := range 60 {
				_, err := wconn.Exec(
					"INSERT INTO dl_conc_ddl_write (id, val) VALUES ($1, $2)",
					i, fmt.Sprintf("row-%d", i),
				)
				if err != nil {
					if isTransactionConflict(err) || isAbortedTransaction(err) || isTableNotFound(err) {
						conflicts.Add(1)
						recoverConnection(wconn)
						continue
					}
					errs <- fmt.Errorf("writer insert %d: %w", i, err)
					return
				}
				// Small jitter to interleave with DDL
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}()

		// DDL worker: ALTER TABLE while writes are happening
		wg.Add(1)
		go func() {
			defer wg.Done()
			dconn := openDuckgresConn(t)
			defer func() { _ = dconn.Close() }()

			time.Sleep(10 * time.Millisecond) // Let some writes land first

			for i := range 5 {
				colName := fmt.Sprintf("extra_%d", i)
				_, err := dconn.Exec(fmt.Sprintf("ALTER TABLE dl_conc_ddl_write ADD COLUMN %s TEXT", colName))
				if err != nil {
					if isTransactionConflict(err) || isAbortedTransaction(err) {
						conflicts.Add(1)
						recoverConnection(dconn)
						continue
					}
					errs <- fmt.Errorf("ddl alter %d: %w", i, err)
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Conflicts = conflicts.Load()
		t.Logf("ddl+write: %d conflicts", m.Conflicts)
	})

	benchSub(t, "concurrent_large_batch_inserts", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_batch (id INTEGER, worker INTEGER, payload TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_batch") }()

		const numWorkers = 4
		const batchesPerWorker = 5
		const rowsPerBatch = 100
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var insertedRows atomic.Int64
		errs := make(chan error, numWorkers*batchesPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for b := range batchesPerWorker {
					tx, err := wconn.Begin()
					if err != nil {
						errs <- fmt.Errorf("worker %d batch %d begin: %w", workerID, b, err)
						return
					}

					// Build a multi-row INSERT
					var values []string
					for r := range rowsPerBatch {
						id := workerID*batchesPerWorker*rowsPerBatch + b*rowsPerBatch + r
						values = append(values, fmt.Sprintf("(%d, %d, 'payload-%d-%d-%d')", id, workerID, workerID, b, r))
					}
					stmt := fmt.Sprintf("INSERT INTO dl_conc_batch (id, worker, payload) VALUES %s", strings.Join(values, ","))

					_, err = tx.Exec(stmt)
					if err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							_ = tx.Rollback()
							continue
						}
						_ = tx.Rollback()
						errs <- fmt.Errorf("worker %d batch %d exec: %w", workerID, b, err)
						return
					}

					if err := tx.Commit(); err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							continue
						}
						errs <- fmt.Errorf("worker %d batch %d commit: %w", workerID, b, err)
						return
					}
					insertedRows.Add(int64(rowsPerBatch))
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = insertedRows.Load(), conflicts.Load()
		t.Logf("large batch inserts: %d rows inserted, %d conflicts", m.Successes, m.Conflicts)

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_batch").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if int64(count) != insertedRows.Load() {
			t.Errorf("row count mismatch: got %d, expected %d", count, insertedRows.Load())
		}
	})

	benchSub(t, "concurrent_create_drop_tables", func(t *testing.T, m *metric) {
		const numWorkers = 4
		const cyclesPerWorker = 8
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var completedCycles atomic.Int64
		errs := make(chan error, numWorkers*cyclesPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for c := range cyclesPerWorker {
					tableName := fmt.Sprintf("dl_conc_lifecycle_%d_%d", workerID, c)

					// CREATE
					_, err := wconn.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, val TEXT)", tableName))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d cycle %d create: %w", workerID, c, err)
						return
					}

					// INSERT
					_, err = wconn.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'test')", tableName))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							_, _ = wconn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
							continue
						}
						errs <- fmt.Errorf("worker %d cycle %d insert: %w", workerID, c, err)
						_, _ = wconn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
						return
					}

					// READ
					var count int
					if err := wconn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							_, _ = wconn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
							continue
						}
						errs <- fmt.Errorf("worker %d cycle %d read: %w", workerID, c, err)
						_, _ = wconn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
						return
					}

					// DROP
					_, err = wconn.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							_, _ = wconn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
							continue
						}
						errs <- fmt.Errorf("worker %d cycle %d drop: %w", workerID, c, err)
						return
					}
					completedCycles.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = completedCycles.Load(), conflicts.Load()
		t.Logf("create/drop cycles: %d completed, %d conflicts", m.Successes, m.Conflicts)
	})

	benchSub(t, "concurrent_inserts_separate_tables", func(t *testing.T, m *metric) {
		// DuckLake uses global snapshot IDs, so even writes to separate tables
		// can conflict. This test specifically targets that behavior.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		const numTables = 6
		const rowsPerTable = 50
		for i := range numTables {
			mustExec(t, conn, fmt.Sprintf("CREATE TABLE dl_conc_sep_%d (id INTEGER, val TEXT)", i))
		}
		defer func() {
			for i := range numTables {
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_conc_sep_%d", i))
			}
		}()

		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var successes atomic.Int64
		errs := make(chan error, numTables*rowsPerTable)

		for tbl := range numTables {
			wg.Add(1)
			go func(tableIdx int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				tableName := fmt.Sprintf("dl_conc_sep_%d", tableIdx)
				for r := range rowsPerTable {
					_, err := wconn.Exec(
						fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableName),
						r, fmt.Sprintf("table%d-row%d", tableIdx, r),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("table %d row %d: %w", tableIdx, r, err)
						return
					}
					successes.Add(1)
				}
			}(tbl)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = successes.Load(), conflicts.Load()
		t.Logf("separate table inserts: %d succeeded, %d conflicts (cross-table)", m.Successes, m.Conflicts)
		if conflicts.Load() > 0 {
			t.Logf("NOTE: %d cross-table transaction conflicts detected — this is a DuckLake global snapshot behavior", conflicts.Load())
		}
	})

	benchSub(t, "rapid_autocommit_inserts", func(t *testing.T, m *metric) {
		// Fivetran-like pattern: many small autocommit inserts in rapid succession
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_rapid (id INTEGER, ts BIGINT, data TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_rapid") }()

		const numWorkers = 10
		const insertsPerWorker = 30
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var successes atomic.Int64
		errs := make(chan error, numWorkers*insertsPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range insertsPerWorker {
					id := workerID*insertsPerWorker + i
					_, err := wconn.Exec(
						"INSERT INTO dl_conc_rapid VALUES ($1, $2, $3)",
						id, time.Now().UnixMicro(), strings.Repeat("x", 100),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d insert %d: %w", workerID, i, err)
						return
					}
					successes.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = successes.Load(), conflicts.Load()
		t.Logf("rapid autocommit: %d succeeded, %d conflicts", m.Successes, m.Conflicts)

		// Check data integrity
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_rapid").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if int64(count) != successes.Load() {
			t.Errorf("row count mismatch: got %d, expected %d", count, successes.Load())
		}

		// High conflict rate is a signal of the DuckLake 0.4 regression
		conflictRate := float64(conflicts.Load()) / float64(numWorkers*insertsPerWorker) * 100
		t.Logf("conflict rate: %.1f%%", conflictRate)
		if conflictRate > 50 {
			t.Errorf("conflict rate too high: %.1f%% — may indicate DuckLake concurrency regression", conflictRate)
		}
	})

	benchSub(t, "concurrent_tx_rollback_stress", func(t *testing.T, m *metric) {
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_rollback (id INTEGER, val TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_rollback") }()

		const numWorkers = 6
		const txPerWorker = 20
		var wg sync.WaitGroup
		var commits atomic.Int64
		var rollbacks atomic.Int64
		var conflicts atomic.Int64
		errs := make(chan error, numWorkers*txPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range txPerWorker {
					tx, err := wconn.Begin()
					if err != nil {
						errs <- fmt.Errorf("worker %d tx %d begin: %w", workerID, i, err)
						return
					}

					id := workerID*txPerWorker + i
					_, err = tx.Exec("INSERT INTO dl_conc_rollback VALUES ($1, $2)", id, fmt.Sprintf("w%d-i%d", workerID, i))
					if err != nil {
						if isTransactionConflict(err) {
							conflicts.Add(1)
							_ = tx.Rollback()
							continue
						}
						_ = tx.Rollback()
						errs <- fmt.Errorf("worker %d tx %d insert: %w", workerID, i, err)
						return
					}

					// Randomly decide to commit or rollback
					if i%3 == 0 {
						_ = tx.Rollback()
						rollbacks.Add(1)
					} else {
						if err := tx.Commit(); err != nil {
							if isTransactionConflict(err) {
								conflicts.Add(1)
								continue
							}
							errs <- fmt.Errorf("worker %d tx %d commit: %w", workerID, i, err)
							return
						}
						commits.Add(1)
					}
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = commits.Load(), conflicts.Load()
		t.Logf("rollback stress: %d commits, %d rollbacks, %d conflicts", m.Successes, rollbacks.Load(), m.Conflicts)

		// Only committed rows should be visible
		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_rollback").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if int64(count) != commits.Load() {
			t.Errorf("row count mismatch: got %d rows, expected %d (commits only)", count, commits.Load())
		}
	})

	benchSub(t, "sustained_concurrent_load", func(t *testing.T, m *metric) {
		// Simulate sustained concurrent load over a time window (like Fivetran sync)
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_conc_sustained (id BIGINT, worker INTEGER, ts BIGINT, payload TEXT)")
		defer func() { _, _ = conn.Exec("DROP TABLE IF EXISTS dl_conc_sustained") }()

		const numWorkers = 6
		const duration = 5 * time.Second
		var wg sync.WaitGroup
		var totalInserted atomic.Int64
		var totalConflicts atomic.Int64
		var totalErrors atomic.Int64

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				deadline := time.Now().Add(duration)
				var seq int
				for time.Now().Before(deadline) {
					seq++
					id := int64(workerID)*1_000_000 + int64(seq)
					_, err := wconn.Exec(
						"INSERT INTO dl_conc_sustained VALUES ($1, $2, $3, $4)",
						id, workerID, time.Now().UnixMicro(), fmt.Sprintf("data-%d-%d", workerID, seq),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							totalConflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						totalErrors.Add(1)
						recoverConnection(wconn) // recover from any broken state
						continue
					}
					totalInserted.Add(1)
				}
			}(w)
		}

		wg.Wait()

		m.Successes, m.Conflicts, m.Errors = totalInserted.Load(), totalConflicts.Load(), totalErrors.Load()
		t.Logf("sustained load (%v): %d inserted, %d conflicts, %d errors",
			duration, m.Successes, m.Conflicts, m.Errors)

		var count int
		if err := conn.QueryRow("SELECT COUNT(*) FROM dl_conc_sustained").Scan(&count); err != nil {
			t.Fatalf("count query failed: %v", err)
		}
		if int64(count) != totalInserted.Load() {
			t.Errorf("row count mismatch: got %d, expected %d", count, totalInserted.Load())
		}

		if totalErrors.Load() > 0 {
			t.Errorf("%d non-conflict errors during sustained load", totalErrors.Load())
		}

		// Report throughput
		throughput := float64(totalInserted.Load()) / duration.Seconds()
		conflictRate := float64(totalConflicts.Load()) / float64(totalInserted.Load()+totalConflicts.Load()) * 100
		t.Logf("throughput: %.0f inserts/sec, conflict rate: %.1f%%", throughput, conflictRate)
	})

	benchSub(t, "concurrent_ctas", func(t *testing.T, m *metric) {
		// CREATE TABLE AS SELECT (CTAS) is a combined DDL+DML operation that
		// creates a new table and populates it in a single transaction. Under
		// concurrency this is especially conflict-prone because the DDL catalog
		// write and the data write both go through the DuckLake metadata store.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Seed a source table
		mustExec(t, conn, "CREATE TABLE dl_ctas_source (id INTEGER, val TEXT, score DOUBLE)")
		for i := range 100 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_ctas_source VALUES (%d, 'item-%d', %f)", i, i, float64(i)*1.5))
		}
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_ctas_source")
			for w := range 8 {
				for c := range 10 {
					_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_ctas_out_%d_%d", w, c))
				}
			}
		}()

		const numWorkers = 6
		const ctasPerWorker = 8
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var created atomic.Int64
		errs := make(chan error, numWorkers*ctasPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for c := range ctasPerWorker {
					tableName := fmt.Sprintf("dl_ctas_out_%d_%d", workerID, c)
					// Each worker creates tables from different slices of the source
					offset := (workerID*ctasPerWorker + c) % 80
					_, err := wconn.Exec(fmt.Sprintf(
						"CREATE TABLE %s AS SELECT * FROM dl_ctas_source WHERE id >= %d AND id < %d",
						tableName, offset, offset+20,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d ctas %d: %w", workerID, c, err)
						return
					}

					// Verify the table was created with correct row count
					var count int
					if err := wconn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d ctas %d verify: %w", workerID, c, err)
						return
					}
					if count != 20 {
						errs <- fmt.Errorf("worker %d ctas %d: expected 20 rows, got %d", workerID, c, count)
						return
					}
					created.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = created.Load(), conflicts.Load()
		t.Logf("concurrent CTAS: %d created, %d conflicts", m.Successes, m.Conflicts)
	})

	benchSub(t, "sqlmesh_ctas_distinct_targets", func(t *testing.T, m *metric) {
		// Reproduces the exact SQLMesh production failure pattern:
		// - Multiple models run concurrently via ThreadPoolExecutor
		// - Each model does CREATE OR REPLACE TABLE <distinct_target> AS SELECT ... FROM <shared_source>
		// - Targets are DIFFERENT tables in the SAME schema
		// - DuckLake conflicts because metadata transactions collide at the catalog level
		//
		// Prod error: "Transaction conflict - attempting to insert into table with
		// index "25667" - but another transaction has altered it"
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		numModels := 10
		if os.Getenv("DUCKGRES_STRESS") != "" {
			numModels = 30
		}

		// Create a shared source table (like SQLMesh upstream model)
		mustExec(t, conn, "CREATE TABLE dl_sqlmesh_source (id INTEGER, ts BIGINT, category TEXT, val DOUBLE)")
		sourceRows := numModels * 50 // 50 rows per category
		for i := range sourceRows {
			cat := fmt.Sprintf("cat_%d", i%numModels)
			mustExec(t, conn, fmt.Sprintf(
				"INSERT INTO dl_sqlmesh_source VALUES (%d, %d, '%s', %f)",
				i, int64(i)*1000, cat, float64(i)*1.5,
			))
		}
		// Pre-create all target tables so CREATE OR REPLACE has something to replace
		// (matches SQLMesh re-run behavior where tables already exist from prior runs)
		for i := range numModels {
			mustExec(t, conn, fmt.Sprintf(
				"CREATE TABLE dl_sqlmesh_model_%d AS SELECT * FROM dl_sqlmesh_source WHERE category = 'cat_%d'",
				i, i,
			))
		}
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_sqlmesh_source")
			for i := range 30 { // clean up max possible models
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_sqlmesh_model_%d", i))
			}
		}()

		// Simulate SQLMesh parallel model evaluation:
		// Each "model" does CREATE OR REPLACE TABLE model_X AS SELECT ... FROM source
		// All models run concurrently, targeting DISTINCT tables in the same schema
		numRounds := 3 // multiple evaluation rounds like sqlmesh plan + apply cycles
		if os.Getenv("DUCKGRES_STRESS") != "" {
			numRounds = 5
		}
		var totalConflicts atomic.Int64
		var totalSuccesses atomic.Int64
		var totalErrors atomic.Int64

		for round := range numRounds {
			var wg sync.WaitGroup
			errs := make(chan error, numModels)

			for model := range numModels {
				wg.Add(1)
				go func(modelID, roundID int) {
					defer wg.Done()
					mconn := openDuckgresConn(t)
					defer func() { _ = mconn.Close() }()

					cat := fmt.Sprintf("cat_%d", modelID)
					tableName := fmt.Sprintf("dl_sqlmesh_model_%d", modelID)

					// This is the exact query SQLMesh generates for a FULL model refresh
					_, err := mconn.Exec(fmt.Sprintf(
						"CREATE OR REPLACE TABLE %s AS SELECT * FROM dl_sqlmesh_source WHERE category = '%s'",
						tableName, cat,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							totalConflicts.Add(1)
							recoverConnection(mconn)
							return
						}
						errs <- fmt.Errorf("round %d model %d: %w", roundID, modelID, err)
						return
					}
					totalSuccesses.Add(1)

					// Verify the table was created with correct data
					var count int
					if err := mconn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							recoverConnection(mconn)
							return
						}
						errs <- fmt.Errorf("round %d model %d verify: %w", roundID, modelID, err)
						return
					}
					if count != 50 { // 50 rows per category
						errs <- fmt.Errorf("round %d model %d: expected 50 rows, got %d", roundID, modelID, count)
					}
				}(model, round)
			}

			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		}

		m.Successes, m.Conflicts, m.Errors = totalSuccesses.Load(), totalConflicts.Load(), totalErrors.Load()
		t.Logf("SQLMesh CTAS pattern: %d succeeded, %d conflicts, %d errors across %d rounds of %d models",
			m.Successes, m.Conflicts, m.Errors, numRounds, numModels)

		conflictRate := float64(m.Conflicts) / float64(m.Successes+m.Conflicts) * 100
		t.Logf("conflict rate: %.1f%%", conflictRate)
		if m.Conflicts > 0 {
			t.Logf("NOTE: DuckLake transaction conflicts on DISTINCT tables in the same schema — this is the SQLMesh prod regression pattern")
		}
	})

	benchSub(t, "sqlmesh_ctas_with_deps", func(t *testing.T, m *metric) {
		// Extended SQLMesh pattern: models have dependency tiers (DAG levels).
		// Tier 1 models run first, tier 2 models depend on tier 1 outputs.
		// Within each tier, models run concurrently.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Raw source
		mustExec(t, conn, "CREATE TABLE dl_sqlmesh_raw (id INTEGER, ts BIGINT, event TEXT, user_id INTEGER)")
		for i := range 300 {
			evt := fmt.Sprintf("event_%d", i%5)
			mustExec(t, conn, fmt.Sprintf(
				"INSERT INTO dl_sqlmesh_raw VALUES (%d, %d, '%s', %d)",
				i, int64(i)*1000, evt, i%20,
			))
		}

		// Pre-create tier-1 and tier-2 tables
		const numTier1 = 5
		const numTier2 = 6
		for i := range numTier1 {
			mustExec(t, conn, fmt.Sprintf(
				"CREATE TABLE dl_sqlmesh_t1_%d AS SELECT * FROM dl_sqlmesh_raw WHERE event = 'event_%d'",
				i, i,
			))
		}
		for i := range numTier2 {
			src := i % numTier1
			mustExec(t, conn, fmt.Sprintf(
				"CREATE TABLE dl_sqlmesh_t2_%d AS SELECT *, %d AS tier2_id FROM dl_sqlmesh_t1_%d",
				i, i, src,
			))
		}

		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_sqlmesh_raw")
			for i := range numTier1 {
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_sqlmesh_t1_%d", i))
			}
			for i := range numTier2 {
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_sqlmesh_t2_%d", i))
			}
		}()

		var tier1Conflicts, tier2Conflicts atomic.Int64
		var tier1Successes, tier2Successes atomic.Int64

		// Tier 1: all run concurrently, each CTAS from raw source
		{
			var wg sync.WaitGroup
			errs := make(chan error, numTier1)
			for i := range numTier1 {
				wg.Add(1)
				go func(modelID int) {
					defer wg.Done()
					mconn := openDuckgresConn(t)
					defer func() { _ = mconn.Close() }()

					_, err := mconn.Exec(fmt.Sprintf(
						"CREATE OR REPLACE TABLE dl_sqlmesh_t1_%d AS SELECT * FROM dl_sqlmesh_raw WHERE event = 'event_%d'",
						modelID, modelID,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							tier1Conflicts.Add(1)
							recoverConnection(mconn)
							return
						}
						errs <- fmt.Errorf("tier1 model %d: %w", modelID, err)
						return
					}
					tier1Successes.Add(1)
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			t.Logf("tier 1: %d succeeded, %d conflicts", tier1Successes.Load(), tier1Conflicts.Load())
		}

		// Tier 2: all run concurrently, each CTAS from a tier-1 output
		{
			var wg sync.WaitGroup
			errs := make(chan error, numTier2)
			for i := range numTier2 {
				wg.Add(1)
				go func(modelID int) {
					defer wg.Done()
					mconn := openDuckgresConn(t)
					defer func() { _ = mconn.Close() }()

					src := modelID % numTier1
					_, err := mconn.Exec(fmt.Sprintf(
						"CREATE OR REPLACE TABLE dl_sqlmesh_t2_%d AS SELECT *, %d AS tier2_id FROM dl_sqlmesh_t1_%d",
						modelID, modelID, src,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							tier2Conflicts.Add(1)
							recoverConnection(mconn)
							return
						}
						errs <- fmt.Errorf("tier2 model %d: %w", modelID, err)
						return
					}
					tier2Successes.Add(1)
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			t.Logf("tier 2: %d succeeded, %d conflicts", tier2Successes.Load(), tier2Conflicts.Load())
		}

		m.Successes = tier1Successes.Load() + tier2Successes.Load()
		m.Conflicts = tier1Conflicts.Load() + tier2Conflicts.Load()
		t.Logf("SQLMesh DAG pattern: %d succeeded, %d conflicts total", m.Successes, m.Conflicts)
	})

	benchSub(t, "concurrent_create_or_replace_as_select", func(t *testing.T, m *metric) {
		// CREATE OR REPLACE TABLE AS SELECT is the most conflict-prone pattern:
		// it drops the existing table and recreates it atomically, so concurrent
		// writers all race to replace the same target table.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		// Seed a source table
		mustExec(t, conn, "CREATE TABLE dl_cortas_source (id INTEGER, category TEXT, amount DOUBLE)")
		for i := range 200 {
			cat := fmt.Sprintf("cat-%d", i%5)
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_cortas_source VALUES (%d, '%s', %f)", i, cat, float64(i)*2.5))
		}
		// Create the target tables upfront so CREATE OR REPLACE has something to replace
		for i := range 4 {
			mustExec(t, conn, fmt.Sprintf("CREATE TABLE dl_cortas_target_%d (id INTEGER)", i))
		}
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_cortas_source")
			for i := range 4 {
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_cortas_target_%d", i))
			}
		}()

		const numWorkers = 8
		const replacesPerWorker = 10
		const numTargets = 4 // Workers compete to replace the same target tables
		var wg sync.WaitGroup
		var conflicts atomic.Int64
		var replaced atomic.Int64
		errs := make(chan error, numWorkers*replacesPerWorker)

		for w := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for r := range replacesPerWorker {
					targetIdx := (workerID + r) % numTargets
					tableName := fmt.Sprintf("dl_cortas_target_%d", targetIdx)
					cat := fmt.Sprintf("cat-%d", (workerID+r)%5)

					_, err := wconn.Exec(fmt.Sprintf(
						"CREATE OR REPLACE TABLE %s AS SELECT * FROM dl_cortas_source WHERE category = '%s'",
						tableName, cat,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							conflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("worker %d replace %d: %w", workerID, r, err)
						return
					}
					replaced.Add(1)
				}
			}(w)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = replaced.Load(), conflicts.Load()
		t.Logf("concurrent CREATE OR REPLACE: %d replaced, %d conflicts", m.Successes, m.Conflicts)

		// Verify all target tables exist and have valid data
		for i := range numTargets {
			tableName := fmt.Sprintf("dl_cortas_target_%d", i)
			var count int
			if err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
				t.Errorf("target %s: query failed: %v", tableName, err)
				continue
			}
			if count == 0 {
				t.Errorf("target %s: empty (expected rows from source)", tableName)
			}
			t.Logf("  %s: %d rows", tableName, count)
		}
	})

	benchSub(t, "ctas_while_writing_source", func(t *testing.T, m *metric) {
		// CTAS reading from a table while other connections are actively
		// writing to it — tests snapshot isolation of the source read.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_ctas_live_source (id INTEGER, val TEXT)")
		for i := range 50 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_ctas_live_source VALUES (%d, 'seed-%d')", i, i))
		}
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_ctas_live_source")
			for i := range 20 {
				_, _ = conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS dl_ctas_snapshot_%d", i))
			}
		}()

		var wg sync.WaitGroup
		var writeConflicts atomic.Int64
		var ctasConflicts atomic.Int64
		var snapshots atomic.Int64
		errs := make(chan error, 200)

		// Writers: continuously insert into the source table
		const numWriters = 3
		const writesPerWorker = 40
		for w := range numWriters {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for i := range writesPerWorker {
					id := 1000 + writerID*writesPerWorker + i
					_, err := wconn.Exec(
						"INSERT INTO dl_ctas_live_source VALUES ($1, $2)",
						id, fmt.Sprintf("live-%d-%d", writerID, i),
					)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							writeConflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("writer %d op %d: %w", writerID, i, err)
						return
					}
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				}
			}(w)
		}

		// CTAS workers: periodically snapshot the source table
		const numSnapshooters = 3
		const snapshotsPerWorker = 5
		for s := range numSnapshooters {
			wg.Add(1)
			go func(snapID int) {
				defer wg.Done()
				sconn := openDuckgresConn(t)
				defer func() { _ = sconn.Close() }()

				for i := range snapshotsPerWorker {
					tableName := fmt.Sprintf("dl_ctas_snapshot_%d", snapID*snapshotsPerWorker+i)
					_, err := sconn.Exec(fmt.Sprintf(
						"CREATE TABLE %s AS SELECT * FROM dl_ctas_live_source",
						tableName,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							ctasConflicts.Add(1)
							recoverConnection(sconn)
							continue
						}
						errs <- fmt.Errorf("snapshot %d-%d: %w", snapID, i, err)
						return
					}

					// Each snapshot should have a consistent row count (>= seed)
					var count int
					if err := sconn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							ctasConflicts.Add(1)
							recoverConnection(sconn)
							continue
						}
						errs <- fmt.Errorf("snapshot %d-%d verify: %w", snapID, i, err)
						return
					}
					if count < 50 {
						errs <- fmt.Errorf("snapshot %d-%d: only %d rows (expected >= 50 seed rows)", snapID, i, count)
						return
					}
					snapshots.Add(1)
					t.Logf("  snapshot %s: %d rows", tableName, count)

					time.Sleep(20 * time.Millisecond) // Space out snapshots
				}
			}(s)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts = snapshots.Load(), writeConflicts.Load()+ctasConflicts.Load()
		t.Logf("CTAS while writing: %d snapshots taken, %d write conflicts, %d CTAS conflicts",
			m.Successes, writeConflicts.Load(), ctasConflicts.Load())
	})

	benchSub(t, "concurrent_replace_while_reading", func(t *testing.T, m *metric) {
		// CREATE OR REPLACE while other connections are reading the same table.
		// Readers should either see the old or new data, never a partial state.
		conn := openDuckgresConn(t)
		defer func() { _ = conn.Close() }()

		mustExec(t, conn, "CREATE TABLE dl_replace_source_a (id INTEGER, val TEXT)")
		mustExec(t, conn, "CREATE TABLE dl_replace_source_b (id INTEGER, val TEXT)")
		mustExec(t, conn, "CREATE TABLE dl_replace_target (id INTEGER, val TEXT)")
		for i := range 30 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_replace_source_a VALUES (%d, 'aaa')", i))
		}
		for i := range 60 {
			mustExec(t, conn, fmt.Sprintf("INSERT INTO dl_replace_source_b VALUES (%d, 'bbb')", i))
		}
		// Initialize target with source_a data
		mustExec(t, conn, "INSERT INTO dl_replace_target SELECT * FROM dl_replace_source_a")
		defer func() {
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_replace_source_a")
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_replace_source_b")
			_, _ = conn.Exec("DROP TABLE IF EXISTS dl_replace_target")
		}()

		var wg sync.WaitGroup
		var replaceConflicts atomic.Int64
		var replaces atomic.Int64
		var reads atomic.Int64
		var inconsistent atomic.Int64
		errs := make(chan error, 200)

		// Replacers: alternate replacing target with source_a and source_b
		const numReplacers = 3
		const replacesPerWorker = 10
		for w := range numReplacers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				wconn := openDuckgresConn(t)
				defer func() { _ = wconn.Close() }()

				for r := range replacesPerWorker {
					source := "dl_replace_source_a"
					if (workerID+r)%2 == 1 {
						source = "dl_replace_source_b"
					}
					_, err := wconn.Exec(fmt.Sprintf(
						"CREATE OR REPLACE TABLE dl_replace_target AS SELECT * FROM %s", source,
					))
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							replaceConflicts.Add(1)
							recoverConnection(wconn)
							continue
						}
						errs <- fmt.Errorf("replacer %d op %d: %w", workerID, r, err)
						return
					}
					replaces.Add(1)
					time.Sleep(10 * time.Millisecond)
				}
			}(w)
		}

		// Readers: continuously read target and verify consistency
		const numReaders = 4
		const readsPerWorker = 30
		for r := range numReaders {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()
				rconn := openDuckgresConn(t)
				defer func() { _ = rconn.Close() }()

				for i := range readsPerWorker {
					var count int
					var val string
					// Read count and a sample value in the same query for consistency
					err := rconn.QueryRow(
						"SELECT COUNT(*), MIN(val) FROM dl_replace_target",
					).Scan(&count, &val)
					if err != nil {
						if isTransactionConflict(err) || isAbortedTransaction(err) {
							recoverConnection(rconn)
							continue
						}
						// Table might be mid-replace
						if isTableNotFound(err) {
							continue
						}
						errs <- fmt.Errorf("reader %d op %d: %w", readerID, i, err)
						return
					}
					reads.Add(1)

					// Count should be exactly 30 (from source_a) or 60 (from source_b)
					// Any other count means we're seeing partial state
					if count != 30 && count != 60 {
						inconsistent.Add(1)
						t.Logf("reader %d op %d: unexpected count %d (val=%q) — possible partial state",
							readerID, i, count, val)
					}

					time.Sleep(5 * time.Millisecond)
				}
			}(r)
		}

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}

		m.Successes, m.Conflicts, m.Errors = replaces.Load(), replaceConflicts.Load(), inconsistent.Load()
		t.Logf("replace while reading: %d replaces, %d reads, %d conflicts, %d inconsistent reads",
			m.Successes, reads.Load(), m.Conflicts, m.Errors)
		if inconsistent.Load() > 0 {
			t.Errorf("%d reads saw partial/inconsistent state (expected 30 or 60 rows)", inconsistent.Load())
		}
	})
}

// isTransactionConflict checks if an error is a DuckLake transaction conflict.
func isTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Transaction conflict") ||
		strings.Contains(msg, "transaction conflict") ||
		strings.Contains(msg, "write-write conflict") ||
		strings.Contains(msg, "Could not commit")
}

// isAbortedTransaction checks if the error is a cascading failure from a prior
// DuckLake transaction conflict where the metadata PostgreSQL connection is
// stuck in an aborted transaction state.
func isAbortedTransaction(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Current transaction is aborted")
}

// recoverConnection issues a ROLLBACK to reset a connection stuck in an
// aborted DuckLake metadata transaction state (cascading from a conflict).
func recoverConnection(db *sql.DB) {
	_, _ = db.Exec("ROLLBACK")
}

// isTableNotFound checks if an error indicates a table doesn't exist
// (can happen during concurrent DDL+DML).
func isTableNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "Table with name") ||
		strings.Contains(msg, "Catalog Error")
}
