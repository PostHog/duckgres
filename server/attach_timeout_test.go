package server

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func openAttachTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestAttachStepExecerFastStatement verifies the happy path: statements that
// finish within the deadline behave exactly like plain Exec/QueryRow.
func TestAttachStepExecerFastStatement(t *testing.T) {
	db := openAttachTestDB(t)
	ae := newAttachStepExecer(db)

	if _, err := ae.Exec("CREATE TABLE t (i INTEGER)"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if _, err := ae.execTimeout(attachStatementTimeout, "INSERT INTO t VALUES (41), (1)"); err != nil {
		t.Fatalf("execTimeout: %v", err)
	}
	var sum int
	if err := ae.queryRowScan("SELECT sum(i) FROM t", &sum); err != nil {
		t.Fatalf("queryRowScan: %v", err)
	}
	if sum != 42 {
		t.Fatalf("sum = %d, want 42", sum)
	}

	// No statements were abandoned, so releaseSem must release synchronously.
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	ae.releaseSem(sem)
	select {
	case sem <- struct{}{}:
	default:
		t.Fatal("releaseSem did not release the semaphore synchronously")
	}
}

// TestAttachStepExecerInterruptsLongQuery proves the deadline actually stops a
// long-running DuckDB statement: the duckdb-go driver propagates ctx
// cancellation by repeatedly calling duckdb_interrupt, which a query observes
// at execution checkpoints. The cross product below would run for minutes;
// with a sub-second deadline the call must fail fast — and because the
// statement responded to the interrupt (returned before the watchdog wall),
// the semaphore is released synchronously.
func TestAttachStepExecerInterruptsLongQuery(t *testing.T) {
	db := openAttachTestDB(t)
	ae := newAttachStepExecer(db)

	start := time.Now()
	_, err := ae.execTimeout(300*time.Millisecond,
		"SELECT count(*) FROM range(100000) t1(i) CROSS JOIN range(100000) t2(j) WHERE (i*j) % 7 = 3")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error %q does not mention the timeout", err)
	}
	// Must return well before the query's natural runtime. The watchdog wall
	// is at timeout+attachInterruptGrace (5.3s); the interrupt path should be
	// far quicker, but allow the full wall plus slack for CI scheduling.
	if elapsed > 300*time.Millisecond+attachInterruptGrace+5*time.Second {
		t.Fatalf("timeout took %s, statement was not bounded", elapsed)
	}

	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	ae.releaseSem(sem)
	// If the interrupt worked (no abandoned statements) the release is
	// synchronous; if the statement had to be abandoned, the release happens
	// once the underlying call returns. Either way it must free up shortly.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case sem <- struct{}{}:
			return
		case <-deadline:
			t.Fatal("semaphore not released after long-query timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestAttachStepExecerWatchdogAbandonsHungCall covers the non-interruptible
// case (a CGO call blocked in extension network I/O never observes
// duckdb_interrupt): the watchdog must return an error at timeout+grace,
// and releaseSem must hold the semaphore until the underlying call actually
// finishes — releasing earlier would let a second attach race the hung one.
func TestAttachStepExecerWatchdogAbandonsHungCall(t *testing.T) {
	origGrace := attachInterruptGrace
	attachInterruptGrace = 100 * time.Millisecond
	defer func() { attachInterruptGrace = origGrace }()

	hung := make(chan struct{})
	ae := newAttachStepExecer(nil) // db unused: run() takes fn directly

	start := time.Now()
	err := ae.run(50*time.Millisecond, "ATTACH 'fake://hung' AS hung", func(ctx context.Context) error {
		<-hung // ignores ctx, like a blocked TCP connect inside an extension
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected watchdog timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "did not respond to interrupt") {
		t.Fatalf("error %q does not identify the abandoned call", err)
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("watchdog fired after %s, before timeout+grace", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("watchdog took %s, not bounded", elapsed)
	}

	// The hung statement is still "running": releaseSem must NOT free the
	// slot yet.
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	ae.releaseSem(sem)
	time.Sleep(50 * time.Millisecond)
	select {
	case sem <- struct{}{}:
		t.Fatal("semaphore released while the abandoned statement was still running")
	default:
	}

	// Once the underlying call finishes, the slot must be released.
	close(hung)
	deadline := time.After(5 * time.Second)
	for {
		select {
		case sem <- struct{}{}:
			return
		case <-deadline:
			t.Fatal("semaphore not released after abandoned statement finished")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestAttachStepExecerReleaseSemNil verifies the nil-semaphore no-op used by
// callers that run without the DuckLake semaphore.
func TestAttachStepExecerReleaseSemNil(t *testing.T) {
	ae := newAttachStepExecer(nil)
	ae.releaseSem(nil) // must not panic or block
}

// TestSummarizeAttachStmt verifies log/error output never includes secret
// material embedded in CREATE SECRET statements.
func TestSummarizeAttachStmt(t *testing.T) {
	secretStmt := `
		CREATE OR REPLACE SECRET ducklake_s3 (
			TYPE s3,
			PROVIDER config,
			KEY_ID 'AKIAEXAMPLEKEY',
			SECRET 'supersecretvalue'
		)`
	got := summarizeAttachStmt(secretStmt)
	if strings.Contains(got, "AKIAEXAMPLEKEY") || strings.Contains(got, "supersecretvalue") {
		t.Fatalf("summary leaks credentials: %q", got)
	}
	if !strings.Contains(got, "CREATE OR REPLACE SECRET ducklake_s3") {
		t.Fatalf("summary lost the identifying prefix: %q", got)
	}

	if got := summarizeAttachStmt("SET ducklake_max_retry_count = 100"); got != "SET ducklake_max_retry_count = 100" {
		t.Fatalf("short statement mangled: %q", got)
	}
}
