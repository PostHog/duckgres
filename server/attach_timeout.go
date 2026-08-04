package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Per-step deadlines for catalog attachment (AttachDuckLake, AttachDeltaCatalog)
// and secret refresh (RefreshS3Secret).
//
// Why these exist: every step in those functions is a plain CGO db.Exec /
// db.QueryRow with no deadline. A hung network dependency inside DuckDB
// (an S3 connect that blackholes, an IMDS probe, a wedged metadata
// Postgres) therefore eats the control plane's entire activate-tenant
// deadline (~60s) with zero Go-level logging — and, far worse, the activation
// holds a drain token (beginDrainWork in duckdbservice/flight_handler.go), so
// a retired worker pod with a hung attach stays Draining for up to
// workerShutdownDrainTime (55m), pinning a warm EC2 node. Per-step deadlines
// turn that into a fast, attributable failure.
const (
	// attachStepTimeout bounds the fast attachment steps: extension
	// INSTALL/LOAD, CREATE SECRET, catalog presence checks, SET statements,
	// and the default-schema CREATE. These are local or single-round-trip
	// operations that complete in milliseconds when healthy; 30s is already
	// generous slack for a cold extension INSTALL from the CDN.
	attachStepTimeout = 30 * time.Second
	// attachStatementTimeout bounds the ATTACH statements themselves (DuckLake,
	// Delta) and the Delta post-attach probe. Deliberately
	// loose: a DuckLake ATTACH against a busy metadata Postgres can
	// legitimately take tens of seconds (catalog snapshot read over many
	// metadata rows), so do NOT tighten this without production latency data.
	attachStatementTimeout = 60 * time.Second
	// attachMigrateStatementTimeout bounds a DuckLake ATTACH that performs an
	// automatic spec migration (AUTOMATIC_MIGRATION TRUE). Migration rewrites
	// metadata tables inside the metadata Postgres and can legitimately run
	// for minutes on a large catalog, so it gets a far looser wall than a
	// normal ATTACH — the point is still "bounded", not "tight".
	attachMigrateStatementTimeout = 15 * time.Minute
)

// attachInterruptGrace is how long past the ctx deadline we keep waiting for
// the driver's duckdb_interrupt to actually unblock the CGO call before
// abandoning the goroutine. The duckdb-go driver re-asserts the interrupt
// every 500ms after cancellation, but DuckDB only observes the flag at query
// execution checkpoints — a statement blocked in extension network I/O (TCP
// connect inside httpfs, IMDS probe) may never see it.
// Variable (not const) so tests can shrink it.
var attachInterruptGrace = 5 * time.Second

// attachStepExecer runs catalog-attachment statements against db with a hard
// per-statement deadline. Each statement runs under a context deadline (the
// duckdb-go driver propagates cancellation by repeatedly calling
// duckdb_interrupt, which stops a query that reaches an execution checkpoint)
// plus a watchdog wall: if the CGO call still hasn't returned
// attachInterruptGrace after the deadline — i.e. it is stuck in
// non-interruptible blocking I/O — the goroutine is abandoned, an error is
// returned to the caller immediately, and the statement is tracked so
// releaseSem only returns the attach semaphore slot once the underlying call
// has actually finished (releasing it earlier would let a second attach race
// the hung one on the same database).
//
// One execer instance covers one sem-held attach region; create it before
// acquiring the semaphore and `defer ae.releaseSem(sem)` in place of the bare
// `defer func() { <-sem }()`.
type attachStepExecer struct {
	db *sql.DB

	mu        sync.Mutex
	abandoned []abandonedAttachStmt
}

type abandonedAttachStmt struct {
	query string
	done  <-chan struct{} // closed when the underlying CGO call finally returns
}

func newAttachStepExecer(db *sql.DB) *attachStepExecer {
	return &attachStepExecer{db: db}
}

// Exec runs query with the default attachStepTimeout. It satisfies
// duckLakeSQLExecer so the execer can be passed straight into
// applyDuckLakePreAttachSettingsWith / configureDuckLakeMetadataPool /
// loadExtensionsWith.
func (a *attachStepExecer) Exec(query string, args ...any) (sql.Result, error) {
	return a.execTimeout(attachStepTimeout, query, args...)
}

// execTimeout runs query with an explicit deadline (use
// attachStatementTimeout for ATTACH statements).
func (a *attachStepExecer) execTimeout(timeout time.Duration, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := a.run(timeout, query, func(ctx context.Context) error {
		r, err := a.db.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// queryRowScan runs a single-row query with the default attachStepTimeout and
// scans the result into dest. On timeout dest is NOT written; callers must
// treat a non-nil error as "value unknown" (all current callers do).
func (a *attachStepExecer) queryRowScan(query string, dest ...any) error {
	return a.run(attachStepTimeout, query, func(ctx context.Context) error {
		return a.db.QueryRowContext(ctx, query).Scan(dest...)
	})
}

// run executes fn (one DuckDB statement) under a timeout. See the
// attachStepExecer doc comment for the two-layer deadline semantics.
func (a *attachStepExecer) run(timeout time.Duration, query string, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		// Cancel only when fn returns: after the deadline fires the driver
		// keeps re-asserting duckdb_interrupt until the call completes, which
		// is exactly what we want for a statement that unblocks late.
		defer cancel()
		errCh <- fn(ctx)
	}()

	wall := time.NewTimer(timeout + attachInterruptGrace)
	defer wall.Stop()
	select {
	case err := <-errCh:
		if err != nil && ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("catalog attachment step timed out after %s (interrupted): %w", timeout, err)
		}
		return err
	case <-wall.C:
		a.mu.Lock()
		a.abandoned = append(a.abandoned, abandonedAttachStmt{query: summarizeAttachStmt(query), done: done})
		a.mu.Unlock()
		slog.Error("Catalog attachment step exceeded its deadline and did not respond to interrupt; abandoning the call. "+
			"The statement may still be executing on its DuckDB connection (likely blocked in non-interruptible network I/O); "+
			"the attach semaphore will not be released until it returns.",
			"timeout", timeout, "grace", attachInterruptGrace, "query", summarizeAttachStmt(query))
		go func(q string, started time.Time) {
			<-done
			slog.Warn("Abandoned catalog attachment statement finally returned.",
				"query", q, "overrun", time.Since(started))
		}(summarizeAttachStmt(query), time.Now())
		return fmt.Errorf("catalog attachment step timed out after %s and did not respond to interrupt (statement may still be executing): %s",
			timeout, summarizeAttachStmt(query))
	}
}

// releaseSem returns the attach semaphore slot acquired by the caller. If any
// statement was abandoned past its deadline, the release is handed to a
// goroutine that waits for the underlying CGO call(s) to actually finish
// first — the semaphore exists to serialize attach work on the database, and
// a hung ATTACH still owns its connection until it returns. If the call never
// returns, the slot is intentionally never released: subsequent attaches fail
// fast on their existing 30s "waiting for attachment lock" timeout instead of
// racing a wedged connection.
//
// No-op when sem is nil (callers that run without the semaphore).
func (a *attachStepExecer) releaseSem(sem chan struct{}) {
	if sem == nil {
		return
	}
	a.mu.Lock()
	abandoned := a.abandoned
	a.abandoned = nil
	a.mu.Unlock()
	if len(abandoned) == 0 {
		<-sem
		return
	}
	go func() {
		for _, st := range abandoned {
			<-st.done
		}
		slog.Warn("Abandoned catalog attachment statement(s) finished; releasing attach semaphore.",
			"count", len(abandoned))
		<-sem
	}()
}

// attachSQLRunner abstracts the statement runner shared by createS3SecretWith
// so catalog-attachment paths run under per-step deadlines (attachStepExecer)
// while standalone callers (checkpoint, querylog) keep plain *sql.DB behavior.
type attachSQLRunner interface {
	Exec(query string, args ...any) (sql.Result, error)
	queryRowScan(query string, dest ...any) error
}

// plainAttachRunner adapts *sql.DB to attachSQLRunner with no deadlines
// (legacy behavior for non-attach callers).
type plainAttachRunner struct{ db *sql.DB }

func (p plainAttachRunner) Exec(query string, args ...any) (sql.Result, error) {
	return p.db.Exec(query, args...)
}

func (p plainAttachRunner) queryRowScan(query string, dest ...any) error {
	return p.db.QueryRow(query).Scan(dest...)
}

// summarizeAttachStmt truncates a statement for log/error output. Attach and
// secret statements can embed credentials further in, but everything before
// the first newline/paren is a safe, identifying prefix.
func summarizeAttachStmt(query string) string {
	const maxLen = 80
	query = strings.TrimSpace(query)
	if i := strings.IndexAny(query, "(\n"); i > 0 && i < maxLen {
		return strings.TrimSpace(query[:i]) + "..."
	}
	if len(query) > maxLen {
		return query[:maxLen] + "..."
	}
	return query
}
