package duckdbservice

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
)

// Audit H3 regression test.
//
// On the process backend, FlightWorkerPool.AcquireWorker co-assigns sessions:
// at pool capacity it hands the connection to the least-loaded LIVE worker,
// so a worker legitimately receives a second concurrent CreateSession
// (process workers run with the default unlimited max-sessions). Every
// session pins a dedicated *sql.Conn from the worker's shared main DB for its
// whole lifetime, and OpenDuckDBPair caps that DB at MaxOpenConns=1 — so
// without resizing, the second CreateSession can never get a connection while
// the first session is alive: it blocks for the full 30s pool timeout, fails
// with the conn-pool-timeout error, the control plane classifies the worker
// as WEDGED (isWorkerConnPoolTimeoutError) and retires it — killing the
// co-resident in-flight first session.
//
// The invariant: a worker must be able to serve as many concurrent sessions
// as it admits. The pool resizes the pair's Main connection capacity to the
// session cap (sizeMainForSessions); this test drives two concurrent
// sessions through the production pair-creation path and requires the second
// to become usable promptly.
func TestSecondAdmittedConcurrentSessionIsNotStarvedByPinnedConn(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		maxSessions: 0, // process-backend default: unlimited admission
		createDBPair: func(server.Config, chan struct{}, string, time.Time, string) (*DuckDBPair, error) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				return nil, err
			}
			// Mirror OpenDuckDBPair's production setting for the
			// client-query DB; the pool is responsible for resizing it to
			// its session admission.
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			return PairFromMain(db), nil
		},
	}
	close(pool.warmupDone) // warmup skipped; CreateSession takes the fallback pair path

	first, _, err := pool.CreateSession("alice", "", 0, nil)
	if err != nil {
		t.Fatalf("first CreateSession: %v", err)
	}
	defer func() { _ = pool.DestroySession(first.ID) }()

	type result struct {
		session *Session
		err     error
	}
	done := make(chan result, 1)
	go func() {
		s, _, err := pool.CreateSession("bob", "", 0, nil)
		done <- result{s, err}
	}()

	// The second session was admitted (no session-cap rejection), so it must
	// become usable promptly — not sit starved behind the first session's
	// pinned conn until the 30s pool timeout fires and the CP retires the
	// whole worker as wedged.
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("second admitted concurrent session failed: %v", r.err)
		}
		_ = pool.DestroySession(r.session.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("second admitted concurrent session starved behind the first session's pinned conn (MaxOpenConns=1): it will hit the 30s conn-pool timeout, which the control plane treats as a wedged worker and retires — killing the first session's in-flight query")
	}
}
