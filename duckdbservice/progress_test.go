package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	bindings "github.com/duckdb/duckdb-go-bindings"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestExtractDuckDBConnection(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	duckConn, err := extractDuckDBConnection(conn)
	if err != nil {
		t.Fatalf("extractDuckDBConnection failed: %v", err)
	}
	if duckConn.Ptr == nil {
		t.Fatal("expected non-nil connection pointer")
	}

	// QueryProgress should return -1 percentage when no query is running.
	qp := bindings.QueryProgress(duckConn)
	pct, _, _ := bindings.QueryProgressTypeMembers(&qp)
	if pct != -1 {
		t.Errorf("expected percentage -1 when idle, got %f", pct)
	}
}

func TestStallCheckThreshold(t *testing.T) {
	// Verify the constant value matches the documented 300 checks × 2s = ~10 minutes.
	if stallCheckThreshold != 300 {
		t.Errorf("expected stallCheckThreshold=300, got %d", stallCheckThreshold)
	}
}

func TestQueryProgressTimeout(t *testing.T) {
	// Verify the timeout is long enough for normal QueryProgress calls
	// but short enough to prevent health check stalls.
	if queryProgressTimeout < 100*time.Millisecond {
		t.Errorf("queryProgressTimeout too short: %v", queryProgressTimeout)
	}
	if queryProgressTimeout > 2*time.Second {
		t.Errorf("queryProgressTimeout too long (must be well under the 3s health check gRPC deadline): %v", queryProgressTimeout)
	}
}

func TestHealthCheckRespondsWhenQueryProgressBlocks(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		warmupDone:  make(chan struct{}),
		startTime:   time.Now(),
	}
	close(pool.warmupDone)

	// Create a session with a fake DuckDB connection whose QueryProgress
	// would block. We can't actually block the CGO call in a unit test,
	// but we CAN verify the restructured code path: snapshot sessions
	// outside the RLock, call QueryProgress per-session, and always
	// produce a response.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	duckConn, err := extractDuckDBConnection(conn)
	if err != nil {
		t.Fatalf("extractDuckDBConnection: %v", err)
	}

	session := &Session{ID: "test-session", duckdbConn: duckConn}
	session.progress.queryActive.Store(true) // simulate active query
	pool.sessions["abcdef1234567890xx"] = session

	handler := &FlightSQLHandler{pool: pool}
	stream := &mockDoActionStream{}

	done := make(chan error, 1)
	go func() {
		done <- handler.doHealthCheck([]byte(`{}`), stream)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("health check returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health check blocked longer than 2s — would trigger CP timeout")
	}

	if len(stream.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(stream.results))
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(stream.results[0].Body, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["healthy"] != true {
		t.Errorf("expected healthy=true, got %v", resp["healthy"])
	}

	// Verify session progress is reported (with pct=-1 since no real query)
	sp, ok := resp["session_progress"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected session_progress map, got %T", resp["session_progress"])
	}
	if len(sp) != 1 {
		t.Fatalf("expected 1 session in progress, got %d", len(sp))
	}
}
