package duckdbservice

import (
	"context"
	"database/sql"
	"testing"

	"github.com/duckdb/duckdb-go/mapping"
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
	qp := mapping.QueryProgress(duckConn)
	pct, _, _ := mapping.QueryProgressTypeMembers(&qp)
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
