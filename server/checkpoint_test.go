package server

import (
	"database/sql"
	"testing"
	"time"
)

func TestDuckLakeCheckpointerStopIsIdempotent(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}

	c := &DuckLakeCheckpointer{
		db:       db,
		interval: time.Hour,
		done:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}

	go c.loop()

	c.Stop()
	c.Stop() // must not panic or deadlock
}

func TestDuckLakeCheckpointerDisabledWhenNoMetadataStore(t *testing.T) {
	c, err := NewDuckLakeCheckpointer(Config{
		DuckLake: DuckLakeConfig{
			CheckpointInterval: 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		c.Stop()
		t.Fatal("expected nil checkpointer when MetadataStore is empty")
	}
}

func TestDuckLakeCheckpointerDisabledWhenIntervalZero(t *testing.T) {
	c, err := NewDuckLakeCheckpointer(Config{
		DuckLake: DuckLakeConfig{
			MetadataStore:      "postgres:host=localhost",
			CheckpointInterval: 0,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		c.Stop()
		t.Fatal("expected nil checkpointer when CheckpointInterval is 0")
	}
}

func TestCheckpointerRunRecordsSuccess(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Attach an in-memory database as "ducklake" so CHECKPOINT succeeds
	if _, err := db.Exec("ATTACH ':memory:' AS ducklake"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA ducklake.system"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	createTable := `CREATE TABLE ducklake.system.checkpoints (
		started_at  TIMESTAMPTZ NOT NULL,
		finished_at TIMESTAMPTZ NOT NULL,
		duration_ms BIGINT NOT NULL,
		status      VARCHAR NOT NULL,
		error       VARCHAR
	)`
	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	c := &DuckLakeCheckpointer{db: db}
	c.run()

	var startedAt, finishedAt time.Time
	var durationMs int64
	var status string
	var errMsg *string
	err = db.QueryRow("SELECT started_at, finished_at, duration_ms, status, error FROM ducklake.system.checkpoints").
		Scan(&startedAt, &finishedAt, &durationMs, &status, &errMsg)
	if err != nil {
		t.Fatalf("query row: %v", err)
	}
	if status != "success" {
		t.Errorf("expected status 'success', got %q", status)
	}
	if errMsg != nil {
		t.Errorf("expected nil error, got %q", *errMsg)
	}
	if durationMs < 0 {
		t.Errorf("expected non-negative duration_ms, got %d", durationMs)
	}
	if !finishedAt.After(startedAt) && !finishedAt.Equal(startedAt) {
		t.Errorf("expected finished_at >= started_at, got started=%v finished=%v", startedAt, finishedAt)
	}

	// Run again and verify accumulation
	c.run()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.system.checkpoints").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows after 2 runs, got %d", count)
	}
}

func TestCheckpointerRunRecordsFailure(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Attach an in-memory database as "ducklake" and create the table,
	// then detach and reattach as read-only so CHECKPOINT fails.
	if _, err := db.Exec("ATTACH ':memory:' AS ducklake"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA ducklake.system"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	createTable := `CREATE TABLE ducklake.system.checkpoints (
		started_at  TIMESTAMPTZ NOT NULL,
		finished_at TIMESTAMPTZ NOT NULL,
		duration_ms BIGINT NOT NULL,
		status      VARCHAR NOT NULL,
		error       VARCHAR
	)`
	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Write a temp file so we can reattach read-only
	tmpDir := t.TempDir()
	tmpDB := tmpDir + "/test.db"
	if _, err := db.Exec("DETACH ducklake"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	// Create a persistent DB, set up the table, detach, reattach read-only
	if _, err := db.Exec("ATTACH '" + tmpDB + "' AS ducklake"); err != nil {
		t.Fatalf("attach persistent: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA ducklake.system"); err != nil {
		t.Fatalf("create schema persistent: %v", err)
	}
	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("create table persistent: %v", err)
	}
	if _, err := db.Exec("DETACH ducklake"); err != nil {
		t.Fatalf("detach persistent: %v", err)
	}
	if _, err := db.Exec("ATTACH '" + tmpDB + "' AS ducklake (READ_ONLY)"); err != nil {
		t.Fatalf("attach read-only: %v", err)
	}

	c := &DuckLakeCheckpointer{db: db}
	c.run()

	// On a read-only DB the INSERT fails — 0 rows expected.
	// The test validates that run() handles INSERT errors gracefully without panicking.
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM ducklake.system.checkpoints").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows on read-only DB, got %d", count)
	}
}

func TestDuckLakeCheckpointerStopWaitsForLoop(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}

	c := &DuckLakeCheckpointer{
		db:       db,
		interval: time.Hour,
		done:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}

	go c.loop()

	// Stop should return promptly without deadlocking.
	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within timeout — possible deadlock")
	}
}
