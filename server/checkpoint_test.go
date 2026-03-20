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
