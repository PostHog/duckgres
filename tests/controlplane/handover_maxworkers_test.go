package controlplane_test

import (
	"database/sql"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestHandoverWithMaxWorkers verifies that connections work after handover
// when max_workers is set. This exercises the worker semaphore path.
func TestHandoverWithMaxWorkers(t *testing.T) {
	opts := cpOpts{maxWorkers: 4}
	h := startControlPlane(t, opts)

	// Verify basic connectivity
	db := h.openConn(t)
	var result int
	if err := db.QueryRow("SELECT 1").Scan(&result); err != nil {
		t.Fatalf("Pre-handover SELECT 1 failed: %v", err)
	}
	_ = db.Close()

	// Do the handover
	h.doHandover(t)

	// Try to connect after handover — this is where "too many connections" would appear
	for i := 0; i < 5; i++ {
		db2 := h.openConn(t)
		var v int
		if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("Post-handover connection %d failed: %v\nLogs:\n%s", i, err, h.logBuf.String())
		}
		_ = db2.Close()
	}
	t.Logf("All 5 post-handover connections succeeded with max_workers=%d", opts.maxWorkers)
}

// TestHandoverWithMaxWorkersAndActiveConns verifies handover works when
// max_workers is set AND there are active connections during the handover.
func TestHandoverWithMaxWorkersAndActiveConns(t *testing.T) {
	opts := cpOpts{maxWorkers: 4}
	h := startControlPlane(t, opts)

	// Open 4 connections (saturate max_workers)
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require connect_timeout=10", h.port)
	dbs := make([]*sql.DB, 4)
	for i := range dbs {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("Failed to open connection %d: %v", i, err)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		dbs[i] = db
		var v int
		if err := dbs[i].QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("Failed to warm up connection %d: %v", i, err)
		}
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	})

	// Start long-running queries on all connections
	var wg sync.WaitGroup
	queryErrors := make([]error, 4)
	for i := range dbs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var count int64
			if err := dbs[idx].QueryRow("SELECT count(*) FROM range(10000000)").Scan(&count); err != nil {
				queryErrors[idx] = err
			}
		}(i)
	}

	// Let queries start
	time.Sleep(500 * time.Millisecond)

	// Trigger handover while workers are busy
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Wait for handover
	if err := h.waitForLog("Handover complete, took over PG listener.", 30*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	// Wait for in-flight queries to finish
	wg.Wait()

	var errCount int
	for _, err := range queryErrors {
		if err != nil {
			errCount++
			t.Logf("In-flight query error: %v", err)
		}
	}

	// Close old connections
	for _, db := range dbs {
		_ = db.Close()
	}

	// Now the critical test: can we make NEW connections after handover?
	time.Sleep(2 * time.Second) // Give old CP time to drain and shut down workers

	for i := 0; i < 8; i++ {
		db2 := h.openConn(t)
		var v int
		if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("Post-handover connection %d failed (max_workers=%d): %v\nLogs:\n%s",
				i, opts.maxWorkers, err, h.logBuf.String())
		}
		_ = db2.Close()
	}
	t.Logf("All 8 post-handover connections succeeded with max_workers=%d (after %d in-flight errors)", opts.maxWorkers, errCount)
}

// TestHandoverWithMaxWorkersRapidConnections opens many connections right
// at the moment of handover to stress-test the semaphore accounting.
func TestHandoverWithMaxWorkersRapidConnections(t *testing.T) {
	opts := cpOpts{maxWorkers: 3}
	h := startControlPlane(t, opts)

	// Warm up
	db := h.openConn(t)
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatalf("Warmup failed: %v", err)
	}
	_ = db.Close()

	// Trigger handover
	if err := h.sendSignal(syscall.SIGUSR1); err != nil {
		t.Fatalf("Failed to send SIGUSR1: %v", err)
	}

	// Immediately start hammering with connections while handover is in progress
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require connect_timeout=15", h.port)
	const numConns = 20
	var wg sync.WaitGroup
	results := make([]string, numConns)
	errors := make([]error, numConns)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Small stagger to spread connections over the transition window
			time.Sleep(time.Duration(idx*100) * time.Millisecond)

			d, err := sql.Open("postgres", dsn)
			if err != nil {
				errors[idx] = fmt.Errorf("open: %w", err)
				return
			}
			defer d.Close()
			d.SetMaxOpenConns(1)
			d.SetMaxIdleConns(1)

			var val int
			if err := d.QueryRow("SELECT 1").Scan(&val); err != nil {
				errors[idx] = err
				return
			}
			results[idx] = "ok"
		}(i)
	}

	// Wait for handover to complete
	if err := h.waitForLog("Handover complete, took over PG listener.", 30*time.Second); err != nil {
		t.Fatalf("Handover did not complete.\nLogs:\n%s", h.logBuf.String())
	}

	wg.Wait()

	successes := 0
	failures := 0
	for i, err := range errors {
		if err != nil {
			failures++
			t.Logf("Connection %d failed: %v", i, err)
		} else {
			successes++
		}
	}

	t.Logf("Rapid connections during handover: %d succeeded, %d failed (max_workers=%d)", successes, failures, opts.maxWorkers)

	// After everything settles, verify we can still connect
	time.Sleep(2 * time.Second)
	for i := 0; i < 5; i++ {
		db2 := h.openConn(t)
		if err := db2.QueryRow("SELECT 1").Scan(&v); err != nil {
			t.Fatalf("Post-settle connection %d failed: %v\nLogs:\n%s", i, err, h.logBuf.String())
		}
		_ = db2.Close()
	}

	if failures > 0 {
		t.Fatalf("%d connections failed during rapid handover test.\nLogs:\n%s", failures, h.logBuf.String())
	}
}
