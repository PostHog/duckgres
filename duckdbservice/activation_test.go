package duckdbservice

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
)

func TestSessionPoolActivateTenantConfiguresTenantRuntime(t *testing.T) {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		duckLakeSem: make(chan struct{}, 1),
		cfg: server.Config{
			Users: map[string]string{"postgres": "postgres"},
		},
		startTime:  time.Now(),
		warmupDone: make(chan struct{}),
	}
	close(pool.warmupDone)

	var captured server.Config
	var opened *sql.DB
	pool.sharedWarmMode = true
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*server.DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		opened = db
		return server.PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		captured = cfg
		return nil
	}
	defer func() {
		if opened != nil {
			_ = opened.Close()
		}
	}()

	err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   1,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			ObjectStore:   "s3://analytics/warehouse/",
		},
	})
	if err != nil {
		t.Fatalf("ActivateTenant: %v", err)
	}

	current := pool.currentActivation()
	if current == nil || current.payload.OrgID != "analytics" {
		t.Fatalf("expected activated org analytics, got %#v", current)
	}
	if captured.DuckLake.MetadataStore == "" || captured.DuckLake.ObjectStore == "" {
		t.Fatalf("expected activated DuckLake config to be applied, got %#v", captured.DuckLake)
	}
	if pool.warmupDB == nil {
		t.Fatal("expected activated warmup DB to be set")
	}
}

func TestSessionPoolActivateTenantRejectsSecondActivation(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		cfg:            server.Config{Users: map[string]string{"postgres": "postgres"}},
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*server.DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return server.PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}

	if err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   1,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	if err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   2,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
		OrgID: "billing",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err == nil {
		t.Fatal("expected second activation to fail")
	}
}

func TestSessionPoolActivateTenantRejectsStaleOwnerEpoch(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		cfg:            server.Config{Users: map[string]string{"postgres": "postgres"}},
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*server.DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return server.PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}

	if err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 2},
		OrgID:                 "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	if err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 1},
		OrgID:                 "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err == nil {
		t.Fatal("expected stale owner epoch to be rejected")
	}
}

func TestSessionPoolActivateTenantAllowsSameOrgTakeover(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		cfg:            server.Config{Users: map[string]string{"postgres": "postgres"}},
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	var activateCalls int
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*server.DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return server.PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		activateCalls++
		return nil
	}

	first := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   2,
			CPInstanceID: "cp-old:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			ObjectStore:   "s3://analytics/warehouse/",
		},
	}
	if err := pool.activateTenant(first); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	second := first
	second.OwnerEpoch = 3
	second.CPInstanceID = "cp-new:boot-b"
	if err := pool.activateTenant(second); err != nil {
		t.Fatalf("takeover ActivateTenant: %v", err)
	}

	if activateCalls != 1 {
		t.Fatalf("expected same-tenant takeover to reuse existing activation, got %d activation calls", activateCalls)
	}
	current := pool.currentActivation()
	if current == nil {
		t.Fatal("expected activation to remain present")
		return
	}
	if current.payload.OwnerEpoch != 3 {
		t.Fatalf("expected owner epoch 3, got %d", current.payload.OwnerEpoch)
	}
	if current.payload.CPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected cp instance id cp-new:boot-b, got %q", current.payload.CPInstanceID)
	}
	if pool.ownerEpoch != 3 {
		t.Fatalf("expected pool owner epoch 3, got %d", pool.ownerEpoch)
	}
	if pool.ownerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected pool owner cp instance id cp-new:boot-b, got %q", pool.ownerCPInstanceID)
	}
}

func TestSessionPoolActivateTenantRejectsSameEpochOwnerChange(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		cfg:            server.Config{Users: map[string]string{"postgres": "postgres"}},
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*server.DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return server.PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}

	first := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   2,
			CPInstanceID: "cp-old:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}
	if err := pool.activateTenant(first); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	second := first
	second.CPInstanceID = "cp-new:boot-b"
	if err := pool.activateTenant(second); err == nil {
		t.Fatal("expected same-epoch owner change to be rejected")
	}
}

func TestSessionPoolValidateControlMetadataAcceptsMismatchedCPInstanceID(t *testing.T) {
	pool := &SessionPool{
		sharedWarmMode:    true,
		ownerEpoch:        4,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}

	// CP instance ID mismatches are no longer rejected — any CP should be
	// able to health-check any worker. Ownership is managed by the config store.
	err := pool.validateControlMetadata(server.WorkerControlMetadata{
		WorkerID:     17,
		OwnerEpoch:   4,
		CPInstanceID: "cp-other:boot-b",
	})
	if err != nil {
		t.Fatalf("expected mismatched cp_instance_id to be accepted, got: %v", err)
	}
}

// TestHealthCheckFailsAfterCPRollingRestart reproduces the epoch mismatch
// that causes worker pod cascading deaths during a CP rolling update.
//
// Scenario:
//  1. CP-old activates a worker with epoch=1
//  2. CP-old is killed in a rolling update
//  3. CP-new starts fresh — it discovers the worker pod via K8s informer
//     but hasn't re-activated it yet, so its in-memory epoch is 0
//  4. CP-new's health check loop sends a health check with epoch=0
//  5. Worker rejects: "stale owner epoch 0 (current 1)"
//  6. After 3 consecutive rejections, CP-new deletes the worker pod
//
// The health check should not kill a worker just because the CP restarted.
// The worker is healthy and serving queries — the epoch mismatch only means
// the CP hasn't re-activated ownership yet.
func TestHealthCheckFailsAfterCPRollingRestart(t *testing.T) {
	// Worker was activated by CP-old with epoch=1
	pool := &SessionPool{
		sharedWarmMode:    true,
		ownerEpoch:        1,
		ownerCPInstanceID: "cp-old:boot-a",
		workerID:          42,
	}

	// CP-new starts fresh after rolling update. It discovers the worker via
	// K8s informer but hasn't re-activated it. Its in-memory epoch for this
	// worker is 0 (default). This is exactly what the health check loop at
	// k8s_pool.go:2270-2278 sends.
	err := pool.validateControlMetadata(server.WorkerControlMetadata{
		WorkerID:     42,
		OwnerEpoch:   0,
		CPInstanceID: "cp-new:boot-b",
	})

	// BUG: This currently fails with "stale owner epoch 0 (current 1)".
	// The health check loop treats this as a failure, and after 3 consecutive
	// failures it deletes the worker pod — even though the worker is perfectly
	// healthy. This cascades to all workers, causing a full cluster outage
	// on every rolling deployment.
	if err != nil {
		t.Fatalf("health check after CP rolling restart should not kill the worker, but got: %v", err)
	}
}

func TestSessionPoolValidateControlMetadataRejectsMismatchedWorkerID(t *testing.T) {
	pool := &SessionPool{
		sharedWarmMode:    true,
		ownerEpoch:        4,
		ownerCPInstanceID: "cp-live:boot-a",
		workerID:          17,
	}

	err := pool.validateControlMetadata(server.WorkerControlMetadata{
		WorkerID:     18,
		OwnerEpoch:   4,
		CPInstanceID: "cp-live:boot-a",
	})
	if err == nil {
		t.Fatal("expected mismatched worker_id to be rejected")
		return
	}
}

// TestReuseExistingActivationDoesNotBlockHealthChecks pins down the deadlock
// that killed workers in mw-prod-us 2026-05-04: reuseExistingActivation used
// to run RefreshS3Secret while still holding p.mu.Lock, so any concurrent
// RLock acquirer (the gRPC health-check goroutine snapshotting sessions)
// stalled until the secret SQL finished — and the secret SQL itself blocked
// on the single client-query connection. After 3 health-check timeouts (~9s)
// the CP force-deleted the worker.
//
// We simulate the slow refresh with a channel and assert that an RLock
// acquirer in another goroutine returns within 100ms while the refresh is
// in-flight. This fails on the pre-fix code path (RLock blocks until the
// channel is closed) and passes on the lock-released path.
func TestReuseExistingActivationDoesNotBlockHealthChecks(t *testing.T) {
	mainDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open main duckdb: %v", err)
	}
	defer mainDB.Close()
	controlDB, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open control duckdb: %v", err)
	}
	defer controlDB.Close()

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
		warmupDB:       mainDB,
		controlDB:      controlDB,
		activation: &activatedTenantRuntime{
			payload: ActivationPayload{
				WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 1},
				OrgID:                 "analytics",
				DuckLake: server.DuckLakeConfig{
					MetadataStore: "postgres:host=meta port=5432 user=u password=p dbname=d",
					ObjectStore:   "s3://analytics/",
					S3AccessKey:   "OLD_ACCESS_KEY",
					S3SecretKey:   "OLD_SECRET_KEY",
				},
			},
			db: mainDB,
		},
		ownerEpoch: 1,
	}
	close(pool.warmupDone)

	released := make(chan struct{})
	refreshStarted := make(chan struct{})
	var refreshDB *sql.DB
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		refreshDB = db
		close(refreshStarted)
		<-released // simulate a CREATE OR REPLACE SECRET stuck behind a long query
		return nil
	}

	newPayload := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 2},
		OrgID:                 "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=meta port=5432 user=u password=p dbname=d",
			ObjectStore:   "s3://analytics/",
			S3AccessKey:   "NEW_ACCESS_KEY",
			S3SecretKey:   "NEW_SECRET_KEY",
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !pool.reuseExistingActivation(newPayload) {
			t.Errorf("reuseExistingActivation returned false; expected true after refresh")
		}
	}()

	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start within 1s")
	}

	rlockAcquired := make(chan struct{})
	go func() {
		pool.mu.RLock()
		close(rlockAcquired)
		pool.mu.RUnlock()
	}()

	select {
	case <-rlockAcquired:
		// Good — RLock returned while RefreshS3Secret is still running.
	case <-time.After(100 * time.Millisecond):
		close(released) // unblock the goroutine so wg.Wait doesn't hang
		t.Fatal("RLock blocked while refresh was in flight: deadlock regression — reuseExistingActivation is holding p.mu.Lock across the slow secret rotation")
	}

	close(released)
	wg.Wait()

	// Fix #2 assertion: the slow refresh must run on controlDB (the side
	// connection), not on the main client-query DB.
	if refreshDB != controlDB {
		t.Errorf("expected refresh to run on controlDB, ran on %p (controlDB=%p, mainDB=%p)", refreshDB, controlDB, mainDB)
	}

	// Sanity: the new payload must be committed.
	if pool.activation == nil || pool.activation.payload.DuckLake.S3AccessKey != "NEW_ACCESS_KEY" {
		t.Fatalf("expected new credentials committed to activation payload; got %#v", pool.activation)
	}
	if pool.ownerEpoch != 2 {
		t.Fatalf("expected ownerEpoch=2 after refresh, got %d", pool.ownerEpoch)
	}
}
