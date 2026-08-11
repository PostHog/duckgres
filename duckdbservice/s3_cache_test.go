package duckdbservice

import (
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/transpiler/transform"
)

// s3CacheTestPool builds a shared-warm pool with a synthetic activation and a
// recording refreshS3Secret stub, for exercising SetS3CacheEnabled without a
// real DuckDB or cache proxy.
func s3CacheTestPool(t *testing.T, objectStore string) (*SessionPool, *[]server.DuckLakeConfig) {
	t.Helper()
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
		s3CacheMode:    transform.S3CacheOn,
		activation: &activatedTenantRuntime{payload: ActivationPayload{
			OrgID: "analytics",
			DuckLake: server.DuckLakeConfig{
				MetadataStore:  "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
				ObjectStore:    objectStore,
				S3Region:       "us-east-1",
				S3UseSSL:       true,
				S3AccessKey:    "ACCESS_KEY",
				S3SecretKey:    "SECRET_KEY",
				S3SessionToken: "SESSION_TOKEN",
			},
		}},
	}
	close(pool.warmupDone)
	var calls []server.DuckLakeConfig
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		calls = append(calls, dlCfg)
		return nil
	}
	return pool, &calls
}

// TestSetS3CacheEnabledSwapsTransportAndBack asserts the core swap contract:
// disabling the cache rebuilds ducklake_s3 from the RAW payload config (the
// org's native HTTPS transport — traffic then CONNECT-tunnels through the
// proxy uncached), re-enabling rebuilds WITH the cache-proxy transport, and
// redundant calls never touch the secret.
func TestSetS3CacheEnabledSwapsTransportAndBack(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")
	pool, calls := s3CacheTestPool(t, "s3://analytics/warehouse/")

	// Redundant enable: already on, no secret touch.
	if err := pool.SetS3CacheEnabled(true); err != nil {
		t.Fatalf("SetS3CacheEnabled(true) on fresh pool: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("redundant enable rebuilt the secret: %d calls", len(*calls))
	}

	// Disable: rebuild with the raw (native HTTPS) transport.
	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("SetS3CacheEnabled(false): %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("disable: %d secret rebuilds, want 1", len(*calls))
	}
	if got := (*calls)[0]; got.HTTPProxy != "" || !got.S3UseSSL || got.S3Endpoint != "" {
		t.Fatalf("bypass rebuild must use the raw org transport (no proxy, HTTPS, no pinned endpoint), got HTTPProxy=%q S3UseSSL=%v S3Endpoint=%q",
			got.HTTPProxy, got.S3UseSSL, got.S3Endpoint)
	}
	pool.mu.RLock()
	mode := pool.s3CacheMode
	pool.mu.RUnlock()
	if mode != transform.S3CacheOff {
		t.Fatalf("s3CacheMode after disable = %q, want off", mode)
	}

	// Redundant disable: no secret touch.
	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("redundant SetS3CacheEnabled(false): %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("redundant disable rebuilt the secret: %d calls", len(*calls))
	}

	// Re-enable: rebuild WITH the cache-proxy transport.
	if err := pool.SetS3CacheEnabled(true); err != nil {
		t.Fatalf("SetS3CacheEnabled(true): %v", err)
	}
	if len(*calls) != 2 {
		t.Fatalf("enable: %d secret rebuilds, want 2", len(*calls))
	}
	if got := (*calls)[1]; got.HTTPProxy != "http://10.0.0.9:8080" || got.S3UseSSL || got.S3Endpoint != "s3.us-east-1.amazonaws.com" {
		t.Fatalf("restore rebuild must carry the cache-proxy transport, got HTTPProxy=%q S3UseSSL=%v S3Endpoint=%q",
			got.HTTPProxy, got.S3UseSSL, got.S3Endpoint)
	}
	pool.mu.RLock()
	mode = pool.s3CacheMode
	pool.mu.RUnlock()
	if mode != transform.S3CacheOn {
		t.Fatalf("s3CacheMode after enable = %q, want on", mode)
	}
}

func TestSetS3CachePassthroughKeepsProxyTransport(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")
	pool, calls := s3CacheTestPool(t, "s3://analytics/warehouse/")

	if err := pool.SetS3CacheMode("passthrough"); err != nil {
		t.Fatalf("SetS3CacheMode(passthrough): %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("passthrough: %d secret rebuilds, want 1", len(*calls))
	}
	if got := (*calls)[0]; got.HTTPProxy != "http://10.0.0.9:8080" || got.S3UseSSL || got.S3Endpoint != "s3.us-east-1.amazonaws.com" {
		t.Fatalf("passthrough must retain the cache-proxy transport, got HTTPProxy=%q S3UseSSL=%v S3Endpoint=%q", got.HTTPProxy, got.S3UseSSL, got.S3Endpoint)
	}
	pool.mu.RLock()
	mode := pool.s3CacheMode
	pool.mu.RUnlock()
	if mode != transform.S3CachePassthrough {
		t.Fatalf("s3CacheMode after passthrough = %q, want passthrough", mode)
	}
}

// TestSetS3CacheEnabledNoOpWithoutCacheProxy asserts the toggle degrades to a
// silent no-op when the node runs no cache proxy (DUCKGRES_CACHE_ENABLED
// unset): the org transport is already direct, there is nothing to bypass,
// and a SET from a client must not error or churn the secret.
func TestSetS3CacheEnabledNoOpWithoutCacheProxy(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "")
	pool, calls := s3CacheTestPool(t, "s3://analytics/warehouse/")

	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("SetS3CacheEnabled(false) without cache proxy: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("no-op rebuilt the secret: %d calls", len(*calls))
	}
}

// TestSetS3CacheEnabledNoOpOutsideSharedWarm asserts non-tenant workers
// (process backend, standalone duckdb-service) no-op: the cache-proxy
// transport override is applied only by shared-warm tenant activation, so
// there is nothing to swap.
func TestSetS3CacheEnabledNoOpOutsideSharedWarm(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	pool, calls := s3CacheTestPool(t, "s3://analytics/warehouse/")
	pool.sharedWarmMode = false

	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("SetS3CacheEnabled(false) outside shared-warm: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("non-shared-warm toggle rebuilt the secret: %d calls", len(*calls))
	}
}

// TestSetS3CacheEnabledRequiresActivation asserts a disable on a
// not-yet-activated shared-warm worker errors instead of silently recording a
// state there is no secret to apply it to.
func TestSetS3CacheEnabledRequiresActivation(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	pool, _ := s3CacheTestPool(t, "s3://analytics/warehouse/")
	pool.activation = nil

	if err := pool.SetS3CacheEnabled(false); err == nil {
		t.Fatal("SetS3CacheEnabled(false) on unactivated worker: nil error, want failure")
	}
}

// TestSetS3CacheEnabledFailurePreservesState asserts a failed secret rebuild
// leaves the flag on its previous value (the session state the CP reports must
// track the secret actually in place) and a retry can still succeed.
func TestSetS3CacheEnabledFailurePreservesState(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")
	pool, calls := s3CacheTestPool(t, "s3://analytics/warehouse/")

	boom := errors.New("boom")
	fail := true
	inner := pool.refreshS3Secret
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		if fail {
			return boom
		}
		return inner(db, dlCfg, sem)
	}

	if err := pool.SetS3CacheEnabled(false); err == nil || !errors.Is(err, boom) {
		t.Fatalf("SetS3CacheEnabled(false) with failing rebuild: err = %v, want boom", err)
	}
	pool.mu.RLock()
	mode := pool.s3CacheMode
	pool.mu.RUnlock()
	if mode != transform.S3CacheOn {
		t.Fatalf("failed rebuild changed s3CacheMode to %q, want on", mode)
	}

	fail = false
	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("retry: %d successful rebuilds, want 1", len(*calls))
	}
}

// TestCredentialRefreshPreservesS3CacheBypass is the inverse of the mw-prod-us
// 2026-07-17 regression test: while a session is bypassing the cache
// (`duckgres.s3_cache = off`), a CP-driven credential rotation re-activation
// must rebuild ducklake_s3 WITHOUT the cache-proxy transport — otherwise the
// hourly STS rotation silently re-routes the session through the cache
// mid-benchmark.
func TestCredentialRefreshPreservesS3CacheBypass(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")

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

	var opened *sql.DB
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		opened = db
		return PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}
	defer func() {
		if opened != nil {
			_ = opened.Close()
		}
	}()

	first := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   2,
			CPInstanceID: "cp-live:boot-a",
			WorkerID:     17,
		},
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore:  "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			ObjectStore:    "s3://analytics/warehouse/",
			S3Region:       "us-east-1",
			S3UseSSL:       true,
			S3AccessKey:    "OLD_ACCESS_KEY",
			S3SecretKey:    "OLD_SECRET_KEY",
			S3SessionToken: "OLD_SESSION_TOKEN",
		},
	}
	if err := pool.activateTenant(first); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	var refreshCfgs []server.DuckLakeConfig
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		refreshCfgs = append(refreshCfgs, dlCfg)
		return nil
	}

	// The live session turns the cache off.
	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("SetS3CacheEnabled(false): %v", err)
	}
	if len(refreshCfgs) != 1 {
		t.Fatalf("disable: %d rebuilds, want 1", len(refreshCfgs))
	}

	// CP-driven credential rotation re-activation while bypassed.
	second := first
	second.DuckLake.S3AccessKey = "NEW_ACCESS_KEY"
	second.DuckLake.S3SecretKey = "NEW_SECRET_KEY"
	second.DuckLake.S3SessionToken = "NEW_SESSION_TOKEN"
	if err := pool.activateTenant(second); err != nil {
		t.Fatalf("credential rotation re-activation: %v", err)
	}
	if len(refreshCfgs) != 2 {
		t.Fatalf("rotation: %d rebuilds, want 2", len(refreshCfgs))
	}
	rotated := refreshCfgs[1]
	if rotated.S3AccessKey != "NEW_ACCESS_KEY" {
		t.Fatalf("rotation rebuild lost the fresh credentials: %q", rotated.S3AccessKey)
	}
	if rotated.HTTPProxy != "" || !rotated.S3UseSSL || rotated.S3Endpoint != "" {
		t.Fatalf("rotation rebuild re-applied the cache-proxy transport while bypassed: HTTPProxy=%q S3UseSSL=%v S3Endpoint=%q",
			rotated.HTTPProxy, rotated.S3UseSSL, rotated.S3Endpoint)
	}

	// Restore, then rotate again: the proxy transport must come back.
	if err := pool.SetS3CacheEnabled(true); err != nil {
		t.Fatalf("SetS3CacheEnabled(true): %v", err)
	}
	third := second
	third.DuckLake.S3AccessKey = "NEWER_ACCESS_KEY"
	third.DuckLake.S3SecretKey = "NEWER_SECRET_KEY"
	third.DuckLake.S3SessionToken = "NEWER_SESSION_TOKEN"
	if err := pool.activateTenant(third); err != nil {
		t.Fatalf("second rotation re-activation: %v", err)
	}
	last := refreshCfgs[len(refreshCfgs)-1]
	if last.HTTPProxy != "http://10.0.0.9:8080" || last.S3UseSSL {
		t.Fatalf("post-restore rotation rebuild lost the cache-proxy transport: HTTPProxy=%q S3UseSSL=%v", last.HTTPProxy, last.S3UseSSL)
	}
}

// TestS3CacheToggleDuringRotationUsesRotatedCreds pins the secretSwapMu
// hold-through-commit invariant: a `duckgres.s3_cache` toggle that arrives
// while a credential-rotation re-activation is mid-rebuild must wait until the
// rotated payload is COMMITTED, so its own rebuild carries the NEW creds. If
// the refresh path released secretSwapMu before the Phase 3 commit, the toggle
// could read the stale pre-rotation payload and last-write the secret with the
// old, soon-to-expire STS credentials while the scheduler records the new
// expiry — killing the session with ExpiredToken an hour later.
func TestS3CacheToggleDuringRotationUsesRotatedCreds(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)

	var opened *sql.DB
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		opened = db
		return PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}
	defer func() {
		if opened != nil {
			_ = opened.Close()
		}
	}()

	first := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 2, CPInstanceID: "cp-live:boot-a", WorkerID: 17},
		OrgID:                 "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			ObjectStore:   "s3://analytics/warehouse/",
			S3Region:      "us-east-1",
			S3UseSSL:      true,
			S3AccessKey:   "OLD_ACCESS_KEY",
			S3SecretKey:   "OLD_SECRET_KEY",
		},
	}
	if err := pool.activateTenant(first); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	rotationEntered := make(chan struct{})
	toggleStarted := make(chan struct{})
	var mu sync.Mutex
	var toggleCfg *server.DuckLakeConfig
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		if dlCfg.HTTPProxy != "" {
			// The rotation rebuild (proxy transport, cache on). Signal the
			// toggle goroutine to start, then give it time to block on
			// secretSwapMu before this rebuild "finishes" — recreating the
			// window between the refresh exec and the payload commit.
			close(rotationEntered)
			<-toggleStarted
			time.Sleep(50 * time.Millisecond)
			return nil
		}
		// The toggle's bypass rebuild (native transport).
		mu.Lock()
		cfg := dlCfg
		toggleCfg = &cfg
		mu.Unlock()
		return nil
	}

	second := first
	second.DuckLake.S3AccessKey = "NEW_ACCESS_KEY"
	second.DuckLake.S3SecretKey = "NEW_SECRET_KEY"

	rotationDone := make(chan error, 1)
	go func() { rotationDone <- pool.activateTenant(second) }()

	<-rotationEntered
	toggleDone := make(chan error, 1)
	go func() {
		close(toggleStarted)
		toggleDone <- pool.SetS3CacheEnabled(false)
	}()

	if err := <-rotationDone; err != nil {
		t.Fatalf("rotation re-activation: %v", err)
	}
	if err := <-toggleDone; err != nil {
		t.Fatalf("SetS3CacheEnabled(false): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if toggleCfg == nil {
		t.Fatal("toggle never rebuilt the secret")
	}
	if toggleCfg.S3AccessKey != "NEW_ACCESS_KEY" {
		t.Fatalf("toggle rebuilt the secret with stale pre-rotation creds: %q (scheduler now believes NEW creds are installed)", toggleCfg.S3AccessKey)
	}
}

// TestS3CacheToggleRacesMetadataOnlyReactivation is the -race regression for
// the retained-pointer read: metadata-only re-activations (needsRefresh=false,
// e.g. an epoch-bump takeover with unchanged credentials) overwrite
// p.activation.payload in place under p.mu WITHOUT taking secretSwapMu, so
// SetS3CacheEnabled must copy the payload while holding p.mu instead of
// reading through a retained *activatedTenantRuntime after the unlock.
// Meaningful under `go test -race`; the toggles and reactivations here
// interleave freely.
func TestS3CacheToggleRacesMetadataOnlyReactivation(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
	}
	close(pool.warmupDone)
	pool.createDBPair = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = db.Close() })
		return PairFromMain(db), nil
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}
	pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
		return nil
	}

	base := ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{OwnerEpoch: 1, CPInstanceID: "cp-live:boot-a", WorkerID: 17},
		OrgID:                 "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			ObjectStore:   "s3://analytics/warehouse/",
			S3Region:      "us-east-1",
			S3UseSSL:      true,
			S3AccessKey:   "ACCESS_KEY", // unchanged across reactivations → needsRefresh=false
			S3SecretKey:   "SECRET_KEY",
		},
	}
	if err := pool.activateTenant(base); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	const rounds = 100
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < rounds; i++ {
			next := base
			next.OwnerEpoch = int64(2 + i)
			if err := pool.activateTenant(next); err != nil {
				t.Errorf("metadata-only re-activation (epoch %d): %v", next.OwnerEpoch, err)
				return
			}
		}
	}()
	for i := 0; i < rounds; i++ {
		if err := pool.SetS3CacheEnabled(i%2 == 0); err != nil {
			t.Fatalf("SetS3CacheEnabled toggle %d: %v", i, err)
		}
	}
	<-done
}

// TestCreateSessionRestoresS3CacheTransport asserts the hard invariant that a
// bypass can never leak into the org's next session: CreateSession on a worker
// whose previous session disabled the cache rebuilds the secret with the
// cache-proxy transport before the session starts, and a failed restore fails
// the session create.
func TestCreateSessionRestoresS3CacheTransport(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	newPool := func(refresh func(*sql.DB, server.DuckLakeConfig, chan struct{}) error) *SessionPool {
		pool := &SessionPool{
			sessions:       make(map[string]*Session),
			stopRefresh:    make(map[string]func()),
			duckLakeSem:    make(chan struct{}, 1),
			warmupDB:       db,
			warmupDone:     make(chan struct{}),
			cfg:            server.Config{SessionInitTimeout: time.Second},
			maxSessions:    1,
			sharedWarmMode: true,
			activation: &activatedTenantRuntime{payload: ActivationPayload{
				OrgID: "analytics",
				DuckLake: server.DuckLakeConfig{
					ObjectStore: "s3://analytics/warehouse/",
					S3Region:    "us-east-1",
					S3UseSSL:    true,
				},
			}, db: db},
			s3CacheMode:     transform.S3CacheOff, // previous session left the cache off
			refreshS3Secret: refresh,
		}
		close(pool.warmupDone)
		return pool
	}

	t.Run("restores proxy transport before the session starts", func(t *testing.T) {
		var restored []server.DuckLakeConfig
		pool := newPool(func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
			restored = append(restored, dlCfg)
			return nil
		})

		session, _, err := pool.CreateSession("root", "", 0, nil)
		if err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		defer func() { _ = pool.DestroySession(session.ID) }()

		if len(restored) != 1 {
			t.Fatalf("restore rebuilds = %d, want 1", len(restored))
		}
		if got := restored[0]; got.HTTPProxy != "http://10.0.0.9:8080" || got.S3UseSSL {
			t.Fatalf("restore rebuild must carry the cache-proxy transport, got HTTPProxy=%q S3UseSSL=%v", got.HTTPProxy, got.S3UseSSL)
		}
		pool.mu.RLock()
		mode := pool.s3CacheMode
		pool.mu.RUnlock()
		if mode != transform.S3CacheOn {
			t.Fatalf("s3CacheMode after CreateSession restore = %q, want on", mode)
		}
	})

	t.Run("failed restore fails the session create", func(t *testing.T) {
		pool := newPool(func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
			return errors.New("boom")
		})

		_, _, err := pool.CreateSession("root", "", 0, nil)
		if err == nil {
			t.Fatal("CreateSession with failing restore: nil error, want failure")
		}
		if !strings.Contains(err.Error(), "restore S3 cache transport") {
			t.Fatalf("CreateSession error = %v, want restore-S3-cache failure", err)
		}
		// The reserved slot must have been released so the worker isn't leaked
		// at cap: a follow-up create (with the restore now succeeding) works.
		pool.refreshS3Secret = func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error { return nil }
		session, _, err := pool.CreateSession("root", "", 0, nil)
		if err != nil {
			t.Fatalf("CreateSession after failed restore: %v", err)
		}
		_ = pool.DestroySession(session.ID)
	})
}

// TestDestroySessionRestoresS3CacheTransport asserts the best-effort restore
// at session teardown: a hot-idle worker between sessions runs its
// checkpointer through the cache proxy again without waiting for the next
// CreateSession.
func TestDestroySessionRestoresS3CacheTransport(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "10.0.0.9")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	var rebuilds []server.DuckLakeConfig
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		warmupDB:       db,
		warmupDone:     make(chan struct{}),
		cfg:            server.Config{SessionInitTimeout: time.Second},
		maxSessions:    1,
		sharedWarmMode: true,
		activation: &activatedTenantRuntime{payload: ActivationPayload{
			OrgID: "analytics",
			DuckLake: server.DuckLakeConfig{
				ObjectStore: "s3://analytics/warehouse/",
				S3Region:    "us-east-1",
				S3UseSSL:    true,
			},
		}, db: db},
		refreshS3Secret: func(db *sql.DB, dlCfg server.DuckLakeConfig, sem chan struct{}) error {
			rebuilds = append(rebuilds, dlCfg)
			return nil
		},
	}
	close(pool.warmupDone)

	session, _, err := pool.CreateSession("root", "", 0, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	// The session turns the cache off mid-flight.
	if err := pool.SetS3CacheEnabled(false); err != nil {
		t.Fatalf("SetS3CacheEnabled(false): %v", err)
	}
	rebuilds = rebuilds[:0]

	if err := pool.DestroySession(session.ID); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}
	if len(rebuilds) != 1 {
		t.Fatalf("destroy restore rebuilds = %d, want 1", len(rebuilds))
	}
	if got := rebuilds[0]; got.HTTPProxy != "http://10.0.0.9:8080" || got.S3UseSSL {
		t.Fatalf("destroy restore must carry the cache-proxy transport, got HTTPProxy=%q S3UseSSL=%v", got.HTTPProxy, got.S3UseSSL)
	}
	pool.mu.RLock()
	mode := pool.s3CacheMode
	pool.mu.RUnlock()
	if mode != transform.S3CacheOn {
		t.Fatalf("s3CacheMode after DestroySession restore = %q, want on", mode)
	}
}
