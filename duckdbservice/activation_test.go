package duckdbservice

import (
	"database/sql"
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
	pool.createDBConnection = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*sql.DB, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		opened = db
		return db, nil
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

	pool.createDBConnection = func(cfg server.Config, sem chan struct{}, username string, startTime time.Time, version string) (*sql.DB, error) {
		return sql.Open("duckdb", "")
	}
	pool.activateDBConnection = func(db *sql.DB, cfg server.Config, sem chan struct{}, username string) error {
		return nil
	}

	if err := pool.activateTenant(ActivationPayload{
		OrgID: "analytics",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err != nil {
		t.Fatalf("first ActivateTenant: %v", err)
	}

	if err := pool.activateTenant(ActivationPayload{
		OrgID: "billing",
		DuckLake: server.DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
		},
	}); err == nil {
		t.Fatal("expected second activation to fail")
	}
}
