package server

import (
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

// DuckLakeCheckpointer runs DuckLake CHECKPOINT on a schedule.
// CHECKPOINT performs full catalog maintenance: expires snapshots,
// merges adjacent files, rewrites data files, and cleans up orphaned files.
type DuckLakeCheckpointer struct {
	db       *sql.DB
	interval time.Duration
	done     chan struct{}
	loopDone chan struct{}
	stopOnce sync.Once
}

// NewDuckLakeCheckpointer opens a dedicated DuckDB connection, attaches DuckLake,
// and starts a background goroutine that runs CHECKPOINT on the configured interval.
func NewDuckLakeCheckpointer(cfg Config) (*DuckLakeCheckpointer, error) {
	if cfg.DuckLake.MetadataStore == "" || cfg.DuckLake.CheckpointInterval <= 0 {
		return nil, nil
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("checkpoint: open duckdb: %w", err)
	}

	extDir := filepath.Join(cfg.DataDir, "extensions")
	if _, err := db.Exec(fmt.Sprintf("SET extension_directory = '%s'", extDir)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("checkpoint: set extension_directory: %w", err)
	}

	if _, err := db.Exec("INSTALL ducklake; LOAD ducklake"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("checkpoint: load ducklake: %w", err)
	}

	dlCfg := cfg.DuckLake
	if dlCfg.ObjectStore != "" {
		needsSecret := dlCfg.S3Endpoint != "" ||
			dlCfg.S3AccessKey != "" ||
			dlCfg.S3Provider == "credential_chain" ||
			dlCfg.S3Provider == "aws_sdk" ||
			dlCfg.S3Chain != "" ||
			dlCfg.S3Profile != ""
		if needsSecret {
			if err := createS3Secret(db, dlCfg); err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("checkpoint: create S3 secret: %w", err)
			}
		}
	}

	attachStmt := buildDuckLakeAttachStmt(dlCfg, duckLakeMigrationNeeded())
	if _, err := db.Exec(attachStmt); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("checkpoint: attach ducklake: %w", err)
	}

	c := &DuckLakeCheckpointer{
		db:       db,
		interval: cfg.DuckLake.CheckpointInterval,
		done:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}

	go c.loop()
	slog.Info("DuckLake checkpoint scheduler started.", "interval", cfg.DuckLake.CheckpointInterval)
	return c, nil
}

// Stop shuts down the checkpoint scheduler, waits for any in-progress
// checkpoint to finish, and closes the database connection.
func (c *DuckLakeCheckpointer) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.done)
		<-c.loopDone
		if c.db != nil {
			_ = c.db.Close()
		}
	})
}

func (c *DuckLakeCheckpointer) loop() {
	defer close(c.loopDone)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.run()
		case <-c.done:
			return
		}
	}
}

func (c *DuckLakeCheckpointer) run() {
	slog.Info("DuckLake checkpoint starting.")
	start := time.Now()
	_, err := c.db.Exec("CHECKPOINT ducklake")
	if err != nil {
		slog.Warn("DuckLake checkpoint failed.", "error", err)
		return
	}
	slog.Info("DuckLake checkpoint complete.", "duration", time.Since(start).Round(time.Millisecond))
}
