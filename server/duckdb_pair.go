package server

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// DuckDBPair holds two *sql.DBs that share the same underlying DuckDB instance:
//
//   - Main is the client-query DB (MaxOpenConns=1, single-session isolation for
//     the user's pgwire session — this is the existing behavior).
//   - Control is reserved for the control plane's non-blocking ops (CREATE OR
//     REPLACE SECRET on credential refresh, ATTACH/DETACH) and is also
//     MaxOpenConns=1 so a control DDL can't fight itself, but it has its own
//     pool so it never queues behind a long-running client query.
//
// Both *sql.DBs share one *duckdb.Connector / one DuckDB Database handle, which
// means catalogs, the SecretManager, and registered extensions are all visible
// across both. The main session running a query benefits from a fresh secret
// the moment Control swaps it — see RefreshS3Secret callers for the rationale.
//
// The owner must call Close on the pair when shutting down. Close on the
// individual *sql.DBs goes through a non-closing wrapper, so the underlying
// DuckDB instance only goes away when DuckDBPair.Close fires.
type DuckDBPair struct {
	Main      *sql.DB
	Control   *sql.DB
	connector *duckdb.Connector
}

// Close closes both *sql.DBs and the underlying DuckDB instance. Safe to call
// multiple times.
func (p *DuckDBPair) Close() error {
	if p == nil {
		return nil
	}
	var firstErr error
	if p.Main != nil {
		if err := p.Main.Close(); err != nil {
			firstErr = err
		}
		p.Main = nil
	}
	if p.Control != nil {
		if err := p.Control.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.Control = nil
	}
	if p.connector != nil {
		if err := p.connector.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		p.connector = nil
	}
	return firstErr
}

// nonClosingConnector turns Close into a noop so the wrapped Connector can be
// shared across multiple sql.OpenDB calls. Each *sql.DB's Close goes through
// this wrapper; the real connector is closed by the owner via DuckDBPair.Close.
type nonClosingConnector struct {
	inner driver.Connector
}

func (n *nonClosingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return n.inner.Connect(ctx)
}

func (n *nonClosingConnector) Driver() driver.Driver { return n.inner.Driver() }

func (*nonClosingConnector) Close() error { return nil }

// OpenDuckDBPair builds the DSN that openBaseDB would have used, opens one
// DuckDB Database via *duckdb.Connector, and returns a Main + Control
// *sql.DB sharing it. The Main DB still goes through the same configuration
// path as openBaseDB (threads, memory_limit, extensions, profiling, cache
// settings); the Control DB is intentionally minimal.
func OpenDuckDBPair(cfg Config, username string) (*DuckDBPair, error) {
	dsn, err := duckDBDSN(cfg, username)
	if err != nil {
		return nil, err
	}

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb connector: %w", err)
	}

	wrapped := &nonClosingConnector{inner: connector}
	mainDB := sql.OpenDB(wrapped)
	controlDB := sql.OpenDB(wrapped)

	mainDB.SetMaxOpenConns(1)
	mainDB.SetMaxIdleConns(1)
	controlDB.SetMaxOpenConns(1)
	controlDB.SetMaxIdleConns(1)

	if err := mainDB.Ping(); err != nil {
		_ = mainDB.Close()
		_ = controlDB.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("failed to ping duckdb (main): %w", err)
	}
	if err := controlDB.Ping(); err != nil {
		_ = mainDB.Close()
		_ = controlDB.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("failed to ping duckdb (control): %w", err)
	}

	if err := configureMainDB(mainDB, cfg, username); err != nil {
		_ = mainDB.Close()
		_ = controlDB.Close()
		_ = connector.Close()
		return nil, err
	}

	return &DuckDBPair{
		Main:      mainDB,
		Control:   controlDB,
		connector: connector,
	}, nil
}

// CreateWorkerDBPair is the worker-process factory: it opens a shared-connector
// DuckDB pair and runs ConfigureDBConnection on Main (pg_catalog,
// information_schema, DuckLake attach) so it matches what the existing
// CreateDBConnection produces. Control is left in its minimal post-Ping
// state — it sees the same Database (and therefore the same SecretManager and
// attached catalogs) thanks to the shared connector, so no per-conn init is
// needed for credential rotation.
func CreateWorkerDBPair(cfg Config, duckLakeSem chan struct{}, username string, serverStartTime time.Time, serverVersion string) (*DuckDBPair, error) {
	pair, err := OpenDuckDBPair(cfg, username)
	if err != nil {
		return nil, err
	}
	if err := ConfigureDBConnection(pair.Main, cfg, duckLakeSem, username, serverStartTime, serverVersion); err != nil {
		_ = pair.Close()
		return nil, err
	}
	return pair, nil
}

// PairFromMain builds a *DuckDBPair that points Main and Control at the same
// *sql.DB and owns no connector. Intended for test fixtures that don't
// exercise the connector-sharing properties — production code uses
// CreateWorkerDBPair, which returns a real pair.
func PairFromMain(db *sql.DB) *DuckDBPair {
	return &DuckDBPair{Main: db, Control: db}
}

// duckDBDSN returns the DSN openBaseDB would build for cfg/username. Kept
// separate so OpenDuckDBPair and openBaseDB stay in lock-step.
func duckDBDSN(cfg Config, username string) (string, error) {
	dsn := ":memory:?allow_unsigned_extensions=true"
	if cfg.FilePersistence && cfg.DataDir != "" && username != "" {
		if strings.ContainsAny(username, "/\\") || strings.Contains(username, "..") {
			return "", fmt.Errorf("invalid username for file persistence: %q (contains path separator or ..)", username)
		}
		if err := os.MkdirAll(cfg.DataDir, 0750); err != nil {
			return "", fmt.Errorf("failed to create data directory %s: %w", cfg.DataDir, err)
		}
		dsn = filepath.Join(cfg.DataDir, username+".duckdb") + "?allow_unsigned_extensions=true"
		slog.Info("Opening file-backed DuckDB.", "path", dsn)
	}
	return dsn, nil
}
