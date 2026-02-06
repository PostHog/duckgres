package controlplane

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/posthog/duckgres/server"
)

// DBPool manages a shared DuckDB database for a long-lived worker.
// The database is opened once and shared across all sessions. Each session
// gets its own *sql.DB with MaxOpenConns=1 for transaction isolation.
type DBPool struct {
	cfg         server.Config
	duckLakeSem chan struct{}

	mu          sync.Mutex
	sessions    map[int32]*sql.DB // keyed by session backend PID
	stopRefresh map[int32]func()  // credential refresh stop functions
}

// NewDBPool creates a new database pool with the given server configuration.
func NewDBPool(cfg server.Config) *DBPool {
	return &DBPool{
		cfg:         cfg,
		duckLakeSem: make(chan struct{}, 1),
		sessions:    make(map[int32]*sql.DB),
		stopRefresh: make(map[int32]func()),
	}
}

// CreateSession creates a new DuckDB connection for a client session.
// The connection is registered by its backend PID for tracking.
func (p *DBPool) CreateSession(pid int32, username string) (*sql.DB, error) {
	db, err := server.CreateDBConnection(p.cfg, p.duckLakeSem, username)
	if err != nil {
		return nil, fmt.Errorf("create session db: %w", err)
	}

	stop := server.StartCredentialRefresh(db, p.cfg.DuckLake)

	p.mu.Lock()
	p.sessions[pid] = db
	p.stopRefresh[pid] = stop
	p.mu.Unlock()

	slog.Debug("Created session database.", "pid", pid, "user", username)
	return db, nil
}

// CloseSession closes and unregisters a session's database connection.
func (p *DBPool) CloseSession(pid int32) {
	p.mu.Lock()
	db, ok := p.sessions[pid]
	stop := p.stopRefresh[pid]
	if ok {
		delete(p.sessions, pid)
		delete(p.stopRefresh, pid)
	}
	p.mu.Unlock()

	if stop != nil {
		stop()
	}

	if ok && db != nil {
		if err := db.Close(); err != nil {
			slog.Warn("Failed to close session database.", "pid", pid, "error", err)
		}
	}
}

// ActiveSessions returns the number of active sessions.
func (p *DBPool) ActiveSessions() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}

// CloseAll closes all session databases. Used during shutdown.
func (p *DBPool) CloseAll(timeout time.Duration) {
	p.mu.Lock()
	sessions := make(map[int32]*sql.DB, len(p.sessions))
	stops := make(map[int32]func(), len(p.stopRefresh))
	for k, v := range p.sessions {
		sessions[k] = v
	}
	for k, v := range p.stopRefresh {
		stops[k] = v
	}
	p.sessions = make(map[int32]*sql.DB)
	p.stopRefresh = make(map[int32]func())
	p.mu.Unlock()

	for _, stop := range stops {
		if stop != nil {
			stop()
		}
	}

	for pid, db := range sessions {
		if err := db.Close(); err != nil {
			slog.Warn("Failed to close session database during shutdown.", "pid", pid, "error", err)
		}
	}
}
