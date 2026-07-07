package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/observe"
)

var ErrQueryLogRejected = errors.New("query log rejected")

const (
	queryLogSinkInitTimeout       = 30 * time.Second
	queryLogInitLockPollInterval  = 10 * time.Millisecond
	queryLogUnusedSinkStopTimeout = 5 * time.Second
)

var newAttachedQueryLogger = func(ctx context.Context, db *sql.DB, cfg server.QueryLogConfig) (server.QueryLogSink, error) {
	return server.NewAttachedQueryLoggerContext(ctx, db, cfg)
}

var newWorkerPostgresQueryLogger = func(ctx context.Context, cfg server.Config) (server.QueryLogSink, error) {
	return server.NewPostgresQueryLoggerContext(ctx, cfg.DuckLake, cfg.QueryLog)
}

func (p *SessionPool) LogQueryEntries(ctx context.Context, entries []server.QueryLogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if p.queryLogClosed() {
		observe.AddQueryLogDroppedEntries("worker_closing", len(entries))
		return ErrWorkerDraining
	}
	if err := p.validateQueryLogEntryIdentities(entries); err != nil {
		observe.AddQueryLogDroppedEntries("worker_rejected", len(entries))
		return err
	}
	sink, err := p.queryLogSinkForCurrentRuntime(ctx)
	if err != nil {
		observe.AddQueryLogDroppedEntries("worker_sink_init_error", len(entries))
		return err
	}
	if sink == nil {
		observe.AddQueryLogDroppedEntries("worker_sink_disabled", len(entries))
		return nil
	}
	for _, entry := range entries {
		sink.Log(entry)
	}
	return nil
}

func (p *SessionPool) queryLogSinkForCurrentRuntime(ctx context.Context) (server.QueryLogSink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.queryLogClosed() {
		return nil, ErrWorkerDraining
	}
	p.queryLogMu.Lock()
	if p.queryLogSink != nil {
		sink := p.queryLogSink
		p.queryLogMu.Unlock()
		return sink, nil
	}
	p.queryLogMu.Unlock()

	cfg, db, ok := p.queryLogRuntime()
	if !ok || !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil, nil
	}

	unlockInit, err := p.lockQueryLogInit(ctx)
	if err != nil {
		return nil, err
	}
	defer unlockInit()

	p.queryLogMu.Lock()
	if p.queryLogClosed() {
		p.queryLogMu.Unlock()
		return nil, ErrWorkerDraining
	}
	if p.queryLogSink != nil {
		sink := p.queryLogSink
		p.queryLogMu.Unlock()
		return sink, nil
	}
	p.queryLogMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, queryLogSinkInitTimeout)
	defer cancel()
	var ql server.QueryLogSink
	if p.sharedWarmMode {
		ql, err = newWorkerPostgresQueryLogger(ctx, cfg)
	} else {
		if db == nil {
			return nil, nil
		}
		ql, err = newAttachedQueryLogger(ctx, db, cfg.QueryLog)
	}
	if err != nil {
		return nil, fmt.Errorf("initialize worker query log: %w", err)
	}
	if ql == nil {
		return nil, nil
	}

	p.queryLogMu.Lock()
	if p.queryLogClosed() {
		p.queryLogMu.Unlock()
		stopUnusedQueryLogSink(ql)
		return nil, ErrWorkerDraining
	}
	if p.queryLogSink != nil {
		sink := p.queryLogSink
		p.queryLogMu.Unlock()
		stopUnusedQueryLogSink(ql)
		return sink, nil
	}
	p.queryLogSink = ql
	p.queryLogMu.Unlock()
	return ql, nil
}

func (p *SessionPool) lockQueryLogInit(ctx context.Context) (func(), error) {
	if p.queryLogInit.TryLock() {
		return p.queryLogInit.Unlock, nil
	}

	ticker := time.NewTicker(queryLogInitLockPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if p.queryLogInit.TryLock() {
				return p.queryLogInit.Unlock, nil
			}
		}
	}
}

func stopUnusedQueryLogSink(sink server.QueryLogSink) {
	if sink == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryLogUnusedSinkStopTimeout)
	defer cancel()
	if err := sink.StopContext(ctx); err != nil {
		slog.Warn("Failed to stop unused worker query log sink.", "error", err)
	}
}

func (p *SessionPool) validateQueryLogEntryIdentities(entries []server.QueryLogEntry) error {
	if !p.sharedWarmMode {
		return nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.activation == nil {
		return fmt.Errorf("%w: worker is not activated", ErrQueryLogRejected)
	}
	orgID := p.activation.payload.OrgID
	workerID := p.workerID
	if workerID == 0 {
		workerID = p.activation.payload.WorkerID
	}
	for _, entry := range entries {
		if entry.OrgID != orgID {
			return fmt.Errorf("%w: entry org_id %q does not match activated org %q", ErrQueryLogRejected, entry.OrgID, orgID)
		}
		if workerID > 0 && entry.WorkerID != workerID {
			return fmt.Errorf("%w: entry worker_id %d does not match active worker %d", ErrQueryLogRejected, entry.WorkerID, workerID)
		}
	}
	return nil
}

func (p *SessionPool) queryLogClosed() bool {
	if p == nil || p.stopCh == nil {
		return false
	}
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

func (p *SessionPool) queryLogRuntime() (server.Config, *sql.DB, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	cfg := p.cfg
	var db *sql.DB
	if p.sharedWarmMode {
		if p.activation == nil {
			return server.Config{}, nil, false
		}
		cfg.DuckLake = p.activation.payload.DuckLake
		overrideS3EndpointForCacheProxy(&cfg.DuckLake)
		if cfg.DuckLake.ApplicationName == "" && p.activation.payload.OrgID != "" {
			cfg.DuckLake.ApplicationName = "duckgres/" + p.activation.payload.OrgID
		}
		db = p.controlDB
		if db == nil {
			db = p.activation.db
		}
	} else {
		db = p.controlDB
	}
	if db == nil {
		db = p.warmupDB
	}
	if db == nil {
		db = p.fallbackDB
	}
	return cfg, db, true
}

func (p *SessionPool) stopQueryLogSink(ctx context.Context) {
	p.queryLogMu.Lock()
	sink := p.queryLogSink
	p.queryLogSink = nil
	p.queryLogMu.Unlock()
	if sink == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	if err := sink.StopContext(ctx); err != nil {
		slog.Warn("Failed to stop worker query log sink.", "error", err)
	}
}
