package duckdbservice

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/observe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var bootstrapBundledExtensions = server.BootstrapBundledExtensions
var exitProcess = os.Exit

var ErrWorkerDraining = errors.New("worker is draining")
var errSessionClosed = errors.New("session closed")

// DuckDBService is a standalone Arrow Flight SQL service backed by DuckDB.
type DuckDBService struct {
	cfg       ServiceConfig
	pool      *SessionPool
	flightSrv flight.Server
}

// SessionPool manages multiple DuckDB sessions keyed by session token.
type SessionPool struct {
	mu          sync.RWMutex
	sessions    map[string]*Session
	stopRefresh map[string]func()
	reserved    int // number of session slots reserved but not yet inserted
	duckLakeSem chan struct{}
	cfg         server.Config
	startTime   time.Time
	maxSessions int
	stopCh      chan struct{}
	stopOnce    sync.Once
	warmupDB    *sql.DB       // Keep this open to keep shared cache alive
	warmupDone  chan struct{} // Closed when Warmup() completes (success or failure)
	dbInitOnce  sync.Once     // Serializes fallback CreateDBConnection when warmup fails
	fallbackDB  *sql.DB       // Lazily created if warmup fails
	fallbackErr error         // Cached error from fallback creation
	// activePair is the DuckDBPair backing whichever of warmupDB/fallbackDB is
	// currently in use. It owns the underlying *duckdb.Connector and a
	// secondary *sql.DB (Control) sharing the same DuckDB instance —
	// see duckdbservice's credential-refresh path for why we need a side
	// connection that doesn't queue behind a long-running client query.
	activePair *DuckDBPair
	// controlDB is activePair.Control, surfaced for direct use by control-side
	// ops (CREATE OR REPLACE SECRET on STS rotation). Always nil before
	// activation; nil-check before use and fall back to the main DB.
	controlDB *sql.DB

	sharedWarmMode       bool
	activation           *activatedTenantRuntime
	ownerEpoch           int64
	ownerCPInstanceID    string
	workerID             int
	activateTenantFunc   func(ActivationPayload) error
	createDBPair         func(server.Config, chan struct{}, string, time.Time, string) (*DuckDBPair, error)
	activateDBConnection func(*sql.DB, server.Config, chan struct{}, string) error
	// refreshS3Secret is the indirection used for credential rotation on the
	// active tenant. Defaults to server.RefreshS3Secret in production; tests
	// inject a stub to verify the credential-refresh path is non-blocking
	// (see TestReuseExistingActivationDoesNotBlockHealthChecks).
	refreshS3Secret func(*sql.DB, server.DuckLakeConfig, chan struct{}) error
	// refreshIcebergSecret is the sibling indirection for rotating the
	// iceberg_sigv4 secret. Runs alongside refreshS3Secret on hot-idle
	// reuse whenever the tenant has iceberg enabled — without it, iceberg
	// queries on a long-lived worker would 403 after STS rotation while
	// DuckLake stays fresh. Defaults to server.RefreshIcebergSecret;
	// stubbed by tests the same way refreshS3Secret is.
	refreshIcebergSecret func(*sql.DB, server.IcebergConfig, chan struct{}, string, string, string) error

	drainMu       sync.Mutex
	draining      bool
	activeWork    int
	drainZero     chan struct{}
	drainZeroOpen bool
}

type trackedTx struct {
	tx          *sql.Tx
	finishDrain func()
	lastUsed    atomic.Int64 // unix nano
	activeWork  atomic.Int32
}

// Session represents a single DuckDB session.
type Session struct {
	ID        string
	DB        *sql.DB
	Conn      *sql.Conn // Dedicated connection for this session
	Username  string
	CreatedAt time.Time
	lastUsed  atomic.Int64 // unix nano

	mu            sync.RWMutex
	connMu        sync.Mutex // serializes operations on Conn and session-owned transactions
	queries       map[string]*QueryHandle
	txns          map[string]*trackedTx
	txnOwner      map[string]string
	closed        bool
	operationOpen bool
	operationIdle chan struct{}
	connWork      int
	connWorkDone  *sync.Cond
	handleCounter atomic.Uint64

	// sqlTxActive tracks whether a SQL-level transaction is in progress on this
	// session's Conn (i.e., BEGIN was sent as raw SQL without a corresponding
	// COMMIT/ROLLBACK). This is distinct from Flight SQL protocol-level
	// transactions tracked in txns. Used to prevent retryOnTransient from
	// retrying statements inside a user-managed transaction — a retry after a
	// transient error would run in autocommit mode (the transaction is dead)
	// and mask the failure from the client.
	sqlTxActive   atomic.Bool
	sqlTxDrain    func()
	sqlTxLastUsed atomic.Int64

	duckdbConn duckdbConnHandle // raw handle for progress polling (zero if extraction failed)
	progress   progressState    // stall detection state
}

// QueryHandle stores an ad-hoc query awaiting its DoGet.
type QueryHandle struct {
	Query     string
	Schema    *arrow.Schema
	TxnID     string
	createdAt time.Time // when registered; the reaper drops a stale ad-hoc handle whose DoGet never arrived (guarded by Session.mu)
	// finishOperation is the session operation token for an ad-hoc query
	// awaiting its DoGet.
	finishOperation func()
	// finishDrain is the drain token of an ad-hoc query awaiting its DoGet.
	finishDrain func()
}

func releaseDrainFunc(release func()) {
	if release != nil {
		release()
	}
}

func (s *Session) hasTrackedTxDrain(txnKey string) bool {
	if txnKey == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ttx := s.txns[txnKey]
	return ttx != nil && ttx.finishDrain != nil
}

func (s *Session) hasSQLTransactionDrain() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sqlTxDrain != nil
}

func (s *Session) allowsDrainContinuation(txnKey string) bool {
	if s.hasTrackedTxDrain(txnKey) {
		return true
	}
	return s.sqlTxActive.Load() && s.hasSQLTransactionDrain()
}

func (t *trackedTx) beginWork() func() {
	if t == nil {
		return func() {}
	}
	t.activeWork.Add(1)
	t.lastUsed.Store(time.Now().UnixNano())
	var once sync.Once
	return func() {
		once.Do(func() {
			t.lastUsed.Store(time.Now().UnixNano())
			t.activeWork.Add(-1)
		})
	}
}

func (s *Session) setSQLTransactionDrain(release func()) bool {
	if release == nil {
		return true
	}
	var old func()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	old = s.sqlTxDrain
	s.sqlTxDrain = release
	s.sqlTxLastUsed.Store(time.Now().UnixNano())
	s.mu.Unlock()
	releaseDrainFunc(old)
	return true
}

func (s *Session) releaseSQLTransactionDrain() {
	var release func()
	s.mu.Lock()
	release = s.sqlTxDrain
	s.sqlTxDrain = nil
	s.sqlTxLastUsed.Store(0)
	s.mu.Unlock()
	releaseDrainFunc(release)
}

func (s *Session) beginOperation() (func(), bool) {
	s.mu.Lock()
	if s.closed || s.operationOpen {
		s.mu.Unlock()
		return nil, false
	}
	s.operationOpen = true
	s.operationIdle = make(chan struct{})
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.operationOpen = false
			if s.operationIdle != nil {
				close(s.operationIdle)
				s.operationIdle = nil
			}
			s.mu.Unlock()
		})
	}, true
}

func (s *Session) beginOperationForTransaction(txnKey string) (func(), bool, bool) {
	s.mu.Lock()
	if _, exists := s.txns[txnKey]; !exists {
		s.mu.Unlock()
		return nil, false, false
	}
	if s.closed || s.operationOpen {
		s.mu.Unlock()
		return nil, true, false
	}
	s.operationOpen = true
	s.operationIdle = make(chan struct{})
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.operationOpen = false
			if s.operationIdle != nil {
				close(s.operationIdle)
				s.operationIdle = nil
			}
			s.mu.Unlock()
		})
	}, true, true
}

func (s *Session) waitOperationIdle(ctx context.Context) error {
	for {
		s.mu.RLock()
		if !s.operationOpen {
			s.mu.RUnlock()
			return nil
		}
		idle := s.operationIdle
		s.mu.RUnlock()

		if idle == nil {
			return errors.New("session operation idle signal missing")
		}
		select {
		case <-idle:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// beginConnWork fences any operation that uses the session connection while a
// raw SQL transaction may be open. It intentionally does not mutate queryActive:
// conn work includes COPY receive and metadata/planning work, while queryActive
// is reserved for query progress and stall reporting.
func (s *Session) beginConnWork() (func(), bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, false
	}
	s.connWork++
	if s.sqlTxActive.Load() {
		s.sqlTxLastUsed.Store(time.Now().UnixNano())
	}
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if s.connWork > 0 {
				s.connWork--
			}
			if s.connWork == 0 && s.connWorkDone != nil {
				s.connWorkDone.Broadcast()
			}
			if s.sqlTxActive.Load() {
				s.sqlTxLastUsed.Store(time.Now().UnixNano())
			}
			s.mu.Unlock()
		})
	}, true
}

func (s *Session) waitConnWorkDoneLocked() {
	if s.connWorkDone == nil {
		s.connWorkDone = sync.NewCond(&s.mu)
	}
	for s.connWork > 0 {
		s.connWorkDone.Wait()
	}
}

func (s *Session) exec(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	if tx != nil {
		s.connMu.Lock()
		defer s.connMu.Unlock()
		return tx.ExecContext(ctx, query, args...)
	}
	return s.execConn(ctx, query, args...)
}

func (s *Session) execConn(ctx context.Context, query string, args ...any) (sql.Result, error) {
	end, ok := s.beginConnWork()
	if !ok {
		return nil, errSessionClosed
	}
	defer end()
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.Conn == nil {
		return nil, errSessionClosed
	}
	return s.Conn.ExecContext(ctx, query, args...)
}

func (s *Session) rollbackConn(ctx context.Context) error {
	_, err := s.execConn(ctx, "ROLLBACK")
	return err
}

func (s *Session) beginTx(ctx context.Context) (*sql.Tx, error) {
	end, ok := s.beginConnWork()
	if !ok {
		return nil, errSessionClosed
	}
	defer end()
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.Conn == nil {
		return nil, errSessionClosed
	}
	return s.Conn.BeginTx(ctx, nil)
}

func (s *Session) queryRows(ctx context.Context, tx *sql.Tx, query string, args ...any) (*sql.Rows, func() error, error) {
	if tx != nil {
		s.connMu.Lock()
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			s.connMu.Unlock()
			return nil, nil, err
		}
		var once sync.Once
		closeRows := func() error {
			var closeErr error
			once.Do(func() {
				closeErr = rows.Close()
				s.connMu.Unlock()
			})
			return closeErr
		}
		return rows, closeRows, nil
	}
	return s.queryConnRows(ctx, query, args...)
}

func (s *Session) queryConnRows(ctx context.Context, query string, args ...any) (*sql.Rows, func() error, error) {
	end, ok := s.beginConnWork()
	if !ok {
		return nil, nil, errSessionClosed
	}
	s.connMu.Lock()
	if s.Conn == nil {
		s.connMu.Unlock()
		end()
		return nil, nil, errSessionClosed
	}
	rows, err := s.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		s.connMu.Unlock()
		end()
		return nil, nil, err
	}
	var once sync.Once
	closeRows := func() error {
		var closeErr error
		once.Do(func() {
			closeErr = rows.Close()
			s.connMu.Unlock()
			end()
		})
		return closeErr
	}
	return rows, closeRows, nil
}

func (s *Session) getQuerySchema(ctx context.Context, query string, tx *sql.Tx) (*arrow.Schema, error) {
	if tx != nil {
		s.connMu.Lock()
		defer s.connMu.Unlock()
		return GetQuerySchema(ctx, nil, query, tx)
	}
	end, ok := s.beginConnWork()
	if !ok {
		return nil, errSessionClosed
	}
	defer end()
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.Conn == nil {
		return nil, errSessionClosed
	}
	return GetQuerySchema(ctx, s.Conn, query, nil)
}

func (s *Session) commitTx(tx *sql.Tx) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return tx.Commit()
}

func (s *Session) rollbackTx(tx *sql.Tx) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return tx.Rollback()
}

// NewDuckDBService creates a new DuckDB service with the given config.
func NewDuckDBService(cfg ServiceConfig) *DuckDBService {
	pool := &SessionPool{
		sessions:             make(map[string]*Session),
		stopRefresh:          make(map[string]func()),
		duckLakeSem:          make(chan struct{}, 1),
		cfg:                  cfg.ServerConfig,
		startTime:            time.Now(),
		maxSessions:          cfg.MaxSessions,
		stopCh:               make(chan struct{}),
		warmupDone:           make(chan struct{}),
		sharedWarmMode:       cfg.RequireActivation || sharedWarmWorkerEnabled(),
		createDBPair:         CreateWorkerDBPair,
		activateDBConnection: server.ActivateDBConnection,
		refreshS3Secret:      server.RefreshS3Secret,
		refreshIcebergSecret: server.RefreshIcebergSecret,
		drainZero:            make(chan struct{}),
		drainZeroOpen:        true,
	}
	pool.activateTenantFunc = pool.activateTenant
	go pool.reapLoop()
	go pool.metadataMetricsLoop()

	return &DuckDBService{
		cfg:  cfg,
		pool: pool,
	}
}

// wipePersistedSecrets removes DuckDB's persistent-secret directories for this
// config. Called on worker startup so persisted secrets never survive a
// recycle. Best-effort: a failure is logged, not fatal — a stale secret is a
// correctness annoyance, not a reason to refuse to start. No-op when no DataDir
// is configured.
//
// We wipe two locations:
//   - the pinned secret_directory (server.SecretDirectory), where new secrets
//     would land under the current config, and
//   - the legacy DuckDB default <DataDir>/.duckdb/stored_secrets, which is where
//     secrets accumulated before we pinned the directory (a worker whose
//     HOME is its DataDir resolves DuckDB's default there). Without this, the
//     historical files that caused the original ambiguity would linger on a
//     persistent DataDir forever.
func wipePersistedSecrets(cfg server.Config) {
	if cfg.DataDir == "" {
		return
	}
	dirs := []string{
		server.SecretDirectory(cfg),
		server.LegacySecretDirectory(cfg),
	}
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("Failed to wipe persisted secret directory on worker startup.", "secret_directory", dir, "error", err)
			continue
		}
		slog.Info("Wiped persisted secret directory on worker startup.", "secret_directory", dir)
	}
}

// Warmup performs one-time initialization of the shared DuckDB instance.
// This loads extensions and attaches catalogs so that subsequent session
// creations are nearly instantaneous.
func (p *SessionPool) Warmup() error {
	defer close(p.warmupDone)

	if os.Getenv("DUCKGRES_MODE") != "duckdb-service" {
		return nil
	}

	// Recycle hook: a worker process starting up is the boundary between one
	// tenant runtime and the next on this disk (a serverless/shared-warm worker
	// is single-org-bound for its whole life, so process start == recycle).
	// Wipe any persisted secrets left on disk before DuckDB's SecretManager
	// reads them, so a CREATE PERSISTENT SECRET from a prior incarnation can't
	// resurface and collide with the in-memory secret re-created at activation,
	// and can't leak across tenants.
	//
	// This matters whenever DataDir survives across worker processes: a
	// container restart within the same pod (EmptyDir survives that — it's only
	// destroyed on pod deletion), or any persistent-volume / warm-node setup. On
	// a fresh pod with a fresh EmptyDir the directory is already empty and this
	// is a no-op.
	wipePersistedSecrets(p.cfg)

	start := time.Now()
	slog.Info("Pre-warming worker DuckDB instance...")

	// Wait for the local cache proxy to be ready before serving queries.
	// When DUCKGRES_CACHE_PROXY_ADDR is set, DuckDB will route S3 traffic
	// through it — worker startup must block until the proxy is healthy.
	// Included in the pre-warm duration so slow proxy startup is visible.
	waitForCacheProxy()
	// Use a system-level username for warmup
	pair, err := p.createDBPair(p.sharedWarmupConfig(), p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
	if err != nil {
		return fmt.Errorf("warmup failed after %v: %w", time.Since(start), err)
	}

	p.mu.Lock()
	p.warmupDB = pair.Main
	p.activePair = pair
	p.controlDB = pair.Control
	p.mu.Unlock()

	slog.Info("Worker pre-warmed successfully.", "duration", time.Since(start))
	return nil
}

const (
	txnIdleTimeout = 10 * time.Minute
	// handleIdleTimeout bounds how long a drain token may sit stranded on a query
	// handle / metadata stream whose matching DoGet never arrived (client
	// cancelled between GetFlightInfo and DoGet, session kept). Without reaping,
	// such a token holds the worker's drain open until the full
	// workerShutdownDrainTime, then a forced shutdown kills genuinely in-flight
	// work. The real GetFlightInfo→DoGet gap is sub-second, so this is generous.
	handleIdleTimeout       = 10 * time.Minute
	reapInterval            = 1 * time.Minute
	metadataMetricsInterval = 30 * time.Second
	workerShutdownDrainTime = 55 * time.Minute
)

func (p *SessionPool) reapLoop() {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.reapIdle(time.Now())
		case <-p.stopCh:
			return
		}
	}
}

// reapIdle rolls back idle transactions and releases drain tokens stranded by a
// GetFlightInfo whose matching DoGet never arrived. Both are bounded so a single
// abandoned client RPC cannot hold the worker's drain open until the shutdown
// timeout (and then force-kill genuinely in-flight work).
func (p *SessionPool) reapIdle(now time.Time) {
	p.mu.RLock()
	sessions := make([]*Session, 0, len(p.sessions))
	for _, s := range p.sessions {
		sessions = append(sessions, s)
	}
	p.mu.RUnlock()

	for _, s := range sessions {
		var releaseDrains []func()
		s.mu.Lock()
		for id, ttx := range s.txns {
			last := time.Unix(0, ttx.lastUsed.Load())
			if now.Sub(last) > txnIdleTimeout {
				if s.operationOpen {
					continue
				}
				if ttx.activeWork.Load() > 0 {
					continue
				}
				if !s.connMu.TryLock() {
					continue
				}
				slog.Warn("Rolling back idle transaction.", "user", s.Username, "txn", id)
				if ttx.tx != nil {
					_ = ttx.tx.Rollback()
				}
				s.connMu.Unlock()
				if ttx.finishDrain != nil {
					releaseDrains = append(releaseDrains, ttx.finishDrain)
					ttx.finishDrain = nil
				}
				delete(s.txns, id)
				delete(s.txnOwner, id)
			}
		}
		if s.sqlTxActive.Load() && s.sqlTxDrain != nil {
			lastNanos := s.sqlTxLastUsed.Load()
			if lastNanos == 0 {
				lastNanos = s.lastUsed.Load()
			}
			last := time.Unix(0, lastNanos)
			if now.Sub(last) > txnIdleTimeout {
				if s.operationOpen {
					// A same-session operation is still logically in progress or
					// awaiting its continuation. Leave the transaction drain active.
				} else if s.connWork > 0 {
					// The connection may be executing, streaming, or planning work inside
					// this raw SQL transaction. Leave the transaction drain token active.
				} else if s.Conn == nil {
					slog.Warn("Skipping idle raw SQL transaction rollback without a session connection.", "user", s.Username)
				} else if !s.connMu.TryLock() {
					// A same-session operation is using the connection but has not reached
					// a connWork-tracked path. Skip instead of blocking the reaper loop.
				} else {
					slog.Warn("Rolling back idle raw SQL transaction.", "user", s.Username)
					rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), time.Second)
					if _, err := s.Conn.ExecContext(rollbackCtx, "ROLLBACK"); err != nil {
						slog.Warn("Idle raw SQL transaction rollback failed; keeping drain work active.", "user", s.Username, "error", err)
					} else {
						releaseDrains = append(releaseDrains, s.sqlTxDrain)
						s.sqlTxDrain = nil
						s.sqlTxActive.Store(false)
						s.sqlTxLastUsed.Store(0)
					}
					rollbackCancel()
					s.connMu.Unlock()
				}
			}
		}
		// Drain tokens stranded on ad-hoc query handles. An actively-streaming
		// DoGet has already popped its handle out of this map, so anything still
		// here past the TTL was abandoned mid-handshake.
		for id, h := range s.queries {
			if now.Sub(h.createdAt) > handleIdleTimeout {
				if h.finishDrain != nil {
					releaseDrains = append(releaseDrains, h.finishDrain)
					h.finishDrain = nil
				}
				if h.finishOperation != nil {
					releaseDrains = append(releaseDrains, h.finishOperation)
					h.finishOperation = nil
				}
				slog.Warn("Reaping abandoned query handle (no DoGet).", "user", s.Username, "handle", id)
				delete(s.queries, id)
			}
		}
		s.mu.Unlock()
		for _, release := range releaseDrains {
			releaseDrainFunc(release)
		}
	}
}

// metadataMetricsLoop periodically scrapes observability signals about the
// DuckLake metadata Postgres from the worker's DuckDB instance and emits them
// as Prometheus gauges. Scrapes only run once the worker has been activated
// (so an org is known and metadata is attached). Ticks are best-effort: any
// query failure is logged once and the next tick retries — we never want this
// loop to hold any locks or block user queries.
func (p *SessionPool) metadataMetricsLoop() {
	ticker := time.NewTicker(metadataMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.scrapeMetadataMetrics()
		case <-p.stopCh:
			return
		}
	}
}

func (p *SessionPool) scrapeMetadataMetrics() {
	act := p.currentActivation()
	if act == nil || act.db == nil {
		return
	}
	orgID := act.payload.OrgID

	// Prefer controlDB (side connection sharing the same DuckDB instance with
	// its own pool) so a long-running user query on the main DB does not
	// queue our scrape. Falls back to the activation DB only when controlDB
	// has not been wired (test paths).
	p.mu.RLock()
	scrapeDB := p.controlDB
	p.mu.RUnlock()
	if scrapeDB == nil {
		scrapeDB = act.db
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if v, ok := scrapeMetadataPoolMax(ctx, scrapeDB); ok {
		observe.MetadataPoolMaxConnections.WithLabelValues(orgID).Set(v)
	}
}

// scrapeMetadataPoolMax reads DuckDB's pg_pool_max_connections setting via
// duckdb_settings(). A value of 0 means the in-process pool is disabled (we
// set this when ViaPgBouncer is true) — still useful to emit as a gauge.
//
// Aurora-side load (active/idle connections, slow queries) is observed via
// pg_stat_activity / Performance Insights, attributed back to duckgres by
// the application_name we tag at ATTACH time (ducklake.Config.ApplicationName).
// We deliberately don't try to scrape pg_stat_activity from the worker via
// postgres_query — an earlier attempt silently no-op'd in our DuckDB build
// and Performance Insights is the better tool for that view anyway.
func scrapeMetadataPoolMax(ctx context.Context, db *sql.DB) (float64, bool) {
	var raw string
	err := db.QueryRowContext(ctx,
		"SELECT value FROM duckdb_settings() WHERE name = 'pg_pool_max_connections'").Scan(&raw)
	if err != nil {
		// Setting may be unavailable until postgres_scanner is loaded; skip
		// silently rather than spamming logs every 30s.
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// Run starts the DuckDB service, blocking until shutdown.
func Run(cfg ServiceConfig) {
	svc := NewDuckDBService(cfg)

	if err := bootstrapBundledExtensions(cfg.ServerConfig.DataDir); err != nil {
		slog.Error("Failed to bootstrap bundled DuckDB extensions.",
			"source", "/app/extensions",
			"extension_directory", cfg.ServerConfig.DataDir+"/extensions",
			"error", err)
		exitProcess(1)
	}

	// Pre-warm the DuckDB instance (load extensions, attach DuckLake)
	// in the background so we don't block the gRPC server from starting.
	// This ensures that waitForWorker doesn't time out during spawn.
	go func() {
		if err := svc.pool.Warmup(); err != nil {
			slog.Warn("Worker pre-warm failed. Sessions may be slow to initialize.", "error", err)
		}
	}()

	var listener net.Listener

	if cfg.ListenFD > 0 {
		// Inherited pre-bound listener from the control plane.
		// The CP creates and binds the Unix socket, then passes the FD via
		// cmd.ExtraFiles to avoid EROFS under systemd ProtectSystem=strict.
		file := os.NewFile(uintptr(cfg.ListenFD), "inherited-listener")
		if file == nil {
			slog.Error("Invalid inherited listener FD", "fd", cfg.ListenFD)
			os.Exit(1)
		}
		var err error
		listener, err = net.FileListener(file)
		_ = file.Close()
		if err != nil {
			slog.Error("Failed to create listener from inherited FD", "fd", cfg.ListenFD, "error", err)
			os.Exit(1)
		}
		slog.Info("Starting DuckDB service (inherited listener)", "addr", listener.Addr().String())
	} else {
		network, addr, err := ParseListenAddr(cfg.ListenAddr)
		if err != nil {
			slog.Error("Invalid listen address", "error", err)
			os.Exit(1)
		}

		// Clean up stale unix socket
		if network == "unix" {
			_ = os.Remove(addr)
		}

		listener, err = net.Listen(network, addr)
		if err != nil {
			slog.Error("Failed to listen", "network", network, "addr", addr, "error", err)
			os.Exit(1)
		}

		// Restrict unix socket permissions to owner only
		if network == "unix" {
			if err := os.Chmod(addr, 0700); err != nil {
				slog.Warn("Failed to set unix socket permissions", "error", err)
			}
		}

		slog.Info("Starting DuckDB service", "network", network, "addr", addr)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Draining DuckDB service before shutdown...")
		svc.BeginDrain()
		ctx, cancel := context.WithTimeout(context.Background(), workerShutdownDrainTime)
		if !svc.WaitForDrain(ctx) {
			slog.Warn("DuckDB service drain timed out before shutdown.", "timeout", workerShutdownDrainTime)
			cancel()
			svc.CloseAll()
			os.Exit(0)
		}
		cancel()
		slog.Info("Shutting down DuckDB service...")
		svc.Shutdown()
		os.Exit(0)
	}()

	if err := svc.Serve(listener); err != nil {
		slog.Error("DuckDB service error", "error", err)
		os.Exit(1)
	}
}

// Serve starts serving on the given listener.
func (svc *DuckDBService) Serve(listener net.Listener) error {
	handler := NewFlightSQLHandler(svc.pool)

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.MaxRecvMsgSize(flightclient.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(flightclient.MaxGRPCMessageSize),
	)
	if svc.cfg.BearerToken != "" {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(BearerTokenUnaryInterceptor(svc.cfg.BearerToken)),
			grpc.ChainStreamInterceptor(BearerTokenStreamInterceptor(svc.cfg.BearerToken)),
		)
	}
	if listener.Addr() != nil && listener.Addr().Network() == "tcp" &&
		svc.cfg.ServerConfig.TLSCertFile != "" && svc.cfg.ServerConfig.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(svc.cfg.ServerConfig.TLSCertFile, svc.cfg.ServerConfig.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("load worker RPC TLS certificates: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})))
	}

	// Wrap the flightsql server with custom action handling.
	// flightsql.NewFlightServer routes standard Flight SQL actions but rejects
	// custom action types. Our wrapper intercepts custom actions (CreateSession,
	// DestroySession, HealthCheck) before falling through to the standard router.
	flightSqlSrv := flightsql.NewFlightServer(handler)
	customSrv := &customActionServer{FlightServer: flightSqlSrv, handler: handler}

	svc.flightSrv = flight.NewServerWithMiddleware(nil, opts...)
	svc.flightSrv.RegisterFlightService(customSrv)
	svc.flightSrv.InitListener(listener)
	return svc.flightSrv.Serve()
}

// Shutdown gracefully stops the service.
func (svc *DuckDBService) Shutdown() {
	if svc.flightSrv != nil {
		svc.flightSrv.Shutdown()
	}
	svc.CloseAll()
}

func (svc *DuckDBService) CloseAll() {
	if svc.pool != nil {
		svc.pool.CloseAll()
	}
}

func (svc *DuckDBService) BeginDrain() {
	if svc.pool != nil {
		svc.pool.BeginDrain()
	}
}

func (svc *DuckDBService) WaitForDrain(ctx context.Context) bool {
	if svc.pool == nil {
		return true
	}
	return svc.pool.WaitForDrain(ctx)
}

// CreateSession creates a new DuckDB session for the given username.
func (p *SessionPool) CreateSession(username, memoryLimit string, threads int) (*Session, error) {
	start := time.Now()
	finishDrain, drainErr := p.beginDrainWork(false)
	if drainErr != nil {
		return nil, drainErr
	}
	defer finishDrain()

	// Reserve a slot under the lock to prevent TOCTOU race on maxSessions.
	p.mu.Lock()
	if p.maxSessions > 0 && len(p.sessions)+p.reserved >= p.maxSessions {
		p.mu.Unlock()
		return nil, fmt.Errorf("max sessions reached (%d)", p.maxSessions)
	}
	p.reserved++
	p.mu.Unlock()

	var (
		db   *sql.DB
		conn *sql.Conn
		err  error
	)

	// Wait for warmup to complete before checking warmupDB.
	// Without this, a CreateSession arriving while warmup is in progress would
	// see warmupDB==nil and call CreateDBConnection concurrently with warmup,
	// causing two goroutines to LOAD native extensions simultaneously in the
	// same process — which corrupts the heap (malloc: unaligned tcache chunk).
	waitStart := time.Now()
	<-p.warmupDone
	if waitDur := time.Since(waitStart); waitDur > 100*time.Millisecond {
		slog.Info("Waited for warmup to complete.", "duration", waitDur)
	}

	// Use shared warmupDB if available (highly preferred for performance)
	db = p.activeSharedDB()
	if db == nil {
		p.mu.RLock()
		db = p.warmupDB
		p.mu.RUnlock()
	}

	if db == nil {
		cfg, cfgErr := p.currentSessionConfig()
		if cfgErr != nil {
			p.mu.Lock()
			p.reserved--
			p.mu.Unlock()
			return nil, cfgErr
		}

		// Fallback: create a shared DB if warmup failed or wasn't run.
		// Uses sync.Once to ensure only one goroutine loads native extensions;
		// concurrent LOAD of the same C++ extension corrupts the heap.
		fallbackStart := time.Now()
		p.dbInitOnce.Do(func() {
			pair, err := p.createDBPair(cfg, p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
			if err != nil {
				p.fallbackErr = err
				return
			}
			p.fallbackDB = pair.Main
			p.activePair = pair
			p.controlDB = pair.Control
		})
		if p.fallbackErr != nil {
			p.mu.Lock()
			p.reserved--
			p.mu.Unlock()
			return nil, fmt.Errorf("create session db: %w", p.fallbackErr)
		}
		db = p.fallbackDB
		slog.Info("Using fallback DB (warmup failed).", "duration", time.Since(fallbackStart))
	}

	// Obtain a dedicated connection for this session to ensure isolation
	// and consistent session settings (search_path, etc.)
	// Use a timeout to prevent indefinite blocking when a previous session's
	// cleanup hasn't returned its connection to the pool yet.
	slog.Debug("Acquiring DB connection from pool.", "user", username)
	connCtx, connCancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err = db.Conn(connCtx)
	connCancel()
	if err != nil {
		p.mu.Lock()
		p.reserved--
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to obtain connection from pool (timeout after 30s): %w", err)
	}
	slog.Debug("Acquired DB connection from pool.", "user", username, "duration", time.Since(start))

	// Initialize the session connection with username-specific state if needed.
	// Since the DB is shared, we must set session-local parameters here.
	initSearchPath(conn, username)

	// Re-apply DuckDB profiling settings on the freshly-pooled connection.
	// ConfigureMainDB sets these once at warmup, but in cluster mode the
	// per-session evictConnFromPool call discards the settings along with
	// the conn — so for every session after the first we need to re-apply.
	// DuckDB rejects `SET GLOBAL` for these keys, so this is the only path.
	server.ApplyProfilingSettings(context.Background(), conn)

	// Apply initial resource limits if provided (optimizes handshake by avoiding
	// roundtrips from control plane).
	if memoryLimit != "" {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET memory_limit = '%s'", memoryLimit)); err != nil {
			slog.Warn("Failed to set initial memory_limit.", "user", username, "error", err)
		}
	}
	if threads > 0 {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET threads = %d", threads)); err != nil {
			slog.Warn("Failed to set initial threads.", "user", username, "error", err)
		}
	}

	// Extract the raw DuckDB connection handle for progress polling.
	// Non-fatal if extraction fails — progress monitoring will be unavailable
	// for this session but everything else works normally.
	var duckConn duckdbConnHandle
	if dc, err := extractDuckDBConnection(conn); err != nil {
		slog.Debug("Could not extract DuckDB connection for progress polling.", "user", username, "error", err)
	} else {
		duckConn = dc
	}

	token := generateSessionToken()
	session := &Session{
		ID:         token,
		DB:         db,
		Conn:       conn,
		Username:   username,
		CreatedAt:  time.Now(),
		queries:    make(map[string]*QueryHandle),
		txns:       make(map[string]*trackedTx),
		txnOwner:   make(map[string]string),
		duckdbConn: duckConn,
	}
	session.lastUsed.Store(time.Now().UnixNano())

	cfg, cfgErr := p.currentSessionConfig()
	if cfgErr != nil {
		_ = conn.Close()
		p.mu.Lock()
		p.reserved--
		p.mu.Unlock()
		return nil, cfgErr
	}

	// In shared-warm (multi-tenant) mode the control plane drives credential
	// refresh by re-activating the worker with a freshly-brokered STS payload
	// before the current creds expire (see controlplane/janitor scheduler +
	// activator.RefreshActivationCredentials). Running the in-process ticker
	// here is wrong for that mode for two reasons:
	//   1. It executes on this session's pinned *sql.Conn, so it serializes
	//      behind any in-flight user query. A long query can starve the
	//      ticker until creds have already expired.
	//   2. For provider="config" with a session token (the multi-tenant
	//      activation shape) the ticker falls into the `else` branch and
	//      replaces the org's STS-brokered secret with a credential_chain
	//      one. DuckDB's built-in chain doesn't see EKS Pod Identity, so
	//      the resulting secret can't actually authenticate against the
	//      org's bucket — quietly downgrading correctness.
	// The single-tenant standalone path still benefits from the ticker (no
	// control plane to drive refreshes), so we only skip it in shared-warm.
	var stop func()
	if p.sharedWarmMode {
		stop = func() {}
	} else {
		stop = server.StartCredentialRefresh(conn, cfg.DuckLake, func() bool {
			session.mu.Lock()
			defer session.mu.Unlock()
			return len(session.txns) > 0
		})
	}

	p.mu.Lock()
	p.reserved--
	p.sessions[token] = session
	p.stopRefresh[token] = stop
	p.mu.Unlock()

	slog.Debug("Created DuckDB session", "user", username, "duration", time.Since(start))
	return session, nil
}

// GetSession returns a session by token.
func (p *SessionPool) GetSession(token string) (*Session, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.sessions[token]
	return s, ok
}

// DestroySession closes and removes a session.
func (p *SessionPool) DestroySession(token string) error {
	p.mu.Lock()
	session, ok := p.sessions[token]
	stop := p.stopRefresh[token]
	if ok {
		delete(p.sessions, token)
		delete(p.stopRefresh, token)
	}
	p.mu.Unlock()

	if !ok {
		return fmt.Errorf("session not found")
	}

	var releaseDrains []func()
	var rollbackTxs []*sql.Tx

	// Roll back any open transactions
	session.mu.Lock()
	session.closed = true
	for id, handle := range session.queries {
		if handle.finishDrain != nil {
			releaseDrains = append(releaseDrains, handle.finishDrain)
			handle.finishDrain = nil
		}
		if handle.finishOperation != nil {
			releaseDrains = append(releaseDrains, handle.finishOperation)
			handle.finishOperation = nil
		}
		delete(session.queries, id)
	}
	if session.sqlTxDrain != nil {
		releaseDrains = append(releaseDrains, session.sqlTxDrain)
		session.sqlTxDrain = nil
		session.sqlTxActive.Store(false)
		session.sqlTxLastUsed.Store(0)
	}
	for id, ttx := range session.txns {
		if ttx.tx != nil {
			rollbackTxs = append(rollbackTxs, ttx.tx)
		}
		if ttx.finishDrain != nil {
			releaseDrains = append(releaseDrains, ttx.finishDrain)
			ttx.finishDrain = nil
		}
		delete(session.txns, id)
		delete(session.txnOwner, id)
	}
	session.waitConnWorkDoneLocked()
	session.mu.Unlock()
	for _, tx := range rollbackTxs {
		_ = session.rollbackTx(tx)
	}
	for _, release := range releaseDrains {
		releaseDrainFunc(release)
	}

	if stop != nil {
		stop()
	}
	if session.Conn != nil {
		session.connMu.Lock()
		// sql.Conn.Close() returns the underlying driver connection to sql.DB's
		// pool rather than closing it. DuckDB temp tables, temp views, and
		// temp macros are connection-scoped, so without intervention they'd
		// leak into the next session that gets the same pooled connection.
		//
		// Two strategies:
		//   - Cluster mode (sharedWarmMode): always evict the conn. The
		//     worker is bound to a single org via activateTenant, so security
		//     boundaries align with worker lifecycle, and the per-session
		//     cleanup loop is mostly wasted work (a typical SELECT-1 session
		//     creates zero user temp objects but the cleanup still issues
		//     ~46 DROP IF EXISTS no-ops against system views in non-temp
		//     schemas). Eviction is also more correct: the existing cleanup
		//     only handles temp tables/views, not temp macros, types, or
		//     sequences — those still leak through pooled-conn reuse.
		//   - Standalone mode: pooled conns can be reused across orgs, so
		//     scrubbing per-conn state is required. Falls back to eviction
		//     if the cleanup fails.
		cleanupStart := time.Now()
		var evicted bool
		if p.sharedWarmMode {
			slog.Debug("Evicting session connection from pool (cluster mode).", "user", session.Username)
			evictConnFromPool(session.Conn)
			evicted = true
		} else {
			slog.Debug("Cleaning up session state (standalone mode).", "user", session.Username)
			clean := cleanupSessionState(session.Conn)
			slog.Debug("Session state cleaned up.", "user", session.Username, "duration", time.Since(cleanupStart), "clean", clean)
			if !clean {
				// Cleanup failed — likely an aborted/INTERRUPT'd conn from a
				// cancelled query. Evict rather than poison the next session.
				evictConnFromPool(session.Conn)
				evicted = true
			}
		}
		connCloseStart := time.Now()
		_ = session.Conn.Close()
		slog.Debug("Session connection closed.", "user", session.Username,
			"cleanup_duration", time.Since(cleanupStart),
			"close_duration", time.Since(connCloseStart),
			"evicted", evicted)
		session.connMu.Unlock()
	}
	// Do NOT close session.DB if it is a shared DB (warmup or fallback)
	p.mu.RLock()
	isShared := session.DB == p.warmupDB || session.DB == p.fallbackDB
	p.mu.RUnlock()

	if session.DB != nil && !isShared {
		if err := session.DB.Close(); err != nil {
			slog.Warn("Failed to close session database", "error", err)
		}
	}

	slog.Debug("Destroyed DuckDB session", "user", session.Username)
	return nil
}

// ActiveSessions returns the number of active sessions.
func (p *SessionPool) ActiveSessions() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.sessions)
}

func (p *SessionPool) BeginDrain() {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.ensureDrainChannelLocked()
	p.draining = true
	if p.activeWork == 0 && p.drainZeroOpen {
		close(p.drainZero)
		p.drainZeroOpen = false
	}
}

func (p *SessionPool) IsDraining() bool {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	return p.draining
}

func (p *SessionPool) ActiveDrainWork() int {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	return p.activeWork
}

func (p *SessionPool) WaitForDrain(ctx context.Context) bool {
	p.drainMu.Lock()
	p.ensureDrainChannelLocked()
	if p.activeWork == 0 {
		p.drainMu.Unlock()
		return true
	}
	ch := p.drainZero
	p.drainMu.Unlock()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *SessionPool) beginDrainWork(allowWhenDraining bool) (func(), error) {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.ensureDrainChannelLocked()
	if p.draining && !allowWhenDraining {
		return nil, ErrWorkerDraining
	}
	if p.draining && !p.drainZeroOpen {
		return nil, ErrWorkerDraining
	}
	// Invariant: drainZeroOpen only goes false while draining (BeginDrain / the
	// release closure), and draining never clears, so reaching here means the
	// channel is open — !draining implies drainZeroOpen, and draining implies it
	// per the guard above. No reopen needed.
	p.activeWork++

	var once sync.Once
	return func() {
		once.Do(func() {
			p.drainMu.Lock()
			defer p.drainMu.Unlock()
			if p.activeWork > 0 {
				p.activeWork--
			}
			if p.draining && p.activeWork == 0 && p.drainZeroOpen {
				close(p.drainZero)
				p.drainZeroOpen = false
			}
		})
	}, nil
}

func (p *SessionPool) ensureDrainChannelLocked() {
	if p.drainZero == nil {
		p.drainZero = make(chan struct{})
		p.drainZeroOpen = true
	}
}

// CloseAll closes all sessions.
func (p *SessionPool) CloseAll() {
	p.stopOnce.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
		}
	})
	p.mu.Lock()
	sessions := make(map[string]*Session, len(p.sessions))
	stops := make(map[string]func(), len(p.stopRefresh))
	for k, v := range p.sessions {
		sessions[k] = v
	}
	for k, v := range p.stopRefresh {
		stops[k] = v
	}
	p.sessions = make(map[string]*Session)
	p.stopRefresh = make(map[string]func())
	p.mu.Unlock()

	for _, stop := range stops {
		if stop != nil {
			stop()
		}
	}

	for _, session := range sessions {
		var releaseDrains []func()
		var rollbackTxs []*sql.Tx
		session.mu.Lock()
		session.closed = true
		for id, handle := range session.queries {
			if handle.finishDrain != nil {
				releaseDrains = append(releaseDrains, handle.finishDrain)
				handle.finishDrain = nil
			}
			if handle.finishOperation != nil {
				releaseDrains = append(releaseDrains, handle.finishOperation)
				handle.finishOperation = nil
			}
			delete(session.queries, id)
		}
		if session.sqlTxDrain != nil {
			releaseDrains = append(releaseDrains, session.sqlTxDrain)
			session.sqlTxDrain = nil
			session.sqlTxActive.Store(false)
			session.sqlTxLastUsed.Store(0)
		}
		for id, ttx := range session.txns {
			if ttx.tx != nil {
				rollbackTxs = append(rollbackTxs, ttx.tx)
			}
			if ttx.finishDrain != nil {
				releaseDrains = append(releaseDrains, ttx.finishDrain)
				ttx.finishDrain = nil
			}
			delete(session.txns, id)
			delete(session.txnOwner, id)
		}
		session.waitConnWorkDoneLocked()
		session.mu.Unlock()
		for _, tx := range rollbackTxs {
			_ = session.rollbackTx(tx)
		}
		for _, release := range releaseDrains {
			releaseDrainFunc(release)
		}

		if session.Conn != nil {
			session.connMu.Lock()
			_ = session.Conn.Close()
			session.connMu.Unlock()
		}
		if session.DB != nil && session.DB != p.warmupDB && session.DB != p.fallbackDB {
			_ = session.DB.Close()
		}
	}

	if p.warmupDB != nil {
		cleanupWorkerCatalogs(p.warmupDB)
	}
	if p.fallbackDB != nil && p.fallbackDB != p.warmupDB {
		cleanupWorkerCatalogs(p.fallbackDB)
	}
	if p.activation != nil && p.activation.db != nil && p.activation.db != p.warmupDB && p.activation.db != p.fallbackDB {
		cleanupWorkerCatalogs(p.activation.db)
		_ = p.activation.db.Close()
	}
	// activePair owns the underlying *duckdb.Connector and the Control side
	// connection; closing it tears the DuckDB instance down once. Both
	// warmupDB and fallbackDB are p.activePair.Main for in-pool DBs, so this
	// supersedes the old per-DB close calls.
	if p.activePair != nil {
		_ = p.activePair.Close()
		p.activePair = nil
		p.warmupDB = nil
		p.fallbackDB = nil
		p.controlDB = nil
	} else {
		// Fallback path for tests that bypass the pair factory and assigned
		// warmupDB/fallbackDB directly without an activePair.
		if p.warmupDB != nil {
			_ = p.warmupDB.Close()
		}
		if p.fallbackDB != nil && p.fallbackDB != p.warmupDB {
			_ = p.fallbackDB.Close()
		}
	}
}

// cleanupWorkerCatalogs detaches the lake catalogs that are actually attached
// on db before shutdown. Probing duckdb_databases() avoids spurious "failed to
// detach" warnings on the warmup/fallback DBs that never had a tenant catalog
// attached, while still cleaning up the activation DB that does.
func cleanupWorkerCatalogs(db *sql.DB) {
	if db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT database_name FROM duckdb_databases() WHERE database_name IN ('delta', 'ducklake')")
	if err != nil {
		slog.Debug("Failed to list attached catalogs during shutdown.", "error", err)
		return
	}
	attached := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			slog.Debug("Failed to scan attached catalog name during shutdown.", "error", err)
			return
		}
		attached[name] = true
	}
	_ = rows.Close()

	if !attached["delta"] && !attached["ducklake"] {
		return
	}
	if _, err := db.ExecContext(ctx, "USE memory"); err != nil {
		slog.Warn("Failed to switch worker DB to memory during shutdown.", "error", err)
		return
	}
	if attached["delta"] {
		if _, err := db.ExecContext(ctx, "DETACH delta"); err != nil {
			slog.Warn("Failed to detach worker Delta catalog during shutdown.", "error", err)
		}
	}
	if attached["ducklake"] {
		if _, err := db.ExecContext(ctx, "DETACH ducklake"); err != nil {
			slog.Warn("Failed to detach worker DuckLake catalog during shutdown.", "error", err)
		}
	}
}

// evictConnFromPool removes a *sql.Conn's underlying driver connection from
// the *sql.DB pool so it's not reused by a future caller.
//
// The standard library exposes no direct API for this, so the idiom is to
// return driver.ErrBadConn from Conn.Raw — the database/sql pool checks for
// that error specifically and discards rather than re-pools the conn (see
// putConn in database/sql). The connection isn't actually broken; we just
// don't want it pooled. The Go team has acknowledged the API gap (see
// golang/go#40722) but no cleaner replacement exists today.
func evictConnFromPool(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
}

// cleanupSessionState drops temporary tables and views on the connection so
// that session-scoped state doesn't leak when the connection is returned to
// the pool. Returns true if every step succeeded and the connection is safe
// to reuse; false if any operation failed (typically because the prior query
// was cancelled and the conn is in an aborted/INTERRUPT'd state).
func cleanupSessionState(conn *sql.Conn) bool {
	// Clear any aborted-transaction state from the prior query before running
	// the cleanup operations. After a cancelled query, DuckDB can leave the
	// session in a state where every subsequent statement returns "INTERRUPT
	// Error: Interrupted!" until ROLLBACK runs. Same pattern as initSearchPath.
	rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 1*time.Second)
	if _, err := conn.ExecContext(rollbackCtx, "ROLLBACK"); err != nil {
		// ROLLBACK with no active txn returns a no-op error in some drivers; not fatal.
		slog.Debug("ROLLBACK during cleanup returned an error.", "error", err)
	}
	rollbackCancel()

	ok := dropTemporary(conn,
		"SELECT table_name FROM duckdb_tables() WHERE temporary = true",
		`DROP TABLE IF EXISTS temp."%s"`,
	)
	// Run views even if tables failed — gives best-effort progress and a
	// consistent ok=false signal if anything didn't complete.
	ok = dropTemporary(conn,
		"SELECT view_name FROM duckdb_views() WHERE temporary = true",
		`DROP VIEW IF EXISTS temp."%s"`,
	) && ok
	return ok
}

func dropTemporary(conn *sql.Conn, query, dropFmt string) bool {
	// Per-step contexts: previously a single 5s context spanned both the
	// SELECT and every DROP. A slow SELECT could exhaust the budget before
	// a single DROP ran, causing all DROPs to fail with deadline-exceeded
	// in lockstep. Now SELECT has its own 3s and each DROP its own 1s, so
	// the failure of one operation doesn't cascade into the next.
	enumCtx, enumCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer enumCancel()
	rows, err := conn.QueryContext(enumCtx, query)
	if err != nil {
		slog.Warn("Failed to query temporary objects for cleanup.", "error", err)
		return false
	}
	var names []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		slog.Warn("Error iterating temporary objects for cleanup.", "error", err)
	}
	_ = rows.Close()

	ok := true
	for _, name := range names {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 1*time.Second)
		if _, err := conn.ExecContext(dropCtx, fmt.Sprintf(dropFmt, name)); err != nil {
			slog.Warn("Failed to drop temporary object during cleanup.", "name", name, "error", err)
			ok = false
		}
		dropCancel()
	}
	return ok
}

// initSearchPath sets the DuckDB search_path for a session connection.
// It tries to include the user's schema first; if that schema doesn't exist,
// it falls back to just 'main'.
//
// memory.main is always included so that pg_catalog macros (pg_get_userbyid,
// format_type, etc.) remain resolvable when the default catalog is ducklake.
// Without it, DuckDB restricts function resolution to the ducklake catalog
// and psql commands like \dt fail.
func initSearchPath(conn *sql.Conn, username string) {
	if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET search_path = '%s,main,memory.main'", username)); err != nil {
		slog.Debug("User schema not found, using default search_path.", "user", username)
		// Clear the aborted transaction state before retrying.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		if _, err := conn.ExecContext(context.Background(), "SET search_path = 'main,memory.main'"); err != nil {
			slog.Warn("Failed to set search_path for session.", "user", username, "error", err)
		}
	}
}

func generateSessionToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate session token: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// customActionServer wraps a flight.FlightServer to add custom DoAction
// handlers while preserving the standard flightsql routing for all other
// methods and standard Flight SQL action types.
type customActionServer struct {
	flight.FlightServer
	handler *FlightSQLHandler
}

func (s *customActionServer) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch cmd.Type {
	case "CreateSession":
		return s.handler.doCreateSession(cmd.Body, stream)
	case "ActivateTenant":
		return s.handler.doActivateTenant(cmd.Body, stream)
	case "DestroySession":
		return s.handler.doDestroySession(cmd.Body, stream)
	case "HealthCheck":
		return s.handler.doHealthCheck(cmd.Body, stream)
	case "WaitSessionIdle":
		return s.handler.doWaitSessionIdle(cmd.Body, stream)
	default:
		// Fall through to standard flightsql action router (BeginTransaction, etc.)
		return s.FlightServer.DoAction(cmd, stream)
	}
}

// DoPut peeks at the first FlightData frame and routes a duckgres custom
// CSV-spool stream to doCopyFromStdin. Anything else is delegated to the
// standard Flight SQL DoPut router (CommandStatementUpdate etc.).
//
// We have to consume the first frame to check the descriptor, so we
// hand the custom handler a reference to it and the rest of the stream;
// for non-custom streams we wrap the underlying server with a small
// adapter that returns the buffered first frame on the first Recv()
// call so the standard handler sees the full stream.
func (s *customActionServer) DoPut(stream flight.FlightService_DoPutServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if IsCopyFromStdinDescriptor(first.GetFlightDescriptor()) {
		return s.handler.doCopyFromStdin(stream.Context(), first, stream)
	}
	return s.FlightServer.DoPut(&prebufferedDoPutServer{
		FlightService_DoPutServer: stream,
		buffered:                  first,
	})
}

// prebufferedDoPutServer returns a single buffered FlightData frame on
// the first Recv() call, then delegates to the wrapped stream. Used by
// customActionServer.DoPut so that consuming the first frame for
// descriptor-routing doesn't strip it from the stream the underlying
// handler sees.
type prebufferedDoPutServer struct {
	flight.FlightService_DoPutServer
	buffered *flight.FlightData
}

func (p *prebufferedDoPutServer) Recv() (*flight.FlightData, error) {
	if p.buffered != nil {
		fd := p.buffered
		p.buffered = nil
		return fd, nil
	}
	return p.FlightService_DoPutServer.Recv()
}

// NewFlightSQLHandler creates a new multi-session Flight SQL handler.
func NewFlightSQLHandler(pool *SessionPool) *FlightSQLHandler {
	h := &FlightSQLHandler{
		pool:  pool,
		alloc: memory.DefaultAllocator,
	}

	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "duckgres"); err != nil {
		panic(fmt.Sprintf("register sql info server name: %v", err))
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0"); err != nil {
		panic(fmt.Sprintf("register sql info server version: %v", err))
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoTransactionsSupported, true); err != nil {
		panic(fmt.Sprintf("register sql info transactions supported: %v", err))
	}

	return h
}
