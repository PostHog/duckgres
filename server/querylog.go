package server

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

// QueryLogEntry represents a single entry in the query log.
type QueryLogEntry struct {
	EventTime       time.Time
	QueryDurationMs int64
	Type            string // "QueryFinish" or "ExceptionWhileProcessing"
	Query           string
	TranspiledQuery *string // nil if unchanged
	QueryKind       string  // "Select","Insert","Update","Delete","DDL","Utility","Copy","Cursor"
	NormalizedHash  uint64
	ResultRows      int64
	WrittenRows     int64
	ExceptionCode   string
	Exception       string
	UserName        string
	CurrentDatabase string
	ClientAddress   string
	ClientPort      int
	ApplicationName string
	PID             int32
	WorkerID        int
	IsTranspiled    bool
	Protocol        string // "simple" or "extended"

	// Resource metrics from worker (zero if unavailable or standalone mode)
	MemoryBytes  int64
	CPUTimeUs    int64
	BytesScanned int64
	BytesWritten int64
}

// QueryLogger durably logs query entries via a WAL and flushes them to DuckLake.
// Every Log() call is synchronously written to the WAL for crash resilience.
// A background goroutine periodically reads the WAL and flushes to DuckLake.
type QueryLogger struct {
	db       *sql.DB
	cfg      QueryLogConfig
	wal      *queryLogWAL
	done     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
}

const (
	maxQueryLength    = 4096
	flushMaxRetries   = 3
	flushRetryBaseMs  = 1000 // 1s, 2s, 4s exponential backoff
)

// NewQueryLogger opens a dedicated :memory: DuckDB, attaches DuckLake,
// creates the system.query_log table, initializes the WAL for crash-resilient
// logging, replays any unfinished WAL entries, and starts the background flush goroutine.
func NewQueryLogger(cfg Config) (*QueryLogger, error) {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil, nil
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("querylog: open duckdb: %w", err)
	}

	// Set extension directory under DataDir so DuckDB doesn't rely on $HOME/.duckdb
	extDir := filepath.Join(cfg.DataDir, "extensions")
	if _, err := db.Exec(fmt.Sprintf("SET extension_directory = '%s'", extDir)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: set extension_directory: %w", err)
	}

	// Load ducklake extension
	if _, err := db.Exec("INSTALL ducklake; LOAD ducklake"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: load ducklake: %w", err)
	}

	// Create S3 secret if needed (reuse the same logic as AttachDuckLake)
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
				return nil, fmt.Errorf("querylog: create S3 secret: %w", err)
			}
		}
	}

	// Attach DuckLake
	dataPath := dlCfg.ObjectStore
	if dataPath == "" {
		dataPath = dlCfg.DataPath
	}
	var attachStmt string
	if dataPath != "" {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake (DATA_PATH '%s')",
			escapeSQLStringLiteral(dlCfg.MetadataStore), escapeSQLStringLiteral(dataPath))
	} else {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", escapeSQLStringLiteral(dlCfg.MetadataStore))
	}
	if _, err := db.Exec(attachStmt); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: attach ducklake: %w", err)
	}

	// Create schema and table
	if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS ducklake.system"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: create schema: %w", err)
	}

	createTable := `CREATE TABLE IF NOT EXISTS ducklake.system.query_log (
		event_time          TIMESTAMPTZ NOT NULL,
		query_duration_ms   BIGINT NOT NULL,
		type                VARCHAR NOT NULL,
		query               VARCHAR NOT NULL,
		transpiled_query    VARCHAR,
		query_kind          VARCHAR,
		normalized_query_hash UBIGINT,
		result_rows         BIGINT DEFAULT 0,
		written_rows        BIGINT DEFAULT 0,
		exception_code      VARCHAR,
		exception           VARCHAR,
		user_name           VARCHAR NOT NULL,
		current_database    VARCHAR,
		client_address      VARCHAR,
		client_port         INTEGER,
		application_name    VARCHAR,
		pid                 INTEGER,
		worker_id           INTEGER DEFAULT -1,
		is_transpiled       BOOLEAN DEFAULT FALSE,
		protocol            VARCHAR DEFAULT 'simple'
	)`
	if _, err := db.Exec(createTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: create table: %w", err)
	}

	// Migrate: add resource metric columns if missing (idempotent)
	for _, col := range []struct{ name, def string }{
		{"memory_bytes", "BIGINT DEFAULT 0"},
		{"cpu_time_us", "BIGINT DEFAULT 0"},
		{"bytes_scanned", "BIGINT DEFAULT 0"},
		{"bytes_written_storage", "BIGINT DEFAULT 0"},
	} {
		stmt := fmt.Sprintf("ALTER TABLE ducklake.system.query_log ADD COLUMN IF NOT EXISTS %s %s", col.name, col.def)
		if _, err := db.Exec(stmt); err != nil {
			slog.Warn("querylog: migrate column failed, continuing.", "column", col.name, "error", err)
		}
	}

	// Configure data inlining
	inlineStmt := fmt.Sprintf(
		"CALL ducklake.set_option('data_inlining_row_limit', %d, schema_name => 'system', table_name => 'query_log')",
		cfg.QueryLog.DataInliningRowLimit)
	if _, err := db.Exec(inlineStmt); err != nil {
		slog.Warn("querylog: failed to set data_inlining_row_limit, continuing without it.", "error", err)
	}

	// Initialize WAL
	wal, err := newWAL(cfg.DataDir, cfg.QueryLog.WALMaxBytes, cfg.QueryLog.GroupCommitInterval)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: init WAL: %w", err)
	}

	ql := &QueryLogger{
		db:   db,
		cfg:  cfg.QueryLog,
		wal:  wal,
		done: make(chan struct{}),
		stop: make(chan struct{}),
	}

	// Replay any WAL entries from a previous crash
	if replayed, replayErr := ql.replayWAL(); replayErr != nil {
		slog.Warn("querylog: WAL replay failed, entries remain in WAL for next cycle.", "error", replayErr)
	} else if replayed > 0 {
		slog.Info("querylog: replayed WAL entries from previous run.", "count", replayed)
	}

	go ql.flushLoop()
	slog.Info("Query log enabled.", "flush_interval", cfg.QueryLog.FlushInterval, "batch_size", cfg.QueryLog.BatchSize, "wal_max_bytes", cfg.QueryLog.WALMaxBytes)
	return ql, nil
}

// Log durably writes an entry to the WAL. This call blocks until the entry
// is persisted to disk (via fdatasync or group commit). It does NOT block on
// the DuckLake flush — that happens asynchronously in the background.
func (ql *QueryLogger) Log(entry QueryLogEntry) {
	if err := ql.wal.Append(entry); err != nil {
		slog.Warn("querylog: WAL append failed, entry may be lost.", "error", err)
	}
}

// Stop flushes remaining WAL entries to DuckLake and shuts down.
func (ql *QueryLogger) Stop() {
	if ql == nil {
		return
	}
	ql.stopOnce.Do(func() {
		close(ql.stop)
		<-ql.done

		// Final flush: drain WAL → DuckLake
		if entries, err := ql.wal.ReadAll(); err == nil && len(entries) > 0 {
			ql.flushBatchWithRetry(entries)
			_ = ql.wal.Truncate()
		}

		ql.compactParquet()
		_ = ql.wal.Close()
		_ = ql.db.Close()
	})
}

// replayWAL reads entries from a previous crash's WAL and flushes them to DuckLake.
func (ql *QueryLogger) replayWAL() (int, error) {
	entries, err := ql.wal.ReadAll()
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, nil
	}

	// Flush in batches
	for i := 0; i < len(entries); i += ql.cfg.BatchSize {
		end := i + ql.cfg.BatchSize
		if end > len(entries) {
			end = len(entries)
		}
		if err := ql.flushBatchWithRetry(entries[i:end]); err != nil {
			return i, fmt.Errorf("replay flush at offset %d: %w", i, err)
		}
	}

	if err := ql.wal.Truncate(); err != nil {
		return len(entries), fmt.Errorf("replay truncate: %w", err)
	}
	return len(entries), nil
}

func (ql *QueryLogger) flushLoop() {
	defer close(ql.done)

	flushTicker := time.NewTicker(ql.cfg.FlushInterval)
	defer flushTicker.Stop()

	compactTicker := time.NewTicker(ql.cfg.CompactInterval)
	defer compactTicker.Stop()

	for {
		select {
		case <-ql.stop:
			return
		case <-flushTicker.C:
			ql.drainWAL()
		case <-compactTicker.C:
			ql.drainWAL()
			ql.compactParquet()
		}
	}
}

// drainWAL reads all entries from the WAL, flushes them to DuckLake, and truncates.
func (ql *QueryLogger) drainWAL() {
	entries, err := ql.wal.ReadAll()
	if err != nil {
		slog.Error("querylog: WAL read failed.", "error", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	// Flush in batches
	for i := 0; i < len(entries); i += ql.cfg.BatchSize {
		end := i + ql.cfg.BatchSize
		if end > len(entries) {
			end = len(entries)
		}
		if err := ql.flushBatchWithRetry(entries[i:end]); err != nil {
			slog.Error("querylog: flush failed after retries, entries remain in WAL.",
				"error", err, "batch_offset", i, "total_entries", len(entries))
			return // Don't truncate — entries stay in WAL for next cycle
		}
	}

	if err := ql.wal.Truncate(); err != nil {
		slog.Error("querylog: WAL truncate failed.", "error", err)
	}
}

// flushBatchWithRetry attempts to write a batch to DuckLake with retries.
// Returns nil on success, or the last error after all retries are exhausted.
func (ql *QueryLogger) flushBatchWithRetry(batch []QueryLogEntry) error {
	if len(batch) == 0 {
		return nil
	}

	var lastErr error
	for attempt := 0; attempt <= flushMaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(flushRetryBaseMs*(1<<(attempt-1))) * time.Millisecond
			time.Sleep(backoff)
		}
		if err := ql.flushBatch(batch); err != nil {
			lastErr = err
			slog.Warn("querylog: flush attempt failed, retrying.",
				"attempt", attempt+1, "max_retries", flushMaxRetries+1, "error", err, "batch_size", len(batch))
			continue
		}
		return nil
	}
	return lastErr
}

func (ql *QueryLogger) flushBatch(batch []QueryLogEntry) error {
	if len(batch) == 0 {
		return nil
	}

	const colCount = 24
	// Build multi-row INSERT with placeholders
	var sb strings.Builder
	sb.WriteString("INSERT INTO ducklake.system.query_log (event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, memory_bytes, cpu_time_us, bytes_scanned, bytes_written_storage) VALUES ")

	args := make([]any, 0, len(batch)*colCount)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * colCount
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20,
			base+21, base+22, base+23, base+24)

		args = append(args,
			e.EventTime,
			e.QueryDurationMs,
			e.Type,
			truncateQuery(e.Query),
			truncateNullableQuery(e.TranspiledQuery),
			e.QueryKind,
			e.NormalizedHash,
			e.ResultRows,
			e.WrittenRows,
			e.ExceptionCode,
			e.Exception,
			e.UserName,
			e.CurrentDatabase,
			e.ClientAddress,
			e.ClientPort,
			e.ApplicationName,
			e.PID,
			e.WorkerID,
			e.IsTranspiled,
			e.Protocol,
			e.MemoryBytes,
			e.CPUTimeUs,
			e.BytesScanned,
			e.BytesWritten,
		)
	}

	if _, err := ql.db.Exec(sb.String(), args...); err != nil {
		return fmt.Errorf("querylog: flush INSERT: %w", err)
	}
	return nil
}

func (ql *QueryLogger) compactParquet() {
	_, err := ql.db.Exec("CALL ducklake_flush_inlined_data('ducklake', schema_name => 'system', table_name => 'query_log')")
	if err != nil {
		slog.Warn("querylog: compact failed.", "error", err)
	}
}

// truncateQuery truncates a query string to maxQueryLength.
func truncateQuery(q string) string {
	if len(q) > maxQueryLength {
		return q[:maxQueryLength]
	}
	return q
}

func truncateNullableQuery(q *string) *string {
	if q == nil {
		return nil
	}
	truncated := truncateQuery(*q)
	return &truncated
}

func escapeSQLStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// classifyQuery maps a command type string to a query_kind value.
func classifyQuery(cmdType string) string {
	switch cmdType {
	case "SELECT", "SHOW", "TABLE", "VALUES", "EXPLAIN":
		return "Select"
	case "INSERT":
		return "Insert"
	case "UPDATE":
		return "Update"
	case "DELETE":
		return "Delete"
	case "CREATE", "ALTER", "DROP", "TRUNCATE":
		return "DDL"
	case "COPY":
		return "Copy"
	case "BEGIN", "COMMIT", "ROLLBACK", "SET", "RESET", "DISCARD", "DEALLOCATE", "LISTEN", "NOTIFY", "UNLISTEN":
		return "Utility"
	default:
		return "Utility"
	}
}

// literalRegexp matches string literals and numeric literals for normalization.
var literalRegexp = regexp.MustCompile(`'[^']*'|"[^"]*"|\b\d+\.?\d*\b`)

// comparisonBoolNullRegexp matches boolean/null values in comparison expressions.
var comparisonBoolNullRegexp = regexp.MustCompile(`(=|<>|!=|<=|>=|<|>)\s*(TRUE|FALSE|NULL)\b`)

// normalizeQueryHash computes a FNV-1a hash of a query after collapsing
// whitespace and replacing literals with placeholders. This groups queries
// that differ only in parameter values.
func normalizeQueryHash(query string) uint64 {
	// Collapse whitespace
	var sb strings.Builder
	inSpace := false
	for _, r := range query {
		if unicode.IsSpace(r) {
			if !inSpace {
				sb.WriteByte(' ')
				inSpace = true
			}
		} else {
			sb.WriteRune(r)
			inSpace = false
		}
	}
	normalized := strings.TrimSpace(strings.ToUpper(sb.String()))

	// Replace literals with ?
	normalized = literalRegexp.ReplaceAllString(normalized, "?")
	normalized = comparisonBoolNullRegexp.ReplaceAllString(normalized, "$1 ?")

	h := fnv.New64a()
	h.Write([]byte(normalized))
	return h.Sum64()
}

// isQueryLogSelfReferential returns true if the query targets system.query_log,
// to prevent infinite recursion.
func isQueryLogSelfReferential(query string) bool {
	upper := strings.ToUpper(query)
	return strings.Contains(upper, "SYSTEM.QUERY_LOG")
}

// logQuery builds a QueryLogEntry from the connection context and sends it to the logger.
func (c *clientConn) logQuery(start time.Time, query, transpiledQuery, cmdType string,
	resultRows, writtenRows int64, errCode, errMsg, protocol string, metrics *QueryMetrics) {

	ql := c.server.queryLogger
	if ql == nil {
		return
	}
	if isQueryLogSelfReferential(query) {
		return
	}

	entryType := "QueryFinish"
	if errCode != "" {
		entryType = "ExceptionWhileProcessing"
	}

	var transpiled *string
	if transpiledQuery != "" && transpiledQuery != query {
		transpiled = &transpiledQuery
	}

	// Extract client address and port from the connection
	var clientAddr string
	var clientPort int
	if addr := c.conn.RemoteAddr(); addr != nil {
		addrStr := addr.String()
		if host, portStr, err := splitHostPort(addrStr); err == nil {
			clientAddr = host
			if p, err := parsePort(portStr); err == nil {
				clientPort = p
			}
		} else {
			clientAddr = addrStr
		}
	}

	entry := QueryLogEntry{
		EventTime:       start,
		QueryDurationMs: time.Since(start).Milliseconds(),
		Type:            entryType,
		Query:           query,
		TranspiledQuery: transpiled,
		QueryKind:       classifyQuery(cmdType),
		NormalizedHash:  normalizeQueryHash(query),
		ResultRows:      resultRows,
		WrittenRows:     writtenRows,
		ExceptionCode:   errCode,
		Exception:       errMsg,
		UserName:        c.username,
		CurrentDatabase: c.database,
		ClientAddress:   clientAddr,
		ClientPort:      clientPort,
		ApplicationName: c.applicationName,
		PID:             c.pid,
		WorkerID:        c.workerID,
		IsTranspiled:    transpiled != nil,
		Protocol:        protocol,
	}

	if metrics != nil {
		entry.MemoryBytes = metrics.MemoryBytes
		entry.CPUTimeUs = metrics.CPUTimeUs
		entry.BytesScanned = metrics.BytesScanned
		entry.BytesWritten = metrics.BytesWritten
	}

	ql.Log(entry)
}

// executorMetrics returns the latest query metrics from the executor if it
// implements MetricsProvider (e.g., FlightExecutor). Returns nil otherwise.
func (c *clientConn) executorMetrics() *QueryMetrics {
	if mp, ok := c.executor.(MetricsProvider); ok {
		return mp.LastQueryMetrics()
	}
	return nil
}

// splitHostPort splits a host:port pair.
func splitHostPort(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}

// parsePort converts a port string to int.
func parsePort(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("invalid port")
	}
	var port int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid port")
		}
		port = port*10 + int(c-'0')
	}
	return port, nil
}
