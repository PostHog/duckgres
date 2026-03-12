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
}

// QueryLogger batches query log entries and writes them to a DuckLake table.
type QueryLogger struct {
	db       *sql.DB
	cfg      QueryLogConfig
	ch       chan QueryLogEntry
	done     chan struct{}
	stopOnce sync.Once
}

const (
	queryLogChannelSize = 10000
	maxQueryLength      = 4096
)

// NewQueryLogger opens a dedicated :memory: DuckDB, attaches DuckLake,
// creates the system.query_log table, and starts the background flush goroutine.
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

	// Configure data inlining
	inlineStmt := fmt.Sprintf(
		"CALL ducklake.set_option('data_inlining_row_limit', %d, schema_name => 'system', table_name => 'query_log')",
		cfg.QueryLog.DataInliningRowLimit)
	if _, err := db.Exec(inlineStmt); err != nil {
		slog.Warn("querylog: failed to set data_inlining_row_limit, continuing without it.", "error", err)
	}

	ql := &QueryLogger{
		db:   db,
		cfg:  cfg.QueryLog,
		ch:   make(chan QueryLogEntry, queryLogChannelSize),
		done: make(chan struct{}),
	}

	go ql.flushLoop()
	slog.Info("Query log enabled.", "flush_interval", cfg.QueryLog.FlushInterval, "batch_size", cfg.QueryLog.BatchSize)
	return ql, nil
}

// Log sends an entry to the query log. Non-blocking; drops if channel is full.
func (ql *QueryLogger) Log(entry QueryLogEntry) {
	select {
	case ql.ch <- entry:
	default:
		slog.Warn("querylog: channel full, dropping entry.")
	}
}

// Stop drains remaining entries and shuts down the flush goroutine.
func (ql *QueryLogger) Stop() {
	if ql == nil {
		return
	}
	ql.stopOnce.Do(func() {
		if ql.ch != nil {
			close(ql.ch)
		}
		if ql.done != nil {
			<-ql.done
		}
		if ql.db != nil {
			_ = ql.db.Close()
		}
	})
}

func (ql *QueryLogger) flushLoop() {
	defer close(ql.done)

	batch := make([]QueryLogEntry, 0, ql.cfg.BatchSize)
	flushTicker := time.NewTicker(ql.cfg.FlushInterval)
	defer flushTicker.Stop()

	compactTicker := time.NewTicker(ql.cfg.CompactInterval)
	defer compactTicker.Stop()

	for {
		select {
		case entry, ok := <-ql.ch:
			if !ok {
				// Channel closed — drain and exit
				if len(batch) > 0 {
					ql.flushBatch(batch)
				}
				ql.compactParquet()
				return
			}
			batch = append(batch, entry)
			if len(batch) >= ql.cfg.BatchSize {
				ql.flushBatch(batch)
				batch = batch[:0]
			}
		case <-flushTicker.C:
			if len(batch) > 0 {
				ql.flushBatch(batch)
				batch = batch[:0]
			}
		case <-compactTicker.C:
			if len(batch) > 0 {
				ql.flushBatch(batch)
				batch = batch[:0]
			}
			ql.compactParquet()
		}
	}
}

func (ql *QueryLogger) flushBatch(batch []QueryLogEntry) {
	if len(batch) == 0 {
		return
	}

	// Build multi-row INSERT with placeholders
	var sb strings.Builder
	sb.WriteString("INSERT INTO ducklake.system.query_log (event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol) VALUES ")

	args := make([]any, 0, len(batch)*20)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 20
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20)

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
		)
	}

	if _, err := ql.db.Exec(sb.String(), args...); err != nil {
		slog.Error("querylog: flush failed.", "error", err, "batch_size", len(batch))
	}
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
	resultRows, writtenRows int64, errCode, errMsg, protocol string) {

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

	ql.Log(QueryLogEntry{
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
	})
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
