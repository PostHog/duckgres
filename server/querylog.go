package server

import (
	"database/sql"
	"errors"
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
	NormalizedHash  int64
	ResultRows      int64
	WrittenRows     int64
	ExceptionCode   string
	Exception       string
	UserName        string
	OrgID           string
	CurrentDatabase string
	ClientAddress   string
	ClientPort      int
	ApplicationName string
	PID             int32
	WorkerID        int
	IsTranspiled    bool
	Protocol        string // "simple" or "extended"
	TraceID         string // OTEL trace ID (empty when tracing is off)
	SpanID          string // OTEL span ID (empty when tracing is off)
}

// QueryLogger batches query log entries and writes them to a DuckLake table.
type QueryLogger struct {
	db       *sql.DB
	cfg      QueryLogConfig
	table    string
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

	if err := applyDuckLakePreAttachSettings(db, dlCfg); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: pre-attach settings: %w", err)
	}

	// Attach DuckLake
	attachStmt := buildDuckLakeAttachStmt(dlCfg, duckLakeMigrationNeeded())
	if _, err := db.Exec(attachStmt); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: attach ducklake: %w", err)
	}

	configureDuckLakeMetadataPool(db)

	// Create schema and table
	if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS ducklake.system"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: create schema: %w", err)
	}

	if err := ensureQueryLogTable(db, "system", "query_log", "ducklake.system.query_log"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("querylog: ensure table: %w", err)
	}

	// Configure data inlining
	inlineStmt := fmt.Sprintf(
		"CALL ducklake.set_option('data_inlining_row_limit', %d, schema => 'system', table_name => 'query_log')",
		cfg.QueryLog.DataInliningRowLimit)
	if _, err := db.Exec(inlineStmt); err != nil {
		slog.Warn("querylog: failed to set data_inlining_row_limit, continuing without it.", "error", err)
	}

	ql := &QueryLogger{
		db:    db,
		cfg:   cfg.QueryLog,
		table: "ducklake.system.query_log",
		ch:    make(chan QueryLogEntry, queryLogChannelSize),
		done:  make(chan struct{}),
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
	table := ql.table
	if table == "" {
		table = "query_log"
	}
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, org_id, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, trace_id, span_id) VALUES ")

	const colsPerRow = 23
	args := make([]any, 0, len(batch)*colsPerRow)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * colsPerRow
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20, base+21, base+22, base+23)

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
			e.OrgID,
			e.CurrentDatabase,
			e.ClientAddress,
			e.ClientPort,
			e.ApplicationName,
			e.PID,
			e.WorkerID,
			e.IsTranspiled,
			e.Protocol,
			e.TraceID,
			e.SpanID,
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

func ensureQueryLogTable(db *sql.DB, tableSchema, tableName, fullTableName string) error {
	colType, err := queryLogColumnType(db, fullTableName, tableSchema, tableName, "normalized_query_hash")
	if err == nil && strings.ToUpper(colType) != "BIGINT" {
		slog.Info("querylog: dropping query_log to migrate normalized_query_hash from UBIGINT to BIGINT.")
		if _, dropErr := db.Exec("DROP TABLE " + fullTableName); dropErr != nil {
			return fmt.Errorf("drop query_log for normalized_query_hash migration: %w", dropErr)
		}
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("inspect normalized_query_hash column: %w", err)
	}

	if _, err := db.Exec(queryLogCreateTableSQL(fullTableName)); err != nil {
		return fmt.Errorf("create query_log table: %w", err)
	}

	hasOrgID, err := queryLogColumnExists(db, fullTableName, tableSchema, tableName, "org_id")
	if err != nil {
		return fmt.Errorf("inspect org_id column: %w", err)
	}
	if !hasOrgID {
		if _, err := db.Exec("ALTER TABLE " + fullTableName + " ADD COLUMN org_id VARCHAR"); err != nil {
			return fmt.Errorf("add org_id column: %w", err)
		}
	}

	// Add trace_id and span_id columns for OTEL tracing correlation.
	for _, col := range []string{"trace_id", "span_id"} {
		hasCol, err := queryLogColumnExists(db, fullTableName, tableSchema, tableName, col)
		if err != nil {
			return fmt.Errorf("inspect %s column: %w", col, err)
		}
		if !hasCol {
			if _, err := db.Exec("ALTER TABLE " + fullTableName + " ADD COLUMN " + col + " VARCHAR"); err != nil {
				return fmt.Errorf("add %s column: %w", col, err)
			}
		}
	}

	return nil
}

func queryLogCreateTableSQL(fullTableName string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		event_time          TIMESTAMPTZ NOT NULL,
		query_duration_ms   BIGINT NOT NULL,
		type                VARCHAR NOT NULL,
		query               VARCHAR NOT NULL,
		transpiled_query    VARCHAR,
		query_kind          VARCHAR,
		normalized_query_hash BIGINT,
		result_rows         BIGINT,
		written_rows        BIGINT,
		exception_code      VARCHAR,
		exception           VARCHAR,
		user_name           VARCHAR NOT NULL,
		org_id              VARCHAR,
		current_database    VARCHAR,
		client_address      VARCHAR,
		client_port         INTEGER,
		application_name    VARCHAR,
		pid                 INTEGER,
		worker_id           INTEGER,
		is_transpiled       BOOLEAN,
		protocol            VARCHAR,
		trace_id            VARCHAR,
		span_id             VARCHAR
	)`, fullTableName)
}

func queryLogColumnExists(db *sql.DB, fullTableName, tableSchema, tableName, columnName string) (bool, error) {
	_, err := queryLogColumnType(db, fullTableName, tableSchema, tableName, columnName)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func queryLogColumnType(db *sql.DB, fullTableName, tableSchema, tableName, columnName string) (string, error) {
	query := fmt.Sprintf("SELECT data_type FROM %s WHERE table_name = $1 AND column_name = $2", queryLogColumnsTable(fullTableName))
	args := []any{tableName, columnName}
	if strings.TrimSpace(tableSchema) != "" {
		query += " AND table_schema = $3"
		args = append(args, tableSchema)
	}

	var colType string
	err := db.QueryRow(query, args...).Scan(&colType)
	return colType, err
}

func queryLogColumnsTable(fullTableName string) string {
	parts := strings.Split(fullTableName, ".")
	if len(parts) == 3 {
		return parts[0] + ".information_schema.columns"
	}
	return "information_schema.columns"
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
func normalizeQueryHash(query string) int64 {
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
	return int64(h.Sum64())
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
		OrgID:           c.orgID,
		CurrentDatabase: c.database,
		ClientAddress:   clientAddr,
		ClientPort:      clientPort,
		ApplicationName: c.applicationName,
		PID:             c.pid,
		WorkerID:        c.workerID,
		IsTranspiled:    transpiled != nil,
		Protocol:        protocol,
		TraceID:         traceIDFromContext(c.ctx),
		SpanID:          spanIDFromContext(c.ctx),
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
