package server

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/usersecrets"
	"github.com/posthog/duckgres/server/wire"
)

// QueryLogEntry represents a single entry in the query log.
//
// The concrete shape lives in server/wire so the DuckDB-free Flight client can
// forward entries to worker pods without importing the full server package.
type QueryLogEntry = wire.QueryLogEntry

// QueryLogger batches query log entries and writes them to durable storage.
type QueryLogger struct {
	db       *sql.DB
	cfg      QueryLogConfig
	table    string
	ch       chan QueryLogEntry
	done     chan struct{}
	stopOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	buffered atomic.Int64
	closeDB  bool
}

// QueryLogSink accepts query log entries and drains them during shutdown.
type QueryLogSink interface {
	Log(QueryLogEntry)
	StopContext(context.Context) error
}

type queryLogEntrySink interface {
	Log(QueryLogEntry)
}

const (
	queryLogChannelSize = 10000
	maxQueryLength      = 4096
)

// NewQueryLogSink creates the native Postgres-backed query-log sink.
func NewQueryLogSink(cfg Config) (QueryLogSink, error) {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil, nil
	}
	return NewPostgresQueryLoggerContext(context.Background(), cfg.DuckLake, cfg.QueryLog)
}

// Log sends an entry to the query log. Non-blocking; drops if channel is full.
func (ql *QueryLogger) Log(entry QueryLogEntry) {
	if ql == nil || ql.ch == nil {
		return
	}
	ql.addBufferedEntries(1)
	queued := false
	defer func() {
		if r := recover(); r != nil {
			if !queued {
				ql.addBufferedEntries(-1)
				observe.AddQueryLogDroppedEntries("logger_closed", 1)
			}
			slog.Warn("querylog: logger stopped while writing entry; dropping entry.")
		}
	}()
	select {
	case ql.ch <- entry:
		queued = true
		observe.IncQueryLogEnqueuedEntries()
	default:
		ql.addBufferedEntries(-1)
		observe.AddQueryLogDroppedEntries("buffer_full", 1)
		slog.Warn("querylog: channel full, dropping entry.")
	}
}

// Stop drains remaining entries and shuts down the flush goroutine.
func (ql *QueryLogger) Stop() {
	_ = ql.StopContext(context.Background())
}

// StopContext drains remaining entries until ctx expires. If the deadline is
// reached, it cancels in-flight storage work; loggers that own a DB handle also
// close it to unblock shutdown.
func (ql *QueryLogger) StopContext(ctx context.Context) error {
	if ql == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var stopErr error
	ql.stopOnce.Do(func() {
		if ql.ch != nil {
			close(ql.ch)
		}
		if ql.done != nil {
			select {
			case <-ql.done:
			case <-ctx.Done():
				stopErr = ctx.Err()
				if ql.cancel != nil {
					ql.cancel()
				}
			}
		}
		if ql.cancel != nil {
			ql.cancel()
		}
		if ql.db != nil && ql.closeDB {
			_ = ql.db.Close()
		}
	})
	return stopErr
}

func queryLogStopContext(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultTimeout)
}

func (ql *QueryLogger) flushLoop() {
	defer close(ql.done)
	defer observe.SetQueryLogBufferedEntries(0)

	batch := make([]QueryLogEntry, 0, ql.cfg.BatchSize)
	flushTicker := time.NewTicker(ql.cfg.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case entry, ok := <-ql.ch:
			if !ok {
				// Channel closed — drain and exit
				if len(batch) > 0 {
					ql.flushBatch(batch)
				}
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
		}
	}
}

func (ql *QueryLogger) addBufferedEntries(delta int64) {
	if ql == nil {
		return
	}
	buffered := ql.buffered.Add(delta)
	if buffered < 0 {
		ql.buffered.Store(0)
		buffered = 0
	}
	observe.SetQueryLogBufferedEntries(int(buffered))
}

func (ql *QueryLogger) context() context.Context {
	if ql == nil {
		return context.Background()
	}
	ctx := ql.ctx
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (ql *QueryLogger) flushBatch(batch []QueryLogEntry) {
	defer ql.addBufferedEntries(-int64(len(batch)))
	start := time.Now()
	err := insertQueryLogEntries(ql.context(), ql.db, ql.table, batch)
	observe.ObserveQueryLogFlushDuration(time.Since(start))
	if err != nil {
		observe.IncQueryLogFlushErrors()
		observe.AddQueryLogDroppedEntries("flush_error", len(batch))
		slog.Error("querylog: flush failed.", "error", err, "batch_size", len(batch))
		return
	}
	observe.AddQueryLogFlushedEntries(len(batch))
}

func insertQueryLogEntries(ctx context.Context, db *sql.DB, table string, batch []QueryLogEntry) error {
	if len(batch) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var sb strings.Builder
	if table == "" {
		table = "query_log"
	}
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, org_id, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, trace_id, span_id, postgres_scan_ms, cpu_time_s, peak_buffer_memory_bytes) VALUES ")

	const colsPerRow = 26
	args := make([]any, 0, len(batch)*colsPerRow)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * colsPerRow
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20, base+21, base+22, base+23, base+24, base+25, base+26)

		args = append(args, queryLogEntryInsertArgs(e)...)
	}

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert query_log entries: %w", err)
	}
	return nil
}

func queryLogEntryInsertArgs(e QueryLogEntry) []any {
	return []any{
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
		e.PostgresScanMs,
		e.CPUTimeSeconds,
		e.PeakBufferMemoryBytes,
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

	var ql queryLogEntrySink
	if c.server != nil && c.server.cfg.QueryLog.Enabled && c.executor != nil {
		if sink, ok := c.executor.(queryLogEntrySink); ok {
			ql = sink
		}
	}
	if ql == nil && c.server != nil && c.server.queryLogSink != nil {
		ql = c.server.queryLogSink
	}
	if ql == nil && c.server != nil && c.server.queryLogger != nil {
		ql = c.server.queryLogger
	}
	if ql == nil {
		return
	}
	profilingSummary := c.lastProfilingSummary
	c.lastProfilingSummary = observe.QueryProfilingSummary{}
	if isQueryLogSelfReferential(query) {
		return
	}

	// CREATE SECRET option lists carry credential material; never persist
	// them to the query log. The engine's error text echoes the offending SQL,
	// so a failed CREATE SECRET leaks the credential via Exception unless the
	// error is redacted too — classify against the original query first.
	errMsg = usersecrets.RedactErrorForLog(query, errMsg)
	query = usersecrets.RedactForLog(query)
	transpiledQuery = usersecrets.RedactForLog(transpiledQuery)

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

	// Consume the profiling rollup left behind by the most recent
	// EnrichSpanWithProfiling on this connection and reset it so a later
	// logQuery without a fresh exec (e.g. parse-failure path) doesn't
	// reuse stale timings from a previous query.
	pgScanMs := int64(profilingSummary.PostgresScanSeconds * 1000)

	ql.Log(QueryLogEntry{
		EventTime:             start,
		QueryDurationMs:       time.Since(start).Milliseconds(),
		Type:                  entryType,
		Query:                 query,
		TranspiledQuery:       transpiled,
		QueryKind:             classifyQuery(cmdType),
		NormalizedHash:        normalizeQueryHash(query),
		ResultRows:            resultRows,
		WrittenRows:           writtenRows,
		ExceptionCode:         errCode,
		Exception:             errMsg,
		UserName:              c.username,
		OrgID:                 c.orgID,
		CurrentDatabase:       c.database,
		ClientAddress:         clientAddr,
		ClientPort:            clientPort,
		ApplicationName:       c.applicationName,
		PID:                   c.pid,
		WorkerID:              c.workerID,
		IsTranspiled:          transpiled != nil,
		Protocol:              protocol,
		TraceID:               observe.TraceIDFromContext(c.ctx),
		SpanID:                observe.SpanIDFromContext(c.ctx),
		PostgresScanMs:        pgScanMs,
		CPUTimeSeconds:        profilingSummary.CPUTimeSeconds,
		PeakBufferMemoryBytes: profilingSummary.PeakBufferMemoryBytes,
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
