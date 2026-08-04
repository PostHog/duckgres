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
	"unicode/utf8"

	"github.com/posthog/duckgres/internal/analytics"
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
	db           *sql.DB
	cfg          QueryLogConfig
	table        string
	ch           chan QueryLogEntry
	done         chan struct{}
	stopOnce     sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
	buffered     atomic.Int64
	closeDB      bool
	prepareBatch func(context.Context, *sql.DB, []QueryLogEntry) error
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
	queryLogChannelSize        = 10000
	queryLogInitTimeout        = 30 * time.Second
	queryLogSurfaceInitTimeout = 2 * time.Second
	maxQueryLength             = 4096
)

var newPostgresQueryLogSink = func(ctx context.Context, cfg Config) (QueryLogSink, error) {
	return NewPostgresQueryLoggerContext(ctx, cfg.DuckLake, cfg.QueryLog)
}

// NewQueryLogSink creates the native Postgres-backed query-log sink.
func NewQueryLogSink(cfg Config) (QueryLogSink, error) {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryLogInitTimeout)
	defer cancel()
	return newPostgresQueryLogSink(ctx, cfg)
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
	ctx := ql.context()
	var err error
	if ql.prepareBatch != nil {
		err = ql.prepareBatch(ctx, ql.db, batch)
	}
	if err == nil {
		err = insertQueryLogEntries(ctx, ql.db, ql.table, batch)
	}
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
	// Column list, placeholder count, and argument order all come from the
	// single registry in querylog_schema.go so they cannot drift apart.
	colsPerRow := len(queryLogEntryColumns())
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(queryLogEntryColumnNames(), ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(batch)*colsPerRow)
	for i, e := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * colsPerRow
		sb.WriteByte('(')
		for col := range colsPerRow {
			if col > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "$%d", base+col+1)
		}
		sb.WriteByte(')')

		args = append(args, queryLogEntryInsertArgs(e)...)
	}

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert query_log entries: %w", err)
	}
	return nil
}

// truncateQuery truncates a query string to maxQueryLength.
func truncateQuery(q string) string {
	if len(q) > maxQueryLength {
		return q[:maxQueryLength]
	}
	return q
}

// boundQueryLogText caps control-plane query-log text before it reaches logs,
// in-memory queues, normalization, or RPC serialization. Clone the retained
// text so a bounded entry cannot keep a much larger query allocation alive.
func boundQueryLogText(text string) string {
	if text == "" {
		return ""
	}
	if len(text) > maxQueryLength {
		end := maxQueryLength
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		text = text[:end]
	}
	return strings.Clone(text)
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

// queryLogSink resolves where this connection's query-log entries go. The
// executor sink (the worker, in control-plane mode) wins: the worker owns the
// tenant's query-log storage.
func (c *clientConn) queryLogSink() queryLogEntrySink {
	if c.server != nil && c.server.cfg.QueryLog.Enabled && c.executor != nil {
		if sink, ok := c.executor.(queryLogEntrySink); ok {
			return sink
		}
	}
	if c.server != nil && c.server.queryLogSink != nil {
		return c.server.queryLogSink
	}
	if c.server != nil && c.server.queryLogger != nil {
		return c.server.queryLogger
	}
	return nil
}

// clientAddrPort splits the peer address for the query log.
func (c *clientConn) clientAddrPort() (string, int) {
	addr := c.conn.RemoteAddr()
	if addr == nil {
		return "", 0
	}
	addrStr := addr.String()
	host, portStr, err := splitHostPort(addrStr)
	if err != nil {
		return addrStr, 0
	}
	port, err := parsePort(portStr)
	if err != nil {
		return host, 0
	}
	return host, port
}

// logQueryStart emits the QueryStart event for a statement that is about to
// execute. It carries identity and client context; resource columns stay zero
// and are filled by the terminal event.
//
// This is what makes an in-flight query visible and, more importantly, what
// makes a query that never returns detectable: a QueryStart with no terminal is
// the only evidence left when a worker is OOM-killed mid-statement.
func (c *clientConn) logQueryStart(scope *queryMetricsScope) {
	if scope == nil || c.server == nil || !c.server.cfg.QueryLog.StartEvents.enabled(scope.queryText) {
		return
	}
	ql := c.queryLogSink()
	if ql == nil {
		return
	}
	// The scope's text is already redacted by the caller, but a query naming
	// the log itself must not be logged or the poll recurses.
	query := boundQueryLogText(scope.queryText)
	if isQueryLogSelfReferential(query) {
		return
	}

	clientAddr, clientPort := c.clientAddrPort()
	meta := c.queryMetadata(scope)
	encodedMeta, accessKinds, metaComplete := queryMetadataColumns(meta)
	// Prefer the parsed kind. A leading-keyword guess reads "WITH ..." as a
	// utility statement, which would make a start event disagree with its own
	// terminal about what the statement was.
	queryKind := meta.QueryKind
	if queryKind == "" {
		queryKind = classifyQuery(leadingSQLKeyword(query))
	}
	ql.Log(QueryLogEntry{
		QueryID:          scope.queryID,
		ParentQueryID:    scope.parentQueryID,
		StatementIndex:   scope.statementIndex,
		EventTime:        scope.start,
		Type:             QueryEventStart,
		Query:            query,
		QueryKind:        queryKind,
		NormalizedHash:   normalizeQueryHash(query),
		UserName:         c.username,
		OrgID:            c.orgID,
		CurrentDatabase:  c.database,
		ClientAddress:    clientAddr,
		ClientPort:       clientPort,
		ApplicationName:  c.applicationName,
		PID:              c.pid,
		WorkerID:         c.workerID,
		TraceID:          observe.TraceIDFromContext(c.ctx),
		SpanID:           observe.SpanIDFromContext(c.ctx),
		QueryMetadata:    encodedMeta,
		AccessKinds:      accessKinds,
		MetadataComplete: metaComplete,
		WorkerTier:       c.currentWorkerTier(),
	})
}

// logQuery builds a QueryLogEntry from the connection context and sends it to the logger.
func (c *clientConn) logQuery(start time.Time, query, transpiledQuery, cmdType string,
	resultRows, writtenRows int64, errCode, errMsg, protocol string) {

	// Consume the profiling rollup left behind by the most recent
	// EnrichSpanWithProfiling up front, so the per-org analytics event and the
	// query-log row observe the same values and it is reset even when the
	// query-log sink is disabled (a later logQuery without a fresh exec must not
	// reuse stale timings from a previous query).
	profilingSummary := c.lastProfilingSummary
	c.lastProfilingSummary = observe.QueryProfilingSummary{}

	if isQueryLogSelfReferential(query) {
		return
	}

	// Per-org product analytics for the terminal event of a successful query.
	// Failures are already captured by query_failed (logQueryError), so gate on
	// errCode == "" to keep the lifecycle clean (initiated -> completed | failed)
	// and avoid double-counting. Emitted independently of the query-log sink so
	// this usage signal does not depend on query-log configuration.
	if errCode == "" {
		analytics.Default().Capture("query_completed", c.orgID, map[string]any{
			"user":        c.username,
			"team_id":     c.teamID,
			"trace_id":    observe.TraceIDFromContext(c.ctx),
			"protocol":    protocol,
			"query_kind":  classifyQuery(cmdType),
			"duration_ms": time.Since(start).Milliseconds(),
			"cpu_seconds": profilingSummary.CPUTimeSeconds,
			"result_rows": resultRows,
		})
	}

	ql := c.queryLogSink()
	if ql == nil {
		return
	}

	// CREATE SECRET option lists carry credential material; never persist
	// them to the query log. The engine's error text echoes the offending SQL,
	// so a failed CREATE SECRET leaks the credential via Exception unless the
	// error is redacted too — classify against the original query first.
	errMsg = boundQueryLogText(usersecrets.RedactErrorForLog(query, errMsg))
	query = usersecrets.RedactForLog(query)
	transpiledQuery = usersecrets.RedactForLog(transpiledQuery)

	// A failure before execution began is ExceptionBeforeStart and has no
	// QueryStart row to pair with. Without a scope we cannot know, so assume
	// the statement started — claiming it never did would be a stronger
	// statement than the evidence supports.
	execStarted := true
	scope := c.activeQueryMetrics
	if scope != nil {
		execStarted = scope.execStarted
	}
	entryType := terminalQueryEventType(errCode, execStarted)

	isTranspiled := transpiledQuery != "" && transpiledQuery != query
	query = boundQueryLogText(query)
	var transpiled *string
	if isTranspiled {
		boundedTranspiledQuery := boundQueryLogText(transpiledQuery)
		transpiled = &boundedTranspiledQuery
	}

	clientAddr, clientPort := c.clientAddrPort()

	pgScanMs := int64(profilingSummary.PostgresScanSeconds * 1000)

	parentQueryID, statementIndex := "", 0
	if scope != nil {
		parentQueryID, statementIndex = scope.parentQueryID, scope.statementIndex
	}
	// The terminal event carries the same extraction as the start event. When
	// the scope has none (a path that logs without one), fall back to the
	// statement text this call was handed so the row still says what was
	// touched.
	meta := c.queryMetadata(scope)
	if scope == nil && c.server != nil && c.server.cfg.QueryLog.Metadata {
		meta = extractQueryMetadata(query)
	}
	encodedMeta, accessKinds, metaComplete := queryMetadataColumns(meta)
	ql.Log(QueryLogEntry{
		QueryID:               c.currentQueryID(),
		ParentQueryID:         parentQueryID,
		StatementIndex:        statementIndex,
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
		IsTranspiled:          isTranspiled,
		Protocol:              protocol,
		TraceID:               observe.TraceIDFromContext(c.ctx),
		SpanID:                observe.SpanIDFromContext(c.ctx),
		PostgresScanMs:        pgScanMs,
		CPUTimeSeconds:        profilingSummary.CPUTimeSeconds,
		PeakBufferMemoryBytes: profilingSummary.PeakBufferMemoryBytes,
		QueryMetadata:         encodedMeta,
		AccessKinds:           accessKinds,
		MetadataComplete:      metaComplete,
		WorkerTier:            c.currentWorkerTier(),
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
