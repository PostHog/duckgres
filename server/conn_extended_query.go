package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/usersecrets"
	"github.com/posthog/duckgres/server/wire"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (c *clientConn) handleParse(body []byte) {
	// Parse message format:
	// - Statement name (null-terminated string)
	// - Query string (null-terminated string)
	// - Number of parameter types (int16)
	// - Parameter type OIDs (int32 each)

	reader := bytes.NewReader(body)

	// Read statement name
	stmtName, err := readCString(reader)
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Parse message")
		return
	}

	// Read query
	query, err := readCString(reader)
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Parse message")
		return
	}
	// Read number of parameter types
	var numParamTypes int16
	if err := binary.Read(reader, binary.BigEndian, &numParamTypes); err != nil {
		c.sendError("ERROR", "08P01", "invalid Parse message")
		return
	}

	// Read parameter type OIDs
	paramTypes := make([]int32, numParamTypes)
	for i := int16(0); i < numParamTypes; i++ {
		if err := binary.Read(reader, binary.BigEndian, &paramTypes[i]); err != nil {
			c.sendError("ERROR", "08P01", "invalid Parse message")
			return
		}
	}

	// Detect cursor operations before passthrough or transpilation.
	// DuckDB doesn't support DECLARE/FETCH/CLOSE natively, so cursor
	// emulation is needed for all users including passthrough.
	cursorTree, cursorParseErr := pg_query.Parse(query)
	transaction := transactionControlFromParseResult(cursorTree)
	if cursorParseErr == nil && len(cursorTree.Stmts) == 1 {
		switch s := cursorTree.Stmts[0].Stmt.Node.(type) {
		case *pg_query.Node_DeclareCursorStmt:
			innerSQL := deparseInnerQuery(s.DeclareCursorStmt.Query)
			transpiledSQL := innerSQL
			if !c.passthrough && innerSQL != "" {
				tr := c.newTranspiler(true)
				innerResult, innerErr := tr.Transpile(innerSQL)
				if innerErr == nil && !innerResult.FallbackToNative {
					transpiledSQL = innerResult.SQL
				}
			}
			delete(c.stmts, stmtName)
			c.stmts[stmtName] = &preparedStmt{
				query:          query,
				convertedQuery: query,
				cursorOp:       cursorOpDeclare,
				cursorName:     s.DeclareCursorStmt.Portalname,
				cursorQuery:    transpiledSQL,
			}
			_ = wire.WriteParseComplete(c.writer)
			return

		case *pg_query.Node_FetchStmt:
			if !isFetchForwardOnly(s.FetchStmt.Direction) || s.FetchStmt.HowMany < 0 {
				c.observeExtendedParseQueryError("0A000", "cursor can only scan forward")
				return
			}
			delete(c.stmts, stmtName)
			c.stmts[stmtName] = &preparedStmt{
				query:          query,
				convertedQuery: query,
				cursorOp:       cursorOpFetch,
				cursorName:     s.FetchStmt.Portalname,
				fetchCount:     s.FetchStmt.HowMany,
				cursorIsMove:   s.FetchStmt.Ismove,
			}
			_ = wire.WriteParseComplete(c.writer)
			return

		case *pg_query.Node_ClosePortalStmt:
			delete(c.stmts, stmtName)
			c.stmts[stmtName] = &preparedStmt{
				query:          query,
				convertedQuery: query,
				cursorOp:       cursorOpClose,
				cursorName:     s.ClosePortalStmt.Portalname,
			}
			_ = wire.WriteParseComplete(c.writer)
			return
		}
	}

	// Intercept pg_cursors queries (e.g. psycopg's "SELECT 1 FROM pg_cursors WHERE name = $1").
	// DuckDB doesn't have this system view; return synthetic results from cursor emulation state.
	if cursorName, parameterized, ok := matchPgCursorsQuery(query); ok {
		delete(c.stmts, stmtName)
		ps := &preparedStmt{
			query:          query,
			convertedQuery: query,
			cursorOp:       cursorOpPgCursorsQuery,
			cursorName:     cursorName,
		}
		if parameterized {
			ps.numParams = 1
			ps.paramTypes = []int32{25} // text OID
		}
		c.stmts[stmtName] = ps
		_ = wire.WriteParseComplete(c.writer)
		return
	}

	// Intercept pg_stat_activity queries. Return synthetic results from the connection registry.
	if matchPgStatActivityQuery(query) {
		delete(c.stmts, stmtName)
		c.stmts[stmtName] = &preparedStmt{
			query:          query,
			convertedQuery: query,
			cursorOp:       cursorOpPgStatActivity,
		}
		_ = wire.WriteParseComplete(c.writer)
		return
	}

	// Passthrough mode: skip transpilation, store query directly
	if c.passthrough {
		// Count $N parameters with a simple regex (pg_query.Parse may fail on DuckDB-native SQL)
		paramCount := countDollarParams(query)
		delete(c.stmts, stmtName)
		c.stmts[stmtName] = &preparedStmt{
			query:          query,
			convertedQuery: query, // No transpilation
			transaction:    transaction,
			paramTypes:     paramTypes,
			numParams:      paramCount,
		}
		_ = wire.WriteParseComplete(c.writer)
		return
	}

	// Transpile PostgreSQL SQL to DuckDB-compatible SQL (with placeholder conversion)
	tr := c.newTranspiler(true) // Enable placeholder conversion for prepared statements
	result, err := tr.Transpile(query)
	if err != nil {
		c.observeExtendedParseQueryError("42601", fmt.Sprintf("syntax error: %v", err))
		return
	}

	// Handle transform-detected errors (e.g., unrecognized config parameter)
	if result.Error != nil {
		c.observeExtendedParseQueryError(transformErrorSQLState(result.Error), result.Error.Error())
		return
	}

	// Handle fallback to native DuckDB: PostgreSQL parsing failed, try DuckDB directly
	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			// Neither PostgreSQL nor DuckDB can parse this query
			c.observeExtendedParseQueryError("42601", fmt.Sprintf("syntax error: %v", err))
			return
		}
		c.logger().Debug("Fallback to native DuckDB: query not valid PostgreSQL but valid DuckDB.", "query", usersecrets.RedactForLog(query))
	}

	// Close existing statement with same name
	delete(c.stmts, stmtName)

	c.stmts[stmtName] = &preparedStmt{
		query:             query,                            // Keep original for logging and Describe
		convertedQuery:    c.rewriteDirectQuery(result.SQL), // Transpiled SQL for execution
		transaction:       transaction,
		paramTypes:        paramTypes,
		numParams:         result.ParamCount,
		isIgnoredSet:      result.IsIgnoredSet,
		isNoOp:            result.IsNoOp,
		noOpTag:           result.NoOpTag,
		querySourceSet:    result.QuerySourceSet,    // SET duckgres.query_source (custom GUC)
		querySourceShow:   result.QuerySourceShow,   // SHOW duckgres.query_source
		statements:        result.Statements,        // Multi-statement rewrite (writable CTE)
		cleanupStatements: result.CleanupStatements, // Cleanup statements
	}

	c.logger().Debug("Prepared statement.", "name", stmtName, "query", usersecrets.RedactForLog(query))
	if len(result.Statements) > 0 {
		c.logger().Debug("Prepared statement multi-statement.", "name", stmtName, "statements", len(result.Statements), "cleanup", len(result.CleanupStatements))
	} else if result.SQL != query {
		c.logger().Debug("Prepared statement transpiled.", "name", stmtName, "transpiled", usersecrets.RedactForLog(result.SQL))
	}
	_ = wire.WriteParseComplete(c.writer)
}

func (c *clientConn) handleDescribe(body []byte) {
	// Describe message format:
	// - Type: 'S' for statement, 'P' for portal
	// - Name (null-terminated)

	if len(body) < 2 {
		c.sendError("ERROR", "08P01", "invalid Describe message")
		return
	}

	descType := body[0]
	name := string(bytes.TrimRight(body[1:], "\x00"))

	switch descType {
	case 'S':
		// Describe prepared statement
		ps, ok := c.stmts[name]
		if !ok {
			c.sendError("ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", name))
			return
		}
		c.logger().Debug("Describe statement.", "name", name, "query", usersecrets.RedactForLog(ps.query))

		// Send parameter description based on the number of $N placeholders we found
		// If the client didn't send explicit types, create them
		paramTypes := ps.paramTypes
		if len(paramTypes) < ps.numParams {
			paramTypes = make([]int32, ps.numParams)
			// Default to text type for unspecified params
			for i := range paramTypes {
				paramTypes[i] = 25 // text OID
			}
		}
		c.sendParameterDescription(paramTypes)

		// Handle cursor operations in Describe
		switch ps.cursorOp {
		case cursorOpDeclare, cursorOpClose:
			// DECLARE and CLOSE don't return rows
			_ = wire.WriteNoData(c.writer)
			return
		case cursorOpFetch:
			// MOVE advances the cursor without returning rows — NoData.
			if ps.cursorIsMove {
				_ = wire.WriteNoData(c.writer)
				return
			}
			// FETCH returns rows — look up cursor to get schema
			cols, colTypes, err := c.getCursorSchema(ps.cursorName)
			if err != nil || len(cols) == 0 {
				_ = wire.WriteNoData(c.writer)
				return
			}
			_ = c.sendRowDescription(cols, colTypes)
			ps.described = true
			return
		case cursorOpPgCursorsQuery:
			_ = c.sendPgCursorsRowDescriptionWithFormats(nil)
			ps.described = true
			return
		case cursorOpPgStatActivity:
			_ = c.sendPgStatActivityRowDescriptionWithFormats(nil)
			ps.described = true
			return
		}

		// duckgres.query_source custom GUC: SET returns no rows; SHOW returns a
		// single text column answered from session state (never probed against
		// DuckDB, which does not know this setting).
		if ps.querySourceSet != nil {
			_ = wire.WriteNoData(c.writer)
			return
		}
		if ps.querySourceShow {
			_ = c.sendRowDescription([]string{querySourceGUCName}, []ColumnTyper{staticColumnType("VARCHAR")})
			ps.described = true
			return
		}

		// For queries that return results, we need to send RowDescription
		// For other queries, send NoData
		returnsResults := queryReturnsResults(ps.query)
		c.logger().Debug("Describe statement returns results check.", "name", name, "returns_results", returnsResults)
		if !returnsResults {
			_ = wire.WriteNoData(c.writer)
			return
		}

		// DML with RETURNING cannot be described without executing the mutation.
		// Reject with an explicit error so clients don't desync (e.g., lib/pq
		// would use Exec-like handling after NoData, silently dropping rows).
		if isDMLReturning(ps.query) {
			c.sendError("ERROR", "0A000", "DML with RETURNING clause cannot be described without executing the mutation; use simple query protocol or skip the Describe step")
			return
		}

		// WITH + DML (no RETURNING) doesn't return results but queryReturnsResults
		// returns true for all WITH-prefixed queries. Send NoData to avoid executing
		// the mutation during schema probing.
		if isWithDML(ps.query) {
			_ = wire.WriteNoData(c.writer)
			return
		}

		// EXPLAIN [ANALYZE] returns a single textual plan column. Describing it via
		// the LIMIT-0 probe below would EXECUTE it — and EXPLAIN ANALYZE of a write
		// mutates — so the statement would run at Describe and again at Execute.
		// Send a synthetic RowDescription without executing.
		if isExplainStmt(ps.query) {
			_ = c.sendRowDescription([]string{explainPlanColumn(ps.query)}, []ColumnTyper{staticColumnType("VARCHAR")})
			ps.described = true
			return
		}

		// For SELECT, we need to describe the result columns
		// The cleanest approach is to add a "WHERE false" or "LIMIT 0" clause
		// to get column info without actually running the query
		describeQuery := strings.TrimRight(strings.TrimSpace(ps.convertedQuery), ";")
		// Try adding LIMIT 0 to avoid needing real parameter values.
		// Only for statements that support LIMIT (SELECT/WITH/VALUES/TABLE/FROM).
		upperDesc := strings.ToUpper(describeQuery)
		if !strings.Contains(upperDesc, "LIMIT") && describeSupportsLimit(upperDesc) {
			describeQuery = describeQuery + " LIMIT 0"
		}

		// Use NULL for all parameters
		args := make([]interface{}, ps.numParams)
		for i := range args {
			args[i] = nil
		}

		rows, err := c.executor.Query(describeQuery, args...)
		if err != nil {
			// Can't describe - send NoData
			c.logger().Debug("Describe failed to get columns.", "error", err)
			_ = wire.WriteNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		_ = rows.Close()

		if len(cols) == 0 {
			_ = wire.WriteNoData(c.writer)
			return
		}

		c.logger().Debug("Describe statement sending RowDescription.", "columns", len(cols))
		_ = c.sendRowDescription(cols, colTypes)
		ps.described = true

	case 'P':
		// Describe portal
		p, ok := c.portals[name]
		if !ok {
			// In PostgreSQL, DECLARE CURSOR creates a named cursor that is also
			// accessible as a portal. psycopg3's ServerCursor sends Describe Portal
			// with the cursor name after DECLARE. Check c.cursors as fallback.
			if _, cursorOk := c.cursors[name]; cursorOk {
				cols, colTypes, err := c.getCursorSchema(name)
				if err != nil {
					c.logger().Debug("Describe cursor-as-portal failed to open.", "cursor", name, "error", err)
					_ = wire.WriteNoData(c.writer)
					return
				}
				_ = c.sendRowDescription(cols, colTypes)
				return
			}
			c.sendError("ERROR", "34000", fmt.Sprintf("portal %q does not exist", name))
			return
		}
		// Terminal portals have released their Bind frame. Describe(P) must use
		// the compact wire metadata captured while the portal was executable,
		// never re-probe with an empty argument list.
		if p.state != portalStateReady {
			_ = c.writeCachedPortalRowDescription(p)
			return
		}

		// Handle cursor operations in portal Describe
		switch p.stmt.cursorOp {
		case cursorOpDeclare, cursorOpClose:
			_ = wire.WriteNoData(c.writer)
			return
		case cursorOpFetch:
			// MOVE advances the cursor without returning rows — NoData.
			if p.stmt.cursorIsMove {
				_ = wire.WriteNoData(c.writer)
				return
			}
			cols, colTypes, err := c.getCursorSchema(p.stmt.cursorName)
			if err != nil || len(cols) == 0 {
				_ = wire.WriteNoData(c.writer)
				return
			}
			if !c.cachePortalRowDescription(p, cols, colTypes) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			p.described = true
			_ = c.writeCachedPortalRowDescription(p)
			return
		case cursorOpPgCursorsQuery:
			if !c.validPortalResultFormats(p, 1) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			if !c.replacePortalRowDescription(p, pgCursorsRowDescriptionBody(p.resultFormats)) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			_ = c.writeCachedPortalRowDescription(p)
			p.described = true
			return
		case cursorOpPgStatActivity:
			if !c.validPortalResultFormats(p, len(pgStatActivityColumns)) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			if !c.replacePortalRowDescription(p, pgStatActivityRowDescriptionBody(p.resultFormats)) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			_ = c.writeCachedPortalRowDescription(p)
			p.described = true
			return
		}

		// For queries that don't return results, send NoData
		if !queryReturnsResults(p.stmt.query) {
			_ = wire.WriteNoData(c.writer)
			return
		}

		// DML with RETURNING cannot be described without executing the mutation.
		// Reject with an explicit error so clients don't desync.
		if isDMLReturning(p.stmt.query) {
			c.sendError("ERROR", "0A000", "DML with RETURNING clause cannot be described without executing the mutation; use simple query protocol or skip the Describe step")
			return
		}

		// WITH + DML (no RETURNING) doesn't return results but queryReturnsResults
		// returns true for all WITH-prefixed queries. Send NoData to avoid executing
		// the mutation during schema probing.
		if isWithDML(p.stmt.query) {
			_ = wire.WriteNoData(c.writer)
			return
		}

		// EXPLAIN [ANALYZE]: synthesize the single plan column without executing
		// (see the statement-Describe branch above).
		if isExplainStmt(p.stmt.query) {
			if !c.cachePortalRowDescription(p, []string{explainPlanColumn(p.stmt.query)}, []ColumnTyper{staticColumnType("VARCHAR")}) {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				return
			}
			_ = c.writeCachedPortalRowDescription(p)
			p.described = true
			p.stmt.described = true
			return
		}

		// For SELECT, we need to describe the result columns. Local executors
		// decode to database/sql arguments, while Flight receives the same
		// compact literal appender used by Execute.
		boundExecutor, usesBoundExecutor := c.executor.(sqlcore.BoundQueryExecutor)
		var args []interface{}
		if usesBoundExecutor {
			if err := p.validateBinaryParameters(); err != nil {
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
				return
			}
		} else {
			var err error
			args, err = p.decodeParams()
			if err != nil {
				// PostgreSQL returns 08P01 (protocol violation) for malformed binary data
				c.finishPortal(p, portalStateFailed, "terminal_failure")
				c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
				return
			}
		}

		// Try to get column info without fully executing expensive queries.
		describeQuery := strings.TrimRight(strings.TrimSpace(p.stmt.convertedQuery), ";")
		upperDesc := strings.ToUpper(describeQuery)
		if !strings.Contains(upperDesc, "LIMIT") && describeSupportsLimit(upperDesc) {
			describeQuery = describeQuery + " LIMIT 0"
		}

		var (
			rows RowSet
			err  error
		)
		if usesBoundExecutor {
			rows, err = boundExecutor.QueryWithBoundParams(describeQuery, p)
		} else {
			rows, err = c.executor.Query(describeQuery, args...)
		}
		if err != nil {
			// Can't describe - send NoData
			_ = wire.WriteNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		_ = rows.Close()

		if len(cols) == 0 {
			_ = wire.WriteNoData(c.writer)
			return
		}

		// Mark both portal and statement as described when we send RowDescription.
		// If we sent NoData above, Execute should still send RowDescription.
		// Setting ps.described ensures future Bind calls that create new portals
		// from this statement inherit described=true, so Execute won't re-send
		// RowDescription. Without this, JDBC drivers that reuse named statements
		// (Bind/Execute without re-Describing) get an unexpected RowDescription
		// and desync their message queue.
		if !c.cachePortalRowDescription(p, cols, colTypes) {
			c.finishPortal(p, portalStateFailed, "terminal_failure")
			return
		}
		p.described = true
		p.stmt.described = true
		_ = c.writeCachedPortalRowDescription(p)

	default:
		c.sendError("ERROR", "08P01", "invalid Describe type")
	}
}

func (c *clientConn) handleExecute(body []byte) {
	// Execute message format:
	// - Portal name (null-terminated)
	// - Maximum rows to return (int32, 0 = no limit)

	reader := bytes.NewReader(body)

	portalName, err := readCString(reader)
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Execute message")
		return
	}

	// Resolve the portal before parsing maxRows so a malformed Execute that
	// still identifies a ready portal releases its retained Bind payload rather
	// than leaving it until Sync or connection close.
	p, ok := c.portals[portalName]
	var maxRows int32
	if err := binary.Read(reader, binary.BigEndian, &maxRows); err != nil {
		c.sendError("ERROR", "08P01", "invalid Execute message")
		if ok && p.state == portalStateReady {
			c.finishPortal(p, portalStateFailed, "terminal_failure")
		}
		return
	}

	if !ok {
		c.sendError("ERROR", "34000", fmt.Sprintf("portal %q does not exist", portalName))
		return
	}
	if p.state != portalStateReady {
		c.sendError("ERROR", "55000", fmt.Sprintf("portal %q is not executable", portalName))
		return
	}
	// Duckgres has no PortalSuspended implementation, so every return below is
	// terminal. Centralizing state/release here prevents an early Execute path
	// from retaining its Bind body until a distant Sync or connection close.
	errorsBeforeExecute := c.errorResponsesSent
	defer func() {
		if c.errorResponsesSent != errorsBeforeExecute {
			c.finishPortal(p, portalStateFailed, "terminal_failure")
			return
		}
		c.finishPortal(p, portalStateDone, "terminal_success")
	}()

	// Redacted form for everything observable (pg_stat_activity, spans,
	// logs): CREATE SECRET option lists carry credential material.
	loggableQuery := usersecrets.RedactForLog(p.stmt.query)

	c.currentQuery.Store(loggableQuery)
	c.queryStart.Store(time.Now())
	defer func() {
		c.currentQuery.Store("")
		c.queryStart.Store(time.Time{})
	}()

	// Handle empty queries - PostgreSQL returns EmptyQueryResponse for these
	trimmedQuery := strings.TrimSpace(p.stmt.query)
	if trimmedQuery == "" || isEmptyQuery(trimmedQuery) {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		return
	}

	start := time.Now()
	queryMetrics := c.beginQueryMetrics(start)
	defer c.finishQueryMetrics(queryMetrics)

	// Handle cursor operations before normal execution
	switch p.stmt.cursorOp {
	case cursorOpDeclare:
		c.handleDeclareCursorExtended(p)
		return
	case cursorOpFetch:
		c.handleFetchCursorExtended(p)
		return
	case cursorOpClose:
		c.handleCloseCursorExtended(p)
		return
	case cursorOpPgCursorsQuery:
		c.handlePgCursorsQueryExtended(p)
		return
	case cursorOpPgStatActivity:
		c.handlePgStatActivityExtended(p)
		return
	}

	queryCtx, span := observe.Tracer().Start(c.ctx, "duckgres.query",
		trace.WithAttributes(
			attribute.String("duckgres.protocol", "extended"),
			attribute.String("duckgres.org_id", c.orgID),
			attribute.String("db.user", c.username),
			attribute.String("db.statement", observe.TruncateForSpan(loggableQuery)),
		),
	)
	defer span.End()

	// Local database/sql executors need []any. Flight can instead append the
	// compact portal views directly into its final SQL request buffer, avoiding
	// string(value) and interface boxing for every text parameter.
	boundExecutor, usesBoundExecutor := c.executor.(sqlcore.BoundQueryExecutor)
	var args []interface{}
	if usesBoundExecutor {
		if err := p.validateBinaryParameters(); err != nil {
			c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
			return
		}
	} else {
		args, err = p.decodeParams()
		if err != nil {
			// PostgreSQL returns 08P01 (protocol violation) for malformed binary data
			c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
			return
		}
	}

	transaction := p.stmt.transaction
	upperQuery := strings.ToUpper(strings.TrimSpace(p.stmt.query))
	cmdType := commandTypeForTransaction(c.getCommandType(upperQuery), transaction)
	returnsResults := queryReturnsResults(p.stmt.query)

	// Intercept persistent-secret DDL (multitenant remote backend): persist /
	// delete the user's stored secret alongside the session-side DDL. Uses
	// the original (untranspiled) text — secret DDL is DuckDB-native and
	// always falls back unmodified. ReadyForQuery is sent by Sync.
	if c.handleUserSecretDDLExtended(p.stmt.query) {
		return
	}

	c.logger().Debug("Execute portal.", "portal", portalName, "params", p.BindParameterCount(), "query", loggableQuery)

	// duckgres.query_source custom GUC (SET / SHOW): intercepted session-side,
	// never forwarded to DuckDB. Determined by the transpiler during Parse.
	if p.stmt.querySourceSet != nil {
		c.setQuerySource(*p.stmt.querySourceSet)
		c.logger().Debug("Set duckgres.query_source.", "value", c.QuerySource())
		_ = c.writeCommandComplete("SET")
		return
	}
	if p.stmt.querySourceShow {
		if !c.cachePortalRowDescription(p, []string{querySourceGUCName}, []ColumnTyper{staticColumnType("VARCHAR")}) {
			return
		}
		if !p.described {
			_ = c.writeCachedPortalRowDescription(p)
		}
		_ = c.sendDataRowWithFormats([]interface{}{c.QuerySource()}, p.resultFormats, nil)
		_ = c.writeCommandComplete("SHOW")
		return
	}

	// Check if this is a PostgreSQL-specific SET command that should be ignored
	// (determined by transpiler during Parse)
	if p.stmt.isIgnoredSet {
		c.logger().Debug("Ignoring PostgreSQL-specific SET.", "query", p.stmt.query)
		_ = c.writeCommandComplete("SET")
		return
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.) - DuckLake doesn't support these
	// (determined by transpiler during Parse)
	if p.stmt.isNoOp {
		c.logger().Debug("No-op command (DuckLake limitation).", "query", p.stmt.query)
		_ = c.writeCommandComplete(p.stmt.noOpTag)
		return
	}

	// Handle multi-statement results (e.g., writable CTE rewrites)
	if len(p.stmt.statements) > 0 {
		c.logger().Debug("Execute multi-statement.", "statements", len(p.stmt.statements), "cleanup", len(p.stmt.cleanupStatements))
		c.executeMultiStatementExtended(p, p.stmt.statements, p.stmt.cleanupStatements, args, p.resultFormats, p.described)
		return
	}

	originalQuery := p.stmt.query
	convertedQuery := p.stmt.convertedQuery

	// Lifecycle log pair for the extended-query path. logQueryStarted /
	// logQueryFinished are the canonical "did a query run on a worker?"
	// signal for Loki / Grafana (PR #519). Without these, the only log a
	// successful extended-query produces is the structured logQuery() to
	// the queryLogger channel, which doesn't carry worker_id and isn't
	// scrape-friendly. queryFinalErr is captured by the deferred call so
	// every termination path — success, ALTER-TABLE-as-VIEW retry,
	// transaction-conflict retry, recovery rollback, fatal error — emits
	// exactly one Finished log per Started.
	queryStart := time.Now()
	var queryRowsAff int64
	var queryFinalErr error
	c.logQueryStarted(convertedQuery)
	defer func() {
		c.logQueryFinished(convertedQuery, queryStart, queryRowsAff, queryFinalErr)
	}()

	if !returnsResults {
		// Handle nested BEGIN: PostgreSQL issues a warning but continues,
		// while DuckDB throws an error. Match PostgreSQL behavior.
		if transaction.begins() && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = c.writeCommandComplete("BEGIN")
			return
		}

		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(transaction)

		// Non-result-returning query: use Exec with converted query
		runExec := func() (ExecResult, error) {
			var result ExecResult
			var err error
			if usesBoundExecutor {
				result, err = boundExecutor.ExecWithBoundParams(convertedQuery, p)
			} else {
				result, err = c.executor.Exec(convertedQuery, args...)
			}
			if err != nil {
				if fallbackResult, handled, fallbackErr := c.execCompatibilityFallback(convertedQuery, err, func(fallbackQuery string) (ExecResult, error) {
					if usesBoundExecutor {
						return boundExecutor.ExecWithBoundParams(fallbackQuery, p)
					}
					return c.executor.Exec(fallbackQuery, args...)
				}); handled {
					return fallbackResult, fallbackErr
				}
			}
			return result, err
		}

		execStart := time.Now()
		execCtx, execSpan := observe.Tracer().Start(queryCtx, "duckgres.execute")
		result, err := runExec()
		c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
		execSpan.End()
		if err != nil {
			if c.txStatus == txStatusIdle && isDuckLakeTransactionConflict(err) {
				ducklakeConflictTotal.Inc()
				result, err = retryOnConflict(runExec)
			}
			if err != nil {
				result, err, _ = recoverAbortedTransaction(
					err,
					c.txStatus == txStatusIdle,
					func() error {
						_, rollbackErr := c.executor.ExecContext(context.Background(), "ROLLBACK")
						return rollbackErr
					},
					runExec,
				)
			}
			if err != nil {
				queryFinalErr = err
				errCode := classifyErrorCode(err)
				errMsg := err.Error()
				if c.isCallerCancellation(err) {
					errMsg = "canceling statement due to user request"
				} else {
					c.logQueryError(convertedQuery, err)
				}
				c.sendError("ERROR", errCode, errMsg)
				c.setTxError()
				c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, errCode, errMsg, "extended")
				return
			}
		}
		var writtenRows int64
		if result != nil {
			writtenRows, _ = result.RowsAffected()
		}
		queryRowsAff = writtenRows
		c.updateTxStatus(transaction)
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, writtenRows, "", "", "extended")
		return
	}

	// Result-returning query: use Query with converted query
	runQuery := func() (RowSet, error) {
		if usesBoundExecutor {
			return boundExecutor.QueryWithBoundParams(convertedQuery, p)
		}
		return c.executor.Query(convertedQuery, args...)
	}

	execStart := time.Now()
	execCtx, execSpan := observe.Tracer().Start(queryCtx, "duckgres.execute")
	rows, err := runQuery()
	if err != nil && c.txStatus == txStatusIdle && isDuckLakeTransactionConflict(err) {
		ducklakeConflictTotal.Inc()
		rows, err = retryOnConflict(runQuery)
	}
	if err != nil {
		rows, err, _ = recoverAbortedTransaction(
			err,
			c.txStatus == txStatusIdle,
			func() error {
				_, rollbackErr := c.executor.ExecContext(context.Background(), "ROLLBACK")
				return rollbackErr
			},
			runQuery,
		)
	}
	c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
	execSpan.End()
	if err != nil {
		queryFinalErr = err
		errCode := classifyErrorCode(err)
		errMsg := err.Error()
		if c.isCallerCancellation(err) {
			errMsg = "canceling statement due to user request"
		} else {
			c.logQueryError(convertedQuery, err)
		}
		c.sendError("ERROR", errCode, errMsg)
		c.setTxError()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, errCode, errMsg, "extended")
		return
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		queryFinalErr = err
		c.logger().Error("Columns error.", "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, "42000", err.Error(), "extended")
		return
	}

	// Get column types for binary encoding
	colTypes, _ := rows.ColumnTypes()
	// Cache serialized portal metadata even when Describe(S) made
	// p.described true and Execute therefore suppresses RowDescription.
	if !c.cachePortalRowDescription(p, cols, colTypes) {
		return
	}
	typeOIDs := make([]int32, len(cols))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send RowDescription if Describe wasn't called before Execute.
	// Some clients skip Describe and go straight to Execute, but still
	// need the column metadata before receiving data rows.
	// Skip if there are no columns - queries that return 0 columns (like
	// DDL accidentally routed here) don't need RowDescription.
	if !p.described && len(cols) > 0 {
		if err := c.writeCachedPortalRowDescription(p); err != nil {
			return
		}
	}

	// Send rows with the format codes from Bind
	rowCount := 0
	for rows.Next() {
		if maxRows > 0 && int32(rowCount) >= maxRows {
			// Portal suspended - but we don't support this yet
			break
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			queryFinalErr = err
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, "42000", err.Error(), "extended")
			return
		}

		if err := c.sendDataRowWithFormats(values, p.resultFormats, typeOIDs); err != nil {
			queryFinalErr = err
			return
		}
		rowCount++
	}
	queryRowsAff = int64(rowCount)

	if err := rows.Err(); err != nil {
		queryFinalErr = err
		errCode := "42000"
		errMsg := err.Error()
		if c.isCallerCancellation(err) {
			errCode = "57014"
			errMsg = "canceling statement due to user request"
			c.sendError("ERROR", errCode, errMsg)
		} else {
			c.logger().Error("Row iteration error.", "error", err)
			c.sendError("ERROR", errCode, errMsg)
		}
		c.setTxError()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, errCode, errMsg, "extended")
		return
	}

	c.updateTxStatus(transaction)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	c.logQuery(start, originalQuery, convertedQuery, cmdType, int64(rowCount), 0, "", "", "extended")
}

func (c *clientConn) handleClose(body []byte) {
	// Close message format:
	// - Type: 'S' for statement, 'P' for portal
	// - Name (null-terminated)

	if len(body) < 2 {
		c.sendError("ERROR", "08P01", "invalid Close message")
		return
	}

	closeType := body[0]
	name := string(bytes.TrimRight(body[1:], "\x00"))

	switch closeType {
	case 'S':
		stmt := c.stmts[name]
		// Closing a statement also closes all portals derived from that
		// statement, including portals whose statement was subsequently
		// re-Parsed under the same name.
		c.dropPortalsForStatement(stmt, name, "close_statement")
		if stmt != nil {
			delete(c.stmts, name)
		}
	case 'P':
		c.dropPortal(name, "close_portal")
	default:
		c.sendError("ERROR", "08P01", "invalid Close type")
		return
	}

	_ = wire.WriteCloseComplete(c.writer)
}

func (c *clientConn) sendParameterDescription(paramTypes []int32) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		// If OID is 0, use text type
		if oid == 0 {
			oid = 25 // text
		}
		_ = binary.Write(&buf, binary.BigEndian, oid)
	}
	_ = wire.WriteMessage(c.writer, 't', buf.Bytes())
}

// readCString reads a null-terminated string from reader
func readCString(r *bytes.Reader) (string, error) {
	var buf bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			break
		}
		buf.WriteByte(b)
	}
	return buf.String(), nil
}

// runExtendedQueryMessage dispatches an extended-query protocol message
// (Parse/Bind/Describe/Execute/Close), implementing the protocol's error
// recovery rule: after an error while processing any extended-query message
// the server must discard subsequent extended-protocol messages until Sync
// arrives. Without this, pipelined clients (libpq pipeline mode, pgx
// SendBatch, JDBC batch) execute queued messages against broken state and
// desync their response accounting.
//
// An error is detected by observing sendError — the single ErrorResponse
// funnel for an established connection — so deep failure paths inside Execute
// arm the skip too. The trigger is deliberately the error event itself, NOT
// txStatus == txStatusError: an aborted transaction must still accept the
// Parse/Bind/Execute of a ROLLBACK sent after Sync.
func (c *clientConn) runExtendedQueryMessage(handler func([]byte), body []byte) {
	if c.ignoreTillSync {
		return
	}
	before := c.errorResponsesSent
	handler(body)
	if c.errorResponsesSent != before {
		c.ignoreTillSync = true
	}
}

func (c *clientConn) handleBind(body []byte) {
	// Bind message format:
	// - Portal name (null-terminated)
	// - Statement name (null-terminated)
	// - Number of parameter format codes (int16)
	// - Parameter format codes (int16 each)
	// - Number of parameter values (int16)
	// - Parameter values (length int32, then data)
	// - Number of result format codes (int16)
	// - Result format codes (int16 each)

	reader := bindFrameReader{body: body}
	portalName, err := reader.readCString()
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	stmtName, err := reader.readCString()
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}

	ps, ok := c.stmts[stmtName]
	if !ok {
		c.sendError("ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", stmtName))
		return
	}

	numParamFormats, err := reader.readInt16()
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numParamFormats < 0 {
		c.sendError("ERROR", "08P01", "invalid parameter format count in Bind message")
		return
	}
	var paramFormats []int16
	if numParamFormats > 0 {
		paramFormats = make([]int16, int(numParamFormats))
		for i := range paramFormats {
			format, err := reader.readInt16()
			if err != nil {
				c.sendError("ERROR", "08P01", "invalid Bind message")
				return
			}
			if format != 0 && format != 1 {
				c.sendError("ERROR", "08P01", "invalid parameter format code in Bind message")
				return
			}
			paramFormats[i] = format
		}
	}

	numParams, err := reader.readInt16()
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numParams < 0 {
		c.sendError("ERROR", "08P01", "invalid parameter count in Bind message")
		return
	}
	if int(numParams) != ps.numParams {
		c.sendError("ERROR", "08P01", fmt.Sprintf("bind message supplies %d parameters, but prepared statement %q requires %d", numParams, stmtName, ps.numParams))
		return
	}
	if len(paramFormats) != 0 && len(paramFormats) != 1 && len(paramFormats) != int(numParams) {
		c.sendError("ERROR", "08P01", "invalid parameter format count in Bind message")
		return
	}

	params := make([]bindParam, int(numParams))
	for i := range params {
		length, err := reader.readInt32()
		if err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
		if length == -1 {
			params[i].length = -1
			continue
		}
		if length < 0 {
			// Only -1 (NULL) is a valid negative length. This preserves the
			// #717/#720 malformed-length behavior without allocating per value.
			c.sendError("ERROR", "08P01", "invalid parameter length in Bind message")
			return
		}
		if int64(length) > int64(reader.remaining()) {
			c.sendError("ERROR", "08P01", fmt.Sprintf("invalid Bind message: parameter %d length %d exceeds remaining message size %d", i+1, length, reader.remaining()))
			return
		}
		params[i] = bindParam{offset: int32(reader.pos), length: length}
		reader.pos += int(length)
	}

	numResultFormats, err := reader.readInt16()
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numResultFormats < 0 {
		c.sendError("ERROR", "08P01", "invalid result format count in Bind message")
		return
	}
	var resultFormats []int16
	if numResultFormats > 0 {
		resultFormats = make([]int16, int(numResultFormats))
		for i := range resultFormats {
			format, err := reader.readInt16()
			if err != nil {
				c.sendError("ERROR", "08P01", "invalid Bind message")
				return
			}
			if format != 0 && format != 1 {
				c.sendError("ERROR", "08P01", "invalid result format code in Bind message")
				return
			}
			resultFormats[i] = format
		}
	}
	if reader.remaining() != 0 {
		c.sendError("ERROR", "08P01", "invalid Bind message: trailing data")
		return
	}
	// For ordinary read-only queries, a multi-code result-format vector is
	// validated once their row metadata is available during Describe or Execute.
	// DML RETURNING and transpiled multi-statement writes cannot safely be
	// probed first: executing to discover their output cardinality could mutate
	// data before a malformed Bind is rejected. Validate every known cardinality
	// here and conservatively reject an ambiguous multi-code vector instead.
	if !bindResultFormatCountSafeBeforeExecute(ps, len(resultFormats)) {
		c.sendError("ERROR", "08P01", "invalid result format count in Bind message")
		return
	}

	// PostgreSQL replaces only the unnamed portal. A duplicate named portal is
	// an error and must leave the original portal/accounting untouched.
	existing := c.portals[portalName]
	if portalName != "" && existing != nil {
		c.sendError("ERROR", "42P03", fmt.Sprintf("portal %q already exists", portalName))
		return
	}

	incomingRetainedBytes := retainedBindStorageBytes(len(body), len(params), len(paramFormats), len(resultFormats)) + retainedPortalNameBytes(portalName, stmtName)
	projectedBytes := c.retainedBindBytes + int64(incomingRetainedBytes)
	if portalName == "" && existing != nil {
		projectedBytes -= int64(existing.retainedStorageBytes())
	}
	if projectedBytes > c.maxRetainedBindBytes() {
		observe.IncPortalBudgetRejection("retained_bytes")
		c.sendError("ERROR", "54000", "retained Bind portal byte budget exceeded")
		return
	}
	projectedPortals := len(c.portals)
	if existing == nil {
		projectedPortals++
	}
	if projectedPortals > c.maxOpenPortals() {
		observe.IncPortalBudgetRejection("open_portals")
		c.sendError("ERROR", "54000", "open portal budget exceeded")
		return
	}

	if portalName == "" && existing != nil {
		c.dropPortal(portalName, "unnamed_rebind")
	}
	c.installPortal(portalName, &portal{
		stmt:              ps,
		stmtName:          stmtName,
		retainedNameBytes: retainedPortalNameBytes(portalName, stmtName),
		bindBody:          body,
		params:            params,
		paramFormats:      paramFormats,
		resultFormats:     resultFormats,
		described:         ps.described, // Inherit from statement Describe state.
		state:             portalStateReady,
	})

	_ = wire.WriteBindComplete(c.writer)
}

// bindResultFormatCountSafeBeforeExecute reports whether a Bind result-format
// vector can be accepted without risking execution of a mutation merely to
// learn its result-column count. PostgreSQL allows zero formats, one format,
// or exactly one per result column.
func bindResultFormatCountSafeBeforeExecute(ps *preparedStmt, formats int) bool {
	if formats <= 1 {
		return true
	}
	if ps == nil {
		return false
	}
	// A non-MOVE FETCH returns the columns of a previously declared cursor.
	// Its schema is not stored on the FETCH prepared statement, but becomes
	// available during Describe or Execute before any cursor row is advanced.
	// Defer an exact multi-code count check to that point. MOVE has no result
	// columns and therefore cannot accept a multi-code vector.
	if ps.cursorOp == cursorOpFetch {
		return !ps.cursorIsMove
	}
	if !queryReturnsResults(ps.query) {
		return false
	}
	// EXPLAIN always returns one plan column. In particular, EXPLAIN ANALYZE
	// may execute a wrapped write, so its cardinality must be rejected here.
	if isExplainStmt(ps.query) {
		return false
	}
	// Writable-CTE rewrites run setup writes before their final result query.
	// Their output cardinality is not safely knowable at Bind time, so reject a
	// multi-code vector rather than run the setup and discover it too late.
	if len(ps.statements) > 0 {
		return false
	}
	if isDMLReturning(ps.query) {
		columns, known := explicitDMLReturningColumnCount(ps.query)
		return known && formats == columns
	}
	return true
}

// explicitDMLReturningColumnCount returns a cardinality only when the original
// DML RETURNING list is a set of explicit targets. A wildcard depends on the
// table schema, so callers must not execute a mutation merely to resolve it.
func explicitDMLReturningColumnCount(query string) (int, bool) {
	tree, err := pg_query.Parse(query)
	if err != nil || len(tree.Stmts) != 1 || tree.Stmts[0] == nil || tree.Stmts[0].Stmt == nil {
		return 0, false
	}

	var targets []*pg_query.Node
	switch stmt := tree.Stmts[0].Stmt.Node.(type) {
	case *pg_query.Node_InsertStmt:
		if stmt.InsertStmt != nil {
			targets = stmt.InsertStmt.ReturningList
		}
	case *pg_query.Node_UpdateStmt:
		if stmt.UpdateStmt != nil {
			targets = stmt.UpdateStmt.ReturningList
		}
	case *pg_query.Node_DeleteStmt:
		if stmt.DeleteStmt != nil {
			targets = stmt.DeleteStmt.ReturningList
		}
	default:
		return 0, false
	}
	if len(targets) == 0 {
		return 0, false
	}
	for _, targetNode := range targets {
		target := targetNode.GetResTarget()
		if target == nil || resultTargetHasWildcard(target) {
			return 0, false
		}
	}
	return len(targets), true
}

func resultTargetHasWildcard(target *pg_query.ResTarget) bool {
	if resultTargetNodeHasWildcard(target.Val) {
		return true
	}
	for _, indirection := range target.Indirection {
		if resultTargetNodeHasWildcard(indirection) {
			return true
		}
	}
	return false
}

// resultTargetNodeHasWildcard follows the AST shapes that expand one target
// into several output columns: `*`, `table.*`, and `(composite_expr).*`.
// Function calls such as count(*) remain a single explicit output target.
func resultTargetNodeHasWildcard(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if node.GetAStar() != nil {
		return true
	}
	if column := node.GetColumnRef(); column != nil {
		for _, field := range column.Fields {
			if field.GetAStar() != nil {
				return true
			}
		}
	}
	if indirection := node.GetAIndirection(); indirection != nil {
		if resultTargetNodeHasWildcard(indirection.Arg) {
			return true
		}
		for _, part := range indirection.Indirection {
			if resultTargetNodeHasWildcard(part) {
				return true
			}
		}
	}
	return false
}

// bindFrameReader parses an already framed Bind body without allocating a
// separate []byte for every parameter value. Its offsets are retained by the
// portal as compact descriptors into the immutable body.
type bindFrameReader struct {
	body []byte
	pos  int
}

func (r *bindFrameReader) remaining() int { return len(r.body) - r.pos }

func (r *bindFrameReader) readCString() (string, error) {
	start := r.pos
	for r.pos < len(r.body) {
		if r.body[r.pos] == 0 {
			value := string(r.body[start:r.pos])
			r.pos++
			return value, nil
		}
		r.pos++
	}
	return "", fmt.Errorf("unterminated cstring")
}

func (r *bindFrameReader) readInt16() (int16, error) {
	if r.remaining() < 2 {
		return 0, fmt.Errorf("short int16")
	}
	value := int16(binary.BigEndian.Uint16(r.body[r.pos:]))
	r.pos += 2
	return value, nil
}

func (r *bindFrameReader) readInt32() (int32, error) {
	if r.remaining() < 4 {
		return 0, fmt.Errorf("short int32")
	}
	value := int32(binary.BigEndian.Uint32(r.body[r.pos:]))
	r.pos += 4
	return value, nil
}
