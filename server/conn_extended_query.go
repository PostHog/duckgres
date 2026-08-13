package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/server/observe"
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
	if err := c.queryAccessPolicy.Authorize(query); err != nil {
		c.observeExtendedParseQueryError("42501", err.Error())
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
	// Exploratory tier: classify once, here, off the tree just parsed —
	// Describe and Execute escalate off the small worker before a pinning
	// statement reaches the executor, without re-parsing per Execute.
	pinsWorker := classifyParsedTier(cursorTree, cursorParseErr) == tierPinning
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
				pinsWorker:     pinsWorker,
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
				pinsWorker:     pinsWorker,
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
				pinsWorker:     pinsWorker,
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
			pinsWorker:     pinsWorker,
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
			pinsWorker:     pinsWorker,
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
			query:           query,
			transpiledQuery: query, // No transpilation or execution rewrite
			convertedQuery:  query,
			paramTypes:      paramTypes,
			numParams:       paramCount,
			pinsWorker:      pinsWorker,
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
		// Lazy activation: validateWithDuckDB EXPLAINs on the engine, and Parse
		// runs above every tier hook. pinsWorker is true here by construction (a
		// parse failure classifies as pinning), so this is the statement's single
		// acquire. A failure is connection-fatal and rides out on c.fatalErr,
		// since this handler cannot return one.
		if err := c.activateForStatement(query, pinsWorker); err != nil {
			return
		}
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
		transpiledQuery:   result.SQL,                       // Before direct execution rewrites
		convertedQuery:    c.rewriteDirectQuery(result.SQL), // Transpiled SQL for execution
		paramTypes:        paramTypes,
		numParams:         result.ParamCount,
		isIgnoredSet:      result.IsIgnoredSet,
		isNoOp:            result.IsNoOp,
		noOpTag:           result.NoOpTag,
		querySourceSet:    result.QuerySourceSet,    // SET duckgres.query_source (custom GUC)
		querySourceShow:   result.QuerySourceShow,   // SHOW duckgres.query_source
		s3CacheSet:        result.S3CacheSet,        // SET duckgres.s3_cache (custom GUC)
		s3CacheShow:       result.S3CacheShow,       // SHOW duckgres.s3_cache
		statements:        result.Statements,        // Multi-statement rewrite (writable CTE)
		cleanupStatements: result.CleanupStatements, // Cleanup statements
		pinsWorker:        pinsWorker,               // Exploratory tier: escalate before Describe/Execute
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

		// duckgres.s3_cache custom GUC: same shape as query_source above.
		if ps.s3CacheSet != nil {
			_ = wire.WriteNoData(c.writer)
			return
		}
		if ps.s3CacheShow {
			_ = c.sendRowDescription([]string{s3CacheGUCName}, []ColumnTyper{staticColumnType("VARCHAR")})
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

		// Exploratory tier: the probe below EXECUTES the statement, so a
		// pinning one must escalate first — LIMIT 0 bounds the rows, not the
		// side effects (`SELECT … INTO t2` returns results by prefix, is not
		// DML-RETURNING, and creates a table when probed). The intercepts above
		// already answered every statement the CP handles itself.
		if err := c.escalateForPinningTier(ps.query, ps.pinsWorker); err != nil {
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
			p.described = true
			_ = c.sendRowDescriptionWithFormats(cols, colTypes, p.resultFormats)
			return
		case cursorOpPgCursorsQuery:
			_ = c.sendPgCursorsRowDescriptionWithFormats(p.resultFormats)
			p.described = true
			return
		case cursorOpPgStatActivity:
			_ = c.sendPgStatActivityRowDescriptionWithFormats(p.resultFormats)
			p.described = true
			return
		}

		// duckgres-namespaced custom GUCs (query_source, s3_cache): answered
		// from session state, never probed against DuckDB (which does not know
		// these settings — the LIMIT-0 probe below would just fail and degrade
		// to NoData). SET returns no rows; SHOW returns a single text column.
		if p.stmt.querySourceSet != nil || p.stmt.s3CacheSet != nil {
			_ = wire.WriteNoData(c.writer)
			return
		}
		if p.stmt.querySourceShow {
			_ = c.sendRowDescriptionWithFormats([]string{querySourceGUCName}, []ColumnTyper{staticColumnType("VARCHAR")}, p.resultFormats)
			p.described = true
			return
		}
		if p.stmt.s3CacheShow {
			_ = c.sendRowDescriptionWithFormats([]string{s3CacheGUCName}, []ColumnTyper{staticColumnType("VARCHAR")}, p.resultFormats)
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
			_ = c.sendRowDescriptionWithFormats([]string{explainPlanColumn(p.stmt.query)}, []ColumnTyper{staticColumnType("VARCHAR")}, p.resultFormats)
			p.described = true
			p.stmt.described = true
			return
		}

		// Exploratory tier: as in the statement-Describe branch above, the
		// LIMIT-0 probe really executes the statement, so a pinning one
		// escalates first.
		if err := c.escalateForPinningTier(p.stmt.query, p.stmt.pinsWorker); err != nil {
			return
		}

		// For SELECT, we need to describe the result columns
		// We'll do a trial query with LIMIT 0 to get column info
		args, err := p.decodeParams()
		if err != nil {
			// PostgreSQL returns 08P01 (protocol violation) for malformed binary data
			c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
			return
		}

		// Try to get column info without fully executing expensive queries.
		describeQuery := strings.TrimRight(strings.TrimSpace(p.stmt.convertedQuery), ";")
		upperDesc := strings.ToUpper(describeQuery)
		if !strings.Contains(upperDesc, "LIMIT") && describeSupportsLimit(upperDesc) {
			describeQuery = describeQuery + " LIMIT 0"
		}

		rows, err := c.executor.Query(describeQuery, args...)
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
		p.described = true
		p.stmt.described = true
		_ = c.sendRowDescriptionWithFormats(cols, colTypes, p.resultFormats)

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

	var maxRows int32
	if err := binary.Read(reader, binary.BigEndian, &maxRows); err != nil {
		c.sendError("ERROR", "08P01", "invalid Execute message")
		return
	}

	p, ok := c.portals[portalName]
	if !ok {
		c.sendError("ERROR", "34000", fmt.Sprintf("portal %q does not exist", portalName))
		return
	}

	// Continuation of a suspended portal: resume streaming from the open
	// rowset. The query must NOT re-run — the client is fetching the next
	// page of the same result set.
	if p.exec != nil {
		c.resumeSuspendedPortal(p, maxRows)
		return
	}

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
	queryMetrics.queryText = loggableQuery
	defer c.finishQueryMetrics(queryMetrics)

	queryCtx, span := observe.Tracer().Start(c.ctx, "duckgres.query",
		trace.WithAttributes(
			attribute.String("duckgres.query_id", queryMetrics.queryID),
			attribute.String("duckgres.protocol", "extended"),
			attribute.String("duckgres.org_id", c.orgID),
			attribute.String("db.user", c.username),
			attribute.String("db.statement", observe.TruncateForSpan(loggableQuery)),
		),
	)
	defer span.End()
	prevCtx := c.ctx
	c.ctx = queryCtx
	defer func() { c.ctx = prevCtx }()
	c.logClientQueryReceived(queryCtx, "extended", p.stmt.query)

	// Handle cursor operations before normal execution
	switch p.stmt.cursorOp {
	case cursorOpDeclare:
		// Same contract as the simple-protocol DECLARE (handleQuery): this
		// case returns above the general pin hook below, so without a hook of
		// its own the cursor's worker-side RowSet would open on the
		// exploratory worker and a later pinning statement would strand it.
		// FETCH/CLOSE need no hook — an open cursor proves its DECLARE already
		// pinned this connection. A failed escalation is connection-fatal; the
		// error is parked on c.fatalErr for runExtendedQueryMessage.
		if err := c.escalateForPinningTier(p.stmt.query, p.stmt.pinsWorker); err != nil {
			return
		}
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

	// Convert parameter values to interface{}, handling binary format
	args, err := p.decodeParams()
	if err != nil {
		// PostgreSQL returns 08P01 (protocol violation) for malformed binary data
		c.sendError("ERROR", "08P01", fmt.Sprintf("insufficient data left in message: %v", err))
		return
	}

	upperQuery := strings.ToUpper(strings.TrimSpace(p.stmt.query))
	cmdType := c.getCommandType(upperQuery)
	returnsResults := queryReturnsResults(p.stmt.query)

	// Secret DDL creates worker-side state and the interception below executes
	// it above the general pin hook, so the exploratory tier escalates first
	// (connection-fatal on failure, parked on c.fatalErr). See
	// escalateForSecretDDL.
	if err := c.escalateForSecretDDL(p.stmt.query); err != nil {
		return
	}

	// Intercept persistent-secret DDL (multitenant remote backend): persist /
	// delete the user's stored secret alongside the session-side DDL. Uses
	// the original (untranspiled) text — secret DDL is DuckDB-native and
	// always falls back unmodified. ReadyForQuery is sent by Sync.
	if c.handleUserSecretDDLExtended(p.stmt.query) {
		return
	}

	c.logger().Debug("Execute portal.", "portal", portalName, "params", len(args), "query", loggableQuery)

	// duckgres.query_source custom GUC (SET / SHOW): intercepted session-side,
	// never forwarded to DuckDB. Determined by the transpiler during Parse.
	if p.stmt.querySourceSet != nil {
		c.setQuerySource(*p.stmt.querySourceSet)
		c.logger().Debug("Set duckgres.query_source.", "value", c.QuerySource())
		_ = c.writeCommandComplete("SET")
		return
	}
	if p.stmt.querySourceShow {
		if !p.described {
			_ = c.sendRowDescription([]string{querySourceGUCName}, []ColumnTyper{staticColumnType("VARCHAR")})
		}
		_ = c.sendDataRowWithFormats([]interface{}{c.QuerySource()}, p.resultFormats, nil)
		_ = c.writeCommandComplete("SHOW")
		return
	}

	// duckgres.s3_cache custom GUC (SET / SHOW): intercepted session-side,
	// applied via the worker transport swap. Determined by the transpiler
	// during Parse. A failed swap errors the Execute so the session state
	// never diverges from the worker's actual transport.
	if p.stmt.s3CacheSet != nil {
		// Lazy activation: the swap needs a worker to apply to (see the matching
		// site in handleQuery). Not pinning, so the exploratory tier is enough.
		if err := c.activateForStatement(p.stmt.query, false); err != nil {
			return
		}
		if err := c.applyS3CacheSetting(*p.stmt.s3CacheSet); err != nil {
			c.sendError("ERROR", "XX000", err.Error())
			return
		}
		_ = c.writeCommandComplete("SET")
		return
	}
	if p.stmt.s3CacheShow {
		// Lazy activation before answering, only if a connect-time option is
		// still pending: see the matching site in handleQuery.
		if err := c.activateForS3CacheShow(p.stmt.query); err != nil {
			return
		}
		if !p.described {
			_ = c.sendRowDescription([]string{s3CacheGUCName}, []ColumnTyper{staticColumnType("VARCHAR")})
		}
		_ = c.sendDataRowWithFormats([]interface{}{c.s3CacheValue()}, p.resultFormats, nil)
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

	// Exploratory tier: a statement that writes or creates session state must
	// run on (and pin) a normal-size worker, so escalate BEFORE execution and
	// keep the small worker stateless by construction. Mirrors the simple-query
	// hook in handleQuery, including its position: the interpreted statements
	// the CP answers itself (cursor / pg_stat_activity / secret DDL / GUC /
	// ignored-SET / no-op) returned above and never reach here, and this sits
	// above the writable-CTE rewrite branch because that branch runs the
	// embedded DML on the worker. Classification came from Parse. A failed
	// escalation is connection-fatal — the previous session is already gone —
	// and rides out on c.fatalErr, since this handler cannot return one.
	if err := c.escalateForPinningTier(p.stmt.query, p.stmt.pinsWorker); err != nil {
		return
	}

	// Handle multi-statement results (e.g., writable CTE rewrites)
	if len(p.stmt.statements) > 0 {
		c.logger().Debug("Execute multi-statement.", "statements", len(p.stmt.statements), "cleanup", len(p.stmt.cleanupStatements))
		c.executeMultiStatementExtended(p.stmt.statements, p.stmt.cleanupStatements, args, p.resultFormats, p.described)
		return
	}

	originalQuery := p.stmt.query
	transpiledQuery := p.stmt.transpiledQuery
	convertedQuery := p.stmt.convertedQuery
	if transpiledQuery == "" {
		// Preserve the existing classification for preparedStmt values built by
		// tests and internal helpers that predate the explicit three-stage form.
		transpiledQuery = convertedQuery
	}
	if !returnsResults && cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
		c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
		_ = c.writeCommandComplete("BEGIN")
		return
	}

	workerOrigin := workerOriginForQueries(originalQuery, transpiledQuery, convertedQuery)
	workerOperation := workerOperationExecute
	if returnsResults {
		workerOperation = workerOperationSelect
	}
	workerStatement := workerStatementForQuery(workerOrigin, workerOperation, convertedQuery)
	queryStart := time.Now()
	var queryRowsAff int64
	var queryFinalErr error
	c.logWorkerStatementStarted(workerStatement)
	defer func() {
		c.logWorkerStatementFinished(workerStatement, queryStart, queryRowsAff, queryFinalErr)
	}()

	if !returnsResults {
		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(cmdType)

		// Non-result-returning query: use Exec with converted query
		runExec := func() (ExecResult, error) {
			result, err := c.executor.Exec(convertedQuery, args...)
			if err != nil {
				if fallbackResult, handled, fallbackErr := c.execCompatibilityFallback(convertedQuery, err, func(fallbackQuery string) (ExecResult, error) {
					return c.runGeneratedWorkerStatement(
						generatedWorkerStatement(workerOriginRewrite, workerOperationCompatibilityFallback),
						func() (ExecResult, error) { return c.executor.Exec(fallbackQuery, args...) },
					)
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
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, writtenRows, "", "", "extended")
		return
	}

	// Result-returning query: use Query with converted query
	runQuery := func() (RowSet, error) {
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
	// Exploratory tier: a read that blew the small worker's memory_limit is
	// transparently re-executed on a normal-size worker. Prepare phase only —
	// nothing has been sent to the client yet, so the retry is invisible. Never
	// inside a transaction: the new worker has none of its accumulated state.
	// runQuery reads c.executor at call time, so it targets the new worker.
	// Same contract as executeSelectQuery; a failed escalation surfaces the
	// ORIGINAL query error as FATAL and terminates the connection.
	if err != nil && c.onExploratoryWorker && isWorkerOutOfMemoryError(err) && c.txStatus == txStatusIdle {
		if escErr := c.escalateWorker(queryCtx, escalateReasonOOM); escErr != nil {
			queryFinalErr = err
			execSpan.End()
			_ = c.failEscalation(convertedQuery, escErr, err.Error())
			return
		}
		rows, err = runQuery()
	}
	if err != nil {
		c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
		execSpan.End()
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
	keepRowsOpen := false
	profilingFinished := false
	finishProfiling := func() {
		if profilingFinished {
			return
		}
		profilingFinished = true
		c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
		execSpan.End()
	}
	finishRows := func() {
		if keepRowsOpen {
			return
		}
		_ = rows.Close()
		finishProfiling()
	}
	defer func() {
		finishRows()
	}()

	cols, err := rows.Columns()
	if err != nil {
		queryFinalErr = err
		c.logger().Error("Columns error.", "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		finishRows()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, "42000", err.Error(), "extended")
		return
	}

	// Get column types for binary encoding
	colTypes, _ := rows.ColumnTypes()
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
		if err := c.sendRowDescriptionWithFormats(cols, colTypes, p.resultFormats); err != nil {
			return
		}
	}

	// Send rows with the format codes from Bind. Shared with the simple-query
	// path so the exploratory tier's zero-row retry below behaves identically
	// on both protocols; the RowDescription was already handled above. maxRows
	// caps the DataRows sent, and reaching it suspends the portal
	// (stream.limitReached below) instead of completing it.
	activeRows := rows
	stream := c.streamSelectRows(rows, cols, colTypes, typeOIDs, false, p.resultFormats, maxRows)

	// Exploratory tier: an OOM raised before a SINGLE DataRow reached the
	// client is still re-executable on a normal-size worker — all the client
	// has seen is the RowDescription, which the identical query on the same
	// engine reproduces exactly, so it is deliberately NOT resent. Once rows
	// are out the door the error must surface: a retry cannot un-send them.
	if stream.rowsErr != nil && stream.rowsSent == 0 &&
		c.onExploratoryWorker && isWorkerOutOfMemoryError(stream.rowsErr) && c.txStatus == txStatusIdle {
		oomErr := stream.rowsErr
		_ = rows.Close()
		if escErr := c.escalateWorker(queryCtx, escalateReasonOOM); escErr != nil {
			queryFinalErr = oomErr
			_ = c.failEscalation(convertedQuery, escErr, oomErr.Error())
			return
		}
		retryRows, retryErr := runQuery()
		if retryErr != nil {
			stream.rowsErr = retryErr
		} else {
			// The retried rowset replaces the original everywhere below —
			// including as the rowset a suspension keeps open.
			rows = retryRows
			activeRows = retryRows
			defer func() {
				if !keepRowsOpen {
					_ = retryRows.Close()
				}
			}()
			stream = c.streamSelectRows(retryRows, cols, colTypes, typeOIDs, false, p.resultFormats, maxRows)
		}
	}

	if stream.scanErr != nil {
		queryFinalErr = stream.scanErr
		c.sendError("ERROR", "42000", stream.scanErr.Error())
		c.setTxError()
		finishRows()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, "42000", stream.scanErr.Error(), "extended")
		return
	}
	if stream.writeErr != nil {
		queryFinalErr = stream.writeErr
		return
	}

	rowCount := stream.rowsSent
	queryRowsAff = int64(rowCount)

	if err := stream.rowsErr; err != nil {
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
		finishRows()
		c.logQuery(start, originalQuery, convertedQuery, cmdType, 0, 0, errCode, errMsg, "extended")
		return
	}

	if stream.limitReached {
		// The row limit was reached with the result set possibly unexhausted:
		// keep the rowset open on the portal and tell the client to Execute
		// again. CommandComplete here would silently truncate the result set
		// to the client's page size (the Hex 1024-row bug). The query log
		// entry is written by the leg that completes the portal.
		keepRowsOpen = true
		p.exec = &portalExec{
			rows:            activeRows,
			cols:            cols,
			typeOIDs:        typeOIDs,
			cmdType:         cmdType,
			rowCount:        int64(rowCount),
			originalQuery:   originalQuery,
			convertedQuery:  convertedQuery,
			start:           start,
			finishProfiling: finishProfiling,
		}
		_ = wire.WritePortalSuspended(c.writer)
		return
	}

	c.updateTxStatus(cmdType)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	finishRows()
	c.logQuery(start, originalQuery, convertedQuery, cmdType, int64(rowCount), 0, "", "", "extended")
}

// resumeSuspendedPortal continues streaming a portal previously suspended by
// Execute hitting its row limit. The query is not re-executed — the portal's
// open rowset picks up exactly where the previous Execute leg stopped. The
// query log entry (spanning all legs, with the cumulative row count) is
// written by whichever leg finishes the portal. There is deliberately no
// exploratory-tier retry here: rows from earlier legs are already out the
// door, so an error must surface (same rule as the mid-stream case above).
func (c *clientConn) resumeSuspendedPortal(p *portal, maxRows int32) {
	exec := p.exec

	loggableQuery := usersecrets.RedactForLog(exec.originalQuery)
	c.currentQuery.Store(loggableQuery)
	c.queryStart.Store(time.Now())
	defer func() {
		c.currentQuery.Store("")
		c.queryStart.Store(time.Time{})
	}()

	// Each Execute leg is one protocol message, so it gets its own metrics
	// scope, mirroring the fresh path — which also gives the leg the terminal
	// wire flush (finishQueryMetrics) every Execute relies on.
	queryMetrics := c.beginQueryMetrics(time.Now())
	queryMetrics.queryText = loggableQuery
	defer c.finishQueryMetrics(queryMetrics)

	stream := c.streamSelectRows(exec.rows, exec.cols, nil, exec.typeOIDs, false, p.resultFormats, maxRows)
	exec.rowCount += int64(stream.rowsSent)

	if stream.scanErr != nil {
		p.closeExec()
		c.sendError("ERROR", "42000", stream.scanErr.Error())
		c.setTxError()
		c.logQuery(exec.start, exec.originalQuery, exec.convertedQuery, exec.cmdType, 0, 0, "42000", stream.scanErr.Error(), "extended")
		return
	}
	if stream.writeErr != nil {
		p.closeExec()
		return
	}
	if err := stream.rowsErr; err != nil {
		p.closeExec()
		errCode := "42000"
		errMsg := err.Error()
		if c.isCallerCancellation(err) {
			errCode = "57014"
			errMsg = "canceling statement due to user request"
		} else {
			c.logger().Error("Row iteration error.", "error", err)
		}
		c.sendError("ERROR", errCode, errMsg)
		c.setTxError()
		c.logQuery(exec.start, exec.originalQuery, exec.convertedQuery, exec.cmdType, 0, 0, errCode, errMsg, "extended")
		return
	}
	if stream.limitReached {
		_ = wire.WritePortalSuspended(c.writer)
		return
	}

	p.closeExec()
	c.updateTxStatus(exec.cmdType)
	tag := buildCommandTagFromRowCount(exec.cmdType, exec.rowCount)
	_ = c.writeCommandComplete(tag)
	c.logQuery(exec.start, exec.originalQuery, exec.convertedQuery, exec.cmdType, exec.rowCount, 0, "", "", "extended")
}

// closeSuspendedPortals releases every suspended portal's open rowset and
// destroys the portal, matching PostgreSQL (portals do not survive
// transaction end). Destroying — not just releasing — matters: a suspended
// portal whose rowset was closed but whose entry survived would re-run the
// query from row 0 on the next Execute, silently replaying the first page as
// a continuation. A 34000 "portal does not exist" is the honest answer.
func (c *clientConn) closeSuspendedPortals() {
	for name, p := range c.portals {
		if p.exec != nil {
			p.closeExec()
			delete(c.portals, name)
		}
	}
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
		delete(c.stmts, name)
	case 'P':
		if p, ok := c.portals[name]; ok {
			p.closeExec()
		}
		delete(c.portals, name)
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
//
// Returns a non-nil error ONLY for a connection-fatal failure (today: a failed
// exploratory-tier escalation, whose switcher already destroyed the session).
// The handlers are void, so such a failure is parked on c.fatalErr and read
// back here; skip-until-Sync is not enough for it — there is no session left to
// resynchronize to, so the message loop must terminate the connection.
func (c *clientConn) runExtendedQueryMessage(handler func([]byte), body []byte) error {
	if c.ignoreTillSync {
		return nil
	}
	before := c.errorResponsesSent
	// Sync — not the handler — owns ReadyForQuery on this protocol; the shared
	// statement-failure paths read this to know that.
	c.inExtendedMessage = true
	func() {
		defer func() { c.inExtendedMessage = false }()
		handler(body)
	}()
	if c.errorResponsesSent != before {
		c.ignoreTillSync = true
	}
	if c.fatalErr != nil {
		c.logger().Error("Extended query error.", "error", c.fatalErr)
		return c.fatalErr
	}
	return nil
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

	reader := bytes.NewReader(body)

	// Read portal name
	portalName, err := readCString(reader)
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}

	// Read statement name
	stmtName, err := readCString(reader)
	if err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}

	// Look up prepared statement
	ps, ok := c.stmts[stmtName]
	if !ok {
		c.sendError("ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", stmtName))
		return
	}

	// Read parameter format codes
	var numParamFormats int16
	if err := binary.Read(reader, binary.BigEndian, &numParamFormats); err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numParamFormats < 0 {
		c.sendError("ERROR", "08P01", "invalid parameter format count in Bind message")
		return
	}
	paramFormats := make([]int16, numParamFormats)
	for i := int16(0); i < numParamFormats; i++ {
		if err := binary.Read(reader, binary.BigEndian, &paramFormats[i]); err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
	}

	// Read parameter values
	var numParams int16
	if err := binary.Read(reader, binary.BigEndian, &numParams); err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numParams < 0 {
		c.sendError("ERROR", "08P01", "invalid parameter count in Bind message")
		return
	}
	paramValues := make([][]byte, numParams)
	for i := int16(0); i < numParams; i++ {
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
		if length == -1 {
			paramValues[i] = nil // NULL
		} else if length < 0 {
			// Only -1 (NULL) is a valid negative length.
			c.sendError("ERROR", "08P01", "invalid parameter length in Bind message")
			return
		} else {
			// The length field is client-controlled; bound the allocation by
			// the remaining bytes of the already-framed Bind message body — a
			// parameter value can never legitimately exceed it. Without this
			// check a client could reserve multi-GiB per parameter (#717).
			if int64(length) > int64(reader.Len()) {
				c.sendError("ERROR", "08P01", fmt.Sprintf("invalid Bind message: parameter %d length %d exceeds remaining message size %d", i+1, length, reader.Len()))
				return
			}
			paramValues[i] = make([]byte, length)
			if _, err := io.ReadFull(reader, paramValues[i]); err != nil {
				c.sendError("ERROR", "08P01", "invalid Bind message")
				return
			}
		}
	}

	// Read result format codes
	var numResultFormats int16
	if err := binary.Read(reader, binary.BigEndian, &numResultFormats); err != nil {
		c.sendError("ERROR", "08P01", "invalid Bind message")
		return
	}
	if numResultFormats < 0 {
		c.sendError("ERROR", "08P01", "invalid result format count in Bind message")
		return
	}
	resultFormats := make([]int16, numResultFormats)
	for i := int16(0); i < numResultFormats; i++ {
		if err := binary.Read(reader, binary.BigEndian, &resultFormats[i]); err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
	}

	// Close existing portal with same name — including the open rowset of a
	// suspended portal being abandoned.
	if old, ok := c.portals[portalName]; ok {
		old.closeExec()
		delete(c.portals, portalName)
	}

	c.portals[portalName] = &portal{
		stmt:          ps,
		paramValues:   paramValues,
		paramFormats:  paramFormats,
		resultFormats: resultFormats,
		described:     ps.described, // Inherit from statement if Describe(S) was called
	}

	_ = wire.WriteBindComplete(c.writer)
}
