package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/wire"
	"github.com/posthog/duckgres/transpiler"
)

// executeQueryDirect executes a query directly against DuckDB without any transpilation.
// Used for passthrough users who send DuckDB-native SQL.
func (c *clientConn) executeQueryDirect(query, cmdType string) error {
	if !queryReturnsResults(query) {
		// Handle nested BEGIN
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = c.writeCommandComplete("BEGIN")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}

		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(cmdType)

		ctx, cleanup := c.queryContext()
		defer cleanup()

		workerStatement := workerStatementWithQuery(workerOriginClient, workerOperationDirectExec, query)
		workerStart := time.Now()
		var workerRows int64
		var workerErr error
		c.logWorkerStatementStarted(workerStatement)
		defer func() {
			c.logWorkerStatementFinished(workerStatement, workerStart, workerRows, workerErr)
		}()

		runExec := func() (ExecResult, error) {
			return c.executor.ExecContext(ctx, query)
		}

		result, err := runExec()
		if err != nil && c.txStatus == txStatusIdle && isDuckLakeTransactionConflict(err) {
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
				func() (ExecResult, error) {
					return c.executor.ExecContext(ctx, query)
				},
			)
		}
		if err != nil {
			workerErr = err
			errCode := classifyErrorCode(err)
			errMsg := err.Error()
			if c.isCallerCancellation(err) {
				errMsg = "canceling statement due to user request"
			} else {
				c.logQueryError(query, err)
			}
			c.sendError("ERROR", errCode, errMsg)
			c.setTxError()
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}

		if result != nil {
			workerRows, _ = result.RowsAffected()
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	_, _, _, err := c.executeSelectQuery(query, cmdType, workerStatementWithQuery(workerOriginClient, workerOperationSelect, query))
	return err
}

// rewriteDirectQuery expands a bare `USE ducklake` to its reliable two-part
// `catalog.schema` target. This is NOT logical-name masking — the catalog name
// is real; the rewrite only works around DuckDB's bare-catalog `USE`
// resolution (a bare `USE ducklake` issued while the session is in another
// catalog resolves `ducklake` as a *schema* within that catalog). Any other
// `USE <name>` and all other statements are passed through unchanged.
func (c *clientConn) rewriteDirectQuery(query string) string {
	if c == nil || c.server == nil || c.passthrough || !c.catalogUseRewrite {
		return query
	}

	stripped := strings.TrimSpace(stripLeadingComments(query))
	if stripped == "" {
		return query
	}

	hasSemicolon := strings.HasSuffix(stripped, ";")
	trimmed := strings.TrimSpace(strings.TrimSuffix(stripped, ";"))

	if len(trimmed) < len("USE") || !strings.EqualFold(trimmed[:len("USE")], "USE") {
		return query
	}

	target := strings.TrimSpace(trimmed[len("USE"):])
	if target == "" {
		return query
	}

	unquoted := target
	if len(target) >= 2 && target[0] == '"' && target[len(target)-1] == '"' {
		unquoted = strings.ReplaceAll(target[1:len(target)-1], `""`, `"`)
	}

	if !strings.EqualFold(unquoted, physicalDuckLakeCatalog) {
		return query
	}
	// `USE ducklake` -> ducklake.main (DuckLake's real schema is `main`).
	target2part := physicalDuckLakeCatalog + ".main"

	rewritten := "USE " + target2part
	if hasSemicolon {
		rewritten += ";"
	}
	return rewritten
}

// physicalDuckLakeCatalog is the physical catalog name DuckLake is attached as.
const physicalDuckLakeCatalog = "ducklake"

// executeSelectQuery runs a result-returning query against DuckDB and streams results to the client.
// Sends RowDescription, DataRow messages, CommandComplete, and ReadyForQuery.
// Returns the number of rows sent, any SQLSTATE+message sent to the client,
// and any connection-level error.
func (c *clientConn) executeSelectQuery(query string, cmdType string, workerStatement workerStatement) (int64, string, string, error) {
	ctx, cleanup := c.queryContext()
	defer cleanup()

	execStart := time.Now()
	execCtx, execSpan := observe.Tracer().Start(ctx, "duckgres.execute")
	// The physical worker pair captures row streaming failures as well as the
	// initial dispatch, while the outer client scope owns logical analytics.
	var workerRows int64
	var workerErr error
	c.logWorkerStatementStarted(workerStatement)
	defer func() {
		c.logWorkerStatementFinished(workerStatement, execStart, workerRows, workerErr)
	}()
	runQuery := func() (RowSet, error) {
		return c.executor.QueryContext(ctx, query)
	}

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
			func() (RowSet, error) {
				return c.executor.QueryContext(ctx, query)
			},
		)
	}
	c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
	execSpan.End()
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalWorkerError(workerStatement.origin, query, err, true)
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return 0, errCode, errMsg, nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerStatement.origin, query, err, true)
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return 0, errCode, errMsg, nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerStatement.origin, query, err, true)
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return 0, errCode, errMsg, nil
	}

	_, sendSpan := observe.Tracer().Start(ctx, "duckgres.send_results")
	defer sendSpan.End()

	// Mid-stream wire-write failures (sendRowDescription / sendDataRowWithFormats)
	// surface a broken socket — typically the AWS NLB tearing down a stalled
	// connection, or the kernel collapsing the socket after TCP_USER_TIMEOUT.
	// They must route through logQueryError so the SQLSTATE-class severity
	// router fires Error-level "Query execution errored." for alerts.
	// Pre-fix the only signal was Info-level "Client query finished." from the
	// deferred lifecycle log, which silently disappears below alerting.
	if err := c.sendRowDescription(cols, colTypes); err != nil {
		workerErr = err
		if workerStatement.origin != workerOriginRewrite && !c.isCallerCancellation(err) {
			c.logQueryError(query, fmt.Errorf("pgwire client write failed sending row description: %w", err))
		}
		return 0, "", "", err
	}

	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			workerErr = err
			errCode, errMsg := c.sendLogicalResultWorkerError(workerStatement.origin, query, err, true)
			c.setTxError()
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return 0, errCode, errMsg, nil
		}

		if err := c.sendDataRowWithFormats(values, nil, typeOIDs); err != nil {
			workerErr = err
			if workerStatement.origin != workerOriginRewrite && !c.isCallerCancellation(err) {
				c.logQueryError(query, fmt.Errorf("pgwire client write failed during result streaming: %w", err))
			}
			return 0, "", "", err
		}
		rowCount++
	}
	workerRows = int64(rowCount)

	if err := rows.Err(); err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerStatement.origin, query, err, true)
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return 0, errCode, errMsg, nil
	}

	c.updateTxStatus(cmdType)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return int64(rowCount), "", "", nil
}

// handleMultiStatementQuery processes multiple semicolon-separated statements
// from a single Q (simple query) message. Per the PostgreSQL wire protocol,
// each statement gets its own RowDescription/DataRow/CommandComplete messages,
// with a single ReadyForQuery at the end. If any statement fails, remaining
// statements are skipped.
func (c *clientConn) handleMultiStatementQuery(start time.Time, query string) (retErr error) {
	logicalCmdType := c.getCommandType(strings.ToUpper(strings.TrimSpace(query)))
	errorResponsesBefore := c.errorResponsesSent
	defer func() {
		var errCode, errMessage string
		if retErr != nil {
			errCode, errMessage = c.clientErrorResponse(retErr)
		} else if c.errorResponsesSent > errorResponsesBefore {
			if scope := c.activeQueryMetrics; scope != nil && scope.client != nil {
				errCode = scope.client.errorCode
				errMessage = scope.client.errorMessage
			}
			if errCode == "" {
				errCode = c.lastErrorCode
			}
		}
		// A semicolon-separated simple Query is one logical client operation.
		// Its physical statements have worker pairs, but never durable entries.
		c.logQuery(start, query, "", logicalCmdType, 0, 0, errCode, errMessage, "simple")
	}()

	// Re-parse with long identifiers protected so that splitting the batch via
	// Deparse below does not silently truncate names > 63 bytes (see
	// transpiler/longident.go). executeSingleStatement transpiles each
	// statement again, which re-applies the same protection.
	parseSQL, longIdents := transpiler.ProtectLongIdentifiers(query)
	tree, err := pg_query.Parse(parseSQL)
	if err != nil {
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}
	c.logger().Debug("Multi-statement simple query.", "count", len(tree.Stmts))

	for _, stmt := range tree.Stmts {
		// Deparse individual statement back to SQL
		singleTree := &pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{stmt},
		}
		singleSQL, err := pg_query.Deparse(singleTree)
		if err != nil {
			c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
			break
		}
		singleSQL = transpiler.RestoreLongIdentifiers(singleSQL, longIdents)

		errSent, fatalErr := c.executeSingleStatementWithDurableLog(singleSQL, false)
		if fatalErr != nil {
			return fatalErr
		}
		if errSent {
			break // Stop processing remaining statements on error
		}
	}

	if err := c.writeReadyForQuery(c.txStatus); err != nil {
		return err
	}
	return c.flushWriter()
}

// executeSingleStatement transpiles and executes a single SQL statement,
// sending results to the client. Does NOT send ReadyForQuery (the caller
// is responsible for that). Returns (true, nil) if an error was sent to the
// client (so the caller can stop processing a batch), or (false, err) for
// fatal connection errors.
func (c *clientConn) executeSingleStatement(query string) (errSent bool, fatalErr error) {
	return c.executeSingleStatementWithDurableLog(query, true)
}

// executeSingleStatementWithDurableLog executes one physical statement from a
// semicolon-separated simple Query. The outer batch owns the durable record;
// standalone callers retain the legacy per-statement record for compatibility.
func (c *clientConn) executeSingleStatementWithDurableLog(query string, logDurable bool) (errSent bool, fatalErr error) {
	start := time.Now()

	// Check for cursor operations before transpilation
	tree, parseErr := pg_query.Parse(query)
	if parseErr == nil && len(tree.Stmts) == 1 {
		switch s := tree.Stmts[0].Stmt.Node.(type) {
		case *pg_query.Node_DeclareCursorStmt:
			innerSQL := deparseInnerQuery(s.DeclareCursorStmt.Query)
			if innerSQL == "" {
				c.sendError("ERROR", "42601", "could not deparse cursor query")
				return true, nil
			}
			transpiledSQL := innerSQL
			if !c.passthrough {
				tr := c.newTranspiler(false)
				result, err := tr.Transpile(innerSQL)
				if err != nil {
					c.sendError("ERROR", "42601", fmt.Sprintf("syntax error in cursor query: %v", err))
					return true, nil
				}
				transpiledSQL = result.SQL
				if result.FallbackToNative {
					transpiledSQL = innerSQL
				}
			}
			c.closeCursor(s.DeclareCursorStmt.Portalname)
			c.cursors[s.DeclareCursorStmt.Portalname] = &cursorState{query: transpiledSQL}
			_ = c.writeCommandComplete("DECLARE CURSOR")
			return false, nil

		case *pg_query.Node_FetchStmt:
			if !isFetchForwardOnly(s.FetchStmt.Direction) {
				c.sendError("ERROR", "0A000", "cursor can only scan forward")
				return true, nil
			}
			cursor, ok := c.cursors[s.FetchStmt.Portalname]
			if !ok {
				c.sendError("ERROR", "34000", fmt.Sprintf("cursor %q does not exist", s.FetchStmt.Portalname))
				return true, nil
			}
			if cursor.rows == nil {
				if err := c.openCursor(cursor); err != nil {
					errCode, errMsg := c.clientErrorResponse(err)
					c.sendError("ERROR", errCode, errMsg)
					c.setTxError()
					return true, nil
				}
			}
			howMany := s.FetchStmt.HowMany
			if howMany < 0 {
				c.sendError("ERROR", "0A000", "cursor can only scan forward")
				return true, nil
			}
			if s.FetchStmt.Ismove {
				moveCount := int64(0)
				for moveCount < howMany && cursor.rows.Next() {
					values := make([]interface{}, len(cursor.cols))
					valuePtrs := make([]interface{}, len(cursor.cols))
					for i := range values {
						valuePtrs[i] = &values[i]
					}
					_ = cursor.rows.Scan(valuePtrs...)
					moveCount++
				}
				_ = c.writeCommandComplete(fmt.Sprintf("MOVE %d", moveCount))
				return false, nil
			}
			if err := c.sendRowDescription(cursor.cols, cursor.colTypes); err != nil {
				return false, err
			}
			rowCount := int64(0)
			for rowCount < howMany && cursor.rows.Next() {
				values := make([]interface{}, len(cursor.cols))
				valuePtrs := make([]interface{}, len(cursor.cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := cursor.rows.Scan(valuePtrs...); err != nil {
					errCode, errMsg := c.clientErrorResponse(err)
					c.sendError("ERROR", errCode, errMsg)
					c.setTxError()
					return true, nil
				}
				if err := c.sendDataRowWithFormats(values, nil, cursor.typeOIDs); err != nil {
					return false, err
				}
				rowCount++
			}
			if err := cursor.rows.Err(); err != nil {
				errCode, errMsg := c.clientErrorResponse(err)
				c.sendError("ERROR", errCode, errMsg)
				c.setTxError()
				return true, nil
			}
			_ = c.writeCommandComplete(fmt.Sprintf("FETCH %d", rowCount))
			return false, nil

		case *pg_query.Node_ClosePortalStmt:
			if s.ClosePortalStmt.Portalname == "" {
				c.closeAllCursors()
			} else {
				if _, ok := c.cursors[s.ClosePortalStmt.Portalname]; !ok {
					c.sendError("ERROR", "34000", fmt.Sprintf("cursor %q does not exist", s.ClosePortalStmt.Portalname))
					return true, nil
				}
				c.closeCursor(s.ClosePortalStmt.Portalname)
			}
			_ = c.writeCommandComplete("CLOSE CURSOR")
			return false, nil
		}
	}

	// Intercept pg_cursors queries
	if cursorName, _, ok := matchPgCursorsQuery(query); ok {
		_, exists := c.cursors[cursorName]
		_ = c.sendPgCursorsRowDescriptionWithFormats(nil)
		rowCount := 0
		if exists {
			_ = c.sendDataRowWithFormats([]interface{}{int64(1)}, nil, []int32{23})
			rowCount = 1
		}
		_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", rowCount))
		return false, nil
	}

	// Intercept pg_stat_activity queries
	if matchPgStatActivityQuery(query) {
		_ = c.sendPgStatActivityRowDescriptionWithFormats(nil)
		conns := c.server.listConns()
		sort.Slice(conns, func(i, j int) bool { return conns[i].pid < conns[j].pid })
		for _, conn := range conns {
			_ = c.sendPgStatActivityDataRow(conn, nil)
		}
		_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", len(conns)))
		return false, nil
	}

	// Transpile
	tr := c.newTranspiler(false)
	result, err := tr.Transpile(query)
	if err != nil {
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		return true, nil
	}

	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			// Not necessarily a syntax error: a parseable native query (e.g.
			// `DESCRIBE x.y.z`) can fail validation with a real catalog/binder
			// error. classifyErrorCode keeps true Parser Errors at 42601 but maps
			// "Catalog Error: schema … does not exist" to 3F000 / 42P01 so clients
			// see the Postgres-equivalent class. See the matching site in conn.go.
			c.sendError("ERROR", classifyErrorCode(err), unwrapFlightError(err.Error()))
			return true, nil
		}
		c.logger().Debug("Fallback to native DuckDB.", "query", query)
	}

	if result.Error != nil {
		c.sendError("ERROR", transformErrorSQLState(result.Error), result.Error.Error())
		return true, nil
	}

	// duckgres.query_source custom GUC (SET / SHOW): intercepted session-side.
	if result.QuerySourceSet != nil {
		c.setQuerySource(*result.QuerySourceSet)
		_ = c.writeCommandComplete("SET")
		return false, nil
	}
	if result.QuerySourceShow {
		_ = c.sendRowDescription([]string{querySourceGUCName}, []ColumnTyper{staticColumnType("VARCHAR")})
		_ = c.sendDataRowWithFormats([]interface{}{c.QuerySource()}, nil, nil)
		_ = c.writeCommandComplete("SHOW")
		return false, nil
	}

	if result.IsIgnoredSet {
		_ = c.writeCommandComplete("SET")
		return false, nil
	}

	if result.IsNoOp {
		_ = c.writeCommandComplete(result.NoOpTag)
		return false, nil
	}

	// Multi-statement rewrites (writable CTEs) not supported inside batches
	if len(result.Statements) > 0 {
		c.sendError("ERROR", "0A000", "writable CTEs not supported in multi-statement queries")
		return true, nil
	}

	executedQuery := c.rewriteDirectQuery(result.SQL)
	workerOrigin := workerOriginForQueries(query, result.SQL, executedQuery)
	logicalTranspiledQuery := logicalWorkerTranspiledQuery(workerOrigin, executedQuery)
	if executedQuery != query {
		if workerOrigin == workerOriginRewrite {
			c.logger().Debug("Generated query rewrite.", "operation", workerOperationSimpleBatchStatement)
		} else {
			c.logger().Debug("Query transpiled.", "executed", executedQuery)
		}
	}

	upperQuery := strings.ToUpper(executedQuery)
	cmdType := c.getCommandType(upperQuery)

	// COPY not supported inside batches
	if cmdType == "COPY" {
		c.sendError("ERROR", "0A000", "COPY not supported in multi-statement queries")
		return true, nil
	}

	if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
		c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
		_ = c.writeCommandComplete("BEGIN")
		return false, nil
	}

	workerStatement := workerStatementForQuery(
		workerOrigin,
		workerOperationSimpleBatchStatement,
		executedQuery,
	)
	workerStart := time.Now()
	var workerRows int64
	var workerErr error
	workerFinished := false
	finishWorker := func() {
		if workerFinished {
			return
		}
		workerFinished = true
		c.logWorkerStatementFinished(workerStatement, workerStart, workerRows, workerErr)
	}
	c.logWorkerStatementStarted(workerStatement)
	defer finishWorker()

	if !queryReturnsResults(executedQuery) {
		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(cmdType)

		ctx, cleanup := c.queryContext()
		defer cleanup()

		compatibilityFallbackUsed := false
		runExec := func() (ExecResult, error) {
			execResult, err := c.executor.ExecContext(ctx, executedQuery)
			if err != nil {
				fallbackResult, handled, fallbackErr := c.execCompatibilityFallback(executedQuery, err, func(fallbackQuery string) (ExecResult, error) {
					return c.executor.ExecContext(ctx, fallbackQuery)
				}, func() {
					compatibilityFallbackUsed = true
					workerErr = err
					finishWorker()
				})
				if handled {
					return fallbackResult, fallbackErr
				}
			}
			return execResult, err
		}

		execResult, err := runExec()
		if err != nil {
			if !compatibilityFallbackUsed {
				if c.txStatus == txStatusIdle && isDuckLakeTransactionConflict(err) {
					ducklakeConflictTotal.Inc()
					execResult, err = retryOnConflict(runExec)
				}
				if err != nil {
					execResult, err, _ = recoverAbortedTransaction(
						err,
						c.txStatus == txStatusIdle,
						func() error {
							_, rollbackErr := c.executor.ExecContext(context.Background(), "ROLLBACK")
							return rollbackErr
						},
						runExec,
					)
				}
			}
			if err != nil {
				var errCode, errMsg string
				if compatibilityFallbackUsed {
					errCode, errMsg = c.sendGeneratedWorkerError(err)
				} else {
					workerErr = err
					errCode, errMsg = c.sendLogicalWorkerError(workerOrigin, executedQuery, err, true)
				}
				c.setTxError()
				if logDurable {
					c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
				}
				return true, nil
			}
		}

		var writtenRows int64
		if execResult != nil {
			writtenRows, _ = execResult.RowsAffected()
		}
		if !compatibilityFallbackUsed {
			workerRows = writtenRows
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, execResult)
		_ = c.writeCommandComplete(tag)
		if logDurable {
			c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, writtenRows, "", "", "simple-batch")
		}
		return false, nil
	}

	// SELECT
	ctx, cleanup := c.queryContext()
	defer cleanup()

	runQuery := func() (RowSet, error) {
		return c.executor.QueryContext(ctx, executedQuery)
	}

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
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalWorkerError(workerOrigin, executedQuery, err, true)
		c.setTxError()
		if logDurable {
			c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
		}
		return true, nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerOrigin, executedQuery, err, false)
		c.setTxError()
		if logDurable {
			c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
		}
		return true, nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerOrigin, executedQuery, err, false)
		c.setTxError()
		if logDurable {
			c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
		}
		return true, nil
	}

	if err := c.sendRowDescription(cols, colTypes); err != nil {
		workerErr = err
		return false, err
	}

	// Extract type OIDs for JSON-aware text formatting
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			workerErr = err
			errCode, errMsg := c.sendLogicalResultWorkerError(workerOrigin, executedQuery, err, false)
			c.setTxError()
			if logDurable {
				c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
			}
			return true, nil
		}

		if err := c.sendDataRowWithFormats(values, nil, typeOIDs); err != nil {
			workerErr = err
			return false, err
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		workerErr = err
		errCode, errMsg := c.sendLogicalResultWorkerError(workerOrigin, executedQuery, err, true)
		c.setTxError()
		if logDurable {
			c.logQuery(start, query, logicalTranspiledQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
		}
		return true, nil
	}
	workerRows = int64(rowCount)

	c.updateTxStatus(cmdType)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	if logDurable {
		c.logQuery(start, query, logicalTranspiledQuery, cmdType, int64(rowCount), 0, "", "", "simple-batch")
	}
	return false, nil
}

// executeMultiStatement handles execution of multi-statement query rewrites.
// This is used for writable CTE transformations where a single PostgreSQL query
// is rewritten into multiple DuckDB statements.
//
// Execution order:
// 1. Execute setup statements (BEGIN, CREATE TEMP TABLE, etc.) - all but the last
// 2. Execute final statement and obtain cursor (rows object)
// 3. Execute cleanup statements (DROP temp tables, COMMIT) - cursor still valid
// 4. Stream rows from cursor to client
func (c *clientConn) executeMultiStatement(start time.Time, originalQuery string, statements []string, cleanup []string) error {
	if len(statements) == 0 {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}
	logicalCmdType := c.getCommandType(strings.ToUpper(strings.TrimSpace(originalQuery)))
	var resultRows, writtenRows int64
	var logErrCode, logErrMessage string
	defer func() {
		// Generated rewrite SQL must never be persisted as the client query's
		// transpiled text. One durable entry represents the logical operation.
		c.logQuery(start, originalQuery, "", logicalCmdType, resultRows, writtenRows, logErrCode, logErrMessage, "simple")
	}()
	ctx, queryCleanup := c.queryContext()
	defer queryCleanup()
	recordTransportError := func(err error) {
		if err != nil && logErrCode == "" {
			logErrCode, logErrMessage = c.clientErrorResponse(err)
		}
	}

	// Check if we're adding our own transaction wrapper
	hasOurTransaction := len(statements) >= 2 &&
		strings.ToUpper(strings.TrimSpace(statements[0])) == "BEGIN" &&
		len(cleanup) > 0 &&
		strings.ToUpper(strings.TrimSpace(cleanup[len(cleanup)-1])) == "COMMIT"

	// If already in a transaction, skip our BEGIN/COMMIT wrapper
	if hasOurTransaction && c.txStatus == txStatusTransaction {
		statements = statements[1:]        // Strip BEGIN
		cleanup = cleanup[:len(cleanup)-1] // Strip COMMIT from cleanup
	}

	// Execute setup statements (all but last). These statements are generated
	// by the rewrite, so expose only their stable operation and position rather
	// than presenting generated SQL as client text.
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		workerStatement := generatedWorkerStatement(
			workerOriginRewrite,
			workerOperationRewriteSetup,
			"step", i+1,
			"total", len(statements)-1,
		)
		c.logger().Debug("Multi-stmt setup.", "step", i+1, "total", len(statements)-1)
		setupStart := time.Now()
		c.logWorkerStatementStarted(workerStatement)
		result, err := c.executor.ExecContext(ctx, stmt)
		var setupRows int64
		if result != nil {
			setupRows, _ = result.RowsAffected()
		}
		c.logWorkerStatementFinished(workerStatement, setupStart, setupRows, err)
		if err != nil {
			c.logger().Error("Multi-stmt setup error.", "step", i+1, "error_code", classifyErrorCode(err))
			c.setTxError()
			// On error, still try to cleanup (best effort)
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
	}

	// Handle final statement
	finalStmt := statements[len(statements)-1]
	upperFinal := strings.ToUpper(strings.TrimSpace(finalStmt))
	finalCmdType := c.getCommandType(upperFinal)
	c.logger().Debug("Multi-stmt final.", "cmd_type", finalCmdType)

	finalStart := time.Now()
	var workerRows int64
	var workerErr error
	workerStatement := generatedWorkerStatement(workerOriginRewrite, workerOperationRewriteFinal)
	c.logWorkerStatementStarted(workerStatement)
	defer func() {
		c.logWorkerStatementFinished(workerStatement, finalStart, workerRows, workerErr)
	}()

	if queryReturnsResults(finalStmt) {
		// Result-returning query: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.executor.QueryContext(ctx, finalStmt)
		if err != nil {
			workerErr = err
			c.logger().Error("Multi-stmt final query error.", "error_code", classifyErrorCode(err))
			c.setTxError()
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		// DuckDB cursor holds result data even after source tables are dropped
		c.executeCleanup(cleanup)

		// Now stream results from cursor. The streaming helper distinguishes a
		// handled worker error from a client-wire transport failure, allowing the
		// final worker event to retain its actual SQLSTATE.
		streamResult := c.streamRowsToClient(rows, finalCmdType)
		if streamResult.workerErr != nil {
			workerErr = streamResult.workerErr
			logErrCode, logErrMessage = c.generatedWorkerErrorTelemetry(workerErr)
		}
		if streamResult.transportErr != nil {
			recordTransportError(streamResult.transportErr)
			return streamResult.transportErr
		}
		return nil

	} else {
		// Non-result query (DML without RETURNING, DDL, etc.): execute then cleanup
		result, err := c.executor.ExecContext(ctx, finalStmt)
		if err != nil {
			workerErr = err
			c.logger().Error("Multi-stmt final exec error.", "error_code", classifyErrorCode(err))
			c.setTxError()
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		if result != nil {
			workerRows, _ = result.RowsAffected()
		}
		writtenRows = workerRows

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion
		tag := c.buildCommandTag(finalCmdType, result)
		if err := c.writeCommandComplete(tag); err != nil {
			recordTransportError(err)
			return err
		}
		if err := c.writeReadyForQuery(c.txStatus); err != nil {
			recordTransportError(err)
			return err
		}
		if err := c.flushWriter(); err != nil {
			recordTransportError(err)
			return err
		}
		return nil
	}
}

// executeCleanup runs cleanup statements, ignoring errors (best effort).
// This is used to clean up temp tables after a multi-statement query. Cleanup
// deliberately bypasses c.queryContext: it must still run after the client
// cancels the logical operation, so it uses the executor's background-context
// Exec path rather than inheriting the canceled query context.
func (c *clientConn) executeCleanup(cleanup []string) {
	for i, stmt := range cleanup {
		workerStatement := generatedWorkerStatement(
			workerOriginRewrite,
			workerOperationRewriteCleanup,
			"step", i+1,
			"total", len(cleanup),
		)
		c.logger().Debug("Multi-stmt cleanup.", "step", i+1, "total", len(cleanup))
		cleanupStart := time.Now()
		c.logWorkerStatementStarted(workerStatement)
		result, err := c.executor.Exec(stmt)
		var cleanupRows int64
		if result != nil {
			cleanupRows, _ = result.RowsAffected()
		}
		c.logWorkerStatementFinished(workerStatement, cleanupStart, cleanupRows, err)
		if err != nil {
			// Log but don't fail - cleanup is best effort
			c.logger().Warn("Multi-stmt cleanup error (ignored).", "step", i+1, "error_code", classifyErrorCode(err))
		}
	}
}

// executeMultiStatementExtended handles execution of multi-statement query rewrites
// for the extended query protocol (Parse/Bind/Execute).
// Unlike executeMultiStatement, this does NOT send ReadyForQuery (that's done by Sync).
func (c *clientConn) executeMultiStatementExtended(start time.Time, originalQuery string, statements []string, cleanup []string, args []interface{}, resultFormats []int16, described bool) {
	if len(statements) == 0 {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		return
	}
	logicalCmdType := c.getCommandType(strings.ToUpper(strings.TrimSpace(originalQuery)))
	var resultRows, writtenRows int64
	var logErrCode, logErrMessage string
	defer func() {
		// Generated rewrite SQL must never be persisted as the client query's
		// transpiled text. One durable entry represents the logical operation.
		c.logQuery(start, originalQuery, "", logicalCmdType, resultRows, writtenRows, logErrCode, logErrMessage, "extended")
	}()
	ctx, queryCleanup := c.queryContext()
	defer queryCleanup()
	recordTransportError := func(err error) {
		if err != nil && logErrCode == "" {
			logErrCode, logErrMessage = c.clientErrorResponse(err)
		}
	}

	// Check if we're adding our own transaction wrapper
	hasOurTransaction := len(statements) >= 2 &&
		strings.ToUpper(strings.TrimSpace(statements[0])) == "BEGIN" &&
		len(cleanup) > 0 &&
		strings.ToUpper(strings.TrimSpace(cleanup[len(cleanup)-1])) == "COMMIT"

	// If already in a transaction, skip our BEGIN/COMMIT wrapper
	if hasOurTransaction && c.txStatus == txStatusTransaction {
		statements = statements[1:]        // Strip BEGIN
		cleanup = cleanup[:len(cleanup)-1] // Strip COMMIT from cleanup
	}

	// Execute setup statements (all but last). Rewrite-generated SQL is never
	// logged as client text; worker events carry only stable operation metadata.
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		workerStatement := generatedWorkerStatement(
			workerOriginRewrite,
			workerOperationRewriteSetup,
			"step", i+1,
			"total", len(statements)-1,
		)
		c.logger().Debug("Multi-stmt-ext setup.", "step", i+1, "total", len(statements)-1)
		setupStart := time.Now()
		c.logWorkerStatementStarted(workerStatement)
		result, err := c.executor.ExecContext(ctx, stmt, args...)
		var setupRows int64
		if result != nil {
			setupRows, _ = result.RowsAffected()
		}
		c.logWorkerStatementFinished(workerStatement, setupStart, setupRows, err)
		if err != nil {
			c.logger().Error("Multi-stmt-ext setup error.", "step", i+1, "error_code", classifyErrorCode(err))
			c.setTxError()
			// On error, still try to cleanup (best effort)
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			return
		}
	}

	// Handle final statement
	finalStmt := statements[len(statements)-1]
	upperFinal := strings.ToUpper(strings.TrimSpace(finalStmt))
	finalCmdType := c.getCommandType(upperFinal)
	c.logger().Debug("Multi-stmt-ext final.", "cmd_type", finalCmdType)

	finalStart := time.Now()
	var workerRows int64
	var workerErr error
	workerStatement := generatedWorkerStatement(workerOriginRewrite, workerOperationRewriteFinal)
	c.logWorkerStatementStarted(workerStatement)
	defer func() {
		c.logWorkerStatementFinished(workerStatement, finalStart, workerRows, workerErr)
	}()

	if queryReturnsResults(finalStmt) {
		// Result-returning query: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.executor.QueryContext(ctx, finalStmt, args...)
		if err != nil {
			workerErr = err
			c.logger().Error("Multi-stmt-ext final query error.", "error_code", classifyErrorCode(err))
			c.setTxError()
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			return
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		c.executeCleanup(cleanup)

		// Preserve a handled worker failure separately from a pgwire write
		// error, so the worker finish and durable log agree on SQLSTATE.
		streamResult := c.streamRowsToClientExtended(rows, finalCmdType, resultFormats, described)
		if streamResult.workerErr != nil {
			workerErr = streamResult.workerErr
			logErrCode, logErrMessage = c.generatedWorkerErrorTelemetry(workerErr)
		}
		if streamResult.transportErr != nil {
			recordTransportError(streamResult.transportErr)
		}

	} else {
		// Non-result query (DML without RETURNING, DDL, etc.): execute then cleanup
		result, err := c.executor.ExecContext(ctx, finalStmt, args...)
		if err != nil {
			workerErr = err
			c.logger().Error("Multi-stmt-ext final exec error.", "error_code", classifyErrorCode(err))
			c.setTxError()
			c.executeCleanup(cleanup)
			logErrCode, logErrMessage = c.sendGeneratedWorkerError(err)
			return
		}
		if result != nil {
			workerRows, _ = result.RowsAffected()
		}
		writtenRows = workerRows

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion (no ReadyForQuery - that's done by Sync)
		tag := c.buildCommandTag(finalCmdType, result)
		if err := c.writeCommandComplete(tag); err != nil {
			recordTransportError(err)
		}
	}
}
