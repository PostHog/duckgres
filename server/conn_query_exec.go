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

		// Lifecycle log pair (PR #519): every DML simple-query gets a
		// matched logQueryStarted / logQueryFinished. Captured via
		// closures so the deferred call sees the eventual rows + err.
		queryStart := time.Now()
		var queryRowsAff int64
		var queryFinalErr error
		c.logQueryStarted(query)
		defer func() {
			c.logQueryFinished(query, queryStart, queryRowsAff, queryFinalErr)
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
			queryFinalErr = err
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
			queryRowsAff, _ = result.RowsAffected()
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	_, _, _, err := c.executeSelectQuery(query, cmdType)
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
func (c *clientConn) executeSelectQuery(query string, cmdType string) (int64, string, string, error) {
	ctx, cleanup := c.queryContext()
	defer cleanup()

	execStart := time.Now()
	execCtx, execSpan := observe.Tracer().Start(ctx, "duckgres.execute")
	// Lifecycle log pair: deferred logQueryFinished captures the eventual
	// rowCount and any error from any return path — including Scan,
	// ColumnTypes, sendRowDescription, and rows.Err() — so the pair is
	// always balanced.
	var queryRowsAff int64
	var queryFinalErr error
	c.logQueryStarted(query)
	defer func() {
		c.logQueryFinished(query, execStart, queryRowsAff, queryFinalErr)
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
		queryFinalErr = err
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
		return 0, errCode, errMsg, nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		queryFinalErr = err
		errCode := "42000"
		errMsg := err.Error()
		if !c.isCallerCancellation(err) {
			c.logQueryError(query, err)
		}
		c.sendError("ERROR", errCode, errMsg)
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return 0, errCode, errMsg, nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		queryFinalErr = err
		errCode := "42000"
		errMsg := err.Error()
		if !c.isCallerCancellation(err) {
			c.logQueryError(query, err)
		}
		c.sendError("ERROR", errCode, errMsg)
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
	// Pre-fix the only signal was Info-level "Query finished." from the
	// deferred lifecycle log, which silently disappears below alerting.
	if err := c.sendRowDescription(cols, colTypes); err != nil {
		queryFinalErr = err
		if !c.isCallerCancellation(err) {
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
			queryFinalErr = err
			errCode := "42000"
			errMsg := err.Error()
			if !c.isCallerCancellation(err) {
				c.logQueryError(query, err)
			}
			c.sendError("ERROR", errCode, errMsg)
			c.setTxError()
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return 0, errCode, errMsg, nil
		}

		if err := c.sendDataRowWithFormats(values, nil, typeOIDs); err != nil {
			queryFinalErr = err
			if !c.isCallerCancellation(err) {
				c.logQueryError(query, fmt.Errorf("pgwire client write failed during result streaming: %w", err))
			}
			return 0, "", "", err
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
		} else {
			c.logQueryError(query, err)
		}
		c.sendError("ERROR", errCode, errMsg)
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
func (c *clientConn) handleMultiStatementQuery(query string) error {
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

		errSent, fatalErr := c.executeSingleStatement(singleSQL)
		if fatalErr != nil {
			return fatalErr
		}
		if errSent {
			break // Stop processing remaining statements on error
		}
	}

	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// executeSingleStatement transpiles and executes a single SQL statement,
// sending results to the client. Does NOT send ReadyForQuery (the caller
// is responsible for that). Returns (true, nil) if an error was sent to the
// client (so the caller can stop processing a batch), or (false, err) for
// fatal connection errors.
func (c *clientConn) executeSingleStatement(query string) (errSent bool, fatalErr error) {
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
					if c.isCallerCancellation(err) {
						c.sendError("ERROR", "57014", "canceling statement due to user request")
					} else {
						c.sendError("ERROR", "42000", err.Error())
					}
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
					c.sendError("ERROR", "42000", err.Error())
					c.setTxError()
					return true, nil
				}
				if err := c.sendDataRowWithFormats(values, nil, cursor.typeOIDs); err != nil {
					return false, err
				}
				rowCount++
			}
			if err := cursor.rows.Err(); err != nil {
				if c.isCallerCancellation(err) {
					c.sendError("ERROR", "57014", "canceling statement due to user request")
				} else {
					c.sendError("ERROR", "42000", err.Error())
				}
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
	if executedQuery != query {
		c.logger().Debug("Query transpiled.", "executed", executedQuery)
	}

	upperQuery := strings.ToUpper(executedQuery)
	cmdType := c.getCommandType(upperQuery)

	// COPY not supported inside batches
	if cmdType == "COPY" {
		c.sendError("ERROR", "0A000", "COPY not supported in multi-statement queries")
		return true, nil
	}

	// Lifecycle log pair: every per-statement run in a multi-statement
	// simple-query batch gets a logQueryStarted / logQueryFinished bracket
	// (PR #519). The deferred close captures whichever code path the
	// statement took — DML, SELECT, retry, transaction-conflict recovery.
	queryStart := time.Now()
	var queryRowsAff int64
	var queryFinalErr error
	c.logQueryStarted(executedQuery)
	defer func() {
		c.logQueryFinished(executedQuery, queryStart, queryRowsAff, queryFinalErr)
	}()

	if !queryReturnsResults(executedQuery) {
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = c.writeCommandComplete("BEGIN")
			return false, nil
		}

		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(cmdType)

		ctx, cleanup := c.queryContext()
		defer cleanup()

		runExec := func() (ExecResult, error) {
			execResult, err := c.executor.ExecContext(ctx, executedQuery)
			if err != nil {
				fallbackResult, handled, fallbackErr := c.execCompatibilityFallback(executedQuery, err, func(fallbackQuery string) (ExecResult, error) {
					return c.executor.ExecContext(ctx, fallbackQuery)
				})
				if handled {
					return fallbackResult, fallbackErr
				}
			}
			return execResult, err
		}

		execResult, err := runExec()
		if err != nil {
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
			if err != nil {
				queryFinalErr = err
				errCode := classifyErrorCode(err)
				errMsg := err.Error()
				if c.isCallerCancellation(err) {
					errMsg = "canceling statement due to user request"
				} else {
					c.logQueryError(executedQuery, err)
				}
				c.sendError("ERROR", errCode, errMsg)
				c.setTxError()
				c.logQuery(start, query, executedQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
				return true, nil
			}
		}

		var writtenRows int64
		if execResult != nil {
			writtenRows, _ = execResult.RowsAffected()
		}
		queryRowsAff = writtenRows
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, execResult)
		_ = c.writeCommandComplete(tag)
		c.logQuery(start, query, executedQuery, cmdType, 0, writtenRows, "", "", "simple-batch")
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
		queryFinalErr = err
		errCode := classifyErrorCode(err)
		errMsg := err.Error()
		if c.isCallerCancellation(err) {
			errMsg = "canceling statement due to user request"
		} else {
			c.logQueryError(executedQuery, err)
		}
		c.sendError("ERROR", errCode, errMsg)
		c.setTxError()
		c.logQuery(start, query, executedQuery, cmdType, 0, 0, errCode, errMsg, "simple-batch")
		return true, nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		queryFinalErr = err
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return true, nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		queryFinalErr = err
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return true, nil
	}

	if err := c.sendRowDescription(cols, colTypes); err != nil {
		queryFinalErr = err
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
			queryFinalErr = err
			c.sendError("ERROR", "42000", err.Error())
			return true, nil
		}

		if err := c.sendDataRowWithFormats(values, nil, typeOIDs); err != nil {
			queryFinalErr = err
			return false, err
		}
		rowCount++
	}
	queryRowsAff = int64(rowCount)

	c.updateTxStatus(cmdType)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	c.logQuery(start, query, executedQuery, cmdType, int64(rowCount), 0, "", "", "simple-batch")
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
func (c *clientConn) executeMultiStatement(statements []string, cleanup []string) error {
	if len(statements) == 0 {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
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

	// Execute setup statements (all but last). Each step is its own
	// logical query on the worker, so each gets its own logQueryStarted /
	// logQueryFinished pair (PR #519).
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		c.logger().Debug("Multi-stmt setup.", "step", i+1, "total", len(statements)-1, "stmt", stmt)
		setupStart := time.Now()
		c.logQueryStarted(stmt)
		result, err := c.executor.Exec(stmt)
		var setupRows int64
		if result != nil {
			setupRows, _ = result.RowsAffected()
		}
		c.logQueryFinished(stmt, setupStart, setupRows, err)
		if err != nil {
			c.logger().Error("Multi-stmt setup error.", "query", stmt, "error", err)
			c.setTxError()
			// On error, still try to cleanup (best effort)
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
	}

	// Handle final statement
	finalStmt := statements[len(statements)-1]
	upperFinal := strings.ToUpper(strings.TrimSpace(finalStmt))
	cmdType := c.getCommandType(upperFinal)
	c.logger().Debug("Multi-stmt final.", "stmt", finalStmt, "cmd_type", cmdType)

	finalStart := time.Now()
	var finalRowsAff int64
	var finalErr error
	c.logQueryStarted(finalStmt)
	defer func() {
		c.logQueryFinished(finalStmt, finalStart, finalRowsAff, finalErr)
	}()

	if queryReturnsResults(finalStmt) {
		// Result-returning query: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.executor.Query(finalStmt)
		if err != nil {
			finalErr = err
			c.logger().Error("Multi-stmt final query error.", "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		// DuckDB cursor holds result data even after source tables are dropped
		c.executeCleanup(cleanup)

		// Now stream results from cursor. streamRowsToClient counts rows
		// internally; we approximate Finished's rowsAff with 0 here
		// (logQuery's own structured channel still records the precise
		// count). A future refactor can plumb the count out of
		// streamRowsToClient.
		err = c.streamRowsToClient(rows, cmdType, finalStmt)
		if err != nil {
			finalErr = err
		}
		return err

	} else {
		// Non-result query (DML without RETURNING, DDL, etc.): execute then cleanup
		result, err := c.executor.Exec(finalStmt)
		if err != nil {
			finalErr = err
			c.logger().Error("Multi-stmt final exec error.", "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		if result != nil {
			finalRowsAff, _ = result.RowsAffected()
		}

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
		_ = c.writeReadyForQuery(c.txStatus)
		return c.flushWriter()
	}
}

// executeCleanup runs cleanup statements, ignoring errors (best effort).
// This is used to clean up temp tables after a multi-statement query.
func (c *clientConn) executeCleanup(cleanup []string) {
	for _, stmt := range cleanup {
		c.logger().Debug("Multi-stmt cleanup.", "stmt", stmt)
		_, err := c.executor.Exec(stmt)
		if err != nil {
			// Log but don't fail - cleanup is best effort
			c.logger().Warn("Multi-stmt cleanup error (ignored).", "error", err)
		}
	}
}

// executeMultiStatementExtended handles execution of multi-statement query rewrites
// for the extended query protocol (Parse/Bind/Execute).
// Unlike executeMultiStatement, this does NOT send ReadyForQuery (that's done by Sync).
func (c *clientConn) executeMultiStatementExtended(p *portal, statements []string, cleanup []string, args []interface{}, resultFormats []int16, described bool) {
	if len(statements) == 0 {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		return
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

	// Execute setup statements (all but last). Each step gets its own
	// logQueryStarted / logQueryFinished pair (PR #519) so multi-stmt
	// setup work is observable per statement, not just per outer query.
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		c.logger().Debug("Multi-stmt-ext setup.", "step", i+1, "total", len(statements)-1, "stmt", stmt)
		setupStart := time.Now()
		c.logQueryStarted(stmt)
		result, err := c.execPortal(p, stmt, args)
		var setupRows int64
		if result != nil {
			setupRows, _ = result.RowsAffected()
		}
		c.logQueryFinished(stmt, setupStart, setupRows, err)
		if err != nil {
			c.logger().Error("Multi-stmt-ext setup error.", "query", stmt, "error", err)
			c.setTxError()
			// On error, still try to cleanup (best effort)
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			return
		}
	}

	// Handle final statement
	finalStmt := statements[len(statements)-1]
	upperFinal := strings.ToUpper(strings.TrimSpace(finalStmt))
	cmdType := c.getCommandType(upperFinal)
	c.logger().Debug("Multi-stmt-ext final.", "stmt", finalStmt, "cmd_type", cmdType)

	finalStart := time.Now()
	var finalRowsAff int64
	var finalErr error
	c.logQueryStarted(finalStmt)
	defer func() {
		c.logQueryFinished(finalStmt, finalStart, finalRowsAff, finalErr)
	}()

	if queryReturnsResults(finalStmt) {
		// Result-returning query: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.queryPortal(p, finalStmt, args)
		if err != nil {
			finalErr = err
			c.logger().Error("Multi-stmt-ext final query error.", "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			return
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		c.executeCleanup(cleanup)

		// Stream results from cursor (extended protocol version). Row count
		// is tracked by streamRowsToClientExtended; the deferred Finished
		// log uses 0 as an approximation (logQuery still records the
		// precise count via the structured channel).
		c.streamRowsToClientExtended(p, rows, cmdType, resultFormats, described, finalStmt)

	} else {
		// Non-result query (DML without RETURNING, DDL, etc.): execute then cleanup
		result, err := c.execPortal(p, finalStmt, args)
		if err != nil {
			finalErr = err
			c.logger().Error("Multi-stmt-ext final exec error.", "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			return
		}
		if result != nil {
			finalRowsAff, _ = result.RowsAffected()
		}

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion (no ReadyForQuery - that's done by Sync)
		tag := c.buildCommandTag(cmdType, result)
		_ = c.writeCommandComplete(tag)
	}
}
