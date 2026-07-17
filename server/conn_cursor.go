package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/server/wire"
)

// --- Server-side cursor emulation ---

// deparseInnerQuery deparses a pg_query Node back to SQL text.
func deparseInnerQuery(node *pg_query.Node) string {
	tree := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: node}},
	}
	sql, err := pg_query.Deparse(tree)
	if err != nil {
		return ""
	}
	return sql
}

// openCursor executes the cursor's stored query and caches the result set metadata.
func (c *clientConn) openCursor(cursor *cursorState) error {
	ctx, cleanup := c.queryContextForCursor()
	cursor.cleanup = cleanup

	// The cursor's underlying SELECT is a physical worker statement at OPEN
	// time. Rows stream through later FETCH calls, so this records only the
	// metadata phase (rows=0) and its initial failure, if any.
	statement := workerStatementWithQuery(workerOriginCursor, workerOperationCursorOpen, cursor.query)
	cursorStart := time.Now()
	var workerErr error
	c.logWorkerStatementStarted(statement)
	defer func() {
		c.logWorkerStatementFinished(statement, cursorStart, 0, workerErr)
	}()

	rows, err := c.executor.QueryContext(ctx, cursor.query)
	if err != nil {
		workerErr = err
		cleanup()
		cursor.cleanup = nil
		return err
	}
	cursor.rows = rows

	cols, err := rows.Columns()
	if err != nil {
		workerErr = err
		_ = rows.Close()
		cursor.rows = nil
		cleanup()
		cursor.cleanup = nil
		return err
	}
	cursor.cols = cols

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		workerErr = err
		_ = rows.Close()
		cursor.rows = nil
		cleanup()
		cursor.cleanup = nil
		return err
	}
	cursor.colTypes = colTypes

	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}
	cursor.typeOIDs = typeOIDs
	return nil
}

// closeCursor closes a specific cursor and cleans up its resources.
func (c *clientConn) closeCursor(name string) {
	cursor, ok := c.cursors[name]
	if !ok {
		return
	}
	if cursor.rows != nil {
		_ = cursor.rows.Close()
	}
	if cursor.cleanup != nil {
		cursor.cleanup()
	}
	delete(c.cursors, name)
}

// closeAllCursors closes all open cursors on this connection.
func (c *clientConn) closeAllCursors() {
	for name := range c.cursors {
		c.closeCursor(name)
	}
}

// closeCursorsAtTxEnd closes all open cursors before a COMMIT/ROLLBACK
// statement executes. PostgreSQL destroys non-holdable cursors at transaction
// end; doing it BEFORE the statement runs (rather than after, in
// updateTxStatus) is required for liveness, not just compatibility: the
// session's DuckDB pool is capped at one connection (openBaseDB), so a
// partially-read cursor's open rowset holds the only connection and the
// COMMIT/ROLLBACK would block on it forever. updateTxStatus keeps its
// post-exec close as a backstop for paths that don't call this hook.
func (c *clientConn) closeCursorsAtTxEnd(cmdType string) {
	if cmdType != "COMMIT" && cmdType != "ROLLBACK" {
		return
	}
	c.closeAllCursors()
}

// getCursorSchema opens the cursor if needed to retrieve column metadata,
// then returns the schema information. Used by handleDescribe for FETCH statements.
func (c *clientConn) getCursorSchema(cursorName string) ([]string, []ColumnTyper, error) {
	cursor, ok := c.cursors[cursorName]
	if !ok {
		return nil, nil, fmt.Errorf("cursor %q does not exist", cursorName)
	}
	if cursor.rows == nil {
		if err := c.openCursor(cursor); err != nil {
			return nil, nil, err
		}
	}
	return cursor.cols, cursor.colTypes, nil
}

// isFetchForwardOnly returns true if the FetchStmt direction is forward-compatible.
func isFetchForwardOnly(dir pg_query.FetchDirection) bool {
	return dir == pg_query.FetchDirection_FETCH_DIRECTION_UNDEFINED ||
		dir == pg_query.FetchDirection_FETCH_FORWARD
}

// pgCursorsLiteralRegex matches pg_cursors queries with a literal name value:
//
//	SELECT 1 FROM pg_cursors WHERE name = 'cursor_name'
//	SELECT 1 FROM pg_catalog.pg_cursors WHERE name = 'cursor_name'
var pgCursorsLiteralRegex = regexp.MustCompile(
	`(?i)^\s*SELECT\s+.+\s+FROM\s+(?:pg_catalog\s*\.\s*)?pg_cursors\s+WHERE\s+name\s*=\s*'([^']*)'`,
)

// pgCursorsParamRegex matches pg_cursors queries with a parameterized name:
//
//	SELECT 1 FROM pg_cursors WHERE name = $1
var pgCursorsParamRegex = regexp.MustCompile(
	`(?i)^\s*SELECT\s+.+\s+FROM\s+(?:pg_catalog\s*\.\s*)?pg_cursors\s+WHERE\s+name\s*=\s*\$1\s*$`,
)

// matchPgCursorsQuery checks if a query is a pg_cursors lookup and extracts the cursor name.
// Returns the cursor name and true for literal queries, empty string and true for parameterized queries.
func matchPgCursorsQuery(query string) (cursorName string, parameterized bool, ok bool) {
	if !strings.Contains(query, "pg_cursors") {
		return "", false, false
	}
	if m := pgCursorsLiteralRegex.FindStringSubmatch(query); m != nil {
		return m[1], false, true
	}
	if pgCursorsParamRegex.MatchString(query) {
		return "", true, true
	}
	return "", false, false
}

// handlePgCursorsQuery handles SELECT FROM pg_cursors in the Simple Query protocol.
// Returns a single row with value "1" if the cursor exists, or zero rows if not.
func (c *clientConn) handlePgCursorsQuery(cursorName string) error {
	_, exists := c.cursors[cursorName]

	// Send RowDescription: single int4 column named "?column?"
	if err := c.sendPgCursorsRowDescriptionWithFormats(nil); err != nil {
		return err
	}

	rowCount := 0
	if exists {
		if err := c.sendDataRowWithFormats([]interface{}{int64(1)}, nil, []int32{23}); err != nil {
			return err
		}
		rowCount = 1
	}

	_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", rowCount))
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// handlePgCursorsQueryExtended handles SELECT FROM pg_cursors in the Extended Query protocol.
func (c *clientConn) handlePgCursorsQueryExtended(p *portal) {
	// Resolve cursor name: either from literal in query or from bind parameter
	cursorName := p.stmt.cursorName
	if cursorName == "" && len(p.paramValues) > 0 && p.paramValues[0] != nil {
		cursorName = string(p.paramValues[0])
	}

	_, exists := c.cursors[cursorName]

	if !p.stmt.described {
		_ = c.sendPgCursorsRowDescriptionWithFormats(p.resultFormats)
	}

	rowCount := 0
	if exists {
		_ = c.sendDataRowWithFormats([]interface{}{int64(1)}, p.resultFormats, []int32{23})
		rowCount = 1
	}

	_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", rowCount))
}

// sendPgCursorsRowDescription sends a RowDescription for a pg_cursors query result (single int4 column).
func (c *clientConn) sendPgCursorsRowDescriptionWithFormats(formatCodes []int16) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(1)) // 1 column
	buf.WriteString("?column?")
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, int32(0))  // table OID
	_ = binary.Write(&buf, binary.BigEndian, int16(0))  // column attr
	_ = binary.Write(&buf, binary.BigEndian, int32(23)) // int4 OID
	_ = binary.Write(&buf, binary.BigEndian, int16(4))  // type size
	_ = binary.Write(&buf, binary.BigEndian, int32(-1)) // typmod
	var format int16
	if len(formatCodes) == 1 {
		format = formatCodes[0]
	} else if len(formatCodes) > 0 {
		format = formatCodes[0]
	}
	_ = binary.Write(&buf, binary.BigEndian, format)
	return wire.WriteMessage(c.writer, wire.MsgRowDescription, buf.Bytes())
}

// handleDeclareCursor handles DECLARE cursor in the Simple Query protocol.
func (c *clientConn) handleDeclareCursor(query string, stmt *pg_query.DeclareCursorStmt) error {
	start := time.Now()
	innerSQL := deparseInnerQuery(stmt.Query)
	if innerSQL == "" {
		c.sendError("ERROR", "42601", "could not deparse cursor query")
		c.logQuery(start, query, query, "DECLARE", 0, 0, "42601", "could not deparse cursor query", "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Transpile the inner SELECT query (skip for passthrough users)
	transpiledSQL := innerSQL
	if !c.passthrough {
		tr := c.newTranspiler(false)
		result, err := tr.Transpile(innerSQL)
		if err != nil {
			errMsg := fmt.Sprintf("syntax error in cursor query: %v", err)
			c.sendError("ERROR", "42601", errMsg)
			c.logQuery(start, query, query, "DECLARE", 0, 0, "42601", errMsg, "simple")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		transpiledSQL = result.SQL
		if result.FallbackToNative {
			transpiledSQL = innerSQL
		}
	}

	// Close existing cursor with same name (PostgreSQL behavior)
	c.closeCursor(stmt.Portalname)

	c.cursors[stmt.Portalname] = &cursorState{query: transpiledSQL}
	c.logger().Debug("Cursor declared.", "cursor", stmt.Portalname, "query", transpiledSQL)

	_ = c.writeCommandComplete("DECLARE CURSOR")
	c.logQuery(start, query, query, "DECLARE", 0, 0, "", "", "simple")
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// handleFetchCursor handles FETCH in the Simple Query protocol.
func (c *clientConn) handleFetchCursor(query string, stmt *pg_query.FetchStmt) error {
	start := time.Now()
	// Validate direction
	if !isFetchForwardOnly(stmt.Direction) {
		c.sendError("ERROR", "0A000", "cursor can only scan forward")
		c.logQuery(start, query, query, "FETCH", 0, 0, "0A000", "cursor can only scan forward", "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	cursor, ok := c.cursors[stmt.Portalname]
	if !ok {
		errMsg := fmt.Sprintf("cursor %q does not exist", stmt.Portalname)
		c.sendError("ERROR", "34000", errMsg)
		c.logQuery(start, query, query, "FETCH", 0, 0, "34000", errMsg, "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Open cursor on first FETCH
	if cursor.rows == nil {
		if err := c.openCursor(cursor); err != nil {
			errCode := "42000"
			errMsg := err.Error()
			if c.isCallerCancellation(err) {
				errCode = "57014"
				errMsg = "canceling statement due to user request"
			}
			c.sendError("ERROR", errCode, errMsg)
			c.setTxError()
			c.logQuery(start, query, query, "FETCH", 0, 0, errCode, errMsg, "simple")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
	}

	// Determine how many rows to fetch (pg_query sets HowMany=MaxInt64 for FETCH ALL)
	howMany := stmt.HowMany
	if howMany < 0 {
		c.sendError("ERROR", "0A000", "cursor can only scan forward")
		c.logQuery(start, query, query, "FETCH", 0, 0, "0A000", "cursor can only scan forward", "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// MOVE: advance position without returning rows
	if stmt.Ismove {
		moveCount := int64(0)
		for moveCount < howMany && cursor.rows.Next() {
			// Read the row to advance position, but don't send it
			values := make([]interface{}, len(cursor.cols))
			valuePtrs := make([]interface{}, len(cursor.cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			_ = cursor.rows.Scan(valuePtrs...)
			moveCount++
		}
		_ = c.writeCommandComplete(fmt.Sprintf("MOVE %d", moveCount))
		c.logQuery(start, query, query, "FETCH", moveCount, 0, "", "", "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Send RowDescription
	if err := c.sendRowDescription(cursor.cols, cursor.colTypes); err != nil {
		return err
	}

	// Stream rows
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
			c.logQuery(start, query, query, "FETCH", 0, 0, "42000", err.Error(), "simple")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}

		if err := c.sendDataRowWithFormats(values, nil, cursor.typeOIDs); err != nil {
			return err
		}
		rowCount++
	}

	if err := cursor.rows.Err(); err != nil {
		errCode := "42000"
		errMsg := err.Error()
		if c.isCallerCancellation(err) {
			errCode = "57014"
			errMsg = "canceling statement due to user request"
		}
		c.sendError("ERROR", errCode, errMsg)
		c.setTxError()
		c.logQuery(start, query, query, "FETCH", 0, 0, errCode, errMsg, "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	_ = c.writeCommandComplete(fmt.Sprintf("FETCH %d", rowCount))
	c.logQuery(start, query, query, "FETCH", rowCount, 0, "", "", "simple")
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// handleCloseCursor handles CLOSE in the Simple Query protocol.
func (c *clientConn) handleCloseCursor(query string, stmt *pg_query.ClosePortalStmt) error {
	start := time.Now()
	if stmt.Portalname == "" {
		// CLOSE ALL
		c.closeAllCursors()
	} else {
		if _, ok := c.cursors[stmt.Portalname]; !ok {
			errMsg := fmt.Sprintf("cursor %q does not exist", stmt.Portalname)
			c.sendError("ERROR", "34000", errMsg)
			c.logQuery(start, query, query, "CLOSE", 0, 0, "34000", errMsg, "simple")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		c.closeCursor(stmt.Portalname)
	}

	c.logger().Debug("Cursor closed.", "cursor", stmt.Portalname)
	_ = c.writeCommandComplete("CLOSE CURSOR")
	c.logQuery(start, query, query, "CLOSE", 0, 0, "", "", "simple")
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// handleDeclareCursorExtended handles DECLARE cursor in the Extended Query protocol.
func (c *clientConn) handleDeclareCursorExtended(p *portal) {
	// Close existing cursor with same name
	c.closeCursor(p.stmt.cursorName)

	c.cursors[p.stmt.cursorName] = &cursorState{query: p.stmt.cursorQuery}
	c.logger().Debug("Cursor declared (extended).", "cursor", p.stmt.cursorName, "query", p.stmt.cursorQuery)

	_ = c.writeCommandComplete("DECLARE CURSOR")
}

// handleFetchCursorExtended handles FETCH in the Extended Query protocol.
func (c *clientConn) handleFetchCursorExtended(p *portal) {
	cursor, ok := c.cursors[p.stmt.cursorName]
	if !ok {
		c.sendError("ERROR", "34000", fmt.Sprintf("cursor %q does not exist", p.stmt.cursorName))
		return
	}

	// Open cursor on first FETCH
	if cursor.rows == nil {
		if err := c.openCursor(cursor); err != nil {
			if c.isCallerCancellation(err) {
				c.sendError("ERROR", "57014", "canceling statement due to user request")
			} else {
				c.sendError("ERROR", "42000", err.Error())
			}
			c.setTxError()
			return
		}
	}

	howMany := p.stmt.fetchCount

	// MOVE: advance position without returning rows (mirrors the
	// simple-protocol path in handleFetchCursor).
	if p.stmt.cursorIsMove {
		moveCount := int64(0)
		for moveCount < howMany && cursor.rows.Next() {
			// Read the row to advance position, but don't send it
			values := make([]interface{}, len(cursor.cols))
			valuePtrs := make([]interface{}, len(cursor.cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			_ = cursor.rows.Scan(valuePtrs...)
			moveCount++
		}
		_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("MOVE %d", moveCount))
		return
	}

	// Send RowDescription if Describe wasn't already called
	if !p.described && len(cursor.cols) > 0 {
		if err := c.sendRowDescriptionWithFormats(cursor.cols, cursor.colTypes, p.resultFormats); err != nil {
			return
		}
	}

	// Stream rows
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
			return
		}

		if err := c.sendDataRowWithFormats(values, p.resultFormats, cursor.typeOIDs); err != nil {
			return
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
		return
	}

	_ = c.writeCommandComplete(fmt.Sprintf("FETCH %d", rowCount))
}

// handleCloseCursorExtended handles CLOSE cursor in the Extended Query protocol.
func (c *clientConn) handleCloseCursorExtended(p *portal) {
	if p.stmt.cursorName == "" {
		c.closeAllCursors()
	} else {
		if _, ok := c.cursors[p.stmt.cursorName]; !ok {
			c.sendError("ERROR", "34000", fmt.Sprintf("cursor %q does not exist", p.stmt.cursorName))
			return
		}
		c.closeCursor(p.stmt.cursorName)
	}

	c.logger().Debug("Cursor closed (extended).", "cursor", p.stmt.cursorName)
	_ = c.writeCommandComplete("CLOSE CURSOR")
}
