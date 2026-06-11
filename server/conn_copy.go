package server

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/posthog/duckgres/duckdbservice/arrowmap"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/wire"
)

func shouldHandleCopyBeforeTranspile(query string) bool {
	trimmed := strings.TrimSpace(query)
	return strings.HasPrefix(strings.ToUpper(trimmed), "COPY")
}

// Regular expressions for parsing COPY commands
var (
	copyToStdoutRegex   = regexp.MustCompile(`(?i)COPY\s+(.+?)\s+TO\s+STDOUT`)
	copyFromStdinRegex  = regexp.MustCompile(`(?i)COPY\s+(\S+)\s*(?:\(([^)]+)\)\s*)?FROM\s+STDIN`)
	copyBinaryRegex     = regexp.MustCompile(`(?i)\b(?:STDIN|STDOUT)\b(?:[^;]*\bFORMAT\s+(?:"?binary"?|BINARY)\b|(?:\s+WITH)?\s+BINARY\b)`)
	copyWithCSVRegex    = regexp.MustCompile(`(?i)\bCSV\b`)
	copyWithHeaderRegex = regexp.MustCompile(`(?i)\bHEADER\b`)
	copyDelimiterRegex  = regexp.MustCompile(`(?i)\bDELIMITER\s+['"](.)['"]\b`)
	copyNullRegex       = regexp.MustCompile(`(?i)\bNULL\s+'([^']*)'`)
	copyQuoteRegex      = regexp.MustCompile(`(?i)\bQUOTE\s+['"](.)['"]\s*`)
	copyEscapeRegex     = regexp.MustCompile(`(?i)\bESCAPE\s+['"](.)['"]\s*`)
)

// stripPublicSchema maps PostgreSQL's "public" schema to DuckDB's default schema
// in COPY table names. Handles both quoted ("public"."table") and unquoted (public.table) forms.
// This mirrors the transpiler's PublicSchemaTransform but operates on raw table name strings
// since COPY commands bypass the SQL transpiler (pg_query can't parse COPY ... FORMAT BINARY).
func stripPublicSchema(tableName string) string {
	// Quoted form: "public"."tablename" → "tablename"
	if strings.HasPrefix(tableName, `"public".`) {
		return tableName[len(`"public".`):]
	}
	// Unquoted form: public.tablename → tablename
	if strings.HasPrefix(strings.ToLower(tableName), "public.") {
		return tableName[len("public."):]
	}
	return tableName
}

// CopyFromOptions contains parsed options from a COPY FROM STDIN command
type CopyFromOptions struct {
	TableName  string
	ColumnList string // Empty string or "(col1, col2, ...)"
	Delimiter  string
	HasHeader  bool
	NullString string
	Quote      string // Quote character (default " for CSV)
	Escape     string // Escape character (default same as Quote)
	IsBinary   bool   // True if FORMAT binary
}

// ParseCopyFromOptions extracts options from a COPY FROM STDIN command
func ParseCopyFromOptions(query string) (*CopyFromOptions, error) {
	upperQuery := strings.ToUpper(query)

	matches := copyFromStdinRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid COPY FROM STDIN syntax")
	}

	opts := &CopyFromOptions{
		TableName:  stripPublicSchema(matches[1]),
		Delimiter:  "\t",  // Default PostgreSQL text format delimiter
		NullString: "\\N", // Default PostgreSQL null representation
	}

	// Extract column list if present
	if len(matches) > 2 && matches[2] != "" {
		opts.ColumnList = fmt.Sprintf("(%s)", matches[2])
	}

	// Detect binary format
	if copyBinaryRegex.MatchString(upperQuery) {
		opts.IsBinary = true
		return opts, nil
	}

	// Parse delimiter
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		opts.Delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		opts.Delimiter = ","
	}

	// Parse header option (only valid with CSV)
	opts.HasHeader = copyWithCSVRegex.MatchString(upperQuery) && copyWithHeaderRegex.MatchString(upperQuery)

	// Parse NULL string option
	if m := copyNullRegex.FindStringSubmatch(query); len(m) > 1 {
		opts.NullString = m[1]
	}

	// Parse QUOTE option (default " for CSV)
	if m := copyQuoteRegex.FindStringSubmatch(query); len(m) > 1 {
		opts.Quote = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		opts.Quote = `"` // Default quote character for CSV
	}

	// Parse ESCAPE option (default same as QUOTE)
	if m := copyEscapeRegex.FindStringSubmatch(query); len(m) > 1 {
		opts.Escape = m[1]
	}

	return opts, nil
}

// CopyToOptions contains parsed options from a COPY TO STDOUT command
type CopyToOptions struct {
	Source    string // Table name or (SELECT query)
	Delimiter string
	HasHeader bool
	IsQuery   bool // True if Source is a query in parentheses
}

// ParseCopyToOptions extracts options from a COPY TO STDOUT command
func ParseCopyToOptions(query string) (*CopyToOptions, error) {
	upperQuery := strings.ToUpper(query)

	matches := copyToStdoutRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid COPY TO STDOUT syntax")
	}

	source := strings.TrimSpace(matches[1])
	opts := &CopyToOptions{
		Source:    source,
		Delimiter: "\t", // Default PostgreSQL text format delimiter
		IsQuery:   strings.HasPrefix(source, "(") && strings.HasSuffix(source, ")"),
	}

	// Parse delimiter
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		opts.Delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		opts.Delimiter = ","
	}

	// Parse header option (only valid with CSV)
	opts.HasHeader = copyWithCSVRegex.MatchString(upperQuery) && copyWithHeaderRegex.MatchString(upperQuery)

	return opts, nil
}

// BuildDuckDBCopyFromSQL generates a DuckDB COPY FROM statement
func BuildDuckDBCopyFromSQL(tableName, columnList, filePath string, opts *CopyFromOptions) string {
	// DuckDB syntax: COPY table FROM 'file' (FORMAT CSV, HEADER, NULL 'value', DELIMITER ',', QUOTE '"')
	// AUTO_DETECT FALSE disables sniffer to prevent it from overriding our settings
	// STRICT_MODE FALSE allows reading rows that don't strictly comply with CSV standard
	// PARALLEL FALSE avoids "Parallel CSV Reader does not support full read" errors
	// on files streamed from COPY FROM STDIN (temp files with no seek support for sniffing)
	copyOptions := []string{"FORMAT CSV", "AUTO_DETECT FALSE", "STRICT_MODE FALSE", "PARALLEL FALSE", "MAX_LINE_SIZE 10485760"}
	if opts.HasHeader {
		copyOptions = append(copyOptions, "HEADER")
	}
	// Always specify NULL string - DuckDB doesn't recognize \N by default
	copyOptions = append(copyOptions, fmt.Sprintf("NULL '%s'", opts.NullString))
	// Always specify DELIMITER explicitly (required when AUTO_DETECT is FALSE)
	copyOptions = append(copyOptions, fmt.Sprintf("DELIMITER '%s'", opts.Delimiter))
	// Always specify QUOTE for CSV to ensure proper quote handling
	if opts.Quote != "" {
		copyOptions = append(copyOptions, fmt.Sprintf("QUOTE '%s'", opts.Quote))
		// Set ESCAPE to match QUOTE for RFC 4180 compliance (doubled quotes = escaped quote)
		escape := opts.Escape
		if escape == "" {
			escape = opts.Quote
		}
		copyOptions = append(copyOptions, fmt.Sprintf("ESCAPE '%s'", escape))
	} else if opts.Escape != "" {
		copyOptions = append(copyOptions, fmt.Sprintf("ESCAPE '%s'", opts.Escape))
	}

	return fmt.Sprintf("COPY %s %s FROM '%s' (%s)",
		tableName, columnList, filePath, strings.Join(copyOptions, ", "))
}

// handleCopy handles COPY TO STDOUT and COPY FROM STDIN commands
func (c *clientConn) handleCopy(query, upperQuery string) error {
	start := time.Now()

	// Check if it's COPY TO STDOUT
	if copyToStdoutRegex.MatchString(upperQuery) {
		return c.handleCopyOut(query, upperQuery)
	}

	// Check if it's COPY FROM STDIN
	if copyFromStdinRegex.MatchString(upperQuery) {
		return c.handleCopyIn(query, upperQuery)
	}

	// For other COPY commands (e.g., COPY TO file), pass through to DuckDB
	c.logQueryStarted(query)
	queryStart := time.Now()
	result, err := c.executor.Exec(query)
	var rowsAffected int64
	if result != nil {
		rowsAffected, _ = result.RowsAffected()
	}
	c.logQueryFinished(query, queryStart, rowsAffected, err)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowsAffected))
	c.logQuery(start, query, query, "COPY", 0, rowsAffected, "", "", "simple")
	_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyOut handles COPY ... TO STDOUT
func (c *clientConn) handleCopyOut(query, upperQuery string) error {
	start := time.Now()
	matches := copyToStdoutRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		c.sendError("ERROR", "42601", "Invalid COPY TO STDOUT syntax")
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42601", "Invalid COPY TO STDOUT syntax", "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// The source can be a table name or a query in parentheses
	source := strings.TrimSpace(matches[1])
	var selectQuery string
	if strings.HasPrefix(source, "(") && strings.HasSuffix(source, ")") {
		selectQuery = source[1 : len(source)-1]
	} else {
		selectQuery = fmt.Sprintf("SELECT * FROM %s", source)
	}

	// Transpile the inner SELECT to handle schema mappings (e.g., public -> main)
	// The outer COPY statement may not have been transpiled if pg_query can't parse
	// the full COPY syntax (e.g., FORMAT "binary").
	// Skip for passthrough users who send DuckDB-native SQL.
	if !c.passthrough {
		tr := c.newTranspiler(false)
		if result, err := tr.Transpile(selectQuery); err == nil && !result.FallbackToNative {
			selectQuery = result.SQL
		}
	}

	// Execute the query. Lifecycle log pair (PR #519): every COPY-OUT
	// driven SELECT gets a logQueryStarted and a logQueryFinished — the
	// row count comes from the iteration loop further down, so the
	// deferred close in the outer function path captures it.
	queryStart := time.Now()
	var copyRowsRead int64
	var copyFinalErr error
	c.logQueryStarted(selectQuery)
	defer func() {
		c.logQueryFinished(selectQuery, queryStart, copyRowsRead, copyFinalErr)
	}()
	rows, err := c.executor.Query(selectQuery)
	if err != nil {
		copyFinalErr = err
		c.logger().Error("COPY TO query failed.", "query", selectQuery, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		c.logger().Error("COPY TO failed to get columns.", "query", selectQuery, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	isBinary := copyBinaryRegex.MatchString(query)

	if isBinary {
		return c.handleCopyOutBinary(query, rows, cols)
	}

	// Get column types for JSON-aware formatting
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		copyFinalErr = err
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Parse text/CSV options
	delimiter := "\t"
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		delimiter = ","
	}

	// Send CopyOutResponse (text format)
	if err := wire.WriteCopyOutResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	_ = c.writer.Flush()

	// Send header if CSV with HEADER
	if copyWithCSVRegex.MatchString(upperQuery) && copyWithHeaderRegex.MatchString(upperQuery) {
		header := strings.Join(cols, delimiter) + "\n"
		if err := wire.WriteCopyData(c.writer, []byte(header)); err != nil {
			return err
		}
	}

	// Send data rows
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			copyFinalErr = err
			c.sendError("ERROR", "42000", err.Error())
			c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "42000", err.Error(), "simple")
			break
		}

		// Format row as tab/comma separated values
		var rowData []string
		for i, v := range values {
			if typeOIDs[i] == OidJSON || typeOIDs[i] == OidJSONB {
				rowData = append(rowData, string(encodeJSON(v)))
			} else {
				rowData = append(rowData, c.formatCopyValue(v))
			}
		}
		line := strings.Join(rowData, delimiter) + "\n"
		if err := wire.WriteCopyData(c.writer, []byte(line)); err != nil {
			copyFinalErr = err
			return err
		}
		rowCount++
	}
	copyRowsRead = int64(rowCount)

	if err := rows.Err(); err != nil {
		copyFinalErr = err
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Send CopyDone
	if err := wire.WriteCopyDone(c.writer); err != nil {
		copyFinalErr = err
		return err
	}

	_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "", "", "simple")
	_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyOutBinary handles COPY ... TO STDOUT (FORMAT binary)
// Implements PostgreSQL's binary COPY format: header, binary-encoded tuples, trailer.
// Sends one CopyData message per tuple, with header prepended to the first tuple
// and trailer appended to the last, matching how clients like DuckDB's postgres
// extension consume binary COPY streams via PQgetCopyData.
func (c *clientConn) handleCopyOutBinary(query string, rows RowSet, cols []string) error {
	start := time.Now()
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, 0, "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Get type OIDs for each column
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send CopyOutResponse (binary format)
	if err := wire.WriteCopyOutResponse(c.writer, int16(len(cols)), false); err != nil {
		return err
	}
	_ = c.writer.Flush()

	// Binary COPY header (19 bytes)
	binaryHeader := []byte{
		'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n', 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	// encodeTuple encodes a single tuple in PostgreSQL binary COPY format
	encodeTuple := func(values []interface{}) []byte {
		var buf bytes.Buffer
		_ = binary.Write(&buf, binary.BigEndian, int16(len(values)))
		for i, v := range values {
			if v == nil {
				_ = binary.Write(&buf, binary.BigEndian, int32(-1))
			} else {
				data := encodeBinary(v, typeOIDs[i])
				if data == nil {
					_ = binary.Write(&buf, binary.BigEndian, int32(-1))
				} else {
					_ = binary.Write(&buf, binary.BigEndian, int32(len(data)))
					buf.Write(data)
				}
			}
		}
		return buf.Bytes()
	}

	// Send each tuple as its own CopyData message.
	// The header is prepended to the first tuple's message.
	rowCount := 0
	firstRow := true
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "42000", err.Error(), "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}

		tupleBytes := encodeTuple(values)

		if firstRow {
			// First CopyData message: header + tuple
			msg := make([]byte, 0, int64(len(binaryHeader))+int64(len(tupleBytes)))
			msg = append(msg, binaryHeader...)
			msg = append(msg, tupleBytes...)
			if err := wire.WriteCopyData(c.writer, msg); err != nil {
				return err
			}
			firstRow = false
		} else {
			if err := wire.WriteCopyData(c.writer, tupleBytes); err != nil {
				return err
			}
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "42000", err.Error(), "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// If no rows, still need to send header + trailer
	if firstRow {
		// No rows at all: send header + trailer in one message
		msg := make([]byte, 0, len(binaryHeader)+2)
		msg = append(msg, binaryHeader...)
		msg = append(msg, 0xFF, 0xFF) // trailer: -1 as int16
		if err := wire.WriteCopyData(c.writer, msg); err != nil {
			return err
		}
	} else {
		// Send trailer as its own CopyData message
		if err := wire.WriteCopyData(c.writer, []byte{0xFF, 0xFF}); err != nil {
			return err
		}
	}

	// Send CopyDone
	if err := wire.WriteCopyDone(c.writer); err != nil {
		return err
	}

	_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	c.logQuery(start, query, query, "COPY", 0, int64(rowCount), "", "", "simple")
	_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyIn handles COPY ... FROM STDIN
func (c *clientConn) handleCopyIn(query, upperQuery string) error {
	copyStartTime := time.Now()
	c.logger().Debug("COPY FROM STDIN starting.", "query", query)

	// Parse COPY options using the helper function
	opts, err := ParseCopyFromOptions(query)
	if err != nil {
		c.sendError("ERROR", "42601", "Invalid COPY FROM STDIN syntax")
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "42601", "Invalid COPY FROM STDIN syntax", "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	tableName := opts.TableName
	columnList := opts.ColumnList
	c.logger().Debug("COPY FROM STDIN parsed.", "table", tableName, "columns", columnList, "binary", opts.IsBinary)

	// Get column info. If a column list is specified, query only those columns
	// in the specified order to match the binary data field order.
	var colQuery string
	if columnList != "" {
		// columnList is "(col1, col2, ...)" — use it in SELECT to get types in COPY order
		colQuery = fmt.Sprintf("SELECT %s FROM %s LIMIT 0", columnList[1:len(columnList)-1], tableName)
	} else {
		colQuery = fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	}
	testRows, err := c.executor.Query(colQuery)
	if err != nil {
		c.logger().Error("COPY FROM table check failed.", "table", tableName, "error", err)
		errMsg := fmt.Sprintf("relation \"%s\" does not exist", tableName)
		c.sendError("ERROR", "42P01", errMsg)
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "42P01", errMsg, "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	cols, _ := testRows.Columns()
	colTypes, _ := testRows.ColumnTypes()
	_ = testRows.Close()

	// Branch to binary handler if binary format
	if opts.IsBinary {
		return c.handleCopyInBinary(query, opts, cols, colTypes)
	}

	// Check for BLOB columns. DuckDB's CSV parser cannot handle raw binary data
	// in BLOB columns — it auto-detects the type but fails to parse the bytes.
	// Fall back to in-process CSV parsing with batched INSERT for these tables.
	var blobColIndices []int
	for i, ct := range colTypes {
		if ct.DatabaseTypeName() == "BLOB" {
			blobColIndices = append(blobColIndices, i)
		}
	}
	if len(blobColIndices) > 0 {
		c.logger().Debug("COPY FROM STDIN: table has BLOB columns, using CSV parse fallback.", "blob_columns", len(blobColIndices))
		return c.handleCopyInCSVWithBlob(query, opts, cols, colTypes, blobColIndices)
	}

	// Send CopyInResponse
	if err := wire.WriteCopyInResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	_ = c.writer.Flush()
	c.logger().Debug("COPY FROM STDIN sent CopyInResponse, waiting for data.")

	// Remote-worker (Flight) executors implement CopyFromStdinExecutor so the
	// CSV bytes are streamed to the worker pod via DoPut and spooled to the
	// worker's filesystem there. The legacy local-tempfile path below works
	// only when CP and worker share a filesystem (standalone / process
	// backend), so prefer the streaming path when it's available.
	if streamer, ok := c.executor.(sqlcore.CopyFromStdinExecutor); ok {
		return c.handleCopyInRemoteStreaming(query, opts, copyStartTime, streamer)
	}

	// Create temp file upfront and stream data directly to it (avoids memory buffering).
	// This approach leverages DuckDB's highly optimized CSV parser which handles
	// type conversions automatically and can load millions of rows in seconds.
	tmpFile, err := os.CreateTemp("", "duckgres-copy-*.csv")
	if err != nil {
		c.logger().Error("COPY FROM STDIN failed to create temp file.", "error", err)
		errMsg := fmt.Sprintf("failed to create temp file: %v", err)
		c.sendError("ERROR", "58000", errMsg)
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "58000", errMsg, "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	// Stream COPY data directly to temp file (no memory buffering)
	rowCount := 0
	copyDataMessages := 0
	bytesWritten := int64(0)
	dataReceiveStart := time.Now()

	for {
		msgType, body, err := wire.ReadMessage(c.reader)
		if err != nil {
			c.logger().Error("COPY FROM STDIN error reading message.", "error", err)
			_ = tmpFile.Close()
			return err
		}

		switch msgType {
		case wire.MsgCopyData:
			// Skip the PostgreSQL text COPY end-of-data marker (\.\n).
			// Some clients send this as a CopyData message before CopyDone.
			if (len(body) == 3 && body[0] == '\\' && body[1] == '.' && body[2] == '\n') ||
				(len(body) == 2 && body[0] == '\\' && body[1] == '.') {
				continue
			}
			n, err := tmpFile.Write(body)
			if err != nil {
				c.logger().Error("COPY FROM STDIN failed to write to temp file.", "error", err)
				_ = tmpFile.Close()
				errMsg := fmt.Sprintf("failed to write to temp file: %v", err)
				c.sendError("ERROR", "58000", errMsg)
				c.setTxError()
				c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "58000", errMsg, "simple")
				_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}
			bytesWritten += int64(n)
			copyDataMessages++
			if copyDataMessages%10000 == 0 {
				c.logger().Debug("COPY FROM STDIN progress.", "messages", copyDataMessages, "bytes", bytesWritten)
			}

		case wire.MsgCopyDone:
			_ = tmpFile.Close()
			dataReceiveElapsed := time.Since(dataReceiveStart)
			c.logger().Debug("COPY FROM STDIN CopyDone received.", "messages", copyDataMessages, "bytes", bytesWritten, "duration", dataReceiveElapsed)

			// Build DuckDB COPY FROM statement using the helper function
			copySQL := BuildDuckDBCopyFromSQL(tableName, columnList, tmpPath, opts)

			c.logger().Debug("COPY FROM STDIN executing native DuckDB COPY.", "sql", copySQL)
			loadStart := time.Now()

			// Lifecycle log pair (PR #519): the native DuckDB COPY FROM is
			// the actual query the worker runs; everything before this is
			// CSV byte-pumping into a tempfile, not worker work.
			c.logQueryStarted(copySQL)
			result, err := c.executor.Exec(copySQL)
			var copyRowsAffected int64
			if result != nil {
				copyRowsAffected, _ = result.RowsAffected()
			}
			c.logQueryFinished(copySQL, loadStart, copyRowsAffected, err)
			if err != nil {
				c.logger().Error("COPY FROM STDIN DuckDB COPY failed.", "error", err)
				errMsg := fmt.Sprintf("COPY failed: %v", err)
				c.sendError("ERROR", "22P02", errMsg)
				c.setTxError()
				c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "22P02", errMsg, "simple")
				_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			rowCount = int(copyRowsAffected)

			totalElapsed := time.Since(copyStartTime)
			loadElapsed := time.Since(loadStart)
			c.logger().Info("COPY FROM STDIN completed.", "rows", rowCount, "total_duration", totalElapsed, "load_duration", loadElapsed)

			_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "", "", "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		case wire.MsgCopyFail:
			// Client cancelled COPY
			errMsg := string(bytes.TrimRight(body, "\x00"))
			exception := fmt.Sprintf("COPY failed: %s", errMsg)
			c.sendError("ERROR", "57014", exception)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "57014", exception, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		default:
			errMsg := fmt.Sprintf("unexpected message type during COPY: %c", msgType)
			c.sendError("ERROR", "08P01", errMsg)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "08P01", errMsg, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}
}

// handleCopyInRemoteStreaming handles COPY FROM STDIN when the executor is
// remote (e.g. Flight, multitenant K8s worker). The control plane does not
// share a filesystem with the worker pod, so the legacy "spool to local
// /tmp, run COPY FROM <path>" approach fails with "No files found". This
// path streams the wire CopyData bytes through the executor's
// CopyFromStdin method, which ships them to the worker via Flight DoPut
// and runs the COPY against a worker-local spool file.
func (c *clientConn) handleCopyInRemoteStreaming(
	query string,
	opts *CopyFromOptions,
	copyStartTime time.Time,
	streamer sqlcore.CopyFromStdinExecutor,
) error {
	tableName := opts.TableName
	columnList := opts.ColumnList

	// Build the COPY SQL with the path placeholder; the worker substitutes
	// in its own tempfile path before executing.
	copySQL := BuildDuckDBCopyFromSQL(tableName, columnList, flightclient.CopyFromStdinPathPlaceholder, opts)
	c.logger().Debug("COPY FROM STDIN streaming to remote worker.", "sql", copySQL)

	r := &copyDataWireReader{c: c}

	loadStart := time.Now()
	c.logQueryStarted(copySQL)
	rowCount, err := streamer.CopyFromStdin(c.ctx, copySQL, r)
	c.logQueryFinished(copySQL, loadStart, rowCount, err)

	// On wire-level CopyFail / unexpected message, the reader returns a
	// non-EOF error so the streamer aborts the gRPC stream (its deferred
	// cancel() prevents the worker from running COPY on partial bytes).
	// We then surface the precise cause via the sticky flags below before
	// falling through to the generic transport-error branch.
	if r.cancelled {
		exception := fmt.Sprintf("COPY failed: %s", r.cancelMsg)
		c.sendError("ERROR", "57014", exception)
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "57014", exception, "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	if r.protoErr != "" {
		c.sendError("ERROR", "08P01", r.protoErr)
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "08P01", r.protoErr, "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	if err != nil {
		c.logger().Error("COPY FROM STDIN remote streaming failed.", "error", err)
		errMsg := fmt.Sprintf("COPY failed: %v", err)
		c.sendError("ERROR", "22P02", errMsg)
		c.setTxError()
		c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "22P02", errMsg, "simple")
		_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	totalElapsed := time.Since(copyStartTime)
	loadElapsed := time.Since(loadStart)
	c.logger().Info("COPY FROM STDIN completed (remote streaming).", "rows", rowCount, "bytes", r.bytesRead,
		"total_duration", totalElapsed, "load_duration", loadElapsed)

	_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	c.logQuery(copyStartTime, query, query, "COPY", 0, rowCount, "", "", "simple")
	_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// errCopyAborted is returned from copyDataWireReader.Read when the client
// signalled CopyFail or sent an unexpected wire message mid-stream. The
// streamer propagates this; the deferred gRPC context cancel then fires,
// which causes the worker's Recv to fail with Canceled and skip running
// the partially-uploaded COPY. handleCopyInRemoteStreaming inspects the
// reader's sticky flags to decide the client-facing error code (57014 for
// cancellation vs 08P01 for protocol error).
var errCopyAborted = errors.New("copy data stream aborted")

// copyDataWireReader adapts the PostgreSQL wire CopyData stream into an
// io.Reader so it can be fed straight into a remote-worker upload. Behavior:
//   - CopyDone           → io.EOF (clean end-of-stream)
//   - CopyFail           → errCopyAborted, with cancelled / cancelMsg sticky
//   - unexpected message → errCopyAborted, with protoErr sticky
//   - wire transport err → that error is returned verbatim
//
// Returning a non-EOF error on cancellation matters for correctness:
// if the reader pretended cancellation was a clean EOF, the streamer would
// CloseSend cleanly and the worker would happily run COPY on the partial
// bytes already received.
type copyDataWireReader struct {
	c *clientConn

	pending   []byte
	bytesRead int64

	done      bool
	cancelled bool
	cancelMsg string
	protoErr  string
}

func (r *copyDataWireReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.done {
			if r.cancelled || r.protoErr != "" {
				return 0, errCopyAborted
			}
			return 0, io.EOF
		}
		msgType, body, err := wire.ReadMessage(r.c.reader)
		if err != nil {
			r.done = true
			return 0, err
		}
		switch msgType {
		case wire.MsgCopyData:
			// Skip the PostgreSQL text COPY end-of-data marker (\.\n).
			if (len(body) == 3 && body[0] == '\\' && body[1] == '.' && body[2] == '\n') ||
				(len(body) == 2 && body[0] == '\\' && body[1] == '.') {
				continue
			}
			r.pending = body
		case wire.MsgCopyDone:
			r.done = true
			return 0, io.EOF
		case wire.MsgCopyFail:
			r.done = true
			r.cancelled = true
			r.cancelMsg = string(bytes.TrimRight(body, "\x00"))
			return 0, errCopyAborted
		default:
			r.done = true
			r.protoErr = fmt.Sprintf("unexpected message type during COPY: %c", msgType)
			return 0, errCopyAborted
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	r.bytesRead += int64(n)
	return n, nil
}

// handleCopyInCSVWithBlob handles COPY FROM STDIN for tables that contain BLOB columns.
// DuckDB's native CSV COPY cannot handle raw binary data in BLOB columns because it
// auto-detects the type and fails to parse the bytes. This method parses the CSV in Go,
// converts BLOB column values to []byte, and uses batched INSERT statements.
func (c *clientConn) handleCopyInCSVWithBlob(query string, opts *CopyFromOptions, cols []string, colTypes []ColumnTyper, blobColIndices []int) error {
	copyStartTime := time.Now()

	// Build a set for O(1) BLOB column index lookup
	isBlobCol := make(map[int]bool, len(blobColIndices))
	for _, idx := range blobColIndices {
		isBlobCol[idx] = true
	}

	// Send CopyInResponse (text format)
	if err := wire.WriteCopyInResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	_ = c.writer.Flush()

	// Buffer all CopyData messages into memory (we need to parse CSV, not stream to file)
	var buf bytes.Buffer
	for {
		msgType, body, err := wire.ReadMessage(c.reader)
		if err != nil {
			return err
		}

		switch msgType {
		case wire.MsgCopyData:
			// Skip end-of-data marker
			if (len(body) == 3 && body[0] == '\\' && body[1] == '.' && body[2] == '\n') ||
				(len(body) == 2 && body[0] == '\\' && body[1] == '.') {
				continue
			}
			buf.Write(body)

		case wire.MsgCopyDone:
			dataReceiveElapsed := time.Since(copyStartTime)
			c.logger().Debug("COPY FROM STDIN (BLOB fallback) CopyDone received.", "bytes", buf.Len(), "duration", dataReceiveElapsed)

			// Parse CSV from the buffered data
			csvReader := csv.NewReader(&buf)
			csvReader.Comma = rune(opts.Delimiter[0])
			csvReader.LazyQuotes = true

			// Skip header row if present
			if opts.HasHeader {
				if _, err := csvReader.Read(); err != nil {
					errMsg := fmt.Sprintf("COPY failed: error reading CSV header: %v", err)
					c.sendError("ERROR", "22P02", errMsg)
					c.setTxError()
					c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "22P02", errMsg, "simple")
					_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
					_ = c.writer.Flush()
					return nil
				}
			}

			// Parse all rows and convert BLOB columns to []byte
			var rows [][]interface{}
			for {
				record, err := csvReader.Read()
				if err != nil {
					break // EOF or error — stop reading
				}
				if len(record) != len(cols) {
					c.logger().Warn("COPY FROM STDIN (BLOB fallback) skipping row with wrong field count.", "expected", len(cols), "got", len(record))
					continue
				}
				row := make([]interface{}, len(cols))
				for j, field := range record {
					if field == opts.NullString {
						row[j] = nil
					} else if isBlobCol[j] {
						row[j] = []byte(field)
					} else {
						row[j] = field
					}
				}
				rows = append(rows, row)
			}

			if len(rows) == 0 {
				_ = wire.WriteCommandComplete(c.writer, "COPY 0")
				c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "", "", "simple")
				_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			loadStart := time.Now()
			rowCount, err := c.batchInsertRows(opts.TableName, opts.ColumnList, cols, rows)
			if err != nil {
				c.logger().Error("COPY FROM STDIN (BLOB fallback) INSERT failed.", "error", err)
				errMsg := fmt.Sprintf("COPY failed: %v", err)
				c.sendError("ERROR", "22P02", errMsg)
				c.setTxError()
				c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "22P02", errMsg, "simple")
				_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			totalElapsed := time.Since(copyStartTime)
			loadElapsed := time.Since(loadStart)
			c.logger().Info("COPY FROM STDIN (BLOB fallback) completed.", "rows", rowCount, "total_duration", totalElapsed, "load_duration", loadElapsed)

			_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "", "", "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		case wire.MsgCopyFail:
			errMsg := string(bytes.TrimRight(body, "\x00"))
			exception := fmt.Sprintf("COPY failed: %s", errMsg)
			c.sendError("ERROR", "57014", exception)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "57014", exception, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		default:
			errMsg := fmt.Sprintf("unexpected message type during COPY: %c", msgType)
			c.sendError("ERROR", "08P01", errMsg)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "08P01", errMsg, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}
}

// handleCopyInBinary handles COPY ... FROM STDIN with binary format.
// It parses the PostgreSQL binary COPY format, decodes each field, and INSERTs rows.
func (c *clientConn) handleCopyInBinary(query string, opts *CopyFromOptions, cols []string, colTypes []ColumnTyper) error {
	copyStartTime := time.Now()

	// Get type OIDs for decoding
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send CopyInResponse (binary format)
	if err := wire.WriteCopyInResponse(c.writer, int16(len(cols)), false); err != nil {
		return err
	}
	_ = c.writer.Flush()
	c.logger().Debug("COPY FROM STDIN binary: sent CopyInResponse.")

	// Collect all CopyData messages into a buffer
	var buf bytes.Buffer
	for {
		msgType, body, err := wire.ReadMessage(c.reader)
		if err != nil {
			c.logger().Error("COPY FROM STDIN binary: error reading message.", "error", err)
			return err
		}

		switch msgType {
		case wire.MsgCopyData:
			buf.Write(body)

		case wire.MsgCopyDone:
			// Parse binary data and insert rows
			data := buf.Bytes()
			rowCount, err := c.parseBinaryCopyAndInsert(data, opts.TableName, opts.ColumnList, cols, typeOIDs)
			if err != nil {
				c.logger().Error("COPY FROM STDIN binary: parse/insert failed.", "error", err)
				errMsg := fmt.Sprintf("COPY failed: %v", err)
				c.sendError("ERROR", "22P02", errMsg)
				c.setTxError()
				c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "22P02", errMsg, "simple")
				_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			elapsed := time.Since(copyStartTime)
			c.logger().Info("COPY FROM STDIN binary completed.", "rows", rowCount, "bytes", buf.Len(), "duration", elapsed)

			_ = wire.WriteCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			c.logQuery(copyStartTime, query, query, "COPY", 0, int64(rowCount), "", "", "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		case wire.MsgCopyFail:
			errMsg := string(bytes.TrimRight(body, "\x00"))
			exception := fmt.Sprintf("COPY failed: %s", errMsg)
			c.sendError("ERROR", "57014", exception)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "57014", exception, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		default:
			errMsg := fmt.Sprintf("unexpected message type during COPY: %c", msgType)
			c.sendError("ERROR", "08P01", errMsg)
			c.setTxError()
			c.logQuery(copyStartTime, query, query, "COPY", 0, 0, "08P01", errMsg, "simple")
			_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}
}

// splitQualifiedName splits a possibly-quoted SQL name like "schema"."table" into parts.
func splitQualifiedName(name string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			inQuotes = !inQuotes
			continue
		}
		if name[i] == '.' && !inQuotes {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(name[i])
	}
	parts = append(parts, current.String())
	return parts
}

// appendWithDuckDBAppender uses the DuckDB Appender API for fast bulk inserts.
// Only works for full-column inserts (no column subset).
// batchInsertRows inserts rows using batched multi-row INSERT statements.
// Used as fallback when Appender can't be used (column subsets, unsupported types).
func (c *clientConn) batchInsertRows(tableName, columnList string, cols []string, rows [][]interface{}) (int, error) {
	const batchSize = 1000
	numCols := len(cols)

	colNames := columnList
	if colNames == "" {
		quotedCols := make([]string, numCols)
		for i, col := range cols {
			quotedCols[i] = fmt.Sprintf(`"%s"`, col)
		}
		colNames = "(" + strings.Join(quotedCols, ", ") + ")"
	}

	rowCount := 0
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		var valueClauses []string
		var args []interface{}
		paramIdx := 1
		for _, row := range batch {
			placeholders := make([]string, numCols)
			for j := range row {
				placeholders[j] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, row[j])
				paramIdx++
			}
			valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s %s VALUES %s",
			tableName, colNames, strings.Join(valueClauses, ", "))

		// Lifecycle log pair (PR #519) per batched INSERT — one query
		// the worker actually runs.
		batchStart := time.Now()
		c.logQueryStarted(insertSQL)
		result, err := c.executor.Exec(insertSQL, args...)
		var rowsAff int64
		if result != nil {
			rowsAff, _ = result.RowsAffected()
		}
		c.logQueryFinished(insertSQL, batchStart, rowsAff, err)
		if err != nil {
			return rowCount, fmt.Errorf("batch INSERT failed at rows %d-%d: %v", start+1, start+len(batch), err)
		}
		rowCount += len(batch)
	}

	return rowCount, nil
}

// parseBinaryCopyAndInsert parses PostgreSQL binary COPY format data and inserts rows.
// Uses the DuckDB Appender API for full-column inserts (fast path), falling back to
// batched multi-row INSERT for column subsets or unsupported types.
func (c *clientConn) parseBinaryCopyAndInsert(data []byte, tableName, columnList string, cols []string, typeOIDs []int32) (int, error) {
	offset := 0

	// Validate and skip header (19+ bytes)
	// Signature: "PGCOPY\n\377\r\n\0" (11 bytes)
	if len(data) < 19 {
		return 0, fmt.Errorf("binary COPY data too short for header")
	}
	expectedSig := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n', 0x00}
	if !bytes.Equal(data[:11], expectedSig) {
		return 0, fmt.Errorf("invalid binary COPY signature")
	}
	offset = 11

	// Flags (4 bytes) and extension area length (4 bytes)
	offset += 4
	extLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	offset += int(extLen) // skip extension area

	numCols := len(cols)

	// Parse all rows from binary data first
	var rows [][]interface{}
	for offset < len(data) {
		if offset+2 > len(data) {
			return len(rows), fmt.Errorf("truncated binary COPY data at tuple header")
		}

		fieldCount := int16(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		// Trailer: field count of -1
		if fieldCount == -1 {
			break
		}

		if int(fieldCount) != numCols {
			return len(rows), fmt.Errorf("binary COPY field count mismatch: got %d, expected %d", fieldCount, numCols)
		}

		values := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			if offset+4 > len(data) {
				return len(rows), fmt.Errorf("truncated binary COPY data at field %d length", i)
			}

			fieldLen := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4

			if fieldLen == -1 {
				values[i] = nil
			} else {
				if offset+int(fieldLen) > len(data) {
					return len(rows), fmt.Errorf("truncated binary COPY data at field %d data", i)
				}
				fieldData := data[offset : offset+int(fieldLen)]
				offset += int(fieldLen)

				decoded, err := decodeBinaryCopy(fieldData, typeOIDs[i])
				if err != nil {
					return len(rows), fmt.Errorf("failed to decode field %d (OID %d, %d bytes): %v", i, typeOIDs[i], fieldLen, err)
				}
				values[i] = decoded
			}
		}
		rows = append(rows, values)
	}

	if len(rows) == 0 {
		return 0, nil
	}

	// Fast path: use Appender for full-column inserts (no column subset)
	if columnList == "" {
		count, err := c.appendWithDuckDBAppender(tableName, rows)
		if err == nil {
			return count, nil
		}
		c.logger().Warn("Appender failed, falling back to batched INSERT.", "table", tableName, "error", err)
	}

	// Fallback: batched multi-row INSERT
	return c.batchInsertRows(tableName, columnList, cols, rows)
}

// decodeBinaryCopy decodes a binary COPY field, using field length to resolve type ambiguity.
// DuckDB's postgres extension may send different integer widths than what the table OID suggests.
func decodeBinaryCopy(data []byte, oid int32) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	// Zero-length fields: return empty string for text types, empty bytes for bytea, nil for others
	if len(data) == 0 {
		switch oid {
		case OidText, OidVarchar, OidBpchar, OidName, OidJSON, OidJSONB:
			return "", nil
		case OidBytea:
			return []byte{}, nil
		default:
			return nil, nil
		}
	}

	switch oid {
	case OidBool:
		return decodeBool(data)
	case OidInt2, OidInt4, OidInt8, OidOid:
		// Use field length to determine actual integer width
		switch len(data) {
		case 2:
			return decodeInt2(data)
		case 4:
			return decodeInt4(data)
		case 8:
			return decodeInt8(data)
		default:
			return string(data), nil
		}
	case OidFloat4:
		if len(data) == 8 {
			return decodeFloat8(data)
		}
		return decodeFloat4(data)
	case OidFloat8:
		if len(data) == 4 {
			return decodeFloat4(data)
		}
		return decodeFloat8(data)
	case OidNumeric:
		return decodeNumeric(data)
	case OidDate:
		return decodeDate(data)
	case OidTimestamp, OidTimestamptz:
		return decodeTimestamp(data)
	case OidTime:
		return decodeTime(data)
	case OidInterval:
		return decodeInterval(data)
	case OidUUID:
		return decodeUUID(data)
	case OidBytea:
		return data, nil
	default:
		// For text, varchar, and unknown types, return as string
		return string(data), nil
	}
}

// formatCopyValue formats a value for COPY output
func (c *clientConn) formatCopyValue(v interface{}) string {
	if v == nil {
		return "\\N"
	}
	switch val := v.(type) {
	case []any:
		return formatArrayValue(val)
	case map[string]any:
		return formatMapValue(val)
	case arrowmap.OrderedMapValue:
		return formatOrderedMapValue(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// parseCopyLine parses a line of COPY input
func (c *clientConn) parseCopyLine(line, delimiter string) []string {
	// Use encoding/csv for proper handling of quoted values
	reader := csv.NewReader(strings.NewReader(line))
	reader.Comma = rune(delimiter[0])
	reader.LazyQuotes = true // Be lenient with quotes

	fields, err := reader.Read()
	if err != nil {
		// Fall back to simple split if CSV parsing fails
		return strings.Split(line, delimiter)
	}
	return fields
}
