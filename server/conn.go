package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/posthog/duckgres/transpiler"
)

type preparedStmt struct {
	query          string
	convertedQuery string
	paramTypes     []int32
	numParams      int
	isIgnoredSet   bool   // True if this is an ignored SET parameter
	isNoOp         bool   // True if this is a no-op command (CREATE INDEX, etc.)
	noOpTag        string // Command tag for no-op commands
	described      bool   // True if Describe(S) was called on this statement
}

type portal struct {
	stmt          *preparedStmt
	paramValues   [][]byte
	resultFormats []int16
	described     bool // true if Describe was called on this portal
}

// Transaction status constants for PostgreSQL wire protocol
const (
	txStatusIdle        = 'I' // Not in a transaction
	txStatusTransaction = 'T' // In a transaction
	txStatusError       = 'E' // In a failed transaction
)

type clientConn struct {
	server     *Server
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	username   string
	database   string
	db         *sql.DB
	pid        int32
	stmts      map[string]*preparedStmt // prepared statements by name
	portals    map[string]*portal       // portals by name
	txStatus   byte                     // current transaction status ('I', 'T', or 'E')
}

// newTranspiler creates a transpiler configured for this connection.
func (c *clientConn) newTranspiler(convertPlaceholders bool) *transpiler.Transpiler {
	return transpiler.New(transpiler.Config{
		DuckLakeMode:        c.server.cfg.DuckLake.MetadataStore != "",
		ConvertPlaceholders: convertPlaceholders,
	})
}

func (c *clientConn) serve() error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.pid = int32(os.Getpid())
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)
	c.txStatus = txStatusIdle

	// Handle startup
	if err := c.handleStartup(); err != nil {
		return fmt.Errorf("startup failed: %w", err)
	}

	// Create a DuckDB connection for this client session
	db, err := c.server.createDBConnection(c.username)
	if err != nil {
		c.sendError("FATAL", "28000", fmt.Sprintf("failed to open database: %v", err))
		return err
	}
	c.db = db
	defer func() {
		if c.db != nil {
			// Detach DuckLake to release the RDS metadata connection
			if c.server.cfg.DuckLake.MetadataStore != "" {
				if _, err := c.db.Exec("DETACH ducklake"); err != nil {
					log.Printf("Warning: failed to detach DuckLake for user %q: %v", c.username, err)
				}
			}
			c.db.Close()
		}
	}()

	// Send initial parameters
	c.sendInitialParams()

	// Send ready for query
	if err := writeReadyForQuery(c.writer, c.txStatus); err != nil {
		return err
	}
	c.writer.Flush()

	// Main message loop
	return c.messageLoop()
}

func (c *clientConn) handleStartup() error {
	tlsUpgraded := false

	for {
		params, err := readStartupMessage(c.reader)
		if err != nil {
			return err
		}

		// Handle SSL request - upgrade to TLS
		if params["__ssl_request"] == "true" {
			// Send 'S' to indicate we support SSL
			if _, err := c.conn.Write([]byte("S")); err != nil {
				return err
			}

			// Upgrade connection to TLS
			tlsConn := tls.Server(c.conn, c.server.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				return fmt.Errorf("TLS handshake failed: %w", err)
			}

			// Replace connection with TLS connection
			c.conn = tlsConn
			c.reader = bufio.NewReader(tlsConn)
			c.writer = bufio.NewWriter(tlsConn)
			tlsUpgraded = true

			log.Printf("TLS connection established from %s", c.conn.RemoteAddr())
			continue
		}

		// Handle cancel request
		if params["__cancel_request"] == "true" {
			return nil
		}

		// Reject non-TLS connections
		if !tlsUpgraded {
			c.sendError("FATAL", "28000", "SSL/TLS connection required")
			return fmt.Errorf("client did not request SSL")
		}

		c.username = params["user"]
		c.database = params["database"]

		if c.username == "" {
			c.sendError("FATAL", "28000", "no user specified")
			return fmt.Errorf("no user specified")
		}

		break
	}

	// Request password
	if err := writeAuthCleartextPassword(c.writer); err != nil {
		return err
	}
	c.writer.Flush()

	// Read password response
	msgType, body, err := readMessage(c.reader)
	if err != nil {
		return err
	}

	if msgType != msgPassword {
		c.sendError("FATAL", "28000", "expected password message")
		return fmt.Errorf("expected password message, got %c", msgType)
	}

	// Password is null-terminated
	password := string(bytes.TrimRight(body, "\x00"))

	// Validate password
	expectedPassword, ok := c.server.cfg.Users[c.username]
	if !ok || expectedPassword != password {
		// Record failed authentication attempt
		banned := c.server.rateLimiter.RecordFailedAuth(c.conn.RemoteAddr())
		if banned {
			log.Printf("IP %s banned after too many failed auth attempts", c.conn.RemoteAddr())
		}
		c.sendError("FATAL", "28P01", "password authentication failed")
		return fmt.Errorf("authentication failed for user %q", c.username)
	}

	// Record successful authentication (clears failed attempt counter)
	c.server.rateLimiter.RecordSuccessfulAuth(c.conn.RemoteAddr())

	// Send auth OK
	if err := writeAuthOK(c.writer); err != nil {
		return err
	}

	log.Printf("User %q authenticated from %s", c.username, c.conn.RemoteAddr())
	return nil
}

func (c *clientConn) sendInitialParams() {
	params := map[string]string{
		"server_version":          "15.0 (Duckgres)",
		"server_encoding":         "UTF8",
		"client_encoding":         "UTF8",
		"DateStyle":               "ISO, MDY",
		"TimeZone":                "UTC",
		"integer_datetimes":       "on",
		"standard_conforming_strings": "on",
	}

	for name, value := range params {
		writeParameterStatus(c.writer, name, value)
	}

	// Send backend key data
	writeBackendKeyData(c.writer, c.pid, 0)
}

func (c *clientConn) messageLoop() error {
	for {
		// Set read deadline if idle timeout is configured
		if c.server.cfg.IdleTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.server.cfg.IdleTimeout))
		}

		msgType, body, err := readMessage(c.reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			// Check if this is a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("[%s] Connection idle timeout, closing", c.username)
				return nil
			}
			return err
		}

		switch msgType {
		case msgQuery:
			if err := c.handleQuery(body); err != nil {
				log.Printf("Query error: %v", err)
			}

		case msgParse:
			// Extended query protocol - Parse
			c.handleParse(body)

		case msgBind:
			// Extended query protocol - Bind
			c.handleBind(body)

		case msgDescribe:
			// Extended query protocol - Describe
			c.handleDescribe(body)

		case msgExecute:
			// Extended query protocol - Execute
			c.handleExecute(body)

		case msgSync:
			// Extended query protocol - Sync
			if err := writeReadyForQuery(c.writer, c.txStatus); err != nil {
				return err
			}
			c.writer.Flush()

		case msgClose:
			// Extended query protocol - Close
			c.handleClose(body)

		case msgFlush:
			c.writer.Flush()

		case msgTerminate:
			return nil

		default:
			log.Printf("Unknown message type: %c", msgType)
		}
	}
}

func (c *clientConn) handleQuery(body []byte) error {
	query := string(bytes.TrimRight(body, "\x00"))
	query = strings.TrimSpace(query)

	// Treat empty queries or queries with just semicolons as empty
	// PostgreSQL returns EmptyQueryResponse for queries like "" or ";" or ";;;"
	if query == "" || isEmptyQuery(query) {
		writeEmptyQueryResponse(c.writer)
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	log.Printf("[%s] Query: %s", c.username, query)

	// Transpile PostgreSQL SQL to DuckDB-compatible SQL
	tr := c.newTranspiler(false)
	result, err := tr.Transpile(query)
	if err != nil {
		// Parse error - send error to client
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Handle transform-detected errors (e.g., unrecognized config parameter)
	if result.Error != nil {
		c.sendError("ERROR", "42704", result.Error.Error())
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Handle ignored SET parameters
	if result.IsIgnoredSet {
		log.Printf("[%s] Ignoring PostgreSQL-specific SET: %s", c.username, query)
		writeCommandComplete(c.writer, "SET")
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.)
	if result.IsNoOp {
		log.Printf("[%s] No-op command (DuckLake limitation): %s", c.username, query)
		writeCommandComplete(c.writer, result.NoOpTag)
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Use the transpiled SQL
	originalQuery := query
	query = result.SQL

	// Log the transpiled query if it differs from the original
	if query != originalQuery {
		log.Printf("[%s] Executed: %s", c.username, query)
	}

	// Determine command type for proper response
	upperQuery := strings.ToUpper(query)
	cmdType := c.getCommandType(upperQuery)

	// Handle COPY commands specially
	if cmdType == "COPY" {
		return c.handleCopy(query, upperQuery)
	}

	// For non-SELECT queries, use Exec
	if cmdType != "SELECT" {
		// Handle nested BEGIN: PostgreSQL issues a warning but continues,
		// while DuckDB throws an error. Match PostgreSQL behavior.
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			writeCommandComplete(c.writer, "BEGIN")
			writeReadyForQuery(c.writer, c.txStatus)
			c.writer.Flush()
			return nil
		}

		result, err := c.db.Exec(query)
		if err != nil {
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			writeReadyForQuery(c.writer, c.txStatus)
			c.writer.Flush()
			return nil
		}

		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		writeCommandComplete(c.writer, tag)
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Execute SELECT query
	rows, err := c.db.Query(query)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}
	defer rows.Close()

	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Send row description
	if err := c.sendRowDescription(cols, colTypes); err != nil {
		return err
	}

	// Send rows
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			c.sendError("ERROR", "42000", err.Error())
			break
		}

		if err := c.sendDataRow(values); err != nil {
			return err
		}
		rowCount++
	}

	// Send command complete (SELECT doesn't change transaction status)
	tag := fmt.Sprintf("SELECT %d", rowCount)
	writeCommandComplete(c.writer, tag)
	writeReadyForQuery(c.writer, c.txStatus)
	c.writer.Flush()

	return nil
}

// isEmptyQuery checks if a query contains only semicolons and whitespace.
// PostgreSQL returns EmptyQueryResponse for queries like ";" or ";;;" or "; ; ;"
func isEmptyQuery(query string) bool {
	for _, r := range query {
		if r != ';' && r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

// stripLeadingComments removes leading SQL comments from a query.
// Handles both block comments /* ... */ and line comments -- ...
func stripLeadingComments(query string) string {
	for {
		query = strings.TrimSpace(query)
		if strings.HasPrefix(query, "/*") {
			end := strings.Index(query, "*/")
			if end == -1 {
				return query
			}
			query = query[end+2:]
		} else if strings.HasPrefix(query, "--") {
			end := strings.Index(query, "\n")
			if end == -1 {
				return ""
			}
			query = query[end+1:]
		} else {
			return query
		}
	}
}

// queryReturnsResults checks if a SQL query returns a result set.
// This is used to determine whether to send RowDescription or NoData.
func queryReturnsResults(query string) bool {
	upper := strings.ToUpper(stripLeadingComments(query))
	// SELECT is the most common
	if strings.HasPrefix(upper, "SELECT") {
		return true
	}
	// WITH ... SELECT (CTEs)
	if strings.HasPrefix(upper, "WITH") {
		return true
	}
	// VALUES clause returns rows
	if strings.HasPrefix(upper, "VALUES") {
		return true
	}
	// SHOW commands return results
	if strings.HasPrefix(upper, "SHOW") {
		return true
	}
	// TABLE is shorthand for SELECT * FROM table
	if strings.HasPrefix(upper, "TABLE") {
		return true
	}
	// EXECUTE can return results if the prepared statement is a SELECT
	if strings.HasPrefix(upper, "EXECUTE") {
		return true
	}
	// EXPLAIN returns results
	if strings.HasPrefix(upper, "EXPLAIN") {
		return true
	}
	// DESCRIBE returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "DESCRIBE") {
		return true
	}
	return false
}

func (c *clientConn) getCommandType(upperQuery string) string {
	// Strip leading comments like /*Fivetran*/ before checking command type
	upperQuery = stripLeadingComments(upperQuery)

	switch {
	case strings.HasPrefix(upperQuery, "SELECT"):
		return "SELECT"
	case strings.HasPrefix(upperQuery, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(upperQuery, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(upperQuery, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(upperQuery, "CREATE TABLE"),
		strings.HasPrefix(upperQuery, "CREATE TEMPORARY TABLE"),
		strings.HasPrefix(upperQuery, "CREATE TEMP TABLE"),
		strings.HasPrefix(upperQuery, "CREATE UNLOGGED TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(upperQuery, "CREATE INDEX"),
		strings.HasPrefix(upperQuery, "CREATE UNIQUE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(upperQuery, "CREATE VIEW"),
		strings.HasPrefix(upperQuery, "CREATE OR REPLACE VIEW"):
		return "CREATE VIEW"
	case strings.HasPrefix(upperQuery, "CREATE SCHEMA"):
		return "CREATE SCHEMA"
	case strings.HasPrefix(upperQuery, "CREATE"):
		return "CREATE"
	case strings.HasPrefix(upperQuery, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(upperQuery, "DROP INDEX"):
		return "DROP INDEX"
	case strings.HasPrefix(upperQuery, "DROP VIEW"):
		return "DROP VIEW"
	case strings.HasPrefix(upperQuery, "DROP SCHEMA"):
		return "DROP SCHEMA"
	case strings.HasPrefix(upperQuery, "DROP"):
		return "DROP"
	case strings.Contains(upperQuery, "ADD CONSTRAINT") ||
		strings.Contains(upperQuery, "ADD PRIMARY KEY") ||
		strings.Contains(upperQuery, "ADD UNIQUE") ||
		strings.Contains(upperQuery, "ADD FOREIGN KEY") ||
		strings.Contains(upperQuery, "ADD CHECK"):
		return "ALTER TABLE ADD CONSTRAINT"
	case strings.HasPrefix(upperQuery, "ALTER"):
		return "ALTER TABLE"
	case strings.HasPrefix(upperQuery, "TRUNCATE"):
		return "TRUNCATE TABLE"
	case strings.HasPrefix(upperQuery, "BEGIN"):
		return "BEGIN"
	case strings.HasPrefix(upperQuery, "COMMIT"):
		return "COMMIT"
	case strings.HasPrefix(upperQuery, "ROLLBACK"):
		return "ROLLBACK"
	case strings.HasPrefix(upperQuery, "SET"):
		return "SET"
	case strings.HasPrefix(upperQuery, "COPY"):
		return "COPY"
	default:
		return "SELECT" // fallback to SELECT behavior
	}
}

// updateTxStatus updates the transaction status based on the executed command.
// This is called after a successful command execution.
func (c *clientConn) updateTxStatus(cmdType string) {
	switch cmdType {
	case "BEGIN":
		c.txStatus = txStatusTransaction
	case "COMMIT", "ROLLBACK":
		c.txStatus = txStatusIdle
	}
	// For other commands, keep the current status
}

// setTxError marks the transaction as failed if we're in a transaction.
// This should be called when a query fails within a transaction.
func (c *clientConn) setTxError() {
	if c.txStatus == txStatusTransaction {
		c.txStatus = txStatusError
	}
}

func (c *clientConn) buildCommandTag(cmdType string, result sql.Result) string {
	switch cmdType {
	case "INSERT":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case "UPDATE":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case "DELETE":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("DELETE %d", rowsAffected)
	default:
		return cmdType
	}
}

// Regular expressions for parsing COPY commands
var (
	copyToStdoutRegex   = regexp.MustCompile(`(?i)COPY\s+(.+?)\s+TO\s+STDOUT`)
	copyFromStdinRegex  = regexp.MustCompile(`(?i)COPY\s+(\S+)\s*(?:\(([^)]+)\)\s*)?FROM\s+STDIN`)
	copyWithCSVRegex    = regexp.MustCompile(`(?i)\bCSV\b`)
	copyWithHeaderRegex = regexp.MustCompile(`(?i)\bHEADER\b`)
	copyDelimiterRegex  = regexp.MustCompile(`(?i)\bDELIMITER\s+['"](.)['"]\b`)
	copyNullRegex       = regexp.MustCompile(`(?i)\bNULL\s+'([^']*)'`)
)

// handleCopy handles COPY TO STDOUT and COPY FROM STDIN commands
func (c *clientConn) handleCopy(query, upperQuery string) error {
	// Check if it's COPY TO STDOUT
	if copyToStdoutRegex.MatchString(upperQuery) {
		return c.handleCopyOut(query, upperQuery)
	}

	// Check if it's COPY FROM STDIN
	if copyFromStdinRegex.MatchString(upperQuery) {
		return c.handleCopyIn(query, upperQuery)
	}

	// For other COPY commands (e.g., COPY TO file), pass through to DuckDB
	result, err := c.db.Exec(query)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	rowsAffected, _ := result.RowsAffected()
	writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowsAffected))
	writeReadyForQuery(c.writer, c.txStatus)
	c.writer.Flush()
	return nil
}

// handleCopyOut handles COPY ... TO STDOUT
func (c *clientConn) handleCopyOut(query, upperQuery string) error {
	matches := copyToStdoutRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		c.sendError("ERROR", "42601", "Invalid COPY TO STDOUT syntax")
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Parse options
	delimiter := "\t"
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		delimiter = ","
	}

	// The source can be a table name or a query in parentheses
	source := strings.TrimSpace(matches[1])
	var selectQuery string
	if strings.HasPrefix(source, "(") && strings.HasSuffix(source, ")") {
		selectQuery = source[1 : len(source)-1]
	} else {
		selectQuery = fmt.Sprintf("SELECT * FROM %s", source)
	}

	// Execute the query
	rows, err := c.db.Query(selectQuery)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	// Send CopyOutResponse
	if err := writeCopyOutResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	c.writer.Flush()

	// Send header if CSV with HEADER
	if copyWithCSVRegex.MatchString(upperQuery) && copyWithHeaderRegex.MatchString(upperQuery) {
		header := strings.Join(cols, delimiter) + "\n"
		if err := writeCopyData(c.writer, []byte(header)); err != nil {
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
			c.sendError("ERROR", "42000", err.Error())
			break
		}

		// Format row as tab/comma separated values
		var rowData []string
		for _, v := range values {
			rowData = append(rowData, c.formatCopyValue(v))
		}
		line := strings.Join(rowData, delimiter) + "\n"
		if err := writeCopyData(c.writer, []byte(line)); err != nil {
			return err
		}
		rowCount++
	}

	// Send CopyDone
	if err := writeCopyDone(c.writer); err != nil {
		return err
	}

	writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	writeReadyForQuery(c.writer, c.txStatus)
	c.writer.Flush()
	return nil
}

// handleCopyIn handles COPY ... FROM STDIN
func (c *clientConn) handleCopyIn(query, upperQuery string) error {
	matches := copyFromStdinRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		c.sendError("ERROR", "42601", "Invalid COPY FROM STDIN syntax")
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}

	tableName := matches[1]
	columnList := ""
	if len(matches) > 2 && matches[2] != "" {
		columnList = fmt.Sprintf("(%s)", matches[2])
	}

	// Parse options
	delimiter := "\t"
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		delimiter = ","
	}
	hasHeader := copyWithCSVRegex.MatchString(upperQuery) && copyWithHeaderRegex.MatchString(upperQuery)

	// Parse NULL string option (e.g., NULL 'custom-null-value')
	nullString := "\\N" // Default PostgreSQL null representation
	if m := copyNullRegex.FindStringSubmatch(query); len(m) > 1 {
		nullString = m[1]
	}

	// Get column count for the table
	colQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	testRows, err := c.db.Query(colQuery)
	if err != nil {
		c.sendError("ERROR", "42P01", fmt.Sprintf("relation \"%s\" does not exist", tableName))
		c.setTxError()
		writeReadyForQuery(c.writer, c.txStatus)
		c.writer.Flush()
		return nil
	}
	cols, _ := testRows.Columns()
	testRows.Close()

	// Send CopyInResponse
	if err := writeCopyInResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	c.writer.Flush()

	// Read COPY data from client
	var allData bytes.Buffer
	rowCount := 0
	headerSkipped := false

	for {
		msgType, body, err := readMessage(c.reader)
		if err != nil {
			return err
		}

		switch msgType {
		case msgCopyData:
			allData.Write(body)

		case msgCopyDone:
			// Process all data
			lines := strings.Split(allData.String(), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				// Skip header if needed
				if hasHeader && !headerSkipped {
					headerSkipped = true
					continue
				}

				// Parse values and insert
				values := c.parseCopyLine(line, delimiter)
				if len(values) == 0 {
					continue
				}

				// Build INSERT statement
				placeholders := make([]string, len(values))
				args := make([]interface{}, len(values))
				for i, v := range values {
					placeholders[i] = "?"
					if v == nullString || v == "\\N" || v == "" {
						args[i] = nil
					} else {
						args[i] = v
					}
				}

				insertSQL := fmt.Sprintf("INSERT INTO %s %s VALUES (%s)",
					tableName, columnList, strings.Join(placeholders, ", "))

				if _, err := c.db.Exec(insertSQL, args...); err != nil {
					c.sendError("ERROR", "22P02", fmt.Sprintf("invalid input: %v", err))
					c.setTxError()
					writeReadyForQuery(c.writer, c.txStatus)
					c.writer.Flush()
					return nil
				}
				rowCount++
			}

			writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			writeReadyForQuery(c.writer, c.txStatus)
			c.writer.Flush()
			return nil

		case msgCopyFail:
			// Client cancelled COPY
			errMsg := string(bytes.TrimRight(body, "\x00"))
			c.sendError("ERROR", "57014", fmt.Sprintf("COPY failed: %s", errMsg))
			c.setTxError()
			writeReadyForQuery(c.writer, c.txStatus)
			c.writer.Flush()
			return nil

		default:
			c.sendError("ERROR", "08P01", fmt.Sprintf("unexpected message type during COPY: %c", msgType))
			c.setTxError()
			writeReadyForQuery(c.writer, c.txStatus)
			c.writer.Flush()
			return nil
		}
	}
}

// formatCopyValue formats a value for COPY output
func (c *clientConn) formatCopyValue(v interface{}) string {
	if v == nil {
		return "\\N"
	}
	return fmt.Sprintf("%v", v)
}

// parseCopyLine parses a line of COPY input
func (c *clientConn) parseCopyLine(line, delimiter string) []string {
	// Simple split - doesn't handle quoted values yet
	return strings.Split(line, delimiter)
}

func (c *clientConn) sendRowDescription(cols []string, colTypes []*sql.ColumnType) error {
	var buf bytes.Buffer

	// Number of fields
	binary.Write(&buf, binary.BigEndian, int16(len(cols)))

	for i, col := range cols {
		// Column name (null-terminated)
		buf.WriteString(col)
		buf.WriteByte(0)

		// Table OID (0 = not from a table)
		binary.Write(&buf, binary.BigEndian, int32(0))

		// Column attribute number (0 = not from a table)
		binary.Write(&buf, binary.BigEndian, int16(0))

		// Data type OID - map DuckDB types to PostgreSQL OIDs
		oid := c.mapTypeOID(colTypes[i])
		binary.Write(&buf, binary.BigEndian, oid)

		// Data type size
		typeSize := c.mapTypeSize(colTypes[i])
		binary.Write(&buf, binary.BigEndian, typeSize)

		// Type modifier (-1 = no modifier)
		binary.Write(&buf, binary.BigEndian, int32(-1))

		// Format code (0 = text, 1 = binary)
		binary.Write(&buf, binary.BigEndian, int16(0))
	}

	return writeMessage(c.writer, msgRowDescription, buf.Bytes())
}

func (c *clientConn) mapTypeOID(colType *sql.ColumnType) int32 {
	return getTypeInfo(colType).OID
}

func (c *clientConn) mapTypeSize(colType *sql.ColumnType) int16 {
	return getTypeInfo(colType).Size
}

func (c *clientConn) sendDataRow(values []interface{}) error {
	return c.sendDataRowWithFormats(values, nil, nil)
}

// sendDataRowWithFormats sends a data row with optional binary encoding
// formatCodes: per-column format codes (0=text, 1=binary), or nil for all text
// typeOIDs: per-column type OIDs for binary encoding, or nil
func (c *clientConn) sendDataRowWithFormats(values []interface{}, formatCodes []int16, typeOIDs []int32) error {
	var buf bytes.Buffer

	// Number of columns
	binary.Write(&buf, binary.BigEndian, int16(len(values)))

	for i, v := range values {
		if v == nil {
			// NULL value
			binary.Write(&buf, binary.BigEndian, int32(-1))
			continue
		}

		// Determine format: binary or text
		useBinary := false
		if formatCodes != nil {
			if len(formatCodes) == 1 {
				// Single format code applies to all columns
				useBinary = formatCodes[0] == 1
			} else if i < len(formatCodes) {
				useBinary = formatCodes[i] == 1
			}
		}

		if useBinary && typeOIDs != nil && i < len(typeOIDs) {
			// Binary encoding
			encoded := encodeBinary(v, typeOIDs[i])
			if encoded == nil {
				// Fallback to text if binary encoding fails
				str := formatValue(v)
				binary.Write(&buf, binary.BigEndian, int32(len(str)))
				buf.WriteString(str)
			} else {
				binary.Write(&buf, binary.BigEndian, int32(len(encoded)))
				buf.Write(encoded)
			}
		} else {
			// Text encoding
			str := formatValue(v)
			binary.Write(&buf, binary.BigEndian, int32(len(str)))
			buf.WriteString(str)
		}
	}

	return writeMessage(c.writer, msgDataRow, buf.Bytes())
}

// formatValue converts a value to its PostgreSQL text representation
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case *string:
		if val == nil {
			return ""
		}
		return *val
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "t"
		}
		return "f"
	case time.Time:
		// PostgreSQL timestamp format without timezone suffix
		if val.IsZero() {
			return ""
		}
		// Use microsecond precision if there are sub-second components
		if val.Nanosecond() != 0 {
			return val.Format("2006-01-02 15:04:05.999999")
		}
		return val.Format("2006-01-02 15:04:05")
	default:
		// For other types, try to convert to string
		return fmt.Sprintf("%v", val)
	}
}

func (c *clientConn) sendError(severity, code, message string) {
	writeErrorResponse(c.writer, severity, code, message)
	c.writer.Flush()
}

func (c *clientConn) sendNotice(severity, code, message string) {
	writeNoticeResponse(c.writer, severity, code, message)
	// Don't flush here - let the caller decide when to flush
}

// Extended query protocol handlers

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

	// Transpile PostgreSQL SQL to DuckDB-compatible SQL (with placeholder conversion)
	tr := c.newTranspiler(true) // Enable placeholder conversion for prepared statements
	result, err := tr.Transpile(query)
	if err != nil {
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		return
	}

	// Handle transform-detected errors (e.g., unrecognized config parameter)
	if result.Error != nil {
		c.sendError("ERROR", "42704", result.Error.Error())
		return
	}

	// Close existing statement with same name
	delete(c.stmts, stmtName)

	c.stmts[stmtName] = &preparedStmt{
		query:          query,             // Keep original for logging and Describe
		convertedQuery: result.SQL,        // Transpiled SQL for execution
		paramTypes:     paramTypes,
		numParams:      result.ParamCount,
		isIgnoredSet:   result.IsIgnoredSet,
		isNoOp:         result.IsNoOp,
		noOpTag:        result.NoOpTag,
	}

	log.Printf("[%s] Prepared statement %q: %s", c.username, stmtName, query)
	if result.SQL != query {
		log.Printf("[%s] Prepared statement %q transpiled: %s", c.username, stmtName, result.SQL)
	}
	writeParseComplete(c.writer)
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
	paramValues := make([][]byte, numParams)
	for i := int16(0); i < numParams; i++ {
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
		if length == -1 {
			paramValues[i] = nil // NULL
		} else {
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
	resultFormats := make([]int16, numResultFormats)
	for i := int16(0); i < numResultFormats; i++ {
		if err := binary.Read(reader, binary.BigEndian, &resultFormats[i]); err != nil {
			c.sendError("ERROR", "08P01", "invalid Bind message")
			return
		}
	}

	// Close existing portal with same name
	delete(c.portals, portalName)

	c.portals[portalName] = &portal{
		stmt:          ps,
		paramValues:   paramValues,
		resultFormats: resultFormats,
		described:     ps.described, // Inherit from statement if Describe(S) was called
	}

	writeBindComplete(c.writer)
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
		log.Printf("[%s] Describe statement %q: %s", c.username, name, ps.query)

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

		// For queries that return results, we need to send RowDescription
		// For other queries, send NoData
		returnsResults := queryReturnsResults(ps.query)
		log.Printf("[%s] Describe statement %q: returnsResults=%v", c.username, name, returnsResults)
		if !returnsResults {
			writeNoData(c.writer)
			return
		}

		// For SELECT, we need to describe the result columns
		// The cleanest approach is to add a "WHERE false" or "LIMIT 0" clause
		// to get column info without actually running the query
		describeQuery := ps.convertedQuery
		// Try adding LIMIT 0 to avoid needing real parameter values
		if !strings.Contains(strings.ToUpper(ps.convertedQuery), "LIMIT") {
			describeQuery = ps.convertedQuery + " LIMIT 0"
		}

		// Use NULL for all parameters
		args := make([]interface{}, ps.numParams)
		for i := range args {
			args[i] = nil
		}

		rows, err := c.db.Query(describeQuery, args...)
		if err != nil {
			// Can't describe - send NoData
			log.Printf("[%s] Describe failed to get columns: %v", c.username, err)
			writeNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		rows.Close()

		if len(cols) == 0 {
			writeNoData(c.writer)
			return
		}

		log.Printf("[%s] Describe statement: sending RowDescription with %d columns", c.username, len(cols))
		c.sendRowDescription(cols, colTypes)
		ps.described = true

	case 'P':
		// Describe portal
		p, ok := c.portals[name]
		if !ok {
			c.sendError("ERROR", "34000", fmt.Sprintf("portal %q does not exist", name))
			return
		}

		// For queries that don't return results, send NoData
		if !queryReturnsResults(p.stmt.query) {
			writeNoData(c.writer)
			return
		}

		// For SELECT, we need to describe the result columns
		// We'll do a trial query with LIMIT 0 to get column info
		args := make([]interface{}, len(p.paramValues))
		for i, v := range p.paramValues {
			if v == nil {
				args[i] = nil
			} else {
				args[i] = string(v)
			}
		}

		// Try to get column info
		rows, err := c.db.Query(p.stmt.convertedQuery, args...)
		if err != nil {
			// Can't describe - send NoData
			writeNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		rows.Close()

		if len(cols) == 0 {
			writeNoData(c.writer)
			return
		}

		// Only mark as described when we actually send RowDescription.
		// If we sent NoData above, Execute should still send RowDescription.
		p.described = true
		c.sendRowDescription(cols, colTypes)

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

	// Convert parameter values to interface{}
	args := make([]interface{}, len(p.paramValues))
	for i, v := range p.paramValues {
		if v == nil {
			args[i] = nil
		} else {
			args[i] = string(v) // Text format assumed
		}
	}

	upperQuery := strings.ToUpper(strings.TrimSpace(p.stmt.query))
	cmdType := c.getCommandType(upperQuery)
	returnsResults := queryReturnsResults(p.stmt.query)

	log.Printf("[%s] Execute %q with %d params: %s", c.username, portalName, len(args), p.stmt.query)

	// Check if this is a PostgreSQL-specific SET command that should be ignored
	// (determined by transpiler during Parse)
	if p.stmt.isIgnoredSet {
		log.Printf("[%s] Ignoring PostgreSQL-specific SET: %s", c.username, p.stmt.query)
		writeCommandComplete(c.writer, "SET")
		return
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.) - DuckLake doesn't support these
	// (determined by transpiler during Parse)
	if p.stmt.isNoOp {
		log.Printf("[%s] No-op command (DuckLake limitation): %s", c.username, p.stmt.query)
		writeCommandComplete(c.writer, p.stmt.noOpTag)
		return
	}

	if !returnsResults {
		// Handle nested BEGIN: PostgreSQL issues a warning but continues,
		// while DuckDB throws an error. Match PostgreSQL behavior.
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			writeCommandComplete(c.writer, "BEGIN")
			return
		}

		// Non-result-returning query: use Exec with converted query
		result, err := c.db.Exec(p.stmt.convertedQuery, args...)
		if err != nil {
			log.Printf("[%s] Execute error: %v", c.username, err)
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			return
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		writeCommandComplete(c.writer, tag)
		return
	}

	// Result-returning query: use Query with converted query
	rows, err := c.db.Query(p.stmt.convertedQuery, args...)
	if err != nil {
		log.Printf("[%s] Query error: %v", c.username, err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Printf("[%s] Columns error: %v", c.username, err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
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
		if err := c.sendRowDescription(cols, colTypes); err != nil {
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
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			return
		}

		if err := c.sendDataRowWithFormats(values, p.resultFormats, typeOIDs); err != nil {
			return
		}
		rowCount++
	}

	tag := fmt.Sprintf("SELECT %d", rowCount)
	writeCommandComplete(c.writer, tag)
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
		delete(c.portals, name)
	}

	writeCloseComplete(c.writer)
}

func (c *clientConn) sendParameterDescription(paramTypes []int32) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		// If OID is 0, use text type
		if oid == 0 {
			oid = 25 // text
		}
		binary.Write(&buf, binary.BigEndian, oid)
	}
	writeMessage(c.writer, 't', buf.Bytes())
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
