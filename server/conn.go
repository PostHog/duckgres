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
	"strings"
)

type preparedStmt struct {
	query         string
	convertedQuery string
	paramTypes    []int32
	numParams     int
}

type portal struct {
	stmt          *preparedStmt
	paramValues   [][]byte
	resultFormats []int16
	described     bool // true if Describe was called on this portal
}

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
}

func (c *clientConn) serve() error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.pid = int32(os.Getpid())
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)

	// Handle startup
	if err := c.handleStartup(); err != nil {
		return fmt.Errorf("startup failed: %w", err)
	}

	// Get/create DuckDB connection for this user
	db, err := c.server.getOrCreateDB(c.username)
	if err != nil {
		c.sendError("FATAL", "28000", fmt.Sprintf("failed to open database: %v", err))
		return err
	}
	c.db = db

	// Send initial parameters
	c.sendInitialParams()

	// Send ready for query
	if err := writeReadyForQuery(c.writer, 'I'); err != nil {
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
		c.sendError("FATAL", "28P01", "password authentication failed")
		return fmt.Errorf("authentication failed for user %q", c.username)
	}

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
		msgType, body, err := readMessage(c.reader)
		if err != nil {
			if err == io.EOF {
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
			if err := writeReadyForQuery(c.writer, 'I'); err != nil {
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

	if query == "" {
		writeEmptyQueryResponse(c.writer)
		writeReadyForQuery(c.writer, 'I')
		c.writer.Flush()
		return nil
	}

	log.Printf("[%s] Query: %s", c.username, query)

	// Rewrite pg_catalog function calls for compatibility
	query = rewritePgCatalogQuery(query)

	// Determine command type for proper response
	upperQuery := strings.ToUpper(query)
	cmdType := c.getCommandType(upperQuery)

	// For non-SELECT queries, use Exec
	if cmdType != "SELECT" {
		result, err := c.db.Exec(query)
		if err != nil {
			c.sendError("ERROR", "42000", err.Error())
			writeReadyForQuery(c.writer, 'I')
			c.writer.Flush()
			return nil
		}

		tag := c.buildCommandTag(cmdType, result)
		writeCommandComplete(c.writer, tag)
		writeReadyForQuery(c.writer, 'I')
		c.writer.Flush()
		return nil
	}

	// Execute SELECT query
	rows, err := c.db.Query(query)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		writeReadyForQuery(c.writer, 'I')
		c.writer.Flush()
		return nil
	}
	defer rows.Close()

	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		writeReadyForQuery(c.writer, 'I')
		c.writer.Flush()
		return nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		writeReadyForQuery(c.writer, 'I')
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

	// Send command complete
	tag := fmt.Sprintf("SELECT %d", rowCount)
	writeCommandComplete(c.writer, tag)
	writeReadyForQuery(c.writer, 'I')
	c.writer.Flush()

	return nil
}

func (c *clientConn) getCommandType(upperQuery string) string {
	switch {
	case strings.HasPrefix(upperQuery, "SELECT"):
		return "SELECT"
	case strings.HasPrefix(upperQuery, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(upperQuery, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(upperQuery, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(upperQuery, "CREATE TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(upperQuery, "CREATE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(upperQuery, "CREATE VIEW"):
		return "CREATE VIEW"
	case strings.HasPrefix(upperQuery, "CREATE"):
		return "CREATE"
	case strings.HasPrefix(upperQuery, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(upperQuery, "DROP INDEX"):
		return "DROP INDEX"
	case strings.HasPrefix(upperQuery, "DROP VIEW"):
		return "DROP VIEW"
	case strings.HasPrefix(upperQuery, "DROP"):
		return "DROP"
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
	default:
		// For other types, try to convert to string
		return fmt.Sprintf("%v", val)
	}
}

func (c *clientConn) sendError(severity, code, message string) {
	writeErrorResponse(c.writer, severity, code, message)
	c.writer.Flush()
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

	// Convert PostgreSQL $1, $2 placeholders to ? for database/sql
	convertedQuery, numParams := convertPlaceholders(query)

	// Close existing statement with same name
	delete(c.stmts, stmtName)

	c.stmts[stmtName] = &preparedStmt{
		query:          query,
		convertedQuery: convertedQuery,
		paramTypes:     paramTypes,
		numParams:      numParams,
	}

	log.Printf("[%s] Prepared statement %q: %s", c.username, stmtName, query)
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

		// For SELECT queries, we need to send RowDescription
		// For other queries, send NoData
		upperQuery := strings.ToUpper(strings.TrimSpace(ps.query))
		if !strings.HasPrefix(upperQuery, "SELECT") {
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

	case 'P':
		// Describe portal
		p, ok := c.portals[name]
		if !ok {
			c.sendError("ERROR", "34000", fmt.Sprintf("portal %q does not exist", name))
			return
		}
		p.described = true

		// For non-SELECT, send NoData
		upperQuery := strings.ToUpper(strings.TrimSpace(p.stmt.query))
		if !strings.HasPrefix(upperQuery, "SELECT") {
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

	log.Printf("[%s] Execute %q with %d params: %s", c.username, portalName, len(args), p.stmt.query)

	if cmdType != "SELECT" {
		// Non-SELECT: use Exec with converted query
		result, err := c.db.Exec(p.stmt.convertedQuery, args...)
		if err != nil {
			c.sendError("ERROR", "42000", err.Error())
			return
		}
		tag := c.buildCommandTag(cmdType, result)
		writeCommandComplete(c.writer, tag)
		return
	}

	// SELECT: use Query with converted query
	rows, err := c.db.Query(p.stmt.convertedQuery, args...)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		return
	}

	// Get column types for binary encoding
	colTypes, _ := rows.ColumnTypes()
	typeOIDs := make([]int32, len(cols))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Don't send RowDescription here - it should come from Describe

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

// convertPlaceholders converts PostgreSQL $1, $2 placeholders to ?
// Returns the converted query and the number of parameters found
func convertPlaceholders(query string) (string, int) {
	result := query
	count := 0
	for i := 1; i <= 100; i++ {
		placeholder := fmt.Sprintf("$%d", i)
		if !strings.Contains(result, placeholder) {
			break
		}
		result = strings.Replace(result, placeholder, "?", 1)
		count++
	}
	return result, count
}
