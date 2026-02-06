package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler"
)

type preparedStmt struct {
	query             string
	convertedQuery    string
	paramTypes        []int32
	numParams         int
	isIgnoredSet      bool     // True if this is an ignored SET parameter
	isNoOp            bool     // True if this is a no-op command (CREATE INDEX, etc.)
	noOpTag           string   // Command tag for no-op commands
	described         bool     // True if Describe(S) was called on this statement
	statements        []string // Multi-statement rewrite (e.g., writable CTE)
	cleanupStatements []string // Cleanup statements for multi-statement (DROP temp tables, COMMIT)
}

type portal struct {
	stmt          *preparedStmt
	paramValues   [][]byte
	paramFormats  []int16 // 0=text, 1=binary for each parameter
	resultFormats []int16
	described     bool // true if Describe was called on this portal
}

// decodeParams converts raw parameter bytes to Go values based on format codes.
// Returns (args, nil) on success, or (nil, error) for malformed binary data.
// On error, caller should send ErrorResponse with SQLSTATE 08P01.
func (p *portal) decodeParams() ([]interface{}, error) {
	args := make([]interface{}, len(p.paramValues))
	for i, v := range p.paramValues {
		if v == nil {
			args[i] = nil
			continue
		}

		// Get type OID for this parameter
		typeOID := int32(0) // Unknown
		if i < len(p.stmt.paramTypes) {
			typeOID = p.stmt.paramTypes[i]
		}

		// Get format code for this parameter
		format := int16(0) // Default to text
		if len(p.paramFormats) == 1 {
			// Single format code applies to all parameters
			format = p.paramFormats[0]
		} else if i < len(p.paramFormats) {
			// Per-parameter format codes
			format = p.paramFormats[i]
		}

		// CRITICAL: Per PostgreSQL spec, when type is unknown (OID 0),
		// IGNORE binary format code and always treat as text.
		// "Anything you have down as UNKNOWN, send as text."
		if typeOID == 0 || format == 0 {
			// Unknown type OR text format: treat as string
			args[i] = string(v)
		} else {
			// Known type AND binary format: decode per type
			val, err := decodeBinary(v, typeOID)
			if err != nil {
				return nil, fmt.Errorf("parameter %d: %w", i+1, err)
			}
			args[i] = val
		}
	}
	return args, nil
}

// Transaction status constants for PostgreSQL wire protocol
const (
	txStatusIdle        = 'I' // Not in a transaction
	txStatusTransaction = 'T' // In a transaction
	txStatusError       = 'E' // In a failed transaction
)

type clientConn struct {
	server    *Server
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	username  string
	database  string
	db        *sql.DB
	pid       int32
	secretKey int32                    // unique key for cancel requests
	stmts     map[string]*preparedStmt // prepared statements by name
	portals   map[string]*portal       // portals by name
	txStatus  byte                     // current transaction status ('I', 'T', or 'E')
}

// newTranspiler creates a transpiler configured for this connection.
func (c *clientConn) newTranspiler(convertPlaceholders bool) *transpiler.Transpiler {
	return transpiler.New(transpiler.Config{
		DuckLakeMode:        c.server.cfg.DuckLake.MetadataStore != "",
		ConvertPlaceholders: convertPlaceholders,
	})
}

// generateSecretKey generates a cryptographically random secret key for cancel requests.
func generateSecretKey() int32 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<31))
	if err != nil {
		// Fallback to time-based key if crypto/rand fails
		return int32(time.Now().UnixNano() & 0x7FFFFFFF)
	}
	return int32(n.Int64())
}

// backendKey returns the backend key for this connection, used for cancel requests.
func (c *clientConn) backendKey() BackendKey {
	return BackendKey{Pid: c.pid, SecretKey: c.secretKey}
}

// queryContext returns a cancellable context for query execution.
// The cancel function is registered with the server so it can be invoked
// via a cancel request from another connection.
// The caller must call the returned cleanup function when the query completes.
//
// In child worker processes, the context is also cancelled when the server's
// externalCancelCh is closed (triggered by SIGUSR1 signal).
func (c *clientConn) queryContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	key := c.backendKey()
	c.server.RegisterQuery(key, cancel)

	// If there's an external cancel channel (child worker mode), set up a goroutine
	// to cancel the context when the channel is closed
	if c.server.externalCancelCh != nil {
		go func() {
			select {
			case <-c.server.externalCancelCh:
				cancel()
			case <-ctx.Done():
				// Context already cancelled, nothing to do
			}
		}()
	}

	cleanup := func() {
		c.server.UnregisterQuery(key)
		cancel()
	}

	return ctx, cleanup
}

// isQueryCancelled checks if an error is due to query cancellation
func isQueryCancelled(err error) bool {
	return err == context.Canceled || (err != nil && strings.Contains(err.Error(), "context canceled"))
}

// isConnectionBroken checks if an error indicates a broken connection
// (e.g., SSL connection closed, network error)
func isConnectionBroken(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "ssl connection has been closed") ||
		strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "network is unreachable") ||
		strings.Contains(errMsg, "no route to host") ||
		strings.Contains(errMsg, "i/o timeout") ||
		strings.Contains(errMsg, "use of closed network connection")
}

// safeCleanupDB safely closes the database connection, handling the case where
// the underlying connection (e.g., DuckLake's SSL connection to RDS) may be broken.
//
// This mitigates crashes from DuckDB throwing C++ exceptions during cleanup by:
// 1. Detecting broken connections early via a health check query
// 2. Explicitly rolling back transactions before Close() to avoid DuckDB's internal ROLLBACK
// 3. Skipping SQL cleanup operations when the connection is known to be broken
//
// Note: If the connection breaks between our health check and Close(), DuckDB may still
// throw a C++ exception. This is a best-effort mitigation, not a complete fix.
func (c *clientConn) safeCleanupDB() {
	cleanupTimeout := 5 * time.Second
	connHealthy := true

	// Check connection health. For DuckLake, we need to actually run a query that
	// touches the metadata connection, not just ping the local DuckDB connection.
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	if c.server.cfg.DuckLake.MetadataStore != "" {
		// Query DuckLake metadata to verify the RDS connection is still alive
		_, err := c.db.ExecContext(ctx, "SELECT 1 FROM ducklake.information_schema.schemata LIMIT 1")
		if err != nil {
			slog.Warn("DuckLake connection unhealthy during cleanup, skipping SQL cleanup.",
				"user", c.username, "error", err)
			connHealthy = false
		}
	} else {
		if err := c.db.PingContext(ctx); err != nil {
			slog.Warn("Database connection unhealthy during cleanup, skipping SQL cleanup.",
				"user", c.username, "error", err)
			connHealthy = false
		}
	}
	cancel()

	// If we're in a transaction, explicitly ROLLBACK before closing.
	// This prevents DuckDB from trying to ROLLBACK internally during Close(),
	// which can throw exceptions if the connection is in a bad state.
	if connHealthy && (c.txStatus == txStatusTransaction || c.txStatus == txStatusError) {
		ctx2, cancel2 := context.WithTimeout(context.Background(), cleanupTimeout)
		_, err := c.db.ExecContext(ctx2, "ROLLBACK")
		cancel2()
		if err != nil {
			slog.Warn("Failed to rollback transaction during cleanup.",
				"user", c.username, "error", err)
			if isConnectionBroken(err) {
				connHealthy = false
			}
		}
	}

	// Detach DuckLake to release the RDS metadata connection (only if connection is healthy)
	if connHealthy && c.server.cfg.DuckLake.MetadataStore != "" {
		// Must switch away from ducklake before detaching - DuckDB doesn't allow
		// detaching the default database
		ctx3, cancel3 := context.WithTimeout(context.Background(), cleanupTimeout)
		_, err := c.db.ExecContext(ctx3, "USE memory")
		cancel3()
		if err != nil {
			slog.Warn("Failed to switch to memory.", "user", c.username, "error", err)
			if isConnectionBroken(err) {
				connHealthy = false
			}
		}

		if connHealthy {
			ctx4, cancel4 := context.WithTimeout(context.Background(), cleanupTimeout)
			_, err := c.db.ExecContext(ctx4, "DETACH ducklake")
			cancel4()
			if err != nil {
				slog.Warn("Failed to detach DuckLake.", "user", c.username, "error", err)
			}
		}
	}

	// Always attempt to close the database connection.
	// If the connection is broken, this may still throw, but we've done our best
	// to clean up the transaction state first.
	if err := c.db.Close(); err != nil {
		slog.Warn("Failed to close database.", "user", c.username, "error", err)
	}
}

// validateWithDuckDB checks if a query is valid DuckDB syntax.
// This is used when PostgreSQL parsing fails to determine if the query should
// be executed natively by DuckDB.
func (c *clientConn) validateWithDuckDB(query string) error {
	// Check if this is a utility command that doesn't support EXPLAIN
	// For these, we skip validation and let DuckDB handle them directly
	if isDuckDBUtilityCommand(query) {
		return nil
	}

	// Skip EXPLAIN validation for queries with parameter placeholders ($1, $2, etc.)
	// EXPLAIN cannot handle unbound parameters - we'll let DuckDB validate at execution time
	if hasParameterPlaceholders(query) {
		return nil
	}

	// Use EXPLAIN to validate the query without executing it
	// DuckDB's EXPLAIN will fail if the query is syntactically invalid
	_, err := c.db.Exec("EXPLAIN " + query)
	if err != nil {
		// Strip "EXPLAIN " from error messages to avoid confusing users,
		// but only if the original query didn't start with EXPLAIN
		errMsg := err.Error()
		upperQuery := strings.ToUpper(strings.TrimSpace(stripLeadingComments(query)))
		if !strings.HasPrefix(upperQuery, "EXPLAIN") {
			errMsg = strings.Replace(errMsg, "EXPLAIN ", "", 1)
		}
		return fmt.Errorf("%s", errMsg)
	}
	return nil
}

// paramPlaceholderRegex matches PostgreSQL-style $N parameter placeholders
var paramPlaceholderRegex = regexp.MustCompile(`\$\d+`)

// hasParameterPlaceholders returns true if the query contains $N placeholders
func hasParameterPlaceholders(query string) bool {
	return paramPlaceholderRegex.MatchString(query)
}

// isDuckDBUtilityCommand checks if a query is a DuckDB utility command
// that doesn't support EXPLAIN validation. These commands are passed
// through directly to DuckDB without pre-validation.
func isDuckDBUtilityCommand(query string) bool {
	// Strip leading comments and get the first keyword (case-insensitive)
	upper := strings.ToUpper(stripLeadingComments(query))

	// List of DuckDB utility commands that don't support EXPLAIN
	// All prefixes should NOT have trailing spaces - we check word boundaries separately
	utilityPrefixes := []string{
		"ATTACH",
		"DETACH",
		"USE",
		"INSTALL",
		"LOAD",
		"UNLOAD",
		"CREATE SECRET",
		"DROP SECRET",
		"CREATE PERSISTENT SECRET",
		"CREATE TEMPORARY SECRET",
		"CREATE OR REPLACE SECRET",
		"CREATE OR REPLACE PERSISTENT SECRET",
		"CREATE OR REPLACE TEMPORARY SECRET",
		"PRAGMA",
		"CHECKPOINT",
		"FORCE CHECKPOINT",
		"EXPORT DATABASE",
		"IMPORT DATABASE",
		"CALL",
		"SET",
		"RESET",
	}

	for _, prefix := range utilityPrefixes {
		if hasCommandPrefix(upper, prefix) {
			return true
		}
	}

	return false
}

// hasCommandPrefix checks if query starts with the given command prefix,
// ensuring it's followed by a word boundary (space, newline, semicolon, or end of string).
func hasCommandPrefix(query, prefix string) bool {
	if !strings.HasPrefix(query, prefix) {
		return false
	}
	// Check that prefix is followed by a word boundary
	if len(query) == len(prefix) {
		return true // Exact match
	}
	next := query[len(prefix)]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == ';'
}

func (c *clientConn) serve() error {
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.pid = int32(os.Getpid())
	c.secretKey = generateSecretKey()
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
			c.safeCleanupDB()
		}
	}()

	// Send initial parameters
	c.sendInitialParams()

	// Send ready for query
	if err := writeReadyForQuery(c.writer, c.txStatus); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

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

			slog.Info("TLS connection established.", "remote_addr", c.conn.RemoteAddr())
			continue
		}

		// Handle cancel request
		if params["__cancel_request"] == "true" {
			// Extract pid and secret key from the cancel request
			if pidStr, ok := params["__cancel_pid"]; ok {
				if secretKeyStr, ok := params["__cancel_secret_key"]; ok {
					pid, _ := strconv.ParseInt(pidStr, 10, 32)
					secretKey, _ := strconv.ParseInt(secretKeyStr, 10, 32)
					key := BackendKey{Pid: int32(pid), SecretKey: int32(secretKey)}
					c.server.CancelQuery(key)
				}
			}
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
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

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
			slog.Warn("IP banned after too many failed auth attempts.", "remote_addr", c.conn.RemoteAddr())
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

	slog.Info("User authenticated.", "user", c.username, "remote_addr", c.conn.RemoteAddr())
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
		if err := writeParameterStatus(c.writer, name, value); err != nil {
			slog.Warn("Failed to write parameter.", "param", name, "error", err)
		}
	}

	// Send backend key data (pid and secret key for cancel requests)
	if err := writeBackendKeyData(c.writer, c.pid, c.secretKey); err != nil {
		slog.Warn("Failed to write backend key data.", "error", err)
	}
}

func (c *clientConn) messageLoop() error {
	for {
		// Set read deadline if idle timeout is configured
		if c.server.cfg.IdleTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.server.cfg.IdleTimeout))
		}

		msgType, body, err := readMessage(c.reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			// Check if this is a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				slog.Info("Connection idle timeout, closing.", "user", c.username)
				return nil
			}
			return err
		}

		switch msgType {
		case msgQuery:
			if err := c.handleQuery(body); err != nil {
				slog.Error("Query error.", "error", err)
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
			_ = c.writer.Flush()

		case msgClose:
			// Extended query protocol - Close
			c.handleClose(body)

		case msgFlush:
			_ = c.writer.Flush()

		case msgTerminate:
			return nil

		default:
			slog.Warn("Unknown message type.", "type", string(msgType))
		}
	}
}

func (c *clientConn) handleQuery(body []byte) error {
	query := string(bytes.TrimRight(body, "\x00"))
	query = strings.TrimSpace(query)

	// Treat empty queries or queries with just semicolons as empty
	// PostgreSQL returns EmptyQueryResponse for queries like "" or ";" or ";;;"
	if query == "" || isEmptyQuery(query) {
		_ = writeEmptyQueryResponse(c.writer)
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	start := time.Now()
	defer func() { queryDurationHistogram.Observe(time.Since(start).Seconds()) }()
	slog.Debug("Query received.", "user", c.username, "query", query)

	// Check for multi-statement query (PostgreSQL simple query protocol supports
	// multiple semicolon-separated statements in a single Q message).
	// Each statement gets its own results, with a single ReadyForQuery at the end.
	tree, parseErr := pg_query.Parse(query)
	if parseErr == nil && len(tree.Stmts) > 1 {
		return c.handleMultiStatementQuery(tree)
	}

	// Transpile PostgreSQL SQL to DuckDB-compatible SQL
	tr := c.newTranspiler(false)
	result, err := tr.Transpile(query)
	if err != nil {
		// Transform error - send error to client
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Handle fallback to native DuckDB: PostgreSQL parsing failed, try DuckDB directly
	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			// Neither PostgreSQL nor DuckDB can parse this query
			c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
		slog.Debug("Fallback to native DuckDB: query not valid PostgreSQL but valid DuckDB.", "user", c.username, "query", query)
	}

	// Handle transform-detected errors (e.g., unrecognized config parameter)
	if result.Error != nil {
		c.sendError("ERROR", "42704", result.Error.Error())
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Handle ignored SET parameters
	if result.IsIgnoredSet {
		slog.Debug("Ignoring PostgreSQL-specific SET.", "user", c.username, "query", query)
		_ = writeCommandComplete(c.writer, "SET")
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.)
	if result.IsNoOp {
		slog.Debug("No-op command (DuckLake limitation).", "user", c.username, "query", query)
		_ = writeCommandComplete(c.writer, result.NoOpTag)
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Handle multi-statement results (writable CTE rewrites)
	if len(result.Statements) > 0 {
		slog.Debug("Multi-statement query.", "user", c.username, "statements", len(result.Statements), "cleanup", len(result.CleanupStatements))
		return c.executeMultiStatement(result.Statements, result.CleanupStatements)
	}

	// Use the transpiled SQL
	originalQuery := query
	query = result.SQL

	// Log the transpiled query if it differs from the original
	if query != originalQuery {
		slog.Debug("Query transpiled.", "user", c.username, "executed", query)
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
			_ = writeCommandComplete(c.writer, "BEGIN")
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}

		ctx, cleanup := c.queryContext()
		defer cleanup()

		result, err := c.db.ExecContext(ctx, query)
		if err != nil {
			// Retry ALTER TABLE as ALTER VIEW if target is a view
			if isAlterTableNotTableError(err) {
				if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(query); ok {
					result, err = c.db.ExecContext(ctx, alteredQuery)
				}
			}
			if err != nil {
				if isQueryCancelled(err) {
					c.sendError("ERROR", "57014", "canceling statement due to user request")
				} else {
					slog.Error("Query execution failed.", "user", c.username, "query", query, "error", err)
					c.sendError("ERROR", "42000", err.Error())
				}
				c.setTxError()
				_ = writeReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}
		}

		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		_ = writeCommandComplete(c.writer, tag)
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Execute SELECT query
	ctx, cleanup := c.queryContext()
	defer cleanup()

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		if isQueryCancelled(err) {
			c.sendError("ERROR", "57014", "canceling statement due to user request")
		} else {
			slog.Error("Query execution failed.", "user", c.username, "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
		}
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	defer func() { _ = rows.Close() }()

	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
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
	_ = writeCommandComplete(c.writer, tag)
	_ = writeReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()

	return nil
}

// handleMultiStatementQuery processes multiple semicolon-separated statements
// from a single Q (simple query) message. Per the PostgreSQL wire protocol,
// each statement gets its own RowDescription/DataRow/CommandComplete messages,
// with a single ReadyForQuery at the end. If any statement fails, remaining
// statements are skipped.
func (c *clientConn) handleMultiStatementQuery(tree *pg_query.ParseResult) error {
	slog.Debug("Multi-statement simple query.", "user", c.username, "count", len(tree.Stmts))

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

		errSent, fatalErr := c.executeSingleStatement(singleSQL)
		if fatalErr != nil {
			return fatalErr
		}
		if errSent {
			break // Stop processing remaining statements on error
		}
	}

	_ = writeReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// executeSingleStatement transpiles and executes a single SQL statement,
// sending results to the client. Does NOT send ReadyForQuery (the caller
// is responsible for that). Returns (true, nil) if an error was sent to the
// client (so the caller can stop processing a batch), or (false, err) for
// fatal connection errors.
func (c *clientConn) executeSingleStatement(query string) (errSent bool, fatalErr error) {
	// Transpile
	tr := c.newTranspiler(false)
	result, err := tr.Transpile(query)
	if err != nil {
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		return true, nil
	}

	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
			return true, nil
		}
		slog.Debug("Fallback to native DuckDB.", "user", c.username, "query", query)
	}

	if result.Error != nil {
		c.sendError("ERROR", "42704", result.Error.Error())
		return true, nil
	}

	if result.IsIgnoredSet {
		_ = writeCommandComplete(c.writer, "SET")
		return false, nil
	}

	if result.IsNoOp {
		_ = writeCommandComplete(c.writer, result.NoOpTag)
		return false, nil
	}

	// Multi-statement rewrites (writable CTEs) not supported inside batches
	if len(result.Statements) > 0 {
		c.sendError("ERROR", "0A000", "writable CTEs not supported in multi-statement queries")
		return true, nil
	}

	executedQuery := result.SQL
	if executedQuery != query {
		slog.Debug("Query transpiled.", "user", c.username, "executed", executedQuery)
	}

	upperQuery := strings.ToUpper(executedQuery)
	cmdType := c.getCommandType(upperQuery)

	// COPY not supported inside batches
	if cmdType == "COPY" {
		c.sendError("ERROR", "0A000", "COPY not supported in multi-statement queries")
		return true, nil
	}

	if cmdType != "SELECT" {
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = writeCommandComplete(c.writer, "BEGIN")
			return false, nil
		}

		ctx, cleanup := c.queryContext()
		defer cleanup()

		execResult, err := c.db.ExecContext(ctx, executedQuery)
		if err != nil {
			if isAlterTableNotTableError(err) {
				if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(executedQuery); ok {
					execResult, err = c.db.ExecContext(ctx, alteredQuery)
				}
			}
			if err != nil {
				if isQueryCancelled(err) {
					c.sendError("ERROR", "57014", "canceling statement due to user request")
				} else {
					slog.Error("Query execution failed.", "user", c.username, "query", executedQuery, "error", err)
					c.sendError("ERROR", "42000", err.Error())
				}
				c.setTxError()
				return true, nil
			}
		}

		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, execResult)
		_ = writeCommandComplete(c.writer, tag)
		return false, nil
	}

	// SELECT
	ctx, cleanup := c.queryContext()
	defer cleanup()

	rows, err := c.db.QueryContext(ctx, executedQuery)
	if err != nil {
		if isQueryCancelled(err) {
			c.sendError("ERROR", "57014", "canceling statement due to user request")
		} else {
			slog.Error("Query execution failed.", "user", c.username, "query", executedQuery, "error", err)
			c.sendError("ERROR", "42000", err.Error())
		}
		c.setTxError()
		return true, nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return true, nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return true, nil
	}

	if err := c.sendRowDescription(cols, colTypes); err != nil {
		return false, err
	}

	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			c.sendError("ERROR", "42000", err.Error())
			return true, nil
		}

		if err := c.sendDataRow(values); err != nil {
			return false, err
		}
		rowCount++
	}

	tag := fmt.Sprintf("SELECT %d", rowCount)
	_ = writeCommandComplete(c.writer, tag)
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
		_ = writeEmptyQueryResponse(c.writer)
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Check if we're adding our own transaction wrapper
	hasOurTransaction := len(statements) >= 2 &&
		strings.ToUpper(strings.TrimSpace(statements[0])) == "BEGIN" &&
		len(cleanup) > 0 &&
		strings.ToUpper(strings.TrimSpace(cleanup[len(cleanup)-1])) == "COMMIT"

	// If already in a transaction, skip our BEGIN/COMMIT wrapper
	if hasOurTransaction && c.txStatus == txStatusTransaction {
		statements = statements[1:] // Strip BEGIN
		cleanup = cleanup[:len(cleanup)-1] // Strip COMMIT from cleanup
	}

	// Execute setup statements (all but last)
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		slog.Debug("Multi-stmt setup.", "user", c.username, "step", i+1, "total", len(statements)-1, "stmt", stmt)
		_, err := c.db.Exec(stmt)
		if err != nil {
			slog.Error("Multi-stmt setup error.", "user", c.username, "query", stmt, "error", err)
			c.setTxError()
			// On error, still try to cleanup (best effort)
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}

	// Handle final statement
	finalStmt := statements[len(statements)-1]
	upperFinal := strings.ToUpper(strings.TrimSpace(finalStmt))
	cmdType := c.getCommandType(upperFinal)
	slog.Debug("Multi-stmt final.", "user", c.username, "stmt", finalStmt, "cmd_type", cmdType)

	if cmdType == "SELECT" || strings.HasPrefix(upperFinal, "WITH") || strings.HasPrefix(upperFinal, "TABLE") {
		// SELECT: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.db.Query(finalStmt)
		if err != nil {
			slog.Error("Multi-stmt final query error.", "user", c.username, "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		// DuckDB cursor holds result data even after source tables are dropped
		c.executeCleanup(cleanup)

		// Now stream results from cursor
		return c.streamRowsToClient(rows, cmdType, finalStmt)

	} else {
		// DML (INSERT/UPDATE/DELETE): execute then cleanup
		result, err := c.db.Exec(finalStmt)
		if err != nil {
			slog.Error("Multi-stmt final exec error.", "user", c.username, "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion
		tag := c.buildCommandTag(cmdType, result)
		_ = writeCommandComplete(c.writer, tag)
		_ = writeReadyForQuery(c.writer, c.txStatus)
		return c.writer.Flush()
	}
}

// executeCleanup runs cleanup statements, ignoring errors (best effort).
// This is used to clean up temp tables after a multi-statement query.
func (c *clientConn) executeCleanup(cleanup []string) {
	for _, stmt := range cleanup {
		slog.Debug("Multi-stmt cleanup.", "user", c.username, "stmt", stmt)
		_, err := c.db.Exec(stmt)
		if err != nil {
			// Log but don't fail - cleanup is best effort
			slog.Warn("Multi-stmt cleanup error (ignored).", "user", c.username, "error", err)
		}
	}
}

// executeMultiStatementExtended handles execution of multi-statement query rewrites
// for the extended query protocol (Parse/Bind/Execute).
// Unlike executeMultiStatement, this does NOT send ReadyForQuery (that's done by Sync).
func (c *clientConn) executeMultiStatementExtended(statements []string, cleanup []string, args []interface{}, resultFormats []int16, described bool) {
	if len(statements) == 0 {
		_ = writeEmptyQueryResponse(c.writer)
		return
	}

	// Check if we're adding our own transaction wrapper
	hasOurTransaction := len(statements) >= 2 &&
		strings.ToUpper(strings.TrimSpace(statements[0])) == "BEGIN" &&
		len(cleanup) > 0 &&
		strings.ToUpper(strings.TrimSpace(cleanup[len(cleanup)-1])) == "COMMIT"

	// If already in a transaction, skip our BEGIN/COMMIT wrapper
	if hasOurTransaction && c.txStatus == txStatusTransaction {
		statements = statements[1:]            // Strip BEGIN
		cleanup = cleanup[:len(cleanup)-1]     // Strip COMMIT from cleanup
	}

	// Execute setup statements (all but last)
	for i := 0; i < len(statements)-1; i++ {
		stmt := statements[i]
		slog.Debug("Multi-stmt-ext setup.", "user", c.username, "step", i+1, "total", len(statements)-1, "stmt", stmt)
		_, err := c.db.Exec(stmt, args...)
		if err != nil {
			slog.Error("Multi-stmt-ext setup error.", "user", c.username, "query", stmt, "error", err)
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
	slog.Debug("Multi-stmt-ext final.", "user", c.username, "stmt", finalStmt, "cmd_type", cmdType)

	if cmdType == "SELECT" || strings.HasPrefix(upperFinal, "WITH") || strings.HasPrefix(upperFinal, "TABLE") {
		// SELECT: obtain cursor FIRST, cleanup SECOND, stream THIRD
		rows, err := c.db.Query(finalStmt, args...)
		if err != nil {
			slog.Error("Multi-stmt-ext final query error.", "user", c.username, "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			return
		}
		defer func() { _ = rows.Close() }()

		// Execute cleanup while cursor is open (data is materialized in cursor)
		c.executeCleanup(cleanup)

		// Stream results from cursor (extended protocol version)
		c.streamRowsToClientExtended(rows, cmdType, resultFormats, described, finalStmt)

	} else {
		// DML (INSERT/UPDATE/DELETE): execute then cleanup
		result, err := c.db.Exec(finalStmt, args...)
		if err != nil {
			slog.Error("Multi-stmt-ext final exec error.", "user", c.username, "query", finalStmt, "error", err)
			c.setTxError()
			c.executeCleanup(cleanup)
			c.sendError("ERROR", "42000", err.Error())
			return
		}

		// Execute cleanup
		c.executeCleanup(cleanup)

		// Send completion (no ReadyForQuery - that's done by Sync)
		tag := c.buildCommandTag(cmdType, result)
		_ = writeCommandComplete(c.writer, tag)
	}
}

// streamRowsToClientExtended sends result rows for the extended query protocol.
// Unlike streamRowsToClient, this does NOT send ReadyForQuery, and supports
// binary result formats and the described flag.
func (c *clientConn) streamRowsToClientExtended(rows *sql.Rows, cmdType string, resultFormats []int16, described bool, query string) {
	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		slog.Error("Failed to get column info.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		slog.Error("Failed to get column types.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}

	// Get type OIDs for binary encoding
	typeOIDs := make([]int32, len(cols))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send RowDescription if Describe wasn't called before Execute
	if !described && len(cols) > 0 {
		if err := c.sendRowDescription(cols, colTypes); err != nil {
			return
		}
	}

	// Stream DataRows with format codes
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			slog.Error("Failed to scan row.", "user", c.username, "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			return
		}

		if err := c.sendDataRowWithFormats(values, resultFormats, typeOIDs); err != nil {
			return
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		slog.Error("Row iteration error.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}

	// Send completion (no ReadyForQuery - that's done by Sync)
	tag := fmt.Sprintf("SELECT %d", rowCount)
	_ = writeCommandComplete(c.writer, tag)
}

// streamRowsToClient sends result rows over the wire protocol.
// The rows cursor must already be obtained before calling this function.
func (c *clientConn) streamRowsToClient(rows *sql.Rows, cmdType string, query string) error {
	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		slog.Error("Failed to get column info.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		slog.Error("Failed to get column types.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Send row description
	if err := c.sendRowDescription(cols, colTypes); err != nil {
		return err
	}

	// Stream DataRows
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			slog.Error("Failed to scan row.", "user", c.username, "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
			break
		}

		if err := c.sendDataRow(values); err != nil {
			return err
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		slog.Error("Row iteration error.", "user", c.username, "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Send completion
	tag := fmt.Sprintf("%s %d", cmdType, rowCount)
	_ = writeCommandComplete(c.writer, tag)
	_ = writeReadyForQuery(c.writer, c.txStatus)
	return c.writer.Flush()
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
	// SUMMARIZE returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "SUMMARIZE") {
		return true
	}
	// FROM-first syntax returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "FROM") {
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
	copyBinaryRegex     = regexp.MustCompile(`(?i)\bFORMAT\s+(?:"?binary"?|BINARY)\b`)
	copyWithCSVRegex    = regexp.MustCompile(`(?i)\bCSV\b`)
	copyWithHeaderRegex = regexp.MustCompile(`(?i)\bHEADER\b`)
	copyDelimiterRegex  = regexp.MustCompile(`(?i)\bDELIMITER\s+['"](.)['"]\b`)
	copyNullRegex       = regexp.MustCompile(`(?i)\bNULL\s+'([^']*)'`)
	copyQuoteRegex      = regexp.MustCompile(`(?i)\bQUOTE\s+['"](.)['"]\s*`)
	copyEscapeRegex     = regexp.MustCompile(`(?i)\bESCAPE\s+['"](.)['"]\s*`)
)

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
		TableName:  matches[1],
		Delimiter:  "\t", // Default PostgreSQL text format delimiter
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
	copyOptions := []string{"FORMAT CSV", "AUTO_DETECT FALSE", "STRICT_MODE FALSE"}
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
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	rowsAffected, _ := result.RowsAffected()
	_ = writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowsAffected))
	_ = writeReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyOut handles COPY ... TO STDOUT
func (c *clientConn) handleCopyOut(query, upperQuery string) error {
	matches := copyToStdoutRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		c.sendError("ERROR", "42601", "Invalid COPY TO STDOUT syntax")
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
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
	tr := c.newTranspiler(false)
	if result, err := tr.Transpile(selectQuery); err == nil && !result.FallbackToNative {
		selectQuery = result.SQL
	}

	// Execute the query
	rows, err := c.db.Query(selectQuery)
	if err != nil {
		slog.Error("COPY TO query failed.", "user", c.username, "query", selectQuery, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		slog.Error("COPY TO failed to get columns.", "user", c.username, "query", selectQuery, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	isBinary := copyBinaryRegex.MatchString(query)

	if isBinary {
		return c.handleCopyOutBinary(rows, cols)
	}

	// Parse text/CSV options
	delimiter := "\t"
	if m := copyDelimiterRegex.FindStringSubmatch(query); len(m) > 1 {
		delimiter = m[1]
	} else if copyWithCSVRegex.MatchString(upperQuery) {
		delimiter = ","
	}

	// Send CopyOutResponse (text format)
	if err := writeCopyOutResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	_ = c.writer.Flush()

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

	_ = writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	_ = writeReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyOutBinary handles COPY ... TO STDOUT (FORMAT binary)
// Implements PostgreSQL's binary COPY format: header, binary-encoded tuples, trailer.
// Sends one CopyData message per tuple, with header prepended to the first tuple
// and trailer appended to the last, matching how clients like DuckDB's postgres
// extension consume binary COPY streams via PQgetCopyData.
func (c *clientConn) handleCopyOutBinary(rows *sql.Rows, cols []string) error {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	// Get type OIDs for each column
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send CopyOutResponse (binary format)
	if err := writeCopyOutResponse(c.writer, int16(len(cols)), false); err != nil {
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
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}

		tupleBytes := encodeTuple(values)

		if firstRow {
			// First CopyData message: header + tuple
			msg := make([]byte, 0, len(binaryHeader)+len(tupleBytes))
			msg = append(msg, binaryHeader...)
			msg = append(msg, tupleBytes...)
			if err := writeCopyData(c.writer, msg); err != nil {
				return err
			}
			firstRow = false
		} else {
			if err := writeCopyData(c.writer, tupleBytes); err != nil {
				return err
			}
		}
		rowCount++
	}

	// If no rows, still need to send header + trailer
	if firstRow {
		// No rows at all: send header + trailer in one message
		msg := make([]byte, 0, len(binaryHeader)+2)
		msg = append(msg, binaryHeader...)
		msg = append(msg, 0xFF, 0xFF) // trailer: -1 as int16
		if err := writeCopyData(c.writer, msg); err != nil {
			return err
		}
	} else {
		// Send trailer as its own CopyData message
		if err := writeCopyData(c.writer, []byte{0xFF, 0xFF}); err != nil {
			return err
		}
	}

	// Send CopyDone
	if err := writeCopyDone(c.writer); err != nil {
		return err
	}

	_ = writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
	_ = writeReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return nil
}

// handleCopyIn handles COPY ... FROM STDIN
func (c *clientConn) handleCopyIn(query, upperQuery string) error {
	copyStartTime := time.Now()
	slog.Debug("COPY FROM STDIN starting.", "user", c.username, "query", query)

	// Parse COPY options using the helper function
	opts, err := ParseCopyFromOptions(query)
	if err != nil {
		c.sendError("ERROR", "42601", "Invalid COPY FROM STDIN syntax")
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}

	tableName := opts.TableName
	columnList := opts.ColumnList
	slog.Debug("COPY FROM STDIN parsed.", "user", c.username, "table", tableName, "columns", columnList, "binary", opts.IsBinary)

	// Get column info. If a column list is specified, query only those columns
	// in the specified order to match the binary data field order.
	var colQuery string
	if columnList != "" {
		// columnList is "(col1, col2, ...)" — use it in SELECT to get types in COPY order
		colQuery = fmt.Sprintf("SELECT %s FROM %s LIMIT 0", columnList[1:len(columnList)-1], tableName)
	} else {
		colQuery = fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)
	}
	testRows, err := c.db.Query(colQuery)
	if err != nil {
		slog.Error("COPY FROM table check failed.", "user", c.username, "table", tableName, "error", err)
		c.sendError("ERROR", "42P01", fmt.Sprintf("relation \"%s\" does not exist", tableName))
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
		_ = c.writer.Flush()
		return nil
	}
	cols, _ := testRows.Columns()
	colTypes, _ := testRows.ColumnTypes()
	_ = testRows.Close()

	// Branch to binary handler if binary format
	if opts.IsBinary {
		return c.handleCopyInBinary(opts, cols, colTypes)
	}

	// Send CopyInResponse
	if err := writeCopyInResponse(c.writer, int16(len(cols)), true); err != nil {
		return err
	}
	_ = c.writer.Flush()
	slog.Debug("COPY FROM STDIN sent CopyInResponse, waiting for data.", "user", c.username)

	// Create temp file upfront and stream data directly to it (avoids memory buffering)
	// This approach leverages DuckDB's highly optimized CSV parser which handles
	// type conversions automatically and can load millions of rows in seconds.
	tmpFile, err := os.CreateTemp("", "duckgres-copy-*.csv")
	if err != nil {
		slog.Error("COPY FROM STDIN failed to create temp file.", "user", c.username, "error", err)
		c.sendError("ERROR", "58000", fmt.Sprintf("failed to create temp file: %v", err))
		c.setTxError()
		_ = writeReadyForQuery(c.writer, c.txStatus)
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
		msgType, body, err := readMessage(c.reader)
		if err != nil {
			slog.Error("COPY FROM STDIN error reading message.", "user", c.username, "error", err)
			_ = tmpFile.Close()
			return err
		}

		switch msgType {
		case msgCopyData:
			n, err := tmpFile.Write(body)
			if err != nil {
				slog.Error("COPY FROM STDIN failed to write to temp file.", "user", c.username, "error", err)
				_ = tmpFile.Close()
				c.sendError("ERROR", "58000", fmt.Sprintf("failed to write to temp file: %v", err))
				c.setTxError()
				_ = writeReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}
			bytesWritten += int64(n)
			copyDataMessages++
			if copyDataMessages%10000 == 0 {
				slog.Debug("COPY FROM STDIN progress.", "user", c.username, "messages", copyDataMessages, "bytes", bytesWritten)
			}

		case msgCopyDone:
			_ = tmpFile.Close()
			dataReceiveElapsed := time.Since(dataReceiveStart)
			slog.Debug("COPY FROM STDIN CopyDone received.", "user", c.username, "messages", copyDataMessages, "bytes", bytesWritten, "duration", dataReceiveElapsed)

			// Build DuckDB COPY FROM statement using the helper function
			copySQL := BuildDuckDBCopyFromSQL(tableName, columnList, tmpPath, opts)

			slog.Debug("COPY FROM STDIN executing native DuckDB COPY.", "user", c.username, "sql", copySQL)
			loadStart := time.Now()

			result, err := c.db.Exec(copySQL)
			if err != nil {
				slog.Error("COPY FROM STDIN DuckDB COPY failed.", "user", c.username, "error", err)
				c.sendError("ERROR", "22P02", fmt.Sprintf("COPY failed: %v", err))
				c.setTxError()
				_ = writeReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			rowCount64, _ := result.RowsAffected()
			rowCount = int(rowCount64)

			totalElapsed := time.Since(copyStartTime)
			loadElapsed := time.Since(loadStart)
			slog.Info("COPY FROM STDIN completed.", "user", c.username, "rows", rowCount, "total_duration", totalElapsed, "load_duration", loadElapsed)

			_ = writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		case msgCopyFail:
			// Client cancelled COPY
			errMsg := string(bytes.TrimRight(body, "\x00"))
			c.sendError("ERROR", "57014", fmt.Sprintf("COPY failed: %s", errMsg))
			c.setTxError()
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		default:
			c.sendError("ERROR", "08P01", fmt.Sprintf("unexpected message type during COPY: %c", msgType))
			c.setTxError()
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}
}

// handleCopyInBinary handles COPY ... FROM STDIN with binary format.
// It parses the PostgreSQL binary COPY format, decodes each field, and INSERTs rows.
func (c *clientConn) handleCopyInBinary(opts *CopyFromOptions, cols []string, colTypes []*sql.ColumnType) error {
	copyStartTime := time.Now()

	// Get type OIDs for decoding
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send CopyInResponse (binary format)
	if err := writeCopyInResponse(c.writer, int16(len(cols)), false); err != nil {
		return err
	}
	_ = c.writer.Flush()
	slog.Debug("COPY FROM STDIN binary: sent CopyInResponse.", "user", c.username)

	// Collect all CopyData messages into a buffer
	var buf bytes.Buffer
	for {
		msgType, body, err := readMessage(c.reader)
		if err != nil {
			slog.Error("COPY FROM STDIN binary: error reading message.", "user", c.username, "error", err)
			return err
		}

		switch msgType {
		case msgCopyData:
			buf.Write(body)

		case msgCopyDone:
			// Parse binary data and insert rows
			data := buf.Bytes()
			rowCount, err := c.parseBinaryCopyAndInsert(data, opts.TableName, opts.ColumnList, cols, typeOIDs)
			if err != nil {
				slog.Error("COPY FROM STDIN binary: parse/insert failed.", "user", c.username, "error", err)
				c.sendError("ERROR", "22P02", fmt.Sprintf("COPY failed: %v", err))
				c.setTxError()
				_ = writeReadyForQuery(c.writer, c.txStatus)
				_ = c.writer.Flush()
				return nil
			}

			elapsed := time.Since(copyStartTime)
			slog.Info("COPY FROM STDIN binary completed.", "user", c.username, "rows", rowCount, "bytes", buf.Len(), "duration", elapsed)

			_ = writeCommandComplete(c.writer, fmt.Sprintf("COPY %d", rowCount))
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		case msgCopyFail:
			errMsg := string(bytes.TrimRight(body, "\x00"))
			c.sendError("ERROR", "57014", fmt.Sprintf("COPY failed: %s", errMsg))
			c.setTxError()
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil

		default:
			c.sendError("ERROR", "08P01", fmt.Sprintf("unexpected message type during COPY: %c", msgType))
			c.setTxError()
			_ = writeReadyForQuery(c.writer, c.txStatus)
			_ = c.writer.Flush()
			return nil
		}
	}
}

// parseBinaryCopyAndInsert parses PostgreSQL binary COPY format data and INSERTs rows.
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
	// flags := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	extLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	offset += int(extLen) // skip extension area

	// Build INSERT statement: INSERT INTO table (col1, col2, ...) VALUES ($1, $2, ...)
	numCols := len(cols)
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	colNames := columnList
	if colNames == "" {
		quotedCols := make([]string, numCols)
		for i, col := range cols {
			quotedCols[i] = fmt.Sprintf(`"%s"`, col)
		}
		colNames = "(" + strings.Join(quotedCols, ", ") + ")"
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s %s VALUES (%s)",
		tableName, colNames, strings.Join(placeholders, ", "))

	// Parse tuples and insert
	rowCount := 0
	for offset < len(data) {
		if offset+2 > len(data) {
			return rowCount, fmt.Errorf("truncated binary COPY data at tuple header")
		}

		fieldCount := int16(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		// Trailer: field count of -1
		if fieldCount == -1 {
			break
		}

		if int(fieldCount) != numCols {
			return rowCount, fmt.Errorf("binary COPY field count mismatch: got %d, expected %d", fieldCount, numCols)
		}

		values := make([]interface{}, numCols)
		for i := 0; i < numCols; i++ {
			if offset+4 > len(data) {
				return rowCount, fmt.Errorf("truncated binary COPY data at field %d length", i)
			}

			fieldLen := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4

			if fieldLen == -1 {
				values[i] = nil
			} else {
				if offset+int(fieldLen) > len(data) {
					return rowCount, fmt.Errorf("truncated binary COPY data at field %d data", i)
				}
				fieldData := data[offset : offset+int(fieldLen)]
				offset += int(fieldLen)

				decoded, err := decodeBinaryCopy(fieldData, typeOIDs[i])
				if err != nil {
					return rowCount, fmt.Errorf("failed to decode field %d (OID %d, %d bytes): %v", i, typeOIDs[i], fieldLen, err)
				}
				values[i] = decoded
			}
		}

		if _, err := c.db.Exec(insertSQL, values...); err != nil {
			return rowCount, fmt.Errorf("INSERT failed at row %d: %v", rowCount+1, err)
		}
		rowCount++
	}

	return rowCount, nil
}

// decodeBinaryCopy decodes a binary COPY field, using field length to resolve type ambiguity.
// DuckDB's postgres extension may send different integer widths than what the table OID suggests.
func decodeBinaryCopy(data []byte, oid int32) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	// Zero-length fields: return empty string for text types, nil for others
	if len(data) == 0 {
		switch oid {
		case OidText, OidVarchar, OidBpchar, OidName, OidJSON, OidJSONB:
			return "", nil
		default:
			return nil, nil
		}
	}

	switch oid {
	case OidBool:
		return decodeBool(data)
	case OidInt2, OidInt4, OidInt8:
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
	case OidDate:
		return decodeDate(data)
	case OidTimestamp, OidTimestamptz:
		return decodeTimestamp(data)
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
	return fmt.Sprintf("%v", v)
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

func (c *clientConn) sendRowDescription(cols []string, colTypes []*sql.ColumnType) error {
	var buf bytes.Buffer

	// Number of fields
	_ = binary.Write(&buf, binary.BigEndian, int16(len(cols)))

	for i, col := range cols {
		// Column name (null-terminated)
		buf.WriteString(col)
		buf.WriteByte(0)

		// Table OID (0 = not from a table)
		_ = binary.Write(&buf, binary.BigEndian, int32(0))

		// Column attribute number (0 = not from a table)
		_ = binary.Write(&buf, binary.BigEndian, int16(0))

		// Data type OID - check for pg_catalog column name overrides first,
		// then fall back to DuckDB type mapping
		oid := c.mapTypeOIDWithColumnName(col, colTypes[i])
		_ = binary.Write(&buf, binary.BigEndian, oid)

		// Data type size - use appropriate size for overridden types
		typeSize := c.mapTypeSizeWithColumnName(col, colTypes[i])
		_ = binary.Write(&buf, binary.BigEndian, typeSize)

		// Type modifier (-1 = no modifier)
		_ = binary.Write(&buf, binary.BigEndian, int32(-1))

		// Format code (0 = text, 1 = binary)
		_ = binary.Write(&buf, binary.BigEndian, int16(0))
	}

	return writeMessage(c.writer, msgRowDescription, buf.Bytes())
}

func (c *clientConn) mapTypeOIDWithColumnName(colName string, colType *sql.ColumnType) int32 {
	// Check if this column name has a specific pg_catalog type override
	if oid, ok := pgCatalogColumnOIDs[colName]; ok {
		return oid
	}
	return getTypeInfo(colType).OID
}

func (c *clientConn) mapTypeSizeWithColumnName(colName string, colType *sql.ColumnType) int16 {
	// Return appropriate sizes for overridden types
	if oid, ok := pgCatalogColumnOIDs[colName]; ok {
		switch oid {
		case OidName:
			return 64 // name is 64 bytes
		case OidChar:
			return 1 // "char" is 1 byte
		case OidText:
			return -1 // text is variable length
		case OidInt2:
			return 2 // smallint is 2 bytes
		}
	}
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
	_ = binary.Write(&buf, binary.BigEndian, int16(len(values)))

	for i, v := range values {
		if v == nil {
			// NULL value
			_ = binary.Write(&buf, binary.BigEndian, int32(-1))
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
				_ = binary.Write(&buf, binary.BigEndian, int32(len(str)))
				buf.WriteString(str)
			} else {
				_ = binary.Write(&buf, binary.BigEndian, int32(len(encoded)))
				buf.Write(encoded)
			}
		} else {
			// Text encoding
			str := formatValue(v)
			_ = binary.Write(&buf, binary.BigEndian, int32(len(str)))
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
	// Class 28 = "Invalid Authorization Specification" (auth failures).
	// All current FATAL errors use class 28, so this covers both auth
	// failures and connection rejections (no SSL, no user, wrong password).
	// NOTE: If one adds a FATAL error with a non-28 code, be sure to add
	// a metric for it here.
	if strings.HasPrefix(code, "28") {
		authFailuresCounter.Inc()
	} else if severity == "ERROR" {
		queryErrorsCounter.Inc()
	}
	_ = writeErrorResponse(c.writer, severity, code, message)
	_ = c.writer.Flush()
}

func (c *clientConn) sendNotice(severity, code, message string) {
	_ = writeNoticeResponse(c.writer, severity, code, message)
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

	// Handle fallback to native DuckDB: PostgreSQL parsing failed, try DuckDB directly
	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			// Neither PostgreSQL nor DuckDB can parse this query
			c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
			return
		}
		slog.Debug("Fallback to native DuckDB: query not valid PostgreSQL but valid DuckDB.", "user", c.username, "query", query)
	}

	// Close existing statement with same name
	delete(c.stmts, stmtName)

	c.stmts[stmtName] = &preparedStmt{
		query:             query,             // Keep original for logging and Describe
		convertedQuery:    result.SQL,        // Transpiled SQL for execution
		paramTypes:        paramTypes,
		numParams:         result.ParamCount,
		isIgnoredSet:      result.IsIgnoredSet,
		isNoOp:            result.IsNoOp,
		noOpTag:           result.NoOpTag,
		statements:        result.Statements,        // Multi-statement rewrite (writable CTE)
		cleanupStatements: result.CleanupStatements, // Cleanup statements
	}

	slog.Debug("Prepared statement.", "user", c.username, "name", stmtName, "query", query)
	if len(result.Statements) > 0 {
		slog.Debug("Prepared statement multi-statement.", "user", c.username, "name", stmtName, "statements", len(result.Statements), "cleanup", len(result.CleanupStatements))
	} else if result.SQL != query {
		slog.Debug("Prepared statement transpiled.", "user", c.username, "name", stmtName, "transpiled", result.SQL)
	}
	_ = writeParseComplete(c.writer)
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
		paramFormats:  paramFormats,
		resultFormats: resultFormats,
		described:     ps.described, // Inherit from statement if Describe(S) was called
	}

	_ = writeBindComplete(c.writer)
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
		slog.Debug("Describe statement.", "user", c.username, "name", name, "query", ps.query)

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
		slog.Debug("Describe statement returns results check.", "user", c.username, "name", name, "returns_results", returnsResults)
		if !returnsResults {
			_ = writeNoData(c.writer)
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
			slog.Debug("Describe failed to get columns.", "user", c.username, "error", err)
			_ = writeNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		_ = rows.Close()

		if len(cols) == 0 {
			_ = writeNoData(c.writer)
			return
		}

		slog.Debug("Describe statement sending RowDescription.", "user", c.username, "columns", len(cols))
		_ = c.sendRowDescription(cols, colTypes)
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
			_ = writeNoData(c.writer)
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

		// Try to get column info
		rows, err := c.db.Query(p.stmt.convertedQuery, args...)
		if err != nil {
			// Can't describe - send NoData
			_ = writeNoData(c.writer)
			return
		}

		cols, _ := rows.Columns()
		colTypes, _ := rows.ColumnTypes()
		_ = rows.Close()

		if len(cols) == 0 {
			_ = writeNoData(c.writer)
			return
		}

		// Only mark as described when we actually send RowDescription.
		// If we sent NoData above, Execute should still send RowDescription.
		p.described = true
		_ = c.sendRowDescription(cols, colTypes)

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

	// Handle empty queries - PostgreSQL returns EmptyQueryResponse for these
	trimmedQuery := strings.TrimSpace(p.stmt.query)
	if trimmedQuery == "" || isEmptyQuery(trimmedQuery) {
		_ = writeEmptyQueryResponse(c.writer)
		return
	}

	start := time.Now()
	defer func() { queryDurationHistogram.Observe(time.Since(start).Seconds()) }()

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

	slog.Debug("Execute portal.", "user", c.username, "portal", portalName, "params", len(args), "query", p.stmt.query)

	// Check if this is a PostgreSQL-specific SET command that should be ignored
	// (determined by transpiler during Parse)
	if p.stmt.isIgnoredSet {
		slog.Debug("Ignoring PostgreSQL-specific SET.", "user", c.username, "query", p.stmt.query)
		_ = writeCommandComplete(c.writer, "SET")
		return
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.) - DuckLake doesn't support these
	// (determined by transpiler during Parse)
	if p.stmt.isNoOp {
		slog.Debug("No-op command (DuckLake limitation).", "user", c.username, "query", p.stmt.query)
		_ = writeCommandComplete(c.writer, p.stmt.noOpTag)
		return
	}

	// Handle multi-statement results (e.g., writable CTE rewrites)
	if len(p.stmt.statements) > 0 {
		slog.Debug("Execute multi-statement.", "user", c.username, "statements", len(p.stmt.statements), "cleanup", len(p.stmt.cleanupStatements))
		c.executeMultiStatementExtended(p.stmt.statements, p.stmt.cleanupStatements, args, p.resultFormats, p.described)
		return
	}

	if !returnsResults {
		// Handle nested BEGIN: PostgreSQL issues a warning but continues,
		// while DuckDB throws an error. Match PostgreSQL behavior.
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = writeCommandComplete(c.writer, "BEGIN")
			return
		}

		// Non-result-returning query: use Exec with converted query
		result, err := c.db.Exec(p.stmt.convertedQuery, args...)
		if err != nil {
			// Retry ALTER TABLE as ALTER VIEW if target is a view
			if isAlterTableNotTableError(err) {
				if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(p.stmt.convertedQuery); ok {
					result, err = c.db.Exec(alteredQuery, args...)
				}
			}
			if err != nil {
				slog.Error("Query execution failed.", "user", c.username, "query", p.stmt.convertedQuery, "original_query", p.stmt.query, "error", err)
				c.sendError("ERROR", "42000", err.Error())
				c.setTxError()
				return
			}
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, result)
		_ = writeCommandComplete(c.writer, tag)
		return
	}

	// Result-returning query: use Query with converted query
	rows, err := c.db.Query(p.stmt.convertedQuery, args...)
	if err != nil {
		slog.Error("Query execution failed.", "user", c.username, "query", p.stmt.convertedQuery, "original_query", p.stmt.query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		slog.Error("Columns error.", "user", c.username, "error", err)
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
	_ = writeCommandComplete(c.writer, tag)
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

	_ = writeCloseComplete(c.writer)
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
	_ = writeMessage(c.writer, 't', buf.Bytes())
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

// isAlterTableNotTableError checks if the error indicates that an ALTER TABLE
// was attempted on a view. DuckDB returns this error when trying to use
// ALTER TABLE ... RENAME TO on a view instead of ALTER VIEW.
func isAlterTableNotTableError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "cannot use alter table") &&
		strings.Contains(msg, "not a table")
}
