package server

import (
	"context"
	"errors"
	"strings"
)

// isQueryCancelled checks whether an error string indicates that *some*
// context cancellation was involved. It does NOT distinguish caller-driven
// cancellation (Ctrl-C, client disconnect, deadline) from infra-driven
// cancellation (worker died, CP closed the gRPC client) — both surface here
// as a wrapped "context canceled" string. Use it for SQLSTATE classification
// where 57014 is correct either way; do not use it to gate error logging,
// since infra cancels are real failures we want surfaced. Use
// (*clientConn).isCallerCancellation for that.
func isQueryCancelled(err error) bool {
	return err == context.Canceled || (err != nil && strings.Contains(err.Error(), "context canceled"))
}

// isCallerCancellation reports whether err is a cancellation that the caller
// asked for — either through pgwire CancelRequest, an explicit ctx cancel, or
// a deadline. Distinct from a gRPC Canceled status that bubbles up purely
// because the CP closed the worker connection (worker death, takeover): in
// that case c.ctx is still healthy, c.ctx.Err() is nil, and the surface error
// must be logged as an infra failure rather than suppressed as "user
// cancelled". This matters for alerting — "Query execution errored." should
// fire on worker kills, not get silently downgraded to "Client query finished.".
func (c *clientConn) isCallerCancellation(err error) bool {
	if !isQueryCancelled(err) {
		return false
	}
	if c == nil || c.ctx == nil {
		return false
	}
	return c.ctx.Err() != nil
}

// isDuckLakeTransactionConflict returns true if the error is a DuckLake
// transaction conflict. These occur when concurrent DuckLake transactions
// try to commit overlapping changes. DuckLake uses global snapshot IDs, so
// even writes to unrelated tables can conflict under concurrency.
func isDuckLakeTransactionConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Transaction conflict")
}

// isDuckLakeMetadataConnectionLost returns true if the error indicates the
// DuckLake metadata store connection was lost during a transaction. This
// typically happens when long-running queries leave the metadata connection
// idle long enough for RDS or a network layer to drop it.
func isDuckLakeMetadataConnectionLost(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "DuckLake transaction") &&
		strings.Contains(msg, "SSL connection has been closed unexpectedly")
}

// classifyErrorCode returns the most appropriate PostgreSQL SQLSTATE for a
// DuckDB error. Transaction conflicts get 40001 (serialization_failure), which
// signals PG-aware clients to retry. Query cancellations get 57014. Remaining
// errors are classified by DuckDB's exception-type prefix; the prefix is the
// only signal the Go driver exposes today, so we parse the message string.
func classifyErrorCode(err error) string {
	// Wire-side SQLSTATE: 57014 is correct for any cancellation regardless of
	// whether the caller drove it. Use isQueryCancelled here, not the
	// clientConn-aware variant — classifyErrorCode is called from contexts
	// that don't have a *clientConn (e.g., direct test fixtures).
	if isQueryCancelled(err) {
		return "57014"
	}
	if isDuckLakeTransactionConflict(err) {
		return "40001" // serialization_failure — client should retry
	}

	// Workers reach the control plane over Arrow Flight SQL, so DuckDB errors
	// arrive wrapped as "flight execute update: rpc error: code = X desc = <msg>".
	// Unwrap to recover the underlying DuckDB exception message so the prefix
	// classifiers below apply; otherwise every DuckLake worker error
	// would fall through to XX000 and the client would see the raw rpc string.
	msg := unwrapFlightError(err.Error())
	switch {
	case strings.HasPrefix(msg, "Catalog Error:"):
		return catalogErrorCode(msg)
	case strings.HasPrefix(msg, "Binder Error:"):
		return binderErrorCode(msg)
	case strings.HasPrefix(msg, "Parser Error:"):
		return "42601" // syntax_error
	case strings.HasPrefix(msg, "Conversion Error:"):
		return conversionErrorCode(msg)
	case strings.HasPrefix(msg, "Out of Range Error:"):
		return "22003" // numeric_value_out_of_range
	case strings.HasPrefix(msg, "Constraint Error:"):
		return constraintErrorCode(msg)
	case strings.HasPrefix(msg, "Permission Error:"):
		return "42501" // insufficient_privilege
	case strings.HasPrefix(msg, "Not implemented Error:"),
		strings.HasPrefix(msg, "Invalid Input Error: Not implemented"):
		return "0A000" // feature_not_supported
	case strings.HasPrefix(msg, "Transaction Error:"),
		strings.HasPrefix(msg, "TransactionContext Error:"):
		return "25000" // invalid_transaction_state — DuckDB emits both prefixes
	case strings.HasPrefix(msg, "Dependency Error:"):
		return "2BP01" // dependent_objects_still_exist
	}
	// Unknown error class — no DuckDB prefix matched. These are
	// typically infra issues (gRPC failures, IO errors, internal panics)
	// rather than user input issues. Classify as XX000 (internal_error)
	// so isUserQueryError correctly routes them to the system-error log
	// path. If a future DuckDB error needs to land in a user class, add
	// a prefix branch above instead of moving the fallback.
	return "XX000"
}

// transformErrorSQLState returns the SQLSTATE for a transpiler-detected error.
// A transform may attach an explicit code (e.g. 0A000 for an unsupported
// feature via transform.CodedError); otherwise it defaults to 42704
// (undefined_object), matching the historical behavior for unrecognized config
// parameters.
func transformErrorSQLState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return "42704"
}

// unwrapFlightError recovers the underlying error message from an Arrow Flight /
// gRPC wrapper. gRPC errors stringify as "… rpc error: code = X desc = <message>",
// and the control plane further prefixes worker failures with "flight execute
// update: " (and similar). When the message is not Flight-wrapped it is returned
// unchanged, so this is safe to apply unconditionally before prefix matching.
func unwrapFlightError(msg string) string {
	// gRPC puts the real message after the final "desc = ".
	if idx := strings.LastIndex(msg, "desc = "); idx != -1 {
		msg = msg[idx+len("desc = "):]
	}
	msg = strings.TrimSpace(msg)
	// The worker's Flight SQL ingress wraps the raw DuckDB error one more time
	// with "failed to execute query: " / "failed to execute update: " (see
	// server/flightsqlingress/ingress.go). That sits between "desc = " and the
	// DuckDB exception prefix, so without stripping it the HasPrefix("Catalog
	// Error:" …) classifiers below all miss and the error falls through to
	// XX000. Strip any one such worker prefix so classification sees the bare
	// DuckDB message.
	for _, p := range []string{"failed to execute update: ", "failed to execute query: "} {
		if strings.HasPrefix(msg, p) {
			msg = strings.TrimSpace(msg[len(p):])
			break
		}
	}
	return msg
}

// catalogErrorCode narrows a "Catalog Error: …" message to a specific SQLSTATE
func catalogErrorCode(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "schema") && strings.Contains(lower, "does not exist"):
		return "3F000" // invalid_schema_name
	case strings.Contains(lower, "table with name") && strings.Contains(lower, "does not exist"):
		return "42P01" // undefined_table
	case strings.Contains(lower, "view with name") && strings.Contains(lower, "does not exist"):
		return "42P01" // undefined_table (views share the code)
	case strings.Contains(lower, "function") && (strings.Contains(lower, "does not exist") || strings.Contains(lower, "with these arguments")):
		return "42883" // undefined_function
	case strings.Contains(lower, "no function matches"):
		return "42883" // undefined_function — DuckDB's overload-resolution failure
	case strings.Contains(lower, "type") && strings.Contains(lower, "does not exist"):
		return "42704" // undefined_object
	case strings.Contains(lower, "does not exist"):
		return "42704" // undefined_object (generic fallback)
	case strings.Contains(lower, "already exists"):
		if strings.Contains(lower, "function") {
			return "42723" // duplicate_function
		}
		if strings.Contains(lower, "schema") {
			return "42P06" // duplicate_schema
		}
		return "42P07" // duplicate_table
	}
	return "42000"
}

// binderErrorCode narrows a "Binder Error: …" message. The binder raises on
// semantic problems discovered after parsing, most commonly missing columns.
func binderErrorCode(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "referenced column") && strings.Contains(lower, "not found"):
		return "42703" // undefined_column
	case strings.Contains(lower, "column") && strings.Contains(lower, "does not exist"):
		return "42703"
	case strings.Contains(lower, "ambiguous"):
		return "42702" // ambiguous_column
	case strings.Contains(lower, "referenced table") && strings.Contains(lower, "not found"):
		return "42P01" // undefined_table — DuckDB raises this for unknown aliases
	case strings.Contains(lower, "no function matches"):
		return "42883" // undefined_function — overload-resolution failure
	}
	return "42601" // syntax_error — binder failures without a narrower match
}

// conversionErrorCode narrows a "Conversion Error: …" message. DuckDB uses
// this prefix for both invalid text representations and numeric overflows
// during casts (e.g. CAST(1000 AS TINYINT));
func conversionErrorCode(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "out of range"),
		strings.Contains(lower, "overflow"),
		strings.Contains(lower, "would be out of range"):
		return "22003" // numeric_value_out_of_range
	}
	return "22P02" // invalid_text_representation
}

// constraintErrorCode narrows a "Constraint Error: …" message to one of the
// integrity_constraint_violation family codes. DuckDB's messages name the
// violated constraint explicitly, so substring matching is reliable.
func constraintErrorCode(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "duplicate key") || strings.Contains(lower, "unique"):
		return "23505" // unique_violation
	case strings.Contains(lower, "not null") || strings.Contains(lower, "null value"):
		return "23502" // not_null_violation
	case strings.Contains(lower, "foreign key"):
		return "23503" // foreign_key_violation
	case strings.Contains(lower, "check constraint"):
		return "23514" // check_violation
	}
	return "23000" // integrity_constraint_violation
}

// userErrorSQLSTATEClasses is the closed set of PostgreSQL SQLSTATE class
// codes (the first two characters) that represent user-input or
// user-state errors — "you wrote a query that doesn't make sense for
// this database state." Anything outside this set is treated as a
// system / infra error (08 connection, 53 resources, 57 operator
// intervention, 58 system, XX internal, …).
//
// The discriminator is the SQLSTATE we already compute for the pgwire
// error response — no new string matching here. Add a class only after
// confirming every code in it is genuinely user-attributable; adding
// erroneously will hide real infra failures from the alert path.
var userErrorSQLSTATEClasses = map[string]struct{}{
	"0A": {}, // feature_not_supported — user used a SQL feature we don't have
	"22": {}, // data_exception — bad input (cast, overflow, encoding)
	"23": {}, // integrity_constraint_violation — unique/fk/check/not_null
	"25": {}, // invalid_transaction_state — nested BEGIN, etc.
	"28": {}, // invalid_authorization_specification — not hit on this path today
	"2B": {}, // dependent_objects_still_exist — DROP without CASCADE
	"3D": {}, // invalid_catalog_name — DB doesn't exist
	"3F": {}, // invalid_schema_name — schema doesn't exist
	"42": {}, // syntax_error_or_access_rule_violation — table/column not found, syntax
	"44": {}, // with_check_option_violation
}

// isUserQueryError tells the log/observability path whether a query
// failure is user-attributable (Info-level "Query execution failed.")
// or a real system error worth alerting on (Error-level "Query
// execution errored."). The discriminator is the SQLSTATE class —
// already computed via classifyErrorCode for the pgwire response.
//
// Cancellations (SQLSTATE 57014) are NOT short-circuited as user errors
// here. The call sites (logQueryError, the clientConn error paths) gate
// caller-driven cancellations upstream via clientConn.isCallerCancellation,
// so by the time isUserQueryError sees a cancel-shaped err the cancellation
// was infra-driven (gRPC client closed because the worker died, the worker
// was retired, etc.) and class 57 = operator intervention is the right
// bucket — that's a real failure we want surfaced at Error level.
func isUserQueryError(err error) bool {
	if err == nil {
		return false
	}
	code := classifyErrorCode(err)
	if len(code) < 2 {
		return false
	}
	_, ok := userErrorSQLSTATEClasses[code[:2]]
	return ok
}

// isConnectionBroken checks if an error indicates a broken connection
// (e.g., SSL connection closed, network error).
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

// isAlterTableNotTableError checks if the error indicates that an ALTER TABLE
// was attempted on a view. DuckDB returns this error when trying to use
// ALTER TABLE ... RENAME TO on a view instead of ALTER VIEW.
func isAlterTableNotTableError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "cannot use alter table") &&
		strings.Contains(msg, "not a table") {
		return true
	}

	if strings.Contains(msg, "can only modify view with alter view statement") {
		return true
	}

	return false
}

// isDropTableOnViewError checks if the error indicates that a DROP TABLE
// was attempted on a view. DuckDB returns:
// "Catalog Error: Existing object X is of type View, trying to drop type Table"
func isDropTableOnViewError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()
	return strings.Contains(msg, "is of type View") &&
		strings.Contains(msg, "trying to drop type Table")
}
