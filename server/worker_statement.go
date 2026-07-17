package server

import (
	"fmt"
	"time"

	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/usersecrets"
)

// workerStatementOrigin is a closed, low-cardinality taxonomy for physical
// statements sent to a worker on a client's behalf. Keep values typed at call
// sites: log consumers rely on this set rather than parsing SQL text.
type workerStatementOrigin string

const (
	workerOriginClient       workerStatementOrigin = "client"
	workerOriginTranspiled   workerStatementOrigin = "transpiled"
	workerOriginRewrite      workerStatementOrigin = "rewrite"
	workerOriginCopy         workerStatementOrigin = "copy"
	workerOriginCopyFallback workerStatementOrigin = "copy_fallback"
	workerOriginCursor       workerStatementOrigin = "cursor"
	workerOriginInternal     workerStatementOrigin = "internal"
)

// workerStatementOperation names the stable physical operation. Operations
// are intentionally low-cardinality too; generated SQL belongs in neither
// the operation nor the log payload.
type workerStatementOperation string

const (
	workerOperationExecute              workerStatementOperation = "execute"
	workerOperationDirectExec           workerStatementOperation = "direct_exec"
	workerOperationSelect               workerStatementOperation = "select"
	workerOperationSimpleBatchStatement workerStatementOperation = "simple_batch_statement"
	workerOperationRewriteSetup         workerStatementOperation = "rewrite_setup"
	workerOperationRewriteFinal         workerStatementOperation = "rewrite_final"
	workerOperationRewriteCleanup       workerStatementOperation = "rewrite_cleanup"
	workerOperationCursorOpen           workerStatementOperation = "cursor_open"
	workerOperationPersistentSecretDDL  workerStatementOperation = "persistent_secret_ddl"
	workerOperationCopyDirect           workerStatementOperation = "copy_direct"
	workerOperationCopyOutSelect        workerStatementOperation = "copy_out_select"
	workerOperationCopySchemaProbe      workerStatementOperation = "copy_schema_probe"
	workerOperationCopyFromStdinNative  workerStatementOperation = "copy_from_stdin_native"
	workerOperationCopyFromStdinStream  workerStatementOperation = "copy_from_stdin_stream"
	workerOperationCopyFallbackBatch    workerStatementOperation = "copy_fallback_batch"
)

// workerStatement describes one physical statement. query is set only for
// client-derived or transpiled SQL. Generated work intentionally carries
// compact metadata instead so it cannot be mistaken for client SQL or leak
// generated placeholders, identifiers, or arguments.
type workerStatement struct {
	origin    workerStatementOrigin
	operation workerStatementOperation
	query     string
	metadata  []any
}

func workerStatementWithQuery(origin workerStatementOrigin, operation workerStatementOperation, query string, metadata ...any) workerStatement {
	return workerStatement{
		origin:    origin,
		operation: operation,
		query:     query,
		metadata:  metadata,
	}
}

// workerStatementForQuery applies the SQL-text policy consistently for a
// physical statement derived from a client operation. A direct or transpiled
// statement may carry bounded/redacted SQL, but a Duckgres rewrite must be
// represented as generated work so its implementation SQL cannot be mistaken
// for client input.
func workerStatementForQuery(origin workerStatementOrigin, operation workerStatementOperation, query string, metadata ...any) workerStatement {
	if origin == workerOriginRewrite {
		return generatedWorkerStatement(origin, operation, metadata...)
	}
	return workerStatementWithQuery(origin, operation, query, metadata...)
}

func generatedWorkerStatement(origin workerStatementOrigin, operation workerStatementOperation, metadata ...any) workerStatement {
	return workerStatement{
		origin:    origin,
		operation: operation,
		metadata:  metadata,
	}
}

func generatedWorkerTelemetryErrorMessage(sqlState string) string {
	return fmt.Sprintf("generated worker statement failed (SQLSTATE %s)", sqlState)
}

func workerOriginForQueries(original, transpiled, executed string) workerStatementOrigin {
	if transpiled == "" {
		transpiled = original
	}
	if executed == "" {
		executed = transpiled
	}
	switch {
	case executed != transpiled:
		return workerOriginRewrite
	case transpiled != original:
		return workerOriginTranspiled
	default:
		return workerOriginClient
	}
}

func (c *clientConn) workerStatementAttrs(statement workerStatement) []any {
	origin := statement.origin
	if origin == "" {
		origin = workerOriginClient
	}
	attrs := make([]any, 0, 8+len(statement.metadata))
	attrs = append(attrs,
		"scope", "worker",
		"origin", string(origin),
		"operation", string(statement.operation),
		"trace_id", observe.TraceIDFromContext(c.ctx),
	)
	if statement.query != "" {
		attrs = append(attrs, "query", boundQueryLogText(usersecrets.RedactForLog(statement.query)))
	}
	attrs = append(attrs, statement.metadata...)
	return attrs
}

func (c *clientConn) logWorkerStatementStarted(statement workerStatement) {
	c.logger().Info("Worker statement started.", c.workerStatementAttrs(statement)...)
}

func (c *clientConn) logWorkerStatementFinished(statement workerStatement, start time.Time, rows int64, err error) {
	attrs := c.workerStatementAttrs(statement)
	attrs = append(attrs,
		"duration_ms", time.Since(start).Milliseconds(),
		"rows", rows,
	)
	if err != nil {
		attrs = append(attrs, "error_code", classifyErrorCode(err))
		if statement.query != "" {
			attrs = append(attrs, "error", boundQueryLogText(usersecrets.RedactErrorForLog(statement.query, err.Error())))
		} else {
			// Generated errors frequently echo generated SQL. Keep the event
			// useful via SQLSTATE without exposing generated text or arguments.
			attrs = append(attrs, "error", "generated worker statement failed")
		}
	}
	c.logger().Info("Worker statement finished.", attrs...)
}
