package duckdbservice

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/duckdbservice/arrowmap"
	"github.com/posthog/duckgres/server"
)

// DuckDBTypeToArrow re-exports arrowmap.DuckDBTypeToArrow for backward
// compatibility with existing callers in this package.
var DuckDBTypeToArrow = arrowmap.DuckDBTypeToArrow

// QualifyTableName re-exports arrowmap.QualifyTableName for backward
// compatibility with existing callers.
var QualifyTableName = arrowmap.QualifyTableName

// QuoteIdent re-exports arrowmap.QuoteIdent for backward compatibility.
var QuoteIdent = arrowmap.QuoteIdent

// AppendValue re-exports arrowmap.AppendValue for backward compatibility.
// The duckdb-go-specific value types (duckdb.Interval, Decimal, UUID,
// OrderedMap, Map) are handled via an arrowmap.Appender registered from
// duckdbservice/appender_init.go, so callers that import duckdbservice get
// full type coverage automatically.
var AppendValue = arrowmap.AppendValue

// RowsToRecord converts sql.Rows into an Arrow RecordBatch of up to batchSize rows.
// Returns nil when there are no more rows.
func RowsToRecord(alloc memory.Allocator, rows *sql.Rows, schema *arrow.Schema, batchSize int) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	scanFields := schema.NumFields()
	explainValueColumn := -1
	scanShapeResolved := false
	count := 0
	// Order matters: check `count < batchSize` first, then call rows.Next().
	// The reverse (rows.Next() && count < batchSize) advances the cursor once
	// more after the final scan and that row is silently dropped — the next
	// call to RowsToRecord starts from the row *after* the one we skipped.
	// Production reads were losing one row at every batch boundary
	// (batchSize=1024) for unbounded SELECTs; COUNT(*) still returned the
	// parquet-metadata row count, so the discrepancy was invisible to
	// aggregation queries. See TestRowsToRecordNoRowsLostAtBatchBoundary.
	for count < batchSize && rows.Next() {
		// Resolve the physical scan shape only after Next succeeds. Some drivers
		// close rows automatically at EOF; calling Columns after that would turn
		// normal completion into "sql: Rows are closed".
		if !scanShapeResolved {
			var err error
			scanFields, explainValueColumn, err = rowScanShape(rows, schema)
			if err != nil {
				return nil, err
			}
			scanShapeResolved = true
		}
		values := make([]interface{}, scanFields)
		valuePtrs := make([]interface{}, scanFields)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		if explainValueColumn >= 0 {
			AppendValue(builder.Field(0), values[explainValueColumn])
		} else {
			for i, val := range values {
				AppendValue(builder.Field(i), val)
			}
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return builder.NewRecordBatch(), nil
}

func rowScanShape(rows *sql.Rows, schema *arrow.Schema) (scanFields int, explainValueColumn int, err error) {
	scanFields = schema.NumFields()
	explainValueColumn = -1
	if !isExplainPlanSchema(schema) {
		return scanFields, explainValueColumn, nil
	}
	cols, err := rows.Columns()
	if err != nil {
		return 0, -1, err
	}
	if len(cols) == 2 &&
		strings.EqualFold(cols[0], "explain_key") &&
		strings.EqualFold(cols[1], "explain_value") {
		return 2, 1, nil
	}
	return scanFields, explainValueColumn, nil
}

func isExplainPlanSchema(schema *arrow.Schema) bool {
	if schema == nil || schema.NumFields() != 1 {
		return false
	}
	name := schema.Field(0).Name
	return name == "physical_plan" || name == "analyzed_plan"
}

type contextQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// isExplainQuery reports whether the (already upper-cased) query is an EXPLAIN
// statement, i.e. starts with the EXPLAIN keyword followed by a delimiter.
func isExplainQuery(upper string) bool {
	s := strings.TrimSpace(upper)
	const kw = "EXPLAIN"
	if !strings.HasPrefix(s, kw) {
		return false
	}
	if len(s) == len(kw) {
		return true
	}
	switch s[len(kw)] {
	case ' ', '\t', '\n', '\r', '(':
		return true
	}
	return false
}

func isNil(i contextQueryer) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// GetQuerySchema executes a query with LIMIT 0 to discover the result schema.
func GetQuerySchema(ctx context.Context, db contextQueryer, query string, tx contextQueryer) (*arrow.Schema, error) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	if tx == nil && isDMLReturningSchemaQuery(q) {
		if conn, ok := db.(*sql.Conn); ok {
			return preparedDMLReturningSchema(ctx, conn, q)
		}
	}
	queryWithLimit := q
	upper := strings.ToUpper(q)
	// EXPLAIN [ANALYZE] returns a fixed single-column textual plan. We must NOT
	// execute it to discover its schema: EXPLAIN ANALYZE runs the statement to
	// gather statistics, so executing it here as a schema probe would run (and,
	// for a write, mutate) the statement a second time on top of the real DoGet
	// execution. Return a synthetic schema without executing.
	if isExplainQuery(upper) {
		name := "physical_plan"
		if strings.Contains(upper, "ANALYZE") {
			name = "analyzed_plan"
		}
		return arrow.NewSchema([]arrow.Field{
			{Name: name, Type: arrowmap.DuckDBTypeToArrow("VARCHAR"), Nullable: true},
		}, nil), nil
	}
	// Only append LIMIT 0 for SELECT/WITH/VALUES/TABLE statements.
	// SHOW, DESCRIBE, EXPLAIN, PRAGMA, CALL etc. don't support LIMIT.
	if !strings.Contains(upper, "LIMIT") && arrowmap.SupportsLimit(upper) {
		queryWithLimit = q + " LIMIT 0"
	}
	var rows *sql.Rows
	var err error
	if !isNil(tx) {
		rows, err = tx.QueryContext(ctx, queryWithLimit)
	} else {
		rows, err = db.QueryContext(ctx, queryWithLimit)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		fields[i] = arrow.Field{Name: ct.Name(), Type: DuckDBTypeToArrow(ct.DatabaseTypeName()), Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

// isDMLReturningSchemaQuery identifies the worker query shapes that need
// prepared-statement metadata. They are executed later by DoGet, so running
// them here merely to learn their schema would apply the mutation twice.
func isDMLReturningSchemaQuery(query string) bool {
	return server.IsDMLReturning(query)
}

// preparedDMLReturningSchema uses duckdb-go's native prepared-statement
// metadata on the session's already-pinned connection. Preparing binds and
// plans the DML RETURNING statement but does not run it, unlike QueryContext.
func preparedDMLReturningSchema(ctx context.Context, conn *sql.Conn, query string) (schema *arrow.Schema, err error) {
	if conn == nil {
		return nil, fmt.Errorf("prepare DML RETURNING schema: nil connection")
	}
	err = conn.Raw(func(driverConn any) (callbackErr error) {
		duckConn, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("prepare DML RETURNING schema: unexpected driver connection %T", driverConn)
		}
		prepared, prepareErr := duckConn.PrepareContext(ctx, query)
		if prepareErr != nil {
			return fmt.Errorf("prepare DML RETURNING schema: %w", prepareErr)
		}
		stmt, ok := prepared.(*duckdb.Stmt)
		if !ok {
			_ = prepared.Close()
			return fmt.Errorf("prepare DML RETURNING schema: unexpected prepared statement %T", prepared)
		}
		defer func() {
			if closeErr := stmt.Close(); closeErr != nil && callbackErr == nil {
				callbackErr = fmt.Errorf("close prepared DML RETURNING schema: %w", closeErr)
			}
		}()

		count, countErr := stmt.ColumnCount()
		if countErr != nil {
			return fmt.Errorf("prepared DML RETURNING column count: %w", countErr)
		}
		fields := make([]arrow.Field, count)
		for i := range fields {
			name, nameErr := stmt.ColumnName(i)
			if nameErr != nil {
				return fmt.Errorf("prepared DML RETURNING column %d name: %w", i, nameErr)
			}
			typeInfo, typeErr := stmt.ColumnTypeInfo(i)
			if typeErr != nil {
				return fmt.Errorf("prepared DML RETURNING column %d type: %w", i, typeErr)
			}
			fields[i] = arrow.Field{Name: name, Type: arrowTypeFromPreparedDuckDB(typeInfo), Nullable: true}
		}
		schema = arrow.NewSchema(fields, nil)
		return nil
	})
	return schema, err
}

func arrowTypeFromPreparedDuckDB(info duckdb.TypeInfo) arrow.DataType {
	if info == nil {
		return arrow.BinaryTypes.String
	}
	switch info.InternalType() {
	case duckdb.TYPE_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean
	case duckdb.TYPE_TINYINT:
		return arrow.PrimitiveTypes.Int8
	case duckdb.TYPE_SMALLINT:
		return arrow.PrimitiveTypes.Int16
	case duckdb.TYPE_INTEGER:
		return arrow.PrimitiveTypes.Int32
	case duckdb.TYPE_BIGINT:
		return arrow.PrimitiveTypes.Int64
	case duckdb.TYPE_UTINYINT:
		return arrow.PrimitiveTypes.Uint8
	case duckdb.TYPE_USMALLINT:
		return arrow.PrimitiveTypes.Uint16
	case duckdb.TYPE_UINTEGER:
		return arrow.PrimitiveTypes.Uint32
	case duckdb.TYPE_UBIGINT:
		return arrow.PrimitiveTypes.Uint64
	case duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT:
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}
	case duckdb.TYPE_FLOAT:
		return arrow.PrimitiveTypes.Float32
	case duckdb.TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM, duckdb.TYPE_BIT:
		return arrow.BinaryTypes.String
	case duckdb.TYPE_BLOB:
		return arrow.BinaryTypes.Binary
	case duckdb.TYPE_DATE:
		return arrow.FixedWidthTypes.Date32
	case duckdb.TYPE_TIME, duckdb.TYPE_TIME_TZ:
		return arrow.FixedWidthTypes.Time64us
	case duckdb.TYPE_TIMESTAMP:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case duckdb.TYPE_TIMESTAMP_S:
		return &arrow.TimestampType{Unit: arrow.Second}
	case duckdb.TYPE_TIMESTAMP_MS:
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case duckdb.TYPE_TIMESTAMP_NS:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case duckdb.TYPE_TIMESTAMP_TZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case duckdb.TYPE_INTERVAL:
		return arrow.FixedWidthTypes.MonthDayNanoInterval
	case duckdb.TYPE_UUID:
		return arrow.BinaryTypes.String
	case duckdb.TYPE_DECIMAL:
		if details, ok := info.Details().(*duckdb.DecimalDetails); ok {
			return &arrow.Decimal128Type{Precision: int32(details.Width), Scale: int32(details.Scale)}
		}
		return &arrow.Decimal128Type{Precision: 18, Scale: 3}
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		if details, ok := info.Details().(*duckdb.ListDetails); ok {
			return arrow.ListOf(arrowTypeFromPreparedDuckDB(details.Child))
		}
		if details, ok := info.Details().(*duckdb.ArrayDetails); ok {
			return arrow.ListOf(arrowTypeFromPreparedDuckDB(details.Child))
		}
	case duckdb.TYPE_MAP:
		if details, ok := info.Details().(*duckdb.MapDetails); ok {
			return arrow.MapOf(arrowTypeFromPreparedDuckDB(details.Key), arrowTypeFromPreparedDuckDB(details.Value))
		}
	case duckdb.TYPE_STRUCT:
		if details, ok := info.Details().(*duckdb.StructDetails); ok {
			fields := make([]arrow.Field, 0, len(details.Entries))
			for _, entry := range details.Entries {
				fields = append(fields, arrow.Field{Name: entry.Name(), Type: arrowTypeFromPreparedDuckDB(entry.Info()), Nullable: true})
			}
			return arrow.StructOf(fields...)
		}
	}
	return arrow.BinaryTypes.String
}
