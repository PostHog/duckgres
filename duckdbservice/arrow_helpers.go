package duckdbservice

import (
	"context"
	"database/sql"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/duckdbservice/arrowmap"
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

	scanFields, explainValueColumn, err := rowScanShape(rows, schema)
	if err != nil {
		return nil, err
	}
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
