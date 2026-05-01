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

	numFields := schema.NumFields()
	count := 0
	for rows.Next() && count < batchSize {
		values := make([]interface{}, numFields)
		valuePtrs := make([]interface{}, numFields)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, val := range values {
			AppendValue(builder.Field(i), val)
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

type contextQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func isNil(i contextQueryer) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// GetQuerySchema executes a query with LIMIT 0 to discover the result schema.
func GetQuerySchema(ctx context.Context, db contextQueryer, query string, tx contextQueryer) (*arrow.Schema, error) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	queryWithLimit := q
	upper := strings.ToUpper(q)
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
