package duckdbservice

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

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

// DuckDBTypeToArrow maps a DuckDB type name to an Arrow DataType.
func DuckDBTypeToArrow(dbType string) arrow.DataType {
	dbType = strings.ToUpper(dbType)
	switch {
	case strings.Contains(dbType, "INT64"), strings.Contains(dbType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(dbType, "INT32"), strings.Contains(dbType, "INTEGER"):
		return arrow.PrimitiveTypes.Int32
	case strings.Contains(dbType, "INT16"), strings.Contains(dbType, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16
	case strings.Contains(dbType, "INT8"), strings.Contains(dbType, "TINYINT"):
		return arrow.PrimitiveTypes.Int8
	case strings.Contains(dbType, "DOUBLE"), strings.Contains(dbType, "FLOAT8"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "REAL"):
		return arrow.PrimitiveTypes.Float32
	case strings.Contains(dbType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(dbType, "VARCHAR"), strings.Contains(dbType, "TEXT"), strings.Contains(dbType, "STRING"):
		return arrow.BinaryTypes.String
	case strings.Contains(dbType, "BLOB"):
		return arrow.BinaryTypes.Binary
	case strings.Contains(dbType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	case strings.Contains(dbType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.BinaryTypes.String
	}
}

// AppendValue appends a value to an Arrow array builder with type coercion.
func AppendValue(builder array.Builder, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch v := val.(type) {
		case int32:
			b.Append(v)
		case int64:
			b.Append(int32(v))
		case int:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int32:
			b.Append(int16(v))
		default:
			b.AppendNull()
		}
	case *array.Int8Builder:
		switch v := val.(type) {
		case int8:
			b.Append(v)
		case int32:
			b.Append(int8(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := val.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// QualifyTableName builds a qualified table name from nullable catalog/schema and table name.
func QualifyTableName(catalog, schema sql.NullString, table string) string {
	parts := make([]string, 0, 3)
	if catalog.Valid {
		parts = append(parts, QuoteIdent(catalog.String))
	}
	if schema.Valid {
		parts = append(parts, QuoteIdent(schema.String))
	}
	parts = append(parts, QuoteIdent(table))
	return strings.Join(parts, ".")
}

// QuoteIdent quotes a SQL identifier to prevent injection.
func QuoteIdent(ident string) string {
	escaped := strings.ReplaceAll(ident, `"`, `""`)
	return `"` + escaped + `"`
}
