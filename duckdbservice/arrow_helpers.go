package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
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
	upper := strings.ToUpper(strings.TrimSpace(dbType))

	// LIST: "INTEGER[]", "VARCHAR[]", etc.
	if strings.HasSuffix(upper, "[]") {
		return arrow.ListOf(DuckDBTypeToArrow(dbType[:len(dbType)-2]))
	}

	// DECIMAL(p,s) / NUMERIC(p,s)
	if strings.HasPrefix(upper, "DECIMAL(") || strings.HasPrefix(upper, "NUMERIC(") {
		p, s := parseDecimalParams(dbType)
		return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(s)}
	}

	// STRUCT(...) and MAP(...)
	if strings.HasPrefix(upper, "STRUCT(") {
		return parseStructType(dbType)
	}
	if strings.HasPrefix(upper, "MAP(") {
		return parseMapType(dbType)
	}

	switch upper {
	// Signed integers
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER", "INT":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64

	// Unsigned integers
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16
	case "UINTEGER":
		return arrow.PrimitiveTypes.Uint32
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64

	// Big integers as Decimal128
	case "HUGEINT":
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}
	case "UHUGEINT":
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}

	// Floats
	case "FLOAT", "REAL":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64

	// Boolean
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean

	// Strings
	case "VARCHAR", "TEXT", "STRING":
		return arrow.BinaryTypes.String
	case "BLOB", "BYTEA":
		return arrow.BinaryTypes.Binary

	// Date
	case "DATE":
		return arrow.FixedWidthTypes.Date32

	// Time
	case "TIME":
		return arrow.FixedWidthTypes.Time64us
	case "TIMETZ":
		// The Go driver converts TIMETZ to UTC (see duckdb-go getTimeTZ),
		// discarding the original timezone offset before we see the value.
		// Time64us preserves the UTC time-of-day correctly; the offset is
		// irrecoverably lost at the driver level.
		return arrow.FixedWidthTypes.Time64us

	// Timestamps (no timezone → empty TimeZone string)
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case "TIMESTAMP_S":
		return &arrow.TimestampType{Unit: arrow.Second}
	case "TIMESTAMP_MS":
		return &arrow.TimestampType{Unit: arrow.Millisecond}
	case "TIMESTAMP_NS":
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMPTZ":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

	// Interval
	case "INTERVAL":
		return arrow.FixedWidthTypes.MonthDayNanoInterval

	// UUID as string (DuckDB's Arrow reader maps FixedSizeBinary(16) to BLOB, not UUID)
	case "UUID":
		return arrow.BinaryTypes.String

	// JSON/ENUM/BIT as string
	case "JSON", "BIT":
		return arrow.BinaryTypes.String

	// Bare DECIMAL without params
	case "DECIMAL", "NUMERIC":
		return &arrow.Decimal128Type{Precision: 18, Scale: 3}

	default:
		// VARCHAR(N), ENUM(...), etc.
		if strings.HasPrefix(upper, "VARCHAR(") || strings.HasPrefix(upper, "ENUM(") {
			return arrow.BinaryTypes.String
		}
		return arrow.BinaryTypes.String
	}
}

// splitTopLevelCommas splits s on commas at parenthesis depth 0,
// respecting double-quoted identifiers (with "" as escape).
func splitTopLevelCommas(s string) []string {
	var parts []string
	depth := 0
	inQuotes := false
	start := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"' && !inQuotes:
			inQuotes = true
		case ch == '"' && inQuotes:
			// Peek ahead for escaped quote ""
			if i+1 < len(s) && s[i+1] == '"' {
				i++ // skip the escaped quote
			} else {
				inQuotes = false
			}
		case ch == '(' && !inQuotes:
			depth++
		case ch == ')' && !inQuotes:
			depth--
		case ch == ',' && depth == 0 && !inQuotes:
			parts = append(parts, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

// extractInnerContent returns the content between the first '(' and last ')'.
func extractInnerContent(dbType string) string {
	lparen := strings.IndexByte(dbType, '(')
	rparen := strings.LastIndexByte(dbType, ')')
	if lparen < 0 || rparen <= lparen {
		return ""
	}
	return dbType[lparen+1 : rparen]
}

// parseStructFieldDef parses a single struct field definition like `"field_name" TYPE`.
// It handles double-quote escaping ("" → ").
func parseStructFieldDef(fieldDef string) (name, typStr string) {
	s := strings.TrimSpace(fieldDef)
	if len(s) > 0 && s[0] == '"' {
		// Quoted identifier: scan past opening ", collect until unescaped closing "
		var nameBuilder strings.Builder
		i := 1 // skip opening quote
		for i < len(s) {
			if s[i] == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					nameBuilder.WriteByte('"')
					i += 2
					continue
				}
				// Closing quote
				i++
				break
			}
			nameBuilder.WriteByte(s[i])
			i++
		}
		name = nameBuilder.String()
		typStr = strings.TrimSpace(s[i:])
		return
	}
	// Fallback: unquoted name, split on first space
	idx := strings.IndexByte(s, ' ')
	if idx < 0 {
		return s, ""
	}
	return s[:idx], strings.TrimSpace(s[idx+1:])
}

// parseStructType parses a DuckDB STRUCT type string into an Arrow StructType.
func parseStructType(dbType string) *arrow.StructType {
	inner := extractInnerContent(dbType)
	parts := splitTopLevelCommas(inner)
	fields := make([]arrow.Field, 0, len(parts))
	for _, part := range parts {
		name, typ := parseStructFieldDef(part)
		if name == "" {
			continue
		}
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     DuckDBTypeToArrow(typ),
			Nullable: true,
		})
	}
	return arrow.StructOf(fields...)
}

// parseMapType parses a DuckDB MAP type string into an Arrow MapType.
func parseMapType(dbType string) *arrow.MapType {
	inner := extractInnerContent(dbType)
	parts := splitTopLevelCommas(inner)
	if len(parts) != 2 {
		return arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	}
	keyType := DuckDBTypeToArrow(parts[0])
	valueType := DuckDBTypeToArrow(parts[1])
	return arrow.MapOf(keyType, valueType)
}

// parseDecimalParams extracts precision and scale from a type name like "DECIMAL(18,2)".
func parseDecimalParams(typeName string) (precision, scale int) {
	lparen := strings.IndexByte(typeName, '(')
	rparen := strings.LastIndexByte(typeName, ')')
	if lparen < 0 || rparen <= lparen {
		return 18, 3
	}
	parts := strings.SplitN(typeName[lparen+1:rparen], ",", 2)
	if len(parts) == 2 {
		n1, _ := fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &precision)
		n2, _ := fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &scale)
		if n1 == 1 && n2 == 1 {
			return
		}
	}
	return 18, 3
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
	case *array.Uint8Builder:
		switch v := val.(type) {
		case uint8:
			b.Append(v)
		case uint16:
			b.Append(uint8(v))
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch v := val.(type) {
		case uint16:
			b.Append(v)
		case uint32:
			b.Append(uint16(v))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch v := val.(type) {
		case uint32:
			b.Append(v)
		case uint64:
			b.Append(uint32(v))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch v := val.(type) {
		case uint64:
			b.Append(v)
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
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			// Floor division to handle pre-epoch dates correctly.
			// Go's integer division truncates toward zero, but Date32
			// needs days since epoch rounded toward negative infinity.
			unix := v.Unix()
			days := unix / 86400
			if unix%86400 < 0 {
				days--
			}
			b.Append(arrow.Date32(days))
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.AppendTime(v)
		default:
			b.AppendNull()
		}
	case *array.Time64Builder:
		switch v := val.(type) {
		case time.Time:
			micros := int64(v.Hour())*3600000000 + int64(v.Minute())*60000000 +
				int64(v.Second())*1000000 + int64(v.Nanosecond())/1000
			b.Append(arrow.Time64(micros))
		default:
			b.AppendNull()
		}
	case *array.MonthDayNanoIntervalBuilder:
		switch v := val.(type) {
		case duckdb.Interval:
			b.Append(arrow.MonthDayNanoInterval{
				Months:      v.Months,
				Days:        v.Days,
				Nanoseconds: v.Micros * 1000,
			})
		default:
			b.AppendNull()
		}
	case *array.Decimal128Builder:
		switch v := val.(type) {
		case duckdb.Decimal:
			b.Append(decimal128.FromBigInt(v.Value))
		case *big.Int:
			b.Append(decimal128.FromBigInt(v))
		default:
			b.AppendNull()
		}
	case *array.FixedSizeBinaryBuilder:
		switch v := val.(type) {
		case duckdb.UUID:
			b.Append(v[:])
		case []byte:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.ListBuilder:
		switch v := val.(type) {
		case []any:
			b.Append(true)
			vb := b.ValueBuilder()
			for _, elem := range v {
				AppendValue(vb, elem)
			}
		default:
			b.AppendNull()
		}
	case *array.StructBuilder:
		switch v := val.(type) {
		case map[string]any:
			b.Append(true)
			st := b.Type().(*arrow.StructType)
			for i := 0; i < st.NumFields(); i++ {
				fieldVal, ok := v[st.Field(i).Name]
				if !ok {
					b.FieldBuilder(i).AppendNull()
				} else {
					AppendValue(b.FieldBuilder(i), fieldVal)
				}
			}
		default:
			b.AppendNull()
		}
	case *array.MapBuilder:
		switch v := val.(type) {
		case duckdb.Map:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for k, item := range v {
				AppendValue(kb, k)
				AppendValue(ib, item)
			}
		case map[any]any:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for k, item := range v {
				AppendValue(kb, k)
				AppendValue(ib, item)
			}
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case duckdb.UUID:
			b.Append(v.String())
		case []byte:
			// TODO: This heuristic (16 bytes + invalid UTF-8 → UUID) is coupled to
			// DuckDBTypeToArrow("UUID") returning String. If UUID mapping changes,
			// update this branch accordingly. The Go driver returns []byte (not
			// duckdb.UUID) when scanning UUID columns into interface{}.
			if len(v) == 16 && !utf8.Valid(v) {
				s := hex.EncodeToString(v)
				b.Append(s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32])
			} else {
				b.Append(string(v))
			}
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
	if !strings.Contains(upper, "LIMIT") && supportsLimit(upper) {
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

// supportsLimit returns true if the (uppercased) query is a statement type
// that accepts a LIMIT clause. Statements like SHOW, DESCRIBE, EXPLAIN,
// PRAGMA, and CALL do not support LIMIT.
func supportsLimit(upper string) bool {
	// Strip leading whitespace (already trimmed, but defensive)
	s := strings.TrimSpace(upper)
	return strings.HasPrefix(s, "SELECT") ||
		strings.HasPrefix(s, "WITH") ||
		strings.HasPrefix(s, "VALUES") ||
		strings.HasPrefix(s, "TABLE") ||
		strings.HasPrefix(s, "FROM") // DuckDB FROM-first syntax
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
