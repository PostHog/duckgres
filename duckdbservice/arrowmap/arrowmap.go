// Package arrowmap provides DuckDB-free helpers for translating DuckDB type
// strings into Arrow types and for quoting/qualifying SQL identifiers.
//
// These helpers are kept in their own package (with no dependency on
// github.com/duckdb/duckdb-go) so that the control plane can use them
// without linking libduckdb.
package arrowmap

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

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

// SupportsLimit returns true if the (uppercased) query is a statement type
// that accepts a LIMIT clause. Statements like SHOW, DESCRIBE, EXPLAIN,
// PRAGMA, and CALL do not support LIMIT.
func SupportsLimit(upper string) bool {
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
