// Package icebergmeta loads real Iceberg column metadata through DuckDB's
// public table binding path (`DESCRIBE SELECT * FROM iceberg.schema.table`).
package icebergmeta

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/posthog/duckgres/server/sqlcore"
)

const ColumnMetadataTable = "__duckgres_iceberg_column_metadata"
const QualifiedColumnMetadataTable = "memory.main." + ColumnMetadataTable

type Filters struct {
	Schemas []string
	Tables  []string
}

type tableRef struct {
	Schema string
	Name   string
}

type describeColumn struct {
	Name       string
	ColumnType string
	Nullable   string
}

func ShouldLoadColumns(query string) bool {
	return strings.Contains(strings.ToLower(query), "information_schema_columns_compat")
}

func LoadColumns(ctx context.Context, executor sqlcore.QueryExecutor, query string) error {
	if executor == nil || !ShouldLoadColumns(query) {
		return nil
	}

	tables, err := listCandidateTables(ctx, executor, ExtractFilters(query))
	if err != nil {
		return err
	}
	for _, table := range tables {
		cols, err := describeTable(ctx, executor, table)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		if _, err := executor.ExecContext(ctx, buildInsertSQL(table, cols)); err != nil {
			return fmt.Errorf("insert iceberg column metadata for %s.%s: %w", table.Schema, table.Name, err)
		}
	}
	return nil
}

func ExtractFilters(query string) Filters {
	return Filters{
		Schemas: extractValues(query, "table_schema"),
		Tables:  extractValues(query, "table_name"),
	}
}

func listCandidateTables(ctx context.Context, executor sqlcore.QueryExecutor, filters Filters) ([]tableRef, error) {
	clauses := []string{
		"table_catalog = 'iceberg'",
		"table_type = 'BASE TABLE'",
		fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s m WHERE m.table_schema = information_schema.tables.table_schema AND m.table_name = information_schema.tables.table_name)", QualifiedColumnMetadataTable),
	}
	if len(filters.Schemas) > 0 {
		clauses = append(clauses, "table_schema IN ("+quoteSQLStringList(filters.Schemas)+")")
	}
	if len(filters.Tables) > 0 {
		clauses = append(clauses, "table_name IN ("+quoteSQLStringList(filters.Tables)+")")
	}
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE ` + strings.Join(clauses, "\n\t\tAND ") + `
		ORDER BY table_schema, table_name
	`

	rows, err := executor.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list iceberg tables for metadata load: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var refs []tableRef
	for rows.Next() {
		var rawSchema, rawName any
		if err := rows.Scan(&rawSchema, &rawName); err != nil {
			return nil, fmt.Errorf("scan iceberg table: %w", err)
		}
		ref := tableRef{Schema: asString(rawSchema), Name: asString(rawName)}
		if ref.Schema == "" || ref.Name == "" {
			continue
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate iceberg tables: %w", err)
	}
	return refs, nil
}

func describeTable(ctx context.Context, executor sqlcore.QueryExecutor, table tableRef) ([]describeColumn, error) {
	query := fmt.Sprintf(
		"DESCRIBE SELECT * FROM iceberg.%s.%s LIMIT 0",
		quoteIdentifier(table.Schema),
		quoteIdentifier(table.Name),
	)
	rows, err := executor.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("describe iceberg table %s.%s: %w", table.Schema, table.Name, err)
	}
	defer func() { _ = rows.Close() }()

	var cols []describeColumn
	for rows.Next() {
		values := make([]any, 6)
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan describe output for %s.%s: %w", table.Schema, table.Name, err)
		}
		name := asString(values[0])
		if name == "" {
			continue
		}
		cols = append(cols, describeColumn{
			Name:       name,
			ColumnType: asString(values[1]),
			Nullable:   asString(values[2]),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate describe output for %s.%s: %w", table.Schema, table.Name, err)
	}
	return cols, nil
}

func buildInsertSQL(table tableRef, cols []describeColumn) string {
	values := make([]string, 0, len(cols))
	for i, col := range cols {
		mapped := mapDuckDBType(col.ColumnType)
		values = append(values, fmt.Sprintf(
			"(%s, %s, %s, %d, %s, %s, %s, %s)",
			quoteSQLString(table.Schema),
			quoteSQLString(table.Name),
			quoteSQLString(col.Name),
			i+1,
			quoteSQLString(nullableToInformationSchema(col.Nullable)),
			quoteSQLString(mapped.DataType),
			nullableIntLiteral(mapped.NumericPrecision),
			nullableIntLiteral(mapped.NumericScale),
		))
	}
	return fmt.Sprintf(`
		INSERT OR IGNORE INTO %s (
			table_schema,
			table_name,
			column_name,
			ordinal_position,
			is_nullable,
			data_type,
			numeric_precision,
			numeric_scale
		)
		VALUES %s
	`, QualifiedColumnMetadataTable, strings.Join(values, ",\n"))
}

type mappedType struct {
	DataType         string
	NumericPrecision *int
	NumericScale     *int
}

func mapDuckDBType(t string) mappedType {
	upper := strings.ToUpper(strings.TrimSpace(t))
	if m := decimalTypeRE.FindStringSubmatch(upper); len(m) == 3 {
		precision := atoi(m[1])
		scale := atoi(m[2])
		return mappedType{DataType: "numeric", NumericPrecision: &precision, NumericScale: &scale}
	}
	switch {
	case upper == "VARCHAR" || upper == "TEXT" || strings.HasPrefix(upper, "VARCHAR("):
		return mappedType{DataType: "text"}
	case upper == "BOOLEAN":
		return mappedType{DataType: "boolean"}
	case upper == "TINYINT" || upper == "SMALLINT":
		return mappedType{DataType: "smallint"}
	case upper == "INTEGER":
		return mappedType{DataType: "integer"}
	case upper == "BIGINT":
		return mappedType{DataType: "bigint"}
	case upper == "HUGEINT":
		return mappedType{DataType: "numeric"}
	case upper == "REAL" || upper == "FLOAT4":
		return mappedType{DataType: "real"}
	case upper == "DOUBLE" || upper == "FLOAT8":
		return mappedType{DataType: "double precision"}
	case upper == "DATE":
		return mappedType{DataType: "date"}
	case upper == "TIME":
		return mappedType{DataType: "time without time zone"}
	case upper == "TIMESTAMP":
		return mappedType{DataType: "timestamp without time zone"}
	case upper == "TIMESTAMPTZ" || upper == "TIMESTAMP WITH TIME ZONE":
		return mappedType{DataType: "timestamp with time zone"}
	case upper == "UUID":
		return mappedType{DataType: "uuid"}
	case upper == "BLOB" || upper == "BYTEA":
		return mappedType{DataType: "bytea"}
	case upper == "JSON":
		return mappedType{DataType: "json"}
	case strings.HasSuffix(upper, "[]") || strings.HasPrefix(upper, "LIST"):
		return mappedType{DataType: "ARRAY"}
	case strings.HasPrefix(upper, "STRUCT") || strings.HasPrefix(upper, "MAP"):
		return mappedType{DataType: "json"}
	default:
		return mappedType{DataType: strings.ToLower(t)}
	}
}

var (
	equalsRETemplate = `%s\s*=\s*'((?:''|[^'])*)'`
	inRETemplate     = `%s\s+IN\s*\(([^)]*)\)`
	stringLiteralRE  = regexp.MustCompile(`'((?:''|[^'])*)'`)
	decimalTypeRE    = regexp.MustCompile(`^DECIMAL\((\d+),\s*(\d+)\)$`)
)

func extractValues(query, column string) []string {
	identifier := `(?i)(?:(?:\w+|"[^"]+")\.)?"?` + regexp.QuoteMeta(column) + `"?`
	seen := make(map[string]struct{})
	var values []string
	for _, pattern := range []string{
		fmt.Sprintf(equalsRETemplate, identifier),
		fmt.Sprintf(inRETemplate, identifier),
	} {
		re := regexp.MustCompile(pattern)
		for _, match := range re.FindAllStringSubmatch(query, -1) {
			if strings.Contains(strings.ToUpper(match[0]), " IN ") {
				for _, lit := range stringLiteralRE.FindAllStringSubmatch(match[1], -1) {
					values = appendUnique(values, seen, unescapeSQLString(lit[1]))
				}
			} else {
				values = appendUnique(values, seen, unescapeSQLString(match[1]))
			}
		}
	}
	return values
}

func appendUnique(values []string, seen map[string]struct{}, value string) []string {
	if _, ok := seen[value]; ok {
		return values
	}
	seen[value] = struct{}{}
	return append(values, value)
}

func nullableToInformationSchema(v string) string {
	if strings.EqualFold(strings.TrimSpace(v), "NO") {
		return "NO"
	}
	return "YES"
}

func quoteIdentifier(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func quoteSQLString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func quoteSQLStringList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quoteSQLString(value)
	}
	return strings.Join(quoted, ", ")
}

func unescapeSQLString(v string) string {
	return strings.ReplaceAll(v, "''", "'")
}

func nullableIntLiteral(v *int) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%d", *v)
}

func atoi(v string) int {
	n := 0
	for _, r := range v {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func asString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}
