package icebergmeta

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/pgtypes"
	"github.com/posthog/duckgres/server/sqlcore"
)

func insertSourceColumns(ctx context.Context, executor sqlcore.QueryExecutor, columns map[tableRef][]sourceColumn) error {
	for table, cols := range columns {
		if _, err := executor.ExecContext(ctx, buildReplaceSQL(table, cols)); err != nil {
			return fmt.Errorf("replace iceberg column metadata for %s.%s: %w", table.Schema, table.Name, err)
		}
	}
	return nil
}

func buildReplaceSQL(table tableRef, cols []sourceColumn) string {
	deleteSQL := fmt.Sprintf(
		"DELETE FROM %s WHERE table_schema = %s AND table_name = %s",
		QualifiedColumnMetadataTable,
		quoteSQLString(table.Schema),
		quoteSQLString(table.Name),
	)
	if len(cols) == 0 {
		return deleteSQL
	}

	values := make([]string, 0, len(cols))
	for i, col := range cols {
		nullable := "YES"
		if col.Required {
			nullable = "NO"
		}
		pgType := pgtypes.ForIceberg(col.Type)
		values = append(values, fmt.Sprintf(
			"(%s, %s, %s, %d, %s, %s, %s, %s, %s)",
			quoteSQLString(table.Schema),
			quoteSQLString(table.Name),
			quoteSQLString(col.Name),
			i+1,
			quoteSQLString(nullable),
			quoteSQLString(pgType.DataType),
			quoteSQLString(pgType.UDTName),
			nullableIntLiteral(pgType.NumericPrecision),
			nullableIntLiteral(pgType.NumericScale),
		))
	}
	return deleteSQL + ";\n" + fmt.Sprintf(`
		INSERT INTO %s (
			table_schema,
			table_name,
			column_name,
			ordinal_position,
			is_nullable,
			data_type,
			udt_name,
			numeric_precision,
			numeric_scale
		)
		VALUES %s
	`, QualifiedColumnMetadataTable, strings.Join(values, ",\n"))
}

func quoteSQLString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func nullableIntLiteral(v *int) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%d", *v)
}
