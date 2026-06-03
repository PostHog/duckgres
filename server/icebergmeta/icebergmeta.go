// Package icebergmeta loads real Iceberg column metadata for
// information_schema.columns compatibility.
package icebergmeta

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/sqlcore"
)

const ColumnMetadataTable = "__duckgres_iceberg_column_metadata"
const QualifiedColumnMetadataTable = "memory.main." + ColumnMetadataTable

type tableRef struct {
	Schema string
	Name   string
}

type Config struct {
	LakekeeperEndpoint        string
	LakekeeperWarehouse       string
	LakekeeperOAuth2ServerURI string
}

type tableFilter struct {
	Schema string
	Name   string
}

type sourceColumn struct {
	Name     string
	Type     string
	Required bool
}

func ShouldLoadColumns(query string) bool {
	return strings.Contains(strings.ToLower(query), "information_schema_columns_compat")
}

func LoadColumns(ctx context.Context, executor sqlcore.QueryExecutor, query string, configs ...Config) error {
	if executor == nil || !ShouldLoadColumns(query) {
		return nil
	}

	cfg := firstConfig(configs)
	if strings.TrimSpace(cfg.LakekeeperOAuth2ServerURI) != "" {
		return nil
	}

	filter := extractTableFilter(query)
	tables, err := listCandidateTables(ctx, executor, filter)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}

	source, err := cfg.restMetadataSource()
	if err != nil {
		return err
	}
	strictMissing := filter.Schema != "" || filter.Name != ""
	columns, err := source.LoadColumns(ctx, cfg.LakekeeperWarehouse, tables, strictMissing)
	if err != nil {
		return err
	}
	return insertSourceColumns(ctx, executor, columns)
}

func firstConfig(configs []Config) Config {
	if len(configs) == 0 {
		return Config{}
	}
	return configs[0]
}

func extractTableFilter(query string) tableFilter {
	lower := strings.ToLower(query)
	return tableFilter{
		Schema: extractSingleQuotedPredicate(query, lower, "table_schema"),
		Name:   extractSingleQuotedPredicate(query, lower, "table_name"),
	}
}

func extractSingleQuotedPredicate(query, lowerQuery, column string) string {
	markers := []string{
		column + " = '",
		column + "='",
		"c." + column + " = '",
		"c." + column + "='",
	}
	for _, marker := range markers {
		idx := strings.Index(lowerQuery, marker)
		if idx < 0 {
			continue
		}
		start := idx + len(marker)
		end := strings.Index(query[start:], "'")
		if end < 0 {
			return ""
		}
		value := query[start : start+end]
		if strings.Contains(value, "%") {
			return ""
		}
		return value
	}
	return ""
}

func listCandidateTables(ctx context.Context, executor sqlcore.QueryExecutor, filter tableFilter) ([]tableRef, error) {
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_catalog = 'iceberg'
		AND table_type = 'BASE TABLE'
	`
	if filter.Schema != "" {
		query += "\n\t\tAND table_schema = " + quoteSQLString(filter.Schema)
	}
	if filter.Name != "" {
		query += "\n\t\tAND table_name = " + quoteSQLString(filter.Name)
	}
	query += "\n\t\tORDER BY table_schema, table_name"

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
