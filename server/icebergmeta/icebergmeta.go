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

	tables, err := listCandidateTables(ctx, executor)
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
	columns, err := source.LoadColumns(ctx, cfg.LakekeeperWarehouse, tables)
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

func listCandidateTables(ctx context.Context, executor sqlcore.QueryExecutor) ([]tableRef, error) {
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_catalog = 'iceberg'
		AND table_type = 'BASE TABLE'
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
