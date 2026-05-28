package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/iceberg"
)

type dropSchemaCascadeTarget struct {
	Catalog  string
	Schema   string
	IfExists bool
}

func parseDropSchemaCascadeTarget(query string) (dropSchemaCascadeTarget, bool) {
	s := strings.TrimSpace(stripLeadingComments(query))
	s = strings.TrimSpace(strings.TrimSuffix(s, ";"))

	pos, ok := consumeSQLKeyword(s, 0, "DROP")
	if !ok {
		return dropSchemaCascadeTarget{}, false
	}
	pos, ok = consumeSQLKeyword(s, pos, "SCHEMA")
	if !ok {
		return dropSchemaCascadeTarget{}, false
	}

	target := dropSchemaCascadeTarget{}
	if next, ok := consumeSQLKeyword(s, pos, "IF"); ok {
		next, ok = consumeSQLKeyword(s, next, "EXISTS")
		if !ok {
			return dropSchemaCascadeTarget{}, false
		}
		target.IfExists = true
		pos = next
	}

	targetSQL, ok := trimTrailingSQLKeyword(s[pos:], "CASCADE")
	if !ok {
		return dropSchemaCascadeTarget{}, false
	}
	parts, ok := splitDropSchemaTarget(targetSQL)
	if !ok || len(parts) == 0 || len(parts) > 2 {
		return dropSchemaCascadeTarget{}, false
	}
	if len(parts) == 1 {
		target.Schema = parts[0]
	} else {
		target.Catalog = parts[0]
		target.Schema = parts[1]
	}
	if target.Schema == "" || target.Catalog == "" && len(parts) == 2 {
		return dropSchemaCascadeTarget{}, false
	}
	return target, true
}

func consumeSQLKeyword(s string, pos int, keyword string) (int, bool) {
	pos = skipSQLSpace(s, pos)
	end := pos + len(keyword)
	if end > len(s) || !strings.EqualFold(s[pos:end], keyword) {
		return pos, false
	}
	if end < len(s) && isSQLIdentChar(s[end]) {
		return pos, false
	}
	return end, true
}

func trimTrailingSQLKeyword(s string, keyword string) (string, bool) {
	s = strings.TrimSpace(s)
	end := len(s) - len(keyword)
	if end < 0 || !strings.EqualFold(s[end:], keyword) {
		return "", false
	}
	if end > 0 && !isSQLSpace(s[end-1]) {
		return "", false
	}
	target := strings.TrimSpace(s[:end])
	return target, target != ""
}

func splitDropSchemaTarget(s string) ([]string, bool) {
	var parts []string
	inQuotes := false
	partStart := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' {
			if inQuotes && i+1 < len(s) && s[i+1] == '"' {
				i++
				continue
			}
			inQuotes = !inQuotes
			continue
		}
		if ch == '.' && !inQuotes {
			part, ok := parseDropSchemaIdentifierPart(s[partStart:i])
			if !ok {
				return nil, false
			}
			parts = append(parts, part)
			partStart = i + 1
			continue
		}
	}
	if inQuotes {
		return nil, false
	}
	part, ok := parseDropSchemaIdentifierPart(s[partStart:])
	if !ok {
		return nil, false
	}
	parts = append(parts, part)
	return parts, true
}

func parseDropSchemaIdentifierPart(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	if s[0] != '"' {
		if strings.Contains(s, `"`) {
			return "", false
		}
		return strings.ToLower(s), true
	}
	if len(s) < 2 || s[len(s)-1] != '"' {
		return "", false
	}
	var ident strings.Builder
	for i := 1; i < len(s)-1; i++ {
		if s[i] == '"' {
			if i+1 >= len(s)-1 || s[i+1] != '"' {
				return "", false
			}
			ident.WriteByte('"')
			i++
			continue
		}
		ident.WriteByte(s[i])
	}
	return ident.String(), true
}

func skipSQLSpace(s string, pos int) int {
	for pos < len(s) && isSQLSpace(s[pos]) {
		pos++
	}
	return pos
}

func isSQLSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '\f':
		return true
	default:
		return false
	}
}

func isSQLIdentChar(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func isIcebergDropSchemaCascadeUnsupported(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently")
}

func (c *clientConn) dropIcebergSchemaCascade(ctx context.Context, query string) (ExecResult, error) {
	target, ok := parseDropSchemaCascadeTarget(query)
	if !ok {
		return nil, fmt.Errorf("not a DROP SCHEMA CASCADE statement")
	}

	catalog := target.Catalog
	if catalog == "" {
		defaultCatalog, err := c.currentSearchPathCatalog(ctx)
		if err != nil {
			return nil, err
		}
		catalog = defaultCatalog
	}
	if !strings.EqualFold(catalog, iceberg.CatalogName) {
		return nil, fmt.Errorf("DROP SCHEMA CASCADE fallback only supports iceberg catalog, got %q", catalog)
	}

	rows, err := c.executor.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_catalog = 'iceberg'
		AND table_schema = ?
		ORDER BY table_name
	`, target.Schema)
	if err != nil {
		return nil, fmt.Errorf("list iceberg schema tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("scan iceberg schema table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list iceberg schema tables: %w", err)
	}

	for _, table := range tables {
		dropTable := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s.%s",
			quoteDuckDBIdentifier(iceberg.CatalogName),
			quoteDuckDBIdentifier(target.Schema),
			quoteDuckDBIdentifier(table),
		)
		if _, err := c.executor.ExecContext(ctx, dropTable); err != nil {
			return nil, fmt.Errorf("drop iceberg table %s.%s: %w", target.Schema, table, err)
		}
	}

	dropSchema := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s",
		quoteDuckDBIdentifier(iceberg.CatalogName),
		quoteDuckDBIdentifier(target.Schema),
	)
	return c.executor.ExecContext(ctx, dropSchema)
}

func (c *clientConn) currentSearchPathCatalog(ctx context.Context) (string, error) {
	rows, err := c.executor.QueryContext(ctx, `
		SELECT lower(regexp_extract(regexp_replace(value, '\s+', '', 'g'), '^([A-Za-z0-9_]+)\.', 1))
		FROM duckdb_settings()
		WHERE name = 'search_path'
	`)
	if err != nil {
		return "", fmt.Errorf("read search_path: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return "", rows.Err()
	}
	var catalog string
	if err := rows.Scan(&catalog); err != nil {
		return "", fmt.Errorf("scan search_path catalog: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return catalog, nil
}

func quoteDuckDBIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
