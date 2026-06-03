package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/iceberg"
	"github.com/posthog/duckgres/server/sqlcore"
)

type dropSchemaCascadeTarget struct {
	Catalog string
	Schema  string
}

func parseDropSchemaCascadeTarget(query string) (dropSchemaCascadeTarget, bool) {
	s := strings.TrimSpace(sqlcore.StripLeadingComments(query))
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
		pos = next
	}

	targetSQL, ok := trimTrailingSQLKeyword(s[pos:], "CASCADE")
	if !ok {
		return dropSchemaCascadeTarget{}, false
	}
	parts, ok := sqlcore.ParseQualifiedIdentifier(targetSQL)
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

	// Inline the schema as an escaped string literal rather than a `?` bind
	// param: the Flight executor (control plane → worker) applies args by
	// string-inlining $N placeholders only, and sends no bind params, so a `?`
	// reaches the worker unbound and fails ("incorrect argument count: have 0
	// want 1"). Inlining works on both the Flight and standalone executors.
	rows, err := c.executor.QueryContext(ctx, fmt.Sprintf(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_catalog = 'iceberg'
		AND table_schema = '%s'
		ORDER BY table_name
	`, escapeSQLStringLiteral(target.Schema)))
	if err != nil {
		return nil, fmt.Errorf("list iceberg schema tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		table, err := scanStringColumn(rows)
		if err != nil {
			return nil, fmt.Errorf("scan iceberg schema table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list iceberg schema tables: %w", err)
	}

	for _, table := range tables {
		dropTable := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s.%s",
			sqlcore.QuoteIdentifier(iceberg.CatalogName),
			sqlcore.QuoteIdentifier(target.Schema),
			sqlcore.QuoteIdentifier(table),
		)
		if _, err := c.executor.ExecContext(ctx, dropTable); err != nil {
			return nil, fmt.Errorf("drop iceberg table %s.%s: %w", target.Schema, table, err)
		}
	}

	dropSchema := fmt.Sprintf("DROP SCHEMA IF EXISTS %s.%s",
		sqlcore.QuoteIdentifier(iceberg.CatalogName),
		sqlcore.QuoteIdentifier(target.Schema),
	)
	return c.executor.ExecContext(ctx, dropSchema)
}

func (c *clientConn) currentSearchPathCatalog(ctx context.Context) (string, error) {
	rows, err := c.executor.QueryContext(ctx, `
		SELECT value
		FROM duckdb_settings()
		WHERE name = 'search_path'
	`)
	if err != nil {
		return "", fmt.Errorf("read search_path: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return "", rows.Err()
	}
	searchPath, err := scanStringColumn(rows)
	if err != nil {
		return "", fmt.Errorf("scan search_path: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return sqlcore.CatalogFromSearchPath(searchPath), nil
}

// scanStringColumn reads a single text column from a Duckgres RowSet. The
// Flight executor's Scan contract requires an *interface{} destination (Arrow
// values come back as interface{}, not typed pointers), so a plain *string
// scan fails at runtime with "destination 0 must be *interface{}".
func scanStringColumn(rows RowSet) (string, error) {
	var raw interface{}
	if err := rows.Scan(&raw); err != nil {
		return "", err
	}
	switch v := raw.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
