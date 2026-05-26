// Package icebergmeta loads real Iceberg column metadata for
// information_schema.columns compatibility.
package icebergmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/server/sqlcore"
)

const ColumnMetadataTable = "__duckgres_iceberg_column_metadata"
const QualifiedColumnMetadataTable = "memory.main." + ColumnMetadataTable
const restCatalogConcurrency = 8

type Filters struct {
	Schemas []string
	Tables  []string
}

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

type describeColumn struct {
	Name       string
	ColumnType string
	Nullable   string
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

	tables, err := listCandidateTables(ctx, executor, ExtractFilters(query))
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

func (c Config) restMetadataSource() (restCatalogMetadataSource, error) {
	endpoint := strings.TrimSpace(c.LakekeeperEndpoint)
	warehouse := strings.TrimSpace(c.LakekeeperWarehouse)
	if endpoint == "" || warehouse == "" {
		return restCatalogMetadataSource{}, fmt.Errorf("Lakekeeper REST catalog metadata requires endpoint and warehouse")
	}
	return restCatalogMetadataSource{
		endpoint: strings.TrimRight(endpoint, "/"),
		client:   &http.Client{Timeout: 30 * time.Second},
	}, nil
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

func insertSourceColumns(ctx context.Context, executor sqlcore.QueryExecutor, columns map[tableRef][]sourceColumn) error {
	for table, cols := range columns {
		converted := make([]describeColumn, 0, len(cols))
		for _, col := range cols {
			nullable := "YES"
			if col.Required {
				nullable = "NO"
			}
			converted = append(converted, describeColumn{
				Name:       col.Name,
				ColumnType: col.Type,
				Nullable:   nullable,
			})
		}
		if len(converted) == 0 {
			continue
		}
		if _, err := executor.ExecContext(ctx, buildInsertSQL(table, converted)); err != nil {
			return fmt.Errorf("insert iceberg column metadata for %s.%s: %w", table.Schema, table.Name, err)
		}
	}
	return nil
}

var errRESTCatalogUnavailable = errors.New("lakekeeper REST catalog metadata unavailable")

type restCatalogMetadataSource struct {
	endpoint string
	client   *http.Client
}

type restConfigResponse struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
}

type restLoadTableResponse struct {
	Metadata restTableMetadata `json:"metadata"`
}

type restTableMetadata struct {
	CurrentSchemaID *int         `json:"current-schema-id"`
	Schemas         []restSchema `json:"schemas"`
	Schema          restSchema   `json:"schema"`
}

type restSchema struct {
	SchemaID int         `json:"schema-id"`
	Fields   []restField `json:"fields"`
}

type restField struct {
	Name     string `json:"name"`
	Type     any    `json:"type"`
	Required bool   `json:"required"`
}

func (s restCatalogMetadataSource) LoadColumns(ctx context.Context, warehouse string, tables []tableRef) (map[tableRef][]sourceColumn, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	prefix, err := s.loadPrefix(ctx, warehouse)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(map[tableRef][]sourceColumn)
	sem := make(chan struct{}, restCatalogConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, table := range tables {
		table := table
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				mu.Unlock()
				return
			}

			cols, err := s.loadTableColumns(ctx, prefix, table)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				return
			}
			out[table] = cols
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func (s restCatalogMetadataSource) loadPrefix(ctx context.Context, warehouse string) (string, error) {
	var resp restConfigResponse
	configURL := s.endpoint + "/v1/config?warehouse=" + url.QueryEscape(warehouse)
	if err := s.getJSON(ctx, configURL, &resp); err != nil {
		return "", err
	}
	if resp.Overrides != nil {
		if prefix := strings.TrimSpace(resp.Overrides["prefix"]); prefix != "" {
			return prefix, nil
		}
	}
	if resp.Defaults != nil {
		if prefix := strings.TrimSpace(resp.Defaults["prefix"]); prefix != "" {
			return prefix, nil
		}
	}
	return "", fmt.Errorf("lakekeeper REST catalog config response missing prefix")
}

func (s restCatalogMetadataSource) loadTableColumns(ctx context.Context, prefix string, table tableRef) ([]sourceColumn, error) {
	var resp restLoadTableResponse
	tableURL := s.endpoint + "/v1/" + url.PathEscape(prefix) +
		"/namespaces/" + encodeRESTNamespace(table.Schema) +
		"/tables/" + url.PathEscape(table.Name)
	if err := s.getJSON(ctx, tableURL, &resp); err != nil {
		return nil, err
	}
	schema, err := currentRESTSchema(resp.Metadata)
	if err != nil {
		return nil, fmt.Errorf("load lakekeeper REST schema for %s.%s: %w", table.Schema, table.Name, err)
	}
	cols := make([]sourceColumn, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		if strings.TrimSpace(field.Name) == "" {
			return nil, fmt.Errorf("load lakekeeper REST schema for %s.%s: column name is empty", table.Schema, table.Name)
		}
		colType, err := restFieldTypeString(field.Type)
		if err != nil {
			return nil, fmt.Errorf("load lakekeeper REST schema for %s.%s column %q: %w", table.Schema, table.Name, field.Name, err)
		}
		cols = append(cols, sourceColumn{Name: field.Name, Type: colType, Required: field.Required})
	}
	return cols, nil
}

func (s restCatalogMetadataSource) getJSON(ctx context.Context, requestURL string, out any) error {
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("%w: build request: %v", errRESTCatalogUnavailable, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: GET %s: %v", errRESTCatalogUnavailable, requestURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%w: read GET %s response: %v", errRESTCatalogUnavailable, requestURL, err)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: GET %s returned HTTP %d", errRESTCatalogUnavailable, requestURL, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("lakekeeper REST catalog GET %s returned HTTP %d: %s", requestURL, resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode lakekeeper REST catalog GET %s response: %w", requestURL, err)
	}
	return nil
}

func currentRESTSchema(metadata restTableMetadata) (restSchema, error) {
	if metadata.CurrentSchemaID != nil {
		for _, schema := range metadata.Schemas {
			if schema.SchemaID == *metadata.CurrentSchemaID {
				return schema, nil
			}
		}
		if metadata.Schema.SchemaID == *metadata.CurrentSchemaID && len(metadata.Schema.Fields) > 0 {
			return metadata.Schema, nil
		}
		return restSchema{}, fmt.Errorf("current schema id %d not found", *metadata.CurrentSchemaID)
	}
	if len(metadata.Schemas) == 1 {
		return metadata.Schemas[0], nil
	}
	if len(metadata.Schema.Fields) > 0 {
		return metadata.Schema, nil
	}
	return restSchema{}, fmt.Errorf("current schema id missing")
}

func restFieldTypeString(v any) (string, error) {
	switch typed := v.(type) {
	case nil:
		return "", fmt.Errorf("type is missing")
	case string:
		if strings.TrimSpace(typed) == "" {
			return "", fmt.Errorf("type is empty")
		}
		return typed, nil
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return "", fmt.Errorf("encode complex type: %w", err)
		}
		return string(encoded), nil
	}
}

func encodeRESTNamespace(schema string) string {
	parts := strings.Split(schema, ".")
	return url.PathEscape(strings.Join(parts, "\x1f"))
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
	case upper == "VARCHAR" || upper == "TEXT" || upper == "STRING" || strings.HasPrefix(upper, "VARCHAR("):
		return mappedType{DataType: "text"}
	case upper == "BOOLEAN" || upper == "BOOL":
		return mappedType{DataType: "boolean"}
	case upper == "TINYINT" || upper == "SMALLINT":
		return mappedType{DataType: "smallint"}
	case upper == "INTEGER" || upper == "INT":
		return mappedType{DataType: "integer"}
	case upper == "BIGINT" || upper == "LONG":
		return mappedType{DataType: "bigint"}
	case upper == "HUGEINT":
		return mappedType{DataType: "numeric"}
	case upper == "REAL" || upper == "FLOAT4":
		return mappedType{DataType: "real"}
	case upper == "DOUBLE" || upper == "FLOAT8" || upper == "DOUBLE PRECISION":
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
	case upper == "BLOB" || upper == "BYTEA" || upper == "BINARY" || upper == "FIXED":
		return mappedType{DataType: "bytea"}
	case upper == "JSON":
		return mappedType{DataType: "json"}
	case strings.HasSuffix(upper, "[]") || strings.HasPrefix(upper, "LIST"):
		return mappedType{DataType: "ARRAY"}
	case strings.HasPrefix(upper, "STRUCT") || strings.HasPrefix(upper, "MAP") || strings.HasPrefix(upper, "{"):
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
