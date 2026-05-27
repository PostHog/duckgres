package icebergmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const restCatalogConcurrency = 8

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

func (c Config) restMetadataSource() (restCatalogMetadataSource, error) {
	endpoint := strings.TrimSpace(c.LakekeeperEndpoint)
	warehouse := strings.TrimSpace(c.LakekeeperWarehouse)
	if endpoint == "" || warehouse == "" {
		return restCatalogMetadataSource{}, fmt.Errorf("lakekeeper REST catalog metadata requires endpoint and warehouse")
	}
	return restCatalogMetadataSource{
		endpoint: strings.TrimRight(endpoint, "/"),
		client:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (s restCatalogMetadataSource) LoadColumns(ctx context.Context, warehouse string, tables []tableRef) (map[tableRef][]sourceColumn, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	prefix, err := s.loadPrefix(ctx, warehouse)
	if err != nil {
		return nil, err
	}

	out := make(map[tableRef][]sourceColumn)
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(restCatalogConcurrency)

	for _, table := range tables {
		table := table
		g.Go(func() error {
			cols, err := s.loadTableColumns(ctx, prefix, table)
			if err != nil {
				return err
			}
			mu.Lock()
			out[table] = cols
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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
