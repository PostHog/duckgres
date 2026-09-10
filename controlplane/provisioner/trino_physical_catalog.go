//go:build kubernetes

package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

const (
	trinoPhysicalCatalogProtocolVersion  = 1
	trinoPhysicalCatalogSchemaVersion    = 1
	maxTrinoPhysicalCatalogResponseBytes = 32 << 20
)

type trinoPhysicalCatalogResponse struct {
	ProtocolVersion      *int                             `json:"protocolVersion"`
	SchemaVersion        *int                             `json:"schemaVersion"`
	Catalog              *hogqlcatalog.PhysicalIdentifier `json:"catalog"`
	CatalogHandleVersion *string                          `json:"catalogHandleVersion"`
	Tables               *[]trinoPhysicalTable            `json:"tables"`
}

type trinoPhysicalTable struct {
	Schema  *hogqlcatalog.PhysicalIdentifier `json:"schema"`
	Table   *hogqlcatalog.PhysicalIdentifier `json:"table"`
	Columns *[]trinoPhysicalColumn           `json:"columns"`
}

type trinoPhysicalColumn struct {
	Name        *hogqlcatalog.PhysicalIdentifier `json:"name"`
	Ordinal     *int                             `json:"ordinal"`
	Type        *string                          `json:"type"`
	Nullable    *bool                            `json:"nullable"`
	Hidden      *bool                            `json:"hidden"`
	StarVisible *bool                            `json:"starVisible"`
}

func (c *trinoCatalogHTTPClient) PhysicalCatalog(ctx context.Context, catalog hogqlcatalog.PhysicalIdentifier) (*hogqlcatalog.PhysicalCatalogMetadata, error) {
	if catalog.Delimited {
		return nil, fmt.Errorf("read Trino physical catalog: delimited catalog names are not supported")
	}
	requestURL, err := url.Parse(c.baseURL + "/v1/hogql/compatibility/physical-catalog")
	if err != nil {
		return nil, fmt.Errorf("build Trino physical catalog URL: %w", err)
	}
	query := requestURL.Query()
	query.Set("catalog", catalog.Value)
	query.Set("protocolVersion", strconv.Itoa(trinoPhysicalCatalogProtocolVersion))
	requestURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build Trino physical catalog request: %w", err)
	}
	username, password := c.credentials()
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Trino-User", username)
	req.Header.Set("Authorization", "Basic "+basicAuth(username, password))

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read Trino physical catalog: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("read Trino physical catalog: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTrinoPhysicalCatalogResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Trino physical catalog response: %w", err)
	}
	if len(body) > maxTrinoPhysicalCatalogResponseBytes {
		return nil, fmt.Errorf("read Trino physical catalog: response exceeds %d bytes", maxTrinoPhysicalCatalogResponseBytes)
	}
	var document trinoPhysicalCatalogResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode Trino physical catalog: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode Trino physical catalog: %w", err)
	}
	return translateTrinoPhysicalCatalog(catalog, document)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("multiple JSON values")
}

func translateTrinoPhysicalCatalog(requested hogqlcatalog.PhysicalIdentifier, document trinoPhysicalCatalogResponse) (*hogqlcatalog.PhysicalCatalogMetadata, error) {
	if document.ProtocolVersion == nil || *document.ProtocolVersion != trinoPhysicalCatalogProtocolVersion {
		return nil, fmt.Errorf("decode Trino physical catalog: unsupported protocolVersion")
	}
	if document.SchemaVersion == nil || *document.SchemaVersion != trinoPhysicalCatalogSchemaVersion {
		return nil, fmt.Errorf("decode Trino physical catalog: unsupported schemaVersion")
	}
	if document.Catalog == nil || *document.Catalog != requested {
		return nil, fmt.Errorf("decode Trino physical catalog: catalog does not match request")
	}
	if document.CatalogHandleVersion == nil || strings.TrimSpace(*document.CatalogHandleVersion) == "" {
		return nil, fmt.Errorf("decode Trino physical catalog: catalogHandleVersion is required")
	}
	if document.Tables == nil {
		return nil, fmt.Errorf("decode Trino physical catalog: tables are required")
	}

	tables := make([]hogqlcatalog.PhysicalTableMetadata, 0, len(*document.Tables))
	for tableIndex, table := range *document.Tables {
		if table.Schema == nil || table.Table == nil || table.Columns == nil {
			return nil, fmt.Errorf("decode Trino physical catalog: table %d is incomplete", tableIndex)
		}
		columns := make([]hogqlcatalog.PhysicalColumnMetadata, 0, len(*table.Columns))
		for columnIndex, column := range *table.Columns {
			if column.Name == nil || column.Ordinal == nil || column.Type == nil || column.Nullable == nil || column.Hidden == nil || column.StarVisible == nil {
				return nil, fmt.Errorf("decode Trino physical catalog: table %d column %d is incomplete", tableIndex, columnIndex)
			}
			if *column.Hidden == *column.StarVisible {
				return nil, fmt.Errorf("decode Trino physical catalog: table %d column %d has inconsistent visibility", tableIndex, columnIndex)
			}
			nullability := hogqlcatalog.ColumnNotNull
			if *column.Nullable {
				nullability = hogqlcatalog.ColumnNullable
			}
			starVisibility := hogqlcatalog.ColumnStarHidden
			if *column.StarVisible {
				starVisibility = hogqlcatalog.ColumnStarVisible
			}
			columns = append(columns, hogqlcatalog.PhysicalColumnMetadata{
				Name:               *column.Name,
				Ordinal:            *column.Ordinal,
				TrinoTypeSignature: *column.Type,
				Nullability:        nullability,
				StarVisibility:     starVisibility,
			})
		}
		tables = append(tables, hogqlcatalog.PhysicalTableMetadata{
			Schema:  *table.Schema,
			Table:   *table.Table,
			Columns: columns,
		})
	}
	return &hogqlcatalog.PhysicalCatalogMetadata{Catalog: *document.Catalog, Tables: tables}, nil
}

var _ hogqlcatalog.PhysicalMetadataProvider = (*trinoCatalogHTTPClient)(nil)
