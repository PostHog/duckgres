//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

func TestTrinoPhysicalCatalogProvider(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %q, want GET", r.Method)
		}
		if got := r.URL.Path; got != "/v1/hogql/compatibility/physical-catalog" {
			t.Fatalf("path = %q", got)
		}
		if got := r.URL.Query().Get("catalog"); got != "ducklake" {
			t.Fatalf("catalog = %q", got)
		}
		if got := r.URL.Query().Get("protocolVersion"); got != "1" {
			t.Fatalf("protocolVersion = %q", got)
		}
		if got := r.Header.Get("X-Trino-User"); got != "admin" {
			t.Fatalf("X-Trino-User = %q", got)
		}
		wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret"))
		if got := r.Header.Get("Authorization"); got != wantAuthorization {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
            "protocolVersion": 1,
            "schemaVersion": 1,
            "catalog": {"value": "ducklake", "delimited": false},
            "catalogHandleVersion": "catalog-version",
            "tables": [{
                "schema": {"value": "analytics", "delimited": false},
                "table": {"value": "Line Items", "delimited": true},
                "columns": [
                    {
                        "name": {"value": "Line ID", "delimited": true},
                        "ordinal": 1,
                        "type": "uuid",
                        "nullable": false,
                        "hidden": false,
                        "starVisible": true
                    },
                    {
                        "name": {"value": "internal", "delimited": false},
                        "ordinal": 3,
                        "type": "array(varchar(7))",
                        "nullable": true,
                        "hidden": true,
                        "starVisible": false
                    }
                ]
            }]
        }`))
	}))
	defer server.Close()

	client := NewTrinoCatalogHTTPClient(server.URL, "admin", "secret", "")
	provider, ok := client.(hogqlcatalog.PhysicalMetadataProvider)
	if !ok {
		t.Fatal("Trino catalog HTTP client does not expose physical metadata")
	}
	metadata, err := provider.PhysicalCatalog(context.Background(), hogqlcatalog.PhysicalIdentifier{Value: "ducklake"})
	if err != nil {
		t.Fatalf("PhysicalCatalog: %v", err)
	}

	if metadata.Catalog != (hogqlcatalog.PhysicalIdentifier{Value: "ducklake"}) {
		t.Fatalf("catalog = %#v", metadata.Catalog)
	}
	if len(metadata.Tables) != 1 {
		t.Fatalf("tables = %#v", metadata.Tables)
	}
	table := metadata.Tables[0]
	if table.Schema != (hogqlcatalog.PhysicalIdentifier{Value: "analytics"}) || table.Table != (hogqlcatalog.PhysicalIdentifier{Value: "Line Items", Delimited: true}) {
		t.Fatalf("table = %#v", table)
	}
	wantColumns := []hogqlcatalog.PhysicalColumnMetadata{
		{
			Name:               hogqlcatalog.PhysicalIdentifier{Value: "Line ID", Delimited: true},
			Ordinal:            1,
			TrinoTypeSignature: "uuid",
			Nullability:        hogqlcatalog.ColumnNotNull,
			StarVisibility:     hogqlcatalog.ColumnStarVisible,
		},
		{
			Name:               hogqlcatalog.PhysicalIdentifier{Value: "internal"},
			Ordinal:            3,
			TrinoTypeSignature: "array(varchar(7))",
			Nullability:        hogqlcatalog.ColumnNullable,
			StarVisibility:     hogqlcatalog.ColumnStarHidden,
		},
	}
	if len(table.Columns) != len(wantColumns) {
		t.Fatalf("columns = %#v", table.Columns)
	}
	for index := range wantColumns {
		if table.Columns[index] != wantColumns[index] {
			t.Fatalf("column %d = %#v, want %#v", index, table.Columns[index], wantColumns[index])
		}
	}
}

func TestTrinoPhysicalCatalogProviderFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		body       string
		wantError  string
	}{
		{
			name:       "HTTP error",
			statusCode: http.StatusForbidden,
			body:       "forbidden",
			wantError:  "status 403: forbidden",
		},
		{
			name:       "unknown field",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":1,"schemaVersion":1,"catalog":{"value":"ducklake","delimited":false},"catalogHandleVersion":"v1","tables":[],"extra":true}`,
			wantError:  "unknown field",
		},
		{
			name:       "wrong protocol",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":2,"schemaVersion":1,"catalog":{"value":"ducklake","delimited":false},"catalogHandleVersion":"v1","tables":[]}`,
			wantError:  "unsupported protocolVersion",
		},
		{
			name:       "wrong catalog",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":1,"schemaVersion":1,"catalog":{"value":"other","delimited":false},"catalogHandleVersion":"v1","tables":[]}`,
			wantError:  "catalog does not match request",
		},
		{
			name:       "missing boolean",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":1,"schemaVersion":1,"catalog":{"value":"ducklake","delimited":false},"catalogHandleVersion":"v1","tables":[{"schema":{"value":"s","delimited":false},"table":{"value":"t","delimited":false},"columns":[{"name":{"value":"c","delimited":false},"ordinal":1,"type":"bigint","hidden":false,"starVisible":true}]}]}`,
			wantError:  "column 0 is incomplete",
		},
		{
			name:       "inconsistent visibility",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":1,"schemaVersion":1,"catalog":{"value":"ducklake","delimited":false},"catalogHandleVersion":"v1","tables":[{"schema":{"value":"s","delimited":false},"table":{"value":"t","delimited":false},"columns":[{"name":{"value":"c","delimited":false},"ordinal":1,"type":"bigint","nullable":false,"hidden":true,"starVisible":true}]}]}`,
			wantError:  "inconsistent visibility",
		},
		{
			name:       "multiple values",
			statusCode: http.StatusOK,
			body:       `{"protocolVersion":1,"schemaVersion":1,"catalog":{"value":"ducklake","delimited":false},"catalogHandleVersion":"v1","tables":[]} {}`,
			wantError:  "multiple JSON values",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.statusCode)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()

			client := NewTrinoCatalogHTTPClient(server.URL, "admin", "secret", "")
			provider := client.(hogqlcatalog.PhysicalMetadataProvider)
			_, err := provider.PhysicalCatalog(context.Background(), hogqlcatalog.PhysicalIdentifier{Value: "ducklake"})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestTrinoPhysicalCatalogProviderRejectsDelimitedCatalog(t *testing.T) {
	t.Parallel()

	client := NewTrinoCatalogHTTPClient("http://unused", "admin", "secret", "")
	provider := client.(hogqlcatalog.PhysicalMetadataProvider)
	_, err := provider.PhysicalCatalog(context.Background(), hogqlcatalog.PhysicalIdentifier{Value: "DuckLake", Delimited: true})
	if err == nil || !strings.Contains(err.Error(), "delimited catalog names") {
		t.Fatalf("error = %v", err)
	}
}
