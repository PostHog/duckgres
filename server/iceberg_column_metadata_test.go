package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/iceberg"
	"github.com/posthog/duckgres/server/icebergmeta"
	"github.com/posthog/duckgres/server/sqlcore"
)

func TestShouldLoadIcebergColumnMetadataOnlyForLakekeeper(t *testing.T) {
	if !shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled:             true,
		Backend:             iceberg.BackendLakekeeper,
		LakekeeperEndpoint:  "http://lakekeeper.invalid/catalog",
		LakekeeperWarehouse: "org-acme",
	}, false) {
		t.Fatal("expected Lakekeeper catalog to load Iceberg column metadata")
	}
	if shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled: true,
		Backend: iceberg.BackendS3Tables,
	}, false) {
		t.Fatal("S3 Tables catalog should not use Lakekeeper REST metadata loading")
	}
	if shouldLoadIcebergColumnMetadata(IcebergConfig{
		Enabled: true,
		Backend: iceberg.BackendLakekeeper,
	}, true) {
		t.Fatal("passthrough connections should not load Iceberg column metadata")
	}
}

func TestLoadIcebergColumnMetadataUsesConnectionIcebergConfig(t *testing.T) {
	cc := &clientConn{
		server: &Server{cfg: Config{}},
	}
	SetConnectionIcebergConfig(cc, IcebergConfig{
		Enabled:             true,
		Backend:             iceberg.BackendLakekeeper,
		LakekeeperEndpoint:  "http://lakekeeper.example/catalog",
		LakekeeperWarehouse: "org-acme",
	})

	cfg := cc.effectiveIcebergConfig()
	if !shouldLoadIcebergColumnMetadata(cfg, false) {
		t.Fatalf("expected tenant Iceberg config to enable metadata loading")
	}
	if cfg.LakekeeperEndpoint != "http://lakekeeper.example/catalog" {
		t.Fatalf("tenant endpoint not preserved: %q", cfg.LakekeeperEndpoint)
	}
	if cfg.LakekeeperWarehouse != "org-acme" {
		t.Fatalf("tenant warehouse not preserved: %q", cfg.LakekeeperWarehouse)
	}
}

func TestQueryWithArgsWithMetadataLoadsIcebergColumns(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			if got, want := r.URL.Query().Get("warehouse"), "org-acme"; got != want {
				t.Fatalf("warehouse query = %q, want %q", got, want)
			}
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse-id"}}`))
		case "/v1/warehouse-id/namespaces/stripe/tables/person":
			_, _ = w.Write([]byte(`{
				"metadata": {
					"current-schema-id": 1,
					"schemas": [
						{
							"schema-id": 1,
							"type": "struct",
							"fields": [
								{"id": 1, "name": "id", "type": "string", "required": true}
							]
						}
					]
				}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	exec := &metadataTrackingExecutor{}
	cc := &clientConn{
		executor: exec,
		tenantIcebergConfig: IcebergConfig{
			Enabled:             true,
			Backend:             iceberg.BackendLakekeeper,
			LakekeeperEndpoint:  srv.URL,
			LakekeeperWarehouse: "org-acme",
		},
		hasTenantIcebergConfig: true,
	}

	rows, err := cc.queryWithArgsWithMetadata(
		context.Background(),
		"SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = $1",
		"stripe",
	)
	if err != nil {
		t.Fatalf("queryWithArgsWithMetadata: %v", err)
	}
	if rows == nil {
		t.Fatal("expected query rows")
	}
	if !exec.loadedMetadata {
		t.Fatal("expected Iceberg column metadata to be loaded before running extended query")
	}
	if len(exec.queryArgs) != 1 || exec.queryArgs[0] != "stripe" {
		t.Fatalf("query args = %#v, want [stripe]", exec.queryArgs)
	}
}

func TestQueryWithArgsWithMetadataSkipsNonCompatQueries(t *testing.T) {
	exec := &metadataTrackingExecutor{}
	cc := &clientConn{
		executor: exec,
		tenantIcebergConfig: IcebergConfig{
			Enabled:             true,
			Backend:             iceberg.BackendLakekeeper,
			LakekeeperEndpoint:  "http://lakekeeper.invalid/catalog",
			LakekeeperWarehouse: "org-acme",
		},
		hasTenantIcebergConfig: true,
	}

	_, err := cc.queryWithArgsWithMetadata(
		context.Background(),
		"SELECT * FROM stripe.person WHERE id = $1",
		"person_1",
	)
	if err != nil {
		t.Fatalf("queryWithArgsWithMetadata: %v", err)
	}
	if exec.loadedMetadata {
		t.Fatal("did not expect metadata loading for non-compat query")
	}
}

type metadataTrackingExecutor struct {
	loadedMetadata bool
	queryArgs      []any
}

func (e *metadataTrackingExecutor) Exec(string, ...any) (sqlcore.ExecResult, error) {
	return nil, nil
}

func (e *metadataTrackingExecutor) ExecContext(_ context.Context, query string, _ ...any) (sqlcore.ExecResult, error) {
	if strings.Contains(query, icebergmeta.ColumnMetadataTable) {
		e.loadedMetadata = true
	}
	return nil, nil
}

func (e *metadataTrackingExecutor) Query(_ string, args ...any) (sqlcore.RowSet, error) {
	e.queryArgs = append([]any{}, args...)
	return &emptyRowSet{}, nil
}

func (e *metadataTrackingExecutor) QueryContext(_ context.Context, query string, _ ...any) (sqlcore.RowSet, error) {
	if strings.Contains(query, "information_schema.tables") {
		return &metadataRows{
			cols: []string{"table_schema", "table_name"},
			rows: [][]any{{"stripe", "person"}},
		}, nil
	}
	return &emptyRowSet{}, nil
}

func (e *metadataTrackingExecutor) ConnContext(context.Context) (sqlcore.RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *metadataTrackingExecutor) PingContext(context.Context) error { return nil }
func (e *metadataTrackingExecutor) Close() error                      { return nil }
func (e *metadataTrackingExecutor) LastProfilingOutput() string       { return "" }

type emptyRowSet struct{}

func (r *emptyRowSet) Columns() ([]string, error)                  { return nil, nil }
func (r *emptyRowSet) ColumnTypes() ([]sqlcore.ColumnTyper, error) { return nil, nil }
func (r *emptyRowSet) Next() bool                                  { return false }
func (r *emptyRowSet) Scan(...any) error                           { return nil }
func (r *emptyRowSet) Close() error                                { return nil }
func (r *emptyRowSet) Err() error                                  { return nil }

type metadataRows struct {
	cols []string
	rows [][]any
	i    int
}

func (r *metadataRows) Columns() ([]string, error)                  { return r.cols, nil }
func (r *metadataRows) ColumnTypes() ([]sqlcore.ColumnTyper, error) { return nil, nil }
func (r *metadataRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *metadataRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	for i := range dest {
		ptr, ok := dest[i].(*any)
		if !ok {
			return errors.New("expected *any dest")
		}
		*ptr = row[i]
	}
	return nil
}
func (r *metadataRows) Close() error { return nil }
func (r *metadataRows) Err() error   { return nil }
