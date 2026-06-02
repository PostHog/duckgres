package icebergmeta

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/posthog/duckgres/server/sqlcore"
)

func TestShouldLoadColumnsOnlyForCompatView(t *testing.T) {
	if !ShouldLoadColumns("SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'") {
		t.Fatal("expected information_schema_columns_compat query to require loading")
	}
	if ShouldLoadColumns("SELECT * FROM memory.main.information_schema_tables_compat") {
		t.Fatal("tables compat query should not require column loading")
	}
}

func TestLoadColumnsRequiresLakekeeperRESTConfig(t *testing.T) {
	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{
				{"billing_public", "public_api_keys"},
			}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'")
	if err == nil {
		t.Fatal("expected missing REST config error")
	}
	if !strings.Contains(err.Error(), "lakekeeper REST catalog metadata requires endpoint and warehouse") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadColumnsUsesLakekeeperREST(t *testing.T) {
	var tableLoads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			if got, want := r.URL.Query().Get("warehouse"), "org-acme"; got != want {
				t.Fatalf("warehouse query = %q, want %q", got, want)
			}
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse-id"}}`))
		case "/v1/warehouse-id/namespaces/billing_public/tables/public_api_keys":
			tableLoads.Add(1)
			_, _ = w.Write([]byte(`{
				"metadata": {
					"current-schema-id": 7,
					"schemas": [
						{
							"schema-id": 7,
							"type": "struct",
							"fields": [
								{"id": 1, "name": "id", "type": "string", "required": true},
								{"id": 2, "name": "amount", "type": "decimal(10,2)", "required": false}
							]
						}
					]
				}
			}`))
		case "/v1/warehouse-id/namespaces/billing_public/tables/billing_productseat":
			tableLoads.Add(1)
			_, _ = w.Write([]byte(`{
				"metadata": {
					"current-schema-id": 7,
					"schemas": [
						{
							"schema-id": 7,
							"type": "struct",
							"fields": [
								{"id": 1, "name": "active", "type": "boolean", "required": false}
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

	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{
				{"billing_public", "public_api_keys"},
				{"billing_public", "billing_productseat"},
			}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'", Config{
		LakekeeperEndpoint:  srv.URL,
		LakekeeperWarehouse: "org-acme",
	})
	if err != nil {
		t.Fatalf("LoadColumns: %v", err)
	}
	if got := strings.Join(exec.queries, "\n"); strings.Contains(got, "DESCRIBE SELECT") {
		t.Fatalf("REST metadata path should not describe Iceberg tables, queries:\n%s", got)
	}
	if got := strings.Join(exec.queries, "\n"); strings.Contains(got, "table_schema IN") || strings.Contains(got, "table_name IN") {
		t.Fatalf("metadata loading should not parse user query predicates into table filters, queries:\n%s", got)
	}
	if got := strings.Join(exec.queries, "\n"); strings.Contains(got, "NOT EXISTS") {
		t.Fatalf("metadata loading should refresh tables explicitly instead of relying on hidden session cache, queries:\n%s", got)
	}
	if got, want := tableLoads.Load(), int32(2); got != want {
		t.Fatalf("REST table loads = %d, want %d", got, want)
	}
	insert := strings.Join(exec.execs, "\n")
	for _, want := range []string{
		"DELETE FROM memory.main.__duckgres_iceberg_column_metadata",
		"INSERT INTO memory.main.__duckgres_iceberg_column_metadata",
		"'public_api_keys'",
		"'id'",
		"'NO'",
		// data_type is normalized to the canonical PostgreSQL type name:
		// Iceberg "string" -> "text", "decimal(10,2)" -> "numeric" (with
		// precision/scale carried in their own columns).
		"'text'",
		"'amount'",
		"'numeric'",
		"10",
		"2",
		"'billing_productseat'",
		"'active'",
		"'boolean'",
	} {
		if !strings.Contains(insert, want) {
			t.Fatalf("insert missing %q in:\n%s", want, insert)
		}
	}
	if strings.Contains(insert, "INSERT OR IGNORE") {
		t.Fatalf("metadata loading should replace rows, not ignore refreshes:\n%s", insert)
	}
}

func TestLoadColumnsReturnsLakekeeperRESTUnavailableError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{{"billing_public", "public_api_keys"}}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'", Config{
		LakekeeperEndpoint:  srv.URL,
		LakekeeperWarehouse: "org-acme",
	})
	if err == nil {
		t.Fatal("expected REST unavailable error")
	}
	if !errors.Is(err, errRESTCatalogUnavailable) {
		t.Fatalf("error = %v, want errRESTCatalogUnavailable", err)
	}
	if got := strings.Join(exec.queries, "\n"); strings.Contains(got, "DESCRIBE SELECT") {
		t.Fatalf("REST-only metadata path should not describe Iceberg tables, queries:\n%s", got)
	}
}

func TestLoadColumnsPropagatesMalformedLakekeeperRESTMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse-id"}}`))
		case "/v1/warehouse-id/namespaces/billing_public/tables/public_api_keys":
			_, _ = w.Write([]byte(`{"metadata":{"current-schema-id": 7, "schemas":[]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{{"billing_public", "public_api_keys"}}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'", Config{
		LakekeeperEndpoint:  srv.URL,
		LakekeeperWarehouse: "org-acme",
	})
	if err == nil {
		t.Fatal("expected malformed REST metadata error")
	}
	if !strings.Contains(err.Error(), "current schema") {
		t.Fatalf("error = %v, want current schema context", err)
	}
	if got := strings.Join(exec.queries, "\n"); strings.Contains(got, "DESCRIBE SELECT") {
		t.Fatalf("malformed REST metadata should not silently fall back to DESCRIBE, queries:\n%s", got)
	}
}

func TestLoadColumnsSkipsOAuth2ConfiguredLakekeeperREST(t *testing.T) {
	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{{"billing_public", "public_api_keys"}}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'", Config{
		LakekeeperEndpoint:        "http://lakekeeper.invalid/catalog",
		LakekeeperWarehouse:       "org-acme",
		LakekeeperOAuth2ServerURI: "http://127.0.0.1:9876/token",
	})
	if err != nil {
		t.Fatalf("LoadColumns: %v", err)
	}
	if len(exec.queries) != 0 {
		t.Fatalf("OAuth2 metadata loading should skip before catalog queries, got:\n%s", strings.Join(exec.queries, "\n"))
	}
	if len(exec.execs) != 0 {
		t.Fatalf("OAuth2 metadata loading should not insert columns, got:\n%s", strings.Join(exec.execs, "\n"))
	}
}

func TestRestCatalogMetadataSourceLimitsConcurrency(t *testing.T) {
	const tableCount = restCatalogConcurrency + 3
	var active atomic.Int32
	var maxActive atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse-id"}}`))
		case strings.HasPrefix(r.URL.Path, "/v1/warehouse-id/namespaces/billing_public/tables/t_"):
			now := active.Add(1)
			for {
				prev := maxActive.Load()
				if now <= prev || maxActive.CompareAndSwap(prev, now) {
					break
				}
			}
			defer active.Add(-1)
			_, _ = w.Write([]byte(`{
				"metadata": {
					"current-schema-id": 0,
					"schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "int", "required": false}]}]
				}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	tables := make([]tableRef, tableCount)
	for i := range tables {
		tables[i] = tableRef{Schema: "billing_public", Name: fmt.Sprintf("t_%02d", i)}
	}

	cols, err := restCatalogMetadataSource{endpoint: srv.URL}.LoadColumns(context.Background(), "org-acme", tables)
	if err != nil {
		t.Fatalf("LoadColumns: %v", err)
	}
	if got, want := len(cols), tableCount; got != want {
		t.Fatalf("loaded tables = %d, want %d", got, want)
	}
	if got := maxActive.Load(); got > restCatalogConcurrency {
		t.Fatalf("max concurrency = %d, want <= %d", got, restCatalogConcurrency)
	}
}

type scriptedExecutor struct {
	rows    []sqlcore.RowSet
	queries []string
	execs   []string
}

func (e *scriptedExecutor) QueryContext(_ context.Context, query string, _ ...any) (sqlcore.RowSet, error) {
	e.queries = append(e.queries, query)
	if len(e.rows) == 0 {
		return nil, errors.New("no scripted rows")
	}
	rows := e.rows[0]
	e.rows = e.rows[1:]
	return rows, nil
}

func (e *scriptedExecutor) ExecContext(_ context.Context, query string, _ ...any) (sqlcore.ExecResult, error) {
	e.execs = append(e.execs, query)
	return nil, nil
}

func (e *scriptedExecutor) Query(string, ...any) (sqlcore.RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *scriptedExecutor) Exec(string, ...any) (sqlcore.ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *scriptedExecutor) ConnContext(context.Context) (sqlcore.RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *scriptedExecutor) PingContext(context.Context) error { return nil }
func (e *scriptedExecutor) Close() error                      { return nil }
func (e *scriptedExecutor) LastProfilingOutput() string       { return "" }

type rowSet struct {
	cols []string
	rows [][]any
	i    int
}

func (r *rowSet) Columns() ([]string, error) { return r.cols, nil }
func (r *rowSet) ColumnTypes() ([]sqlcore.ColumnTyper, error) {
	return nil, errors.New("not used")
}
func (r *rowSet) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *rowSet) Scan(dest ...any) error {
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
func (r *rowSet) Close() error { return nil }
func (r *rowSet) Err() error   { return nil }
