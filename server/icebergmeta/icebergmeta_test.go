package icebergmeta

import (
	"context"
	"errors"
	"strings"
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

func TestExtractFiltersHandlesEqualsAndInPredicates(t *testing.T) {
	f := ExtractFilters(`
		SELECT *
		FROM memory.main.information_schema_columns_compat c
		WHERE c.table_schema = 'billing_public'
		  AND c.table_name IN ('public_api_keys', 'billing_productseat')
	`)

	if got, want := strings.Join(f.Schemas, ","), "billing_public"; got != want {
		t.Fatalf("Schemas = %q, want %q", got, want)
	}
	if got, want := strings.Join(f.Tables, ","), "public_api_keys,billing_productseat"; got != want {
		t.Fatalf("Tables = %q, want %q", got, want)
	}
}

func TestLoadColumnsDescribesUnloadedIcebergTablesAndInsertsMetadata(t *testing.T) {
	exec := &scriptedExecutor{
		rows: []sqlcore.RowSet{
			&rowSet{cols: []string{"table_schema", "table_name"}, rows: [][]any{{"billing_public", "public_api_keys"}}},
			&rowSet{cols: []string{"column_name", "column_type", "null", "key", "default", "extra"}, rows: [][]any{
				{"id", "VARCHAR", "NO", nil, nil, nil},
				{"amount", "DECIMAL(10,2)", "YES", nil, nil, nil},
			}},
		},
	}

	err := LoadColumns(context.Background(), exec, "SELECT * FROM memory.main.information_schema_columns_compat WHERE table_schema = 'billing_public'")
	if err != nil {
		t.Fatalf("LoadColumns: %v", err)
	}

	joinedQueries := strings.Join(exec.queries, "\n---\n")
	for _, want := range []string{
		"FROM information_schema.tables",
		"table_catalog = 'iceberg'",
		"FROM memory.main.__duckgres_iceberg_column_metadata",
		"table_schema IN ('billing_public')",
		`DESCRIBE SELECT * FROM iceberg."billing_public"."public_api_keys" LIMIT 0`,
	} {
		if !strings.Contains(joinedQueries, want) {
			t.Fatalf("queries missing %q in:\n%s", want, joinedQueries)
		}
	}

	if len(exec.execs) != 1 {
		t.Fatalf("ExecContext calls = %d, want 1", len(exec.execs))
	}
	insert := exec.execs[0]
	for _, want := range []string{
		"INSERT OR IGNORE INTO memory.main.__duckgres_iceberg_column_metadata",
		"'billing_public'",
		"'public_api_keys'",
		"'id'",
		"'text'",
		"'amount'",
		"'numeric'",
		"10",
		"2",
	} {
		if !strings.Contains(insert, want) {
			t.Fatalf("insert missing %q in:\n%s", want, insert)
		}
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
