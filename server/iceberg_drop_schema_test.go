package server

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/sqlcore"
)

func TestParseDropSchemaCascadeTarget(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		want   dropSchemaCascadeTarget
		wantOK bool
	}{
		{
			name:   "qualified iceberg schema",
			query:  "DROP SCHEMA IF EXISTS iceberg.fivetran_testing_schema_abc CASCADE",
			wantOK: true,
			want: dropSchemaCascadeTarget{
				Catalog: "iceberg",
				Schema:  "fivetran_testing_schema_abc",
			},
		},
		{
			name:   "unqualified schema",
			query:  "DROP SCHEMA fivetran_testing_schema_abc CASCADE",
			wantOK: true,
			want: dropSchemaCascadeTarget{
				Schema: "fivetran_testing_schema_abc",
			},
		},
		{
			name:   "quoted identifiers",
			query:  `DROP SCHEMA IF EXISTS "iceberg"."MixedSchema" CASCADE`,
			wantOK: true,
			want: dropSchemaCascadeTarget{
				Catalog: "iceberg",
				Schema:  "MixedSchema",
			},
		},
		{
			name:   "unquoted identifiers normalize",
			query:  `/*Fivetran*/ DROP SCHEMA IF EXISTS ICEBERG.Fivetran_Testing_Schema_ABC CASCADE;`,
			wantOK: true,
			want: dropSchemaCascadeTarget{
				Catalog: "iceberg",
				Schema:  "fivetran_testing_schema_abc",
			},
		},
		{
			name:   "not cascade",
			query:  "DROP SCHEMA IF EXISTS iceberg.foo",
			wantOK: false,
		},
		{
			name:   "drop table ignored",
			query:  "DROP TABLE iceberg.foo.bar CASCADE",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseDropSchemaCascadeTarget(tt.query)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if got != tt.want {
				t.Fatalf("target = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestIsIcebergDropSchemaCascadeUnsupported(t *testing.T) {
	err := errors.New("flight execute update: rpc error: code = InvalidArgument desc = failed to execute update: Not implemented Error: DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently")
	if !isIcebergDropSchemaCascadeUnsupported(err) {
		t.Fatal("expected Iceberg DROP SCHEMA CASCADE unsupported error")
	}
	if isIcebergDropSchemaCascadeUnsupported(errors.New(`Catalog Error: Table with name "person" already exists`)) {
		t.Fatal("table already exists must not trigger schema cascade fallback")
	}
}

func TestDropIcebergSchemaCascadeDropsTablesThenSchema(t *testing.T) {
	exec := &icebergDropSchemaCascadeExecutor{
		tableNames: []string{`plain`, `quote"table`},
	}
	c := &clientConn{executor: exec}

	if _, err := c.dropIcebergSchemaCascade(context.Background(), `DROP SCHEMA IF EXISTS iceberg."fivetran testing" CASCADE`); err != nil {
		t.Fatalf("dropIcebergSchemaCascade returned error: %v", err)
	}

	want := []string{
		`DROP TABLE IF EXISTS "iceberg"."fivetran testing"."plain"`,
		`DROP TABLE IF EXISTS "iceberg"."fivetran testing"."quote""table"`,
		`DROP SCHEMA IF EXISTS "iceberg"."fivetran testing"`,
	}
	if !slices.Equal(exec.execQueries, want) {
		t.Fatalf("exec queries = %#v, want %#v", exec.execQueries, want)
	}
}

func TestDropIcebergSchemaCascadeUsesCurrentSearchPathForUnqualifiedSchema(t *testing.T) {
	exec := &icebergDropSchemaCascadeExecutor{
		searchPath: `"iceberg"."public",memory.main`,
		tableNames: []string{`person`},
	}
	c := &clientConn{executor: exec}

	if _, err := c.dropIcebergSchemaCascade(context.Background(), `DROP SCHEMA IF EXISTS stripe CASCADE`); err != nil {
		t.Fatalf("dropIcebergSchemaCascade returned error: %v", err)
	}

	want := []string{
		`DROP TABLE IF EXISTS "iceberg"."stripe"."person"`,
		`DROP SCHEMA IF EXISTS "iceberg"."stripe"`,
	}
	if !slices.Equal(exec.execQueries, want) {
		t.Fatalf("exec queries = %#v, want %#v", exec.execQueries, want)
	}
}

func TestDropIcebergSchemaCascadeRejectsNonIcebergSearchPath(t *testing.T) {
	exec := &icebergDropSchemaCascadeExecutor{
		searchPath: "ducklake.main,memory.main",
		tableNames: []string{`person`},
	}
	c := &clientConn{executor: exec}

	if _, err := c.dropIcebergSchemaCascade(context.Background(), `DROP SCHEMA IF EXISTS stripe CASCADE`); err == nil {
		t.Fatal("expected non-Iceberg search_path catalog to be rejected")
	}
	if len(exec.execQueries) != 0 {
		t.Fatalf("exec queries = %#v, want none", exec.execQueries)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	if got := sqlcore.QuoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("QuoteIdentifier = %q", got)
	}
}

type icebergDropSchemaCascadeExecutor struct {
	noopProfiling
	searchPath  string
	execQueries []string
	tableNames  []string
}

func (e *icebergDropSchemaCascadeExecutor) QueryContext(_ context.Context, query string, _ ...any) (RowSet, error) {
	if strings.Contains(query, "duckdb_settings()") {
		return &icebergDropSchemaCascadeRows{values: []string{e.searchPath}}, nil
	}
	return &icebergDropSchemaCascadeRows{values: e.tableNames}, nil
}

func (e *icebergDropSchemaCascadeExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	e.execQueries = append(e.execQueries, query)
	return &fakeExecResult{}, nil
}

func (e *icebergDropSchemaCascadeExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *icebergDropSchemaCascadeExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *icebergDropSchemaCascadeExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *icebergDropSchemaCascadeExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *icebergDropSchemaCascadeExecutor) Close() error {
	return nil
}

type icebergDropSchemaCascadeRows struct {
	values []string
	idx    int
}

func (r *icebergDropSchemaCascadeRows) Columns() ([]string, error) { return []string{"value"}, nil }
func (r *icebergDropSchemaCascadeRows) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("VARCHAR")}, nil
}
func (r *icebergDropSchemaCascadeRows) Next() bool {
	if r.idx >= len(r.values) {
		return false
	}
	r.idx++
	return true
}
func (r *icebergDropSchemaCascadeRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.values[r.idx-1]
	return nil
}
func (r *icebergDropSchemaCascadeRows) Close() error { return nil }
func (r *icebergDropSchemaCascadeRows) Err() error   { return nil }
