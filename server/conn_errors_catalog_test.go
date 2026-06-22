package server

import (
	"errors"
	"testing"
)

// A native-fallback query that fails validation (e.g. `DESCRIBE x.y.z` against a
// missing schema/table — DESCRIBE is not valid PostgreSQL so it always takes the
// fallback-to-native path) used to be reported to the client as 42601
// (syntax_error). That mislabels a runtime catalog miss as a parse failure and
// breaks postgres-protocol clients that branch on the error class: a Dagster
// backfill probing `DESCRIBE ducklake.posthog.events` on a fresh, empty catalog
// caught only UndefinedTable (42P01) / InvalidSchemaName (3F000) and so never
// took its "table absent → CREATE SCHEMA + CREATE TABLE" path.
//
// The fix routes the validateWithDuckDB error through classifyErrorCode (the same
// classifier the execution path already uses), so the SQLSTATE matches what real
// Postgres returns. These cases lock that mapping in for both the raw DuckDB
// message and the Flight-wrapped form the control plane actually sees.
func TestCatalogMissDescribeClassifiesLikePostgres(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "missing schema, flight-wrapped (the production DESCRIBE failure)",
			err: errors.New("flight execute update: rpc error: code = InvalidArgument desc = failed to execute update: " +
				`Catalog Error: Table with name "posthog.events" does not exist because schema "posthog" does not exist.`),
			want: "3F000", // invalid_schema_name — schema clause wins, matching Postgres
		},
		{
			name: "missing schema, raw",
			err:  errors.New(`Catalog Error: schema "posthog" does not exist`),
			want: "3F000",
		},
		{
			name: "missing table on an existing schema",
			err:  errors.New(`Catalog Error: Table with name "events" does not exist!`),
			want: "42P01", // undefined_table
		},
		{
			name: "genuine parser error still classifies as syntax_error",
			err:  errors.New(`Parser Error: syntax error at or near "DEScRIBE"`),
			want: "42601",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyErrorCode(tc.err); got != tc.want {
				t.Errorf("classifyErrorCode = %q, want %q", got, tc.want)
			}
		})
	}
}
