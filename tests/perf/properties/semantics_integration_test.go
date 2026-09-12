package properties

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/tests/perf/core"
)

// This in-memory edge-case corpus exercises the actual workload SQL. It does
// not write Parquet or replace the separate published-fixture correctness gate.
func TestCatalogPropertySemantics(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`CREATE SCHEMA properties_perf`,
		`CREATE TABLE properties_perf.events_supported (event VARCHAR, timestamp TIMESTAMPTZ, properties VARCHAR, properties_typed STRUCT("$browser" VARCHAR))`,
		`INSERT INTO properties_perf.events_supported VALUES
   ('start', '2026-03-17 00:00:00+00', '{"$browser":"Chrome"}', {'$browser':'Chrome'}),
   ('start', '2026-03-17 23:59:59.999999+00', '{"$browser":"Chrome"}', {'$browser':'Chrome'}),
   ('other', '2026-03-17 12:00:00+00', '{"$browser":"chrome"}', {'$browser':'chrome'}),
   ('other', '2026-03-17 12:00:00+00', '{"$browser":"A\"B\\C"}', {'$browser':'A"B\C'}),
   ('other', '2026-03-17 12:00:00+00', '{"$browser":"null"}', {'$browser':'null'}),
   ('other', '2026-03-17 12:00:00+00', '{"$browser":""}', {'$browser':''}),
   ('missing', '2026-03-17 12:00:00+00', '{}', {'$browser':NULL}),
   ('jsonnull', '2026-03-17 12:00:00+00', '{"$browser":null}', {'$browser':NULL}),
   ('sqlnull', '2026-03-17 12:00:00+00', NULL, NULL),
   ('before', '2026-03-16 23:59:59.999999+00', '{"$browser":"Chrome"}', {'$browser':'Chrome'}),
   ('after', '2026-03-18 00:00:00+00', '{"$browser":"Chrome"}', {'$browser':'Chrome'})`,
		`CREATE TABLE properties_perf.events_variant AS SELECT *, CAST(CAST(properties AS JSON) AS VARIANT) AS properties_variant FROM properties_perf.events_supported`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	c := Catalog(&Manifest{Start: time.Date(2026, 3, 17, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC)})
	for _, zone := range []string{"UTC", "America/Toronto"} {
		if _, err := db.Exec("SET TimeZone = '" + zone + "'"); err != nil {
			t.Fatal(err)
		}
		for _, q := range c.Queries {
			t.Run(zone+"/"+q.QueryID, func(t *testing.T) {
				sqlText, err := q.SQLFor(core.ProtocolPGWire)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := db.QueryContext(context.Background(), sqlText)
				if err != nil {
					t.Fatal(err)
				}
				got, err := core.ReadSQLResults(rows)
				if err != nil {
					t.Fatal(err)
				}
				expected := [][]*string{{semanticsCell("Chrome"), semanticsCell("2")}, {semanticsCell(""), semanticsCell("1")}, {semanticsCell("A\"B\\C"), semanticsCell("1")}, {semanticsCell("chrome"), semanticsCell("1")}, {semanticsCell("null"), semanticsCell("1")}}
				if strings.Contains(q.IntentID, "chrome") {
					expected = [][]*string{{semanticsCell("start"), semanticsCell("2")}}
				}
				if !reflect.DeepEqual(got, expected) {
					t.Fatalf("results differ: got %s want %s", semanticsValues(got), semanticsValues(expected))
				}
			})
		}
	}
}
func semanticsCell(s string) *string { return &s }
func semanticsValues(rows [][]*string) [][]string {
	result := make([][]string, len(rows))
	for i, row := range rows {
		result[i] = make([]string, len(row))
		for j, cell := range row {
			if cell == nil {
				result[i][j] = "<SQL NULL>"
			} else {
				result[i][j] = *cell
			}
		}
	}
	return result
}

func TestFixtureValidationRejectsIncompatibleBrowserTypes(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	m := &Manifest{Rows: 1}
	m.Config.Start = "2026-03-17T00:00:00Z"
	m.Config.End = "2026-03-18T00:00:00Z"
	if _, err := db.Exec(`CREATE SCHEMA properties_perf`); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"events_supported", "events_variant"} {
		if _, err := db.Exec(`CREATE TABLE properties_perf.` + table + ` (properties VARCHAR, timestamp TIMESTAMPTZ)`); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, properties string
		nulls, chrome    int64
		bad              bool
	}{
		{"missing", "{}", 1, 0, false}, {"json null", `{"$browser":null}`, 1, 0, false},
		{"string", `{"$browser":"Chrome"}`, 0, 1, false}, {"case", `{"$browser":"chrome"}`, 0, 0, false},
		{"escaped", `{"$browser":"A\"B\\C"}`, 0, 0, false},
		{"number", `{"$browser":42}`, 0, 0, true}, {"boolean", `{"$browser":true}`, 0, 0, true},
		{"array", `{"$browser":[]}`, 0, 0, true}, {"object", `{"$browser":{}}`, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.Coverage.BrowserNullRows = tc.nulls
			m.Coverage.ChromeRows = tc.chrome
			for _, table := range []string{"events_supported", "events_variant"} {
				if _, err := db.Exec(`DELETE FROM properties_perf.` + table); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO properties_perf.`+table+` VALUES (?,TIMESTAMPTZ '2026-03-17 12:00:00+00')`, tc.properties); err != nil {
					t.Fatal(err)
				}
			}
			_, err := db.Exec(m.ValidationSQL())
			if (err != nil) != tc.bad {
				t.Fatalf("validation error=%v, expected rejection=%v", err, tc.bad)
			}
		})
	}
}
