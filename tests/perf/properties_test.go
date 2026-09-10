package perf

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/tests/perf/core"
)

// Execute the checked-in workload, not copies of its SQL, on synthetic data.
func TestPostHogPropertiesQueries(t *testing.T) {
	catalog, err := core.LoadCatalog("queries/ducklake_posthog_tables.yaml")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
CREATE SCHEMA posthog;
CREATE SCHEMA frozen_v1;
CREATE TABLE posthog.events(event VARCHAR, properties VARCHAR, timestamp TIMESTAMPTZ);
INSERT INTO posthog.events VALUES
('pageview', '{"$browser":"Chrome"}', '2026-03-01 00:00:00+00'),
('pageview', '{"$browser":"Chrome"}', '2026-03-01 12:00:00+00'),
('signup', '{"$browser":"Chrome"}', '2026-03-01 23:59:59+00'),
('pageview', '{"$browser":"Firefox"}', '2026-03-01 12:00:00+00'),
('pageview', '{"$browser":"Browser \"Preview\""}', '2026-03-01 12:00:00+00'),
('missing', '{}', '2026-03-01 12:00:00+00'),
('json_null', '{"$browser":null}', '2026-03-01 12:00:00+00'),
('sql_null', NULL, '2026-03-01 12:00:00+00'),
('too_early', '{"$browser":"Chrome"}', '2026-02-28 23:59:59+00'),
('too_late', '{"$browser":"Chrome"}', '2026-03-02 00:00:00+00');
CREATE VIEW frozen_v1.events_file_view AS SELECT * FROM posthog.events;
CREATE VIEW main.events AS SELECT * FROM posthog.events;`)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		Name  string
		Count int64
	}
	wants := map[string][]result{
		"q_events_by_browser_one_day_balanced_v4":     {{"Chrome", 3}, {`Browser "Preview"`, 1}, {"Firefox", 1}},
		"q_events_by_name_chrome_one_day_balanced_v4": {{"pageview", 2}, {"signup", 1}},
	}
	seen := 0
	for _, query := range catalog.Queries {
		base := strings.Split(query.QueryID, "__")[0]
		want, ok := wants[base]
		if !ok {
			continue
		}
		seen++
		t.Run(query.QueryID, func(t *testing.T) {
			rows, err := db.Query(query.CanonicalSQL())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var got []result
			for rows.Next() {
				var value result
				if err := rows.Scan(&value.Name, &value.Count); err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
	if seen != 6 {
		t.Fatalf("executed %d property query variants, want 6", seen)
	}
}
