package core

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Exercise the actual benchmark SQL against a small fixture with out-of-order
// funnel events, person snapshot duplicates, tenant collisions and boundary dates.
func TestCoverageCatalogSemantics(t *testing.T) {
	c, err := LoadCatalog(filepath.Join("..", "queries", "ducklake_posthog_coverage.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Queries) != 36 {
		t.Fatalf("got %d variants, want 12 complete triples", len(c.Queries))
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	_, err = db.Exec(`CREATE SCHEMA posthog;
 CREATE TABLE posthog.events(team_id BIGINT, person_id VARCHAR, event VARCHAR, timestamp TIMESTAMPTZ, uuid VARCHAR);
 CREATE TABLE posthog.persons(team_id BIGINT, id VARCHAR, is_identified BOOLEAN);
 INSERT INTO posthog.persons VALUES (1,'a',true),(1,'a',true),(1,'b',false),(2,'a',false);
 INSERT INTO posthog.events VALUES
 (1,'a','$autocapture','2026-03-17 00:00:00+00','1'),
 (1,'a','$pageview','2026-03-17 00:10:00+00','2'),
 (1,'a','$autocapture','2026-03-17 00:20:00+00','3'),
 (1,'a','$pageview','2026-03-26 00:00:00+00','4'),
 (1,'b','$pageview','2026-03-18 00:00:00+00','5'),
 (2,'a','$pageview','2026-03-18 00:00:00+00','6'),
 (1,NULL,'other','2026-03-18 00:00:00+00','7'),
 (1,'c','$autocapture','2026-03-17 00:00:00+00','8'),
 (1,'c','$pageview','2026-03-17 00:40:00+00','9');`)
	if err != nil {
		t.Fatal(err)
	}
	validation, err := os.ReadFile(filepath.Join("..", "..", "mw-dev", "scenario", "sql", "validate_posthog_coverage.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(validation)); err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
	defer func() {
		if _, err := db.Exec("DELETE FROM posthog.persons"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(validation)); err == nil || !strings.Contains(err.Error(), "overlap") {
			t.Fatalf("missing join population must fail: %v", err)
		}
	}()

	expected := map[string][]string{
		"q_coverage_daily_uniques_v1":          {"2026-03-17 2", "2026-03-18 2", "2026-03-26 1"},
		"q_coverage_event_trends_v1":           {"2026-03-17 $autocapture 3", "2026-03-17 $pageview 2", "2026-03-18 $pageview 2", "2026-03-26 $pageview 1"},
		"q_coverage_person_enrichment_v1":      {"false 2", "true 3"},
		"q_coverage_person_activity_join_v1":   {"3 5"},
		"q_coverage_recent_events_v1":          {"7 other <nil>", "6 $pageview a", "5 $pageview b", "9 $pageview c", "3 $autocapture a", "2 $pageview a", "8 $autocapture c", "1 $autocapture a"},
		"q_coverage_active_actors_v1":          {"1 a 3", "1 c 2", "1 b 1", "2 a 1"},
		"q_coverage_ordered_funnel_v1":         {"4 1"},
		"q_coverage_weekly_retention_v1":       {"4 1"},
		"q_coverage_repeat_activity_cohort_v1": {"1"},
		"q_coverage_exclusion_cohort_v1":       {"2"},
		"q_coverage_daily_person_model_v1":     {"4 7"},
		"q_coverage_session_model_v1":          {"5 7"},
	}
	for i := 0; i < len(c.Queries); i += 3 {
		q := c.Queries[i]
		base := q.QueryID[:len(q.QueryID)-len("__ducklake_table")]
		t.Run(base, func(t *testing.T) {
			want, ok := expected[base]
			if !ok {
				t.Fatalf("missing semantic expectation for %s", base)
			}
			rows, err := db.Query(q.PGWireSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := rows.Close(); err != nil {
					t.Errorf("close rows: %v", err)
				}
			}()
			cols, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for rows.Next() {
				values := make([]any, len(cols))
				pointers := make([]any, len(cols))
				for j := range values {
					pointers[j] = &values[j]
				}
				if err := rows.Scan(pointers...); err != nil {
					t.Fatal(err)
				}
				line := ""
				for j, v := range values {
					if j > 0 {
						line += " "
					}
					line += fmt.Sprint(v)
				}
				got = append(got, line)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}
