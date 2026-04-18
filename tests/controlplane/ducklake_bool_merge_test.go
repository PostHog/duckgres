//go:build linux || darwin

package controlplane_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	integration "github.com/posthog/duckgres/tests/integration"
)

const (
	duckLakeMetadataPort = 35433
	duckLakeMinIOPort    = 39000
)

func ensureDuckLakeInfra(t *testing.T) {
	t.Helper()

	if integration.IsDuckLakeInfraRunning(duckLakeMetadataPort, duckLakeMinIOPort) {
		return
	}
	if err := integration.StartDuckLakeInfraContainers(); err != nil {
		t.Fatalf("StartDuckLakeInfraContainers: %v", err)
	}
	t.Cleanup(func() {
		if err := integration.StopPostgresContainer(); err != nil {
			t.Fatalf("StopPostgresContainer: %v", err)
		}
	})
	if err := integration.WaitForDuckLakeInfra(duckLakeMetadataPort, duckLakeMinIOPort, 30*time.Second); err != nil {
		t.Fatalf("WaitForDuckLakeInfra: %v", err)
	}
}

func queryIntPairs(t *testing.T, db *sql.DB, query string) [][2]int {
	t.Helper()

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var pairs [][2]int
	for rows.Next() {
		var left, right int
		if err := rows.Scan(&left, &right); err != nil {
			t.Fatalf("Scan(%q): %v", query, err)
		}
		pairs = append(pairs, [2]int{left, right})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows(%q): %v", query, err)
	}
	return pairs
}

func queryInts(t *testing.T, db *sql.DB, query string) []int {
	t.Helper()

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var values []int
	for rows.Next() {
		var value int
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan(%q): %v", query, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows(%q): %v", query, err)
	}
	return values
}

func TestDuckLakeBooleanPredicatesAndMergeInControlPlane(t *testing.T) {
	ensureDuckLakeInfra(t)

	h := startControlPlane(t, cpOpts{
		duckLake:             true,
		duckLakeMetadataPort: duckLakeMetadataPort,
		duckLakeMinIOPort:    duckLakeMinIOPort,
	})
	db := h.openConn(t)

	setup := []string{
		"DROP SCHEMA IF EXISTS merge_debug CASCADE",
		"CREATE SCHEMA merge_debug",
		"CREATE TABLE merge_debug.sub_tgt(id INT, val INT)",
		"CREATE TABLE merge_debug.sub_data(id INT, val INT, active BOOLEAN)",
		"INSERT INTO merge_debug.sub_tgt VALUES (1, 10)",
		"INSERT INTO merge_debug.sub_data VALUES (1, 100, true), (2, 20, true), (3, 30, false), (4, 40, NULL)",
	}
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Exec(%q): %v", stmt, err)
		}
	}

	boolCases := []struct {
		name  string
		query string
		want  []int
	}{
		{
			name:  "equals true",
			query: "SELECT id FROM merge_debug.sub_data WHERE active = true ORDER BY id",
			want:  []int{1, 2},
		},
		{
			name:  "equals false",
			query: "SELECT id FROM merge_debug.sub_data WHERE active = false ORDER BY id",
			want:  []int{3},
		},
		{
			name:  "not equals true",
			query: "SELECT id FROM merge_debug.sub_data WHERE active != true ORDER BY id",
			want:  []int{3},
		},
		{
			name:  "not equals false",
			query: "SELECT id FROM merge_debug.sub_data WHERE active != false ORDER BY id",
			want:  []int{1, 2},
		},
		{
			name:  "subquery equals true",
			query: "SELECT id FROM (SELECT id FROM merge_debug.sub_data WHERE active = true) s ORDER BY id",
			want:  []int{1, 2},
		},
		{
			name:  "not of not equals true excludes null",
			query: "SELECT id FROM merge_debug.sub_data WHERE NOT (active != true) ORDER BY id",
			want:  []int{1, 2},
		},
		{
			name:  "case when condition rewrites",
			query: "SELECT id FROM merge_debug.sub_data WHERE CASE WHEN active = true THEN true ELSE false END ORDER BY id",
			want:  []int{1, 2},
		},
	}

	for _, tc := range boolCases {
		t.Run(tc.name, func(t *testing.T) {
			got := queryInts(t, db, tc.query)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("Query(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}

	mergeSubquery := `
MERGE INTO merge_debug.sub_tgt AS t
USING (SELECT id, val FROM merge_debug.sub_data WHERE active = true) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)
`
	if _, err := db.Exec(mergeSubquery); err != nil {
		t.Fatalf("Exec(merge subquery): %v", err)
	}
	if got, want := queryIntPairs(t, db, "TABLE merge_debug.sub_tgt ORDER BY id"), [][2]int{{1, 100}, {2, 20}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("subquery MERGE result = %v, want %v", got, want)
	}

	if _, err := db.Exec("TRUNCATE merge_debug.sub_tgt"); err != nil {
		t.Fatalf("TRUNCATE sub_tgt: %v", err)
	}
	if _, err := db.Exec("INSERT INTO merge_debug.sub_tgt VALUES (1, 10)"); err != nil {
		t.Fatalf("reseed sub_tgt: %v", err)
	}

	mergeCTE := `
MERGE INTO merge_debug.sub_tgt AS t
USING (
	WITH active_rows AS (
		SELECT id, val FROM merge_debug.sub_data WHERE active = true
	)
	SELECT id, val FROM active_rows
) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)
`
	if _, err := db.Exec(mergeCTE); err != nil {
		t.Fatalf("Exec(merge cte): %v", err)
	}
	if got, want := queryIntPairs(t, db, "TABLE merge_debug.sub_tgt ORDER BY id"), [][2]int{{1, 100}, {2, 20}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("cte MERGE result = %v, want %v", got, want)
	}

	if _, err := db.Exec("TRUNCATE merge_debug.sub_tgt"); err != nil {
		t.Fatalf("TRUNCATE sub_tgt before base MERGE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO merge_debug.sub_tgt VALUES (1, 10)"); err != nil {
		t.Fatalf("reseed sub_tgt before base MERGE: %v", err)
	}

	mergeBaseTable := `
MERGE INTO merge_debug.sub_tgt AS t
USING merge_debug.sub_data AS s
ON t.id = s.id
WHEN MATCHED AND s.active IS TRUE THEN UPDATE SET val = s.val
WHEN NOT MATCHED AND s.active IS TRUE THEN INSERT VALUES (s.id, s.val)
`
	if _, err := db.Exec(mergeBaseTable); err != nil {
		t.Fatalf("Exec(base table MERGE): %v", err)
	}
	if got, want := queryIntPairs(t, db, "TABLE merge_debug.sub_tgt ORDER BY id"), [][2]int{{1, 100}, {2, 20}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("base table MERGE result = %v, want %v", got, want)
	}
}
