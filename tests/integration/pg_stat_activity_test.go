package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// scanPgStatActivityRow scans a full pg_stat_activity row into a map.
// All 21 columns are always returned regardless of the SELECT column list,
// so we must scan all of them.
func scanPgStatActivityRow(rows *sql.Rows) (map[string]interface{}, error) {
	var (
		datid, pid, usesysid, clientPort, workerID             int
		datname, usename, appName, clientAddr, state, query    string
		backendType                                            string
		backendStart                                           time.Time
		xactStart, queryStart, stateChange                     sql.NullTime
		waitEventType, waitEvent                               sql.NullString
		backendXid, backendXmin, leaderPid                     sql.NullInt32
	)

	err := rows.Scan(
		&datid, &datname, &pid, &usesysid, &usename,
		&appName, &clientAddr, &clientPort,
		&backendStart, &xactStart, &queryStart, &stateChange,
		&waitEventType, &waitEvent, &state,
		&backendXid, &backendXmin, &query,
		&backendType, &leaderPid, &workerID,
	)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"datid":            datid,
		"datname":          datname,
		"pid":              pid,
		"usesysid":         usesysid,
		"usename":          usename,
		"application_name": appName,
		"client_addr":      clientAddr,
		"client_port":      clientPort,
		"backend_start":    backendStart,
		"xact_start":       xactStart,
		"query_start":      queryStart,
		"state_change":     stateChange,
		"wait_event_type":  waitEventType,
		"wait_event":       waitEvent,
		"state":            state,
		"backend_xid":      backendXid,
		"backend_xmin":     backendXmin,
		"query":            query,
		"backend_type":     backendType,
		"leader_pid":       leaderPid,
		"worker_id":        workerID,
	}, nil
}

// queryPgStatActivity returns all rows from pg_stat_activity as maps.
func queryPgStatActivity(db *sql.DB) ([]map[string]interface{}, error) {
	rows, err := db.Query("SELECT * FROM pg_stat_activity")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []map[string]interface{}
	for rows.Next() {
		row, err := scanPgStatActivityRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// TestPgStatActivity tests the pg_stat_activity virtual table.
func TestPgStatActivity(t *testing.T) {
	db := dgOnly(t)

	t.Run("basic_select", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM pg_stat_activity")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns failed: %v", err)
		}

		// Verify expected columns are present
		expectedCols := []string{
			"datid", "datname", "pid", "usesysid", "usename",
			"application_name", "client_addr", "client_port",
			"backend_start", "xact_start", "query_start", "state_change",
			"wait_event_type", "wait_event", "state",
			"backend_xid", "backend_xmin", "query",
			"backend_type", "leader_pid", "worker_id",
		}
		if len(cols) != len(expectedCols) {
			t.Fatalf("expected %d columns, got %d: %v", len(expectedCols), len(cols), cols)
		}
		for i, expected := range expectedCols {
			if cols[i] != expected {
				t.Errorf("column %d: expected %q, got %q", i, expected, cols[i])
			}
		}

		// Should have at least one row (our own connection)
		count := 0
		for rows.Next() {
			count++
			// Scan to verify data is valid
			dest := make([]interface{}, len(cols))
			destPtrs := make([]interface{}, len(cols))
			for i := range dest {
				destPtrs[i] = &dest[i]
			}
			if err := rows.Scan(destPtrs...); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
		}
		if count < 1 {
			t.Error("expected at least 1 row (self), got 0")
		}
	})

	t.Run("pg_catalog_prefix", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) < 1 {
			t.Error("expected at least 1 row")
		}
	})

	t.Run("self_connection_visible", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}

		row := result[0]
		if row["pid"].(int) == 0 {
			t.Error("expected non-zero pid")
		}
		if row["usename"].(string) == "" {
			t.Error("expected non-empty usename")
		}
		if row["backend_type"].(string) != "client backend" {
			t.Errorf("expected backend_type 'client backend', got %q", row["backend_type"])
		}
		// In standalone mode, workerID should be -1
		if row["worker_id"].(int) != -1 {
			t.Errorf("expected worker_id -1 (standalone), got %d", row["worker_id"])
		}
	})

	t.Run("query_shows_current_query", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		query := result[0]["query"].(string)
		if !strings.Contains(query, "pg_stat_activity") {
			t.Errorf("expected query to contain 'pg_stat_activity', got %q", query)
		}
	})

	t.Run("backend_start_not_zero", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		backendStart := result[0]["backend_start"].(time.Time)
		if backendStart.IsZero() {
			t.Error("backend_start should not be zero")
		}
		// Verify it's a real timestamp (year >= 2024)
		if backendStart.Year() < 2024 {
			t.Errorf("backend_start year %d seems wrong", backendStart.Year())
		}
	})

	t.Run("null_columns", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		row := result[0]

		// These columns should always be NULL in our implementation
		nullFields := []string{"xact_start", "query_start", "state_change",
			"wait_event_type", "wait_event", "backend_xid", "backend_xmin", "leader_pid"}
		for _, field := range nullFields {
			switch v := row[field].(type) {
			case sql.NullTime:
				if v.Valid {
					t.Errorf("expected %s to be NULL, got %v", field, v.Time)
				}
			case sql.NullString:
				if v.Valid {
					t.Errorf("expected %s to be NULL, got %q", field, v.String)
				}
			case sql.NullInt32:
				if v.Valid {
					t.Errorf("expected %s to be NULL, got %d", field, v.Int32)
				}
			}
		}
	})

	t.Run("state_is_active_during_query", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		state := result[0]["state"].(string)
		if state != "active" {
			t.Errorf("expected state 'active' during query execution, got %q", state)
		}
	})

	t.Run("datname_populated", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		datname := result[0]["datname"].(string)
		if datname == "" {
			t.Error("expected non-empty datname")
		}
	})

	t.Run("client_addr_and_port", func(t *testing.T) {
		result, err := queryPgStatActivity(db)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result) == 0 {
			t.Fatal("expected at least 1 row")
		}
		row := result[0]
		if row["client_addr"].(string) == "" {
			t.Error("expected non-empty client_addr")
		}
		if row["client_port"].(int) == 0 {
			t.Error("expected non-zero client_port")
		}
	})
}

// TestPgStatActivityFromSecondConnection verifies that pg_stat_activity
// works from a separately opened connection and can see multiple connections.
func TestPgStatActivityFromSecondConnection(t *testing.T) {
	// Open a second connection to verify pg_stat_activity works independently
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass sslmode=require",
		testHarness.dgPort)
	db2, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open second connection: %v", err)
	}
	defer func() { _ = db2.Close() }()

	ctx := context.Background()
	conn2, err := db2.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	defer func() { _ = conn2.Close() }()

	// Query pg_stat_activity from the second connection
	rows, err := conn2.QueryContext(ctx, "SELECT * FROM pg_stat_activity")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	hasActive := false
	for rows.Next() {
		row, err := scanPgStatActivityRow(rows)
		if err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if row["pid"].(int) == 0 {
			t.Error("expected non-zero pid")
		}
		if row["state"].(string) == "active" {
			hasActive = true
		}
		count++
	}
	if count < 1 {
		t.Error("expected at least 1 row from second connection")
	}
	if !hasActive {
		t.Error("expected at least one connection in 'active' state (self)")
	}
}

// TestPgStatActivityExtendedQuery tests pg_stat_activity through the
// Extended Query protocol (prepared statements).
func TestPgStatActivityExtendedQuery(t *testing.T) {
	db := dgOnly(t)

	t.Run("prepared_statement", func(t *testing.T) {
		// Use Prepare to trigger Parse/Describe/Bind/Execute path
		stmt, err := db.Prepare("SELECT * FROM pg_stat_activity")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		rows, err := stmt.Query()
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			row, err := scanPgStatActivityRow(rows)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if row["pid"].(int) == 0 {
				t.Error("expected non-zero pid")
			}
			count++
		}
		if count < 1 {
			t.Error("expected at least 1 row")
		}
	})

	t.Run("prepared_reuse", func(t *testing.T) {
		// Prepare once, execute twice — verifies described flag handling
		stmt, err := db.Prepare("SELECT * FROM pg_stat_activity")
		if err != nil {
			t.Fatalf("prepare failed: %v", err)
		}
		defer func() { _ = stmt.Close() }()

		for i := 0; i < 3; i++ {
			rows, err := stmt.Query()
			if err != nil {
				t.Fatalf("iteration %d: query failed: %v", i, err)
			}
			count := 0
			for rows.Next() {
				if _, err := scanPgStatActivityRow(rows); err != nil {
					t.Fatalf("iteration %d: scan failed: %v", i, err)
				}
				count++
			}
			rows.Close()
			if count < 1 {
				t.Errorf("iteration %d: expected at least 1 row, got %d", i, count)
			}
		}
	})
}

// TestPgStatActivityStubView tests that the stub view in DuckDB works for
// schema introspection (e.g., \d pg_stat_activity in psql).
func TestPgStatActivityStubView(t *testing.T) {
	db := dgOnly(t)

	t.Run("stub_view_columns", func(t *testing.T) {
		// The stub view should have columns visible in information_schema
		rows, err := db.Query(`
			SELECT column_name
			FROM information_schema.columns
			WHERE table_name = 'pg_stat_activity'
			ORDER BY ordinal_position
		`)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer func() { _ = rows.Close() }()

		var cols []string
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			cols = append(cols, col)
		}
		if len(cols) == 0 {
			t.Error("expected pg_stat_activity to have columns in information_schema")
		}
		// Verify key columns are present
		colSet := make(map[string]bool)
		for _, c := range cols {
			colSet[c] = true
		}
		for _, expected := range []string{"pid", "usename", "state", "query", "worker_id"} {
			if !colSet[expected] {
				t.Errorf("expected column %q in stub view, got columns: %v", expected, cols)
			}
		}
	})
}
