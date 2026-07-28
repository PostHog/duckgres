package server

import (
	"database/sql"
	"strings"
	"testing"
)

// TestQueryLogRegistryGeneratesEveryColumnSurface is the anti-drift test. The
// column list used to be repeated in five places; these assertions fail if a
// generated surface stops agreeing with the registry.
func TestQueryLogRegistryGeneratesEveryColumnSurface(t *testing.T) {
	entryColumns := queryLogEntryColumns()
	if len(entryColumns) == 0 {
		t.Fatal("registry has no entry columns")
	}

	seen := make(map[string]struct{}, len(queryLogColumns))
	for _, column := range queryLogColumns {
		if column.Name == "" || column.PGType == "" {
			t.Fatalf("column %+v is missing a name or type", column)
		}
		if _, dup := seen[column.Name]; dup {
			t.Fatalf("column %q is registered twice", column.Name)
		}
		seen[column.Name] = struct{}{}
	}

	// INSERT argument count must match the INSERT column count, or values land
	// in the wrong columns.
	args := queryLogEntryInsertArgs(QueryLogEntry{})
	if len(args) != len(entryColumns) {
		t.Fatalf("insert args = %d, entry columns = %d", len(args), len(entryColumns))
	}

	createSQL := postgresQueryLogCreateTableSQL()
	viewSQL := duckLakeQueryLogViewSelectSQL()
	repairList := postgresQueryLogColumns
	for _, column := range queryLogColumns {
		if !strings.Contains(createSQL, column.Name+" "+column.PGType) {
			t.Fatalf("CREATE TABLE missing %q:\n%s", column.Name, createSQL)
		}
		if !strings.Contains(repairList, column.Name) {
			t.Fatalf("partition-repair column list missing %q: %s", column.Name, repairList)
		}
	}
	for _, name := range queryLogEntryColumnNames() {
		if !strings.Contains(viewSQL, "\n\t"+name) {
			t.Fatalf("DuckLake view SELECT missing %q:\n%s", name, viewSQL)
		}
	}

	// Table-managed columns are copied by the repair path but never written by
	// duckgres and never exposed in the view.
	if strings.Contains(viewSQL, "\n\tid") {
		t.Fatalf("DuckLake view must not expose the identity column:\n%s", viewSQL)
	}
}

// TestQueryLogAppendedColumnsAreAddable pins the rule that makes the migration
// safe: a column appended after the initial schema reaches existing tenants
// through ALTER TABLE ... ADD COLUMN, and a bare NOT NULL column cannot be
// added to a populated table.
func TestQueryLogAppendedColumnsAreAddable(t *testing.T) {
	if len(queryLogColumns) < queryLogInitialSchemaColumns {
		t.Fatalf("queryLogInitialSchemaColumns = %d exceeds the registry size %d",
			queryLogInitialSchemaColumns, len(queryLogColumns))
	}
	for _, column := range queryLogColumns[queryLogInitialSchemaColumns:] {
		upper := strings.ToUpper(column.PGType)
		if strings.Contains(upper, "NOT NULL") && !strings.Contains(upper, "DEFAULT") {
			t.Fatalf("appended column %q is NOT NULL without a DEFAULT; it cannot be added to a populated table",
				column.Name)
		}
		if strings.Contains(upper, "GENERATED") {
			t.Fatalf("appended column %q is an identity column; adding one rewrites the table", column.Name)
		}
	}
}

func TestPostgresQueryLogAddColumnSQLCoversAppendedColumns(t *testing.T) {
	stmts := strings.Join(postgresQueryLogAddColumnSQL(), "\n")

	for _, column := range queryLogColumns[queryLogInitialSchemaColumns:] {
		want := "ALTER TABLE querylog.query_log_entries ADD COLUMN IF NOT EXISTS " + column.Name + " " + column.PGType
		if !strings.Contains(stmts, want) {
			t.Fatalf("migration missing %q:\n%s", want, stmts)
		}
	}
	// Initial-schema columns ship in the CREATE TABLE and must not be re-added.
	for _, column := range queryLogColumns[:queryLogInitialSchemaColumns] {
		if strings.Contains(stmts, " "+column.Name+" ") {
			t.Fatalf("migration should not touch initial-schema column %q:\n%s", column.Name, stmts)
		}
	}
}

// TestPostgresQueryLogSchemaRunsMigrationBeforeIndexes keeps the ordering that
// lets a future index reference an appended column.
func TestPostgresQueryLogSchemaRunsMigrationBeforeIndexes(t *testing.T) {
	sqlText := strings.Join(postgresQueryLogBaseSchemaSQL(), "\n")
	migration := strings.Index(sqlText, "ADD COLUMN IF NOT EXISTS")
	firstIndex := strings.Index(sqlText, "CREATE INDEX")
	if migration < 0 {
		t.Fatalf("base schema has no column migration:\n%s", sqlText)
	}
	if firstIndex < 0 || migration > firstIndex {
		t.Fatalf("column migration must precede index creation:\n%s", sqlText)
	}
}

// --- shared DuckDB fixtures -------------------------------------------------
//
// The query-log tests back the sink with an in-memory DuckDB table standing in
// for tenant Postgres. Generating that fixture from the registry means adding a
// column stays a one-line diff instead of also editing every fixture.

// duckDBQueryLogColumnType maps a registry PG type to its DuckDB equivalent,
// dropping constraints and defaults the fixture does not need.
func duckDBQueryLogColumnType(pgType string) string {
	base := pgType
	for _, suffix := range []string{" NOT NULL", " DEFAULT", " GENERATED"} {
		if i := strings.Index(base, suffix); i >= 0 {
			base = base[:i]
		}
	}
	switch strings.TrimSpace(strings.ToUpper(base)) {
	case "TEXT":
		return "VARCHAR"
	case "DOUBLE PRECISION":
		return "DOUBLE"
	default:
		return strings.TrimSpace(base)
	}
}

// duckDBQueryLogTableSQL builds a DuckDB table matching the registry. When
// includeManaged is false the identity column is omitted, matching the columns
// the sink actually writes.
func duckDBQueryLogTableSQL(table string, includeManaged bool) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(table)
	sb.WriteString(" (")
	first := true
	for _, column := range queryLogColumns {
		if column.Arg == nil && !includeManaged {
			continue
		}
		if !first {
			sb.WriteByte(',')
		}
		first = false
		sb.WriteString("\n\t")
		sb.WriteString(column.Name)
		sb.WriteByte(' ')
		sb.WriteString(duckDBQueryLogColumnType(column.PGType))
	}
	sb.WriteString("\n)")
	return sb.String()
}

// insertQueryLogFixtureRow writes one entry through the registry's own argument
// order, so the fixture cannot disagree with the production INSERT.
func insertQueryLogFixtureRow(t *testing.T, db *sql.DB, table string, entry QueryLogEntry) {
	t.Helper()

	names := queryLogEntryColumnNames()
	placeholders := make([]string, len(names))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	stmt := "INSERT INTO " + table + " (" + strings.Join(names, ", ") + ") VALUES (" +
		strings.Join(placeholders, ", ") + ")"
	if _, err := db.Exec(stmt, queryLogEntryInsertArgs(entry)...); err != nil {
		t.Fatalf("insert query-log fixture row: %v", err)
	}
}
