package server

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server/chsql"
)

func TestClickHouseMacros(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	chsql.InitMacros(db)

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"toString", "SELECT toString(42)", "42"},
		{"toInt32", "SELECT toInt32('123')", "123"},
		{"toInt64", "SELECT toInt64('9999999999')", "9999999999"},
		{"toFloat", "SELECT toFloat('3.14')", "3.14"},
		{"toInt32OrNull", "SELECT toInt32OrNull('abc')", ""},
		{"toInt32OrZero", "SELECT toInt32OrZero('abc')", "0"},
		{"intDiv", "SELECT intDiv(10, 3)", "3"},
		{"modulo", "SELECT modulo(10, 3)", "1"},
		{"empty_true", "SELECT empty('')", "true"},
		{"empty_false", "SELECT empty('hi')", "false"},
		{"notEmpty", "SELECT notEmpty('hi')", "true"},
		{"lengthUTF8", "SELECT lengthUTF8('hello')", "5"},
		{"toYear", "SELECT toYear(DATE '2024-06-15')", "2024"},
		{"toMonth", "SELECT toMonth(DATE '2024-06-15')", "6"},
		{"toDayOfMonth", "SELECT toDayOfMonth(DATE '2024-06-15')", "15"},
		{"toYYYYMMDD", "SELECT toYYYYMMDD(DATE '2024-06-15')", "20240615"},
		{"toYYYYMM", "SELECT toYYYYMM(DATE '2024-06-15')", "202406"},
		{"protocol", "SELECT protocol('https://duckdb.org/docs')", "https"},
		{"protocol_empty", "SELECT protocol('no-scheme')", ""},
		{"domain", "SELECT domain('https://duckdb.org/docs')", "duckdb.org"},
		{"topLevelDomain", "SELECT topLevelDomain('https://duckdb.org/docs')", "org"},
		{"IPv4NumToString", "SELECT IPv4NumToString(3232235777)", "192.168.1.1"},
		{"JSONExtractString", `SELECT JSONExtractString('{"a":"hello"}', '$.a')`, `"hello"`},
		{"JSONHas_true", `SELECT JSONHas('{"a":1}', '$.a')`, "true"},
		{"JSONHas_false", `SELECT JSONHas('{"a":1}', '$.b')`, "false"},
		{"ifNull", "SELECT ifNull(NULL, 42)", "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRow(tt.query).Scan(&got); err != nil {
				t.Fatalf("query %q failed: %v", tt.query, err)
			}
			actual := ""
			if got.Valid {
				actual = got.String
			}
			if actual != tt.want {
				t.Errorf("got %q, want %q", actual, tt.want)
			}
		})
	}
}

func TestClickHouseMacros_SplitByChar(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	chsql.InitMacros(db)

	// splitByChar returns a list; verify via array_length
	var got int
	if err := db.QueryRow("SELECT array_length(splitByChar(',', 'a,b,c'))").Scan(&got); err != nil {
		t.Fatalf("splitByChar query failed: %v", err)
	}
	if got != 3 {
		t.Errorf("splitByChar(',' , 'a,b,c') length = %d, want 3", got)
	}
}

func TestClickHouseMacros_GenerateUUIDv4(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	chsql.InitMacros(db)

	var got string
	if err := db.QueryRow("SELECT CAST(generateUUIDv4() AS VARCHAR)").Scan(&got); err != nil {
		t.Fatalf("generateUUIDv4 query failed: %v", err)
	}
	// UUID v4 format: 8-4-4-4-12 hex chars
	if len(got) != 36 {
		t.Errorf("generateUUIDv4() returned %q (len %d), expected UUID format", got, len(got))
	}
}
