package server

import "testing"

func TestParseStartupOptions(t *testing.T) {
	tests := []struct {
		name    string
		options string
		key     string
		want    string
	}{
		{"space-separated -c", "-c search_path=ducklake.main", "search_path", "ducklake.main"},
		{"combined -c", "-csearch_path=ducklake.main", "search_path", "ducklake.main"},
		{"double-dash", "--search_path=ducklake.main", "search_path", "ducklake.main"},
		{"multiple settings", "-c search_path=ducklake.main -c work_mem=64MB", "work_mem", "64MB"},
		{"escaped space in value", `-c search_path=ducklake.main,\ memory.main`, "search_path", "ducklake.main, memory.main"},
		{"empty", "", "search_path", ""},
		{"unrelated", "-c work_mem=64MB", "search_path", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseStartupOptions(tc.options)[tc.key]; got != tc.want {
				t.Fatalf("ParseStartupOptions(%q)[%q] = %q, want %q", tc.options, tc.key, got, tc.want)
			}
		})
	}
}

func TestSanitizeSearchPath(t *testing.T) {
	ok := []string{
		"ducklake.main",
		"ducklake.main, memory.main",
		`"ducklake"."main"`,
		"ducklake.main",
		"my_schema",
	}
	for _, s := range ok {
		if got, valid := SanitizeSearchPath(s); !valid || got != s {
			t.Errorf("SanitizeSearchPath(%q) = (%q, %v), want valid passthrough", s, got, valid)
		}
	}

	bad := []string{
		"",
		"ducklake.main'; DROP SCHEMA x; --",
		"ducklake.main; SELECT 1",
		"foo' || (SELECT 1)",
		"a(b)",
		// Standalone metacharacters: these guard that the `\-` in the allowlist
		// is an escaped literal hyphen, NOT a range (e.g. `"`..`\`) that would
		// silently admit ' and ; — the exact thing that would break out of the
		// single-quoted `SET search_path = '<value>'` statement.
		"'",
		";",
		`x',memory.main`,
		`a'b`,
	}
	for _, s := range bad {
		if _, valid := SanitizeSearchPath(s); valid {
			t.Errorf("SanitizeSearchPath(%q) accepted an unsafe value", s)
		}
	}
}
