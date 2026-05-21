package server

import "testing"

func TestParseStartupOptions(t *testing.T) {
	tests := []struct {
		name    string
		options string
		key     string
		want    string
	}{
		{"space-separated -c", "-c search_path=iceberg.public", "search_path", "iceberg.public"},
		{"combined -c", "-csearch_path=iceberg.public", "search_path", "iceberg.public"},
		{"double-dash", "--search_path=iceberg.public", "search_path", "iceberg.public"},
		{"multiple settings", "-c search_path=iceberg.public -c work_mem=64MB", "work_mem", "64MB"},
		{"escaped space in value", `-c search_path=iceberg.public,\ memory.main`, "search_path", "iceberg.public, memory.main"},
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
		"iceberg.public",
		"iceberg.public, memory.main",
		`"iceberg"."public"`,
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
		"iceberg.public'; DROP SCHEMA x; --",
		"iceberg.public; SELECT 1",
		"foo' || (SELECT 1)",
		"a(b)",
	}
	for _, s := range bad {
		if _, valid := SanitizeSearchPath(s); valid {
			t.Errorf("SanitizeSearchPath(%q) accepted an unsafe value", s)
		}
	}
}
