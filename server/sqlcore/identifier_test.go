package sqlcore

import (
	"slices"
	"testing"
)

func TestParseQualifiedIdentifier(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   []string
		wantOK bool
	}{
		{name: "unquoted lowercases", input: "ANALYTICS.Fivetran_Testing", want: []string{"analytics", "fivetran_testing"}, wantOK: true},
		{name: "quoted preserves case", input: `"analytics"."MixedSchema"`, want: []string{"analytics", "MixedSchema"}, wantOK: true},
		{name: "escaped quote", input: `"analytics"."quote""schema"`, want: []string{"analytics", `quote"schema`}, wantOK: true},
		{name: "empty part", input: "analytics..schema", wantOK: false},
		{name: "unterminated quote", input: `"analytics"."schema`, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseQualifiedIdentifier(tt.input)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("parts = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestCatalogFromSearchPath(t *testing.T) {
	tests := []struct {
		name       string
		searchPath string
		want       string
	}{
		{name: "qualified catalog first", searchPath: "analytics.public,memory.main", want: "analytics"},
		{name: "quoted qualified catalog first", searchPath: `"analytics"."public", memory.main`, want: "analytics"},
		{name: "escaped comma inside quoted schema", searchPath: `"analytics"."pub,lic", memory.main`, want: "analytics"},
		{name: "unqualified first entry", searchPath: "main,memory.main", want: ""},
		{name: "empty", searchPath: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CatalogFromSearchPath(tt.searchPath); got != tt.want {
				t.Fatalf("CatalogFromSearchPath(%q) = %q, want %q", tt.searchPath, got, tt.want)
			}
		})
	}
}
