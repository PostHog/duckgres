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
		{name: "unquoted lowercases", input: "DUCKLAKE.Fivetran_Testing", want: []string{"ducklake", "fivetran_testing"}, wantOK: true},
		{name: "quoted preserves case", input: `"ducklake"."MixedSchema"`, want: []string{"ducklake", "MixedSchema"}, wantOK: true},
		{name: "escaped quote", input: `"ducklake"."quote""schema"`, want: []string{"ducklake", `quote"schema`}, wantOK: true},
		{name: "empty part", input: "ducklake..schema", wantOK: false},
		{name: "unterminated quote", input: `"ducklake"."schema`, wantOK: false},
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
		{name: "qualified catalog first", searchPath: "ducklake.main,memory.main", want: "ducklake"},
		{name: "quoted qualified catalog first", searchPath: `"ducklake"."main", memory.main`, want: "ducklake"},
		{name: "escaped comma inside quoted schema", searchPath: `"ducklake"."ma,in", memory.main`, want: "ducklake"},
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
