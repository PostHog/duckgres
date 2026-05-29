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
		{name: "unquoted lowercases", input: "ICEBERG.Fivetran_Testing", want: []string{"iceberg", "fivetran_testing"}, wantOK: true},
		{name: "quoted preserves case", input: `"iceberg"."MixedSchema"`, want: []string{"iceberg", "MixedSchema"}, wantOK: true},
		{name: "escaped quote", input: `"iceberg"."quote""schema"`, want: []string{"iceberg", `quote"schema`}, wantOK: true},
		{name: "empty part", input: "iceberg..schema", wantOK: false},
		{name: "unterminated quote", input: `"iceberg"."schema`, wantOK: false},
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
		{name: "qualified catalog first", searchPath: "iceberg.public,memory.main", want: "iceberg"},
		{name: "quoted qualified catalog first", searchPath: `"iceberg"."public", memory.main`, want: "iceberg"},
		{name: "escaped comma inside quoted schema", searchPath: `"iceberg"."pub,lic", memory.main`, want: "iceberg"},
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
