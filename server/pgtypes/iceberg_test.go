package pgtypes

import "testing"

func TestForIceberg(t *testing.T) {
	tests := []struct {
		raw      string
		dataType string
		udtName  string
	}{
		{"long", "bigint", "int8"},
		{"string", "text", "text"},
		{"decimal(10,2)", "numeric", "numeric"},
		{`{"type":"struct","fields":[]}`, "jsonb", "jsonb"},
		{"fixed[16]", "bytea", "bytea"},
	}
	for _, tt := range tests {
		got := ForIceberg(tt.raw)
		if got.DataType != tt.dataType || got.UDTName != tt.udtName {
			t.Fatalf("ForIceberg(%q) = %#v, want data_type=%q udt_name=%q", tt.raw, got, tt.dataType, tt.udtName)
		}
	}
}
