package icebergmeta

import "testing"

func TestCanonicalPGType(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		// Primitives
		{"boolean", "boolean"},
		{"int", "integer"},
		{"long", "bigint"},
		{"float", "real"},
		{"double", "double precision"},
		{"date", "date"},
		{"time", "time without time zone"},
		{"timestamp", "timestamp without time zone"},
		{"timestamptz", "timestamp with time zone"},
		{"timestamp_ns", "timestamp without time zone"},
		{"timestamptz_ns", "timestamp with time zone"},
		{"string", "text"},
		{"uuid", "uuid"},
		{"binary", "bytea"},
		// decimal -> numeric (precision/scale carried separately)
		{"decimal(10,2)", "numeric"},
		{"decimal(38, 9)", "numeric"},
		// fixed-length binary
		{"fixed[16]", "bytea"},
		{"fixed(16)", "bytea"},
		// nested / complex -> jsonb
		{`{"type":"struct","fields":[]}`, "jsonb"},
		{`{"type":"list","element":"int"}`, "jsonb"},
		{`{"type":"map","key":"string","value":"int"}`, "jsonb"},
		{"struct", "jsonb"},
		{"list", "jsonb"},
		{"map", "jsonb"},
		{"variant", "jsonb"},
		// case-insensitivity and whitespace
		{"  LONG  ", "bigint"},
		{"STRING", "text"},
		// empty / unknown
		{"", ""},
		{"someunknowntype", "someunknowntype"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := CanonicalPGType(tt.in); got != tt.want {
				t.Errorf("CanonicalPGType(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
