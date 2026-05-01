package arrowmap

import (
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDecimalValueString(t *testing.T) {
	tests := []struct {
		name  string
		value *big.Int
		scale int
		want  string
	}{
		{name: "scale_zero_positive", value: big.NewInt(42), scale: 0, want: "42"},
		{name: "scale_zero_negative", value: big.NewInt(-42), scale: 0, want: "-42"},
		{name: "typical_pi", value: big.NewInt(314159), scale: 5, want: "3.14159"},
		{name: "typical_pi_negative", value: big.NewInt(-314159), scale: 5, want: "-3.14159"},
		{name: "smaller_than_scale", value: big.NewInt(5), scale: 2, want: "0.05"},
		{name: "smaller_than_scale_negative", value: big.NewInt(-5), scale: 2, want: "-0.05"},
		{name: "exactly_one", value: big.NewInt(100), scale: 2, want: "1.00"},
		{name: "zero_value", value: big.NewInt(0), scale: 3, want: "0.000"},
		// Defensive: nil Value should not panic. Falls back to "0" — this
		// path shouldn't fire for normal scan results, but guarding it keeps
		// fmt.Sprint("%v", DecimalValue{}) safe in error reporting.
		{name: "nil_value", value: nil, scale: 5, want: "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := DecimalValue{Value: tt.value, Scale: tt.scale}
			if got := d.String(); got != tt.want {
				t.Errorf("DecimalValue{%v, %d}.String() = %q, want %q", tt.value, tt.scale, got, tt.want)
			}
		})
	}
}

func TestDuckDBTypeToArrow(t *testing.T) {
	tests := []struct {
		dbType   string
		expected arrow.DataType
	}{
		// Signed integers
		{"TINYINT", arrow.PrimitiveTypes.Int8},
		{"SMALLINT", arrow.PrimitiveTypes.Int16},
		{"INTEGER", arrow.PrimitiveTypes.Int32},
		{"BIGINT", arrow.PrimitiveTypes.Int64},

		// Unsigned integers (regression: must NOT map to signed)
		{"UTINYINT", arrow.PrimitiveTypes.Uint8},
		{"USMALLINT", arrow.PrimitiveTypes.Uint16},
		{"UINTEGER", arrow.PrimitiveTypes.Uint32},
		{"UBIGINT", arrow.PrimitiveTypes.Uint64},

		// Big integers as Decimal128
		{"HUGEINT", &arrow.Decimal128Type{Precision: 38, Scale: 0}},

		// Floats
		{"FLOAT", arrow.PrimitiveTypes.Float32},
		{"REAL", arrow.PrimitiveTypes.Float32},
		{"DOUBLE", arrow.PrimitiveTypes.Float64},

		// Boolean
		{"BOOLEAN", arrow.FixedWidthTypes.Boolean},
		{"BOOL", arrow.FixedWidthTypes.Boolean},

		// Strings
		{"VARCHAR", arrow.BinaryTypes.String},
		{"TEXT", arrow.BinaryTypes.String},
		{"STRING", arrow.BinaryTypes.String},
		{"VARCHAR(255)", arrow.BinaryTypes.String},

		// Binary
		{"BLOB", arrow.BinaryTypes.Binary},
		{"BYTEA", arrow.BinaryTypes.Binary},

		// Date
		{"DATE", arrow.FixedWidthTypes.Date32},

		// Time
		{"TIME", arrow.FixedWidthTypes.Time64us},
		{"TIMETZ", arrow.FixedWidthTypes.Time64us},

		// Timestamps — plain TIMESTAMP must NOT have timezone
		{"TIMESTAMP", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"TIMESTAMP_S", &arrow.TimestampType{Unit: arrow.Second}},
		{"TIMESTAMP_MS", &arrow.TimestampType{Unit: arrow.Millisecond}},
		{"TIMESTAMP_NS", &arrow.TimestampType{Unit: arrow.Nanosecond}},
		// TIMESTAMPTZ must have UTC timezone
		{"TIMESTAMPTZ", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},

		// Interval
		{"INTERVAL", arrow.FixedWidthTypes.MonthDayNanoInterval},

		// UUID (as string; FixedSizeBinary(16) maps to BLOB in DuckDB, not UUID)
		{"UUID", arrow.BinaryTypes.String},

		// JSON/BIT as string
		{"JSON", arrow.BinaryTypes.String},
		{"BIT", arrow.BinaryTypes.String},

		// DECIMAL with parameters
		{"DECIMAL(18,2)", &arrow.Decimal128Type{Precision: 18, Scale: 2}},
		{"DECIMAL(10,5)", &arrow.Decimal128Type{Precision: 10, Scale: 5}},
		{"NUMERIC(38,0)", &arrow.Decimal128Type{Precision: 38, Scale: 0}},

		// Bare DECIMAL
		{"DECIMAL", &arrow.Decimal128Type{Precision: 18, Scale: 3}},

		// ENUM as string
		{"ENUM('a', 'b', 'c')", arrow.BinaryTypes.String},

		// LIST types (recursive)
		{"INTEGER[]", arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{"VARCHAR[]", arrow.ListOf(arrow.BinaryTypes.String)},
		{"BIGINT[]", arrow.ListOf(arrow.PrimitiveTypes.Int64)},
		{"DOUBLE[]", arrow.ListOf(arrow.PrimitiveTypes.Float64)},
		{"BOOLEAN[]", arrow.ListOf(arrow.FixedWidthTypes.Boolean)},

		// STRUCT types (recursive)
		{`STRUCT("a" INTEGER, "b" VARCHAR)`, arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		)},

		// MAP types (recursive)
		{"MAP(VARCHAR, INTEGER)", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)},

		// Nested: struct containing map
		{`STRUCT("a" MAP(VARCHAR, INTEGER), "b" BIGINT)`, arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: true},
			arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		)},

		// Nested: map containing struct
		{`MAP(VARCHAR, STRUCT("x" INTEGER))`, arrow.MapOf(
			arrow.BinaryTypes.String,
			arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
		)},

		// Deeply nested struct
		{`STRUCT("a" STRUCT("b" INTEGER))`, arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.StructOf(
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			), Nullable: true},
		)},

		// LIST inside STRUCT field
		{`STRUCT("a" INTEGER[], "b" VARCHAR)`, arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		)},

		// LIST of STRUCT
		{`STRUCT("a" INTEGER)[]`, arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		))},

		// Case insensitivity
		{"integer", arrow.PrimitiveTypes.Int32},
		{"varchar", arrow.BinaryTypes.String},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond}},

		// Unknown falls back to string
		{"UNKNOWN_TYPE", arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got := DuckDBTypeToArrow(tt.dbType)
			if got.ID() != tt.expected.ID() {
				t.Errorf("DuckDBTypeToArrow(%q) ID = %v, want %v", tt.dbType, got.ID(), tt.expected.ID())
				return
			}
			if got.String() != tt.expected.String() {
				t.Errorf("DuckDBTypeToArrow(%q) = %v, want %v", tt.dbType, got, tt.expected)
			}
		})
	}
}

func TestParseDecimalParams(t *testing.T) {
	tests := []struct {
		typeName  string
		wantPrec  int
		wantScale int
	}{
		{"DECIMAL(18,2)", 18, 2},
		{"DECIMAL(10,5)", 10, 5},
		{"DECIMAL(38,0)", 38, 0},
		{"NUMERIC(5,3)", 5, 3},
		{"DECIMAL", 18, 3},   // default fallback
		{"DECIMAL()", 18, 3}, // empty parens
		{"DECIMAL(abc,def)", 18, 3}, // non-numeric
		{"DECIMAL(18,)", 18, 3},     // missing scale
		{"DECIMAL(,2)", 18, 3},      // missing precision
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			p, s := parseDecimalParams(tt.typeName)
			if p != tt.wantPrec || s != tt.wantScale {
				t.Errorf("parseDecimalParams(%q) = (%d, %d), want (%d, %d)",
					tt.typeName, p, s, tt.wantPrec, tt.wantScale)
			}
		})
	}
}

func TestSplitTopLevelCommas(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "simple fields",
			input: `"a" INTEGER, "b" VARCHAR`,
			want:  []string{`"a" INTEGER`, `"b" VARCHAR`},
		},
		{
			name:  "nested parens",
			input: `"a" MAP(VARCHAR, INTEGER), "b" BIGINT`,
			want:  []string{`"a" MAP(VARCHAR, INTEGER)`, `"b" BIGINT`},
		},
		{
			name:  "no commas",
			input: `VARCHAR`,
			want:  []string{`VARCHAR`},
		},
		{
			name:  "map params",
			input: `VARCHAR, STRUCT("x" INT)`,
			want:  []string{`VARCHAR`, `STRUCT("x" INT)`},
		},
		{
			name:  "empty",
			input: "",
			want:  []string{""},
		},
		{
			name:  "deeply nested",
			input: `"a" STRUCT("b" MAP(VARCHAR, INTEGER)), "c" BIGINT`,
			want:  []string{`"a" STRUCT("b" MAP(VARCHAR, INTEGER))`, `"c" BIGINT`},
		},
		{
			name:  "quoted field with comma in name",
			input: `"a,b" INTEGER, "c" VARCHAR`,
			want:  []string{`"a,b" INTEGER`, `"c" VARCHAR`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitTopLevelCommas(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("splitTopLevelCommas(%q) = %v (len %d), want %v (len %d)",
					tt.input, got, len(got), tt.want, len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitTopLevelCommas(%q)[%d] = %q, want %q",
						tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseStructFieldDef(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantName string
		wantType string
	}{
		{
			name:     "simple field",
			input:    `"a" INTEGER`,
			wantName: "a",
			wantType: "INTEGER",
		},
		{
			name:     "escaped quotes in name",
			input:    `"a""b" VARCHAR`,
			wantName: `a"b`,
			wantType: "VARCHAR",
		},
		{
			name:     "nested type",
			input:    `"data" MAP(VARCHAR, INTEGER)`,
			wantName: "data",
			wantType: "MAP(VARCHAR, INTEGER)",
		},
		{
			name:     "unquoted name fallback",
			input:    `field_name BIGINT`,
			wantName: "field_name",
			wantType: "BIGINT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotName, gotType := parseStructFieldDef(tt.input)
			if gotName != tt.wantName {
				t.Errorf("parseStructFieldDef(%q) name = %q, want %q", tt.input, gotName, tt.wantName)
			}
			if gotType != tt.wantType {
				t.Errorf("parseStructFieldDef(%q) type = %q, want %q", tt.input, gotType, tt.wantType)
			}
		})
	}
}
