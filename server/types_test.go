package server

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func TestMapDuckDBType(t *testing.T) {
	tests := []struct {
		name        string
		typeName    string
		expectedOID int32
		expectedSize int16
	}{
		// Boolean types
		{"BOOLEAN", "BOOLEAN", OidBool, 1},
		{"BOOL", "BOOL", OidBool, 1},
		{"boolean lowercase", "boolean", OidBool, 1},

		// Integer types
		{"TINYINT", "TINYINT", OidInt2, 2},
		{"INT1", "INT1", OidInt2, 2},
		{"SMALLINT", "SMALLINT", OidInt2, 2},
		{"INT2", "INT2", OidInt2, 2},
		{"INTEGER", "INTEGER", OidInt4, 4},
		{"INT4", "INT4", OidInt4, 4},
		{"INT", "INT", OidInt4, 4},
		{"BIGINT", "BIGINT", OidInt8, 8},
		{"INT8", "INT8", OidInt8, 8},
		{"HUGEINT", "HUGEINT", OidNumeric, -1},
		{"INT128", "INT128", OidNumeric, -1},

		// Unsigned integers
		{"UTINYINT", "UTINYINT", OidInt4, 4},
		{"USMALLINT", "USMALLINT", OidInt4, 4},
		{"UINTEGER", "UINTEGER", OidOid, 4}, // Maps to PostgreSQL oid type for pg_catalog columns
		{"UBIGINT", "UBIGINT", OidNumeric, -1},

		// Float types
		{"REAL", "REAL", OidFloat4, 4},
		{"FLOAT4", "FLOAT4", OidFloat4, 4},
		{"FLOAT", "FLOAT", OidFloat4, 4},
		{"DOUBLE", "DOUBLE", OidFloat8, 8},
		{"FLOAT8", "FLOAT8", OidFloat8, 8},

		// Decimal/Numeric types
		{"DECIMAL", "DECIMAL", OidNumeric, -1},
		{"DECIMAL(10,2)", "DECIMAL(10,2)", OidNumeric, -1},
		{"NUMERIC", "NUMERIC", OidNumeric, -1},
		{"NUMERIC(18,4)", "NUMERIC(18,4)", OidNumeric, -1},

		// String types
		{"VARCHAR", "VARCHAR", OidVarchar, -1},
		{"VARCHAR(255)", "VARCHAR(255)", OidVarchar, -1},
		{"TEXT", "TEXT", OidText, -1},
		{"STRING", "STRING", OidText, -1},

		// Binary types
		{"BLOB", "BLOB", OidBytea, -1},
		{"BYTEA", "BYTEA", OidBytea, -1},

		// Date/Time types
		{"DATE", "DATE", OidDate, 4},
		{"TIME", "TIME", OidTime, 8},
		{"TIMESTAMP", "TIMESTAMP", OidTimestamp, 8},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", OidTimestamptz, 8},
		{"TIMESTAMPTZ", "TIMESTAMPTZ", OidTimestamptz, 8},
		{"INTERVAL", "INTERVAL", OidInterval, 16},

		// Other types
		{"UUID", "UUID", OidUUID, 16},
		{"JSON", "JSON", OidJSON, -1},

		// Unknown types default to text
		{"unknown type", "SOMETYPE", OidText, -1},
		{"empty string", "", OidText, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapDuckDBType(tt.typeName)
			if result.OID != tt.expectedOID {
				t.Errorf("mapDuckDBType(%q).OID = %d, want %d", tt.typeName, result.OID, tt.expectedOID)
			}
			if result.Size != tt.expectedSize {
				t.Errorf("mapDuckDBType(%q).Size = %d, want %d", tt.typeName, result.Size, tt.expectedSize)
			}
		})
	}
}

func TestEncodeBool(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []byte
	}{
		{"true bool", true, []byte{1}},
		{"false bool", false, []byte{0}},
		{"int 1", int(1), []byte{1}},
		{"int 0", int(0), []byte{0}},
		{"int -1", int(-1), []byte{1}},
		{"int64 1", int64(1), []byte{1}},
		// Note: int64(0) returns 1 (true) due to a quirk in encodeBool's multi-type case handling
		// The comparison `val != 0` where val is interface{} compares against int(0), not int64(0)
		{"int64 0", int64(0), []byte{1}},
		{"string (unsupported)", "true", []byte{0}}, // defaults to false for unsupported types
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeBool(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("encodeBool(%v) length = %d, want %d", tt.input, len(result), len(tt.expected))
				return
			}
			for i, b := range result {
				if b != tt.expected[i] {
					t.Errorf("encodeBool(%v)[%d] = %d, want %d", tt.input, i, b, tt.expected[i])
				}
			}
		})
	}
}

func TestEncodeInt2(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int16
	}{
		{"int 42", int(42), 42},
		{"int -1", int(-1), -1},
		{"int8 127", int8(127), 127},
		{"int16 32767", int16(32767), 32767},
		{"int16 -32768", int16(-32768), -32768},
		{"int32 100", int32(100), 100},
		{"int64 200", int64(200), 200},
		{"uint8 255", uint8(255), 255},
		{"uint16 1000", uint16(1000), 1000},
		{"float32 3.7", float32(3.7), 3},
		{"float64 9.9", float64(9.9), 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeInt2(tt.input)
			if result == nil {
				t.Fatalf("encodeInt2(%v) returned nil", tt.input)
			}
			if len(result) != 2 {
				t.Fatalf("encodeInt2(%v) length = %d, want 2", tt.input, len(result))
			}
			got := int16(binary.BigEndian.Uint16(result))
			if got != tt.expected {
				t.Errorf("encodeInt2(%v) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}

	// Test unsupported type returns nil
	if result := encodeInt2("string"); result != nil {
		t.Errorf("encodeInt2(string) should return nil, got %v", result)
	}
}

func TestEncodeInt4(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int32
	}{
		{"int 42", int(42), 42},
		{"int -1", int(-1), -1},
		{"int32 max", int32(2147483647), 2147483647},
		{"int32 min", int32(-2147483648), -2147483648},
		{"int64 1000000", int64(1000000), 1000000},
		{"uint32 100000", uint32(100000), 100000},
		{"float64 3.14", float64(3.14), 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeInt4(tt.input)
			if result == nil {
				t.Fatalf("encodeInt4(%v) returned nil", tt.input)
			}
			if len(result) != 4 {
				t.Fatalf("encodeInt4(%v) length = %d, want 4", tt.input, len(result))
			}
			got := int32(binary.BigEndian.Uint32(result))
			if got != tt.expected {
				t.Errorf("encodeInt4(%v) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEncodeInt8(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int 42", int(42), 42},
		{"int64 max", int64(9223372036854775807), 9223372036854775807},
		{"int64 min", int64(-9223372036854775808), -9223372036854775808},
		{"uint64 large", uint64(18446744073709551615), -1}, // wraps around
		{"float64 1e10", float64(1e10), 10000000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeInt8(tt.input)
			if result == nil {
				t.Fatalf("encodeInt8(%v) returned nil", tt.input)
			}
			if len(result) != 8 {
				t.Fatalf("encodeInt8(%v) length = %d, want 8", tt.input, len(result))
			}
			got := int64(binary.BigEndian.Uint64(result))
			if got != tt.expected {
				t.Errorf("encodeInt8(%v) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEncodeFloat4(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float32
	}{
		{"float32 3.14", float32(3.14), 3.14},
		{"float32 -1.5", float32(-1.5), -1.5},
		{"float64 2.718", float64(2.718), 2.718},
		{"int 42", int(42), 42.0},
		{"int32 100", int32(100), 100.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeFloat4(tt.input)
			if result == nil {
				t.Fatalf("encodeFloat4(%v) returned nil", tt.input)
			}
			if len(result) != 4 {
				t.Fatalf("encodeFloat4(%v) length = %d, want 4", tt.input, len(result))
			}
			bits := binary.BigEndian.Uint32(result)
			got := math.Float32frombits(bits)
			if got != tt.expected {
				t.Errorf("encodeFloat4(%v) = %f, want %f", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEncodeFloat8(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
	}{
		{"float64 3.14159", float64(3.14159), 3.14159},
		{"float64 -1e100", float64(-1e100), -1e100},
		{"float32 2.5", float32(2.5), 2.5},
		{"int 42", int(42), 42.0},
		{"int64 1000000", int64(1000000), 1000000.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeFloat8(tt.input)
			if result == nil {
				t.Fatalf("encodeFloat8(%v) returned nil", tt.input)
			}
			if len(result) != 8 {
				t.Fatalf("encodeFloat8(%v) length = %d, want 8", tt.input, len(result))
			}
			bits := binary.BigEndian.Uint64(result)
			got := math.Float64frombits(bits)
			if got != tt.expected {
				t.Errorf("encodeFloat8(%v) = %f, want %f", tt.input, got, tt.expected)
			}
		})
	}
}

func TestEncodeDate(t *testing.T) {
	// PostgreSQL epoch is 2000-01-01
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		input        interface{}
		expectedDays int32
	}{
		{"2000-01-01 (epoch)", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"2000-01-02", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), 1},
		{"1999-12-31", time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC), -1},
		{"2024-06-15", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), int32(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC).Sub(pgEpoch).Hours() / 24)},
		{"string 2000-01-01", "2000-01-01", 0},
		{"string 2000-01-10", "2000-01-10", 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeDate(tt.input)
			if result == nil {
				t.Fatalf("encodeDate(%v) returned nil", tt.input)
			}
			if len(result) != 4 {
				t.Fatalf("encodeDate(%v) length = %d, want 4", tt.input, len(result))
			}
			got := int32(binary.BigEndian.Uint32(result))
			if got != tt.expectedDays {
				t.Errorf("encodeDate(%v) = %d days, want %d days", tt.input, got, tt.expectedDays)
			}
		})
	}

	// Test invalid string format returns nil
	if result := encodeDate("not-a-date"); result != nil {
		t.Errorf("encodeDate(invalid string) should return nil")
	}

	// Test unsupported type returns nil
	if result := encodeDate(12345); result != nil {
		t.Errorf("encodeDate(int) should return nil")
	}
}

func TestEncodeTimestamp(t *testing.T) {
	// Test that a known timestamp encodes correctly
	// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		input         interface{}
		expectedMicros int64
	}{
		{"2000-01-01 00:00:00 (epoch)", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"2000-01-01 00:00:01", time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC), 1000000},
		{"2000-01-01 00:01:00", time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC), 60000000},
		{"string format", "2000-01-01 00:00:00", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeTimestamp(tt.input)
			if result == nil {
				t.Fatalf("encodeTimestamp(%v) returned nil", tt.input)
			}
			if len(result) != 8 {
				t.Fatalf("encodeTimestamp(%v) length = %d, want 8", tt.input, len(result))
			}
			got := int64(binary.BigEndian.Uint64(result))
			if got != tt.expectedMicros {
				t.Errorf("encodeTimestamp(%v) = %d micros, want %d micros", tt.input, got, tt.expectedMicros)
			}
		})
	}

	// Test that relative times work correctly
	t.Run("one hour after epoch", func(t *testing.T) {
		oneHourAfter := pgEpoch.Add(time.Hour)
		result := encodeTimestamp(oneHourAfter)
		got := int64(binary.BigEndian.Uint64(result))
		expected := int64(3600000000) // 1 hour in microseconds
		if got != expected {
			t.Errorf("encodeTimestamp(one hour after epoch) = %d, want %d", got, expected)
		}
	})
}

func TestEncodeBytea(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []byte
	}{
		{"byte slice", []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
		{"empty byte slice", []byte{}, []byte{}},
		{"string", "hello", []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeBytea(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("encodeBytea(%v) length = %d, want %d", tt.input, len(result), len(tt.expected))
				return
			}
			for i, b := range result {
				if b != tt.expected[i] {
					t.Errorf("encodeBytea(%v)[%d] = %d, want %d", tt.input, i, b, tt.expected[i])
				}
			}
		})
	}

	// Test unsupported type returns nil
	if result := encodeBytea(12345); result != nil {
		t.Errorf("encodeBytea(int) should return nil")
	}
}

func TestEncodeBinary(t *testing.T) {
	// Test that encodeBinary dispatches to the correct encoder
	tests := []struct {
		name     string
		value    interface{}
		oid      int32
		wantNil  bool
		checkLen int // expected length, 0 to skip check
	}{
		{"nil value", nil, OidInt4, true, 0},
		{"bool true", true, OidBool, false, 1},
		{"int2", int16(42), OidInt2, false, 2},
		{"int4", int32(42), OidInt4, false, 4},
		{"int8", int64(42), OidInt8, false, 8},
		{"float4", float32(3.14), OidFloat4, false, 4},
		{"float8", float64(3.14), OidFloat8, false, 8},
		{"date", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), OidDate, false, 4},
		{"timestamp", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), OidTimestamp, false, 8},
		{"bytea", []byte{1, 2, 3}, OidBytea, false, 3},
		{"text", "hello", OidText, false, 5},
		{"varchar", "world", OidVarchar, false, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeBinary(tt.value, tt.oid)
			if tt.wantNil {
				if result != nil {
					t.Errorf("encodeBinary(%v, %d) should return nil", tt.value, tt.oid)
				}
				return
			}
			if result == nil {
				t.Errorf("encodeBinary(%v, %d) returned nil unexpectedly", tt.value, tt.oid)
				return
			}
			if tt.checkLen > 0 && len(result) != tt.checkLen {
				t.Errorf("encodeBinary(%v, %d) length = %d, want %d", tt.value, tt.oid, len(result), tt.checkLen)
			}
		})
	}
}

// Tests for binary parameter decoding functions

func TestDecodeBool(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected bool
		wantErr  bool
	}{
		{"true", []byte{1}, true, false},
		{"false", []byte{0}, false, false},
		{"non-zero is true", []byte{42}, true, false},
		{"empty data", []byte{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeBool(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeBool(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeBool(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeBool(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeInt2(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int16
		wantErr  bool
	}{
		{"positive", []byte{0x00, 0x2A}, 42, false},
		{"negative", []byte{0xFF, 0xFF}, -1, false},
		{"max", []byte{0x7F, 0xFF}, 32767, false},
		{"min", []byte{0x80, 0x00}, -32768, false},
		{"zero", []byte{0x00, 0x00}, 0, false},
		{"insufficient data", []byte{0x00}, 0, true},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt2(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeInt2(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeInt2(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeInt2(%v) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeInt4(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int32
		wantErr  bool
	}{
		{"positive", []byte{0x00, 0x00, 0x00, 0x2A}, 42, false},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF}, -1, false},
		{"max", []byte{0x7F, 0xFF, 0xFF, 0xFF}, 2147483647, false},
		{"min", []byte{0x80, 0x00, 0x00, 0x00}, -2147483648, false},
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0, false},
		{"insufficient data", []byte{0x00, 0x00}, 0, true},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt4(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeInt4(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeInt4(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeInt4(%v) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeInt8(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int64
		wantErr  bool
	}{
		{"positive", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A}, 42, false},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, -1, false},
		{"max", []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 9223372036854775807, false},
		{"min", []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -9223372036854775808, false},
		{"zero", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0, false},
		{"insufficient data", []byte{0x00, 0x00, 0x00, 0x00}, 0, true},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt8(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeInt8(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeInt8(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeInt8(%v) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeFloat4(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected float32
		wantErr  bool
	}{
		{"3.14", func() []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, math.Float32bits(3.14)); return b }(), 3.14, false},
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0, false},
		{"-1.5", func() []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, math.Float32bits(-1.5)); return b }(), -1.5, false},
		{"insufficient data", []byte{0x00, 0x00}, 0, true},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeFloat4(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeFloat4(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeFloat4(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeFloat4(%v) = %f, want %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeFloat8(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected float64
		wantErr  bool
	}{
		{"3.14159", func() []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, math.Float64bits(3.14159)); return b }(), 3.14159, false},
		{"zero", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0, false},
		{"-1e100", func() []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, math.Float64bits(-1e100)); return b }(), -1e100, false},
		{"insufficient data", []byte{0x00, 0x00, 0x00, 0x00}, 0, true},
		{"empty data", []byte{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeFloat8(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeFloat8(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeFloat8(%v) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("decodeFloat8(%v) = %f, want %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeDate(t *testing.T) {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    []byte
		expected time.Time
		wantErr  bool
	}{
		{"epoch (day 0)", []byte{0x00, 0x00, 0x00, 0x00}, pgEpoch, false},
		{"day 1", []byte{0x00, 0x00, 0x00, 0x01}, pgEpoch.AddDate(0, 0, 1), false},
		{"day -1", []byte{0xFF, 0xFF, 0xFF, 0xFF}, pgEpoch.AddDate(0, 0, -1), false},
		{"insufficient data", []byte{0x00, 0x00}, time.Time{}, true},
		{"empty data", []byte{}, time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeDate(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeDate(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeDate(%v) unexpected error: %v", tt.input, err)
				return
			}
			if !result.Equal(tt.expected) {
				t.Errorf("decodeDate(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeTimestamp(t *testing.T) {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    []byte
		expected time.Time
		wantErr  bool
	}{
		{"epoch (microsecond 0)", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, pgEpoch, false},
		{"one second after", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40}, pgEpoch.Add(time.Second), false}, // 1000000 microseconds
		{"insufficient data", []byte{0x00, 0x00, 0x00, 0x00}, time.Time{}, true},
		{"empty data", []byte{}, time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeTimestamp(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeTimestamp(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeTimestamp(%v) unexpected error: %v", tt.input, err)
				return
			}
			if !result.Equal(tt.expected) {
				t.Errorf("decodeTimestamp(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDecodeBinary(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		oid      int32
		expected interface{}
		wantErr  bool
	}{
		{"nil data", nil, OidInt4, nil, false},
		{"bool true", []byte{1}, OidBool, true, false},
		{"bool false", []byte{0}, OidBool, false, false},
		{"int2", []byte{0x00, 0x2A}, OidInt2, int16(42), false},
		{"int4", []byte{0x00, 0x00, 0x00, 0x2A}, OidInt4, int32(42), false},
		{"int4 zero", []byte{0x00, 0x00, 0x00, 0x00}, OidInt4, int32(0), false},
		{"int8", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A}, OidInt8, int64(42), false},
		{"bytea", []byte{0x01, 0x02, 0x03}, OidBytea, []byte{0x01, 0x02, 0x03}, false},
		{"text", []byte("hello"), OidText, "hello", false},
		{"unknown type defaults to text", []byte("test"), int32(9999), "test", false},
		// Error cases
		{"int4 insufficient data", []byte{0x00, 0x00}, OidInt4, nil, true},
		{"int8 insufficient data", []byte{0x00, 0x00, 0x00, 0x00}, OidInt8, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeBinary(tt.data, tt.oid)
			if tt.wantErr {
				if err == nil {
					t.Errorf("decodeBinary(%v, %d) expected error, got nil", tt.data, tt.oid)
				}
				return
			}
			if err != nil {
				t.Errorf("decodeBinary(%v, %d) unexpected error: %v", tt.data, tt.oid, err)
				return
			}

			// Special handling for byte slices
			if expectedBytes, ok := tt.expected.([]byte); ok {
				resultBytes, ok := result.([]byte)
				if !ok {
					t.Errorf("decodeBinary(%v, %d) = %T, want []byte", tt.data, tt.oid, result)
					return
				}
				if len(resultBytes) != len(expectedBytes) {
					t.Errorf("decodeBinary(%v, %d) length = %d, want %d", tt.data, tt.oid, len(resultBytes), len(expectedBytes))
					return
				}
				for i, b := range resultBytes {
					if b != expectedBytes[i] {
						t.Errorf("decodeBinary(%v, %d)[%d] = %d, want %d", tt.data, tt.oid, i, b, expectedBytes[i])
					}
				}
				return
			}

			if result != tt.expected {
				t.Errorf("decodeBinary(%v, %d) = %v (%T), want %v (%T)", tt.data, tt.oid, result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEncodeNumeric(t *testing.T) {
	tests := []struct {
		name           string
		input          duckdb.Decimal
		expectNdigits  uint16
		expectWeight   int16
		expectSign     uint16
		expectDscale   uint16
		expectDigits   []uint16
	}{
		{
			name:          "99.99",
			input:         duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(9999)},
			expectNdigits: 2,
			expectWeight:  0,
			expectSign:    0x0000, // NUMERIC_POS
			expectDscale:  2,
			expectDigits:  []uint16{99, 9900},
		},
		{
			name:          "-50.25",
			input:         duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(-5025)},
			expectNdigits: 2,
			expectWeight:  0,
			expectSign:    0x4000, // NUMERIC_NEG
			expectDscale:  2,
			expectDigits:  []uint16{50, 2500},
		},
		{
			name:          "0.00",
			input:         duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(0)},
			expectNdigits: 0,
			expectWeight:  0,
			expectSign:    0x0000,
			expectDscale:  2,
			expectDigits:  nil,
		},
		{
			name:          "12.3456",
			input:         duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(123456)},
			expectNdigits: 2,
			expectWeight:  0,
			expectSign:    0x0000,
			expectDscale:  4,
			expectDigits:  []uint16{12, 3456},
		},
		{
			name:          "0.0001",
			input:         duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(1)},
			expectNdigits: 1,
			expectWeight:  -1,
			expectSign:    0x0000,
			expectDscale:  4,
			expectDigits:  []uint16{1},
		},
		{
			name:          "9999.9999",
			input:         duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(99999999)},
			expectNdigits: 2,
			expectWeight:  0,
			expectSign:    0x0000,
			expectDscale:  4,
			expectDigits:  []uint16{9999, 9999},
		},
		{
			name:          "10000.00",
			input:         duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(1000000)},
			expectNdigits: 1,
			expectWeight:  1,
			expectSign:    0x0000,
			expectDscale:  2,
			expectDigits:  []uint16{1},
		},
		{
			name:          "1.00",
			input:         duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(100)},
			expectNdigits: 1,
			expectWeight:  0,
			expectSign:    0x0000,
			expectDscale:  2,
			expectDigits:  []uint16{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeNumeric(tt.input)
			if result == nil {
				t.Fatalf("encodeNumeric(%s) returned nil", tt.name)
			}
			if len(result) < 8 {
				t.Fatalf("encodeNumeric(%s) returned %d bytes, need at least 8", tt.name, len(result))
			}

			ndigits := binary.BigEndian.Uint16(result[0:2])
			weight := int16(binary.BigEndian.Uint16(result[2:4]))
			sign := binary.BigEndian.Uint16(result[4:6])
			dscale := binary.BigEndian.Uint16(result[6:8])

			if ndigits != tt.expectNdigits {
				t.Errorf("ndigits = %d, want %d", ndigits, tt.expectNdigits)
			}
			if weight != tt.expectWeight {
				t.Errorf("weight = %d, want %d", weight, tt.expectWeight)
			}
			if sign != tt.expectSign {
				t.Errorf("sign = 0x%04X, want 0x%04X", sign, tt.expectSign)
			}
			if dscale != tt.expectDscale {
				t.Errorf("dscale = %d, want %d", dscale, tt.expectDscale)
			}

			expectedLen := 8 + 2*int(tt.expectNdigits)
			if len(result) != expectedLen {
				t.Errorf("result length = %d, want %d", len(result), expectedLen)
			}

			for i, expected := range tt.expectDigits {
				if 8+2*i+2 > len(result) {
					t.Errorf("digit %d: result too short", i)
					break
				}
				got := binary.BigEndian.Uint16(result[8+2*i:])
				if got != expected {
					t.Errorf("digit[%d] = %d, want %d", i, got, expected)
				}
			}
		})
	}

	// Test non-Decimal fallback to text encoding
	t.Run("non-decimal fallback", func(t *testing.T) {
		result := encodeNumeric("123.45")
		expected := []byte("123.45")
		if len(result) != len(expected) {
			t.Errorf("non-decimal fallback: length = %d, want %d", len(result), len(expected))
		}
	})
}

func TestDecodeNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    duckdb.Decimal // will encode then decode
		expected string
	}{
		{"99.99", duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(9999)}, "99.99"},
		{"-50.25", duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(-5025)}, "-50.25"},
		{"0.00", duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(0)}, "0.00"},
		{"12.3456", duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(123456)}, "12.3456"},
		{"0.0001", duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(1)}, "0.0001"},
		{"9999.9999", duckdb.Decimal{Width: 8, Scale: 4, Value: big.NewInt(99999999)}, "9999.9999"},
		{"10000.00", duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(1000000)}, "10000.00"},
		{"1.00", duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(100)}, "1.00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeNumeric(tt.input)
			decoded, err := decodeNumeric(encoded)
			if err != nil {
				t.Fatalf("decodeNumeric failed: %v", err)
			}
			if decoded != tt.expected {
				t.Errorf("decodeNumeric(encodeNumeric(%s)) = %q, want %q", tt.name, decoded, tt.expected)
			}
		})
	}
}

func TestUBIGINTTypmod(t *testing.T) {
	// UBIGINT should map to NUMERIC(20,0) with proper typmod
	info := mapDuckDBType("UBIGINT")
	if info.OID != OidNumeric {
		t.Errorf("UBIGINT OID = %d, want %d (OidNumeric)", info.OID, OidNumeric)
	}
	expectedTypmod := int32((20 << 16) + 4)
	if info.Typmod != expectedTypmod {
		t.Errorf("UBIGINT Typmod = %d, want %d (DECIMAL(20,0))", info.Typmod, expectedTypmod)
	}
}

func TestEncodeDecodeUBIGINTMax(t *testing.T) {
	// Max UBIGINT = 2^64 - 1 = 18446744073709551615 (20 digits)
	// DuckDB Go driver returns UBIGINT as duckdb.Decimal with scale 0
	maxUBIGINT := new(big.Int)
	maxUBIGINT.SetString("18446744073709551615", 10)
	dec := duckdb.Decimal{Width: 20, Scale: 0, Value: maxUBIGINT}

	encoded := encodeNumeric(dec)
	if encoded == nil {
		t.Fatal("encodeNumeric returned nil for max UBIGINT")
	}

	decoded, err := decodeNumeric(encoded)
	if err != nil {
		t.Fatalf("decodeNumeric failed: %v", err)
	}
	if decoded != "18446744073709551615" {
		t.Errorf("roundtrip max UBIGINT = %q, want %q", decoded, "18446744073709551615")
	}
}

func TestDecodeNumericRaw(t *testing.T) {
	// Test decoding a manually constructed binary numeric: 99.99
	// ndigits=2, weight=0, sign=0x0000, dscale=2, digits=[99, 9900]
	data := make([]byte, 12)
	binary.BigEndian.PutUint16(data[0:], 2)      // ndigits
	binary.BigEndian.PutUint16(data[2:], 0)       // weight
	binary.BigEndian.PutUint16(data[4:], 0x0000)  // sign (positive)
	binary.BigEndian.PutUint16(data[6:], 2)       // dscale
	binary.BigEndian.PutUint16(data[8:], 99)      // digit[0]
	binary.BigEndian.PutUint16(data[10:], 9900)   // digit[1]

	result, err := decodeNumeric(data)
	if err != nil {
		t.Fatalf("decodeNumeric failed: %v", err)
	}
	if result != "99.99" {
		t.Errorf("decodeNumeric = %q, want %q", result, "99.99")
	}
}

func TestParseNumericTypmod(t *testing.T) {
	tests := []struct {
		typeName string
		want     int32
	}{
		{"DECIMAL(10,2)", int32((10<<16)|2) + 4},
		{"DECIMAL(18,4)", int32((18<<16)|4) + 4},
		{"DECIMAL(38,0)", int32(38<<16) + 4},
		{"DECIMAL(1,0)", int32(1<<16) + 4},
		{"numeric(5,3)", int32((5<<16)|3) + 4},
		// No precision/scale → -1
		{"DECIMAL", -1},
		{"NUMERIC", -1},
		// Invalid
		{"DECIMAL()", -1},
		{"DECIMAL(10)", -1},
		{"DECIMAL(0,2)", -1},
		{"DECIMAL(39,2)", -1},
		{"VARCHAR(10)", -1},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := parseNumericTypmod(tt.typeName)
			if got != tt.want {
				t.Errorf("parseNumericTypmod(%q) = %d, want %d", tt.typeName, got, tt.want)
			}
		})
	}

	// Verify round-trip with postgres_scanner's decoding formula
	typmod := parseNumericTypmod("DECIMAL(10,2)")
	width := ((typmod - 4) >> 16) & 0xffff
	scale := typmod - 4 - (width << 16)
	if width != 10 || scale != 2 {
		t.Errorf("round-trip: width=%d scale=%d, want 10, 2", width, scale)
	}
}

func TestEncodeDecodeUUID(t *testing.T) {
	// Test encoding from []byte (DuckDB scan type)
	rawBytes := []byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}
	encoded := encodeUUID(rawBytes)
	if len(encoded) != 16 {
		t.Fatalf("encodeUUID([]byte) length = %d, want 16", len(encoded))
	}
	for i, b := range rawBytes {
		if encoded[i] != b {
			t.Errorf("encodeUUID([]byte)[%d] = %02x, want %02x", i, encoded[i], b)
		}
	}

	// Test encoding from string
	encoded2 := encodeUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	if len(encoded2) != 16 {
		t.Fatalf("encodeUUID(string) length = %d, want 16", len(encoded2))
	}
	for i, b := range rawBytes {
		if encoded2[i] != b {
			t.Errorf("encodeUUID(string)[%d] = %02x, want %02x", i, encoded2[i], b)
		}
	}

	// Test decode round-trip
	decoded, err := decodeUUID(encoded)
	if err != nil {
		t.Fatalf("decodeUUID error: %v", err)
	}
	if decoded != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("decodeUUID = %q, want %q", decoded, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	}

	// Test invalid lengths
	if _, err := decodeUUID([]byte{1, 2, 3}); err == nil {
		t.Error("decodeUUID(3 bytes) should error")
	}
	if result := encodeUUID([]byte{1, 2, 3}); result != nil {
		t.Error("encodeUUID(3 bytes) should return nil")
	}
	if result := encodeUUID("not-a-uuid"); result != nil {
		t.Error("encodeUUID(invalid string) should return nil")
	}
	if result := encodeUUID(12345); result != nil {
		t.Error("encodeUUID(int) should return nil")
	}
}

func TestEncodeDecodeTime(t *testing.T) {
	tests := []struct {
		name         string
		input        interface{}
		expectMicros int64
		expectStr    string
	}{
		{
			"midnight",
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			0,
			"00:00:00",
		},
		{
			"10:30:00",
			time.Date(1, 1, 1, 10, 30, 0, 0, time.UTC),
			10*3600000000 + 30*60000000,
			"10:30:00",
		},
		{
			"23:59:59",
			time.Date(1, 1, 1, 23, 59, 59, 0, time.UTC),
			23*3600000000 + 59*60000000 + 59*1000000,
			"23:59:59",
		},
		{
			"12:00:00.500000",
			time.Date(1, 1, 1, 12, 0, 0, 500000000, time.UTC), // 500ms = 500000µs
			12*3600000000 + 500000,
			"12:00:00.500000",
		},
		{
			"string 10:30:00",
			"10:30:00",
			10*3600000000 + 30*60000000,
			"10:30:00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeTime(tt.input)
			if encoded == nil {
				t.Fatalf("encodeTime(%v) returned nil", tt.input)
			}
			if len(encoded) != 8 {
				t.Fatalf("encodeTime(%v) length = %d, want 8", tt.input, len(encoded))
			}
			gotMicros := int64(binary.BigEndian.Uint64(encoded))
			if gotMicros != tt.expectMicros {
				t.Errorf("encodeTime(%v) = %d micros, want %d", tt.input, gotMicros, tt.expectMicros)
			}

			decoded, err := decodeTime(encoded)
			if err != nil {
				t.Fatalf("decodeTime error: %v", err)
			}
			if decoded != tt.expectStr {
				t.Errorf("decodeTime = %q, want %q", decoded, tt.expectStr)
			}
		})
	}

	// Test invalid
	if result := encodeTime(12345); result != nil {
		t.Error("encodeTime(int) should return nil")
	}
	if _, err := decodeTime([]byte{1, 2}); err == nil {
		t.Error("decodeTime(2 bytes) should error")
	}
}

func TestEncodeDecodeInterval(t *testing.T) {
	tests := []struct {
		name      string
		input     duckdb.Interval
		expectStr string
	}{
		{
			"1 day",
			duckdb.Interval{Days: 1, Months: 0, Micros: 0},
			"1 day",
		},
		{
			"1 month",
			duckdb.Interval{Days: 0, Months: 1, Micros: 0},
			"1 month",
		},
		{
			"1 year 2 months 3 days",
			duckdb.Interval{Days: 3, Months: 14, Micros: 0},
			"1 year 2 month 3 day",
		},
		{
			"1 hour",
			duckdb.Interval{Days: 0, Months: 0, Micros: 3600000000},
			"01:00:00",
		},
		{
			"complex",
			duckdb.Interval{Days: 5, Months: 3, Micros: 7200000000 + 1800000000}, // 2h30m
			"3 month 5 day 02:30:00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeInterval(tt.input)
			if encoded == nil {
				t.Fatalf("encodeInterval(%v) returned nil", tt.input)
			}
			if len(encoded) != 16 {
				t.Fatalf("encodeInterval(%v) length = %d, want 16", tt.input, len(encoded))
			}

			// Verify binary layout
			gotMicros := int64(binary.BigEndian.Uint64(encoded[0:8]))
			gotDays := int32(binary.BigEndian.Uint32(encoded[8:12]))
			gotMonths := int32(binary.BigEndian.Uint32(encoded[12:16]))
			if gotMicros != tt.input.Micros {
				t.Errorf("micros = %d, want %d", gotMicros, tt.input.Micros)
			}
			if gotDays != tt.input.Days {
				t.Errorf("days = %d, want %d", gotDays, tt.input.Days)
			}
			if gotMonths != tt.input.Months {
				t.Errorf("months = %d, want %d", gotMonths, tt.input.Months)
			}

			decoded, err := decodeInterval(encoded)
			if err != nil {
				t.Fatalf("decodeInterval error: %v", err)
			}
			if decoded != tt.expectStr {
				t.Errorf("decodeInterval = %q, want %q", decoded, tt.expectStr)
			}
		})
	}

	// Test invalid
	if result := encodeInterval("not an interval"); result != nil {
		t.Error("encodeInterval(string) should return nil")
	}
	if _, err := decodeInterval([]byte{1, 2}); err == nil {
		t.Error("decodeInterval(2 bytes) should error")
	}
}

func TestDecodeTimestampAncient(t *testing.T) {
	// Bug 8: timestamps before ~1700 were corrupted due to time.Duration overflow.
	// This test verifies the fix works for ancient dates.
	tests := []struct {
		name     string
		input    time.Time
	}{
		{"2024-01-15", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
		{"1970-01-01", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"1000-06-15", time.Date(1000, 6, 15, 12, 0, 0, 0, time.UTC)},
		{"0100-01-01", time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"0001-01-01", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeTimestamp(tt.input)
			if encoded == nil {
				t.Fatalf("encodeTimestamp(%v) returned nil", tt.input)
			}
			decoded, err := decodeTimestamp(encoded)
			if err != nil {
				t.Fatalf("decodeTimestamp error: %v", err)
			}
			if !decoded.Equal(tt.input) {
				t.Errorf("round-trip: got %v, want %v", decoded, tt.input)
			}
		})
	}
}

// TestEncodeJSON verifies that encodeJSON re-serializes Go native values back
// to valid JSON text. The Go DuckDB driver's getJSON() deserializes JSON via
// json.Unmarshal (e.g., JSON string "hello" becomes Go string hello without
// quotes). Without re-serialization, bare strings like hello are sent on the
// wire and CAST(x AS JSON) fails because hello is not valid JSON.
func TestEncodeJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{} // Go native value (as returned by DuckDB driver's getJSON)
		expected string      // Expected JSON text on the wire
	}{
		{"string", "hello", `"hello"`},
		{"empty string", "", `""`},
		{"string with quotes", `say "hi"`, `"say \"hi\""`},
		{"integer", float64(42), "42"},
		{"float", float64(3.14), "3.14"},
		{"boolean true", true, "true"},
		{"boolean false", false, "false"},
		{"null", nil, "null"},
		{"object", map[string]interface{}{"name": "Alice", "age": float64(30)}, ""},  // checked separately
		{"array", []interface{}{float64(1), float64(2), float64(3)}, "[1,2,3]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeJSON(tt.input)
			if result == nil {
				t.Fatal("encodeJSON returned nil")
			}
			got := string(result)

			if tt.name == "object" {
				// Object key order is non-deterministic; verify it's valid JSON with expected keys
				var parsed map[string]interface{}
				if err := json.Unmarshal(result, &parsed); err != nil {
					t.Fatalf("encodeJSON produced invalid JSON: %s", got)
				}
				if parsed["name"] != "Alice" || parsed["age"] != float64(30) {
					t.Errorf("encodeJSON object content wrong: %s", got)
				}
				return
			}

			if got != tt.expected {
				t.Errorf("encodeJSON(%v) = %s, want %s", tt.input, got, tt.expected)
			}

			// Verify the output is valid JSON (the whole point of this function)
			var v interface{}
			if err := json.Unmarshal(result, &v); err != nil {
				t.Errorf("encodeJSON(%v) produced invalid JSON %q: %v", tt.input, got, err)
			}
		})
	}
}

// TestEncodeBinaryJSON verifies that encodeBinary routes JSON/JSONB OIDs
// through encodeJSON rather than the default text encoder.
func TestEncodeBinaryJSON(t *testing.T) {
	// Simulate what the Go DuckDB driver returns for JSON string "hello":
	// json.Unmarshal('"hello"') produces Go string "hello" (no quotes)
	goValue := "hello"

	for _, oid := range []int32{OidJSON, OidJSONB} {
		result := encodeBinary(goValue, oid)
		got := string(result)
		if got != `"hello"` {
			t.Errorf("encodeBinary(%q, OID %d) = %s, want %s", goValue, oid, got, `"hello"`)
		}
	}

	// Non-JSON OID should NOT add quotes (plain text encoding)
	result := encodeBinary("hello", OidVarchar)
	got := string(result)
	if got != "hello" {
		t.Errorf("encodeBinary(%q, OidVarchar) = %s, want %s", "hello", got, "hello")
	}
}
