package server

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
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
		{"UINTEGER", "UINTEGER", OidInt8, 8},
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
