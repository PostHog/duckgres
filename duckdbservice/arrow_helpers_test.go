package duckdbservice

import (
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

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

		// STRUCT/MAP fall back to string (Phase 2)
		{"STRUCT(a INTEGER, b VARCHAR)", arrow.BinaryTypes.String},
		{"MAP(VARCHAR, INTEGER)", arrow.BinaryTypes.String},

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
		{"DECIMAL(abc,def)", 18, 3},  // non-numeric
		{"DECIMAL(18,)", 18, 3},      // missing scale
		{"DECIMAL(,2)", 18, 3},       // missing precision
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

func TestAppendValue(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("Int8Builder", func(t *testing.T) {
		b := array.NewInt8Builder(alloc)
		defer b.Release()
		AppendValue(b, int8(42))
		AppendValue(b, nil)
		arr := b.NewInt8Array()
		defer arr.Release()
		if arr.Len() != 2 {
			t.Fatalf("len = %d, want 2", arr.Len())
		}
		if arr.Value(0) != 42 {
			t.Errorf("value(0) = %d, want 42", arr.Value(0))
		}
		if !arr.IsNull(1) {
			t.Error("value(1) should be null")
		}
	})

	t.Run("Uint8Builder", func(t *testing.T) {
		b := array.NewUint8Builder(alloc)
		defer b.Release()
		AppendValue(b, uint8(200))
		AppendValue(b, nil)
		arr := b.NewUint8Array()
		defer arr.Release()
		if arr.Value(0) != 200 {
			t.Errorf("value(0) = %d, want 200", arr.Value(0))
		}
		if !arr.IsNull(1) {
			t.Error("value(1) should be null")
		}
	})

	t.Run("Uint16Builder", func(t *testing.T) {
		b := array.NewUint16Builder(alloc)
		defer b.Release()
		AppendValue(b, uint16(60000))
		arr := b.NewUint16Array()
		defer arr.Release()
		if arr.Value(0) != 60000 {
			t.Errorf("value(0) = %d, want 60000", arr.Value(0))
		}
	})

	t.Run("Uint32Builder", func(t *testing.T) {
		b := array.NewUint32Builder(alloc)
		defer b.Release()
		AppendValue(b, uint32(4000000000))
		arr := b.NewUint32Array()
		defer arr.Release()
		if arr.Value(0) != 4000000000 {
			t.Errorf("value(0) = %d, want 4000000000", arr.Value(0))
		}
	})

	t.Run("Uint64Builder", func(t *testing.T) {
		b := array.NewUint64Builder(alloc)
		defer b.Release()
		AppendValue(b, uint64(18000000000000000000))
		arr := b.NewUint64Array()
		defer arr.Release()
		if arr.Value(0) != 18000000000000000000 {
			t.Errorf("value(0) = %d, want 18000000000000000000", arr.Value(0))
		}
	})

	t.Run("Date32Builder", func(t *testing.T) {
		b := array.NewDate32Builder(alloc)
		defer b.Release()
		// 2024-01-15 = 19737 days since Unix epoch (54 years: 41*365 + 13*366 + 14)
		testDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		AppendValue(b, testDate)
		arr := b.NewDate32Array()
		defer arr.Release()
		if arr.Value(0) != 19737 {
			t.Errorf("value(0) = %d, want 19737", arr.Value(0))
		}
	})

	t.Run("Date32Builder_PreEpoch", func(t *testing.T) {
		b := array.NewDate32Builder(alloc)
		defer b.Release()
		// 1969-12-31 = -1 days since epoch
		AppendValue(b, time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC))
		// 1960-01-01 = -3653 days since epoch
		AppendValue(b, time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC))
		arr := b.NewDate32Array()
		defer arr.Release()
		if got := arr.Value(0); got != -1 {
			t.Errorf("1969-12-31: got %d, want -1", got)
		}
		if got := arr.Value(1); got != -3653 {
			t.Errorf("1960-01-01: got %d, want -3653", got)
		}
	})

	t.Run("TimestampBuilder", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()
		testTime := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)
		AppendValue(rb.Field(0), testTime)
		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Timestamp)
		if col.Len() != 1 {
			t.Fatalf("len = %d, want 1", col.Len())
		}
		// Value should be microseconds since epoch
		got := col.Value(0)
		want := arrow.Timestamp(testTime.UnixMicro())
		if got != want {
			t.Errorf("value = %d, want %d", got, want)
		}
	})

	t.Run("Time64Builder", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "t", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()
		// 12:30:45.123456
		testTime := time.Date(0, 1, 1, 12, 30, 45, 123456000, time.UTC)
		AppendValue(rb.Field(0), testTime)
		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Time64)
		expected := arrow.Time64(12*3600000000 + 30*60000000 + 45*1000000 + 123456)
		if col.Value(0) != expected {
			t.Errorf("value = %d, want %d", col.Value(0), expected)
		}
	})

	t.Run("MonthDayNanoIntervalBuilder", func(t *testing.T) {
		b := array.NewMonthDayNanoIntervalBuilder(alloc)
		defer b.Release()
		interval := duckdb.Interval{Months: 2, Days: 15, Micros: 3600000000} // 1 hour
		AppendValue(b, interval)
		arr := b.NewMonthDayNanoIntervalArray()
		defer arr.Release()
		got := arr.Value(0)
		if got.Months != 2 || got.Days != 15 || got.Nanoseconds != 3600000000*1000 {
			t.Errorf("value = %+v, want {Months:2 Days:15 Nanoseconds:3600000000000}", got)
		}
	})

	t.Run("Decimal128Builder", func(t *testing.T) {
		dt := &arrow.Decimal128Type{Precision: 18, Scale: 2}
		b := array.NewDecimal128Builder(alloc, dt)
		defer b.Release()
		// duckdb.Decimal with Value=12345, Scale=2 → 123.45
		dec := duckdb.Decimal{Width: 18, Scale: 2, Value: big.NewInt(12345)}
		AppendValue(b, dec)
		arr := b.NewDecimal128Array()
		defer arr.Release()
		expected := decimal128.FromBigInt(big.NewInt(12345))
		if arr.Value(0) != expected {
			t.Errorf("value = %v, want %v", arr.Value(0), expected)
		}
	})

	t.Run("Decimal128Builder_BigInt", func(t *testing.T) {
		dt := &arrow.Decimal128Type{Precision: 38, Scale: 0}
		b := array.NewDecimal128Builder(alloc, dt)
		defer b.Release()
		bigVal := new(big.Int).SetUint64(18446744073709551615) // max uint64
		AppendValue(b, bigVal)
		arr := b.NewDecimal128Array()
		defer arr.Release()
		expected := decimal128.FromBigInt(bigVal)
		if arr.Value(0) != expected {
			t.Errorf("value = %v, want %v", arr.Value(0), expected)
		}
	})

	t.Run("FixedSizeBinaryBuilder_UUID", func(t *testing.T) {
		dt := &arrow.FixedSizeBinaryType{ByteWidth: 16}
		b := array.NewFixedSizeBinaryBuilder(alloc, dt)
		defer b.Release()
		uuid := duckdb.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		AppendValue(b, uuid)
		arr := b.NewFixedSizeBinaryArray()
		defer arr.Release()
		got := arr.Value(0)
		if len(got) != 16 || got[0] != 0x01 || got[15] != 0x10 {
			t.Errorf("value = %v, want UUID bytes", got)
		}
	})

	t.Run("ListBuilder", func(t *testing.T) {
		dt := arrow.ListOf(arrow.PrimitiveTypes.Int32)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "list", Type: dt, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()
		AppendValue(rb.Field(0), []any{int32(1), int32(2), int32(3)})
		AppendValue(rb.Field(0), nil)
		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.List)
		if col.Len() != 2 {
			t.Fatalf("len = %d, want 2", col.Len())
		}
		if col.IsNull(1) != true {
			t.Error("value(1) should be null")
		}
		// Check first list's values
		start, end := col.ValueOffsets(0)
		values := col.ListValues().(*array.Int32)
		if end-start != 3 {
			t.Fatalf("list length = %d, want 3", end-start)
		}
		if values.Value(int(start)) != 1 || values.Value(int(start)+1) != 2 || values.Value(int(start)+2) != 3 {
			t.Error("list values mismatch")
		}
	})

	t.Run("StringBuilder_UUID", func(t *testing.T) {
		b := array.NewStringBuilder(alloc)
		defer b.Release()
		// Test duckdb.UUID type (value receiver String())
		uuid := duckdb.UUID{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		AppendValue(b, uuid)
		// Test []byte (what the Go driver actually returns for UUID via Scan(&interface{}))
		uuidBytes := []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		AppendValue(b, uuidBytes)
		arr := b.NewStringArray()
		defer arr.Release()
		want := "550e8400-e29b-41d4-a716-446655440000"
		if got := arr.Value(0); got != want {
			t.Errorf("UUID type: value(0) = %q, want %q", got, want)
		}
		if got := arr.Value(1); got != want {
			t.Errorf("[]byte UUID: value(1) = %q, want %q", got, want)
		}
	})

	t.Run("StringBuilder_fallback", func(t *testing.T) {
		b := array.NewStringBuilder(alloc)
		defer b.Release()
		AppendValue(b, "hello")
		AppendValue(b, 42) // non-string should use fmt.Sprintf
		arr := b.NewStringArray()
		defer arr.Release()
		if arr.Value(0) != "hello" {
			t.Errorf("value(0) = %q, want %q", arr.Value(0), "hello")
		}
		if arr.Value(1) != "42" {
			t.Errorf("value(1) = %q, want %q", arr.Value(1), "42")
		}
	})
}
