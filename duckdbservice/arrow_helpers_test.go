package duckdbservice

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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

	t.Run("StructBuilder", func(t *testing.T) {
		st := arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "s", Type: st, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()

		// Valid struct
		AppendValue(rb.Field(0), map[string]any{"a": int32(42), "b": "hello"})
		// Null struct
		AppendValue(rb.Field(0), nil)
		// Struct with missing field (b should be null)
		AppendValue(rb.Field(0), map[string]any{"a": int32(7)})

		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Struct)

		if col.Len() != 3 {
			t.Fatalf("len = %d, want 3", col.Len())
		}

		// Check valid struct
		aCol := col.Field(0).(*array.Int32)
		bCol := col.Field(1).(*array.String)
		if aCol.Value(0) != 42 {
			t.Errorf("row0.a = %d, want 42", aCol.Value(0))
		}
		if bCol.Value(0) != "hello" {
			t.Errorf("row0.b = %q, want %q", bCol.Value(0), "hello")
		}

		// Check null struct
		if !col.IsNull(1) {
			t.Error("row1 should be null")
		}

		// Check struct with missing field
		if aCol.Value(2) != 7 {
			t.Errorf("row2.a = %d, want 7", aCol.Value(2))
		}
		if !bCol.IsNull(2) {
			t.Error("row2.b should be null")
		}
	})

	t.Run("MapBuilder", func(t *testing.T) {
		mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "m", Type: mt, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()

		// Valid map (using duckdb.Map)
		AppendValue(rb.Field(0), duckdb.Map{"x": int32(1), "y": int32(2)})
		// Null map
		AppendValue(rb.Field(0), nil)
		// Empty map
		AppendValue(rb.Field(0), duckdb.Map{})

		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Map)

		if col.Len() != 3 {
			t.Fatalf("len = %d, want 3", col.Len())
		}

		// Valid map: should have 2 entries
		start, end := col.ValueOffsets(0)
		if end-start != 2 {
			t.Errorf("row0 entries = %d, want 2", end-start)
		}

		// Null map
		if !col.IsNull(1) {
			t.Error("row1 should be null")
		}

		// Empty map: should have 0 entries but not be null
		start, end = col.ValueOffsets(2)
		if end-start != 0 {
			t.Errorf("row2 entries = %d, want 0", end-start)
		}
		if col.IsNull(2) {
			t.Error("row2 should not be null (empty map)")
		}
	})

	t.Run("MapBuilder_map_any_any", func(t *testing.T) {
		mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "m", Type: mt, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()

		// map[any]any fallback
		AppendValue(rb.Field(0), map[any]any{"x": int32(1)})

		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Map)

		if col.Len() != 1 {
			t.Fatalf("len = %d, want 1", col.Len())
		}
		start, end := col.ValueOffsets(0)
		if end-start != 1 {
			t.Errorf("row0 entries = %d, want 1", end-start)
		}
	})

	t.Run("StructBuilder_nested_map", func(t *testing.T) {
		// STRUCT("a" MAP(VARCHAR, INTEGER))
		st := arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: true},
		)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "s", Type: st, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()

		AppendValue(rb.Field(0), map[string]any{
			"a": duckdb.Map{"k": int32(99)},
		})

		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Struct)
		mapCol := col.Field(0).(*array.Map)

		start, end := mapCol.ValueOffsets(0)
		if end-start != 1 {
			t.Errorf("nested map entries = %d, want 1", end-start)
		}
	})

	t.Run("MapBuilder_nested_struct", func(t *testing.T) {
		// MAP(VARCHAR, STRUCT("x" INTEGER))
		mt := arrow.MapOf(
			arrow.BinaryTypes.String,
			arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
		)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "m", Type: mt, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(alloc, schema)
		defer rb.Release()

		AppendValue(rb.Field(0), duckdb.Map{
			"key1": map[string]any{"x": int32(10)},
		})

		rec := rb.NewRecordBatch()
		defer rec.Release()
		col := rec.Column(0).(*array.Map)

		start, end := col.ValueOffsets(0)
		if end-start != 1 {
			t.Errorf("map entries = %d, want 1", end-start)
		}
	})
}

// TestNestedTypesRoundTrip exercises DuckDB driver → Scan → RowsToRecord → Arrow IPC
// round-trip for STRUCT, MAP, and nested combinations. Each subtest creates a table,
// inserts data, converts through RowsToRecord, serializes via Arrow IPC, deserializes,
// and runs a validate function against the deserialized record.
//
// These tests confirm Duckgres correctly serializes nested types over Flight SQL.
func TestNestedTypesRoundTrip(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	tests := []struct {
		name     string
		ddl      string
		inserts  []string
		query    string
		validate func(t *testing.T, rec arrow.RecordBatch)
	}{
		{
			name:    "struct basic",
			ddl:     `CREATE TABLE t_struct(s STRUCT(i INTEGER, j INTEGER))`,
			inserts: []string{`INSERT INTO t_struct VALUES ({'i': 10, 'j': 20})`},
			query:   "SELECT * FROM t_struct",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Struct)
				iCol := col.Field(0).(*array.Int32)
				jCol := col.Field(1).(*array.Int32)
				if v := iCol.Value(0); v != 10 {
					t.Errorf("s.i = %d, want 10", v)
				}
				if v := jCol.Value(0); v != 20 {
					t.Errorf("s.j = %d, want 20", v)
				}
			},
		},
		{
			name: "struct with nulls",
			ddl:  `CREATE TABLE t_struct_null(s STRUCT(i INTEGER, j INTEGER))`,
			inserts: []string{
				`INSERT INTO t_struct_null VALUES ({'i': 1, 'j': 2})`,
				`INSERT INTO t_struct_null VALUES (NULL)`,
				`INSERT INTO t_struct_null VALUES ({'i': 3, 'j': 4})`,
			},
			query: "SELECT * FROM t_struct_null ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 3 {
					t.Fatalf("rows = %d, want 3", rec.NumRows())
				}
				col := rec.Column(0).(*array.Struct)
				iCol := col.Field(0).(*array.Int32)

				if v := iCol.Value(0); v != 1 {
					t.Errorf("row0.i = %d, want 1", v)
				}
				if !col.IsNull(1) {
					t.Error("row1 should be null")
				}
				if v := iCol.Value(2); v != 3 {
					t.Errorf("row2.i = %d, want 3", v)
				}
			},
		},
		{
			name: "struct multiple rows",
			ddl:  `CREATE TABLE t_struct_multi(s STRUCT(x BIGINT, y VARCHAR))`,
			inserts: []string{
				`INSERT INTO t_struct_multi VALUES ({'x': 100, 'y': 'alpha'})`,
				`INSERT INTO t_struct_multi VALUES ({'x': 200, 'y': 'beta'})`,
				`INSERT INTO t_struct_multi VALUES ({'x': 300, 'y': 'gamma'})`,
			},
			query: "SELECT * FROM t_struct_multi ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 3 {
					t.Fatalf("rows = %d, want 3", rec.NumRows())
				}
				col := rec.Column(0).(*array.Struct)
				xCol := col.Field(0).(*array.Int64)
				yCol := col.Field(1).(*array.String)

				wantX := []int64{100, 200, 300}
				wantY := []string{"alpha", "beta", "gamma"}
				for i := range wantX {
					if v := xCol.Value(i); v != wantX[i] {
						t.Errorf("row%d.x = %d, want %d", i, v, wantX[i])
					}
					if v := yCol.Value(i); v != wantY[i] {
						t.Errorf("row%d.y = %q, want %q", i, v, wantY[i])
					}
				}
			},
		},
		{
			name:    "nested struct in struct",
			ddl:     `CREATE TABLE t_nested_struct(s STRUCT(nested STRUCT(x INTEGER), label VARCHAR))`,
			inserts: []string{`INSERT INTO t_nested_struct VALUES ({'nested': {'x': 42}, 'label': 'hi'})`},
			query:   "SELECT * FROM t_nested_struct",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				outer := rec.Column(0).(*array.Struct)
				nested := outer.Field(0).(*array.Struct)
				xCol := nested.Field(0).(*array.Int32)
				label := outer.Field(1).(*array.String)

				if v := xCol.Value(0); v != 42 {
					t.Errorf("s.nested.x = %d, want 42", v)
				}
				if v := label.Value(0); v != "hi" {
					t.Errorf("s.label = %q, want %q", v, "hi")
				}
			},
		},
		{
			name:    "struct with list field",
			ddl:     `CREATE TABLE t_struct_list(s STRUCT(tags VARCHAR[], count INTEGER))`,
			inserts: []string{`INSERT INTO t_struct_list VALUES ({'tags': ['a', 'b', 'c'], 'count': 3})`},
			query:   "SELECT * FROM t_struct_list",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				outer := rec.Column(0).(*array.Struct)
				tags := outer.Field(0).(*array.List)
				countCol := outer.Field(1).(*array.Int32)

				start, end := tags.ValueOffsets(0)
				if end-start != 3 {
					t.Fatalf("tags length = %d, want 3", end-start)
				}
				vals := tags.ListValues().(*array.String)
				want := []string{"a", "b", "c"}
				for i, w := range want {
					if v := vals.Value(int(start) + i); v != w {
						t.Errorf("tags[%d] = %q, want %q", i, v, w)
					}
				}
				if v := countCol.Value(0); v != 3 {
					t.Errorf("count = %d, want 3", v)
				}
			},
		},
		{
			name:    "list of struct",
			ddl:     `CREATE TABLE t_list_struct(items STRUCT(k VARCHAR, v INTEGER)[])`,
			inserts: []string{`INSERT INTO t_list_struct VALUES ([{'k': 'one', 'v': 1}, {'k': 'two', 'v': 2}])`},
			query:   "SELECT * FROM t_list_struct",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.List)
				start, end := col.ValueOffsets(0)
				if end-start != 2 {
					t.Fatalf("list length = %d, want 2", end-start)
				}
				structs := col.ListValues().(*array.Struct)
				kCol := structs.Field(0).(*array.String)
				vCol := structs.Field(1).(*array.Int32)

				if v := kCol.Value(int(start)); v != "one" {
					t.Errorf("items[0].k = %q, want %q", v, "one")
				}
				if v := vCol.Value(int(start) + 1); v != 2 {
					t.Errorf("items[1].v = %d, want 2", v)
				}
			},
		},
		{
			name:    "map basic",
			ddl:     `CREATE TABLE t_map(m MAP(VARCHAR, INTEGER))`,
			inserts: []string{`INSERT INTO t_map VALUES (map {'a': 1, 'b': 2})`},
			query:   "SELECT * FROM t_map",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Map)
				start, end := col.ValueOffsets(0)
				if end-start != 2 {
					t.Fatalf("map entries = %d, want 2", end-start)
				}
				keys := col.Keys().(*array.String)
				items := col.Items().(*array.Int32)
				got := make(map[string]int32)
				for i := int(start); i < int(end); i++ {
					got[keys.Value(i)] = items.Value(i)
				}
				if got["a"] != 1 || got["b"] != 2 {
					t.Errorf("map = %v, want {a:1, b:2}", got)
				}
			},
		},
		{
			name: "map with null and empty",
			ddl:  `CREATE TABLE t_map_null(m MAP(VARCHAR, INTEGER))`,
			inserts: []string{
				`INSERT INTO t_map_null VALUES (map {'x': 99})`,
				`INSERT INTO t_map_null VALUES (NULL)`,
				`INSERT INTO t_map_null VALUES (map([],  []))`,
			},
			query: "SELECT * FROM t_map_null ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 3 {
					t.Fatalf("rows = %d, want 3", rec.NumRows())
				}
				col := rec.Column(0).(*array.Map)

				// Row 0: one entry
				s0, e0 := col.ValueOffsets(0)
				if e0-s0 != 1 {
					t.Errorf("row0 entries = %d, want 1", e0-s0)
				}

				// Row 1: null
				if !col.IsNull(1) {
					t.Error("row1 should be null")
				}

				// Row 2: empty map (not null)
				s2, e2 := col.ValueOffsets(2)
				if e2-s2 != 0 {
					t.Errorf("row2 entries = %d, want 0", e2-s2)
				}
				if col.IsNull(2) {
					t.Error("row2 should not be null (empty map)")
				}
			},
		},
		{
			name:    "map with struct values",
			ddl:     `CREATE TABLE t_map_struct(m MAP(VARCHAR, STRUCT(x INTEGER)))`,
			inserts: []string{`INSERT INTO t_map_struct VALUES (map {'key1': {'x': 10}})`},
			query:   "SELECT * FROM t_map_struct",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Map)
				start, end := col.ValueOffsets(0)
				if end-start != 1 {
					t.Fatalf("map entries = %d, want 1", end-start)
				}
				keys := col.Keys().(*array.String)
				structs := col.Items().(*array.Struct)
				xCol := structs.Field(0).(*array.Int32)

				if v := keys.Value(int(start)); v != "key1" {
					t.Errorf("key = %q, want %q", v, "key1")
				}
				if v := xCol.Value(int(start)); v != 10 {
					t.Errorf("value.x = %d, want 10", v)
				}
			},
		},
		{
			name: "struct with null fields",
			ddl:  `CREATE TABLE t_struct_nullfield(s STRUCT(i INTEGER, j INTEGER))`,
			inserts: []string{
				`INSERT INTO t_struct_nullfield VALUES ({'i': 10, 'j': NULL})`,
				`INSERT INTO t_struct_nullfield VALUES ({'i': NULL, 'j': 20})`,
			},
			query: "SELECT * FROM t_struct_nullfield ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 2 {
					t.Fatalf("rows = %d, want 2", rec.NumRows())
				}
				col := rec.Column(0).(*array.Struct)
				iCol := col.Field(0).(*array.Int32)
				jCol := col.Field(1).(*array.Int32)

				// Row 0: i=10, j=NULL
				if !col.IsValid(0) {
					t.Error("row0 struct should not be null")
				}
				if v := iCol.Value(0); v != 10 {
					t.Errorf("row0.i = %d, want 10", v)
				}
				if !jCol.IsNull(0) {
					t.Errorf("row0.j should be null, got %d", jCol.Value(0))
				}

				// Row 1: i=NULL, j=20
				if !iCol.IsNull(1) {
					t.Errorf("row1.i should be null, got %d", iCol.Value(1))
				}
				if v := jCol.Value(1); v != 20 {
					t.Errorf("row1.j = %d, want 20", v)
				}
			},
		},
		{
			name:    "nested struct with null inner",
			ddl:     `CREATE TABLE t_struct_nullinner(s STRUCT(nested STRUCT(x INTEGER), label VARCHAR))`,
			inserts: []string{`INSERT INTO t_struct_nullinner VALUES ({'nested': NULL, 'label': 'test'})`},
			query:   "SELECT * FROM t_struct_nullinner",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				outer := rec.Column(0).(*array.Struct)
				nested := outer.Field(0).(*array.Struct)
				label := outer.Field(1).(*array.String)

				if !nested.IsNull(0) {
					t.Error("inner struct should be null")
				}
				if v := label.Value(0); v != "test" {
					t.Errorf("label = %q, want %q", v, "test")
				}
			},
		},
		{
			name: "map with null values in entries",
			ddl:  `CREATE TABLE t_map_nullval(m MAP(VARCHAR, INTEGER))`,
			inserts: []string{
				`INSERT INTO t_map_nullval VALUES (map {'a': 1, 'b': NULL})`,
			},
			query: "SELECT * FROM t_map_nullval",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Map)
				start, end := col.ValueOffsets(0)
				if end-start != 2 {
					t.Fatalf("map entries = %d, want 2", end-start)
				}
				keys := col.Keys().(*array.String)
				items := col.Items().(*array.Int32)

				for i := int(start); i < int(end); i++ {
					k := keys.Value(i)
					switch k {
					case "a":
						if items.IsNull(i) {
							t.Error("m['a'] should not be null")
						} else if v := items.Value(i); v != 1 {
							t.Errorf("m['a'] = %d, want 1", v)
						}
					case "b":
						if !items.IsNull(i) {
							t.Errorf("m['b'] should be null, got %d", items.Value(i))
						}
					default:
						t.Errorf("unexpected key %q", k)
					}
				}
			},
		},
		{
			name:    "map with integer keys",
			ddl:     `CREATE TABLE t_map_intkey(m MAP(INTEGER, VARCHAR))`,
			inserts: []string{`INSERT INTO t_map_intkey VALUES (map {1: 'one', 2: 'two'})`},
			query:   "SELECT * FROM t_map_intkey",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Map)
				start, end := col.ValueOffsets(0)
				if end-start != 2 {
					t.Fatalf("map entries = %d, want 2", end-start)
				}
				keys := col.Keys().(*array.Int32)
				items := col.Items().(*array.String)

				got := make(map[int32]string)
				for i := int(start); i < int(end); i++ {
					got[keys.Value(i)] = items.Value(i)
				}
				if got[1] != "one" || got[2] != "two" {
					t.Errorf("map = %v, want {1:one, 2:two}", got)
				}
			},
		},
		{
			name:    "map with list values",
			ddl:     `CREATE TABLE t_map_list(m MAP(VARCHAR, INTEGER[]))`,
			inserts: []string{`INSERT INTO t_map_list VALUES (map {'x': [1, 2, 3], 'y': [4]})`},
			query:   "SELECT * FROM t_map_list",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				col := rec.Column(0).(*array.Map)
				start, end := col.ValueOffsets(0)
				if end-start != 2 {
					t.Fatalf("map entries = %d, want 2", end-start)
				}
				keys := col.Keys().(*array.String)
				lists := col.Items().(*array.List)

				for i := int(start); i < int(end); i++ {
					k := keys.Value(i)
					ls, le := lists.ValueOffsets(i)
					vals := lists.ListValues().(*array.Int32)
					switch k {
					case "x":
						if le-ls != 3 {
							t.Errorf("m['x'] length = %d, want 3", le-ls)
						}
						for j, want := range []int32{1, 2, 3} {
							if v := vals.Value(int(ls) + j); v != want {
								t.Errorf("m['x'][%d] = %d, want %d", j, v, want)
							}
						}
					case "y":
						if le-ls != 1 {
							t.Errorf("m['y'] length = %d, want 1", le-ls)
						}
						if v := vals.Value(int(ls)); v != 4 {
							t.Errorf("m['y'][0] = %d, want 4", v)
						}
					default:
						t.Errorf("unexpected key %q", k)
					}
				}
			},
		},
		{
			name: "struct with empty list field",
			ddl:  `CREATE TABLE t_struct_emptylist(s STRUCT(tags VARCHAR[], n INTEGER))`,
			inserts: []string{
				`INSERT INTO t_struct_emptylist VALUES ({'tags': [], 'n': 0})`,
				`INSERT INTO t_struct_emptylist VALUES ({'tags': ['a'], 'n': 1})`,
			},
			query: "SELECT * FROM t_struct_emptylist ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 2 {
					t.Fatalf("rows = %d, want 2", rec.NumRows())
				}
				outer := rec.Column(0).(*array.Struct)
				tags := outer.Field(0).(*array.List)

				// Row 0: empty list
				s0, e0 := tags.ValueOffsets(0)
				if e0-s0 != 0 {
					t.Errorf("row0 tags length = %d, want 0", e0-s0)
				}
				if tags.IsNull(0) {
					t.Error("row0 tags should be empty list, not null")
				}

				// Row 1: one element
				s1, e1 := tags.ValueOffsets(1)
				if e1-s1 != 1 {
					t.Errorf("row1 tags length = %d, want 1", e1-s1)
				}
			},
		},
		{
			name:    "deeply nested three levels",
			ddl:     `CREATE TABLE t_deep(s STRUCT(a STRUCT(b STRUCT(c INTEGER))))`,
			inserts: []string{`INSERT INTO t_deep VALUES ({'a': {'b': {'c': 777}}})`},
			query:   "SELECT * FROM t_deep",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				l1 := rec.Column(0).(*array.Struct)
				l2 := l1.Field(0).(*array.Struct)
				l3 := l2.Field(0).(*array.Struct)
				cCol := l3.Field(0).(*array.Int32)

				if v := cCol.Value(0); v != 777 {
					t.Errorf("s.a.b.c = %d, want 777", v)
				}
			},
		},
		{
			name: "struct many rows offset stress",
			ddl:  `CREATE TABLE t_struct_stress(s STRUCT(v INTEGER))`,
			inserts: func() []string {
				stmts := make([]string, 50)
				for i := range stmts {
					stmts[i] = fmt.Sprintf(`INSERT INTO t_struct_stress VALUES ({'v': %d})`, i)
				}
				return stmts
			}(),
			query: "SELECT * FROM t_struct_stress ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 50 {
					t.Fatalf("rows = %d, want 50", rec.NumRows())
				}
				col := rec.Column(0).(*array.Struct)
				vCol := col.Field(0).(*array.Int32)
				for i := 0; i < 50; i++ {
					if v := vCol.Value(i); v != int32(i) {
						t.Errorf("row%d.v = %d, want %d", i, v, i)
					}
				}
			},
		},
		{
			name: "map many rows offset stress",
			ddl:  `CREATE TABLE t_map_stress(m MAP(VARCHAR, INTEGER))`,
			inserts: func() []string {
				stmts := make([]string, 50)
				for i := range stmts {
					stmts[i] = fmt.Sprintf(`INSERT INTO t_map_stress VALUES (map {'k': %d})`, i)
				}
				return stmts
			}(),
			query: "SELECT * FROM t_map_stress ORDER BY rowid",
			validate: func(t *testing.T, rec arrow.RecordBatch) {
				if rec.NumRows() != 50 {
					t.Fatalf("rows = %d, want 50", rec.NumRows())
				}
				col := rec.Column(0).(*array.Map)
				for i := 0; i < 50; i++ {
					s, e := col.ValueOffsets(i)
					if e-s != 1 {
						t.Errorf("row%d entries = %d, want 1", i, e-s)
						continue
					}
					keys := col.Keys().(*array.String)
					items := col.Items().(*array.Int32)
					if k := keys.Value(int(s)); k != "k" {
						t.Errorf("row%d key = %q, want %q", i, k, "k")
					}
					if v := items.Value(int(s)); v != int32(i) {
						t.Errorf("row%d value = %d, want %d", i, v, i)
					}
				}
			},
		},
	}

	alloc := memory.NewGoAllocator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup table
			if _, err := db.Exec(tt.ddl); err != nil {
				t.Fatalf("DDL: %v", err)
			}
			for _, ins := range tt.inserts {
				if _, err := db.Exec(ins); err != nil {
					t.Fatalf("insert: %v", err)
				}
			}

			// Get schema and rows
			schema, err := GetQuerySchema(context.Background(), db, tt.query, nil)
			if err != nil {
				t.Fatalf("schema: %v", err)
			}
			rows, err := db.Query(tt.query)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			defer func() { _ = rows.Close() }()

			rec, err := RowsToRecord(alloc, rows, schema, 1024)
			if err != nil {
				t.Fatalf("RowsToRecord: %v", err)
			}
			if rec == nil {
				t.Fatal("RowsToRecord returned nil")
			}
			defer rec.Release()

			// Validate before IPC (driver → Arrow builder path)
			t.Run("pre-IPC", func(t *testing.T) {
				tt.validate(t, rec)
			})

			// Serialize through Arrow IPC (same path as Flight SQL transport)
			var buf bytes.Buffer
			w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
			defer func() {
				if err := w.Close(); err != nil {
					t.Fatalf("IPC close: %v", err)
				}
			}()
			if err := w.Write(rec); err != nil {
				t.Fatalf("IPC write: %v", err)
			}

			r, err := ipc.NewReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(alloc))
			if err != nil {
				t.Fatalf("IPC reader: %v", err)
			}
			defer r.Release()

			if !r.Next() {
				t.Fatal("no record in IPC stream")
			}
			got := r.RecordBatch()

			// Validate after IPC (tests the serialization path)
			t.Run("post-IPC", func(t *testing.T) {
				tt.validate(t, got)
			})
		})
	}
}

func TestGetQuerySchemaTrailingSemicolon(t *testing.T) {
	// Regression test: queries ending with ";" caused "syntax error at or near LIMIT"
	// because GetQuerySchema appended " LIMIT 0" after the semicolon, producing "; LIMIT 0".
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	tests := []struct {
		name  string
		query string
	}{
		{"no semicolon", "SELECT 1 AS n"},
		{"trailing semicolon", "SELECT 1 AS n;"},
		{"trailing semicolon with spaces", "SELECT 1 AS n ; "},
		{"CTE with semicolon", "WITH cte AS (SELECT 42 AS val) SELECT * FROM cte;"},
		{"query with existing LIMIT", "SELECT 1 AS n LIMIT 1"},
		{"query with existing LIMIT and semicolon", "SELECT 1 AS n LIMIT 1;"},
		{"SHOW statement", "SHOW TABLES"},
		{"SHOW with semicolon", "SHOW TABLES;"},
		{"DESCRIBE statement", "DESCRIBE SELECT 1"},
		{"FROM-first syntax", "FROM range(3)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := GetQuerySchema(context.Background(), db, tt.query, nil)
			if err != nil {
				t.Fatalf("GetQuerySchema(%q) error: %v", tt.query, err)
			}
			if schema.NumFields() == 0 {
				t.Fatalf("GetQuerySchema(%q) returned 0 fields", tt.query)
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
