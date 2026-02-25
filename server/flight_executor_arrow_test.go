package server

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Tests for arrowTypeToDuckDB — verifies that Arrow types are mapped back to
// DuckDB type name strings correctly. STRUCT and MAP are currently missing
// and fall through to "VARCHAR".

func TestArrowTypeToDuckDB_Struct(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "j", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	got := arrowTypeToDuckDB(st)
	expected := `STRUCT("i" INTEGER, "j" INTEGER)`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(STRUCT) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_NestedStruct(t *testing.T) {
	inner := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	outer := arrow.StructOf(
		arrow.Field{Name: "nested", Type: inner, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	got := arrowTypeToDuckDB(outer)
	expected := `STRUCT("nested" STRUCT("x" INTEGER), "y" INTEGER)`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(nested STRUCT) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_Map(t *testing.T) {
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	got := arrowTypeToDuckDB(mt)
	expected := "MAP(VARCHAR, INTEGER)"
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(MAP) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_MapWithStructValues(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	mt := arrow.MapOf(arrow.BinaryTypes.String, st)
	got := arrowTypeToDuckDB(mt)
	expected := `MAP(VARCHAR, STRUCT("x" INTEGER, "y" INTEGER))`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(MAP with STRUCT values) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_ListOfStruct(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	lt := arrow.ListOf(st)
	got := arrowTypeToDuckDB(lt)
	expected := `STRUCT("i" INTEGER)[]`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(LIST of STRUCT) = %q, want %q", got, expected)
	}
}

// Tests for extractArrowValue — verifies that Go values are correctly
// extracted from Arrow arrays. STRUCT and MAP currently fall through to
// the string fallback (ValueStr).
//
// Expected return types (matching what conn.go formatValue consumes):
//   STRUCT → map[string]interface{}
//   MAP    → OrderedMapValue (keys preserve original Arrow types and insertion order)

func TestExtractArrowValue_Struct(t *testing.T) {
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "j", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(10)
	sb.FieldBuilder(1).(*array.Int32Builder).Append(20)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(STRUCT) returned %T, want map[string]interface{}", val)
	}
	if m["i"] != int32(10) {
		t.Errorf("struct field 'i' = %v (%T), want int32(10)", m["i"], m["i"])
	}
	if m["j"] != int32(20) {
		t.Errorf("struct field 'j' = %v (%T), want int32(20)", m["j"], m["j"])
	}
}

func TestExtractArrowValue_StructNull(t *testing.T) {
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	if val != nil {
		t.Errorf("extractArrowValue(NULL STRUCT) = %v, want nil", val)
	}
}

func TestExtractArrowValue_NestedStruct(t *testing.T) {
	alloc := memory.NewGoAllocator()
	inner := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	outer := arrow.StructOf(
		arrow.Field{Name: "nested", Type: inner, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: outer, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	ob := rb.Field(0).(*array.StructBuilder)
	ob.Append(true)
	ib := ob.FieldBuilder(0).(*array.StructBuilder)
	ib.Append(true)
	ib.FieldBuilder(0).(*array.Int32Builder).Append(42)
	ob.FieldBuilder(1).(*array.Int32Builder).Append(99)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(nested STRUCT) returned %T, want map[string]interface{}", val)
	}

	nested, ok := m["nested"].(map[string]interface{})
	if !ok {
		t.Fatalf("nested field returned %T, want map[string]interface{}", m["nested"])
	}
	if nested["x"] != int32(42) {
		t.Errorf("nested.x = %v, want int32(42)", nested["x"])
	}
	if m["y"] != int32(99) {
		t.Errorf("y = %v, want int32(99)", m["y"])
	}
}

func TestExtractArrowValue_Map(t *testing.T) {
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	// Row 0: {"a": 1}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	// Row 1: {"x": 10, "y": 20}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("x")
	mb.ItemBuilder().(*array.Int32Builder).Append(10)
	mb.KeyBuilder().(*array.StringBuilder).Append("y")
	mb.ItemBuilder().(*array.Int32Builder).Append(20)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	// Row 0: single-entry map
	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP) row 0 returned %T, want OrderedMapValue", val)
	}
	if len(m.Keys) != 1 {
		t.Fatalf("expected 1 map entry, got %d", len(m.Keys))
	}
	if m.Keys[0] != "a" {
		t.Errorf("Keys[0] = %v (%T), want \"a\"", m.Keys[0], m.Keys[0])
	}
	if m.Values[0] != int32(1) {
		t.Errorf("Values[0] = %v, want int32(1)", m.Values[0])
	}

	// Row 1: two-entry map
	val = extractArrowValue(rec.Column(0), 1)
	m, ok = val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP) row 1 returned %T, want OrderedMapValue", val)
	}
	if len(m.Keys) != 2 {
		t.Fatalf("expected 2 map entries, got %d", len(m.Keys))
	}
	if m.Keys[0] != "x" || m.Keys[1] != "y" {
		t.Errorf("Keys = %v, want [x, y]", m.Keys)
	}
	if m.Values[0] != int32(10) {
		t.Errorf("Values[0] = %v, want int32(10)", m.Values[0])
	}
	if m.Values[1] != int32(20) {
		t.Errorf("Values[1] = %v, want int32(20)", m.Values[1])
	}
}

func TestExtractArrowValue_MapNull(t *testing.T) {
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	mb.AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	if val != nil {
		t.Errorf("extractArrowValue(NULL MAP) = %v, want nil", val)
	}
}

// --- Edge cases: composites that recurse through working paths into broken ones ---

func TestExtractArrowValue_ListOfStruct(t *testing.T) {
	// LIST works today, but the recursive extractArrowValue call for each
	// element hits the STRUCT string fallback → list of garbage strings.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(st), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	lb := rb.Field(0).(*array.ListBuilder)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	lb.Append(true)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(1)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("hello")
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(2)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("world")

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	elems, ok := val.([]any)
	if !ok {
		t.Fatalf("extractArrowValue(LIST of STRUCT) returned %T, want []any", val)
	}
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elems))
	}
	elem0, ok := elems[0].(map[string]interface{})
	if !ok {
		t.Fatalf("element 0 returned %T, want map[string]interface{}", elems[0])
	}
	if elem0["a"] != int32(1) {
		t.Errorf("elem[0].a = %v, want int32(1)", elem0["a"])
	}
	if elem0["b"] != "hello" {
		t.Errorf("elem[0].b = %v, want \"hello\"", elem0["b"])
	}
}

func TestExtractArrowValue_StructContainingList(t *testing.T) {
	// STRUCT containing a LIST field — once STRUCT is fixed, the LIST
	// child must also extract correctly via recursion.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("alice")
	lb := sb.FieldBuilder(1).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.StringBuilder).Append("admin")
	lb.ValueBuilder().(*array.StringBuilder).Append("user")

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(STRUCT with LIST) returned %T, want map[string]interface{}", val)
	}
	if m["name"] != "alice" {
		t.Errorf("name = %v, want \"alice\"", m["name"])
	}
	tags, ok := m["tags"].([]any)
	if !ok {
		t.Fatalf("tags field returned %T, want []any", m["tags"])
	}
	if len(tags) != 2 || tags[0] != "admin" || tags[1] != "user" {
		t.Errorf("tags = %v, want [admin, user]", tags)
	}
}

func TestExtractArrowValue_MapEmpty(t *testing.T) {
	// Non-null map with zero entries. ValueOffsets returns start==end.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true) // non-null, zero entries

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(empty MAP) returned %T, want OrderedMapValue", val)
	}
	if len(m.Keys) != 0 {
		t.Errorf("expected empty map, got %d entries", len(m.Keys))
	}
}

func TestExtractArrowValue_MapIntegerKeys(t *testing.T) {
	// MAP(INTEGER, VARCHAR) — keys are not strings; they get stringified
	// through formatValue when sent over the PG wire protocol.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.Int32Builder).Append(1)
	mb.ItemBuilder().(*array.StringBuilder).Append("one")
	mb.KeyBuilder().(*array.Int32Builder).Append(2)
	mb.ItemBuilder().(*array.StringBuilder).Append("two")

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP int keys) returned %T, want OrderedMapValue", val)
	}
	if m.Keys[0] != int32(1) {
		t.Errorf("Keys[0] = %v (%T), want int32(1)", m.Keys[0], m.Keys[0])
	}
	if m.Keys[1] != int32(2) {
		t.Errorf("Keys[1] = %v (%T), want int32(2)", m.Keys[1], m.Keys[1])
	}
	if m.Values[0] != "one" {
		t.Errorf("Values[0] = %v, want \"one\"", m.Values[0])
	}
	if m.Values[1] != "two" {
		t.Errorf("Values[1] = %v, want \"two\"", m.Values[1])
	}
}

func TestExtractArrowValue_MapWithNullValues(t *testing.T) {
	// Non-null map where some values are null.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("present")
	mb.ItemBuilder().(*array.Int32Builder).Append(42)
	mb.KeyBuilder().(*array.StringBuilder).Append("missing")
	mb.ItemBuilder().(*array.Int32Builder).AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP with null values) returned %T, want OrderedMapValue", val)
	}
	if m.Values[0] != int32(42) {
		t.Errorf("m[\"present\"] = %v, want int32(42)", m.Values[0])
	}
	if m.Values[1] != nil {
		t.Errorf("m[\"missing\"] = %v, want nil", m.Values[1])
	}
}

func TestExtractArrowValue_StructMultipleRows(t *testing.T) {
	// Multiple rows — extract from row index > 0 to catch offset bugs.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	for i := int32(0); i < 5; i++ {
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(i * 10)
	}

	rec := rb.NewRecordBatch()
	defer rec.Release()

	for row := 0; row < 5; row++ {
		val := extractArrowValue(rec.Column(0), row)
		m, ok := val.(map[string]interface{})
		if !ok {
			t.Fatalf("row %d: returned %T, want map[string]interface{}", row, val)
		}
		expected := int32(row) * 10
		if m["v"] != expected {
			t.Errorf("row %d: v = %v, want %d", row, m["v"], expected)
		}
	}
}

func TestExtractArrowValue_MapMultipleRows(t *testing.T) {
	// Multiple rows with varying entry counts — offset arithmetic must be correct.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	// Row 0: 1 entry
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	// Row 1: 0 entries (empty)
	mb.Append(true)
	// Row 2: 3 entries
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("x")
	mb.ItemBuilder().(*array.Int32Builder).Append(10)
	mb.KeyBuilder().(*array.StringBuilder).Append("y")
	mb.ItemBuilder().(*array.Int32Builder).Append(20)
	mb.KeyBuilder().(*array.StringBuilder).Append("z")
	mb.ItemBuilder().(*array.Int32Builder).Append(30)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	// Row 0
	val := extractArrowValue(rec.Column(0), 0)
	m0, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("row 0: returned %T, want OrderedMapValue", val)
	}
	if len(m0.Keys) != 1 || m0.Values[0] != int32(1) {
		t.Errorf("row 0: got %v, want {a:1}", m0)
	}

	// Row 1 (empty)
	val = extractArrowValue(rec.Column(0), 1)
	m1, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("row 1: returned %T, want OrderedMapValue", val)
	}
	if len(m1.Keys) != 0 {
		t.Errorf("row 1: expected empty map, got %v", m1)
	}

	// Row 2
	val = extractArrowValue(rec.Column(0), 2)
	m2, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("row 2: returned %T, want OrderedMapValue", val)
	}
	if len(m2.Keys) != 3 || m2.Values[2] != int32(30) {
		t.Errorf("row 2: got %v, want {x:10,y:20,z:30}", m2)
	}
}

// --- Edge cases: arrowTypeToDuckDB with composite types ---

func TestArrowTypeToDuckDB_MapIntegerKeys(t *testing.T) {
	mt := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
	got := arrowTypeToDuckDB(mt)
	expected := "MAP(INTEGER, VARCHAR)"
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(MAP int keys) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_StructContainingList(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	)
	got := arrowTypeToDuckDB(st)
	expected := `STRUCT("name" VARCHAR, "tags" VARCHAR[])`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(STRUCT with LIST) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_StructWithVarchar(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	got := arrowTypeToDuckDB(st)
	expected := `STRUCT("id" BIGINT, "label" VARCHAR)`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(STRUCT with VARCHAR) = %q, want %q", got, expected)
	}
}

// --- Pathological recursive nesting ---

func TestExtractArrowValue_ListOfMap(t *testing.T) {
	// LIST recurses into MAP — both nested type handlers needed.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(mt), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	lb := rb.Field(0).(*array.ListBuilder)
	mb := lb.ValueBuilder().(*array.MapBuilder)
	lb.Append(true)
	// First map in list: {"a": 1}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	// Second map in list: {"b": 2, "c": 3}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("b")
	mb.ItemBuilder().(*array.Int32Builder).Append(2)
	mb.KeyBuilder().(*array.StringBuilder).Append("c")
	mb.ItemBuilder().(*array.Int32Builder).Append(3)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	elems, ok := val.([]any)
	if !ok {
		t.Fatalf("extractArrowValue(LIST of MAP) returned %T, want []any", val)
	}
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elems))
	}
	m0, ok := elems[0].(OrderedMapValue)
	if !ok {
		t.Fatalf("element 0 returned %T, want OrderedMapValue", elems[0])
	}
	if m0.Values[0] != int32(1) {
		t.Errorf("elem[0][\"a\"] = %v, want int32(1)", m0.Values[0])
	}
	m1, ok := elems[1].(OrderedMapValue)
	if !ok {
		t.Fatalf("element 1 returned %T, want OrderedMapValue", elems[1])
	}
	if len(m1.Keys) != 2 {
		t.Errorf("elem[1] has %d entries, want 2", len(m1.Keys))
	}
}

func TestExtractArrowValue_MapOfMapValues(t *testing.T) {
	// MAP(VARCHAR, MAP(VARCHAR, INT)) — nested maps.
	alloc := memory.NewGoAllocator()
	inner := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	outer := arrow.MapOf(arrow.BinaryTypes.String, inner)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: outer, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	om := rb.Field(0).(*array.MapBuilder)
	im := om.ItemBuilder().(*array.MapBuilder)
	om.Append(true)
	om.KeyBuilder().(*array.StringBuilder).Append("outer_key")
	im.Append(true)
	im.KeyBuilder().(*array.StringBuilder).Append("inner_key")
	im.ItemBuilder().(*array.Int32Builder).Append(99)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP of MAP) returned %T, want OrderedMapValue", val)
	}
	inner_val, ok := m.Values[0].(OrderedMapValue)
	if !ok {
		t.Fatalf("inner map returned %T, want OrderedMapValue", m.Values[0])
	}
	if inner_val.Values[0] != int32(99) {
		t.Errorf("inner[\"inner_key\"] = %v, want int32(99)", inner_val.Values[0])
	}
}

func TestExtractArrowValue_MapOfListValues(t *testing.T) {
	// MAP(VARCHAR, INT[]) — map values are lists.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	lb := mb.ItemBuilder().(*array.ListBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("nums")
	lb.Append(true)
	lb.ValueBuilder().(*array.Int32Builder).Append(10)
	lb.ValueBuilder().(*array.Int32Builder).Append(20)
	lb.ValueBuilder().(*array.Int32Builder).Append(30)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("extractArrowValue(MAP of LIST) returned %T, want OrderedMapValue", val)
	}
	nums, ok := m.Values[0].([]any)
	if !ok {
		t.Fatalf("m[\"nums\"] returned %T, want []any", m.Values[0])
	}
	if len(nums) != 3 || nums[0] != int32(10) || nums[2] != int32(30) {
		t.Errorf("nums = %v, want [10, 20, 30]", nums)
	}
}

func TestExtractArrowValue_StructContainingMap(t *testing.T) {
	// STRUCT with a MAP field — both type handlers needed.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "meta", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(1)
	mb := sb.FieldBuilder(1).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("color")
	mb.ItemBuilder().(*array.StringBuilder).Append("red")

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(STRUCT with MAP) returned %T, want map[string]interface{}", val)
	}
	if m["id"] != int32(1) {
		t.Errorf("id = %v, want int32(1)", m["id"])
	}
	meta, ok := m["meta"].(OrderedMapValue)
	if !ok {
		t.Fatalf("meta field returned %T, want OrderedMapValue", m["meta"])
	}
	if meta.Values[0] != "red" {
		t.Errorf("meta[\"color\"] = %v, want \"red\"", meta.Values[0])
	}
}

func TestExtractArrowValue_DeeplyNestedThreeLevels(t *testing.T) {
	// STRUCT(a STRUCT(b STRUCT(c INT))) — three levels deep.
	alloc := memory.NewGoAllocator()
	c := arrow.StructOf(arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	b := arrow.StructOf(arrow.Field{Name: "b", Type: c, Nullable: true})
	a := arrow.StructOf(arrow.Field{Name: "a", Type: b, Nullable: true})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: a, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb1 := sb.FieldBuilder(0).(*array.StructBuilder)
	sb1.Append(true)
	sb2 := sb1.FieldBuilder(0).(*array.StructBuilder)
	sb2.Append(true)
	sb2.FieldBuilder(0).(*array.Int32Builder).Append(7)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	top, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("returned %T, want map[string]interface{}", val)
	}
	mid, ok := top["a"].(map[string]interface{})
	if !ok {
		t.Fatalf("a returned %T, want map[string]interface{}", top["a"])
	}
	inner, ok := mid["b"].(map[string]interface{})
	if !ok {
		t.Fatalf("b returned %T, want map[string]interface{}", mid["b"])
	}
	if inner["c"] != int32(7) {
		t.Errorf("a.b.c = %v, want int32(7)", inner["c"])
	}
}

func TestExtractArrowValue_StructAllFieldsNull(t *testing.T) {
	// Non-null struct where every field is null. The struct itself is present
	// but all children are nil — must not confuse with a null struct.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true) // struct is non-null
	sb.FieldBuilder(0).(*array.Int32Builder).AppendNull()
	sb.FieldBuilder(1).(*array.StringBuilder).AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(STRUCT all nulls) returned %T, want map[string]interface{}", val)
	}
	if m["a"] != nil {
		t.Errorf("a = %v, want nil", m["a"])
	}
	if m["b"] != nil {
		t.Errorf("b = %v, want nil", m["b"])
	}
}

func TestExtractArrowValue_StructSingleField(t *testing.T) {
	// Single-field struct — degenerate case.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "only", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Float64Builder).Append(3.14)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("returned %T, want map[string]interface{}", val)
	}
	if m["only"] != float64(3.14) {
		t.Errorf("only = %v, want 3.14", m["only"])
	}
}

func TestExtractArrowValue_StructMixedNullNonNullRows(t *testing.T) {
	// Alternating null and non-null struct rows in the same batch.
	// Catches bugs where null tracking drifts from child array indexing.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	// Row 0: non-null
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(100)
	// Row 1: null
	sb.AppendNull()
	// Row 2: non-null
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(200)
	// Row 3: null
	sb.AppendNull()
	// Row 4: non-null
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(300)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	// Row 0
	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("row 0: returned %T, want map[string]interface{}", val)
	}
	if m["v"] != int32(100) {
		t.Errorf("row 0: v = %v, want 100", m["v"])
	}
	// Row 1
	if extractArrowValue(rec.Column(0), 1) != nil {
		t.Errorf("row 1: want nil")
	}
	// Row 2
	val = extractArrowValue(rec.Column(0), 2)
	m, ok = val.(map[string]interface{})
	if !ok {
		t.Fatalf("row 2: returned %T, want map[string]interface{}", val)
	}
	if m["v"] != int32(200) {
		t.Errorf("row 2: v = %v, want 200", m["v"])
	}
	// Row 3
	if extractArrowValue(rec.Column(0), 3) != nil {
		t.Errorf("row 3: want nil")
	}
	// Row 4
	val = extractArrowValue(rec.Column(0), 4)
	m, ok = val.(map[string]interface{})
	if !ok {
		t.Fatalf("row 4: returned %T, want map[string]interface{}", val)
	}
	if m["v"] != int32(300) {
		t.Errorf("row 4: v = %v, want 300", m["v"])
	}
}

func TestExtractArrowValue_MapMixedNullNonNullRows(t *testing.T) {
	// Same pattern for MAP: alternating null/non-null rows.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	// Row 0: non-null
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	// Row 1: null
	mb.AppendNull()
	// Row 2: non-null
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("b")
	mb.ItemBuilder().(*array.Int32Builder).Append(2)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("row 0: returned %T, want OrderedMapValue", val)
	}
	if m.Values[0] != int32(1) {
		t.Errorf("row 0: a = %v, want 1", m.Values[0])
	}

	if extractArrowValue(rec.Column(0), 1) != nil {
		t.Errorf("row 1: want nil")
	}

	val = extractArrowValue(rec.Column(0), 2)
	m, ok = val.(OrderedMapValue)
	if !ok {
		t.Fatalf("row 2: returned %T, want OrderedMapValue", val)
	}
	if m.Values[0] != int32(2) {
		t.Errorf("row 2: b = %v, want 2", m.Values[0])
	}
}

func TestExtractArrowValue_StructManyFields(t *testing.T) {
	// 10-field struct — catches any hardcoded field-count assumptions.
	alloc := memory.NewGoAllocator()
	fields := make([]arrow.Field, 10)
	for i := range fields {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("f%d", i),
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: true,
		}
	}
	st := arrow.StructOf(fields...)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	for i := 0; i < 10; i++ {
		sb.FieldBuilder(i).(*array.Int32Builder).Append(int32(i * 100))
	}

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("returned %T, want map[string]interface{}", val)
	}
	if len(m) != 10 {
		t.Fatalf("expected 10 fields, got %d", len(m))
	}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("f%d", i)
		if m[key] != int32(i*100) {
			t.Errorf("m[%q] = %v, want %d", key, m[key], i*100)
		}
	}
}

func TestExtractArrowValue_ListEmpty(t *testing.T) {
	// Non-null list with 0 elements — currently works but pin the behavior.
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	lb := rb.Field(0).(*array.ListBuilder)
	lb.Append(true) // non-null, 0 elements

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	elems, ok := val.([]any)
	if !ok {
		t.Fatalf("returned %T, want []any", val)
	}
	if len(elems) != 0 {
		t.Errorf("expected empty list, got %d elements", len(elems))
	}
}

func TestExtractArrowValue_ListWithNullElements(t *testing.T) {
	// List containing null elements — pin that nulls inside lists work.
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	lb := rb.Field(0).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.Int32Builder).Append(1)
	lb.ValueBuilder().(*array.Int32Builder).AppendNull()
	lb.ValueBuilder().(*array.Int32Builder).Append(3)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	elems, ok := val.([]any)
	if !ok {
		t.Fatalf("returned %T, want []any", val)
	}
	if len(elems) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elems))
	}
	if elems[0] != int32(1) {
		t.Errorf("elem[0] = %v, want int32(1)", elems[0])
	}
	if elems[1] != nil {
		t.Errorf("elem[1] = %v, want nil", elems[1])
	}
	if elems[2] != int32(3) {
		t.Errorf("elem[2] = %v, want int32(3)", elems[2])
	}
}

func TestExtractArrowValue_ListOfList(t *testing.T) {
	// Nested lists — LIST(LIST(INT)). Recurses through the same code path.
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32)), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	outer := rb.Field(0).(*array.ListBuilder)
	inner := outer.ValueBuilder().(*array.ListBuilder)
	outer.Append(true)
	inner.Append(true)
	inner.ValueBuilder().(*array.Int32Builder).Append(1)
	inner.ValueBuilder().(*array.Int32Builder).Append(2)
	inner.Append(true)
	inner.ValueBuilder().(*array.Int32Builder).Append(3)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	outerList, ok := val.([]any)
	if !ok {
		t.Fatalf("returned %T, want []any", val)
	}
	if len(outerList) != 2 {
		t.Fatalf("expected 2 inner lists, got %d", len(outerList))
	}
	inner0, ok := outerList[0].([]any)
	if !ok {
		t.Fatalf("inner[0] returned %T, want []any", outerList[0])
	}
	if len(inner0) != 2 || inner0[0] != int32(1) || inner0[1] != int32(2) {
		t.Errorf("inner[0] = %v, want [1, 2]", inner0)
	}
	inner1, ok := outerList[1].([]any)
	if !ok {
		t.Fatalf("inner[1] returned %T, want []any", outerList[1])
	}
	if len(inner1) != 1 || inner1[0] != int32(3) {
		t.Errorf("inner[1] = %v, want [3]", inner1)
	}
}

func TestExtractArrowValue_MapWithStructValues(t *testing.T) {
	// MAP(VARCHAR, STRUCT(x INT, y INT)) — map values are structs.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	mt := arrow.MapOf(arrow.BinaryTypes.String, st)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	mb := rb.Field(0).(*array.MapBuilder)
	sb := mb.ItemBuilder().(*array.StructBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("point")
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(10)
	sb.FieldBuilder(1).(*array.Int32Builder).Append(20)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(OrderedMapValue)
	if !ok {
		t.Fatalf("returned %T, want OrderedMapValue", val)
	}
	// MAP values that are STRUCTs come back as map[string]interface{} from extractArrowValue
	point, ok := m.Values[0].(map[string]interface{})
	if !ok {
		t.Fatalf("m[\"point\"] returned %T, want map[string]interface{}", m.Values[0])
	}
	if point["x"] != int32(10) || point["y"] != int32(20) {
		t.Errorf("point = %v, want {x:10, y:20}", point)
	}
}

func TestExtractArrowValue_NestedStructWithNullInner(t *testing.T) {
	// Outer struct non-null, inner struct null. The map should have
	// the inner key present with a nil value.
	alloc := memory.NewGoAllocator()
	inner := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	outer := arrow.StructOf(
		arrow.Field{Name: "inner", Type: inner, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: outer, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StructBuilder).AppendNull() // inner is null
	sb.FieldBuilder(1).(*array.Int32Builder).Append(42)

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("returned %T, want map[string]interface{}", val)
	}
	if m["inner"] != nil {
		t.Errorf("inner = %v, want nil", m["inner"])
	}
	if m["y"] != int32(42) {
		t.Errorf("y = %v, want int32(42)", m["y"])
	}
}

// --- Additional arrowTypeToDuckDB edge cases ---

func TestArrowTypeToDuckDB_MapOfMap(t *testing.T) {
	inner := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	outer := arrow.MapOf(arrow.BinaryTypes.String, inner)
	got := arrowTypeToDuckDB(outer)
	expected := "MAP(VARCHAR, MAP(VARCHAR, INTEGER))"
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(MAP of MAP) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_MapOfList(t *testing.T) {
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.ListOf(arrow.PrimitiveTypes.Int32))
	got := arrowTypeToDuckDB(mt)
	expected := "MAP(VARCHAR, INTEGER[])"
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(MAP of LIST) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_ListOfMap(t *testing.T) {
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	lt := arrow.ListOf(mt)
	got := arrowTypeToDuckDB(lt)
	expected := "MAP(VARCHAR, INTEGER)[]"
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(LIST of MAP) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_DeeplyNestedThreeLevels(t *testing.T) {
	c := arrow.StructOf(arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	b := arrow.StructOf(arrow.Field{Name: "b", Type: c, Nullable: true})
	a := arrow.StructOf(arrow.Field{Name: "a", Type: b, Nullable: true})
	got := arrowTypeToDuckDB(a)
	expected := `STRUCT("a" STRUCT("b" STRUCT("c" INTEGER)))`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(3-deep STRUCT) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_StructContainingMap(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "meta", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	)
	got := arrowTypeToDuckDB(st)
	expected := `STRUCT("id" INTEGER, "meta" MAP(VARCHAR, VARCHAR))`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(STRUCT with MAP) = %q, want %q", got, expected)
	}
}

func TestArrowTypeToDuckDB_StructSingleField(t *testing.T) {
	st := arrow.StructOf(
		arrow.Field{Name: "only", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	)
	got := arrowTypeToDuckDB(st)
	expected := `STRUCT("only" DOUBLE)`
	if got != expected {
		t.Errorf("arrowTypeToDuckDB(single-field STRUCT) = %q, want %q", got, expected)
	}
}

// --- Remaining null/field edge cases ---

func TestExtractArrowValue_StructWithNullField(t *testing.T) {
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "j", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()

	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(10)
	sb.FieldBuilder(1).(*array.Int32Builder).AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()

	val := extractArrowValue(rec.Column(0), 0)
	m, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("extractArrowValue(STRUCT with null field) returned %T, want map[string]interface{}", val)
	}
	if m["i"] != int32(10) {
		t.Errorf("field 'i' = %v, want int32(10)", m["i"])
	}
	if m["j"] != nil {
		t.Errorf("field 'j' = %v, want nil", m["j"])
	}
}

// --- Tests verifying extractArrowValue → AppendValue round-trip for MAP ---
// These simulate the exact code path in flightsqlingress/ingress.go:rowSetToRecord
// where extractArrowValue output feeds into duckdbservice.AppendValue.

func TestExtractThenAppend_MapBasic(t *testing.T) {
	// Build a source Arrow MAP record, extract via extractArrowValue,
	// feed into AppendValue, verify the rebuilt record matches.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	// Build source record
	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	mb.KeyBuilder().(*array.StringBuilder).Append("b")
	mb.ItemBuilder().(*array.Int32Builder).Append(2)

	src := rb.NewRecordBatch()
	defer src.Release()

	// Extract → Append (simulates rowSetToRecord)
	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	// Verify the rebuilt MAP is not null and has correct values
	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt MAP is null — AppendValue did not recognize extractArrowValue's OrderedMapValue")
	}
	// Re-extract from rebuilt record to verify data
	rebuilt := extractArrowValue(col, 0)
	rm, ok := rebuilt.(OrderedMapValue)
	if !ok {
		t.Fatalf("re-extracted value is %T, want OrderedMapValue", rebuilt)
	}
	if rm.Keys[0] != "a" || rm.Keys[1] != "b" {
		t.Errorf("rebuilt Keys = %v, want [a, b]", rm.Keys)
	}
	if rm.Values[0] != int32(1) || rm.Values[1] != int32(2) {
		t.Errorf("rebuilt Values = %v, want [1, 2]", rm.Values)
	}
}

func TestExtractThenAppend_MapIntegerKeys(t *testing.T) {
	// MAP(INTEGER, VARCHAR) — keys are not strings, must survive round-trip.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.Int32Builder).Append(1)
	mb.ItemBuilder().(*array.StringBuilder).Append("one")
	mb.KeyBuilder().(*array.Int32Builder).Append(2)
	mb.ItemBuilder().(*array.StringBuilder).Append("two")

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt MAP(INT,VARCHAR) is null")
	}
	rebuilt := extractArrowValue(col, 0)
	rm, ok := rebuilt.(OrderedMapValue)
	if !ok {
		t.Fatalf("re-extracted value is %T, want OrderedMapValue", rebuilt)
	}
	if rm.Keys[0] != int32(1) {
		t.Errorf("rebuilt Keys[0] = %v (%T), want int32(1)", rm.Keys[0], rm.Keys[0])
	}
	if rm.Keys[1] != int32(2) {
		t.Errorf("rebuilt Keys[1] = %v (%T), want int32(2)", rm.Keys[1], rm.Keys[1])
	}
	if rm.Values[0] != "one" || rm.Values[1] != "two" {
		t.Errorf("rebuilt MAP values = %v, want {1:one, 2:two}", rm)
	}
}

func TestExtractThenAppend_MapNull(t *testing.T) {
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	mb.AppendNull()

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if !col.IsNull(0) {
		t.Error("rebuilt NULL MAP should be null")
	}
}

func TestExtractThenAppend_MapEmpty(t *testing.T) {
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true) // non-null, zero entries

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt empty MAP should not be null")
	}
	rebuilt := extractArrowValue(col, 0)
	rm, ok := rebuilt.(OrderedMapValue)
	if !ok {
		t.Fatalf("re-extracted value is %T, want OrderedMapValue", rebuilt)
	}
	if len(rm.Keys) != 0 {
		t.Errorf("rebuilt MAP has %d entries, want 0", len(rm.Keys))
	}
}

func TestExtractThenAppend_MapMultipleRows(t *testing.T) {
	// Multiple rows with null, empty, and populated maps.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	// Row 0: {"x": 10}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("x")
	mb.ItemBuilder().(*array.Int32Builder).Append(10)
	// Row 1: NULL
	mb.AppendNull()
	// Row 2: empty
	mb.Append(true)
	// Row 3: {"a": 1, "b": 2, "c": 3}
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("a")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)
	mb.KeyBuilder().(*array.StringBuilder).Append("b")
	mb.ItemBuilder().(*array.Int32Builder).Append(2)
	mb.KeyBuilder().(*array.StringBuilder).Append("c")
	mb.ItemBuilder().(*array.Int32Builder).Append(3)

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	for row := 0; row < 4; row++ {
		val := extractArrowValue(src.Column(0), row)
		appendValue(rb2.Field(0), val)
	}

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)

	// Row 0: {"x": 10}
	if col.IsNull(0) {
		t.Fatal("row 0: should not be null")
	}
	r0 := extractArrowValue(col, 0).(OrderedMapValue)
	if r0.Values[0] != int32(10) {
		t.Errorf("row 0: x = %v, want 10", r0.Values[0])
	}

	// Row 1: NULL
	if !col.IsNull(1) {
		t.Error("row 1: should be null")
	}

	// Row 2: empty
	if col.IsNull(2) {
		t.Fatal("row 2: should not be null")
	}
	r2 := extractArrowValue(col, 2).(OrderedMapValue)
	if len(r2.Keys) != 0 {
		t.Errorf("row 2: expected empty, got %v", r2)
	}

	// Row 3: {a:1, b:2, c:3}
	if col.IsNull(3) {
		t.Fatal("row 3: should not be null")
	}
	r3 := extractArrowValue(col, 3).(OrderedMapValue)
	if len(r3.Keys) != 3 || r3.Values[0] != int32(1) || r3.Values[1] != int32(2) || r3.Values[2] != int32(3) {
		t.Errorf("row 3: got %v, want {a:1,b:2,c:3}", r3)
	}
}

func TestExtractThenAppend_MapWithNullValues(t *testing.T) {
	// MAP entries where the value is NULL.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mb := rb.Field(0).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("present")
	mb.ItemBuilder().(*array.Int32Builder).Append(42)
	mb.KeyBuilder().(*array.StringBuilder).Append("missing")
	mb.ItemBuilder().(*array.Int32Builder).AppendNull()

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt MAP with null values should not itself be null")
	}
	rebuilt := extractArrowValue(col, 0).(OrderedMapValue)
	if rebuilt.Values[0] != int32(42) {
		t.Errorf("present = %v, want 42", rebuilt.Values[0])
	}
	if rebuilt.Values[1] != nil {
		t.Errorf("missing = %v, want nil", rebuilt.Values[1])
	}
}

func TestExtractThenAppend_MapOfMap(t *testing.T) {
	// MAP(VARCHAR, MAP(VARCHAR, INT)) — nested maps survive round-trip.
	alloc := memory.NewGoAllocator()
	inner := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	outer := arrow.MapOf(arrow.BinaryTypes.String, inner)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: outer, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	om := rb.Field(0).(*array.MapBuilder)
	im := om.ItemBuilder().(*array.MapBuilder)
	om.Append(true)
	om.KeyBuilder().(*array.StringBuilder).Append("outer")
	im.Append(true)
	im.KeyBuilder().(*array.StringBuilder).Append("inner")
	im.ItemBuilder().(*array.Int32Builder).Append(99)

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt nested MAP is null")
	}
	rebuilt := extractArrowValue(col, 0).(OrderedMapValue)
	innerMap, ok := rebuilt.Values[0].(OrderedMapValue)
	if !ok {
		t.Fatalf("inner value is %T, want OrderedMapValue", rebuilt.Values[0])
	}
	if innerMap.Values[0] != int32(99) {
		t.Errorf("inner[\"inner\"] = %v, want 99", innerMap.Values[0])
	}
}

func TestExtractThenAppend_StructContainingMap(t *testing.T) {
	// STRUCT with a MAP field — both type paths exercised.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "meta", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: st, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	sb := rb.Field(0).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(1)
	mmb := sb.FieldBuilder(1).(*array.MapBuilder)
	mmb.Append(true)
	mmb.KeyBuilder().(*array.StringBuilder).Append("color")
	mmb.ItemBuilder().(*array.StringBuilder).Append("red")

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	scol := dst.Column(0).(*array.Struct)
	if scol.IsNull(0) {
		t.Fatal("rebuilt STRUCT is null")
	}
	rebuilt := extractArrowValue(scol, 0).(map[string]interface{})
	if rebuilt["id"] != int32(1) {
		t.Errorf("id = %v, want 1", rebuilt["id"])
	}
	meta, ok := rebuilt["meta"].(OrderedMapValue)
	if !ok {
		t.Fatalf("meta is %T, want OrderedMapValue", rebuilt["meta"])
	}
	if meta.Values[0] != "red" {
		t.Errorf("meta[color] = %v, want red", meta.Values[0])
	}
}

func TestExtractThenAppend_MapWithStructValues(t *testing.T) {
	// MAP(VARCHAR, STRUCT(x INT, y INT)) — map values are structs.
	alloc := memory.NewGoAllocator()
	st := arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	)
	mt := arrow.MapOf(arrow.BinaryTypes.String, st)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "m", Type: mt, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	mmb := rb.Field(0).(*array.MapBuilder)
	ssb := mmb.ItemBuilder().(*array.StructBuilder)
	mmb.Append(true)
	mmb.KeyBuilder().(*array.StringBuilder).Append("point")
	ssb.Append(true)
	ssb.FieldBuilder(0).(*array.Int32Builder).Append(10)
	ssb.FieldBuilder(1).(*array.Int32Builder).Append(20)

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	col := dst.Column(0).(*array.Map)
	if col.IsNull(0) {
		t.Fatal("rebuilt MAP(VARCHAR,STRUCT) is null")
	}
	rebuilt := extractArrowValue(col, 0).(OrderedMapValue)
	point, ok := rebuilt.Values[0].(map[string]interface{})
	if !ok {
		t.Fatalf("point value is %T, want map[string]interface{}", rebuilt.Values[0])
	}
	if point["x"] != int32(10) || point["y"] != int32(20) {
		t.Errorf("point = %v, want {x:10, y:20}", point)
	}
}

func TestExtractThenAppend_ListOfMap(t *testing.T) {
	// LIST(MAP(VARCHAR, INT)) — list elements are maps.
	alloc := memory.NewGoAllocator()
	mt := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "list", Type: arrow.ListOf(mt), Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(alloc, schema)
	defer rb.Release()
	lb := rb.Field(0).(*array.ListBuilder)
	mmb := lb.ValueBuilder().(*array.MapBuilder)
	lb.Append(true)
	mmb.Append(true)
	mmb.KeyBuilder().(*array.StringBuilder).Append("a")
	mmb.ItemBuilder().(*array.Int32Builder).Append(1)
	mmb.Append(true)
	mmb.KeyBuilder().(*array.StringBuilder).Append("b")
	mmb.ItemBuilder().(*array.Int32Builder).Append(2)

	src := rb.NewRecordBatch()
	defer src.Release()

	rb2 := array.NewRecordBuilder(alloc, schema)
	defer rb2.Release()
	val := extractArrowValue(src.Column(0), 0)
	appendValue(rb2.Field(0), val)

	dst := rb2.NewRecordBatch()
	defer dst.Release()

	lcol := dst.Column(0).(*array.List)
	if lcol.IsNull(0) {
		t.Fatal("rebuilt LIST(MAP) is null")
	}
	rebuilt := extractArrowValue(lcol, 0).([]any)
	if len(rebuilt) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(rebuilt))
	}
	e0 := rebuilt[0].(OrderedMapValue)
	if e0.Values[0] != int32(1) {
		t.Errorf("elem[0] = %v, want {a:1}", e0)
	}
	e1 := rebuilt[1].(OrderedMapValue)
	if e1.Values[0] != int32(2) {
		t.Errorf("elem[1] = %v, want {b:2}", e1)
	}
}

// appendValue is a test-only reimplementation of the subset of
// duckdbservice.AppendValue logic needed for MAP round-trip tests.
// It lives here because server cannot import duckdbservice.
// Keep in sync with duckdbservice.AppendValue when MAP handling changes.
func appendValue(builder array.Builder, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}
	switch b := builder.(type) {
	case *array.Int32Builder:
		b.Append(val.(int32))
	case *array.Int64Builder:
		b.Append(val.(int64))
	case *array.Float64Builder:
		b.Append(val.(float64))
	case *array.StringBuilder:
		b.Append(val.(string))
	case *array.ListBuilder:
		elems := val.([]any)
		b.Append(true)
		for _, elem := range elems {
			appendValue(b.ValueBuilder(), elem)
		}
	case *array.StructBuilder:
		m := val.(map[string]interface{})
		b.Append(true)
		st := b.Type().(*arrow.StructType)
		for i := 0; i < st.NumFields(); i++ {
			fv, ok := m[st.Field(i).Name]
			if !ok || fv == nil {
				b.FieldBuilder(i).AppendNull()
			} else {
				appendValue(b.FieldBuilder(i), fv)
			}
		}
	case *array.MapBuilder:
		switch v := val.(type) {
		case OrderedMapValue:
			b.Append(true)
			for i, k := range v.Keys {
				appendValue(b.KeyBuilder(), k)
				item := v.Values[i]
				if item == nil {
					b.ItemBuilder().AppendNull()
				} else {
					appendValue(b.ItemBuilder(), item)
				}
			}
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// --- formatOrderedMapValue tests ---

func TestFormatOrderedMapValue_Basic(t *testing.T) {
	m := OrderedMapValue{Keys: []any{"a"}, Values: []any{int32(1)}}
	got := formatOrderedMapValue(m)
	if got != "{a=1}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{a=1}")
	}
}

func TestFormatOrderedMapValue_IntegerKeys(t *testing.T) {
	m := OrderedMapValue{Keys: []any{int32(1)}, Values: []any{"one"}}
	got := formatOrderedMapValue(m)
	if got != "{1=one}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{1=one}")
	}
}

func TestFormatOrderedMapValue_Empty(t *testing.T) {
	m := OrderedMapValue{Keys: []any{}, Values: []any{}}
	got := formatOrderedMapValue(m)
	if got != "{}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{}")
	}
}

func TestFormatOrderedMapValue_NilValue(t *testing.T) {
	m := OrderedMapValue{Keys: []any{"k"}, Values: []any{nil}}
	got := formatOrderedMapValue(m)
	if got != "{k=}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{k=}")
	}
}

func TestFormatOrderedMapValue_PreservesOrder(t *testing.T) {
	// Verifies that key order in output matches Keys slice.
	m := OrderedMapValue{
		Keys:   []any{"z", "a", "m"},
		Values: []any{int32(1), int32(2), int32(3)},
	}
	got := formatOrderedMapValue(m)
	expected := "{z=1, a=2, m=3}"
	if got != expected {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, expected)
	}
}
