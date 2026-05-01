package server

import (
	"testing"

	"github.com/posthog/duckgres/duckdbservice/arrowmap"
)

// formatOrderedMapValue tests. Live here in package server because the
// function under test calls formatValue (defined in conn.go) which switches
// on the duckdb-go driver's value types — it can't move to a duckdb-free
// subpackage.

func TestFormatOrderedMapValue_Basic(t *testing.T) {
	m := arrowmap.OrderedMapValue{Keys: []any{"a"}, Values: []any{int32(1)}}
	got := formatOrderedMapValue(m)
	if got != "{a=1}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{a=1}")
	}
}

func TestFormatOrderedMapValue_IntegerKeys(t *testing.T) {
	m := arrowmap.OrderedMapValue{Keys: []any{int32(1)}, Values: []any{"one"}}
	got := formatOrderedMapValue(m)
	if got != "{1=one}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{1=one}")
	}
}

func TestFormatOrderedMapValue_Empty(t *testing.T) {
	m := arrowmap.OrderedMapValue{Keys: []any{}, Values: []any{}}
	got := formatOrderedMapValue(m)
	if got != "{}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{}")
	}
}

func TestFormatOrderedMapValue_NilValue(t *testing.T) {
	m := arrowmap.OrderedMapValue{Keys: []any{"k"}, Values: []any{nil}}
	got := formatOrderedMapValue(m)
	if got != "{k=}" {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, "{k=}")
	}
}

func TestFormatOrderedMapValue_PreservesOrder(t *testing.T) {
	m := arrowmap.OrderedMapValue{
		Keys:   []any{"z", "a", "m"},
		Values: []any{int32(1), int32(2), int32(3)},
	}
	got := formatOrderedMapValue(m)
	expected := "{z=1, a=2, m=3}"
	if got != expected {
		t.Errorf("formatOrderedMapValue = %q, want %q", got, expected)
	}
}
