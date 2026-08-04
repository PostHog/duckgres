package core

import "testing"

func TestIntentMatcherReturnsPGWireSQL(t *testing.T) {
	m := NewIntentMatcher()
	q := Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: "SELECT 1",
	}
	if got, err := m.SQLFor(q, ProtocolPGWire); err != nil || got != "SELECT 1" {
		t.Fatalf("unexpected pgwire SQL result: sql=%q err=%v", got, err)
	}
}
