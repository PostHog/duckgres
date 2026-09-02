package core

import "testing"

func TestIntentMatcherReturnsCanonicalSQLForBothProtocols(t *testing.T) {
	m := NewIntentMatcher()
	q := Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: "SELECT 1",
	}
	for _, protocol := range []Protocol{ProtocolPGWire, ProtocolTrino} {
		if got, err := m.SQLFor(q, protocol); err != nil || got != "SELECT 1" {
			t.Fatalf("unexpected %s SQL result: sql=%q err=%v", protocol, got, err)
		}
	}
}
