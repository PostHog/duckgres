package core

import "testing"

func TestIntentMatcherReturnsProtocolVariant(t *testing.T) {
	m := NewIntentMatcher()
	q := Query{
		QueryID:    "q1",
		IntentID:   "i1",
		PGWireSQL:  "SELECT 1",
		DuckhogSQL: "SELECT 2",
	}
	if got, err := m.SQLFor(q, ProtocolPGWire); err != nil || got != "SELECT 1" {
		t.Fatalf("unexpected pgwire SQL result: sql=%q err=%v", got, err)
	}
	if got, err := m.SQLFor(q, ProtocolFlight); err != nil || got != "SELECT 2" {
		t.Fatalf("unexpected flight SQL result: sql=%q err=%v", got, err)
	}
}

func TestIntentMatcherRejectsMissingVariant(t *testing.T) {
	m := NewIntentMatcher()
	q := Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: "SELECT 1",
	}
	if _, err := m.SQLFor(q, ProtocolFlight); err == nil {
		t.Fatalf("expected missing protocol SQL to fail")
	}
}
