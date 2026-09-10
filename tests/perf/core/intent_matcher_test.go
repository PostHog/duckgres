package core

import "testing"

func TestIntentMatcherReturnsCanonicalSQLForBothProtocols(t *testing.T) {
	m := NewIntentMatcher()
	q := Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: "SELECT 1",
	}
	for _, protocol := range []Protocol{ProtocolPGWire, ProtocolPGWireUncached, ProtocolPGWireCached, ProtocolTrino} {
		if got, err := m.SQLFor(q, protocol); err != nil || got != "SELECT 1" {
			t.Fatalf("unexpected %s SQL result: sql=%q err=%v", protocol, got, err)
		}
	}
}

func TestIntentMatcherReturnsRenderedDialectSQL(t *testing.T) {
	matcher := NewIntentMatcher()
	query := Query{
		QueryID:   "property",
		PGWireSQL: `SELECT json_extract_string(properties, '$."$browser"')`,
		TrinoSQL:  `SELECT json_extract_scalar(properties, '$["$browser"]')`,
		AthenaSQL: `SELECT json_extract_scalar(properties, '$["$browser"]')`,
	}
	for _, protocol := range []Protocol{ProtocolPGWire, ProtocolPGWireUncached, ProtocolPGWireCached, ProtocolTrino, ProtocolAthena} {
		got, err := matcher.SQLFor(query, protocol)
		if err != nil || got != query.SQLForProtocol(protocol) {
			t.Errorf("%s SQL = %q, error = %v; want %q", protocol, got, err, query.SQLForProtocol(protocol))
		}
	}
}
