package core

import (
	"strings"
	"testing"
)

func TestPairedJSONStringDialect(t *testing.T) {
	queries, err := expandPairedQuery(pairedQueryDefinition{QueryIDBase: "property", IntentID: "property", SQLTemplate: `SELECT {{ json_string "properties" "$browser" }} FROM {{ relation "events" }}`}, map[StorageTarget]map[string]string{
		StorageTargetRawView: {"events": "raw.events"}, StorageTargetDuckLakeTable: {"events": "lake.events"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range queries {
		for _, p := range []Protocol{ProtocolPGWire, ProtocolPGWireCached, ProtocolPGWireUncached, ProtocolTrino, ProtocolAthena} {
			want := `json_extract_string("properties", '$."$browser"')`
			if p == ProtocolTrino || p == ProtocolAthena {
				want = `json_extract_scalar("properties", '$["$browser"]')`
			}
			if !strings.Contains(q.SQLForProtocol(p), want) {
				t.Fatalf("%s: %s lacks %s", p, q.SQLForProtocol(p), want)
			}
		}
	}
	legacy := Query{PGWireSQL: "SELECT 1"}
	if legacy.SQLForProtocol(ProtocolTrino) != legacy.CanonicalSQL() {
		t.Fatal("legacy SQL changed")
	}
}

func TestJSONStringRejectsUnsafeActions(t *testing.T) {
	for _, action := range []string{
		`{{ json_string "properties); DROP TABLE x" "$browser" }}`,
		`{{ json_string "properties" "$.nested" }}`,
		`{{ json_string "properties" "" }}`,
		`{{ json_string "properties" "$browser" | printf }}`,
		`{{ json_string "properties" "$browser" "extra" }}`,
		`'{{ json_string "properties" "$browser" }}'`,
		`/* {{ json_string "properties" "$browser" }} */`,
	} {
		_, err := expandPairedQuery(pairedQueryDefinition{QueryIDBase: "bad", IntentID: "bad", SQLTemplate: "SELECT " + action + ` FROM {{ relation "events" }}`}, map[StorageTarget]map[string]string{StorageTargetRawView: {"events": "raw.events"}, StorageTargetDuckLakeTable: {"events": "lake.events"}})
		if err == nil {
			t.Errorf("accepted %s", action)
		}
	}
}
