package core

import "testing"

func TestRepresentationCatalogRejectsInvalidRouting(t *testing.T) {
	for _, fields := range []string{
		"representation: typo",
		"representation: variant\n    targets: [trino]",
		"representation: struct\n    targets: [unknown]",
		"representation: struct\n    targets: [pgwire, pgwire]",
	} {
		raw := "name: properties\ndataset_scale: 1\nmeasure_iterations: 1\ntargets: [pgwire, trino]\nqueries:\n  - query_id: browser\n    intent_id: browser\n    pgwire_sql: SELECT 1\n    " + fields + "\n"
		if _, err := ParseCatalog([]byte(raw)); err == nil {
			t.Errorf("accepted invalid routing: %s", fields)
		}
	}
}
