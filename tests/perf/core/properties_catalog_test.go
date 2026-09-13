package core

import "testing"

func TestRepresentationCatalogRejectsInvalidRouting(t *testing.T) {
	for _, fields := range []string{
		"representation: typo",
		"representation: variant\n    targets: [athena]",
		"representation: struct\n    targets: [unknown]",
		"representation: struct\n    targets: [pgwire, pgwire]",
	} {
		raw := "name: properties\ndataset_scale: 1\nmeasure_iterations: 1\ntargets: [pgwire, trino, athena]\nqueries:\n  - query_id: browser\n    intent_id: browser\n    pgwire_sql: SELECT 1\n    " + fields + "\n"
		if _, err := ParseCatalog([]byte(raw)); err == nil {
			t.Errorf("accepted invalid routing: %s", fields)
		}
	}
}

func TestRepresentationCatalogAllowsTrinoVariant(t *testing.T) {
	for _, protocol := range []string{"trino", "trino_cached"} {
		raw := "name: properties\ndataset_scale: 1\nmeasure_iterations: 1\ntargets: [" + protocol + "]\nqueries:\n  - query_id: browser\n    intent_id: browser\n    pgwire_sql: SELECT 1\n    representation: variant\n    targets: [" + protocol + "]\n"
		if _, err := ParseCatalog([]byte(raw)); err != nil {
			t.Fatalf("variant reader must reach runtime validation: %v", err)
		}
	}
}
func TestValidationOnlyCatalogRequiresRepresentation(t *testing.T) {
	raw := "name: properties\ndataset_scale: 1\nmeasure_iterations: 1\ntargets: [pgwire]\nqueries:\n  - query_id: browser\n    intent_id: browser\n    pgwire_sql: SELECT 1\n    validation_only: true\n"
	if _, err := ParseCatalog([]byte(raw)); err == nil {
		t.Fatal("unlabeled validation-only query would never run")
	}
}
