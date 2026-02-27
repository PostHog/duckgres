package core

import (
	"strings"
	"testing"
)

func TestParseCatalogSuccess(t *testing.T) {
	raw := `
name: smoke
description: smoke suite
seed: 7
dataset_scale: 1
targets: [pgwire, flight]
warmup_iterations: 1
measure_iterations: 2
queries:
  - query_id: q1
    intent_id: i1
    tags: [smoke]
    params:
      customer_id: 42
    pgwire_sql: SELECT 42
    duckhog_sql: SELECT 42
`
	catalog, err := ParseCatalog([]byte(raw))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	if catalog.Name != "smoke" {
		t.Fatalf("expected name smoke, got %q", catalog.Name)
	}
	if len(catalog.Queries) != 1 {
		t.Fatalf("expected one query, got %d", len(catalog.Queries))
	}
	if catalog.Queries[0].QueryID != "q1" || catalog.Queries[0].IntentID != "i1" {
		t.Fatalf("unexpected query identity: %+v", catalog.Queries[0])
	}
}

func TestParseCatalogRejectsDuplicateQueryIDs(t *testing.T) {
	raw := `
name: bad
description: dup query ids
seed: 1
dataset_scale: 1
targets: [pgwire]
warmup_iterations: 0
measure_iterations: 1
queries:
  - query_id: q1
    intent_id: i1
    pgwire_sql: SELECT 1
    duckhog_sql: SELECT 1
  - query_id: q1
    intent_id: i2
    pgwire_sql: SELECT 2
    duckhog_sql: SELECT 2
`
	_, err := ParseCatalog([]byte(raw))
	if err == nil {
		t.Fatalf("expected duplicate query_id to fail")
	}
	if !strings.Contains(err.Error(), "duplicate query_id") {
		t.Fatalf("expected duplicate query_id error, got %v", err)
	}
}
