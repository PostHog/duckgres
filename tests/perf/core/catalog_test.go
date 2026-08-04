package core

import (
	"reflect"
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
	if catalog.Queries[0].StorageTarget != "" {
		t.Fatalf("legacy query unexpectedly has storage target %q", catalog.Queries[0].StorageTarget)
	}
}

func TestParseCatalogExpandsPairedQueriesIntoStorageTargets(t *testing.T) {
	catalog, err := ParseCatalog([]byte(pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_events_daily
    intent_id: ph.events.daily.v1
    tags: [posthog, events, time-series]
    params:
      tenant_id: 42
    sql_template: |
      SELECT date_trunc('day', "timestamp") AS day, COUNT(*) AS events
      FROM {{ relation "events" }}
      WHERE "timestamp" >= TIMESTAMPTZ '2026-03-01 00:00:00+00'
      GROUP BY 1
      ORDER BY 1
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	if got, want := queryIDs(catalog), []string{"q_events_daily__raw_view", "q_events_daily__managed_table"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected generated query order: got %v want %v", got, want)
	}
	for _, query := range catalog.Queries {
		if query.IntentID != "ph.events.daily.v1" {
			t.Fatalf("generated query did not retain intent_id: %+v", query)
		}
		if !reflect.DeepEqual(query.Tags, []string{"posthog", "events", "time-series"}) || !reflect.DeepEqual(query.Params, map[string]any{"tenant_id": 42}) {
			t.Fatalf("generated query did not retain shared metadata: %+v", query)
		}
		if query.PGWireSQL != query.DuckhogSQL {
			t.Fatalf("generated SQL must be copied into both protocol fields: %+v", query)
		}
	}
	if got, want := catalog.Queries[0].StorageTarget, StorageTargetRawView; got != want {
		t.Fatalf("raw query target: got %q want %q", got, want)
	}
	if got, want := catalog.Queries[1].StorageTarget, StorageTargetManagedTable; got != want {
		t.Fatalf("managed query target: got %q want %q", got, want)
	}
	if got, want := catalog.Queries[0].PGWireSQL, "SELECT date_trunc('day', \"timestamp\") AS day, COUNT(*) AS events\nFROM \"frozen_v1\".\"events_file_view\"\nWHERE \"timestamp\" >= TIMESTAMPTZ '2026-03-01 00:00:00+00'\nGROUP BY 1\nORDER BY 1\n"; got != want {
		t.Fatalf("raw query SQL: got %q want %q", got, want)
	}
	if got, want := catalog.Queries[1].PGWireSQL, "SELECT date_trunc('day', \"timestamp\") AS day, COUNT(*) AS events\nFROM \"posthog\".\"events\"\nWHERE \"timestamp\" >= TIMESTAMPTZ '2026-03-01 00:00:00+00'\nGROUP BY 1\nORDER BY 1\n"; got != want {
		t.Fatalf("managed query SQL: got %q want %q", got, want)
	}
}

func TestParseCatalogExpandsMultipleRelationsInDeclarationOrder(t *testing.T) {
	catalog, err := ParseCatalog([]byte(pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_join
    intent_id: ph.join.v1
    tags: [posthog]
    params: {}
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }} e JOIN {{ relation "persons" }} p ON e.person_id = p.id
  - query_id_base: q_events
    intent_id: ph.events.v1
    tags: [posthog]
    params: {}
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
queries:
  - query_id: legacy_after
    intent_id: legacy.intent
    pgwire_sql: SELECT 1
    duckhog_sql: SELECT 1
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	if got, want := queryIDs(catalog), []string{"q_join__raw_view", "q_join__managed_table", "q_events__raw_view", "q_events__managed_table", "legacy_after"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected mixed catalog order: got %v want %v", got, want)
	}
	if got, want := catalog.Queries[0].PGWireSQL, "SELECT COUNT(*) FROM \"frozen_v1\".\"events_file_view\" e JOIN \"frozen_v1\".\"persons_file_view\" p ON e.person_id = p.id"; got != want {
		t.Fatalf("raw multi-relation SQL: got %q want %q", got, want)
	}
	if got, want := catalog.Queries[1].PGWireSQL, "SELECT COUNT(*) FROM \"posthog\".\"events\" e JOIN \"posthog\".\"persons\" p ON e.person_id = p.id"; got != want {
		t.Fatalf("managed multi-relation SQL: got %q want %q", got, want)
	}
}

func TestParseCatalogRejectsInvalidPairedDefinitions(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "missing variants", yaml: catalogYAML("relation_variants: {}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "storage variants"},
		{name: "invalid variant", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  archive: {events: posthog.events}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "storage variants"},
		{name: "missing base id", yaml: pairedCatalogYAML("paired_queries:\n  - intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "query_id_base"},
		{name: "missing intent", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "intent_id"},
		{name: "missing binding", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  managed_table: {persons: posthog.persons}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "missing relation binding"},
		{name: "unknown binding", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"orders\" }}\n"), want: "missing relation binding"},
		{name: "malicious identifier", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  managed_table: {events: 'posthog.events; DROP TABLE posthog.events'}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "invalid relation identifier"},
		{name: "whitespace identifier", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  managed_table: {events: 'posthog. events'}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "invalid relation identifier"},
		{name: "comment identifier", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  managed_table: {events: 'posthog.events -- managed table'}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "invalid relation identifier"},
		{name: "expression identifier", yaml: catalogYAML("relation_variants:\n  raw_view: {events: frozen_v1.events_file_view}\n  managed_table: {events: 'lower(posthog.events)'}\npaired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ relation \"events\" }}\n"), want: "invalid relation identifier"},
		{name: "unsupported action", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * FROM {{ .Events }}\n"), want: "unsupported template action"},
		{name: "no placeholder", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT 1\n"), want: "relation placeholder"},
		{name: "rendered write", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: INSERT INTO {{ relation \"events\" }} VALUES (1)\n"), want: "SELECT-only"},
		{name: "rendered select into", yaml: pairedCatalogYAML("paired_queries:\n  - query_id_base: q\n    intent_id: i\n    sql_template: SELECT * INTO derived_events FROM {{ relation \"events\" }}\n"), want: "SELECT-only"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCatalog([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseCatalog error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestParseCatalogRejectsGeneratedQueryIDCollisions(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "explicit legacy id", yaml: pairedCatalogYAML(`
queries:
  - query_id: q_events__raw_view
    intent_id: legacy.intent
    pgwire_sql: SELECT 1
    duckhog_sql: SELECT 1
paired_queries:
  - query_id_base: q_events
    intent_id: paired.intent
    sql_template: SELECT * FROM {{ relation "events" }}
`)},
		{name: "two paired bases", yaml: pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: one
    sql_template: SELECT * FROM {{ relation "events" }}
  - query_id_base: q_events
    intent_id: two
    sql_template: SELECT * FROM {{ relation "events" }}
`)},
		{name: "legacy duplicate", yaml: pairedCatalogYAML(`
queries:
  - query_id: q_legacy
    intent_id: one
    pgwire_sql: SELECT 1
    duckhog_sql: SELECT 1
  - query_id: q_legacy
    intent_id: two
    pgwire_sql: SELECT 2
    duckhog_sql: SELECT 2
`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCatalog([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), "duplicate query_id") {
				t.Fatalf("ParseCatalog error = %v, want duplicate query_id", err)
			}
		})
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
		return
	}
	if !strings.Contains(err.Error(), "duplicate query_id") {
		t.Fatalf("expected duplicate query_id error, got %v", err)
	}
}

func TestValidateReadOnlyCatalogAcceptsSelectOnlyQueries(t *testing.T) {
	catalog := Catalog{
		Queries: []Query{
			{
				QueryID:    "q1",
				IntentID:   "i1",
				PGWireSQL:  "SELECT $$into$$ AS label;",
				DuckhogSQL: "/* comment */ SELECT 1",
			},
		},
	}
	if err := ValidateReadOnlyCatalog(catalog); err != nil {
		t.Fatalf("ValidateReadOnlyCatalog returned error: %v", err)
	}
}

func TestValidateReadOnlyCatalogRejectsNonSelectQueries(t *testing.T) {
	catalog := Catalog{
		Queries: []Query{
			{
				QueryID:    "q_write",
				IntentID:   "i_write",
				PGWireSQL:  "INSERT INTO perf_orders VALUES (1, 'na', 100)",
				DuckhogSQL: "SELECT 1",
			},
		},
	}
	err := ValidateReadOnlyCatalog(catalog)
	if err == nil {
		t.Fatalf("expected non-select query to fail")
		return
	}
	if !strings.Contains(err.Error(), "SELECT-only") {
		t.Fatalf("expected SELECT-only error, got %v", err)
	}
}

func TestArtifactSinkContractRemainsUnchangedForPairedQueries(t *testing.T) {
	catalog, err := ParseCatalog([]byte(pairedCatalogYAML(`
paired_queries:
  - query_id_base: q_events
    intent_id: ph.events.v1
    sql_template: SELECT COUNT(*) FROM {{ relation "events" }}
`)))
	if err != nil {
		t.Fatalf("ParseCatalog returned error: %v", err)
	}
	if got, want := queryIDs(catalog), []string{"q_events__raw_view", "q_events__managed_table"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected runtime query representation: got %v want %v", got, want)
	}
	if got := catalog.Queries[0].StorageTarget; got != StorageTargetRawView {
		t.Fatalf("unexpected storage target %q", got)
	}
}

func pairedCatalogYAML(body string) string {
	return catalogYAML(strings.TrimSuffix(`
relation_variants:
  raw_view:
    events: frozen_v1.events_file_view
    persons: frozen_v1.persons_file_view
  managed_table:
    events: posthog.events
    persons: posthog.persons
	`, "\t") + body)
}

func catalogYAML(body string) string {
	return `
name: paired
description: paired query catalog
seed: 1
dataset_scale: 1
targets: [pgwire]
warmup_iterations: 0
measure_iterations: 1
` + body
}

func queryIDs(c Catalog) []string {
	ids := make([]string, 0, len(c.Queries))
	for _, query := range c.Queries {
		ids = append(ids, query.QueryID)
	}
	return ids
}
