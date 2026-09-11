package core

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestHoglakeFullCorpusCoverage(t *testing.T) {
	raw, err := os.ReadFile("../queries/ducklake_posthog_tables.yaml")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := catalogEntries(raw)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := ParseCatalog(raw)
	if err != nil {
		t.Fatal(err)
	}
	targeted := false
	for _, target := range catalog.Targets {
		if target == ProtocolTrinoHoglake {
			targeted = true
		}
	}
	if !targeted {
		t.Fatal("full corpus must target trino_hoglake")
	}
	for _, entry := range entries {
		if entry.paired == nil {
			continue
		}
		count := 0
		for _, query := range catalog.Queries {
			if query.IntentID != entry.paired.IntentID || !querySupportsProtocol(query, ProtocolTrinoHoglake) {
				continue
			}
			count++
			for _, reference := range catalog.Queries {
				if reference.IntentID == query.IntentID && reference.StorageTarget == StorageTargetDuckLakeTable && reference.CanonicalSQL() != query.CanonicalSQL() {
					t.Fatalf("intent %s differs from the canonical DuckLake SQL", query.IntentID)
				}
			}
			if query.StorageTarget != StorageTargetHoglakeTable {
				t.Fatalf("wrong Hoglake storage: %+v", query)
			}
			if _, err := NewIntentMatcher().SQLFor(query, ProtocolTrinoHoglake); err != nil {
				t.Fatal(err)
			}
			for _, target := range []Protocol{ProtocolPGWire, ProtocolTrino, ProtocolTrinoCached, ProtocolAthena} {
				if querySupportsProtocol(query, target) {
					t.Fatalf("Hoglake query leaked to %s", target)
				}
			}
		}
		if count != 1 {
			t.Fatalf("intent %s has %d Hoglake queries, want 1", entry.paired.IntentID, count)
		}
	}
}

func TestHoglakeFuturePairedIntentSharesSQLAcrossSeparateCatalogs(t *testing.T) {
	raw := strings.Replace(pairedCatalogYAML(`
paired_queries:
 - query_id_base: future_join
   intent_id: future_join
   sql_template: SELECT e.person_id FROM {{ relation "events" }} e JOIN {{ relation "persons" }} p ON e.person_id = p.id
`), "targets: [pgwire]", "targets: [pgwire, trino_hoglake]", 1)
	_, err := ParseCatalog([]byte(raw))
	if err == nil || !strings.Contains(err.Error(), "hoglake_table") {
		t.Fatalf("missing variant error: %v", err)
	}
	raw = strings.Replace(raw, "relation_variants:", "relation_variants:\n  hoglake_table: {events: posthog.events, persons: posthog.persons}", 1)
	catalog, err := ParseCatalog([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Queries) != 3 {
		t.Fatalf("got %d queries", len(catalog.Queries))
	}
	if catalog.Queries[1].CanonicalSQL() != catalog.Queries[2].CanonicalSQL() {
		t.Fatal("separate catalogs must share identical SQL")
	}
}

type failingHoglakeDriver struct{ calls int }

func (d *failingHoglakeDriver) Protocol() Protocol { return ProtocolTrinoHoglake }
func (d *failingHoglakeDriver) Close() error       { return nil }
func (d *failingHoglakeDriver) Execute(context.Context, Query, []any) (ExecutionResult, error) {
	d.calls++
	return ExecutionResult{}, fmt.Errorf("unsupported fixture query")
}

func TestHoglakeQueryFailuresRemainInFullCorpusResults(t *testing.T) {
	catalog, err := LoadCatalog("../queries/ducklake_posthog_tables.yaml")
	if err != nil {
		t.Fatal(err)
	}
	intents := map[string]bool{}
	for _, query := range catalog.Queries {
		intents[query.IntentID] = true
	}
	catalog.Targets = []Protocol{ProtocolTrinoHoglake}
	driver := &failingHoglakeDriver{}
	sink := &inMemorySink{}
	summary, err := NewQueryRunner(RunnerConfig{Catalog: catalog, Drivers: map[Protocol]ProtocolDriver{driver.Protocol(): driver}, Sink: sink}).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := len(intents) * catalog.MeasureIterations
	if summary.TotalQueries != want || summary.TotalErrors != want || len(sink.results) != want {
		t.Fatalf("failures omitted: %+v results=%d want=%d", summary, len(sink.results), want)
	}
	for _, result := range sink.results {
		if result.Status != "error" || result.Error != "unsupported fixture query" || result.Protocol != driver.Protocol() {
			t.Fatalf("lost failure details: %+v", result)
		}
	}
}
