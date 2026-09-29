package perf

import (
	"os"
	"path/filepath"
	"testing"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

func TestDistinctProfileSelection(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_PROFILE_DISTINCT", "true")
	t.Setenv("DUCKGRES_SCENARIO_PROFILE_ORDERED_FUNNEL", "false")
	t.Setenv("DUCKGRES_SCENARIO_EXPERIMENT_ORDERED_FUNNEL", "false")
	t.Setenv("DUCKGRES_SCENARIO_PROFILE_RECIPIENT", "age1test")
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "age"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	catalog, err := perfcore.LoadCatalog("../../../perf/queries/ducklake_posthog_tables.yaml")
	if err != nil {
		t.Fatal(err)
	}
	catalog.Targets = []perfcore.Protocol{perfcore.ProtocolTrinoCached}
	result, recipient, err := profileCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if recipient == "" || result.WarmupIterations != 5 || result.MeasureIterations != 5 {
		t.Fatalf("invalid diagnostic sampling: recipient=%q warmup=%d measured=%d", recipient, result.WarmupIterations, result.MeasureIterations)
	}
	runnable := 0
	for _, query := range result.Queries {
		if (perfcore.Catalog{Queries: []perfcore.Query{query}}).NeedsDriver(perfcore.ProtocolTrinoCached) {
			runnable++
		}
		if query.IntentID != "intent_events_distinct_persons_v5" {
			t.Fatalf("unexpected query %s", query.IntentID)
		}
	}
	if runnable != 1 {
		t.Fatalf("expected exactly one runnable distinct query, got %d", runnable)
	}
	catalog.Targets = []perfcore.Protocol{perfcore.ProtocolAthena}
	if _, _, err := profileCatalog(catalog); err == nil {
		t.Fatal("accepted Athena")
	}
	catalog.Targets = []perfcore.Protocol{perfcore.ProtocolTrinoCached}
	t.Setenv("DUCKGRES_SCENARIO_PROFILE_ORDERED_FUNNEL", "true")
	if _, _, err := profileCatalog(catalog); err == nil {
		t.Fatal("accepted conflicting profiles")
	}
}
