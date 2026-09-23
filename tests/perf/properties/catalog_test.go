package properties

import (
	"slices"
	"testing"

	"github.com/posthog/duckgres/tests/perf/core"
)

func TestCatalogMeasuresJSONOnEveryDuckgresAndTrinoTarget(t *testing.T) {
	jsonTargets := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached}
	seen := map[string]bool{}
	for _, q := range Catalog().Queries {
		switch q.Representation {
		case "json":
			seen[q.IntentID] = true
			if !slices.Equal(q.Targets, jsonTargets) || q.SkipReason != "" {
				t.Fatalf("%s targets = %v (skip %q), want every Duckgres and Trino target measured", q.QueryID, q.Targets, q.SkipReason)
			}
		case "struct":
			if !slices.Equal(q.Targets, []core.Protocol{core.ProtocolAthena}) {
				t.Fatalf("%s targets = %v, want Athena only", q.QueryID, q.Targets)
			}
		}
	}
	if len(seen) != 2 {
		t.Fatalf("JSON intents = %v, want both properties intents", seen)
	}
}

func TestCachedTrinoJSONHasItsOwnRunLabel(t *testing.T) {
	if got, want := core.ProtocolTrinoCached.RunLabel("json"), "trino (cache)"; got != want {
		t.Fatalf("RunLabel = %q, want %q", got, want)
	}
	if got, want := core.ProtocolTrinoCached.RunLabel("variant"), "trino (cache+variant)"; got != want {
		t.Fatalf("RunLabel = %q, want %q", got, want)
	}
}
