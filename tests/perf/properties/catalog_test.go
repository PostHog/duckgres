package properties

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"gopkg.in/yaml.v3"

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
	if got, want := core.ProtocolTrino.RunLabel("variant"), "trino (variant)"; got != want {
		t.Fatalf("RunLabel = %q, want %q", got, want)
	}
}

func TestTrinoMeasuresShreddedVariantOnBothCacheSettings(t *testing.T) {
	seen := 0
	for _, q := range Catalog().Queries {
		if q.Representation != "variant" {
			continue
		}
		seen++
		if q.SkipReason != "" || !slices.Equal(q.Targets, []core.Protocol{core.ProtocolTrino, core.ProtocolTrinoCached}) {
			t.Fatalf("%s targets = %v (skip %q), want uncached and cached Trino measured", q.QueryID, q.Targets, q.SkipReason)
		}
		if !strings.Contains(q.PGWireSQL, `CAST(properties_variant['$browser'] AS VARCHAR)`) || !strings.Contains(q.PGWireSQL, `"properties_perf"."events_variant"`) {
			t.Fatalf("%s must subscript the shredded column so Hoglake prunes to $browser: %s", q.QueryID, q.PGWireSQL)
		}
	}
	if seen != 2 {
		t.Fatalf("VARIANT queries = %d, want one per intent", seen)
	}
	// The scenario hands the catalog over as YAML; it must parse and validate.
	raw, err := yaml.Marshal(Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ParseCatalog(raw); err != nil {
		t.Fatal(err)
	}
}

type fakeGlue struct{ params map[string]string }

func (f fakeGlue) GetTable(context.Context, *glue.GetTableInput, ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	return &glue.GetTableOutput{Table: &gluetypes.Table{Parameters: f.params}}, nil
}

func TestVariantLocationComesFromTheAthenaTableParameter(t *testing.T) {
	got, err := VariantLocation(context.Background(), fakeGlue{map[string]string{VariantLocationParameter: "s3://b/run/canonical/"}}, "db", "t")
	if err != nil || got != "s3://b/run/canonical/" {
		t.Fatalf("VariantLocation = %q, %v", got, err)
	}
	if _, err := VariantLocation(context.Background(), fakeGlue{}, "db", "t"); err == nil || !strings.Contains(err.Error(), VariantLocationParameter) {
		t.Fatalf("missing parameter must fail naming it, got %v", err)
	}
}

func TestIntentIDsMarkTheMultiDayFixture(t *testing.T) {
	// The fixture changed size at v2; reusing v1 IDs would divide new latencies
	// by old fixed baselines on the perf dashboard.
	for _, q := range Catalog().Queries {
		if !strings.HasSuffix(q.IntentID, ".v2") || !strings.Contains(q.QueryID, "_v2__") {
			t.Fatalf("query %s intent %s, want v2 identifiers", q.QueryID, q.IntentID)
		}
	}
}
