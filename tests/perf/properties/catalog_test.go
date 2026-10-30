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

var trinoTargets = []core.Protocol{core.ProtocolTrino, core.ProtocolTrinoCached}

func TestCatalogMeasuresJSONOnEveryDuckgresAndTrinoTarget(t *testing.T) {
	jsonTargets := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached}
	seen := map[string]bool{}
	for _, q := range Catalog().Queries {
		want := jsonTargets
		if q.IntentID == "properties.lib_version_breakdown.v2" {
			want = trinoTargets // a Trino-only comparison of a key the shredded copy does not shred
		}
		switch q.Representation {
		case "json":
			seen[q.IntentID] = true
			if !slices.Equal(q.Targets, want) || q.SkipReason != "" {
				t.Fatalf("%s targets = %v (skip %q), want %v measured", q.QueryID, q.Targets, q.SkipReason, want)
			}
		case "struct":
			if !slices.Equal(q.Targets, []core.Protocol{core.ProtocolAthena}) || !strings.Contains(q.PGWireSQL, `properties_typed."$browser"`) {
				t.Fatalf("%s targets = %v, want Athena only, on the $browser STRUCT", q.QueryID, q.Targets)
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("JSON intents = %v, want all three properties intents", seen)
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
	if got, want := core.ProtocolTrino.RunLabel("variant_unshredded"), "trino (variant unshredded)"; got != want {
		t.Fatalf("RunLabel = %q, want %q", got, want)
	}
	if got, want := core.ProtocolTrinoCached.RunLabel("variant_unshredded"), "trino (cache+variant unshredded)"; got != want {
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
		if q.SkipReason != "" || !slices.Equal(q.Targets, trinoTargets) {
			t.Fatalf("%s targets = %v (skip %q), want uncached and cached Trino measured", q.QueryID, q.Targets, q.SkipReason)
		}
		key := "$browser"
		if q.IntentID == "properties.lib_version_breakdown.v2" {
			key = "$lib_version"
		}
		if !strings.Contains(q.PGWireSQL, `CAST(properties_variant['`+key+`'] AS VARCHAR)`) || !strings.Contains(q.PGWireSQL, `"properties_perf"."events_variant" `) {
			t.Fatalf("%s must subscript the shredded column so Hoglake prunes to %s: %s", q.QueryID, key, q.PGWireSQL)
		}
	}
	if seen != 3 {
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

// The unshredded series is the shredded one's exact query on the unshredded
// copy, so the two VARIANT forms differ only in the file layout they read.
func TestTrinoMeasuresUnshreddedVariantNextToJSONAndShredded(t *testing.T) {
	shredded := map[string]core.Query{}
	for _, q := range Catalog().Queries {
		if q.Representation == "variant" {
			shredded[q.IntentID] = q
		}
	}
	seen := 0
	for _, q := range Catalog().Queries {
		if q.Representation != "variant_unshredded" {
			continue
		}
		seen++
		want, ok := shredded[q.IntentID]
		if !ok || q.SkipReason != "" || !slices.Equal(q.Targets, trinoTargets) {
			t.Fatalf("%s targets = %v (skip %q), want uncached and cached Trino next to the shredded series", q.QueryID, q.Targets, q.SkipReason)
		}
		if got := strings.ReplaceAll(q.PGWireSQL, `"events_variant_unshredded"`, `"events_variant"`); got != want.PGWireSQL || got == q.PGWireSQL {
			t.Fatalf("%s must be %s on events_variant_unshredded: %s", q.QueryID, want.QueryID, q.PGWireSQL)
		}
	}
	if seen != 3 {
		t.Fatalf("unshredded VARIANT queries = %d, want one per intent", seen)
	}
	raw, err := yaml.Marshal(Catalog())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ParseCatalog(raw); err != nil {
		t.Fatal(err)
	}
}

// Trino's JSON path needs bracket notation for every dollar-prefixed key, the
// unshredded $lib_version as well as $browser.
func TestTrinoJSONQueriesUseBracketPathsForEveryKey(t *testing.T) {
	for _, q := range Catalog().Queries {
		if q.Representation != "json" {
			continue
		}
		sql, err := q.SQLFor(core.ProtocolTrino)
		if err != nil {
			t.Fatal(err)
		}
		bracketed := strings.Contains(sql, `json_extract_scalar(properties, '$["$browser"]')`) ||
			strings.Contains(sql, `json_extract_scalar(properties, '$["$lib_version"]')`)
		if strings.Contains(sql, `$."`) || strings.Contains(sql, "json_extract_string(") || !bracketed {
			t.Fatalf("%s Trino SQL = %s", q.QueryID, sql)
		}
	}
}
