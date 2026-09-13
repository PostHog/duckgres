package properties

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
	"gopkg.in/yaml.v3"
)

func TestCatalogUsesManifestBoundsAndEquivalentIntents(t *testing.T) {
	m := &Manifest{Start: time.Date(2026, 3, 17, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC)}
	c := Catalog(m)
	if len(c.Queries) != 6 {
		t.Fatalf("queries: %d", len(c.Queries))
	}
	seen := map[string]bool{}
	for _, q := range c.Queries {
		if seen[q.QueryID] {
			t.Fatalf("duplicate ID %s", q.QueryID)
		}
		seen[q.QueryID] = true
		for _, want := range []string{"2026-03-17 00:00:00 UTC", "2026-03-18 00:00:00 UTC", "LIMIT 20", "ORDER BY"} {
			if !strings.Contains(q.PGWireSQL, want) {
				t.Errorf("%s missing %s", q.QueryID, want)
			}
		}
		wantTargets := map[string][]core.Protocol{
			"json":    {core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached, core.ProtocolAthena},
			"variant": {core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached},
			"struct":  {core.ProtocolAthena},
		}
		if !reflect.DeepEqual(q.Targets, wantTargets[q.Representation]) {
			t.Errorf("%s targets=%v", q.QueryID, q.Targets)
		}
		if q.Representation == "variant" && !strings.Contains(q.PGWireSQL, "properties_variant['$browser']") {
			t.Errorf("variant SQL must use shared subscript access: %s", q.PGWireSQL)
		}
		if q.ValidationOnly != (q.Representation == "json") {
			t.Errorf("%s validation-only=%v", q.QueryID, q.ValidationOnly)
		}
		if strings.Contains(q.IntentID, "chrome") && !strings.Contains(q.PGWireSQL, "= 'Chrome'") {
			t.Fatal("missing exact Chrome filter")
		}
	}
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = core.ParseCatalog(raw); err != nil {
		t.Fatal(err)
	}
}
