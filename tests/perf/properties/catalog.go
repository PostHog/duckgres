package properties

import (
	"fmt"

	"github.com/posthog/duckgres/tests/perf/core"
)

// Catalog compares the selected dataset using each reader-supported representation.
func Catalog() core.Catalog {
	targets := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached, core.ProtocolAthena}
	trino := []core.Protocol{core.ProtocolTrino, core.ProtocolTrinoCached}
	c := core.Catalog{Name: "properties-perf-v1", Description: "Browser properties on the selected generated dataset", DatasetScale: 1, WarmupIterations: 1, MeasureIterations: 4, Targets: targets}
	// $browser is shredded in the shredded copy; $lib_version is not, so its
	// VARIANT reads go through the residual lookup. That intent compares Trino
	// only: the VARIANT decision is Trino's, and STRUCT holds $browser alone.
	for _, intent := range []struct{ name, key string }{
		{"browser_breakdown", "$browser"}, {"chrome_events", "$browser"}, {"lib_version_breakdown", "$lib_version"},
	} {
		for _, rep := range []string{"json", "struct", "variant", "variant_unshredded"} {
			relation := `"properties_perf"."events_supported"`
			expr := fmt.Sprintf(`json_extract_string(properties, '$."%s"')`, intent.key)
			qt := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached}
			if intent.key != "$browser" {
				qt = trino
			}
			switch rep {
			case "struct":
				if intent.key != "$browser" {
					continue
				}
				expr = `properties_typed."$browser"`
				qt = targets[4:]
			case "variant", "variant_unshredded":
				// Hoglake pushes the subscript down to the key's sub-column when
				// the file shreds it (PostHog/trino#100); otherwise it reads the
				// whole value and looks the key up.
				expr = fmt.Sprintf(`CAST(properties_variant['%s'] AS VARCHAR)`, intent.key)
				relation = `"properties_perf"."events_variant"`
				if rep == "variant_unshredded" {
					relation = `"properties_perf"."events_variant_unshredded"`
				}
				qt = trino
			}
			var sql string
			switch intent.name {
			case "browser_breakdown":
				sql = fmt.Sprintf("SELECT %s AS browser, COUNT(*) AS event_count FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY event_count DESC, browser ASC LIMIT 20", expr, relation, expr)
			case "lib_version_breakdown":
				sql = fmt.Sprintf("SELECT %s AS lib_version, COUNT(*) AS event_count FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY event_count DESC, lib_version ASC LIMIT 20", expr, relation, expr)
			default:
				sql = fmt.Sprintf("SELECT event, COUNT(*) AS event_count FROM %s WHERE %s = 'Chrome' GROUP BY event ORDER BY event_count DESC, event ASC NULLS LAST LIMIT 20", relation, expr)
			}
			// v2: the fixture became a contiguous multi-day range (about 40x the
			// v1 single day), so v1 latencies and baselines are not comparable.
			q := core.Query{QueryID: "properties_" + intent.name + "_v2__" + rep, IntentID: "properties." + intent.name + ".v2", Representation: rep, Targets: qt, PGWireSQL: sql}
			c.Queries = append(c.Queries, q)
		}
	}
	return c
}
