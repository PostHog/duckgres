package properties

import (
	"fmt"

	"github.com/posthog/duckgres/tests/perf/core"
)

// Catalog compares the selected dataset using each reader-supported representation.
func Catalog() core.Catalog {
	targets := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached, core.ProtocolAthena}
	c := core.Catalog{Name: "properties-perf-v1", Description: "Browser properties on the selected generated dataset", DatasetScale: 1, WarmupIterations: 1, MeasureIterations: 4, Targets: targets}
	for _, intent := range []string{"browser_breakdown", "chrome_events"} {
		for _, rep := range []string{"json", "struct", "variant"} {
			relation := `"properties_perf"."events_supported"`
			expr := `json_extract_string(properties, '$."$browser"')`
			qt := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolTrinoCached}
			switch rep {
			case "struct":
				expr = `properties_typed."$browser"`
				qt = targets[4:]
			case "variant":
				expr = `CAST(properties_variant['$browser'] AS VARCHAR)`
				relation = `"properties_perf"."events_variant"`
				qt = []core.Protocol{core.ProtocolTrinoCached}
			}
			var sql string
			if intent == "browser_breakdown" {
				sql = fmt.Sprintf("SELECT %s AS browser, COUNT(*) AS event_count FROM %s WHERE %s IS NOT NULL GROUP BY 1 ORDER BY event_count DESC, browser ASC LIMIT 20", expr, relation, expr)
			} else {
				sql = fmt.Sprintf("SELECT event, COUNT(*) AS event_count FROM %s WHERE %s = 'Chrome' GROUP BY event ORDER BY event_count DESC, event ASC NULLS LAST LIMIT 20", relation, expr)
			}
			q := core.Query{QueryID: "properties_" + intent + "_v1__" + rep, IntentID: "properties." + intent + ".v1", Representation: rep, Targets: qt, PGWireSQL: sql}
			if rep == "variant" {
				q.SkipReason = "Hoglake does not support the VARIANT representation"
			}
			c.Queries = append(c.Queries, q)
		}
	}
	return c
}
