package properties

import (
	"fmt"
	"github.com/posthog/duckgres/tests/perf/core"
)

// Catalog compares the same immutable day using each reader-supported representation.
// The fixed IDs describe the workload; the manifest digest identifies its dataset.
func Catalog(m *Manifest) core.Catalog {
	targets := []core.Protocol{core.ProtocolPGWireUncached, core.ProtocolPGWireCached, core.ProtocolTrino, core.ProtocolAthena}
	c := core.Catalog{Name: "properties-perf-v1", Description: "Browser properties on one validated immutable day", DatasetScale: 1, WarmupIterations: 1, MeasureIterations: 4, Targets: targets}
	bounds := fmt.Sprintf("timestamp >= CAST('%s' AS TIMESTAMP WITH TIME ZONE) AND timestamp < CAST('%s' AS TIMESTAMP WITH TIME ZONE)", m.Start.UTC().Format("2006-01-02 15:04:05 UTC"), m.End.UTC().Format("2006-01-02 15:04:05 UTC"))
	for _, intent := range []string{"browser_breakdown", "chrome_events"} {
		for _, rep := range []string{"json", "struct", "variant"} {
			relation := `"properties_perf"."events_supported"`
			expr := `json_extract_string(properties, '$."$browser"')`
			qt := targets
			switch rep {
			case "struct":
				expr = `properties_typed."$browser"`
			case "variant":
				expr = `CAST(properties_variant."$browser" AS VARCHAR)`
				relation = `"properties_perf"."events_variant"`
				qt = targets[:2]
			}
			var sql string
			if intent == "browser_breakdown" {
				sql = fmt.Sprintf("SELECT %s AS browser, COUNT(*) AS event_count FROM %s WHERE %s AND %s IS NOT NULL GROUP BY 1 ORDER BY event_count DESC, browser ASC LIMIT 20", expr, relation, bounds, expr)
			} else {
				sql = fmt.Sprintf("SELECT event, COUNT(*) AS event_count FROM %s WHERE %s AND %s = 'Chrome' GROUP BY event ORDER BY event_count DESC, event ASC NULLS LAST LIMIT 20", relation, bounds, expr)
			}
			c.Queries = append(c.Queries, core.Query{QueryID: "properties_" + intent + "_v1__" + rep, IntentID: "properties." + intent + ".v1", Representation: rep, Targets: qt, PGWireSQL: sql})
		}
	}
	return c
}
