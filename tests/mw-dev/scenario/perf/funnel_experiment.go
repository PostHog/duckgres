package perf

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

// Preserve the catalog's starts definition, physical relation and all join/time
// predicates. Fail closed if the bounded query no longer has the expected shape.
func singleStartFunnel(query perfcore.Query) (perfcore.Query, error) {
	prefix, rest, ok := strings.Cut(query.CanonicalSQL(), "), completions AS (")
	if !ok {
		return query, fmt.Errorf("unexpected funnel CTE shape")
	}
	_, joined, ok := strings.Cut(rest, "FROM starts s JOIN ")
	if !ok {
		return query, fmt.Errorf("unexpected funnel join shape")
	}
	predicates, _, ok := strings.Cut(joined, "GROUP BY s.team_id, s.person_id")
	if !ok || strings.Count(predicates, "WHERE e.event") != 1 {
		return query, fmt.Errorf("unexpected funnel predicate shape")
	}
	query.QueryID += "__single_start"
	query.PGWireSQL = prefix + `), matched AS (
 SELECT s.team_id, s.person_id,
        MAX(CASE WHEN e.person_id IS NOT NULL THEN 1 END) AS completed
 FROM starts s LEFT JOIN ` + strings.Replace(predicates, "WHERE e.event", "AND e.event", 1) + `
 GROUP BY s.team_id, s.person_id
 )
 SELECT COUNT(*) AS started_persons, COUNT(completed) AS completed_persons
 FROM matched
 ) coverage_query`
	return query, nil
}

// These paired samples are separate from the catalog's ordinary measurements.
// Fetching the complete aggregate result permits equality checks on every run.
func measureFunnelExperiment(ctx context.Context, reader perfcore.ResultReader, original perfcore.Query, expected [][]*string) (map[string]any, perfcore.Query, error) {
	rewritten, err := singleStartFunnel(original)
	document := map[string]any{"name": "single_start", "sql": rewritten.CanonicalSQL(), "warmup_iterations": 1, "measure_iterations": 4}
	if err != nil {
		return document, rewritten, err
	}
	queries := []perfcore.Query{original, rewritten}
	// First validate both values; do not time a non-equivalent rewrite.
	for _, q := range queries {
		result, err := reader.ReadResults(ctx, q, nil)
		if err != nil {
			return document, rewritten, err
		}
		if !reflect.DeepEqual(expected, result) {
			return document, rewritten, fmt.Errorf("funnel experiment validation result mismatch")
		}
	}
	// Explicit warmups follow validation. Neither contributes a timing sample.
	for _, q := range queries {
		result, err := reader.ReadResults(ctx, q, nil)
		if err != nil {
			return document, rewritten, err
		}
		if !reflect.DeepEqual(expected, result) {
			return document, rewritten, fmt.Errorf("funnel experiment warmup result mismatch")
		}
	}
	samples := make([]map[string]any, 0, 8)
	document["samples"] = samples
	// ABBA ABBA gives each variant four samples, balancing position within pairs.
	for _, variant := range []int{0, 1, 1, 0, 0, 1, 1, 0} {
		started := time.Now()
		result, err := reader.ReadResults(ctx, queries[variant], nil)
		elapsed := time.Since(started)
		if err != nil {
			return document, rewritten, err
		}
		if !reflect.DeepEqual(expected, result) {
			return document, rewritten, fmt.Errorf("funnel experiment measured result mismatch")
		}
		label := "original"
		if variant == 1 {
			label = "single_start"
		}
		samples = append(samples, map[string]any{"variant": label, "duration_ms": float64(elapsed) / float64(time.Millisecond)})
		document["samples"] = samples
	}
	document["results"] = expected
	return document, rewritten, nil
}
