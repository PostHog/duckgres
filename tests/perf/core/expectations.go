package core

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// QueryExpectations bounds provider statistics for one query on one
// protocol. The perf gate evaluates them over the measured iterations after a
// run; a nil bound is not checked. Latency is deliberately not boundable:
// wall-clock time on a shared lane is too noisy to gate on, while split counts
// and bytes scanned expose plan regressions deterministically.
type QueryExpectations struct {
	// MaxTotalSplits bounds Trino's total split (driver) count.
	MaxTotalSplits *int64 `json:"max_total_splits,omitempty"`
	// MaxBytesScanned bounds the provider's bytes scanned (Trino physical
	// input, Athena data scanned).
	MaxBytesScanned *int64 `json:"max_bytes_scanned,omitempty"`
}

const (
	expectationMaxTotalSplits  = "max_total_splits"
	expectationMaxBytesScanned = "max_bytes_scanned"
)

var byteSizePattern = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|TiB)$`)

var byteSizeUnits = map[string]float64{
	"KiB": 1 << 10,
	"MiB": 1 << 20,
	"GiB": 1 << 30,
	"TiB": 1 << 40,
}

// parseExpectations reads an optional `expectations:` mapping of protocol to
// bounds. Unknown bound names fail loudly so a typo cannot disable the gate.
func parseExpectations(owner string, node yaml.Node) (map[Protocol]QueryExpectations, error) {
	if node.Kind == 0 {
		return nil, nil
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%s expectations: %s", owner, fmt.Sprintf(format, args...))
	}
	if node.Kind != yaml.MappingNode {
		return nil, fail("must be a mapping of protocol to bounds")
	}
	expectations := make(map[Protocol]QueryExpectations, len(node.Content)/2)
	for index := 0; index < len(node.Content); index += 2 {
		protocol := Protocol(node.Content[index].Value)
		value := node.Content[index+1]
		if _, duplicate := expectations[protocol]; duplicate {
			return nil, fail("duplicate protocol %q", protocol)
		}
		if value.Kind != yaml.MappingNode {
			return nil, fail("protocol %q must map bound names to values", protocol)
		}
		var bounds QueryExpectations
		for boundIndex := 0; boundIndex < len(value.Content); boundIndex += 2 {
			name, raw := value.Content[boundIndex].Value, value.Content[boundIndex+1]
			if raw.Kind != yaml.ScalarNode {
				return nil, fail("protocol %q %s must be a scalar", protocol, name)
			}
			switch name {
			case expectationMaxTotalSplits:
				limit, err := strconv.ParseInt(raw.Value, 10, 64)
				if err != nil || limit < 0 {
					return nil, fail("protocol %q %s %q must be a non-negative integer", protocol, name, raw.Value)
				}
				bounds.MaxTotalSplits = &limit
			case expectationMaxBytesScanned:
				limit, err := parseByteSize(raw.Value)
				if err != nil {
					return nil, fail("protocol %q %s %q must be a non-negative byte count or a size in KiB, MiB, GiB, or TiB", protocol, name, raw.Value)
				}
				bounds.MaxBytesScanned = &limit
			default:
				return nil, fail("protocol %q has unknown expectation %q (supported: %s, %s)", protocol, name, expectationMaxTotalSplits, expectationMaxBytesScanned)
			}
		}
		if bounds.MaxTotalSplits == nil && bounds.MaxBytesScanned == nil {
			return nil, fail("protocol %q sets no bound", protocol)
		}
		expectations[protocol] = bounds
	}
	if len(expectations) == 0 {
		return nil, nil
	}
	return expectations, nil
}

func parseByteSize(text string) (int64, error) {
	if value, err := strconv.ParseInt(text, 10, 64); err == nil {
		if value < 0 {
			return 0, errors.New("negative byte size")
		}
		return value, nil
	}
	match := byteSizePattern.FindStringSubmatch(strings.TrimSpace(text))
	if match == nil {
		return 0, errors.New("invalid byte size")
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, err
	}
	bytes := math.Round(value * byteSizeUnits[match[2]])
	if bytes > math.MaxInt64 {
		return 0, errors.New("byte size overflows")
	}
	return int64(bytes), nil
}

// validateExpectations checks bounds against the protocols a query can run on.
func validateExpectations(owner string, expectations map[Protocol]QueryExpectations, targets []Protocol) error {
	for _, protocol := range sortedExpectationProtocols(expectations) {
		targeted := false
		for _, target := range targets {
			if target == protocol {
				targeted = true
				break
			}
		}
		if !targeted {
			return fmt.Errorf("%s expectations: protocol %q is not a target of this query", owner, protocol)
		}
		if expectations[protocol].MaxTotalSplits != nil && protocol != ProtocolTrino && protocol != ProtocolTrinoCached {
			return fmt.Errorf("%s expectations: %s applies only to Trino targets, not %q", owner, expectationMaxTotalSplits, protocol)
		}
	}
	return nil
}

// expectationsForStorageTarget keeps the bounds of protocols that execute a
// paired query's variant, so each bound lives on the query it applies to.
func expectationsForStorageTarget(expectations map[Protocol]QueryExpectations, target StorageTarget) map[Protocol]QueryExpectations {
	var served map[Protocol]QueryExpectations
	for protocol, bounds := range expectations {
		if !querySupportsProtocol(Query{StorageTarget: target}, protocol) {
			continue
		}
		if served == nil {
			served = make(map[Protocol]QueryExpectations)
		}
		served[protocol] = bounds
	}
	return served
}

func sortedExpectationProtocols(expectations map[Protocol]QueryExpectations) []Protocol {
	return slices.Sorted(maps.Keys(expectations))
}

// HasExpectations reports whether any query declares perf gate bounds.
func (c Catalog) HasExpectations() bool {
	for _, query := range c.Queries {
		if len(query.Expectations) > 0 {
			return true
		}
	}
	return false
}

// ExpectationViolation is one bound that a measured (query, protocol) pair
// broke, or could not be checked because its metric was not captured.
type ExpectationViolation struct {
	QueryID  string
	Protocol Protocol
	Metric   string
	Bound    string
	Limit    int64
	// Missing reports that the metric was not captured, so the bound could
	// not be checked; a gate that cannot see its metric must not pass.
	Missing bool
	// Offending counts the measured iterations that exceeded the bound (or
	// lacked the metric) out of Measured successful measured iterations.
	Offending int
	Measured  int
	// Observed and Iteration identify the worst iteration; for a missing
	// metric, Iteration is the first iteration without it.
	Observed      int64
	Iteration     int
	EngineQueryID string
	bytes         bool
}

func (v ExpectationViolation) String() string {
	format := strconv.FormatInt
	if v.bytes {
		format = func(value int64, _ int) string { return formatByteCount(value) }
	}
	if v.Missing {
		return fmt.Sprintf("%s on %s: %s was not captured in %d of %d measured iterations (first: iteration %d); %s %s cannot be checked",
			v.QueryID, v.Protocol, v.Metric, v.Offending, v.Measured, v.Iteration, v.Bound, format(v.Limit, 10))
	}
	engineQuery := ""
	if v.EngineQueryID != "" {
		engineQuery = ", Trino query " + v.EngineQueryID
	}
	return fmt.Sprintf("%s on %s: %s %s exceeds %s %s in %d of %d measured iterations (worst: iteration %d%s)",
		v.QueryID, v.Protocol, v.Metric, format(v.Observed, 10), v.Bound, format(v.Limit, 10), v.Offending, v.Measured, v.Iteration, engineQuery)
}

func formatByteCount(value int64) string {
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	if value < 1<<10 {
		return strconv.FormatInt(value, 10)
	}
	scaled, unit := float64(value)/(1<<10), units[0]
	for _, next := range units[1:] {
		if scaled < 1<<10 {
			break
		}
		scaled, unit = scaled/(1<<10), next
	}
	return fmt.Sprintf("%d (%.1f %s)", value, scaled, unit)
}

type boundedMetric struct {
	name    string
	bound   string
	bytes   bool
	limit   func(QueryExpectations) *int64
	observe func(*ServiceMetrics) (int64, bool)
}

var boundedMetrics = []boundedMetric{
	{
		name:  "total_splits",
		bound: expectationMaxTotalSplits,
		limit: func(e QueryExpectations) *int64 { return e.MaxTotalSplits },
		observe: func(m *ServiceMetrics) (int64, bool) {
			if m == nil || m.Trino == nil {
				return 0, false
			}
			return m.Trino.TotalSplits, true
		},
	},
	{
		name:  "bytes_scanned",
		bound: expectationMaxBytesScanned,
		bytes: true,
		limit: func(e QueryExpectations) *int64 { return e.MaxBytesScanned },
		observe: func(m *ServiceMetrics) (int64, bool) {
			if m == nil {
				return 0, false
			}
			return m.BytesScanned, true
		},
	},
}

// EvaluateExpectations checks every declared bound against the successful
// measured iterations of its (query, protocol) pair, in catalog order.
// Warmups and failed iterations are not checked (query errors fail the step on
// their own), nor are protocols outside the catalog's targets.
func EvaluateExpectations(catalog Catalog, results []QueryResult) []ExpectationViolation {
	type pair struct {
		queryID  string
		protocol Protocol
	}
	measured := make(map[pair][]QueryResult)
	for _, result := range results {
		if result.MeasureIteration <= 0 || result.Status != "ok" {
			continue
		}
		key := pair{queryID: result.QueryID, protocol: result.Protocol}
		measured[key] = append(measured[key], result)
	}
	var violations []ExpectationViolation
	for _, query := range catalog.Queries {
		for _, protocol := range catalog.Targets {
			bounds, ok := query.Expectations[protocol]
			if !ok {
				continue
			}
			iterations := measured[pair{queryID: query.QueryID, protocol: protocol}]
			if len(iterations) == 0 {
				continue
			}
			for _, metric := range boundedMetrics {
				limit := metric.limit(bounds)
				if limit == nil {
					continue
				}
				base := ExpectationViolation{
					QueryID: query.QueryID, Protocol: protocol, Metric: metric.name, Bound: metric.bound,
					Limit: *limit, Measured: len(iterations), bytes: metric.bytes,
				}
				exceeded, missing := base, base
				missing.Missing = true
				for _, result := range iterations {
					observed, captured := metric.observe(result.ServiceMetrics)
					if !captured {
						if missing.Offending == 0 {
							missing.Iteration = result.MeasureIteration
						}
						missing.Offending++
						continue
					}
					if observed <= *limit {
						continue
					}
					if exceeded.Offending == 0 || observed > exceeded.Observed {
						exceeded.Observed, exceeded.Iteration = observed, result.MeasureIteration
						exceeded.EngineQueryID = ""
						if result.ServiceMetrics.Trino != nil {
							exceeded.EngineQueryID = result.ServiceMetrics.Trino.QueryID
						}
					}
					exceeded.Offending++
				}
				if exceeded.Offending > 0 {
					violations = append(violations, exceeded)
				}
				if missing.Offending > 0 {
					violations = append(violations, missing)
				}
			}
		}
	}
	return violations
}

// ExpectationsError summarizes violations as the perf gate failure, or nil.
func ExpectationsError(violations []ExpectationViolation) error {
	if len(violations) == 0 {
		return nil
	}
	lines := make([]string, 0, len(violations))
	for _, violation := range violations {
		lines = append(lines, "- "+violation.String())
	}
	return fmt.Errorf("perf gate failed: %d expectation(s) violated:\n%s", len(violations), strings.Join(lines, "\n"))
}
