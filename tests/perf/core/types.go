package core

import (
	"fmt"
	"strings"
	"time"
)

type Protocol string

const (
	ProtocolPGWire         Protocol = "pgwire"
	ProtocolPGWireUncached Protocol = "pgwire_uncached"
	ProtocolPGWireCached   Protocol = "pgwire_cached"
	ProtocolTrino          Protocol = "trino"
	ProtocolTrinoCached    Protocol = "trino_cached"
	ProtocolAthena         Protocol = "athena"
)

// RunLabel names the properties comparison without changing protocol routing or
// relabeling older/full-corpus measurements that lack representation metadata.
func (p Protocol) RunLabel(representation string) string {
	switch {
	case p == ProtocolPGWireUncached && representation == "json":
		return "duckgres (vanilla)"
	case p == ProtocolPGWireCached && representation == "json":
		return "duckgres (cache)"
	case p == ProtocolTrino && representation == "json":
		return "trino (vanilla)"
	case p == ProtocolTrinoCached && representation == "json":
		return "trino (cache)"
	case p == ProtocolTrinoCached && representation == "variant":
		return "trino (cache+variant)"
	default:
		return string(p)
	}
}

// StorageTarget identifies the physical relation family selected for a paired
// catalog query. It is runtime-only metadata; artifacts continue to use the
// existing query ID and intent ID fields.
type StorageTarget string

const (
	StorageTargetDuckLakeTable  StorageTarget = "ducklake_table"
	StorageTargetHoglakeTable   StorageTarget = "hoglake_table"
	StorageTargetAthenaExternal StorageTarget = "athena_external"
)

type Catalog struct {
	Name              string     `yaml:"name"`
	Description       string     `yaml:"description"`
	Seed              int64      `yaml:"seed"`
	DatasetScale      int        `yaml:"dataset_scale"`
	Targets           []Protocol `yaml:"targets"`
	WarmupIterations  int        `yaml:"warmup_iterations"`
	MeasureIterations int        `yaml:"measure_iterations"`
	Queries           []Query    `yaml:"queries"`
}

type Query struct {
	// SkipReason records an explicitly unsupported comparison without executing it.
	SkipReason string `yaml:"skip_reason,omitempty" json:"skip_reason,omitempty"`
	// ValidationOnly keeps a baseline in the correctness gate but excludes warmup and measurements.
	ValidationOnly bool           `yaml:"validation_only,omitempty" json:"validation_only,omitempty"`
	Representation string         `yaml:"representation,omitempty" json:"representation,omitempty"`
	Targets        []Protocol     `yaml:"targets,omitempty" json:"-"`
	QueryID        string         `yaml:"query_id"`
	IntentID       string         `yaml:"intent_id"`
	Tags           []string       `yaml:"tags"`
	Params         map[string]any `yaml:"params"`
	PGWireSQL      string         `yaml:"pgwire_sql"`
	StorageTarget  StorageTarget  `yaml:"-" json:"-"`
	// Expectations holds optional perf gate bounds per protocol, parsed from
	// the query's `expectations:` mapping by the catalog loader.
	Expectations map[Protocol]QueryExpectations `yaml:"-" json:"-"`
}

// CanonicalSQL returns the single rendered SQL statement shared by protocol
// drivers. The yaml name is retained for backward compatibility with existing
// catalogs; protocol-specific copies would allow benchmark definitions to
// drift.
func (q Query) CanonicalSQL() string {
	return q.PGWireSQL
}

type ExecutionResult struct {
	Rows           int64
	Duration       time.Duration
	ServiceMetrics *ServiceMetrics
}

// ServiceMetrics captures provider-side execution details which are useful
// for separating queueing and planning from engine work. It is optional so
// PGWire keeps its existing artifact contract. Athena fills the provider
// fields from its query statistics; Trino fills the shared timing and
// bytes-scanned fields from the coordinator plus the Trino-only details.
type ServiceMetrics struct {
	QueueDuration    time.Duration `json:"queue_duration_ns"`
	PlanningDuration time.Duration `json:"planning_duration_ns"`
	EngineDuration   time.Duration `json:"engine_duration_ns"`
	ServiceDuration  time.Duration `json:"service_duration_ns"`
	BytesScanned     int64         `json:"bytes_scanned"`
	DPUCount         float64       `json:"dpu_count"`
	ResultReused     bool          `json:"result_reused"`
	EngineVersion    string        `json:"engine_version"`
	// Trino is set only for Trino targets. Athena rows leave it nil, which
	// keeps their Trino-only artifact columns blank.
	Trino *TrinoQueryStats `json:"trino,omitempty"`
}

// Sources of Trino query statistics, recorded so a reader can tell the
// complete coordinator query info from the client-protocol fallback.
const (
	// TrinoStatsSourceQueryInfo is the coordinator's GET /v1/query/{queryId}.
	TrinoStatsSourceQueryInfo = "query_info"
	// TrinoStatsSourceStatement is the final client-protocol statement
	// response. It has no execution time or physical input row count.
	TrinoStatsSourceStatement = "statement_stats"
)

// TrinoQueryStats records the Trino query statistics that explain where a
// query's time went: planning a split per file, reading footers, scanning.
type TrinoQueryStats struct {
	QueryID         string `json:"query_id"`
	Source          string `json:"source"`
	TotalSplits     int64  `json:"total_splits"`
	CompletedSplits int64  `json:"completed_splits"`
	// PhysicalInputRows is nil when the statistics source does not report it.
	PhysicalInputRows *int64        `json:"physical_input_rows,omitempty"`
	CPUDuration       time.Duration `json:"cpu_duration_ns"`
	PeakMemoryBytes   int64         `json:"peak_memory_bytes"`
}

// HasEngineDuration reports whether EngineDuration was measured rather than
// left at zero by a statistics source that does not report it.
func (m ServiceMetrics) HasEngineDuration() bool {
	return m.Trino == nil || m.Trino.Source != TrinoStatsSourceStatement
}

type QueryResult struct {
	Representation   string          `json:"representation,omitempty"`
	QueryID          string          `json:"query_id"`
	IntentID         string          `json:"intent_id"`
	MeasureIteration int             `json:"measure_iteration"`
	Protocol         Protocol        `json:"protocol"`
	Status           string          `json:"status"`
	Error            string          `json:"error,omitempty"`
	ErrorClass       string          `json:"error_class,omitempty"`
	Rows             int64           `json:"rows"`
	Duration         time.Duration   `json:"duration_ns"`
	StartedAt        time.Time       `json:"started_at"`
	ServiceMetrics   *ServiceMetrics `json:"service_metrics,omitempty"`
}

// Suites published under one dataset. Dashboards filter on this closed set.
const (
	SuiteTables     = "tables"
	SuiteProperties = "properties"
)

// ValidateSuite rejects a suite outside the closed set, so a typo fails when a
// run starts rather than after hours of measurement at publish time.
func ValidateSuite(suite string) error {
	if suite != SuiteTables && suite != SuiteProperties {
		return fmt.Errorf("unknown suite %q (want %q or %q)", suite, SuiteTables, SuiteProperties)
	}
	return nil
}

type RunSummary struct {
	RunID          string `json:"run_id"`
	DatasetVersion string `json:"dataset_version"`
	// Suite names the comparison family inside a dataset ("tables" or
	// "properties"); one nightly publishes several suites under one dataset.
	Suite string `json:"suite,omitempty"`
	// FixtureVersion identifies the exact fixture a suite measured when it is
	// finer-grained than the dataset (e.g. the properties object inventory).
	FixtureVersion string `json:"fixture_version,omitempty"`
	// NightlyRunID names the run every suite of one nightly belongs to: the
	// table-suite run's ID. Consumers pair suites by it rather than by run IDs.
	NightlyRunID  string    `json:"nightly_run_id,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	TotalQueries  int       `json:"total_queries"`
	TotalErrors   int       `json:"total_errors"`
	WarmupQueries int       `json:"warmup_queries"`
}

// SQLFor preserves canonical SQL except for the JSON scalar extractor in the
// explicitly labeled properties workload. The browser key keeps the same
// semantics while using each engine's supported JSON path syntax.
func (q Query) SQLFor(protocol Protocol) (string, error) {
	sql := q.CanonicalSQL()
	if q.Representation != "" && (protocol == ProtocolTrino || protocol == ProtocolTrinoCached || protocol == ProtocolAthena) {
		sql = strings.ReplaceAll(sql, "json_extract_string(", "json_extract_scalar(")
		// Trino/Athena require bracket notation for the dollar-prefixed key.
		// Match the complete literal used by the generated properties catalog.
		sql = strings.ReplaceAll(sql, `'$."$browser"'`, `'$["$browser"]'`)
	}
	if q.Representation != "" && protocol == ProtocolAthena {
		// Athena uses the run-specific Glue database, with a flat external table.
		sql = strings.ReplaceAll(sql, `"properties_perf"."events_supported"`, `"properties_events_supported"`)
	}
	return sql, nil
}
