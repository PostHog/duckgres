package core

import (
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

// StorageTarget identifies the physical relation family selected for a paired
// catalog query. It is runtime-only metadata; artifacts continue to use the
// existing query ID and intent ID fields.
type StorageTarget string

const (
	StorageTargetRawView        StorageTarget = "raw_view"
	StorageTargetDuckLakeTable  StorageTarget = "ducklake_table"
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
	Representation string         `yaml:"representation,omitempty" json:"representation,omitempty"`
	Targets        []Protocol     `yaml:"targets,omitempty" json:"-"`
	QueryID        string         `yaml:"query_id"`
	IntentID       string         `yaml:"intent_id"`
	Tags           []string       `yaml:"tags"`
	Params         map[string]any `yaml:"params"`
	PGWireSQL      string         `yaml:"pgwire_sql"`
	StorageTarget  StorageTarget  `yaml:"-" json:"-"`
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
// PGWire and Trino keep their existing artifact contract.
type ServiceMetrics struct {
	QueueDuration    time.Duration `json:"queue_duration_ns"`
	PlanningDuration time.Duration `json:"planning_duration_ns"`
	EngineDuration   time.Duration `json:"engine_duration_ns"`
	ServiceDuration  time.Duration `json:"service_duration_ns"`
	BytesScanned     int64         `json:"bytes_scanned"`
	DPUCount         float64       `json:"dpu_count"`
	ResultReused     bool          `json:"result_reused"`
	EngineVersion    string        `json:"engine_version"`
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

type RunSummary struct {
	RunID          string    `json:"run_id"`
	DatasetVersion string    `json:"dataset_version"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	TotalQueries   int       `json:"total_queries"`
	TotalErrors    int       `json:"total_errors"`
	WarmupQueries  int       `json:"warmup_queries"`
}

// SQLFor preserves canonical SQL except for the JSON scalar extractor in the
// explicitly labeled properties workload. Its paths and semantics stay shared.
func (q Query) SQLFor(protocol Protocol) (string, error) {
	sql := q.CanonicalSQL()
	if q.Representation != "" && (protocol == ProtocolTrino || protocol == ProtocolTrinoCached || protocol == ProtocolAthena) {
		sql = strings.ReplaceAll(sql, "json_extract_string(", "json_extract_scalar(")
	}
	if q.Representation != "" && protocol == ProtocolAthena {
		// Athena uses the run-specific Glue database, with a flat external table.
		sql = strings.ReplaceAll(sql, `"properties_perf"."events_supported"`, `"properties_events_supported"`)
	}
	return sql, nil
}
