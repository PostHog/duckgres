package core

import "time"

type Protocol string

const (
	ProtocolPGWire Protocol = "pgwire"
	ProtocolFlight Protocol = "flight"
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
	QueryID    string         `yaml:"query_id"`
	IntentID   string         `yaml:"intent_id"`
	Tags       []string       `yaml:"tags"`
	Params     map[string]any `yaml:"params"`
	PGWireSQL  string         `yaml:"pgwire_sql"`
	DuckhogSQL string         `yaml:"duckhog_sql"`
}

type ExecutionResult struct {
	Rows     int64
	Duration time.Duration
}

type QueryResult struct {
	QueryID          string        `json:"query_id"`
	IntentID         string        `json:"intent_id"`
	MeasureIteration int           `json:"measure_iteration"`
	Protocol         Protocol      `json:"protocol"`
	Status           string        `json:"status"`
	Error            string        `json:"error,omitempty"`
	ErrorClass       string        `json:"error_class,omitempty"`
	Rows             int64         `json:"rows"`
	Duration         time.Duration `json:"duration_ns"`
	StartedAt        time.Time     `json:"started_at"`
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
