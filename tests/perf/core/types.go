package core

import (
	"fmt"
	"strings"
	"time"
)

type Protocol string

const (
	ProtocolPGWire Protocol = "pgwire"
	ProtocolTrino  Protocol = "trino"
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
	QueryID   string         `yaml:"query_id"`
	IntentID  string         `yaml:"intent_id"`
	Tags      []string       `yaml:"tags"`
	Params    map[string]any `yaml:"params"`
	PGWireSQL string         `yaml:"pgwire_sql"`
	TrinoSQL  string         `yaml:"trino_sql"`
}

// SupportsProtocol reports whether a query has SQL for a protocol. A catalog
// may contain engine-specific queries, which the runner skips for other
// configured protocols.
func (q Query) SupportsProtocol(protocol Protocol) bool {
	_, err := q.SQLFor(protocol)
	return err == nil
}

func (q Query) SQLFor(protocol Protocol) (string, error) {
	var sql string
	var field string
	switch protocol {
	case ProtocolPGWire:
		sql, field = q.PGWireSQL, "pgwire_sql"
	case ProtocolTrino:
		sql, field = q.TrinoSQL, "trino_sql"
	default:
		return "", fmt.Errorf("unknown protocol %q", protocol)
	}
	if strings.TrimSpace(sql) == "" {
		return "", fmt.Errorf("query %s missing %s", q.QueryID, field)
	}
	return sql, nil
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

// ProtocolEnvironment is the non-secret comparison metadata recorded per
// protocol in summary.json. Two engines' numbers are only comparable if the
// artifact says WHAT ran: engine and version, the pinned image (with digest
// where available), the topology that was actually ready, the catalog/schema
// identity, and the session time zone. Nothing here is or may become a
// credential.
type ProtocolEnvironment struct {
	Protocol Protocol `json:"protocol"`
	Engine   string   `json:"engine,omitempty"`
	Version  string   `json:"version,omitempty"`
	// ConnectorVersion identifies the storage connector (e.g. the Brikk
	// DuckLake connector build) where the engine exposes one.
	ConnectorVersion string `json:"connector_version,omitempty"`
	// Image is the pinned container image reference; a digest reference is the
	// authoritative record of both engine and connector build.
	Image string `json:"image,omitempty"`
	// RequestedWorkers / ReadyWorkers record the topology. They must match for
	// a run to mean what it claims.
	RequestedWorkers int    `json:"requested_workers,omitempty"`
	ReadyWorkers     int    `json:"ready_workers,omitempty"`
	Catalog          string `json:"catalog,omitempty"`
	Schema           string `json:"schema,omitempty"`
	TimeZone         string `json:"time_zone,omitempty"`
}

// Merge fills empty fields of e from other. Configured values (what the
// control plane told the scenario) win over probed ones, so a driver can only
// ADD detail, never contradict the recorded pin.
func (e ProtocolEnvironment) Merge(other ProtocolEnvironment) ProtocolEnvironment {
	if e.Engine == "" {
		e.Engine = other.Engine
	}
	if e.Version == "" {
		e.Version = other.Version
	}
	if e.ConnectorVersion == "" {
		e.ConnectorVersion = other.ConnectorVersion
	}
	if e.Image == "" {
		e.Image = other.Image
	}
	if e.RequestedWorkers == 0 {
		e.RequestedWorkers = other.RequestedWorkers
	}
	if e.ReadyWorkers == 0 {
		e.ReadyWorkers = other.ReadyWorkers
	}
	if e.Catalog == "" {
		e.Catalog = other.Catalog
	}
	if e.Schema == "" {
		e.Schema = other.Schema
	}
	if e.TimeZone == "" {
		e.TimeZone = other.TimeZone
	}
	return e
}

type RunSummary struct {
	RunID          string    `json:"run_id"`
	DatasetVersion string    `json:"dataset_version"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	TotalQueries   int       `json:"total_queries"`
	TotalErrors    int       `json:"total_errors"`
	WarmupQueries  int       `json:"warmup_queries"`
	// Environments is one entry per catalog target, in target order.
	Environments []ProtocolEnvironment `json:"environments,omitempty"`
}
