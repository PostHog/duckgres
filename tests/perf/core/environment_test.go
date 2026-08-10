package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type reportingDriver struct {
	protocol Protocol
	env      ProtocolEnvironment
	err      error
}

func (d reportingDriver) Protocol() Protocol { return d.protocol }

func (d reportingDriver) Execute(context.Context, Query, []any) (ExecutionResult, error) {
	return ExecutionResult{Rows: 1, Duration: time.Millisecond}, nil
}

func (d reportingDriver) Close() error { return nil }

func (d reportingDriver) Environment(context.Context) (ProtocolEnvironment, error) {
	return d.env, d.err
}

type silentDriver struct{ protocol Protocol }

func (d silentDriver) Protocol() Protocol { return d.protocol }

func (d silentDriver) Execute(context.Context, Query, []any) (ExecutionResult, error) {
	return ExecutionResult{Rows: 1, Duration: time.Millisecond}, nil
}

func (d silentDriver) Close() error { return nil }

func environmentTestCatalog() Catalog {
	return Catalog{
		Targets:           []Protocol{ProtocolPGWire, ProtocolTrino},
		MeasureIterations: 1,
		Queries: []Query{{
			QueryID: "q1", IntentID: "i1",
			PGWireSQL: "SELECT 1", TrinoSQL: "SELECT 1",
		}},
	}
}

func TestRunnerRecordsConfiguredAndProbedEnvironments(t *testing.T) {
	runner := NewQueryRunner(RunnerConfig{
		RunID:   "run-1",
		Catalog: environmentTestCatalog(),
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: reportingDriver{protocol: ProtocolPGWire, env: ProtocolEnvironment{Engine: "duckgres", Version: "duckgres 1.2.3"}},
			ProtocolTrino:  reportingDriver{protocol: ProtocolTrino, env: ProtocolEnvironment{Engine: "trino", Version: "483"}},
		},
		Environments: []ProtocolEnvironment{
			{Protocol: ProtocolPGWire, Catalog: "ducklake", TimeZone: "UTC"},
			{
				Protocol: ProtocolTrino, Catalog: "ducklake", Schema: "posthog", TimeZone: "UTC",
				Image: "registry.example/trino-brikk@sha256:abc", RequestedWorkers: 4, ReadyWorkers: 4,
				ConnectorVersion: "483-0.2.0",
			},
		},
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(summary.Environments) != 2 {
		t.Fatalf("environments = %+v, want one per target", summary.Environments)
	}
	pgwire, trino := summary.Environments[0], summary.Environments[1]
	if pgwire.Protocol != ProtocolPGWire || trino.Protocol != ProtocolTrino {
		t.Fatalf("environments are not in catalog-target order: %+v", summary.Environments)
	}
	if pgwire.Version != "duckgres 1.2.3" || pgwire.Catalog != "ducklake" || pgwire.TimeZone != "UTC" {
		t.Fatalf("pgwire environment = %+v", pgwire)
	}
	if trino.Version != "483" || trino.ConnectorVersion != "483-0.2.0" {
		t.Fatalf("trino versions = %+v", trino)
	}
	if trino.Image != "registry.example/trino-brikk@sha256:abc" {
		t.Fatalf("trino image = %q", trino.Image)
	}
	if trino.RequestedWorkers != 4 || trino.ReadyWorkers != 4 {
		t.Fatalf("trino worker counts = %d/%d", trino.RequestedWorkers, trino.ReadyWorkers)
	}
	if trino.Schema != "posthog" || trino.TimeZone != "UTC" {
		t.Fatalf("trino catalog identity = %+v", trino)
	}
}

// Configured values are the recorded pin; a driver may only add detail.
func TestRunnerEnvironmentPrefersConfiguredValues(t *testing.T) {
	runner := NewQueryRunner(RunnerConfig{
		Catalog: Catalog{Targets: []Protocol{ProtocolTrino}, MeasureIterations: 1,
			Queries: []Query{{QueryID: "q1", IntentID: "i1", TrinoSQL: "SELECT 1"}}},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolTrino: reportingDriver{protocol: ProtocolTrino, env: ProtocolEnvironment{
				Engine: "trino", Image: "some-other-image", ReadyWorkers: 1,
			}},
		},
		Environments: []ProtocolEnvironment{{
			Protocol: ProtocolTrino, Image: "registry.example/trino-brikk@sha256:abc", ReadyWorkers: 4,
		}},
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	env := summary.Environments[0]
	if env.Image != "registry.example/trino-brikk@sha256:abc" || env.ReadyWorkers != 4 {
		t.Fatalf("environment = %+v, want the configured pin to win", env)
	}
}

// Comparison metadata is best-effort: a probe failure records what is known
// and never fails the benchmark.
func TestRunnerToleratesEnvironmentProbeFailures(t *testing.T) {
	runner := NewQueryRunner(RunnerConfig{
		Catalog: Catalog{Targets: []Protocol{ProtocolPGWire, ProtocolTrino}, MeasureIterations: 1,
			Queries: []Query{{QueryID: "q1", IntentID: "i1", PGWireSQL: "SELECT 1", TrinoSQL: "SELECT 1"}}},
		Drivers: map[Protocol]ProtocolDriver{
			ProtocolPGWire: silentDriver{protocol: ProtocolPGWire},
			ProtocolTrino: reportingDriver{protocol: ProtocolTrino, err: os.ErrDeadlineExceeded,
				env: ProtocolEnvironment{Engine: "trino"}},
		},
		Environments: []ProtocolEnvironment{{Protocol: ProtocolTrino, Image: "registry.example/trino-brikk@sha256:abc"}},
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(summary.Environments) != 2 {
		t.Fatalf("environments = %+v", summary.Environments)
	}
	if summary.Environments[1].Image != "registry.example/trino-brikk@sha256:abc" {
		t.Fatalf("trino environment lost its configured pin: %+v", summary.Environments[1])
	}
}

func TestArtifactSummaryCarriesEnvironmentsAndNoSecrets(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewArtifactSink(dir)
	if err != nil {
		t.Fatalf("NewArtifactSink: %v", err)
	}
	summary := RunSummary{
		RunID: "run-1",
		Environments: []ProtocolEnvironment{
			{Protocol: ProtocolPGWire, Engine: "duckgres", Catalog: "ducklake", TimeZone: "UTC"},
			{
				Protocol: ProtocolTrino, Engine: "trino", Version: "483", ConnectorVersion: "483-0.2.0",
				Image: "registry.example/trino-brikk@sha256:abc", RequestedWorkers: 4, ReadyWorkers: 4,
				Catalog: "ducklake", Schema: "posthog", TimeZone: "UTC",
			},
		},
	}
	if err := sink.Close(summary, ""); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var decoded RunSummary
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if len(decoded.Environments) != 2 {
		t.Fatalf("summary environments = %+v", decoded.Environments)
	}
	for _, banned := range []string{"password", "secret", "aws_access", "iam", "arn:aws"} {
		if strings.Contains(strings.ToLower(string(raw)), banned) {
			t.Fatalf("summary.json contains %q:\n%s", banned, raw)
		}
	}
}
