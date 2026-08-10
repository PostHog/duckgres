package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type ProtocolDriver interface {
	Protocol() Protocol
	Execute(ctx context.Context, query Query, args []any) (ExecutionResult, error)
	Close() error
}

type ResultSink interface {
	Record(result QueryResult) error
	Close(summary RunSummary, serverMetrics string) error
}

// EnvironmentReporter is optionally implemented by a driver that can describe
// the engine it is talking to. It is probed once per run, before any measured
// query, and any error is ignored: comparison metadata must never fail a
// benchmark.
type EnvironmentReporter interface {
	Environment(ctx context.Context) (ProtocolEnvironment, error)
}

type RunnerConfig struct {
	RunID          string
	Catalog        Catalog
	DatasetVersion string
	Drivers        map[Protocol]ProtocolDriver
	Sink           ResultSink
	OnSetup        func(context.Context) error
	OnTeardown     func(context.Context) error
	Now            func() time.Time
	// Environments carries what the CALLER already knows about each protocol
	// (the lifecycle-reported image and worker counts, the catalog/schema, the
	// session time zone). Driver-probed detail fills the gaps.
	Environments []ProtocolEnvironment
}

type QueryRunner struct {
	cfg     RunnerConfig
	matcher *IntentMatcher
	metrics *RunnerMetrics
}

func NewQueryRunner(cfg RunnerConfig) *QueryRunner {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &QueryRunner{
		cfg:     cfg,
		matcher: NewIntentMatcher(),
		metrics: NewRunnerMetrics(),
	}
}

func (r *QueryRunner) Run(ctx context.Context) (RunSummary, error) {
	startedAt := r.cfg.Now()
	runID := r.cfg.RunID
	if runID == "" {
		runID = startedAt.UTC().Format("20060102T150405Z")
	}
	summary := RunSummary{
		RunID:          runID,
		DatasetVersion: r.cfg.DatasetVersion,
		StartedAt:      startedAt,
		FinishedAt:     startedAt,
	}

	if r.cfg.OnSetup != nil {
		if err := r.cfg.OnSetup(ctx); err != nil {
			return summary, fmt.Errorf("runner setup: %w", err)
		}
	}
	if r.cfg.OnTeardown != nil {
		defer func() {
			_ = r.cfg.OnTeardown(ctx)
		}()
	}

	for _, protocol := range r.cfg.Catalog.Targets {
		if _, ok := r.cfg.Drivers[protocol]; !ok {
			return summary, fmt.Errorf("missing driver for protocol %q", protocol)
		}
	}

	summary.Environments = r.resolveEnvironments(ctx)

	warmupIterations := r.cfg.Catalog.WarmupIterations
	for i := 0; i < warmupIterations; i++ {
		if err := r.executeIteration(ctx, false, 0, &summary); err != nil {
			return summary, err
		}
	}
	measureIterations := r.cfg.Catalog.MeasureIterations
	for i := 0; i < measureIterations; i++ {
		if err := r.executeIteration(ctx, true, i+1, &summary); err != nil {
			return summary, err
		}
	}

	summary.FinishedAt = r.cfg.Now()
	metricsText, err := r.metrics.OpenMetricsText()
	if err != nil {
		return summary, err
	}
	if r.cfg.Sink != nil {
		if err := r.cfg.Sink.Close(summary, metricsText); err != nil {
			return summary, fmt.Errorf("close sink: %w", err)
		}
	}
	return summary, nil
}

// resolveEnvironments merges the caller-supplied comparison metadata with
// whatever each driver can report about its engine, one entry per catalog
// target in target order.
func (r *QueryRunner) resolveEnvironments(ctx context.Context) []ProtocolEnvironment {
	configured := make(map[Protocol]ProtocolEnvironment, len(r.cfg.Environments))
	for _, env := range r.cfg.Environments {
		configured[env.Protocol] = env
	}
	var environments []ProtocolEnvironment
	for _, protocol := range r.cfg.Catalog.Targets {
		env := configured[protocol]
		env.Protocol = protocol
		if reporter, ok := r.cfg.Drivers[protocol].(EnvironmentReporter); ok {
			// Best-effort: a probe failure must never fail the benchmark.
			if probed, err := reporter.Environment(ctx); err == nil {
				env = env.Merge(probed)
			}
		}
		environments = append(environments, env)
	}
	return environments
}

func (r *QueryRunner) MetricsGatherer() prometheus.Gatherer {
	return r.metrics.Gatherer()
}

func (r *QueryRunner) executeIteration(ctx context.Context, measure bool, measureIteration int, summary *RunSummary) error {
	for _, query := range r.cfg.Catalog.Queries {
		args := orderedParamValues(query.Params)
		for _, protocol := range r.cfg.Catalog.Targets {
			if !query.SupportsProtocol(protocol) {
				continue
			}
			driver := r.cfg.Drivers[protocol]
			started := r.cfg.Now()
			result := QueryResult{
				QueryID:          query.QueryID,
				IntentID:         query.IntentID,
				MeasureIteration: measureIteration,
				Protocol:         protocol,
				StartedAt:        started,
			}

			execResult, err := driver.Execute(ctx, query, args)
			if execResult.Duration <= 0 {
				execResult.Duration = time.Since(started)
			}
			result.Duration = execResult.Duration
			result.Rows = execResult.Rows
			if err != nil {
				result.Status = "error"
				result.Error = err.Error()
				result.ErrorClass = "execution_error"
			} else {
				result.Status = "ok"
			}
			r.metrics.Observe(result)

			if measure {
				summary.TotalQueries++
				if result.Status == "error" {
					summary.TotalErrors++
				}
				if r.cfg.Sink != nil {
					if err := r.cfg.Sink.Record(result); err != nil {
						return fmt.Errorf("sink record (%s/%s): %w", protocol, query.QueryID, err)
					}
				}
			} else {
				summary.WarmupQueries++
			}
		}
	}
	return nil
}

func orderedParamValues(params map[string]any) []any {
	if len(params) == 0 {
		return nil
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]any, 0, len(keys))
	for _, k := range keys {
		values = append(values, params[k])
	}
	return values
}
