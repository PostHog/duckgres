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

type RunnerConfig struct {
	Catalog        Catalog
	DatasetVersion string
	Drivers        map[Protocol]ProtocolDriver
	Sink           ResultSink
	OnSetup        func(context.Context) error
	OnTeardown     func(context.Context) error
	Now            func() time.Time
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
	summary := RunSummary{
		RunID:          startedAt.UTC().Format("20060102T150405Z"),
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

	warmupIterations := r.cfg.Catalog.WarmupIterations
	for i := 0; i < warmupIterations; i++ {
		if err := r.executeIteration(ctx, false, &summary); err != nil {
			return summary, err
		}
	}
	measureIterations := r.cfg.Catalog.MeasureIterations
	for i := 0; i < measureIterations; i++ {
		if err := r.executeIteration(ctx, true, &summary); err != nil {
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

func (r *QueryRunner) MetricsGatherer() prometheus.Gatherer {
	return r.metrics.Gatherer()
}

func (r *QueryRunner) executeIteration(ctx context.Context, measure bool, summary *RunSummary) error {
	for _, query := range r.cfg.Catalog.Queries {
		args := orderedParamValues(query.Params)
		for _, protocol := range r.cfg.Catalog.Targets {
			driver := r.cfg.Drivers[protocol]
			started := r.cfg.Now()
			result := QueryResult{
				QueryID:   query.QueryID,
				IntentID:  query.IntentID,
				Protocol:  protocol,
				StartedAt: started,
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
