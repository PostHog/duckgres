package trino

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestExecutorProvisionWaitAndDeprovisionCluster(t *testing.T) {
	lifecycle := &recordingLifecycle{
		provisioned: Cluster{ID: "trino-run-123"},
		ready:       Cluster{ID: "trino-run-123", Endpoint: "http://trino.example:8080"},
	}
	state := NewState()
	executor := NewExecutor(ExecutorConfig{
		Lifecycle: lifecycle,
		State:     state,
		WaitOptions: WaitOptions{
			Timeout:      time.Minute,
			PollInterval: 5 * time.Second,
		},
	})

	provision := core.Step{
		ID:   "provision_trino",
		Type: StepTypeProvisionTrino,
		With: map[string]any{
			"org_id": "benchmark-org",
			"request": map[string]any{
				"workers": 4,
				"image":   "registry.example/trino@sha256:abc",
			},
		},
	}
	if err := executor.ExecuteStep(context.Background(), provision); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if lifecycle.provisionRequest.OrgID != "benchmark-org" {
		t.Fatalf("provision org = %q", lifecycle.provisionRequest.OrgID)
	}
	if workers, ok := lifecycle.provisionRequest.Config["workers"].(int); !ok || workers != 4 {
		t.Fatalf("provision request = %#v", lifecycle.provisionRequest.Config)
	}

	wait := core.Step{
		ID:   "wait_trino_ready",
		Type: StepTypeWaitTrinoReady,
		With: map[string]any{
			"org_id":        "benchmark-org",
			"timeout":       "2m",
			"poll_interval": "2s",
			"max_attempts":  3,
		},
	}
	if err := executor.ExecuteStep(context.Background(), wait); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if lifecycle.waitCluster.ID != "trino-run-123" {
		t.Fatalf("wait cluster = %#v", lifecycle.waitCluster)
	}
	if lifecycle.waitOptions.Timeout != 2*time.Minute || lifecycle.waitOptions.PollInterval != 2*time.Second || lifecycle.waitOptions.MaxAttempts != 3 {
		t.Fatalf("wait options = %#v", lifecycle.waitOptions)
	}
	cluster, ok := state.Cluster("benchmark-org")
	if !ok || cluster.Endpoint != "http://trino.example:8080" {
		t.Fatalf("stored ready cluster = %#v, present=%t", cluster, ok)
	}

	deprovision := core.Step{
		ID:   "deprovision_trino",
		Type: StepTypeDeprovisionTrino,
		With: map[string]any{
			"org_id": "benchmark-org",
		},
	}
	if err := executor.ExecuteStep(context.Background(), deprovision); err != nil {
		t.Fatalf("deprovision: %v", err)
	}
	if lifecycle.deprovisionCluster.ID != "trino-run-123" {
		t.Fatalf("deprovision cluster = %#v", lifecycle.deprovisionCluster)
	}
}

func TestExecutorDeprovisionIsNoopWhenProvisionDidNotReturnCluster(t *testing.T) {
	lifecycle := &recordingLifecycle{}
	executor := NewExecutor(ExecutorConfig{Lifecycle: lifecycle})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "deprovision_trino",
		Type: StepTypeDeprovisionTrino,
		With: map[string]any{
			"org_id": "benchmark-org",
		},
	})
	if err != nil {
		t.Fatalf("deprovision: %v", err)
	}
	if lifecycle.deprovisionCalls != 0 {
		t.Fatalf("deprovision calls = %d, want 0", lifecycle.deprovisionCalls)
	}
}

func TestExecutorRejectsInvalidLifecycleStepConfig(t *testing.T) {
	executor := NewExecutor(ExecutorConfig{Lifecycle: &recordingLifecycle{}})
	for _, step := range []core.Step{
		{ID: "missing-request", Type: StepTypeProvisionTrino, With: map[string]any{"org_id": "benchmark-org"}},
		{ID: "missing-cluster", Type: StepTypeWaitTrinoReady, With: map[string]any{"org_id": "benchmark-org"}},
		{ID: "missing-org", Type: StepTypeDeprovisionTrino, With: map[string]any{}},
	} {
		t.Run(step.ID, func(t *testing.T) {
			err := executor.ExecuteStep(context.Background(), step)
			if err == nil {
				t.Fatal("expected invalid configuration error")
			}
			var classified core.ClassifiedError
			if !errors.As(err, &classified) {
				t.Fatalf("error = %T %v, want classified", err, err)
			}
			if classified.ErrorClass() != ErrorClassInvalidStepConfig {
				t.Fatalf("error class = %q, want %q", classified.ErrorClass(), ErrorClassInvalidStepConfig)
			}
		})
	}

	state := NewState()
	state.StoreCluster("benchmark-org", Cluster{ID: "trino-run-123"})
	err := NewExecutor(ExecutorConfig{Lifecycle: &recordingLifecycle{}, State: state}).ExecuteStep(context.Background(), core.Step{
		ID:   "bad-timeout",
		Type: StepTypeWaitTrinoReady,
		With: map[string]any{"org_id": "benchmark-org", "timeout": "not-a-duration"},
	})
	if err == nil {
		t.Fatal("expected invalid timeout to fail")
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassInvalidStepConfig {
		t.Fatalf("error = %T %v, want %q", err, err, ErrorClassInvalidStepConfig)
	}
}

func TestExecutorRejectsReadyClusterWithoutEndpoint(t *testing.T) {
	executor := NewExecutor(ExecutorConfig{
		Lifecycle: &recordingLifecycle{
			provisioned: Cluster{ID: "trino-run-123"},
			ready:       Cluster{ID: "trino-run-123"},
		},
	})
	provision := core.Step{ID: "provision", Type: StepTypeProvisionTrino, With: map[string]any{"org_id": "benchmark-org", "request": map[string]any{}}}
	if err := executor.ExecuteStep(context.Background(), provision); err != nil {
		t.Fatalf("provision: %v", err)
	}
	err := executor.ExecuteStep(context.Background(), core.Step{ID: "wait", Type: StepTypeWaitTrinoReady, With: map[string]any{"org_id": "benchmark-org"}})
	if err == nil {
		t.Fatal("expected ready cluster without endpoint to fail")
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassLifecycle {
		t.Fatalf("error = %T %v, want %q", err, err, ErrorClassLifecycle)
	}
}

type recordingLifecycle struct {
	provisioned Cluster
	ready       Cluster
	err         error

	provisionRequest   ProvisionRequest
	waitCluster        Cluster
	waitOptions        WaitOptions
	deprovisionCluster Cluster
	deprovisionCalls   int
}

func (l *recordingLifecycle) ProvisionTrino(_ context.Context, request ProvisionRequest) (Cluster, error) {
	l.provisionRequest = request
	return l.provisioned, l.err
}

func (l *recordingLifecycle) WaitTrinoReady(_ context.Context, cluster Cluster, options WaitOptions) (Cluster, error) {
	l.waitCluster = cluster
	l.waitOptions = options
	return l.ready, l.err
}

func (l *recordingLifecycle) DeprovisionTrino(_ context.Context, cluster Cluster) error {
	l.deprovisionCalls++
	l.deprovisionCluster = cluster
	return l.err
}
