package provision

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/scenario/core"
)

func TestExecutorProvisionStepStoresPasswordForLaterSteps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/orgs/scenario-org/provision" {
			t.Fatalf("path = %s, want provision path", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":   "provisioning started",
			"org":      "scenario-org",
			"username": "root",
			"password": "root-password",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	state := NewState()
	executor := NewExecutor(ExecutorConfig{Client: client, State: state})

	err = executor.ExecuteStep(context.Background(), core.Step{
		ID:   "provision",
		Type: StepTypeProvisionWarehouse,
		With: map[string]any{
			"org_id": "scenario-org",
			"request": map[string]any{
				"database_name":  "scenario_db",
				"metadata_store": map[string]any{"type": "cnpg-shard"},
				"ducklake":       map[string]any{"enabled": true},
			},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	resp, ok := state.ProvisionResponse("scenario-org")
	if !ok {
		t.Fatal("expected provision response in state")
	}
	if resp.Password != "root-password" {
		t.Fatalf("stored password = %q, want root-password", resp.Password)
	}
}

func TestExecutorRunsDeprovisionAfterWorkloadFailure(t *testing.T) {
	deprovisionCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/orgs/scenario-org/provision":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":   "provisioning started",
				"org":      "scenario-org",
				"username": "root",
				"password": "root-password",
			})
		case "/api/v1/orgs/scenario-org/deprovision":
			deprovisionCalled = true
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "deprovisioning started",
				"org":    "scenario-org",
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	provisionExecutor := NewExecutor(ExecutorConfig{Client: client})
	workloadErr := errors.New("workload failed")
	scenario, err := core.ParseScenario([]byte(`
name: cleanup-after-failure
steps:
  - id: provision
    type: provision_warehouse
    with:
      org_id: scenario-org
      request:
        database_name: scenario_db
        metadata_store:
          type: cnpg-shard
        ducklake:
          enabled: true
  - id: workload
    type: fake_workload
  - id: deprovision
    type: deprovision_warehouse
    depends_on: [workload]
    always_run: true
    with:
      org_id: scenario-org
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}
	runner := core.NewRunner(core.RunnerConfig{
		RunID:    "run-cleanup",
		Scenario: scenario,
		Executor: core.StepExecutorFunc(func(ctx context.Context, step core.Step) error {
			if step.Type == "fake_workload" {
				return workloadErr
			}
			return provisionExecutor.ExecuteStep(ctx, step)
		}),
	})

	_, err = runner.Run(context.Background())
	if !errors.Is(err, workloadErr) {
		t.Fatalf("runner error = %v, want workload error", err)
	}
	if !deprovisionCalled {
		t.Fatal("expected deprovision to run after workload failure")
	}
}

func TestExecutorTreatsCleanupNotFoundAsAlreadyCleanedUp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/orgs/scenario-org/provision":
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "provision failed before warehouse row was created",
			})
		case "/api/v1/orgs/scenario-org/deprovision":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "warehouse not found",
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	provisionExecutor := NewExecutor(ExecutorConfig{Client: client})
	scenario, err := core.ParseScenario([]byte(`
name: cleanup-after-partial-provision
steps:
  - id: provision
    type: provision_warehouse
    with:
      org_id: scenario-org
      request:
        database_name: scenario_db
  - id: deprovision
    type: deprovision_warehouse
    depends_on: [provision]
    always_run: true
    with:
      org_id: scenario-org
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}
	runner := core.NewRunner(core.RunnerConfig{
		RunID:    "run-partial-provision",
		Scenario: scenario,
		Executor: provisionExecutor,
	})

	_, err = runner.Run(context.Background())
	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Fatalf("runner error = %v, want provision API failure", err)
	}
	results := runner.Results()
	if len(results) != 2 {
		t.Fatalf("results = %+v, want 2 results", results)
	}
	if results[1].Status != core.StepStatusOK {
		t.Fatalf("cleanup result = %+v, want success", results[1])
	}
}

func TestExecutorCleanupTimeoutIsDistinctFromWorkloadFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/orgs/scenario-org/deprovision":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "deprovisioning started",
				"org":    "scenario-org",
			})
		case "/api/v1/orgs/scenario-org/warehouse/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"org_id": "scenario-org",
				"state":  WarehouseStateDeleting,
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	provisionExecutor := NewExecutor(ExecutorConfig{
		Client: client,
		WaitOptions: WaitOptions{
			MaxAttempts: 2,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
	})
	workloadErr := errors.New("workload failed")
	scenario, err := core.ParseScenario([]byte(`
name: cleanup-timeout
steps:
  - id: workload
    type: fake_workload
  - id: deprovision
    type: deprovision_warehouse
    always_run: true
    with:
      org_id: scenario-org
      verify_deleted: true
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}
	runner := core.NewRunner(core.RunnerConfig{
		RunID:    "run-cleanup-timeout",
		Scenario: scenario,
		Executor: core.StepExecutorFunc(func(ctx context.Context, step core.Step) error {
			if step.Type == "fake_workload" {
				return workloadErr
			}
			return provisionExecutor.ExecuteStep(ctx, step)
		}),
	})

	_, err = runner.Run(context.Background())
	if !errors.Is(err, workloadErr) {
		t.Fatalf("runner error = %v, want workload failure", err)
	}
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("runner error = %v, want cleanup timeout", err)
	}
	results := runner.Results()
	if len(results) != 2 || results[1].Status != core.StepStatusFailed {
		t.Fatalf("deprovision result = %+v", results)
	}
	if results[1].ErrorClass != ErrorClassCleanupTimeout {
		t.Fatalf("cleanup error class = %q, want %q", results[1].ErrorClass, ErrorClassCleanupTimeout)
	}
}

func TestExecutorRejectsInvalidWaitOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org_id": "scenario-org",
			"state":  WarehouseStateReady,
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	executor := NewExecutor(ExecutorConfig{Client: client})
	err = executor.ExecuteStep(context.Background(), core.Step{
		ID:   "wait",
		Type: StepTypeWaitWarehouseReady,
		With: map[string]any{
			"org_id":         "scenario-org",
			"timeout":        "not-a-duration",
			"max_attempts":   "not-an-int",
			"verify_deleted": "maybe",
		},
	})
	if err == nil {
		t.Fatal("expected invalid wait options to fail")
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) {
		t.Fatalf("error = %T %v, want classified error", err, err)
	}
	if classified.ErrorClass() != ErrorClassInvalidStepConfig {
		t.Fatalf("error class = %q, want %q", classified.ErrorClass(), ErrorClassInvalidStepConfig)
	}
}
