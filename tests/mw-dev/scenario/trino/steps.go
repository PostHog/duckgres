// Package trino defines scenario lifecycle contracts for a benchmark Trino
// cluster. The concrete control-plane/Kubernetes implementation is injected as
// a Lifecycle; this package does not create infrastructure itself.
package trino

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

const (
	StepTypeProvisionTrino   = "provision_trino"
	StepTypeWaitTrinoReady   = "wait_trino_ready"
	StepTypeDeprovisionTrino = "deprovision_trino"

	ErrorClassConfig            = "configuration_error"
	ErrorClassInvalidStepConfig = "invalid_step_config"
	ErrorClassLifecycle         = "trino_lifecycle_error"
	ErrorClassCleanup           = "trino_cleanup_error"
	ErrorClassUnsupportedStep   = "unsupported_step"
)

// ProvisionRequest identifies the warehouse whose DuckLake tables Trino will
// read. Config is deliberately opaque to the scenario runner: it is passed to
// the control-plane implementation and must never contain credentials.
type ProvisionRequest struct {
	OrgID  string
	Config map[string]any
}

// Lifecycle states reported by the control-plane benchmark API. pending is a
// polling state; failed is terminal.
const (
	StatePending = "pending"
	StateReady   = "ready"
	StateFailed  = "failed"
)

// Cluster is the non-secret state a subsequent Trino query executor needs, plus
// the comparison metadata the perf artifact records. It never carries
// credentials: the control plane owns the metadata reader password and the
// read-only S3 identity and returns neither.
type Cluster struct {
	ID       string `json:"id"`
	State    string `json:"state,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	// RequestedWorkers / ReadyWorkers make a run's topology auditable from the
	// artifact rather than assumed.
	RequestedWorkers int `json:"requested_workers,omitempty"`
	ReadyWorkers     int `json:"ready_workers,omitempty"`
	// Image is the pinned Trino+Brikk image reference (digest where pinned).
	Image string `json:"image,omitempty"`
}

// WaitOptions are lifecycle polling controls. The HTTP client honours all
// three: it polls at PollInterval until Timeout or MaxAttempts is reached
// rather than issuing a single status request.
type WaitOptions struct {
	PollInterval time.Duration
	Timeout      time.Duration
	MaxAttempts  int
}

// Lifecycle is implemented by Client, the control-plane-backed Trino
// provisioner. Keeping it narrow lets scenario contracts be tested without
// Kubernetes credentials or a live control plane.
type Lifecycle interface {
	ProvisionTrino(context.Context, ProvisionRequest) (Cluster, error)
	WaitTrinoReady(context.Context, Cluster, WaitOptions) (Cluster, error)
	DeprovisionTrino(context.Context, Cluster) error
}

type ExecutorConfig struct {
	Lifecycle   Lifecycle
	State       *State
	WaitOptions WaitOptions
}

type Executor struct {
	lifecycle   Lifecycle
	state       *State
	waitOptions WaitOptions
}

// State intentionally contains only non-secret cluster identity and endpoint.
type State struct {
	mu       sync.Mutex
	clusters map[string]Cluster
}

func NewState() *State {
	return &State{clusters: make(map[string]Cluster)}
}

func (s *State) StoreCluster(orgID string, cluster Cluster) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters[orgID] = cluster
}

func (s *State) Cluster(orgID string) (Cluster, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cluster, ok := s.clusters[orgID]
	return cluster, ok
}

func (s *State) DeleteCluster(orgID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clusters, orgID)
}

func NewExecutor(cfg ExecutorConfig) *Executor {
	state := cfg.State
	if state == nil {
		state = NewState()
	}
	return &Executor{lifecycle: cfg.Lifecycle, state: state, waitOptions: cfg.WaitOptions}
}

func (e *Executor) ExecuteStep(ctx context.Context, step core.Step) error {
	if e.lifecycle == nil {
		return classified(ErrorClassConfig, fmt.Errorf("trino lifecycle is required"))
	}
	switch step.Type {
	case StepTypeProvisionTrino:
		return e.provision(ctx, step)
	case StepTypeWaitTrinoReady:
		return e.waitReady(ctx, step)
	case StepTypeDeprovisionTrino:
		return e.deprovision(ctx, step)
	default:
		return classified(ErrorClassUnsupportedStep, fmt.Errorf("unsupported trino step type %q", step.Type))
	}
}

func (e *Executor) provision(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	request, err := requiredMap(step, "request")
	if err != nil {
		return err
	}
	cluster, err := e.lifecycle.ProvisionTrino(ctx, ProvisionRequest{OrgID: orgID, Config: request})
	if err != nil {
		return classified(ErrorClassLifecycle, err)
	}
	if cluster.ID == "" {
		return classified(ErrorClassLifecycle, fmt.Errorf("step %s provisioned Trino cluster without an ID", step.ID))
	}
	e.state.StoreCluster(orgID, cluster)
	return nil
}

func (e *Executor) waitReady(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	cluster, ok := e.state.Cluster(orgID)
	if !ok {
		return classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s requires a provisioned Trino cluster for org %q", step.ID, orgID))
	}
	opts, err := e.waitOptionsForStep(step)
	if err != nil {
		return err
	}
	ready, err := e.lifecycle.WaitTrinoReady(ctx, cluster, opts)
	if err != nil {
		return classified(ErrorClassLifecycle, err)
	}
	if ready.ID == "" {
		ready.ID = cluster.ID
	}
	if ready.ID != cluster.ID {
		return classified(ErrorClassLifecycle, fmt.Errorf("step %s ready Trino cluster ID %q does not match provisioned cluster %q", step.ID, ready.ID, cluster.ID))
	}
	if ready.Endpoint == "" {
		return classified(ErrorClassLifecycle, fmt.Errorf("step %s ready Trino cluster %q has no endpoint", step.ID, ready.ID))
	}
	e.state.StoreCluster(orgID, ready)
	return nil
}

func (e *Executor) deprovision(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	cluster, ok := e.state.Cluster(orgID)
	if !ok {
		// Cleanup steps are always_run and must be safe after partial provision.
		return nil
	}
	if err := e.lifecycle.DeprovisionTrino(ctx, cluster); err != nil {
		return classified(ErrorClassCleanup, err)
	}
	e.state.DeleteCluster(orgID)
	return nil
}

func (e *Executor) waitOptionsForStep(step core.Step) (WaitOptions, error) {
	opts := e.waitOptions
	if timeout, ok, err := durationFromWith(step, "timeout"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.Timeout = timeout
	}
	if interval, ok, err := durationFromWith(step, "poll_interval"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.PollInterval = interval
	}
	if maxAttempts, ok, err := intFromWith(step, "max_attempts"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.MaxAttempts = maxAttempts
	}
	return opts, nil
}

func requiredString(step core.Step, key string) (string, error) {
	value, ok := step.With[key]
	if !ok {
		return "", classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s requires with.%s", step.ID, key))
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return "", classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a non-empty string", step.ID, key))
	}
	return text, nil
}

func requiredMap(step core.Step, key string) (map[string]any, error) {
	value, ok := step.With[key]
	if !ok {
		return nil, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s requires with.%s", step.ID, key))
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a map", step.ID, key))
	}
	return result, nil
}

func durationFromWith(step core.Step, key string) (time.Duration, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	text, ok := value.(string)
	if !ok {
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a Go duration", step.ID, key))
	}
	duration, err := time.ParseDuration(text)
	if err != nil || duration < 0 {
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a non-negative Go duration", step.ID, key))
	}
	return duration, true, nil
}

func intFromWith(step core.Step, key string) (int, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	switch value := value.(type) {
	case int:
		if value < 0 {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must not be negative", step.ID, key))
		}
		return value, true, nil
	case string:
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a non-negative integer", step.ID, key))
		}
		return parsed, true, nil
	default:
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a non-negative integer", step.ID, key))
	}
}

type classifiedError struct {
	class string
	err   error
}

func (e classifiedError) Error() string      { return e.err.Error() }
func (e classifiedError) Unwrap() error      { return e.err }
func (e classifiedError) ErrorClass() string { return e.class }

func classified(class string, err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{class: class, err: err}
}
