package provision

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

const (
	StepTypeProvisionWarehouse   = "provision_warehouse"
	StepTypeWaitWarehouseReady   = "wait_warehouse_ready"
	StepTypeDeprovisionWarehouse = "deprovision_warehouse"
)

type ExecutorConfig struct {
	Client      *Client
	State       *State
	WaitOptions WaitOptions
}

type Executor struct {
	client      *Client
	state       *State
	waitOptions WaitOptions
}

type State struct {
	mu                 sync.Mutex
	provisionResponses map[string]ProvisionResponse
	statuses           map[string]WarehouseStatus
}

func NewExecutor(cfg ExecutorConfig) *Executor {
	state := cfg.State
	if state == nil {
		state = NewState()
	}
	return &Executor{
		client:      cfg.Client,
		state:       state,
		waitOptions: cfg.WaitOptions,
	}
}

func NewState() *State {
	return &State{
		provisionResponses: make(map[string]ProvisionResponse),
		statuses:           make(map[string]WarehouseStatus),
	}
}

func (s *State) StoreProvisionResponse(orgID string, resp ProvisionResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.provisionResponses[orgID] = resp
}

func (s *State) ProvisionResponse(orgID string) (ProvisionResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp, ok := s.provisionResponses[orgID]
	return resp, ok
}

func (s *State) StoreStatus(orgID string, status WarehouseStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[orgID] = status
}

func (s *State) Status(orgID string) (WarehouseStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.statuses[orgID]
	return status, ok
}

func (e *Executor) ExecuteStep(ctx context.Context, step core.Step) error {
	if e.client == nil {
		return classified(ErrorClassConfig, fmt.Errorf("provision executor client is required"))
	}
	switch step.Type {
	case StepTypeProvisionWarehouse:
		return e.executeProvision(ctx, step)
	case StepTypeWaitWarehouseReady:
		return e.executeWaitReady(ctx, step)
	case StepTypeDeprovisionWarehouse:
		return e.executeDeprovision(ctx, step)
	default:
		return classified(ErrorClassUnsupportedStep, fmt.Errorf("unsupported provision step type %q", step.Type))
	}
}

func (e *Executor) executeProvision(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	request, err := requestMap(step)
	if err != nil {
		return err
	}
	resp, err := e.client.Provision(ctx, orgID, request)
	if err != nil {
		return classified(ErrorClassProvisionStepError, err)
	}
	e.state.StoreProvisionResponse(orgID, resp)
	return nil
}

func (e *Executor) executeWaitReady(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	opts, err := e.waitOptionsForStep(step)
	if err != nil {
		return err
	}
	status, err := e.client.WaitWarehouseReady(ctx, orgID, opts)
	if err != nil {
		return err
	}
	e.state.StoreStatus(orgID, status)
	return nil
}

func (e *Executor) executeDeprovision(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	verifyDeleted, err := boolFromWith(step, "verify_deleted")
	if err != nil {
		return err
	}
	opts, err := e.waitOptionsForStep(step)
	if err != nil {
		return err
	}
	if _, err := e.client.Deprovision(ctx, orgID); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil
		}
		return classified(ErrorClassCleanupError, err)
	}
	if !verifyDeleted {
		return nil
	}
	status, err := e.client.WaitWarehouseDeleted(ctx, orgID, opts)
	if err != nil {
		if errorsIs(err, ErrWaitTimeout) {
			return classified(ErrorClassCleanupTimeout, err)
		}
		return classified(ErrorClassCleanupError, err)
	}
	e.state.StoreStatus(orgID, status)
	return nil
}

func (e *Executor) waitOptionsForStep(step core.Step) (WaitOptions, error) {
	opts := e.waitOptions
	if timeout, ok, err := durationFromWith(step, "timeout"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.Timeout = timeout
	}
	if cleanupTimeout, ok, err := durationFromWith(step, "cleanup_timeout"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.Timeout = cleanupTimeout
	}
	if interval, ok, err := durationFromWith(step, "poll_interval"); err != nil {
		return WaitOptions{}, err
	} else if ok {
		opts.PollInterval = interval
	}
	if interval, ok, err := durationFromWith(step, "interval"); err != nil {
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

func requestMap(step core.Step) (map[string]any, error) {
	value, ok := step.With["request"]
	if !ok {
		return nil, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s requires with.request", step.ID))
	}
	request, ok := value.(map[string]any)
	if !ok {
		return nil, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.request must be a map", step.ID))
	}
	return request, nil
}

func boolFromWith(step core.Step, key string) (bool, error) {
	value, ok := step.With[key]
	if !ok {
		return false, nil
	}
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		parsed, err := strconv.ParseBool(typed)
		if err != nil {
			return false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a boolean", step.ID, key))
		}
		return parsed, nil
	default:
		return false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a boolean", step.ID, key))
	}
}

func durationFromWith(step core.Step, key string) (time.Duration, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	switch typed := value.(type) {
	case time.Duration:
		if typed < 0 {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must not be negative", step.ID, key))
		}
		return typed, true, nil
	case string:
		parsed, err := time.ParseDuration(typed)
		if err != nil {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a Go duration: %w", step.ID, key, err))
		}
		if parsed < 0 {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must not be negative", step.ID, key))
		}
		return parsed, true, nil
	default:
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be a Go duration string", step.ID, key))
	}
}

func intFromWith(step core.Step, key string) (int, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	var parsed int
	switch typed := value.(type) {
	case int:
		parsed = typed
	case int64:
		parsed = int(typed)
	case float64:
		if typed != float64(int(typed)) {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be an integer", step.ID, key))
		}
		parsed = int(typed)
	case string:
		var err error
		parsed, err = strconv.Atoi(typed)
		if err != nil {
			return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be an integer: %w", step.ID, key, err))
		}
	default:
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must be an integer", step.ID, key))
	}
	if parsed < 0 {
		return 0, false, classified(ErrorClassInvalidStepConfig, fmt.Errorf("step %s with.%s must not be negative", step.ID, key))
	}
	return parsed, true, nil
}

func errorsIs(err, target error) bool {
	return err != nil && target != nil && errors.Is(err, target)
}
