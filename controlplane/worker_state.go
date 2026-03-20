package controlplane

import (
	"fmt"
	"time"
)

// WorkerLifecycleState models the shared warm-worker lifecycle introduced for
// late tenant binding. The current production worker pools do not act on this
// state yet; this is scaffolding for later shared-pool work.
type WorkerLifecycleState string

const (
	WorkerLifecycleIdle       WorkerLifecycleState = "idle"
	WorkerLifecycleReserved   WorkerLifecycleState = "reserved"
	WorkerLifecycleActivating WorkerLifecycleState = "activating"
	WorkerLifecycleHot        WorkerLifecycleState = "hot"
	WorkerLifecycleDraining   WorkerLifecycleState = "draining"
	WorkerLifecycleRetired    WorkerLifecycleState = "retired"
)

// WorkerAssignment carries tenant-specific metadata once a shared worker has
// been reserved for a team.
type WorkerAssignment struct {
	TeamName       string
	LeaseExpiresAt time.Time
}

// SharedWorkerState holds the additive lifecycle/assignment model for shared
// warm workers. Existing worker pools can keep using their current session
// counters until later PRs wire this state into scheduling decisions.
type SharedWorkerState struct {
	Lifecycle  WorkerLifecycleState
	Assignment *WorkerAssignment
}

// NormalizedLifecycle treats the zero value as an idle, unassigned worker so
// existing worker structs can adopt this model without extra initialization.
func (s SharedWorkerState) NormalizedLifecycle() WorkerLifecycleState {
	if s.Lifecycle == "" {
		return WorkerLifecycleIdle
	}
	return s.Lifecycle
}

// Validate checks that the lifecycle and assignment metadata are internally
// consistent.
func (s SharedWorkerState) Validate() error {
	switch lifecycle := s.NormalizedLifecycle(); lifecycle {
	case WorkerLifecycleIdle:
		if s.Assignment != nil {
			return fmt.Errorf("lifecycle %q cannot have an assignment", lifecycle)
		}
		return nil
	case WorkerLifecycleReserved, WorkerLifecycleActivating, WorkerLifecycleHot, WorkerLifecycleDraining:
		if err := validateWorkerAssignment(s.Assignment); err != nil {
			return fmt.Errorf("lifecycle %q requires a valid assignment: %w", lifecycle, err)
		}
		return nil
	case WorkerLifecycleRetired:
		if s.Assignment == nil {
			return nil
		}
		if err := validateWorkerAssignment(s.Assignment); err != nil {
			return fmt.Errorf("lifecycle %q has invalid assignment metadata: %w", lifecycle, err)
		}
		return nil
	default:
		return fmt.Errorf("unknown worker lifecycle %q", lifecycle)
	}
}

// Transition validates a lifecycle change and returns the next worker state.
// When assignment metadata is omitted, the current assignment is carried
// forward for states that remain tenant-bound.
func (s SharedWorkerState) Transition(next WorkerLifecycleState, assignment *WorkerAssignment) (SharedWorkerState, error) {
	current := s.NormalizedLifecycle()
	if err := s.Validate(); err != nil {
		return SharedWorkerState{}, err
	}
	if !isAllowedWorkerLifecycleTransition(current, next) {
		return SharedWorkerState{}, fmt.Errorf("invalid worker lifecycle transition %q -> %q", current, next)
	}

	nextState := SharedWorkerState{Lifecycle: next}
	switch next {
	case WorkerLifecycleIdle:
		nextState.Assignment = nil
	case WorkerLifecycleReserved, WorkerLifecycleActivating, WorkerLifecycleHot, WorkerLifecycleDraining:
		resolved, err := resolveWorkerAssignment(s.Assignment, assignment)
		if err != nil {
			return SharedWorkerState{}, err
		}
		nextState.Assignment = resolved
	case WorkerLifecycleRetired:
		if assignment != nil {
			resolved, err := resolveWorkerAssignment(s.Assignment, assignment)
			if err != nil {
				return SharedWorkerState{}, err
			}
			nextState.Assignment = resolved
		} else {
			nextState.Assignment = cloneWorkerAssignment(s.Assignment)
		}
	default:
		return SharedWorkerState{}, fmt.Errorf("unknown worker lifecycle %q", next)
	}

	if err := nextState.Validate(); err != nil {
		return SharedWorkerState{}, err
	}
	return nextState, nil
}

func isAllowedWorkerLifecycleTransition(current, next WorkerLifecycleState) bool {
	switch current {
	case WorkerLifecycleIdle:
		return next == WorkerLifecycleReserved || next == WorkerLifecycleRetired
	case WorkerLifecycleReserved:
		return next == WorkerLifecycleActivating || next == WorkerLifecycleRetired
	case WorkerLifecycleActivating:
		return next == WorkerLifecycleHot || next == WorkerLifecycleRetired
	case WorkerLifecycleHot:
		return next == WorkerLifecycleDraining || next == WorkerLifecycleRetired
	case WorkerLifecycleDraining:
		return next == WorkerLifecycleRetired
	case WorkerLifecycleRetired:
		return false
	default:
		return false
	}
}

func resolveWorkerAssignment(current, proposed *WorkerAssignment) (*WorkerAssignment, error) {
	if current != nil {
		if err := validateWorkerAssignment(current); err != nil {
			return nil, fmt.Errorf("current assignment: %w", err)
		}
	}
	if proposed != nil {
		if err := validateWorkerAssignment(proposed); err != nil {
			return nil, fmt.Errorf("proposed assignment: %w", err)
		}
	}

	switch {
	case current == nil && proposed == nil:
		return nil, fmt.Errorf("assignment is required")
	case current == nil:
		return cloneWorkerAssignment(proposed), nil
	case proposed == nil:
		return cloneWorkerAssignment(current), nil
	case current.TeamName != proposed.TeamName:
		return nil, fmt.Errorf("assignment team cannot change from %q to %q", current.TeamName, proposed.TeamName)
	default:
		return cloneWorkerAssignment(proposed), nil
	}
}

func validateWorkerAssignment(assignment *WorkerAssignment) error {
	if assignment == nil {
		return fmt.Errorf("missing assignment")
	}
	if assignment.TeamName == "" {
		return fmt.Errorf("missing team name")
	}
	if assignment.LeaseExpiresAt.IsZero() {
		return fmt.Errorf("missing lease expiry")
	}
	return nil
}

func cloneWorkerAssignment(assignment *WorkerAssignment) *WorkerAssignment {
	if assignment == nil {
		return nil
	}
	cloned := *assignment
	return &cloned
}

func cloneSharedWorkerState(state SharedWorkerState) SharedWorkerState {
	return SharedWorkerState{
		Lifecycle:  state.NormalizedLifecycle(),
		Assignment: cloneWorkerAssignment(state.Assignment),
	}
}
