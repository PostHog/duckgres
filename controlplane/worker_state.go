package controlplane

import (
	"fmt"
	"strconv"
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
	WorkerLifecycleHotIdle    WorkerLifecycleState = "hot_idle"
	WorkerLifecycleDraining   WorkerLifecycleState = "draining"
	WorkerLifecycleRetired    WorkerLifecycleState = "retired"
)

// WorkerProfile describes the pod shape a session asked for via connection-string
// startup options (duckgres.colocate / worker_cpu / worker_memory / worker_tier).
// It is a match dimension on WorkerAssignment, ORTHOGONAL to Image: a reserved or
// warm worker may only be handed to a request whose profile Equal()s it.
//
// The nil/zero profile is the DEFAULT exclusive profile — today's behavior:
// empty CPU/Memory (the pool-global request applies) and Colocate=false (one pod
// per node). Normalizing the default to empty strings (rather than the literal
// 46/360 pool values) is what keeps legacy worker records claimable without a
// data migration.
//
// Only these three fields are persisted (WorkerRecord) and matched. Pod
// scheduling — nodeSelector, toleration, and the one-pod-per-node anti-affinity —
// is DERIVED from Colocate plus the pool's config at spawn time, so the
// reserved-spawn path can reconstruct everything it needs from the stored record.
type WorkerProfile struct {
	CPU      string // normalized k8s quantity (e.g. "4"); "" => pool-global request
	Memory   string // normalized k8s quantity (e.g. "16Gi"); "" => pool-global request
	Colocate bool   // true => bin-pack (exclusiveNode=false); false => exclusive node
}

// MatchKey is the identity used to decide whether an existing worker can serve a
// request. NodeSelector is excluded because it is derived from Colocate. A nil
// profile shares the key of the zero/default profile, so legacy and default
// requests match the same workers.
func (wp *WorkerProfile) MatchKey() string {
	if wp == nil {
		return "||false"
	}
	return wp.CPU + "|" + wp.Memory + "|" + strconv.FormatBool(wp.Colocate)
}

// Equal reports whether two profiles match (nil == zero == default).
func (wp *WorkerProfile) Equal(other *WorkerProfile) bool {
	return wp.MatchKey() == other.MatchKey()
}

// Parts returns the persisted/match primitives for the profile, decomposing nil
// to the default profile. Used to cross the controlplane→configstore package
// boundary (which cannot reference the WorkerProfile type) without losing the
// nil==default convention.
func (wp *WorkerProfile) Parts() (cpu, memory string, colocate bool) {
	if wp == nil {
		return "", "", false
	}
	return wp.CPU, wp.Memory, wp.Colocate
}

// WorkerAssignment carries tenant-specific metadata once a shared worker has
// been reserved for an org.
type WorkerAssignment struct {
	OrgID      string
	MaxWorkers int
	Image      string
	// Profile is the requested worker shape. nil => the default exclusive
	// profile (today's behavior). It is immutable for a reserved worker's life;
	// enforcement is wired in alongside the scheduling changes (see design doc).
	Profile *WorkerProfile
	// MaxColocatedCPU / MaxColocatedMemBytes are the org's colocated resource
	// budget, enforced authoritatively (cross-CP) inside the claim transaction.
	// 0 = unbounded on that axis. Only colocated claims count against them.
	MaxColocatedCPU      int
	MaxColocatedMemBytes uint64
	// SuppressWarmMissRecord skips recording a warm-capacity miss (demand signal
	// + metric) for this acquire attempt. Set by a server-side acquire wait that
	// polls repeatedly so it records demand at most once per throttle interval
	// instead of on every retry, keeping the demand signal and miss counter from
	// being inflated ~Nx by a single waiting connection.
	SuppressWarmMissRecord bool
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
	case WorkerLifecycleReserved, WorkerLifecycleActivating, WorkerLifecycleHot, WorkerLifecycleHotIdle:
		if err := validateWorkerAssignment(s.Assignment); err != nil {
			return fmt.Errorf("lifecycle %q requires a valid assignment: %w", lifecycle, err)
		}
		return nil
	case WorkerLifecycleDraining, WorkerLifecycleRetired:
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
	case WorkerLifecycleReserved, WorkerLifecycleActivating, WorkerLifecycleHot, WorkerLifecycleHotIdle:
		resolved, err := resolveWorkerAssignment(s.Assignment, assignment)
		if err != nil {
			return SharedWorkerState{}, err
		}
		nextState.Assignment = resolved
	case WorkerLifecycleDraining:
		if s.Assignment != nil || assignment != nil {
			resolved, err := resolveWorkerAssignment(s.Assignment, assignment)
			if err != nil {
				return SharedWorkerState{}, err
			}
			nextState.Assignment = resolved
		}
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
		return next == WorkerLifecycleReserved || next == WorkerLifecycleDraining || next == WorkerLifecycleRetired
	case WorkerLifecycleReserved:
		return next == WorkerLifecycleActivating || next == WorkerLifecycleDraining || next == WorkerLifecycleRetired
	case WorkerLifecycleActivating:
		return next == WorkerLifecycleHot || next == WorkerLifecycleDraining || next == WorkerLifecycleRetired
	case WorkerLifecycleHot:
		return next == WorkerLifecycleDraining || next == WorkerLifecycleRetired || next == WorkerLifecycleHotIdle
	case WorkerLifecycleHotIdle:
		return next == WorkerLifecycleReserved || next == WorkerLifecycleDraining || next == WorkerLifecycleRetired
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
	case current.OrgID != proposed.OrgID:
		return nil, fmt.Errorf("assignment org cannot change from %q to %q", current.OrgID, proposed.OrgID)
	default:
		return cloneWorkerAssignment(proposed), nil
	}
}

func validateWorkerAssignment(assignment *WorkerAssignment) error {
	if assignment == nil {
		return fmt.Errorf("missing assignment")
	}
	if assignment.OrgID == "" {
		return fmt.Errorf("missing org ID")
	}
	return nil
}

func cloneWorkerAssignment(assignment *WorkerAssignment) *WorkerAssignment {
	if assignment == nil {
		return nil
	}
	cloned := *assignment
	cloned.Profile = cloneWorkerProfile(assignment.Profile)
	return &cloned
}

func cloneWorkerProfile(profile *WorkerProfile) *WorkerProfile {
	if profile == nil {
		return nil
	}
	cloned := *profile
	return &cloned
}

func cloneSharedWorkerState(state SharedWorkerState) SharedWorkerState {
	return SharedWorkerState{
		Lifecycle:  state.NormalizedLifecycle(),
		Assignment: cloneWorkerAssignment(state.Assignment),
	}
}
