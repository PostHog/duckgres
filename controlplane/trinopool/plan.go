package trinopool

import "sort"

// PlanAction is the single lifecycle step the operator may take for a pool on
// one tick. One step at a time is deliberate: a stuck rollout must not be able
// to spawn a chain of replacements while nobody is looking.
type PlanAction string

const (
	PlanActionNone   PlanAction = "none"
	PlanActionCreate PlanAction = "create"
	PlanActionDrain  PlanAction = "drain"
)

// InstanceView is the planner's read-only projection of an instance row.
type InstanceView struct {
	ID        string
	Phase     Phase
	ReleaseID string
	// SpecOutdated compares desired configuration with this instance's immutable specification.
	SpecOutdated bool
	// Repair marks an instance created against the failure-repair budget rather
	// than the planned surge budget. The two budgets are separate so a failure
	// during a release rollout does not stall capacity restoration.
	Repair bool
	// CreatedAt orders drain candidates deterministically; zero is fine, ID is
	// the tiebreaker.
	CreatedAt int64
}

// PoolState is everything the planner needs. It carries no clients and no
// clock, so every decision is reproducible in a test.
type PoolState struct {
	DesiredInstances int
	MinServing       int
	MaxSurge         int
	MaxRepair        int
	DesiredReleaseID string
	// Frozen is set when desired configuration is missing or invalid. The pool
	// then holds its last-good state: no creates, no drains, no deletes.
	Frozen       bool
	FrozenReason string
	Instances    []InstanceView
}

// Plan is the decided step plus the reason, which is surfaced to operators.
// A blocked plan always explains itself: "nothing happened" is the hardest
// state to debug from metrics alone.
type Plan struct {
	Action     PlanAction
	InstanceID string
	Repair     bool
	// RepairFor names the failed instance a repair replaces. The Gateway
	// charges an activation to the repair budget only when it is set, so a
	// repair without it silently spends the single planned surge instead.
	RepairFor string
	Reason    string
}

// PlanNext decides the one step to take. The order of the rules is the
// contract:
//
//  1. A frozen or unconfigured pool does nothing at all.
//  2. Restoring capacity below the desired count outranks upgrading a release.
//  3. Only one create may be in flight, and only one drain.
//  4. A drain happens only once the replacement is actually serving and the
//     serving floor survives the departure.
//  5. Nothing is ever forced to free a budget.
func PlanNext(state PoolState) Plan {
	if state.Frozen {
		return Plan{Action: PlanActionNone, Reason: blockedReason("pool is frozen", state.FrozenReason)}
	}
	// A zero desired count is missing configuration, never an instruction to
	// empty the pool.
	if state.DesiredInstances < 1 {
		return Plan{Action: PlanActionNone, Reason: "pool has no desired instance count"}
	}

	var live, healthy, serving, preparing, repairing, draining int
	for _, instance := range state.Instances {
		if !instance.Phase.OccupiesCapacity() {
			continue
		}
		live++
		switch instance.Phase {
		case PhaseSuspect, PhaseLost, PhaseFailedPreparing:
			// Still holds a pod, but cannot be counted on to serve. A failed
			// candidate is in this group until its cleanup completes: counting
			// it as healthy would hide the capacity deficit it caused.
		default:
			healthy++
		}
		if instance.Phase.Serving() {
			serving++
		}
		if preServing(instance.Phase) {
			preparing++
			if instance.Repair {
				repairing++
			}
		}
		if instance.Phase == PhaseDraining || instance.Phase == PhaseSealed || instance.Phase == PhaseRetiring {
			draining++
		}
	}

	// 2. Capacity deficit. Counting `preparing` here is what keeps a slow
	// replacement from being duplicated every tick.
	if deficit := state.DesiredInstances - healthy; deficit > preparing {
		// Filling an empty pool needs no special budget. Only a deficit caused
		// by instances that still HOLD a slot while being unable to serve
		// (SUSPECT/LOST, pods not yet verified absent) needs the repair budget,
		// because the replacement necessarily runs above the desired count.
		if live < state.DesiredInstances {
			return Plan{Action: PlanActionCreate, Reason: "creating an instance to reach the desired instance count"}
		}
		if repairing >= state.MaxRepair {
			return Plan{Action: PlanActionNone, Reason: "capacity is short but the repair budget is exhausted; investigate the failed instances"}
		}
		if live >= state.DesiredInstances+state.MaxSurge+state.MaxRepair {
			return Plan{Action: PlanActionNone, Reason: "capacity is short but no live compute slot is free; investigate the failed instances"}
		}
		return Plan{
			Action: PlanActionCreate, Repair: true,
			RepairFor: repairTarget(state),
			Reason:    "restoring capacity lost to a failed instance",
		}
	}

	// 3. One lifecycle operation at a time.
	if preparing > 0 {
		return Plan{Action: PlanActionNone, Reason: "an instance is already preparing"}
	}
	if draining > 0 {
		return Plan{Action: PlanActionNone, Reason: "an instance is already draining"}
	}

	outdated := outdatedServing(state)
	if len(outdated) == 0 {
		return Plan{Action: PlanActionNone, Reason: "pool matches the desired specification and instance count"}
	}

	// 4. Drain only when the floor survives it.
	if serving-1 >= state.MinServing {
		return Plan{Action: PlanActionDrain, InstanceID: outdated[0].ID, Reason: "replacing an instance with an outdated specification"}
	}

	// 5. Otherwise surge one replacement, within the surge budget.
	if live >= state.DesiredInstances+state.MaxSurge {
		return Plan{Action: PlanActionNone, Reason: "release rollout is waiting for the surge budget to free up"}
	}
	return Plan{Action: PlanActionCreate, Reason: "surging a replacement for an instance with an outdated specification"}
}

// outdatedServing lists serving instances that do not run the desired specification,
// oldest first so replacement order is deterministic across leaders.
func outdatedServing(state PoolState) []InstanceView {
	var outdated []InstanceView
	for _, instance := range state.Instances {
		if instance.Phase.Serving() && (instance.ReleaseID != state.DesiredReleaseID || instance.SpecOutdated) {
			outdated = append(outdated, instance)
		}
	}
	sort.Slice(outdated, func(i, j int) bool {
		if outdated[i].CreatedAt != outdated[j].CreatedAt {
			return outdated[i].CreatedAt < outdated[j].CreatedAt
		}
		return outdated[i].ID < outdated[j].ID
	})
	return outdated
}

func preServing(phase Phase) bool {
	switch phase {
	case PhasePending, PhaseCreating, PhasePreparing, PhaseValidating, PhaseAdmitted:
		return true
	default:
		return false
	}
}

func blockedReason(base, detail string) string {
	if detail == "" {
		return base
	}
	return base + ": " + detail
}

// repairTarget picks the instance a repair replaces: a PROVEN failure first,
// because a SUSPECT member may still recover and charging the repair budget for
// it would spend a budget on a member that never failed. Oldest first, so the
// choice is stable across leaders.
func repairTarget(state PoolState) string {
	var suspect string
	var lost string
	for _, instance := range state.Instances {
		switch instance.Phase {
		case PhaseLost:
			if lost == "" || instance.ID < lost {
				lost = instance.ID
			}
		case PhaseSuspect:
			if suspect == "" || instance.ID < suspect {
				suspect = instance.ID
			}
		}
	}
	if lost != "" {
		return lost
	}
	return suspect
}
