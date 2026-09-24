package trinopool

import (
	"strings"
	"testing"
)

func TestPlanConfigurationOnlyRollout(t *testing.T) {
	for _, test := range []struct {
		name           string
		serving, surge int
		action         PlanAction
	}{
		{"surge at serving floor", 3, 1, PlanActionCreate},
		{"drain above serving floor", 4, 1, PlanActionDrain},
		{"wait without surge budget", 3, 0, PlanActionNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := servingPool("r1", test.serving)
			state.MaxSurge = test.surge
			state.Instances[0].SpecOutdated = true
			plan := PlanNext(state)
			if plan.Action != test.action || plan.Repair {
				t.Fatalf("plan = %+v", plan)
			}
			if plan.Action == PlanActionDrain && plan.InstanceID != state.Instances[0].ID {
				t.Fatal("drained a current instance")
			}
			if strings.Contains(plan.Reason, "release") {
				t.Fatalf("configuration-only reason names a release: %s", plan.Reason)
			}
		})
	}
}

func TestPlanUnreadableRolloutEvidenceIsInstanceLocal(t *testing.T) {
	state := servingPool("r1", 4)
	state.Instances[0].RolloutBlocked = true
	state.Instances[0].ReleaseID = "r0"
	state.Instances[1].SpecOutdated = true
	plan := PlanNext(state)
	if plan.Action != PlanActionDrain || plan.InstanceID != state.Instances[1].ID {
		t.Fatalf("unreadable sibling blocked a safe configuration drain: %+v", plan)
	}
	state.Instances[1].SpecOutdated = false
	plan = PlanNext(state)
	if plan.Action != PlanActionNone || !strings.Contains(plan.Reason, "unreadable") {
		t.Fatalf("unreadable evidence was ignored or selected for drain: %+v", plan)
	}
}

func servingPool(release string, count int) PoolState {
	state := PoolState{DesiredInstances: 3, MinServing: 3, MaxSurge: 1, MaxRepair: 1, DesiredReleaseID: release}
	for index := 0; index < count; index++ {
		state.Instances = append(state.Instances, InstanceView{
			ID: string(rune('a'+index)) + "-instance", Phase: PhaseServing, ReleaseID: release,
		})
	}
	return state
}

func TestPlanCreatesUpToDesiredCount(t *testing.T) {
	plan := PlanNext(servingPool("r1", 1))
	if plan.Action != PlanActionCreate || plan.Repair {
		t.Fatalf("expected a plain create, got %+v", plan)
	}
}

func TestPlanIsSatisfiedAtDesiredCount(t *testing.T) {
	if plan := PlanNext(servingPool("r1", 3)); plan.Action != PlanActionNone {
		t.Fatalf("expected no action at the desired count, got %+v", plan)
	}
}

// Missing or unreadable desired configuration must freeze the pool at its
// last-good state. It must never be read as "desired count zero", which would
// delete the fleet.
func TestFrozenPoolTakesNoAction(t *testing.T) {
	state := servingPool("r1", 1)
	state.Frozen = true
	if plan := PlanNext(state); plan.Action != PlanActionNone {
		t.Fatalf("a frozen pool planned %+v", plan)
	}
}

func TestPlanNeverActsOnAnUnconfiguredPool(t *testing.T) {
	// A zero-valued desired count is missing configuration, not an instruction.
	state := servingPool("r1", 3)
	state.DesiredInstances = 0
	if plan := PlanNext(state); plan.Action != PlanActionNone {
		t.Fatalf("an unconfigured pool planned %+v", plan)
	}
}

// One lifecycle operation at a time: a second create while one is preparing
// would let a stuck rollout spawn an unbounded chain of instances.
func TestPlanWaitsForAnInFlightCreate(t *testing.T) {
	state := servingPool("r1", 1)
	state.Instances = append(state.Instances, InstanceView{ID: "new", Phase: PhasePreparing, ReleaseID: "r1"})
	if plan := PlanNext(state); plan.Action != PlanActionNone {
		t.Fatalf("expected the planner to wait, got %+v", plan)
	}
}

func TestPlanWaitsForAnInFlightDrain(t *testing.T) {
	state := servingPool("r2", 3)
	state.Instances[0].ReleaseID = "r1"
	state.Instances = append(state.Instances, InstanceView{ID: "new", Phase: PhaseDraining, ReleaseID: "r2"})
	if plan := PlanNext(state); plan.Action != PlanActionNone {
		t.Fatalf("expected the planner to wait for the drain, got %+v", plan)
	}
}

// A new release surges one instance above the desired count, and only one.
func TestPlanSurgesOneInstanceForANewRelease(t *testing.T) {
	state := servingPool("r1", 3)
	state.DesiredReleaseID = "r2"
	plan := PlanNext(state)
	if plan.Action != PlanActionCreate || plan.Repair {
		t.Fatalf("expected a surge create, got %+v", plan)
	}
}

func TestPlanDrainsTheOutdatedInstanceOnlyOnceTheSurgeIsServing(t *testing.T) {
	state := servingPool("r1", 3)
	state.DesiredReleaseID = "r2"
	state.Instances = append(state.Instances, InstanceView{ID: "surge", Phase: PhaseServing, ReleaseID: "r2"})

	plan := PlanNext(state)
	if plan.Action != PlanActionDrain {
		t.Fatalf("expected a drain, got %+v", plan)
	}
	if plan.InstanceID == "surge" {
		t.Fatal("the planner drained the new instance instead of an outdated one")
	}
}

// The floor is the whole point of the surge: a planned drain may never take the
// ready serving count below the minimum.
func TestPlanNeverDrainsBelowTheServingFloor(t *testing.T) {
	state := servingPool("r1", 3)
	state.DesiredReleaseID = "r2"
	// No surge instance exists, so draining now would leave two serving.
	for _, instance := range state.Instances {
		if instance.Phase == PhaseServing && instance.ReleaseID != "r2" {
			// sanity: the fixture really is outdated
			goto check
		}
	}
	t.Fatal("fixture is not outdated")
check:
	if plan := PlanNext(state); plan.Action == PlanActionDrain {
		t.Fatalf("planner drained below the serving floor: %+v", plan)
	}
}

// Restoring lost capacity outranks upgrading a release.
func TestPlanRepairsLostCapacityBeforeUpgrading(t *testing.T) {
	state := servingPool("r1", 3)
	state.DesiredReleaseID = "r2"
	state.Instances[0].Phase = PhaseLost
	plan := PlanNext(state)
	if plan.Action != PlanActionCreate || !plan.Repair {
		t.Fatalf("expected a repair create, got %+v", plan)
	}
}

// The repair budget is one extra live instance, not an open-ended supply: a
// flapping cluster must block and alert rather than spawn forever.
func TestPlanBoundsTheRepairBudget(t *testing.T) {
	state := servingPool("r1", 3)
	for index := range state.Instances {
		state.Instances[index].Phase = PhaseLost
	}
	state.Instances = append(state.Instances, InstanceView{ID: "repair", Phase: PhasePreparing, ReleaseID: "r1", Repair: true})
	plan := PlanNext(state)
	if plan.Action != PlanActionNone {
		t.Fatalf("expected the repair budget to bound the plan, got %+v", plan)
	}
	if plan.Reason == "" {
		t.Fatal("a blocked plan must carry a reason for the operator")
	}
}

// A terminal tombstone records history; it must not hold a live compute slot.
func TestRetiredInstancesDoNotConsumeCapacity(t *testing.T) {
	state := servingPool("r1", 3)
	for index := 0; index < 5; index++ {
		state.Instances = append(state.Instances, InstanceView{ID: "old", Phase: PhaseRetired, ReleaseID: "r0"})
	}
	if plan := PlanNext(state); plan.Action != PlanActionNone {
		t.Fatalf("tombstones changed the plan: %+v", plan)
	}
}

func TestPlanReportsBlockedSurge(t *testing.T) {
	// Desired release changed, the surge slot is already spent by a stuck
	// instance that is serving but still outdated: nothing may be forced.
	state := PoolState{DesiredInstances: 3, MinServing: 3, MaxSurge: 1, MaxRepair: 1, DesiredReleaseID: "r2"}
	for index := 0; index < 4; index++ {
		state.Instances = append(state.Instances, InstanceView{ID: string(rune('a' + index)), Phase: PhaseServing, ReleaseID: "r1"})
	}
	plan := PlanNext(state)
	if plan.Action != PlanActionDrain {
		t.Fatalf("expected the extra outdated instance to be drained, got %+v", plan)
	}
}

// A repair has to NAME the instance it replaces. The Gateway charges an
// activation to the repair budget only when repairFor points at a failed
// member; without it the repair spends the single planned surge, so a failure
// during a release rollout cannot be repaired at all.
func TestRepairPlanNamesTheInstanceItReplaces(t *testing.T) {
	state := servingPool("r1", 3)
	state.Instances[1].ID = "broken-one"
	state.Instances[1].Phase = PhaseLost

	plan := PlanNext(state)
	if plan.Action != PlanActionCreate || !plan.Repair {
		t.Fatalf("expected a repair create, got %+v", plan)
	}
	if plan.RepairFor != "broken-one" {
		t.Fatalf("repair names %q, want the failed instance", plan.RepairFor)
	}
}

// A SUSPECT instance is not yet proven dead, so it is not yet a repair target:
// naming it would charge the repair budget for a member that may still recover.
func TestRepairPrefersAProvenFailure(t *testing.T) {
	state := servingPool("r1", 3)
	state.Instances[0].ID = "suspected"
	state.Instances[0].Phase = PhaseSuspect
	state.Instances[1].ID = "lost-one"
	state.Instances[1].Phase = PhaseLost

	plan := PlanNext(state)
	if plan.RepairFor != "lost-one" {
		t.Fatalf("repair names %q, want the lost instance", plan.RepairFor)
	}
}
