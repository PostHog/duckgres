package trinopool

import "testing"

func TestPhaseTransitionsFollowTheLifecycle(t *testing.T) {
	allowed := [][2]Phase{
		{PhasePending, PhaseCreating},
		{PhaseCreating, PhasePreparing},
		{PhasePreparing, PhaseValidating},
		{PhaseValidating, PhaseAdmitted},
		{PhaseAdmitted, PhaseServing},
		{PhaseServing, PhaseDraining},
		{PhaseDraining, PhaseSealed},
		{PhaseSealed, PhaseRetiring},
		{PhaseRetiring, PhaseRetired},
	}
	for _, step := range allowed {
		if err := ValidateTransition(step[0], step[1]); err != nil {
			t.Errorf("%s -> %s rejected: %v", step[0], step[1], err)
		}
	}
}

// Retirement is the one irreversible claim in the whole design: once Gateway
// has issued it, the instance's incarnation can never serve again. A resume
// would let a retired member take new work.
func TestRetiringNeverResumes(t *testing.T) {
	for _, target := range []Phase{PhaseServing, PhaseAdmitted, PhaseDraining, PhaseSealed, PhasePreparing, PhaseValidating} {
		if err := ValidateTransition(PhaseRetiring, target); err == nil {
			t.Errorf("RETIRING -> %s was allowed", target)
		}
		if err := ValidateTransition(PhaseRetired, target); err == nil {
			t.Errorf("RETIRED -> %s was allowed", target)
		}
	}
}

// A drained instance is gone; a lost one is a failure with preserved history.
// Collapsing the two would report a dead coordinator's abandoned queries as a
// successful drain.
func TestFailureBranchIsSeparateFromDrain(t *testing.T) {
	if err := ValidateTransition(PhaseServing, PhaseSuspect); err != nil {
		t.Fatalf("SERVING -> SUSPECT rejected: %v", err)
	}
	if err := ValidateTransition(PhaseSuspect, PhaseLost); err != nil {
		t.Fatalf("SUSPECT -> LOST rejected: %v", err)
	}
	if err := ValidateTransition(PhaseLost, PhaseFailureRetired); err != nil {
		t.Fatalf("LOST -> FAILURE_RETIRED rejected: %v", err)
	}
	// A suspected member leaves through the planned drain when it cannot be
	// proven dead - a crash-looping coordinator keeps its objects, so a loss
	// claim never gets its evidence and the member would otherwise hold its
	// slot forever.
	if err := ValidateTransition(PhaseSuspect, PhaseDraining); err != nil {
		t.Fatalf("SUSPECT -> DRAINING rejected: %v", err)
	}
	// It does NOT come back locally. The Gateway excluded it and only a fresh
	// certified admission un-excludes it, so a local recovery would leave this
	// row claiming a member serves while the Gateway routes nothing to it.
	if err := ValidateTransition(PhaseSuspect, PhaseServing); err == nil {
		t.Fatal("SUSPECT -> SERVING was allowed; a local recovery diverges from the Gateway")
	}
	// ... but a lost one cannot, and it must never look like a clean drain.
	for _, target := range []Phase{PhaseServing, PhaseSealed, PhaseRetired} {
		if err := ValidateTransition(PhaseLost, target); err == nil {
			t.Errorf("LOST -> %s was allowed", target)
		}
	}
}

// A candidate that failed before it was ever admitted is the only instance the
// operator may clean up without a Gateway retirement receipt.
func TestFailedPreparingIsReachableOnlyBeforeAdmission(t *testing.T) {
	for _, from := range []Phase{PhasePending, PhaseCreating, PhasePreparing, PhaseValidating} {
		if err := ValidateTransition(from, PhaseFailedPreparing); err != nil {
			t.Errorf("%s -> FAILED_PREPARING rejected: %v", from, err)
		}
	}
	for _, from := range []Phase{PhaseAdmitted, PhaseServing, PhaseDraining, PhaseSealed} {
		if err := ValidateTransition(from, PhaseFailedPreparing); err == nil {
			t.Errorf("%s -> FAILED_PREPARING was allowed after admission", from)
		}
	}
}

func TestPhaseClassification(t *testing.T) {
	// Serving capacity is what the minimum-serving floor counts.
	if !PhaseServing.Serving() || PhaseDraining.Serving() || PhaseAdmitted.Serving() {
		t.Error("serving classification is wrong")
	}
	// Live compute is what the surge budget counts: anything that occupies a
	// pod, including a draining or suspect instance.
	//
	// FAILED_PREPARING is in this group deliberately: the candidate's pods are
	// still running and its Gateway member is still PREPARING, which the
	// Gateway counts as live. Treating it as a tombstone hid a whole leaked
	// cluster and let one failed candidate exhaust the registration budget.
	for _, phase := range []Phase{PhasePending, PhaseCreating, PhasePreparing, PhaseValidating, PhaseAdmitted, PhaseServing, PhaseDraining, PhaseSealed, PhaseRetiring, PhaseSuspect, PhaseLost, PhaseFailedPreparing} {
		if !phase.OccupiesCapacity() {
			t.Errorf("%s should occupy capacity", phase)
		}
	}
	// A failed candidate is cleaned up rather than abandoned.
	if PhaseFailedPreparing.Terminal() {
		t.Error("FAILED_PREPARING must be able to reach FAILURE_RETIRED, or its resources and Gateway member leak")
	}
	// A historical tombstone must not consume a live slot forever.
	for _, phase := range []Phase{PhaseRetired, PhaseFailureRetired} {
		if phase.OccupiesCapacity() {
			t.Errorf("%s should not occupy capacity", phase)
		}
		if !phase.Terminal() {
			t.Errorf("%s should be terminal", phase)
		}
	}
}

// Deleting Kubernetes objects before Gateway has irreversibly claimed the
// incarnation can destroy running queries.
func TestOnlyRetirementPhasesPermitDeletion(t *testing.T) {
	for _, phase := range []Phase{PhaseRetiring, PhaseRetired, PhaseFailureRetired, PhaseFailedPreparing} {
		if !phase.PermitsResourceDeletion() {
			t.Errorf("%s should permit deletion", phase)
		}
	}
	for _, phase := range []Phase{PhasePending, PhaseCreating, PhasePreparing, PhaseValidating, PhaseAdmitted, PhaseServing, PhaseDraining, PhaseSealed, PhaseSuspect, PhaseLost} {
		if phase.PermitsResourceDeletion() {
			t.Errorf("%s must not permit deletion", phase)
		}
	}
}

func TestUnknownPhaseIsRejected(t *testing.T) {
	if err := ValidateTransition(Phase("BANANA"), PhaseServing); err == nil {
		t.Fatal("an unknown phase was accepted")
	}
	if Phase("BANANA").Valid() {
		t.Fatal("an unknown phase reported itself valid")
	}
}
