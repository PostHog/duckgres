//go:build kubernetes

package controlplane

// Member lifecycle retries under a lost response, and the failure repair of a
// coordinator that restarted in place.
//
// The Gateway journals every mutation under its step identity and hashes the
// WHOLE request body, minus the authority envelope. A retry that rebuilds any
// other field from freshly read state therefore reports a CHANGED INTENT, and
// the step can never be resolved again: not on the next tick, not after a
// leader change, never. These tests drive the real loop against the fake that
// journals payloads the same way.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func TestALostResponseDoesNotChangeTheRetriedMemberRequest(t *testing.T) {
	// Registration re-observes the coordinator's boot identity on every
	// attempt. If the response is lost and the coordinator restarts before the
	// retry, the retry carries a different bootId under the same step id.
	t.Run("register after an in-place restart", func(t *testing.T) {
		harness := newOperatorHarness(t)
		observations := 0
		harness.operator.identity = func(context.Context, string) (string, error) {
			observations++
			if observations <= 1 {
				return "process-1", nil
			}
			return "process-2", nil
		}
		harness.gateway.loseResponse = map[string]bool{"register": true}

		harness.tickTolerant(10)

		instanceID := harness.store.order[0]
		instance := harness.store.instances[instanceID]
		if instance.Phase == string(trinopool.PhaseCreating) {
			t.Fatalf("phase = %s, want the lost registration resolved rather than a member "+
				"holding a live slot while the row never leaves CREATING", instance.Phase)
		}
		// The Gateway bound the member to the identity of the FIRST attempt, so
		// that is the incarnation this row has to carry - never the new process.
		if instance.CoordinatorBootID != "process-1" {
			t.Fatalf("recorded boot id = %q, want the identity the Gateway recorded", instance.CoordinatorBootID)
		}
	})

	// The suspicion reason is chosen by whichever check fired first. A lost
	// response followed by a tick that observes the OTHER condition sends a
	// different reason under the same step id.
	t.Run("suspect with a different reason", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.tick(t, 20)
		instanceID := harness.store.order[0]
		instance := harness.placeInstance(t, instanceID, trinopool.PhaseServing, "ACTIVE")

		// First the restart check fires; its response is lost.
		harness.operator.identity = func(context.Context, string) (string, error) { return "process-2", nil }
		harness.gateway.loseResponse = map[string]bool{"suspect": true}
		_ = harness.operator.reconcileOnce(context.Background())

		// Then the coordinator stops reporting ready, so the next attempt would
		// carry the unhealthy reason instead.
		harness.kube.observed.CoordinatorReady = false
		instance.PhaseChangedAt = time.Now().Add(-time.Hour)
		harness.tickTolerant(5)

		if instance.Phase != string(trinopool.PhaseSuspect) {
			t.Fatalf("phase = %s, want the lost suspicion resolved to SUSPECT", instance.Phase)
		}
	})

	// The loss claim stamps a fresh observedAt on every attempt.
	t.Run("lost", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.tick(t, 20)
		instanceID := harness.store.order[0]
		instance := harness.placeInstance(t, instanceID, trinopool.PhaseSuspect, "SUSPECT")
		harness.kube.absent = true
		harness.kube.observed.PodsPresent = 0
		harness.gateway.loseResponse = map[string]bool{"lost": true}

		// observedAt has second precision, so the retry must land in a later
		// second - which is what every real reconcile tick does.
		_ = harness.operator.reconcileOnce(context.Background())
		time.Sleep(1100 * time.Millisecond)
		harness.tickTolerant(6)

		if instance.Phase == string(trinopool.PhaseSuspect) {
			t.Fatalf("phase = %s, want the lost claim resolved: the member is LOST at the "+
				"Gateway and its objects are never cleaned up from here", instance.Phase)
		}
	})
}

// One instance that cannot make progress must not stop the pool: the planner
// runs after the instance loop, so an unrecoverable member used to block every
// repair, drain and replacement for as long as it stayed broken.
func TestOneUnrecoverableInstanceDoesNotStallTheRestOfThePool(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	wedged := harness.store.order[0]
	instance := harness.placeInstance(t, wedged, trinopool.PhaseSuspect, "SUSPECT")
	// A claim the Gateway refuses outright: retrying cannot change a decision.
	harness.gateway.lostErr = errors.New("gateway refused the loss claim")
	harness.kube.absent = true
	harness.kube.observed.PodsPresent = 0

	harness.tickTolerant(10)

	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want the refused claim to leave the member suspect", instance.Phase)
	}
	// The rest of the pool keeps converging: the planner still runs, so the
	// member that cannot recover is replaced rather than holding the pool.
	for _, id := range harness.store.order {
		if id == wedged {
			continue
		}
		if phase := harness.store.instances[id].Phase; phase == string(trinopool.PhasePending) {
			t.Fatalf("instance %s = %s, want the other instances to keep progressing", id, phase)
		}
	}
	if len(harness.store.order) < 4 {
		t.Fatalf("instances = %d, want the planner to have started a replacement despite the "+
			"stuck member", len(harness.store.order))
	}
}

// A coordinator that restarts in place keeps every object it had, so the
// absence evidence a loss claim used to require could never arrive. The member
// held its slot until the drain timer, and a drain cannot finish while the dead
// JVM's transactions are still pinned to it - so the instance, and the repair
// slot behind it, were retained forever.
func TestASamePodRestartWithPinnedWorkReachesFailureRepair(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	if instance.Phase != string(trinopool.PhaseServing) {
		t.Fatalf("phase = %s, want a serving member to start from", instance.Phase)
	}

	// Work the dead JVM will never finish: the Gateway keeps reporting it,
	// because a lost process does not drain.
	harness.gateway.obligations[instanceID] = trinogateway.Obligations{
		Generation: harness.gateway.members[instanceID].Generation,
		Phase:      "ACTIVE", OpenTransactions: 1, ActiveQueries: 2,
	}

	// The container restarted inside the same Pod: same pod UID, a new Trino
	// process, and Kubernetes' own record that the previous container ended.
	harness.operator.identity = func(context.Context, string) (string, error) { return "process-2", nil }
	// The identity probe is paced; this is the first observation after the
	// restart, which is what the real loop makes once the interval elapses.
	harness.operator.identityObservedAt = nil
	// Deleting the failed member's objects actually removes them, so the
	// retirement this test is about can reach its end.
	harness.kube.absentAfterDelete = true
	harness.kube.observed.CoordinatorPods = []trinoPoolCoordinatorPod{{
		UID:      "pod-uid-1",
		Restarts: 1,
		LastTerminated: &trinoPoolContainerTermination{
			ContainerID: "containerd://old", ExitCode: 137, Reason: "OOMKilled",
			FinishedAt: "2026-09-19T12:00:00Z",
		},
	}}

	harness.tickTolerant(20)

	if instance.Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatalf("phase = %s, want the restarted member to complete failure repair", instance.Phase)
	}
	member := harness.gateway.members[instanceID]
	if member.Phase != "RETIRED" || member.RetirementKind != "FAILED" {
		t.Fatalf("gateway member = %s/%s, want RETIRED/FAILED - its work was lost, not drained",
			member.Phase, member.RetirementKind)
	}
	// The claim rests on Kubernetes' own record that the container hosting the
	// admitted process ended, bound to that exact incarnation.
	claim := harness.gateway.lastLost
	if claim.Evidence != trinogateway.EvidenceProcessTerminated ||
		claim.Termination.Source != "kubernetes-coordinator-container-terminated" {
		t.Fatalf("loss evidence = %s/%s, want the container termination observation",
			claim.Evidence, claim.Termination.Source)
	}
	if claim.Termination.BootID != "process-1" || claim.Termination.PodUID != "pod-uid-1" {
		t.Fatalf("loss evidence identifies %s/%s, want the admitted incarnation",
			claim.Termination.PodUID, claim.Termination.BootID)
	}
	// Its pinned work must never be reported as a clean drain.
	for _, call := range harness.gateway.calls {
		if call == "seal:"+instanceID {
			t.Fatalf("sealed a member whose process died with work still pinned to it")
		}
	}
	// The repair slot is released, so the pool can replace it.
	if len(harness.store.order) < 4 {
		t.Fatalf("instances = %d, want a replacement for the failed member", len(harness.store.order))
	}
}

// A probe that answers with the admitted identity is not evidence of anything,
// and a pod that never restarted leaves no termination record. Neither may
// produce a loss claim: a member is only ever declared dead on proof.
func TestARestartlessSuspicionIsNeverDeclaredLost(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	instanceID := harness.store.order[0]
	instance := harness.placeInstance(t, instanceID, trinopool.PhaseSuspect, "SUSPECT")

	// The pod is present, has never restarted, and the process still answers
	// with the identity that was admitted.
	harness.kube.observed.CoordinatorPods = []trinoPoolCoordinatorPod{{UID: "pod-uid-1"}}
	harness.operator.identity = func(context.Context, string) (string, error) { return "process-1", nil }

	harness.tickTolerant(5)

	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want a suspicion with no termination evidence to stay suspect", instance.Phase)
	}
	for _, call := range harness.gateway.calls {
		if call == "lost:"+instanceID {
			t.Fatalf("claimed a loss without evidence that the process ended")
		}
	}
}
