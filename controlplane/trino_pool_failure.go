//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// The failure branch.
//
// A serving member whose pods are gone has NOT drained: its queries and
// transactions were lost, and recording that as a clean SEALED/RETIRED would
// erase the difference in the only durable record anyone will read afterwards.
// So failure is its own path, and every step of it is evidence-driven:
//
//   - SUSPECT means "stop sending new work here". It authorizes nothing
//     destructive and is reversible, because a probe failure is not death.
//   - LOST requires POSITIVE evidence that the exact incarnation terminated.
//     Kubernetes reporting the pods absent is such evidence; a timeout is not.
//   - Only after the Gateway records the loss may the resources be removed, and
//     the member is reported as failed rather than drained.

// trinoPoolSuspectAfter is how long an admitted or serving instance may look
// unhealthy before it is excluded from new work. It is deliberately not a
// deletion timer: nothing is destroyed at the end of it.
const trinoPoolSuspectAfter = 2 * time.Minute

// observeHealth moves a serving or admitted instance onto the failure branch
// when the cluster stops reporting a healthy coordinator, and back when it
// recovers.
func (o *trinoPoolOperator) observeHealth(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	phase := trinopool.Phase(instance.Phase)
	observed, err := o.kube(o.lease.Epoch).Observe(ctx, inventoryOf(instance))
	if err != nil {
		// Not being able to LOOK is not evidence about the member. The instance
		// keeps its phase and the next tick tries again.
		return false, nil
	}

	healthy := observed.CoordinatorReady && observed.CoordinatorPodUID == instance.CoordinatorPodUID
	switch phase {
	case trinopool.PhaseServing, trinopool.PhaseAdmitted:
		if healthy || time.Since(instance.PhaseChangedAt) < trinoPoolSuspectAfter {
			return false, nil
		}
		return true, o.suspectInstance(ctx, instance, phase, "the coordinator is not reporting healthy")
	case trinopool.PhaseSuspect:
		if healthy {
			// Recovery. A member excluded on suspicion returns to service
			// rather than being retired on the strength of a bad minute.
			slog.Info("Trino pool instance recovered from suspicion.",
				"pool", o.config.PublicID, "instance", instance.InstanceID)
			return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
				trinopool.PhaseSuspect, trinopool.PhaseServing, map[string]any{"last_error": ""}))
		}
		return o.claimLossIfProven(ctx, instance, observed)
	default:
		return false, nil
	}
}

func (o *trinoPoolOperator) suspectInstance(ctx context.Context, instance configstore.TrinoPoolInstance, from trinopool.Phase, reason string) error {
	member, err := o.gateway.SuspectMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.SuspectMemberRequest{
		Step:               o.step(instance.InstanceID, "suspect"),
		ExpectedGeneration: instance.GatewayGeneration,
		Reason:             reason,
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("suspect member %s: %w", instance.InstanceID, err))
	}
	slog.Warn("Trino pool instance suspected.",
		"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", reason)
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		from, trinopool.PhaseSuspect, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
			"last_error":         reason,
		}))
}

// claimLossIfProven records a loss ONLY with positive evidence that the exact
// incarnation is gone. Verified absence of every recorded object is that
// evidence; a failing probe is not, because a partitioned coordinator may still
// be serving queries nobody can see.
func (o *trinoPoolOperator) claimLossIfProven(ctx context.Context, instance configstore.TrinoPoolInstance, observed trinoPoolObservation) (bool, error) {
	absent, err := o.kube(o.lease.Epoch).ResourcesAbsent(ctx, inventoryOf(instance))
	if err != nil {
		return false, nil
	}
	if !absent || observed.PodsPresent != 0 {
		// Still present somewhere: the member stays SUSPECT, excluded from new
		// work, and nothing is deleted or declared.
		return false, nil
	}

	member, err := o.gateway.LostMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.LostMemberRequest{
		Step:               o.step(instance.InstanceID, "lost"),
		ExpectedGeneration: instance.GatewayGeneration,
		Evidence:           trinogateway.EvidenceProcessTerminated,
		Termination: trinogateway.TerminationProof{
			PodUID:        instance.CoordinatorPodUID,
			BootID:        instance.CoordinatorBootID,
			NodeID:        instance.CoordinatorNodeID,
			CoordinatorID: instance.CoordinatorID,
			Source:        "kubernetes-resources-absent",
			ObservedAt:    nowUTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("record loss of %s: %w", instance.InstanceID, err))
	}
	slog.Warn("Trino pool instance lost; its work was not drained.",
		"pool", o.config.PublicID, "instance", instance.InstanceID, "retirementKind", member.RetirementKind)
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseSuspect, trinopool.PhaseLost, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
			"failure_reason":     "the coordinator process terminated with outstanding work",
		}))
}

// cleanupFailedCandidate releases everything a candidate that can never be
// admitted is still holding.
//
// A FAILED_PREPARING instance is not finished business: its Deployments,
// Service and ConfigMaps are still running a whole Trino cluster, and its
// Gateway member is still PREPARING, which the Gateway counts as LIVE. One such
// instance at desired+surge is enough to refuse every later registration - no
// repair, no rollout - so this path is what keeps a single restarted candidate
// from wedging the pool.
//
// The order is deliberate. Kubernetes objects are deleted FIRST, because the
// loss claim needs positive evidence that the process terminated and a running
// pod is not that. Deleting before any Gateway step is sound only because this
// member provably never admitted work: it was refused before activation, so
// there is nothing to drain and nothing to lose.
func (o *trinoPoolOperator) cleanupFailedCandidate(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	inventory := inventoryOf(instance)
	kube := o.kube(o.lease.Epoch)
	if err := kube.Delete(ctx, inventory); err != nil {
		return true, fmt.Errorf("clean up failed candidate %s: %w", instance.InstanceID, err)
	}
	absent, err := kube.ResourcesAbsent(ctx, inventory)
	if err != nil || !absent {
		// Deletion is in progress. The instance keeps its slot until absence is
		// observed, so a terminating pod is never counted as freed capacity.
		return false, err
	}

	if instance.GatewayIncarnation == "" {
		// The candidate failed before it ever registered, so there is no member
		// to release.
		return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
			trinopool.PhaseFailedPreparing, trinopool.PhaseFailureRetired, nil))
	}

	// The Gateway is authoritative for its own member, and suspecting bumps the
	// generation, so the CAS value is read back rather than taken from the row.
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		return true, fmt.Errorf("read failed candidate %s: %w", instance.InstanceID, err)
	}
	switch member.Phase {
	case "PREPARING", "ACTIVE", "DRAINING", "SEALED":
		return true, o.suspectFailedCandidate(ctx, instance, member)
	case "SUSPECT":
		lost, err := o.gateway.LostMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.LostMemberRequest{
			Step:               o.step(instance.InstanceID, "lost"),
			ExpectedGeneration: member.Generation,
			Evidence:           trinogateway.EvidenceProcessTerminated,
			Termination: trinogateway.TerminationProof{
				PodUID:        instance.CoordinatorPodUID,
				BootID:        instance.CoordinatorBootID,
				NodeID:        instance.CoordinatorNodeID,
				CoordinatorID: instance.CoordinatorID,
				Source:        "kubernetes-resources-absent",
				ObservedAt:    nowUTC().Format(time.RFC3339),
			},
		})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("record loss of failed candidate %s: %w", instance.InstanceID, err))
		}
		return true, o.dropAuthority(o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, instance.InstanceID, map[string]any{
			"gateway_state":      lost.Phase,
			"gateway_generation": lost.Generation,
		}))
	default:
		// LOST or already retired: the slot is released and the record can be
		// closed.
		return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
			trinopool.PhaseFailedPreparing, trinopool.PhaseFailureRetired, map[string]any{
				"gateway_state":      member.Phase,
				"gateway_generation": member.Generation,
			}))
	}
}

func (o *trinoPoolOperator) suspectFailedCandidate(ctx context.Context, instance configstore.TrinoPoolInstance, member trinogateway.Member) error {
	suspected, err := o.gateway.SuspectMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.SuspectMemberRequest{
		Step:               o.step(instance.InstanceID, "suspect"),
		ExpectedGeneration: member.Generation,
		Reason:             failureReason(instance),
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("suspect failed candidate %s: %w", instance.InstanceID, err))
	}
	return o.dropAuthority(o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, instance.InstanceID, map[string]any{
		"gateway_state":      suspected.Phase,
		"gateway_generation": suspected.Generation,
	}))
}

// failureReason is what the Gateway records for the exclusion. It is never
// empty: the Gateway requires a reason, and "unknown" in a durable failure
// record is worse than a generic one.
func failureReason(instance configstore.TrinoPoolInstance) string {
	if reason := instance.FailureReason; reason != "" {
		return reason
	}
	return "the candidate could not be admitted"
}

// completeFailureRetirement finishes a LOST member. The resources are already
// verifiably absent - that was the evidence for the loss claim - so this
// records the terminal state and releases the slot the planner's repair budget
// is waiting on.
func (o *trinoPoolOperator) completeFailureRetirement(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	inventory := inventoryOf(instance)
	if err := o.kube(o.lease.Epoch).Delete(ctx, inventory); err != nil {
		return true, fmt.Errorf("clean up lost instance %s: %w", instance.InstanceID, err)
	}
	absent, err := o.kube(o.lease.Epoch).ResourcesAbsent(ctx, inventory)
	if err != nil || !absent {
		return false, err
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseLost, trinopool.PhaseFailureRetired, nil))
}
