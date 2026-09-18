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
			CoordinatorID: instance.CoordinatorNodeID,
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
