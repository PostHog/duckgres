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

// trinoPoolSuspectDrainAfter is how long a member may stay excluded before it
// is replaced through the PLANNED path.
//
// SUSPECT had exactly one exit that freed its slot: LOST, which requires
// verified absence of every recorded object. A crash-looping coordinator keeps
// its Deployment forever, so it kept its slot forever, and a second such
// failure exhausted the repair budget and stalled the pool. Draining is the
// honest alternative: it is refused if it would break the serving floor, it
// preserves whatever work the member still holds, and it ends in a retirement
// receipt rather than a loss claim nobody could prove.
const trinoPoolSuspectDrainAfter = 15 * time.Minute

// trinoPoolIdentityObserveEvery paces the per-member identity probe. The
// question it answers - "is this still the process that was admitted" - changes
// only when a process restarts, so asking every tick would be one authenticated
// request per member per five seconds for an answer that almost never moves.
const trinoPoolIdentityObserveEvery = 30 * time.Second

// observeHealth moves a serving or admitted instance onto the failure branch
// when the cluster stops reporting a healthy coordinator, or when the process
// behind it is no longer the incarnation that was admitted.
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
		if healthy {
			// A ready pod with the same UID is not the same PROCESS: a
			// container can restart inside it and come back ready with a new
			// Trino incarnation. The Gateway binds the member to the boot
			// identity it registered and refuses to dispatch to anything else,
			// so this row would report SERVING while the pool quietly lost
			// capacity - green here, empty there.
			return o.observeProcessIdentity(ctx, instance, phase)
		}
		if time.Since(instance.PhaseChangedAt) < trinoPoolSuspectAfter {
			return false, nil
		}
		return true, o.suspectInstance(ctx, instance, phase, "the coordinator is not reporting healthy")
	case trinopool.PhaseSuspect:
		// There is deliberately no path back to service.
		//
		// Suspicion is the GATEWAY's state as much as this row's: it excluded
		// the member, and nothing short of a fresh certified admission puts it
		// back. Flipping the local row to SERVING would leave the Gateway
		// excluding a member this controller believes is serving - a row that
		// says one thing while the pool does another. Re-admitting is worse: a
		// member that failed its health check and recovered is an uncertain
		// incarnation, and the pool has a cheap way to get a certain one.
		//
		// So a suspected member always leaves, and only the ROUTE depends on the
		// evidence: proven dead, or drained like any planned replacement once
		// capacity allows it.
		if progressed, err := o.claimLossIfProven(ctx, instance, observed); progressed || err != nil {
			return progressed, err
		}
		if time.Since(instance.PhaseChangedAt) < trinoPoolSuspectDrainAfter {
			// A brief blip is given time to become provable one way or the
			// other before its replacement is started.
			return false, nil
		}
		return o.drainSuspectInstance(ctx, instance)
	default:
		return false, nil
	}
}

// observeProcessIdentity checks that the coordinator answering for a serving
// member is still the incarnation that was admitted.
//
// Bounded on purpose: it is one authenticated request per member per
// trinoPoolIdentityObserveEvery, not per tick, because the question it answers
// changes only when a process restarts.
//
// A probe that does not answer is NOT evidence. A timeout means the controller
// could not look - the same as a failed Observe - and a member is never
// suspected, let alone declared dead, on that basis. Only a DIFFERENT process
// identity is evidence, and the stored one is never quietly updated to match:
// the recorded identity is what the Gateway admitted, and a new process is a
// new member that has to earn its own admission.
func (o *trinoPoolOperator) observeProcessIdentity(
	ctx context.Context,
	instance configstore.TrinoPoolInstance,
	phase trinopool.Phase,
) (bool, error) {
	if o.identity == nil || instance.CoordinatorBootID == "" || instance.EndpointURL == "" {
		return false, nil
	}
	if last, seen := o.identityObservedAt[instance.InstanceID]; seen &&
		time.Since(last) < trinoPoolIdentityObserveEvery {
		return false, nil
	}

	bootID, err := o.identity(ctx, instance.EndpointURL)
	if err != nil {
		slog.Debug("Trino pool member did not answer the identity probe.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", err)
		return false, nil
	}
	if o.identityObservedAt == nil {
		o.identityObservedAt = map[string]time.Time{}
	}
	o.identityObservedAt[instance.InstanceID] = time.Now()
	if bootID == instance.CoordinatorBootID {
		return false, nil
	}

	slog.Warn("Trino pool member is answering with a different process than the one admitted.",
		"pool", o.config.PublicID, "instance", instance.InstanceID,
		"admitted", instance.CoordinatorBootID, "observed", bootID)
	return true, o.suspectInstance(ctx, instance, phase,
		"the coordinator process restarted; the admitted incarnation is gone")
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
// The route is the Gateway's OWN never-admitted retirement: a PREPARING member
// that has admitted no work may be retired directly, and the Gateway verifies
// that for itself rather than taking this controller's word for it. That claim
// is what authorizes deleting the objects, exactly as it does for a planned
// replacement - no loss claim, no termination evidence, and no pretending a
// candidate that never served was drained.
func (o *trinoPoolOperator) cleanupFailedCandidate(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	if instance.GatewayIncarnation == "" {
		// The candidate failed before it ever registered, so there is no member
		// to release: only the objects, if any, and the record.
		return o.removeFailedCandidateResources(ctx, instance, trinogateway.Member{})
	}

	// The Gateway is authoritative for its own member, and every step bumps the
	// generation, so the CAS value is read back rather than taken from the row.
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		return true, fmt.Errorf("read failed candidate %s: %w", instance.InstanceID, err)
	}
	switch member.Phase {
	case "RETIRING", "RETIRED":
		return o.removeFailedCandidateResources(ctx, instance, member)
	default:
		retired, err := o.gateway.RetireMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
			Step:               o.step(instance.InstanceID, "retire"),
			ExpectedGeneration: member.Generation,
		})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("claim retirement of failed candidate %s: %w", instance.InstanceID, err))
		}
		slog.Warn("Trino pool is retiring a candidate that could never be admitted.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", failureReason(instance))
		return true, o.dropAuthority(o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, instance.InstanceID, map[string]any{
			"gateway_state":      retired.Phase,
			"gateway_generation": retired.Generation,
		}))
	}
}

// removeFailedCandidateResources deletes a retired candidate's objects and
// closes its record once they are verifiably gone.
func (o *trinoPoolOperator) removeFailedCandidateResources(
	ctx context.Context,
	instance configstore.TrinoPoolInstance,
	member trinogateway.Member,
) (bool, error) {
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
	updates := map[string]any{}
	if member.InstanceID != "" && member.Phase != "RETIRED" {
		reported, err := o.gateway.MemberRetired(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
			Step:               o.step(instance.InstanceID, "retired"),
			ExpectedGeneration: member.Generation,
			ResourcesAbsent:    true,
		})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("report retirement of failed candidate %s: %w", instance.InstanceID, err))
		}
		updates["gateway_state"], updates["gateway_generation"] = reported.Phase, reported.Generation
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseFailedPreparing, trinopool.PhaseFailureRetired, updates))
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

// drainSuspectInstance replaces a member that is neither healthy nor provably
// gone, through the ordinary drain.
//
// The Gateway decides whether it may go: a drain that would breach the serving
// floor is refused, and that refusal is authoritative - it is never overridden,
// so a pool that is already at its floor keeps the flaky member rather than
// dropping below it. Nothing is destroyed here either; the drain ends in a
// retirement claim like any planned replacement.
func (o *trinoPoolOperator) drainSuspectInstance(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	member, err := o.gateway.DrainMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step:               o.step(instance.InstanceID, "drain"),
		ExpectedGeneration: instance.GatewayGeneration,
	})
	if err != nil {
		slog.Info("Trino pool cannot yet drain a suspected member.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", err)
		return false, o.dropAuthority(err)
	}
	slog.Warn("Trino pool is draining a member that stayed suspect.",
		"pool", o.config.PublicID, "instance", instance.InstanceID,
		"suspectFor", time.Since(instance.PhaseChangedAt).Round(time.Second))
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseSuspect, trinopool.PhaseDraining, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
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

	// The Gateway's retirement protocol still has to run. Closing the local row
	// while its member sat in LOST left the two records permanently
	// disagreeing about whether that incarnation was finished with - and the
	// retirement kind, FAILED, is the durable statement that its work was lost
	// rather than drained.
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		return true, fmt.Errorf("read lost member %s: %w", instance.InstanceID, err)
	}
	switch member.Phase {
	case "RETIRED":
		return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
			trinopool.PhaseLost, trinopool.PhaseFailureRetired, map[string]any{
				"gateway_state":      member.Phase,
				"gateway_generation": member.Generation,
			}))
	case "RETIRING":
		reported, err := o.gateway.MemberRetired(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
			Step:               o.step(instance.InstanceID, "retired"),
			ExpectedGeneration: member.Generation,
			ResourcesAbsent:    true,
		})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("report retirement of lost member %s: %w", instance.InstanceID, err))
		}
		return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
			trinopool.PhaseLost, trinopool.PhaseFailureRetired, map[string]any{
				"gateway_state":      reported.Phase,
				"gateway_generation": reported.Generation,
			}))
	default:
		claimed, err := o.gateway.RetireMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
			Step:               o.step(instance.InstanceID, "retire"),
			ExpectedGeneration: member.Generation,
		})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("claim retirement of lost member %s: %w", instance.InstanceID, err))
		}
		return true, o.dropAuthority(o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, instance.InstanceID, map[string]any{
			"gateway_state":      claimed.Phase,
			"gateway_generation": claimed.Generation,
		}))
	}
}
