//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

type trinoPoolRecoveryStore interface {
	ListTrinoPoolRecoveries(context.Context, string) ([]configstore.TrinoPoolRecovery, error)
}

func (o *trinoPoolOperator) recoveryRequests(ctx context.Context) (map[string]configstore.TrinoPoolRecovery, error) {
	requests := make(map[string]configstore.TrinoPoolRecovery)
	store, ok := o.store.(trinoPoolRecoveryStore)
	if !ok {
		return requests, nil
	}
	rows, err := store.ListTrinoPoolRecoveries(ctx, o.config.PoolID)
	if err != nil {
		return nil, fmt.Errorf("read pool recovery requests: %w", err)
	}
	for _, row := range rows {
		requests[row.InstanceID] = row
	}
	return requests, nil
}

// Keep the local phase DRAINING until the Gateway grants failed retirement.
// An older leader must not enter its LOST cleanup before that claim exists.
func (o *trinoPoolOperator) recoverInstance(ctx context.Context, i configstore.TrinoPoolInstance, r configstore.TrinoPoolRecovery) (bool, error) {
	if !r.DestructiveAuthorization || r.OperationID == "" || r.Reason == "" || r.RequestedBy == "" ||
		r.PoolID != i.PoolID || r.InstanceID != i.InstanceID || r.Incarnation != i.GatewayIncarnation ||
		r.PodUID != i.CoordinatorPodUID || r.BootID != i.CoordinatorBootID || r.NodeID != i.CoordinatorNodeID || r.CoordinatorID != i.CoordinatorID {
		return false, errors.New("recovery authorization does not match the recorded instance")
	}
	phase := trinopool.Phase(i.Phase)
	current, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, i.InstanceID)
	if err != nil {
		return false, err
	}
	// A seal can win the generation CAS while the admin request is accepted.
	// Preserve that clean drain and let its existing journal finish retirement.
	clean := current.Phase == "SEALED" || ((current.Phase == "RETIRING" || current.Phase == "RETIRED") && current.RetirementKind == "DRAINED")
	if recoveryMemberMatches(current, r) && clean && (phase == trinopool.PhaseDraining || phase == trinopool.PhaseSealed || phase == trinopool.PhaseRetiring) {
		if err := o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, i.InstanceID, map[string]any{"last_error": ""}); err != nil {
			return true, o.dropAuthority(err)
		}
		return o.progressInstance(ctx, i)
	}
	if phase != trinopool.PhaseDraining && phase != trinopool.PhaseSuspect && phase != trinopool.PhaseLost {
		return false, fmt.Errorf("recovery cannot advance local phase %s", phase)
	}
	if !recoveryInventoryComplete(i) {
		return false, errors.New("recovery requires complete owned-resource identities")
	}
	offsets := map[string]int64{"DRAINING": 0, "SUSPECT": 1, "LOST": 2, "RETIRING": 3, "RETIRED": 4}
	offset, ok := offsets[current.Phase]
	if !ok || !recoveryMemberMatches(current, r) || current.Generation != r.ExpectedGeneration+offset {
		return false, errors.New("gateway member changed since recovery authorization")
	}
	if offset < 3 {
		if err := o.checkRecoveryPreconditions(ctx, i, r, current); err != nil {
			return false, err
		}
	}
	// Identical requests resolve unknown outcomes from the Gateway's durable
	// step journal. Its payload hash excludes only the leadership envelope.
	member, err := o.gateway.SuspectMember(ctx, o.config.RoutingGroup, i.InstanceID, trinogateway.SuspectMemberRequest{
		Step: o.recoveryStep(r, "suspect"), ExpectedGeneration: r.ExpectedGeneration, Reason: r.Reason,
	})
	if err != nil {
		return true, o.dropAuthority(err)
	}
	if !recoveryMemberMatches(member, r) || member.Phase != "SUSPECT" || member.Generation != r.ExpectedGeneration+1 {
		return true, errors.New("invalid recovery suspicion receipt")
	}
	member, err = o.gateway.LostMember(ctx, o.config.RoutingGroup, i.InstanceID, trinogateway.LostMemberRequest{
		Step: o.recoveryStep(r, "lost"), ExpectedGeneration: r.ExpectedGeneration + 1,
		Evidence: trinogateway.EvidenceDestructiveOverride, DestructiveAuthorization: true, Reason: r.Reason,
		Termination: trinogateway.TerminationProof{PodUID: r.PodUID, BootID: r.BootID, NodeID: r.NodeID, CoordinatorID: r.CoordinatorID, Source: "administrator-override"},
	})
	if err != nil {
		return true, o.dropAuthority(err)
	}
	if !recoveryMemberMatches(member, r) || member.Phase != "LOST" || member.Generation != r.ExpectedGeneration+2 {
		return true, errors.New("invalid recovery loss receipt")
	}
	member, err = o.gateway.RetireMember(ctx, o.config.RoutingGroup, i.InstanceID, trinogateway.MemberStepRequest{
		Step: o.recoveryStep(r, "retire"), ExpectedGeneration: r.ExpectedGeneration + 2,
	})
	if err != nil {
		return true, o.dropAuthority(err)
	}
	if !recoveryMemberMatches(member, r) || member.Phase != "RETIRING" || member.Generation != r.ExpectedGeneration+3 || member.RetirementKind != "FAILED" {
		return true, errors.New("invalid recovery retirement receipt")
	}
	receipt, err := marshalRetirementReceipt(member)
	if err != nil {
		return true, err
	}
	updates := map[string]any{"gateway_state": member.Phase, "gateway_generation": member.Generation, "retirement_receipt": receipt, "failure_reason": r.Reason, "last_error": ""}
	switch phase {
	case trinopool.PhaseDraining:
		err := o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, i.InstanceID, phase, trinopool.PhaseSuspect, updates))
		if err == nil {
			slog.Info("Trino pool administrative recovery retirement claimed.", "pool", o.config.PublicID, "instance", i.InstanceID, "operation", r.OperationID)
		}
		return true, err
	case trinopool.PhaseSuspect:
		return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, i.InstanceID, phase, trinopool.PhaseLost, updates))
	default:
		// Check the current durable owner before entering Kubernetes teardown.
		if err := o.store.RecordTrinoPoolInstanceFields(ctx, o.lease, i.InstanceID, updates); err != nil {
			return true, o.dropAuthority(err)
		}
		return o.finishRecovery(ctx, i, r)
	}
}

func recoveryMemberMatches(m trinogateway.Member, r configstore.TrinoPoolRecovery) bool {
	return m.InstanceID == r.InstanceID && m.Incarnation == r.Incarnation && m.PodUID == r.PodUID && m.BootID == r.BootID && m.NodeID == r.NodeID && m.CoordinatorID == r.CoordinatorID
}

func recoveryInventoryComplete(i configstore.TrinoPoolInstance) bool {
	return instanceNamespace(i) != "" && i.CoordinatorContainerID != "" &&
		i.CoordinatorDeploymentName != "" && i.CoordinatorDeploymentUID != "" &&
		i.WorkerDeploymentName != "" && i.WorkerDeploymentUID != "" &&
		i.ServiceName != "" && i.ServiceUID != "" &&
		i.ConfigMapName != "" && i.ConfigMapUID != "" &&
		(i.WorkerConfigMapName == "") == (i.WorkerConfigMapUID == "")
}

func (o *trinoPoolOperator) recoveryStep(r configstore.TrinoPoolRecovery, step string) trinogateway.Step {
	return trinogateway.Step{OperationID: "admin-recovery:" + r.OperationID, StepID: "recovery-" + step, ControllerEpoch: o.lease.Epoch, OwnerIdentity: o.owner}
}

func (o *trinoPoolOperator) checkRecoveryPreconditions(ctx context.Context, i configstore.TrinoPoolInstance, r configstore.TrinoPoolRecovery, m trinogateway.Member) error {
	pool, err := o.gateway.GetPool(ctx, o.config.RoutingGroup)
	if err != nil {
		return err
	}
	floor := max(pool.MinServing, o.config.Spec.MinServing)
	if pool.ServingMembers < int64(floor) {
		return errors.New("recovery would proceed below the serving floor")
	}
	obligations, err := o.gateway.GetObligations(ctx, o.config.RoutingGroup, i.InstanceID)
	if err != nil {
		return err
	}
	if obligations.InstanceID != i.InstanceID || obligations.Incarnation != r.Incarnation || obligations.Generation != m.Generation {
		return errors.New("recovery requires obligations for the authorized incarnation and current generation")
	}
	kube := o.kube(o.lease.Epoch)
	absent, err := kube.CoordinatorPodAbsent(ctx, inventoryOf(i), r.PodUID)
	if err != nil {
		return err
	}
	if absent {
		// Keep the immutable override payload for every replay, regardless of the current evidence.
		// Failed retirement retains obligations that the absent process can no longer complete.
		slog.Info("Trino pool recovery verified the admitted coordinator pod is absent.",
			"pool", o.config.PublicID, "instance", i.InstanceID, "operation", r.OperationID)
		return nil
	}
	if obligations.PendingRequests != 0 || obligations.OpenTransactions != 0 {
		return errors.New("recovery requires current obligations without pending requests or open transactions")
	}
	// Once our suspicion step commits, a process exit must not strand recovery.
	// Replaying that exact step below proves this request owns the transition;
	// deletion remains scoped to the instance's original resource UIDs.
	if m.Phase != "DRAINING" {
		return nil
	}
	observed, err := kube.Observe(ctx, inventoryOf(i))
	if err != nil {
		return err
	}
	if len(observed.CoordinatorPods) != 1 || observed.CoordinatorPods[0].UID != r.PodUID || observed.CoordinatorPods[0].Terminating || observed.CoordinatorPods[0].RunningContainerID != i.CoordinatorContainerID {
		return errors.New("recovery coordinator pod no longer matches authorization")
	}
	if o.identity == nil {
		return errors.New("recovery requires a coordinator identity probe")
	}
	boot, err := o.identity(ctx, i.EndpointURL)
	if err != nil {
		return err
	}
	if boot != r.BootID {
		return errors.New("recovery coordinator process changed")
	}
	return nil
}

func (o *trinoPoolOperator) finishRecovery(ctx context.Context, i configstore.TrinoPoolInstance, r configstore.TrinoPoolRecovery) (bool, error) {
	inventory := inventoryOf(i)
	kube := o.kube(o.lease.Epoch)
	if err := kube.Delete(ctx, inventory); err != nil {
		return true, err
	}
	absent, err := kube.ResourcesAbsent(ctx, inventory)
	if err != nil || !absent {
		return false, err
	}
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, i.InstanceID)
	if err != nil {
		return true, err
	}
	// A previous leader may have completed the same failed retirement through
	// its ordinary LOST cleanup before recording the local checkpoint.
	if member.Phase != "RETIRED" {
		member, err = o.gateway.MemberRetired(ctx, o.config.RoutingGroup, i.InstanceID, trinogateway.MemberStepRequest{
			Step: o.recoveryStep(r, "retired"), ExpectedGeneration: r.ExpectedGeneration + 3, ResourcesAbsent: true,
		})
	}
	if err != nil {
		return true, o.dropAuthority(err)
	}
	if !recoveryMemberMatches(member, r) || member.Phase != "RETIRED" || member.Generation != r.ExpectedGeneration+4 || member.RetirementKind != "FAILED" {
		return true, errors.New("invalid completed recovery receipt")
	}
	err = o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, i.InstanceID, trinopool.PhaseLost, trinopool.PhaseFailureRetired, map[string]any{
		"gateway_state": member.Phase, "gateway_generation": member.Generation, "last_error": "",
	}))
	if err == nil {
		slog.Info("Trino pool administrative recovery completed.", "pool", o.config.PublicID, "instance", i.InstanceID, "operation", r.OperationID)
	}
	return true, err
}
