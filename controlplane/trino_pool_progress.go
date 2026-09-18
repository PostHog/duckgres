//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// One instance, one step per tick.
//
// Every step is: do the external effect, then record what came back. When the
// response is lost the next tick re-reads instead of re-doing, which is why
// each effect is either idempotent by construction (deterministic Kubernetes
// names) or idempotent by identity (the Gateway's operation/step guard).

// progressInstances advances the first instance that has work to do and reports
// whether it did anything. Doing one at a time keeps a burst of instances from
// issuing a burst of external effects under one lease.
func (o *trinoPoolOperator) progressInstances(ctx context.Context, instances []configstore.TrinoPoolInstance) (bool, error) {
	for _, instance := range instances {
		phase := trinopool.Phase(instance.Phase)
		if phase.Terminal() || phase == trinopool.PhaseServing {
			continue
		}
		progressed, err := o.progressInstance(ctx, instance)
		if err != nil {
			return true, err
		}
		if progressed {
			return true, nil
		}
	}
	return false, nil
}

func (o *trinoPoolOperator) progressInstance(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	switch trinopool.Phase(instance.Phase) {
	case trinopool.PhasePending:
		return true, o.createResources(ctx, instance)
	case trinopool.PhaseCreating:
		return o.registerWhenReady(ctx, instance)
	case trinopool.PhasePreparing:
		return o.validateCandidate(ctx, instance)
	case trinopool.PhaseValidating:
		return true, o.admitCandidate(ctx, instance)
	case trinopool.PhaseAdmitted:
		return o.markServing(ctx, instance)
	case trinopool.PhaseDraining:
		return o.sealWhenDrained(ctx, instance)
	case trinopool.PhaseSealed:
		return true, o.claimRetirement(ctx, instance)
	case trinopool.PhaseRetiring:
		return o.deleteResources(ctx, instance)
	default:
		return false, nil
	}
}

// createResources instantiates the instance's OWN blueprint snapshot, not the
// pool's current one: a release that landed after this instance was recorded
// must not change what it runs.
func (o *trinoPoolOperator) createResources(ctx context.Context, instance configstore.TrinoPoolInstance) error {
	blueprint, err := trinopool.ParseBlueprint([]byte(instance.BlueprintSnapshot))
	if err != nil {
		return fmt.Errorf("instance %s has an unreadable blueprint snapshot: %w", instance.InstanceID, err)
	}
	objects, err := blueprint.Instantiate(o.identityFor(instance.InstanceID))
	if err != nil {
		return fmt.Errorf("instantiate %s: %w", instance.InstanceID, err)
	}
	inventory, err := o.kube(o.lease.Epoch).Apply(ctx, objects)
	if err != nil {
		return fmt.Errorf("create resources for %s: %w", instance.InstanceID, err)
	}
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhasePending, trinopool.PhaseCreating, inventoryUpdates(inventory)))
}

// registerWhenReady waits for the pods, then registers an unroutable PREPARING
// member. The Gateway backend record is created first and INACTIVE: pooled
// registration reads the endpoint from it, and activating it through the legacy
// route would make the backend eligible with no certificate at all.
func (o *trinoPoolOperator) registerWhenReady(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	inventory := inventoryOf(instance)
	observed, err := o.kube(o.lease.Epoch).Observe(ctx, inventory)
	if err != nil {
		return false, fmt.Errorf("observe %s: %w", instance.InstanceID, err)
	}
	if !observed.CoordinatorReady || observed.ReadyWorkers == 0 || observed.ReadyWorkers != observed.DesiredWorkers {
		// Still converging. Not an error, and not something to time out into a
		// failure: the plan's budgets already bound how many instances exist.
		return false, nil
	}

	if err := o.gateway.EnsureInactiveBackend(ctx, trinogateway.Backend{
		Name:         o.backendName(instance.InstanceID),
		ProxyTo:      o.endpointFor(instance.InstanceID),
		RoutingGroup: o.config.RoutingGroup,
		Active:       false,
	}); err != nil {
		return true, fmt.Errorf("register gateway backend for %s: %w", instance.InstanceID, err)
	}

	member, err := o.gateway.RegisterMember(ctx, o.config.RoutingGroup, trinogateway.RegisterMemberRequest{
		Step:           o.step(instance.InstanceID, "register"),
		InstanceID:     instance.InstanceID,
		BackendName:    o.backendName(instance.InstanceID),
		URL:            o.endpointFor(instance.InstanceID),
		PodUID:         observed.CoordinatorPodUID,
		BootID:         observed.CoordinatorPodUID,
		ConfigRevision: instance.ReleaseID,
		RepairFor:      repairTarget(instance),
	})
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("register member %s: %w", instance.InstanceID, err))
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseCreating, trinopool.PhasePreparing, map[string]any{
			"coordinator_pod_uid":  observed.CoordinatorPodUID,
			"gateway_incarnation":  member.Incarnation,
			"gateway_backend_name": member.BackendName,
			"gateway_state":        member.Phase,
			"gateway_generation":   member.Generation,
		}))
}

func (o *trinoPoolOperator) validateCandidate(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	observed, err := o.kube(o.lease.Epoch).Observe(ctx, inventoryOf(instance))
	if err != nil {
		return false, fmt.Errorf("observe %s: %w", instance.InstanceID, err)
	}
	validation, err := o.validate(ctx, instance.EndpointURL, observed.ReadyWorkers)
	if err != nil {
		// A candidate that is not ready yet stays PREPARING and is probed again.
		// It holds a live slot, which the surge budget already accounts for.
		slog.Info("Trino pool candidate is not ready yet.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", err)
		return false, nil
	}
	receipt, err := marshalValidationReceipt(validation)
	if err != nil {
		return true, err
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhasePreparing, trinopool.PhaseValidating, map[string]any{
			"coordinator_node_id":      validation.NodeID,
			"coordinator_boot_id":      validation.ProcessID,
			"applied_catalog_revision": validation.AppliedRevision,
			"validation_receipt":       receipt,
			"validated_at":             nowUTC(),
		}))
}

// admitCandidate is the single certified-activation call. The Gateway enforces
// the budgets, the certificate freshness and any open publication barrier, and
// independently verifies the live process identity.
func (o *trinoPoolOperator) admitCandidate(ctx context.Context, instance configstore.TrinoPoolInstance) error {
	validation, err := unmarshalValidationReceipt(instance.ValidationReceipt)
	if err != nil {
		return fmt.Errorf("instance %s has an unreadable validation receipt: %w", instance.InstanceID, err)
	}
	member, err := o.gateway.AdmitMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.AdmitMemberRequest{
		Step:               o.step(instance.InstanceID, "admit"),
		ExpectedGeneration: instance.GatewayGeneration,
		Receipt: trinogateway.ValidationReceipt{
			CertificateHash: validation.CertificateHash,
			ConfigRevision:  instance.ReleaseID,
			AuthRevision:    validation.AuthRevision,
			PodUID:          instance.CoordinatorPodUID,
			BootID:          validation.ProcessID,
			NodeID:          validation.NodeID,
			CoordinatorID:   validation.CoordinatorID,
			ReadyWorkers:    validation.ReadyWorkers,
			Checks:          validation.Checks,
		},
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("admit member %s: %w", instance.InstanceID, err))
	}
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseValidating, trinopool.PhaseAdmitted, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
		}))
}

// markServing records that the Gateway considers the member eligible. Serving
// is the Gateway's judgement, not ours: it is what the minimum-serving floor
// counts.
func (o *trinoPoolOperator) markServing(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		return false, fmt.Errorf("read member %s: %w", instance.InstanceID, err)
	}
	if member.Phase != "ACTIVE" || !member.Eligible {
		return false, nil
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseAdmitted, trinopool.PhaseServing, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
		}))
}

// sealWhenDrained asks the Gateway what is still pinned to the member. There is
// no drain deadline: a timer that sealed a member with open transactions would
// be a decision to lose them.
func (o *trinoPoolOperator) sealWhenDrained(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	obligations, err := o.gateway.GetObligations(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		return false, fmt.Errorf("read obligations for %s: %w", instance.InstanceID, err)
	}
	if !obligations.Drained || obligations.Outstanding() > 0 {
		slog.Debug("Trino pool instance is still draining.",
			"pool", o.config.PublicID, "instance", instance.InstanceID,
			"outstanding", obligations.Outstanding())
		return false, nil
	}
	member, err := o.gateway.SealMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step:               o.step(instance.InstanceID, "seal"),
		ExpectedGeneration: obligations.Generation,
	})
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("seal member %s: %w", instance.InstanceID, err))
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseDraining, trinopool.PhaseSealed, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
		}))
}

// claimRetirement takes the irreversible retirement claim. Nothing is deleted
// before this returns: the claim is the only thing that authorizes it.
func (o *trinoPoolOperator) claimRetirement(ctx context.Context, instance configstore.TrinoPoolInstance) error {
	member, err := o.gateway.RetireMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step:               o.step(instance.InstanceID, "retire"),
		ExpectedGeneration: instance.GatewayGeneration,
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("retire member %s: %w", instance.InstanceID, err))
	}
	receipt, err := marshalRetirementReceipt(member)
	if err != nil {
		return err
	}
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseSealed, trinopool.PhaseRetiring, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
			"retirement_receipt": receipt,
		}))
}

// deleteResources removes the recorded objects and completes retirement only
// once they are verifiably absent - terminating pods included.
func (o *trinoPoolOperator) deleteResources(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	inventory := inventoryOf(instance)
	kube := o.kube(o.lease.Epoch)
	if err := kube.Delete(ctx, inventory); err != nil {
		return true, fmt.Errorf("delete resources for %s: %w", instance.InstanceID, err)
	}
	absent, err := kube.ResourcesAbsent(ctx, inventory)
	if err != nil {
		return true, fmt.Errorf("verify absence for %s: %w", instance.InstanceID, err)
	}
	if !absent {
		// Deletion is in progress. The instance stays RETIRING and keeps its
		// slot until absence is observed, so a terminating pod is never counted
		// as freed capacity.
		return false, nil
	}
	if _, err := o.gateway.MemberRetired(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step:               o.step(instance.InstanceID, "retired"),
		ExpectedGeneration: instance.GatewayGeneration,
		ResourcesAbsent:    true,
	}); err != nil {
		return true, o.dropAuthority(fmt.Errorf("report retirement of %s: %w", instance.InstanceID, err))
	}
	slog.Info("Trino pool instance retired.", "pool", o.config.PublicID, "instance", instance.InstanceID)
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseRetiring, trinopool.PhaseRetired, nil))
}

func repairTarget(instance configstore.TrinoPoolInstance) string {
	if !instance.Repair {
		return ""
	}
	// The Gateway charges the activation to the repair budget when it names the
	// instance being replaced. Naming this instance itself is wrong, but the
	// durable model does not record the failed peer yet; until it does, the
	// repair flag only affects duckgres-side accounting.
	return ""
}

func inventoryOf(instance configstore.TrinoPoolInstance) trinoPoolInventory {
	return trinoPoolInventory{
		Namespace:                 instanceNamespace(instance),
		ConfigMapName:             instance.ConfigMapName,
		ConfigMapUID:              instance.ConfigMapUID,
		ServiceName:               instance.ServiceName,
		ServiceUID:                instance.ServiceUID,
		CoordinatorDeploymentName: instance.CoordinatorDeploymentName,
		CoordinatorDeploymentUID:  instance.CoordinatorDeploymentUID,
		WorkerDeploymentName:      instance.WorkerDeploymentName,
		WorkerDeploymentUID:       instance.WorkerDeploymentUID,
	}
}

func inventoryUpdates(inventory trinoPoolInventory) map[string]any {
	return map[string]any{
		"config_map_name":             inventory.ConfigMapName,
		"config_map_uid":              inventory.ConfigMapUID,
		"service_name":                inventory.ServiceName,
		"service_uid":                 inventory.ServiceUID,
		"coordinator_deployment_name": inventory.CoordinatorDeploymentName,
		"coordinator_deployment_uid":  inventory.CoordinatorDeploymentUID,
		"worker_deployment_name":      inventory.WorkerDeploymentName,
		"worker_deployment_uid":       inventory.WorkerDeploymentUID,
	}
}
