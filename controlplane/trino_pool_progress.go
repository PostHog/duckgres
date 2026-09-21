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

// One instance, one step per tick.
//
// Every step is: do the external effect, then record what came back. When the
// response is lost the next tick re-reads instead of re-doing, which is why
// each effect is either idempotent by construction (deterministic Kubernetes
// names) or idempotent by identity (the Gateway's operation/step guard).

// progressInstances advances the first instance that has work to do and reports
// whether it did anything. Doing one at a time keeps a burst of instances from
// issuing a burst of external effects under one lease.
//
// One instance is NOT allowed to stop the pool. Returning on the first error
// ended the tick before the planner ran, so a member that could not make
// progress - a Gateway decision no retry can change, an object nobody can
// delete - held every repair, drain and replacement for as long as it stayed
// broken. A failing instance is recorded and skipped; the errors are reported
// together, and only a lost fence stops the sweep, because after that nothing
// this process writes can land anyway. This is the isolation the tenant loop
// already applies.
func (o *trinoPoolOperator) progressInstances(ctx context.Context, instances []configstore.TrinoPoolInstance) (bool, error) {
	var failures []error
	for _, instance := range instances {
		phase := trinopool.Phase(instance.Phase)
		if phase.Terminal() {
			continue
		}
		progressed, err := o.progressInstance(ctx, instance)
		if err != nil {
			failures = append(failures, fmt.Errorf("instance %s: %w", instance.InstanceID, err))
			if o.fenced {
				return true, errors.Join(failures...)
			}
			slog.Warn("Trino pool instance step failed; continuing with the rest of the pool.",
				"pool", o.config.PublicID, "instance", instance.InstanceID,
				"phase", instance.Phase, "error", err)
			continue
		}
		if progressed {
			return true, errors.Join(failures...)
		}
	}
	return false, errors.Join(failures...)
}

func (o *trinoPoolOperator) progressInstance(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	switch trinopool.Phase(instance.Phase) {
	case trinopool.PhasePending:
		return true, o.createResources(ctx, instance)
	case trinopool.PhaseCreating:
		return o.registerWhenReady(ctx, instance)
	case trinopool.PhasePreparing:
		if o.pool == nil || o.pool.DesiredReleaseID == "" || o.pool.DesiredBlueprintDigest == "" {
			return false, errors.New("candidate supersession requires a complete desired pool specification")
		}
		blueprint, err := trinopool.ParseBlueprint([]byte(instance.BlueprintSnapshot))
		if err != nil {
			return false, fmt.Errorf("read candidate blueprint for supersession: %w", err)
		}
		// Configuration changes cannot repair an immutable candidate snapshot.
		// Only PREPARING is safe here: VALIDATING can have an unknown admission outcome.
		// Cleanup still requires the Gateway's guarded retirement claim before deletion.
		if instance.ReleaseID != o.pool.DesiredReleaseID || blueprint.Digest() != o.pool.DesiredBlueprintDigest {
			return true, o.failCandidate(ctx, instance, trinopool.PhasePreparing,
				"candidate blueprint was superseded before admission")
		}
		return o.validateCandidate(ctx, instance)
	case trinopool.PhaseValidating:
		return true, o.admitCandidate(ctx, instance)
	case trinopool.PhaseAdmitted:
		if progressed, err := o.markServing(ctx, instance); progressed || err != nil {
			return progressed, err
		}
		return o.observeHealth(ctx, instance)
	case trinopool.PhaseServing, trinopool.PhaseSuspect:
		return o.observeHealth(ctx, instance)
	case trinopool.PhaseLost:
		return o.completeFailureRetirement(ctx, instance)
	case trinopool.PhaseFailedPreparing:
		return o.cleanupFailedCandidate(ctx, instance)
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
	// A lost registration response is resolved by reading the member back, not
	// by building the request again. The request carries the coordinator's
	// observed pod and boot identity, and the Gateway hashes the whole request
	// under the step identity: once the coordinator has restarted in place, a
	// rebuilt request carries a different boot id, reports a changed intent,
	// and this instance can never leave CREATING while its member holds a live
	// slot. What the Gateway recorded is also the only identity a later receipt
	// or loss claim may present, so it is adopted verbatim.
	if adopted, err := o.adoptRegisteredMember(ctx, instance, observed); adopted || err != nil {
		return adopted, err
	}
	if !observed.CoordinatorReady || observed.ReadyWorkers == 0 || observed.ReadyWorkers != observed.DesiredWorkers {
		// Still converging. Not an error, and not something to time out into a
		// failure: the plan's budgets already bound how many instances exist.
		return false, nil
	}

	// The coordinator's process identity is read BEFORE registration, because
	// the Gateway binds (podUid, bootId) at registration and later requires the
	// admission receipt to carry the identical pair. Registering the pod UID as
	// the boot id and admitting with the coordinator's processId made every
	// admission fail POOL_NOT_CERTIFIED. There is exactly one authoritative boot
	// identity: the processId, which changes on every JVM start.
	bootID, err := o.identity(ctx, instance.EndpointURL)
	if err != nil {
		slog.Info("Trino pool candidate has no readable process identity yet.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", err)
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
		BootID:         bootID,
		ConfigRevision: instance.ReleaseID,
		RepairFor:      instance.RepairFor,
	})
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("register member %s: %w", instance.InstanceID, err))
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseCreating, trinopool.PhasePreparing, map[string]any{
			"coordinator_pod_uid": observed.CoordinatorPodUID,
			"coordinator_boot_id": bootID,
			// The container instance the admitted process runs in. A termination
			// record names a container instance, so without this there is nothing
			// to correlate one with, and an unrelated restart from before
			// admission reads exactly like the death of this process.
			"coordinator_container_id": runningCoordinatorContainer(observed, observed.CoordinatorPodUID),
			// The Gateway observes the coordinator's node and coordinator ids
			// itself at registration and binds the member to them. Recording
			// what it returned is the only way a later loss claim can present
			// the identical pair; deriving them again would risk a value the
			// Gateway never recorded, and the claim would be refused.
			"coordinator_node_id":  member.NodeID,
			"coordinator_id":       member.CoordinatorID,
			"gateway_incarnation":  member.Incarnation,
			"gateway_backend_name": member.BackendName,
			"gateway_state":        member.Phase,
			"gateway_generation":   member.Generation,
		}))
}

// adoptRegisteredMember resolves a registration whose response was lost.
//
// The Gateway is authoritative for its own member, so the identity it recorded
// is adopted rather than re-derived - including a boot identity the coordinator
// has already replaced. That is the correct outcome, not a workaround: the
// candidate then fails its validation against the live process and is replaced
// through the path that exists for exactly that, which also releases the live
// slot the member is holding.
func (o *trinoPoolOperator) adoptRegisteredMember(
	ctx context.Context,
	instance configstore.TrinoPoolInstance,
	observed trinoPoolObservation,
) (bool, error) {
	member, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
	if err != nil {
		if errors.Is(err, trinogateway.ErrNotFound) {
			// Nothing was registered under this identity, so registration has
			// not happened yet and proceeds normally.
			return false, nil
		}
		return true, fmt.Errorf("read back member %s: %w", instance.InstanceID, err)
	}
	if member.InstanceID != instance.InstanceID {
		// Instance identities are never reused, so this cannot happen; adopting
		// another member's incarnation would bind this row to a process it was
		// never registered for, so it is refused rather than assumed.
		return true, fmt.Errorf("read back member %s returned instance %q", instance.InstanceID, member.InstanceID)
	}
	slog.Info("Trino pool adopted a member whose registration response was lost.",
		"pool", o.config.PublicID, "instance", instance.InstanceID, "phase", member.Phase)
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseCreating, trinopool.PhasePreparing, map[string]any{
			"coordinator_pod_uid": member.PodUID,
			"coordinator_boot_id": member.BootID,
			// Only when the pod the Gateway recorded is still the one running:
			// a container id from a different pod would correlate a later
			// termination record with the wrong process.
			"coordinator_container_id": runningCoordinatorContainer(observed, member.PodUID),
			"coordinator_node_id":      member.NodeID,
			"coordinator_id":           member.CoordinatorID,
			"gateway_incarnation":      member.Incarnation,
			"gateway_backend_name":     member.BackendName,
			"gateway_state":            member.Phase,
			"gateway_generation":       member.Generation,
		}))
}

// runningCoordinatorContainer is the container instance currently running in the
// named pod, or "" when that pod is not among the observed ones. Empty means the
// termination evidence below has nothing to correlate against and stays unavailable.
func runningCoordinatorContainer(observed trinoPoolObservation, podUID string) string {
	if podUID == "" {
		return ""
	}
	for _, pod := range observed.CoordinatorPods {
		if pod.UID == podUID {
			return pod.RunningContainerID
		}
	}
	return ""
}

func (o *trinoPoolOperator) validateCandidate(ctx context.Context, instance configstore.TrinoPoolInstance) (bool, error) {
	observed, err := o.kube(o.lease.Epoch).Observe(ctx, inventoryOf(instance))
	if err != nil {
		return false, fmt.Errorf("observe %s: %w", instance.InstanceID, err)
	}
	validation, err := o.validate(ctx, instance.EndpointURL, observed, o.expectationFor(instance))
	if err != nil {
		// A candidate that is not ready yet stays PREPARING and is probed again.
		// It holds a live slot, which the surge budget already accounts for.
		slog.Info("Trino pool candidate is not ready yet.",
			"pool", o.config.PublicID, "instance", instance.InstanceID, "reason", err)
		return false, nil
	}
	// The registered boot identity is what the Gateway will compare the receipt
	// against. If the coordinator restarted since registration, this member's
	// incarnation is gone: admitting it is impossible, and waiting for it is
	// pointless, so the candidate fails and a fresh instance replaces it.
	if instance.CoordinatorBootID != "" && validation.ProcessID != instance.CoordinatorBootID {
		slog.Warn("Trino pool candidate restarted before admission; failing it.",
			"pool", o.config.PublicID, "instance", instance.InstanceID,
			"registered", instance.CoordinatorBootID, "observed", validation.ProcessID)
		return true, o.failCandidate(ctx, instance, trinopool.PhasePreparing,
			"the coordinator process restarted before admission")
	}
	receipt, err := marshalValidationReceipt(validation)
	if err != nil {
		return true, err
	}
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhasePreparing, trinopool.PhaseValidating, map[string]any{
			"coordinator_node_id":      validation.NodeID,
			"coordinator_id":           validation.CoordinatorID,
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
	request := trinogateway.AdmitMemberRequest{
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
	}

	// Admission is the one step whose lost response is genuinely ambiguous: the
	// member may already be ACTIVE. Recording the intent first means the next
	// attempt - possibly a different leader - reads the outcome back instead of
	// deciding from nothing.
	var member trinogateway.Member
	if err := o.runDurableStep(ctx,
		configstore.TrinoPoolOperationSpec{
			OperationID: "instance:" + instance.InstanceID,
			PoolID:      o.config.PoolID,
			InstanceID:  instance.InstanceID,
			Kind:        configstore.TrinoPoolOperationReplace,
			IntentHash:  instance.SpecDigest,
		},
		// The step identity is the BUSINESS INTENT - this instance, this
		// validated process, this revision - and deliberately NOT the authority
		// envelope. The expected generation moves whenever the effect actually
		// lands, so hashing the whole request would turn the retry after a lost
		// response into a permanent "changed intent" conflict, which is exactly
		// the case the record exists to resolve.
		"admit", trinoPoolAdmitIntent{
			InstanceID:      instance.InstanceID,
			CertificateHash: validation.CertificateHash,
			ConfigRevision:  instance.ReleaseID,
			BootID:          validation.ProcessID,
		},
		func(ctx context.Context) (string, error) {
			admitted, err := o.gateway.AdmitMember(ctx, o.config.RoutingGroup, instance.InstanceID, request)
			if err != nil {
				return "", err
			}
			member = admitted
			return fmt.Sprintf(`{"phase":%q,"generation":%d}`, admitted.Phase, admitted.Generation), nil
		},
	); err != nil {
		if errors.Is(err, trinogateway.ErrPublicationBarrier) {
			// A member joining while a tenant publication is open must
			// acknowledge that publication's target revision, which a member
			// registered under a release id cannot. The barrier is the thing
			// that has to give way: it can be reopened against the membership
			// that includes this member, whereas a candidate refused here would
			// wait for a barrier that is itself waiting for capacity.
			o.retireOpenBarrierForAdmission(ctx, instance.InstanceID, err)
		}
		return o.dropAuthority(fmt.Errorf("admit member %s: %w", instance.InstanceID, err))
	}
	if member.Phase == "" {
		// The step was already recorded as complete by an earlier attempt. Read
		// the member back rather than trusting the recorded snapshot: the
		// Gateway is authoritative for its own state.
		current, err := o.gateway.GetMember(ctx, o.config.RoutingGroup, instance.InstanceID)
		if err != nil {
			return fmt.Errorf("read back admitted member %s: %w", instance.InstanceID, err)
		}
		member = current
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
	// ReadyToSeal permits the first attempt. Drained permits replay after a lost seal response.
	// Neither observation permits deletion; the Gateway must still grant sealing and retirement.
	if (!obligations.ReadyToSeal && !obligations.Drained) || obligations.Outstanding() > 0 {
		slog.Debug("Trino pool instance is still draining.",
			"pool", o.config.PublicID, "instance", instance.InstanceID,
			"outstanding", obligations.Outstanding())
		return false, nil
	}
	member, err := o.gateway.SealMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step: o.step(instance.InstanceID, "seal"),
		// The generation comes from the RECORD, not from the obligations read
		// above. The Gateway hashes the whole request under the step identity,
		// so a seal whose response was lost can only be resolved by repeating
		// the identical request - and the obligations of a member that has
		// already been sealed report the generation that seal produced. Sending
		// that back would be a changed intent, and this member could never
		// finish draining.
		ExpectedGeneration: instance.GatewayGeneration,
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
	o.closeInstanceOperation(ctx, instance.InstanceID, "retired", "")
	return true, o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseRetiring, trinopool.PhaseRetired, nil))
}

func inventoryOf(instance configstore.TrinoPoolInstance) trinoPoolInventory {
	return trinoPoolInventory{
		Namespace:                 instanceNamespace(instance),
		ConfigMapName:             instance.ConfigMapName,
		ConfigMapUID:              instance.ConfigMapUID,
		WorkerConfigMapName:       instance.WorkerConfigMapName,
		WorkerConfigMapUID:        instance.WorkerConfigMapUID,
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
		"worker_config_map_name":      inventory.WorkerConfigMapName,
		"worker_config_map_uid":       inventory.WorkerConfigMapUID,
		"service_name":                inventory.ServiceName,
		"service_uid":                 inventory.ServiceUID,
		"coordinator_deployment_name": inventory.CoordinatorDeploymentName,
		"coordinator_deployment_uid":  inventory.CoordinatorDeploymentUID,
		"worker_deployment_name":      inventory.WorkerDeploymentName,
		"worker_deployment_uid":       inventory.WorkerDeploymentUID,
	}
}

// trinoPoolAdmitIntent is what an admission MEANS, separate from the authority
// envelope that carries it.
type trinoPoolAdmitIntent struct {
	InstanceID      string
	CertificateHash string
	ConfigRevision  string
	BootID          string
}

// expectationFor is what this instance must prove before it can be admitted.
//
// The catalog revision comes from the pool's DURABLE publication revision, not
// from a constant: a structurally healthy coordinator sitting at an older
// revision is not certified for the current pool, because it would serve a
// catalog set that does not yet include the newest tenant. Bootstrap is the
// natural zero — before anything is published there is nothing to be behind.
func (o *trinoPoolOperator) expectationFor(instance configstore.TrinoPoolInstance) trinoPoolExpectation {
	expectation := trinoPoolExpectation{InternalHTTP: true}
	if o.pool != nil {
		expectation.CatalogRevision = o.pool.PublicationRevision
	}
	// What the pool's DURABLE record accepts, not what this process last
	// projected: a replica's own memory says what it published, which is
	// exactly the thing in question when replicas disagree.
	if o.acceptedProjection != nil {
		expectation.ProjectionDigest = o.acceptedProjection()
	}
	// The image comes from the instance's OWN snapshot, so a release that
	// landed after this instance was created cannot retroactively change what
	// it is required to be running.
	if blueprint, err := trinopool.ParseBlueprint([]byte(instance.BlueprintSnapshot)); err == nil {
		expectation.Image = blueprint.Image
	}
	return expectation
}

// failCandidate records a candidate that can never be admitted.
//
// FAILED_PREPARING retains its capacity until cleanup verifies resource absence.
// Cleanup requires the Gateway's retirement claim for any registered member.
func (o *trinoPoolOperator) failCandidate(ctx context.Context, instance configstore.TrinoPoolInstance, from trinopool.Phase, reason string) error {
	o.closeInstanceOperation(ctx, instance.InstanceID, "failed", reason)
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		from, trinopool.PhaseFailedPreparing, map[string]any{
			"failure_reason": reason,
			"last_error":     reason,
		}))
}

// closeInstanceOperation marks an instance's durable operation terminal.
//
// An operation that is never closed leaves `terminal_at` NULL forever: the
// table only grows, and nothing can tell work in flight from work whose
// instance has already reached the end of its life. It is best-effort - the
// instance's own phase is the authoritative record - so a failure here is
// logged rather than propagated.
func (o *trinoPoolOperator) closeInstanceOperation(ctx context.Context, instanceID, phase, lastError string) {
	if o.operations == nil {
		return
	}
	if err := o.operations.FinishTrinoPoolOperation(ctx, o.lease, "instance:"+instanceID, phase, lastError); err != nil &&
		!errors.Is(err, configstore.ErrTrinoPoolConflict) {
		slog.Debug("Trino pool operation could not be closed.",
			"pool", o.config.PublicID, "instance", instanceID, "error", err)
	}
}
