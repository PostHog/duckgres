//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// The tenant publication barrier.
//
// Publishing a tenant's principals tells the Gateway which logins belong to it.
// It does NOT admit the tenant: the Gateway's admission gate dispatches work
// only for a tenant in state ADMITTED, and only a committed publication puts it
// there. Without this driver the gate, once enabled, denies every tenant
// forever - which is why it is here and not a helper waiting for a caller.
//
// The barrier is what makes admission mean something: it commits only when
// EVERY currently active member has acknowledged the tenant's configuration, so
// a tenant is never dispatchable to a coordinator that has not got its catalog,
// its password line or its authorization data yet.
//
// Order per tick, at most one external step:
//
//  1. revoke a tenant that has disappeared from the projection (never skip it
//     silently: its logins would stay dispatchable);
//  2. publish a changed principal binding;
//  3. advance ONE tenant's barrier by one step - open, one receipt, or commit.
//
// Every decision reads the DURABLE record, never a leader's memory: a restart
// or a leadership move must not republish blindly or assume an admission that
// never committed.

// trinoPoolTenantStore is the org projection the principal binding is derived
// from. It is the SAME projection that writes the coordinator's password file,
// so the gate can never key on a principal Trino would reject or miss one it
// authenticates.
type trinoPoolTenantStore interface {
	ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error)
}

// trinoPoolPublicationStore is the durable publication state.
type trinoPoolPublicationStore interface {
	ListTrinoPoolPublications(ctx context.Context, poolID string) ([]configstore.TrinoPoolPublication, error)
	RecordTrinoPoolTenantPrincipals(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID, principalRevision string) error
	RecordTrinoPoolPublicationOpen(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID, publicationID, targetRevision string) error
	RecordTrinoPoolPublicationCommitted(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID, targetRevision, receipt string) error
	RecordTrinoPoolTenantRevoked(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID, reason string) error
	// BeginTrinoPoolPublicationAttempt bumps the tenant's occurrence counter,
	// which every durable step identity for that tenant carries.
	BeginTrinoPoolPublicationAttempt(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID string) (int64, error)
	// RecordTrinoPoolPublicationFailure / ClearTrinoPoolPublicationFailure are
	// the per-tenant durable backoff: the driver takes one tenant at a time, so
	// a failing tenant must step aside rather than hold the queue.
	RecordTrinoPoolPublicationFailure(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID string, nextAttemptAt time.Time, lastError string) error
	ClearTrinoPoolPublicationFailure(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID string) error
}

// trinoPoolAcknowledgement is what one member reports about the configuration
// it is serving.
type trinoPoolAcknowledgement struct {
	ProcessID       string
	AppliedRevision int64
	// ProjectionCurrent reports that this member's authorization and
	// authentication data are the ones this control plane is serving.
	ProjectionCurrent bool
}

// advanceTenantAdmissions drives the barrier for this pool's tenants.
func (o *trinoPoolOperator) advanceTenantAdmissions(ctx context.Context) error {
	if !o.config.Pool.TenantAdmission || o.tenants == nil || o.publications == nil {
		// The gate is off for this pool, so there is nothing to admit against.
		// Publishing a binding anyway would suggest a guarantee the pool is not
		// making.
		return nil
	}
	orgs, err := o.tenants.ListTrinoEnabledOrgs()
	if err != nil {
		return fmt.Errorf("list tenants for pool %s: %w", o.config.PublicID, err)
	}
	// The admission gate certifies members against the published catalog
	// revision, so that number has to be the one the catalog STORE is at - not
	// the last one a publication managed to write down.
	if err := o.ensureCatalogWatermark(ctx); err != nil {
		return err
	}
	bindings := trinoPoolBindingsFor(orgs, o.config.PoolID)
	recorded, err := o.publications.ListTrinoPoolPublications(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("read publications for pool %s: %w", o.config.PublicID, err)
	}
	state := make(map[string]configstore.TrinoPoolPublication, len(recorded))
	for _, publication := range recorded {
		state[publication.OrgID] = publication
	}

	if progressed, err := o.revokeDepartedTenant(ctx, bindings, recorded); progressed || err != nil {
		return err
	}
	if progressed, err := o.publishChangedBindings(ctx, bindings, state); progressed || err != nil {
		return err
	}
	return o.advanceOneBarrier(ctx, bindings, state)
}

// revokeDepartedTenant withdraws the admission of a tenant that is no longer in
// the projection.
//
// Skipping it would leave the tenant's principals dispatchable indefinitely: the
// Gateway replaces a principal set only when it is published, so a warehouse
// that was disabled or deleted keeps its logins admitted until somebody says
// otherwise. One per tick, because each is an external mutation.
func (o *trinoPoolOperator) revokeDepartedTenant(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	recorded []configstore.TrinoPoolPublication,
) (bool, error) {
	present := make(map[string]bool, len(bindings))
	for _, binding := range bindings {
		// A tenant whose last projectable login was removed is DEPARTED, not
		// "nothing to do": its principals stay dispatchable until somebody
		// publishes over them, and an empty set is not publishable. Skipping it
		// left a warehouse admitted under logins that no longer exist.
		if len(binding.Principals) == 0 {
			continue
		}
		present[binding.Tenant] = true
	}
	for _, publication := range recorded {
		if present[publication.OrgID] || publication.State == configstore.TrinoPublicationRevoked {
			continue
		}
		// A new occurrence for this revocation. Bumping it first means the
		// identity is durable before the call, so a lost response resolves
		// against the same occurrence rather than minting a second one.
		attempt, err := o.publications.BeginTrinoPoolPublicationAttempt(ctx, o.lease, o.config.PoolID, publication.OrgID)
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("start a revocation for %s: %w", publication.OrgID, err))
		}
		if _, err := o.gateway.RevokeTenant(ctx, o.config.RoutingGroup, publication.OrgID, trinogateway.RevokeTenantRequest{
			// The occurrence counter is what makes a SECOND revocation - after
			// the tenant was re-enabled and admitted again - a new operation
			// rather than a replay that returns the first revocation's outcome
			// and leaves the tenant admitted.
			Step:   o.step("tenant."+publication.OrgID, fmt.Sprintf("revoke.a%d", attempt)),
			Reason: "the warehouse is no longer served by this pool",
		}); err != nil {
			return true, o.dropAuthority(fmt.Errorf("revoke tenant %s: %w", publication.OrgID, err))
		}
		slog.Info("Trino pool tenant admission revoked.",
			"pool", o.config.PublicID, "tenant", publication.OrgID)
		return true, o.dropAuthority(o.publications.RecordTrinoPoolTenantRevoked(ctx, o.lease,
			o.config.PoolID, publication.OrgID, "the warehouse is no longer served by this pool"))
	}
	return false, nil
}

// publishChangedBindings publishes one tenant's principal set when it differs
// from the DURABLE record of what was last published.
func (o *trinoPoolOperator) publishChangedBindings(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	state map[string]configstore.TrinoPoolPublication,
) (bool, error) {
	now := nowUTC()
	eligible := make([]trinoPoolTenantBinding, 0, len(bindings))
	for _, binding := range bindings {
		if len(binding.Principals) == 0 {
			// A tenant with no projectable login has nothing to publish: the
			// Gateway rejects an empty set, and inventing one would bind a
			// principal that cannot authenticate. Its admission is withdrawn by
			// the revocation pass instead.
			continue
		}
		publication := state[binding.Tenant]
		if publication.PrincipalRevision == binding.Revision &&
			publication.State != configstore.TrinoPublicationRevoked {
			continue
		}
		if publication.NextAttemptAt != nil && now.Before(*publication.NextAttemptAt) {
			continue
		}
		eligible = append(eligible, binding)
	}
	if len(eligible) == 0 {
		return false, nil
	}
	// Rotate, for the same reason the barrier does: a tenant whose publication
	// keeps failing must not hold the front of the queue forever.
	o.bindingCursor++
	for offset := range eligible {
		binding := eligible[(int(o.bindingCursor)+offset)%len(eligible)]
		admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, binding.Tenant,
			trinogateway.PublishPrincipalsRequest{
				Step:       o.step("tenant."+binding.Tenant, "principals."+binding.Revision),
				Revision:   binding.Revision,
				Principals: binding.Principals,
			})
		if err != nil {
			if o.fenced {
				return true, err
			}
			// This tenant earned a wait; the next tick reaches a different one.
			delay := trinoPoolRetryDelay(state[binding.Tenant].Attempts)
			if recordErr := o.publications.RecordTrinoPoolPublicationFailure(ctx, o.lease,
				o.config.PoolID, binding.Tenant, now.Add(delay), err.Error()); recordErr != nil {
				return true, o.dropAuthority(recordErr)
			}
			return true, fmt.Errorf("publish principals for %s: %w", binding.Tenant, err)
		}
		slog.Info("Trino pool tenant binding published.",
			"pool", o.config.PublicID, "tenant", binding.Tenant,
			"principals", len(binding.Principals), "state", admission.State)
		if err := o.publications.RecordTrinoPoolTenantPrincipals(ctx, o.lease,
			o.config.PoolID, binding.Tenant, binding.Revision); err != nil {
			return true, o.dropAuthority(err)
		}
		return true, o.dropAuthority(o.publications.ClearTrinoPoolPublicationFailure(ctx, o.lease,
			o.config.PoolID, binding.Tenant))
	}
	return false, nil
}

// advanceOneBarrier moves a single tenant one step closer to being admitted.
//
// Two properties matter at fleet scale, where a pool serves thousands of
// warehouses:
//
//   - A tenant's target is ITS OWN, not a snapshot of the whole fleet's
//     configuration. An admitted tenant is finished until its own catalog or
//     its own logins change; provisioning a NEW warehouse must not re-admit
//     every existing one, which at one external step per five-second tick would
//     have delayed that new tenant by hours.
//   - Selection ROTATES and honours each tenant's durable backoff. Always
//     taking the first eligible tenant let one permanently failing warehouse
//     starve every tenant behind it forever.
func (o *trinoPoolOperator) advanceOneBarrier(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	state map[string]configstore.TrinoPoolPublication,
) error {
	now := nowUTC()
	eligible := make([]trinoPoolTenantBinding, 0, len(bindings))
	for _, binding := range bindings {
		publication := state[binding.Tenant]
		if len(binding.Principals) == 0 || publication.PrincipalRevision != binding.Revision {
			// The binding has to be published before the barrier can mean
			// anything: the admission it opens is for that principal set.
			continue
		}
		if o.tenantIsCurrent(binding, publication) {
			continue
		}
		if publication.NextAttemptAt != nil && now.Before(*publication.NextAttemptAt) {
			// This tenant is serving out the wait its last failure earned.
			continue
		}
		eligible = append(eligible, binding)
	}
	if len(eligible) == 0 {
		return nil
	}
	// Rotate the starting point so no tenant owns the front of the queue.
	o.barrierCursor++
	binding := eligible[int(o.barrierCursor%uint64(len(eligible)))]
	publication := state[binding.Tenant]

	if o.expectedProjection().Policy == "" {
		// Nothing has been published yet, so there is no configuration for a
		// member to acknowledge. Opening a barrier against an unknown
		// projection would admit a tenant against nothing.
		return nil
	}
	if publication.Attempt < 1 {
		// This tenant has no occurrence yet. Starting one is this tick's step;
		// the next tick opens its barrier.
		return o.reopenBarrier(ctx, binding, "the tenant has no publication attempt yet")
	}
	target := o.targetRevisionFor(binding, publication)
	if target == "" {
		return nil
	}
	err := o.stepBarrier(ctx, binding, publication, target)
	if err == nil {
		return o.dropAuthority(o.publications.ClearTrinoPoolPublicationFailure(ctx, o.lease,
			o.config.PoolID, binding.Tenant))
	}
	if o.fenced {
		// Authority loss is not this tenant's problem and must not be recorded
		// as one.
		return err
	}
	// Record the wait this tenant earned, then report the failure. The next
	// tick moves on to somebody else.
	delay := trinoPoolRetryDelay(publication.Attempts)
	if recordErr := o.publications.RecordTrinoPoolPublicationFailure(ctx, o.lease,
		o.config.PoolID, binding.Tenant, now.Add(delay), err.Error()); recordErr != nil {
		return o.dropAuthority(recordErr)
	}
	return err
}

// tenantIsCurrent reports that this tenant's admission already covers its
// current intent.
//
// The intent is the tenant's OWN: the principal set it has now, admitted under
// the attempt that is recorded for it. It deliberately does not include the
// pool's current catalog revision or the fleet-wide projection digest - those
// move every time ANY warehouse is provisioned or any login changes anywhere,
// which would re-admit the entire fleet for one new tenant. What a member must
// have applied is checked where it belongs: at receipt time, against that
// member, before its acknowledgement is recorded.
func (o *trinoPoolOperator) tenantIsCurrent(
	binding trinoPoolTenantBinding,
	publication configstore.TrinoPoolPublication,
) bool {
	return publication.State == configstore.TrinoPublicationAdmitted &&
		publication.AdmittedTargetRevision != "" &&
		publication.PrincipalRevision == binding.Revision &&
		publication.AdmittedTargetRevision == o.targetRevisionFor(binding, publication)
}

// stepBarrier performs the ONE next step of a tenant's barrier.
func (o *trinoPoolOperator) stepBarrier(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	publication configstore.TrinoPoolPublication,
	target string,
) error {
	poolState, err := o.gateway.GetPool(ctx, o.config.RoutingGroup)
	if err != nil {
		return fmt.Errorf("read pool state for %s: %w", o.config.PublicID, err)
	}
	if poolState.ServingMembers < int64(o.config.Spec.MinServing) {
		// The Gateway refuses a publication below the serving floor. Opening one
		// to be told so is noise; the next tick tries again.
		slog.Debug("Trino pool publication is waiting for the serving floor.",
			"pool", o.config.PublicID, "tenant", binding.Tenant, "serving", poolState.ServingMembers)
		return nil
	}

	publicationID := trinoPoolPublicationID(binding.Tenant, target)
	current, err := o.openOrReadBarrier(ctx, binding, publication, target, publicationID, poolState.MembershipGeneration)
	if err != nil || current == nil {
		return err
	}
	switch current.Phase {
	case "ADMITTED":
		return o.recordAdmitted(ctx, binding, target, *current)
	case "ABANDONED":
		// An abandoned barrier is finished business, not a fault to escalate:
		// membership changes during a deployment, and abandoning is how the
		// protocol lets an attempt that can no longer commit get out of the way.
		// The tenant gets a NEW attempt, which is a new identity end to end -
		// reusing this one would replay the abandoned attempt's outcome forever.
		return o.reopenBarrier(ctx, binding, "the previous attempt was abandoned")
	}
	// The membership this barrier was opened against has moved on: a member
	// joined, failed or was replaced. Its commit can never satisfy the
	// Gateway's generation check, and while it stays open a joining member
	// cannot be admitted - which is the cycle where the commit is waiting for a
	// replacement the open barrier itself refuses.
	if current.MembershipGeneration != poolState.MembershipGeneration {
		return o.abandonForMembershipChange(ctx, binding, *current, poolState.MembershipGeneration)
	}
	if len(current.MissingMembers) > 0 {
		return o.recordOneReceipt(ctx, binding, *current, target)
	}
	return o.commitBarrier(ctx, binding, *current, target, poolState.MembershipGeneration)
}

// abandonForMembershipChange retires an attempt whose membership moved, so the
// next one can be opened against the membership that exists now.
//
// This is the ordinary case during a rollout, not an exception: replacing an
// instance changes the membership generation, and an attempt opened before that
// can neither commit nor step aside on its own.
func (o *trinoPoolOperator) abandonForMembershipChange(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	current trinogateway.Publication,
	membershipGeneration int64,
) error {
	slog.Info("Trino pool publication is being reopened against the current membership.",
		"pool", o.config.PublicID, "tenant", binding.Tenant, "publication", current.PublicationID,
		"openedAt", current.MembershipGeneration, "now", membershipGeneration)
	if _, err := o.gateway.AbandonPublication(ctx, o.config.RoutingGroup, current.PublicationID,
		o.step("publication."+binding.Tenant, "abandon."+current.PublicationID)); err != nil {
		// An already-committed barrier refuses to be abandoned, which is a
		// legitimate answer: the next tick reads it back as ADMITTED.
		return o.dropAuthority(fmt.Errorf("abandon publication %s: %w", current.PublicationID, err))
	}
	return o.reopenBarrier(ctx, binding, "the pool membership changed during the attempt")
}

// retireOpenBarrierForAdmission abandons an open tenant publication that is
// standing in the way of a member's admission.
//
// The Gateway requires a member joining during an open publication to
// acknowledge that publication's target revision. A member registers under its
// RELEASE id, so it can never satisfy a tenant barrier's target, and the two
// would wait for each other: the member for the barrier to close, the barrier
// for a membership that includes the member. Compute wins - the barrier is
// reopened afterwards against the membership that then exists, and a tenant
// waiting a few more seconds is cheaper than a pool that cannot grow.
//
// Best effort by construction: the admission has already failed and is being
// reported. This only removes the obstacle for the next attempt.
func (o *trinoPoolOperator) retireOpenBarrierForAdmission(ctx context.Context, instanceID string, cause error) {
	if o.publications == nil {
		return
	}
	recorded, err := o.publications.ListTrinoPoolPublications(ctx, o.config.PoolID)
	if err != nil {
		slog.Warn("Trino pool could not read publications while admitting a member.",
			"pool", o.config.PublicID, "instance", instanceID, "error", err)
		return
	}
	for _, publication := range recorded {
		if publication.State != configstore.TrinoPublicationAdmitting || publication.PublicationID == "" {
			continue
		}
		slog.Info("Trino pool is reopening a tenant publication so a member can be admitted.",
			"pool", o.config.PublicID, "tenant", publication.OrgID,
			"publication", publication.PublicationID, "instance", instanceID, "reason", cause)
		if _, err := o.gateway.AbandonPublication(ctx, o.config.RoutingGroup, publication.PublicationID,
			o.step("publication."+publication.OrgID, "abandon."+publication.PublicationID)); err != nil {
			slog.Warn("Trino pool could not abandon the open publication.",
				"pool", o.config.PublicID, "tenant", publication.OrgID, "error", err)
			return
		}
		if _, err := o.publications.BeginTrinoPoolPublicationAttempt(ctx, o.lease, o.config.PoolID, publication.OrgID); err != nil {
			slog.Warn("Trino pool could not start the next publication attempt.",
				"pool", o.config.PublicID, "tenant", publication.OrgID, "error", err)
		}
		return
	}
}

// reopenBarrier starts a NEW attempt for this tenant.
//
// The attempt counter is durable and monotone, and every step identity carries
// it, so the new attempt shares nothing with the one it replaces: the Gateway
// sees a new publication, a new open step, new receipts and a new commit. The
// next tick performs its first step.
func (o *trinoPoolOperator) reopenBarrier(ctx context.Context, binding trinoPoolTenantBinding, reason string) error {
	attempt, err := o.publications.BeginTrinoPoolPublicationAttempt(ctx, o.lease, o.config.PoolID, binding.Tenant)
	if err != nil {
		return o.dropAuthority(fmt.Errorf("start a new publication attempt for %s: %w", binding.Tenant, err))
	}
	slog.Info("Trino pool publication attempt started.",
		"pool", o.config.PublicID, "tenant", binding.Tenant, "attempt", attempt, "reason", reason)
	return nil
}

// openOrReadBarrier returns the live barrier, opening it when this tenant does
// not have one at this target yet.
//
// The identity is recorded BEFORE the Gateway call, so a lost response is
// resolved by reading that publication back instead of opening a second barrier
// - which the Gateway refuses anyway, leaving the first one open forever.
func (o *trinoPoolOperator) openOrReadBarrier(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	publication configstore.TrinoPoolPublication,
	target, publicationID string,
	membershipGeneration int64,
) (*trinogateway.Publication, error) {
	if publication.PublicationID == publicationID && publication.TargetRevision == target {
		current, err := o.gateway.GetPublication(ctx, o.config.RoutingGroup, publicationID)
		if err == nil {
			return &current, nil
		}
		if !trinogateway.IsNotFound(err) {
			return nil, fmt.Errorf("read publication %s: %w", publicationID, err)
		}
		// Recorded but absent on the Gateway: the open never landed. Falling
		// through re-opens it under the same identity.
	}
	if err := o.publications.RecordTrinoPoolPublicationOpen(ctx, o.lease,
		o.config.PoolID, binding.Tenant, publicationID, target); err != nil {
		return nil, o.dropAuthority(fmt.Errorf("record publication intent for %s: %w", binding.Tenant, err))
	}
	opened, err := o.gateway.OpenPublication(ctx, o.config.RoutingGroup, trinogateway.OpenPublicationRequest{
		Step:                         o.step("publication."+binding.Tenant, "open."+target),
		PublicationID:                publicationID,
		Tenant:                       binding.Tenant,
		TargetRevision:               target,
		ExpectedMembershipGeneration: membershipGeneration,
		PayloadHash:                  trinoPoolPublicationPlanHash(binding, target),
	})
	if err != nil {
		return nil, o.dropAuthority(fmt.Errorf("open publication for %s: %w", binding.Tenant, err))
	}
	slog.Info("Trino pool publication opened.",
		"pool", o.config.PublicID, "tenant", binding.Tenant, "publication", publicationID,
		"target", target, "required", len(opened.RequiredMembers))
	return &opened, nil
}

// recordOneReceipt acknowledges ONE member, after verifying against that member
// itself that it is serving the configuration the barrier requires.
//
// The verification is the point of the receipt. Asserting acknowledgement from
// what this controller published - rather than from what the member reports
// having loaded - would commit a barrier while a coordinator still lacks the
// tenant's catalog, password line or authorization data, and the tenant's first
// query would fail on the member the Gateway just declared ready for it.
func (o *trinoPoolOperator) recordOneReceipt(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	current trinogateway.Publication,
	target string,
) error {
	instances, err := o.store.ListTrinoPoolInstances(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("list pool instances: %w", err)
	}
	byID := make(map[string]configstore.TrinoPoolInstance, len(instances))
	for _, instance := range instances {
		byID[instance.InstanceID] = instance
	}

	for _, instanceID := range current.MissingMembers {
		instance, known := byID[instanceID]
		if !known {
			return fmt.Errorf("publication %s requires member %s, which this pool has no record of",
				current.PublicationID, instanceID)
		}
		acknowledgement, err := o.acknowledge(ctx, instance)
		if err != nil {
			slog.Info("Trino pool member is not serving the tenant's configuration yet.",
				"pool", o.config.PublicID, "tenant", binding.Tenant, "instance", instanceID, "reason", err)
			return nil
		}
		if acknowledgement.ProcessID != instance.CoordinatorBootID {
			// The member restarted since it registered. Its receipt would be
			// refused, and the Gateway's own rule is that a restart invalidates
			// one: the health path will notice and replace it.
			slog.Warn("Trino pool member restarted; its acknowledgement cannot be recorded.",
				"pool", o.config.PublicID, "tenant", binding.Tenant, "instance", instanceID)
			return nil
		}
		if _, err := o.gateway.RecordPublicationReceipt(ctx, o.config.RoutingGroup, current.PublicationID,
			trinogateway.PublicationReceiptRequest{
				Step:            o.step("publication."+binding.Tenant, "receipt."+instanceID),
				InstanceID:      instanceID,
				PodUID:          instance.CoordinatorPodUID,
				BootID:          instance.CoordinatorBootID,
				AppliedRevision: target,
				AuthFingerprint: o.projectionFingerprint(),
			}); err != nil {
			return o.dropAuthority(fmt.Errorf("record acknowledgement of %s for %s: %w",
				instanceID, binding.Tenant, err))
		}
		slog.Info("Trino pool member acknowledged a tenant's configuration.",
			"pool", o.config.PublicID, "tenant", binding.Tenant, "instance", instanceID, "target", target)
		return nil
	}
	return nil
}

// commitBarrier closes the barrier, which is what actually admits the tenant.
func (o *trinoPoolOperator) commitBarrier(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	current trinogateway.Publication,
	target string,
	membershipGeneration int64,
) error {
	var committed trinogateway.Publication
	if err := o.runDurableStep(ctx,
		configstore.TrinoPoolOperationSpec{
			OperationID: "publication:" + binding.Tenant + ":" + target,
			PoolID:      o.config.PoolID,
			Kind:        configstore.TrinoPoolOperationPublish,
			IntentHash:  trinoPoolPublicationPlanHash(binding, target),
		},
		"commit",
		trinoPoolCommitIntent{Tenant: binding.Tenant, Target: target, PublicationID: current.PublicationID},
		func(ctx context.Context) (string, error) {
			result, err := o.gateway.CommitPublication(ctx, o.config.RoutingGroup, current.PublicationID,
				trinogateway.CommitPublicationRequest{
					Step:                         o.step("publication."+binding.Tenant, "commit."+target),
					ExpectedMembershipGeneration: membershipGeneration,
				})
			if err != nil {
				return "", err
			}
			committed = result
			return fmt.Sprintf(`{"phase":%q,"tenantState":%q}`, result.Phase, result.TenantState), nil
		},
	); err != nil {
		return fmt.Errorf("commit publication for %s: %w", binding.Tenant, err)
	}
	if committed.Phase == "" {
		// The step was recorded complete by an earlier attempt. The Gateway is
		// authoritative for its own state, so the outcome is read back rather
		// than assumed from the record.
		current, err := o.gateway.GetPublication(ctx, o.config.RoutingGroup, current.PublicationID)
		if err != nil {
			return fmt.Errorf("read back publication for %s: %w", binding.Tenant, err)
		}
		committed = current
	}
	if committed.Phase != "ADMITTED" {
		return fmt.Errorf("publication for %s did not admit the tenant: phase %s", binding.Tenant, committed.Phase)
	}
	return o.recordAdmitted(ctx, binding, target, committed)
}

// recordAdmitted checkpoints an admission the Gateway has already made. The
// Gateway's record is authoritative from the moment it commits, so a failure
// here delays the checkpoint - it never retracts the admission.
func (o *trinoPoolOperator) recordAdmitted(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	target string,
	publication trinogateway.Publication,
) error {
	receipt, err := json.Marshal(map[string]any{
		"publicationId": publication.PublicationID,
		"tenantState":   publication.TenantState,
		"receipts":      len(publication.Receipts),
	})
	if err != nil {
		return fmt.Errorf("encode publication receipt for %s: %w", binding.Tenant, err)
	}
	slog.Info("Trino pool tenant admitted.",
		"pool", o.config.PublicID, "tenant", binding.Tenant, "target", target,
		"members", len(publication.Receipts))
	return o.dropAuthority(o.publications.RecordTrinoPoolPublicationCommitted(ctx, o.lease,
		o.config.PoolID, binding.Tenant, target, string(receipt)))
}

// acknowledge asks ONE member what it is actually serving.
func (o *trinoPoolOperator) acknowledge(ctx context.Context, instance configstore.TrinoPoolInstance) (trinoPoolAcknowledgement, error) {
	if o.acknowledgement == nil {
		return trinoPoolAcknowledgement{}, fmt.Errorf("this control plane cannot probe pool members")
	}
	if trinopool.Phase(instance.Phase) != trinopool.PhaseServing && trinopool.Phase(instance.Phase) != trinopool.PhaseAdmitted {
		return trinoPoolAcknowledgement{}, fmt.Errorf("member %s is %s", instance.InstanceID, instance.Phase)
	}
	acknowledgement, err := o.acknowledgement(ctx, instance.EndpointURL, o.expectedProjection(), o.publishedCatalogRevision())
	if err != nil {
		return trinoPoolAcknowledgement{}, err
	}
	if !acknowledgement.ProjectionCurrent {
		return trinoPoolAcknowledgement{}, fmt.Errorf("member %s is not serving the current authorization and authentication projection", instance.InstanceID)
	}
	return acknowledgement, nil
}

func (o *trinoPoolOperator) expectedProjection() trinoPoolProjectionRevisions {
	if o.projection == nil {
		return trinoPoolProjectionRevisions{}
	}
	return o.projection()
}

func (o *trinoPoolOperator) publishedCatalogRevision() int64 {
	if o.pool == nil {
		return 0
	}
	return o.pool.PublicationRevision
}

// ensureCatalogWatermark makes the admission gate's revision authoritative
// before any tenant is published, admitted or certified against it.
//
// The failure this exists for: a catalog commits, the follow-up write of its
// revision onto the pool row fails, and NOTHING republishes it. That catalog
// already exists, so no later mutation carries the number forward; the gate
// keeps certifying members against a revision that predates the tenant, and a
// warehouse can be admitted - and reported ready - without its catalog. The
// tenant loop reads the enabled orgs on its own, so it would not even notice
// that a provisioner call had failed.
//
// Recovery is a bounded read of the store's own writer state, and it fails
// CLOSED: a watermark that cannot be read or cannot be checkpointed stops this
// tick's admissions rather than proceeding against a number nobody can
// confirm. The instance lifecycle is unaffected - reconcileOnce isolates this
// step's error - so a pool still repairs and drains while admissions hold.
func (o *trinoPoolOperator) ensureCatalogWatermark(ctx context.Context) error {
	if o.catalogWatermark == nil {
		// This cell publishes through a coordinator, which owns the catalog
		// store itself. There is no duckgres-side authority to compare against,
		// so the behaviour is exactly what it was.
		return nil
	}
	published, err := o.catalogWatermark(ctx)
	if err != nil {
		return fmt.Errorf("read the published catalog revision for pool %s: %w", o.config.PublicID, err)
	}
	if o.pool == nil {
		return fmt.Errorf("pool %s has no durable row to checkpoint against", o.config.PublicID)
	}
	if published <= o.pool.PublicationRevision {
		return nil
	}
	// The row is behind the store. Checkpoint it under this term's authority
	// before anything is certified against the stale value.
	if err := o.store.RecordTrinoPoolPublicationRevision(ctx, o.lease, o.config.PoolID, published); err != nil {
		return o.dropAuthority(fmt.Errorf("checkpoint the published catalog revision %d for pool %s: %w",
			published, o.config.PublicID, err))
	}
	slog.Warn("Trino pool recovered a catalog revision the publication never checkpointed.",
		"pool", o.config.PublicID, "recorded", o.pool.PublicationRevision, "published", published)
	o.pool.PublicationRevision = published
	return nil
}

// targetRevisionFor names ONE tenant's attempt: the principal set being
// admitted, and which attempt is admitting it.
//
// The Gateway compares this string verbatim between the barrier and every
// receipt, so it identifies the attempt rather than describing the fleet. It is
// deliberately NOT a global configuration fingerprint: the catalog revision and
// the projection digest move whenever any warehouse anywhere is provisioned or
// changes a login, so a global target would expire every tenant's admission for
// somebody else's change and re-admit the whole fleet one five-second step at a
// time.
//
// What a member must actually be serving is checked against that member, at
// receipt time - it must have applied the published catalog revision and be
// deciding with the current projections - which is strictly stronger than
// anything a name could assert, and costs nothing when nothing changed.
//
// An empty projection means this control plane has published nothing yet, so
// there is no configuration for a member to acknowledge and no barrier to open.
func (o *trinoPoolOperator) targetRevisionFor(
	binding trinoPoolTenantBinding,
	publication configstore.TrinoPoolPublication,
) string {
	projection := o.expectedProjection()
	if projection.Policy == "" || projection.Password == "" || projection.Group == "" {
		return ""
	}
	if publication.Attempt < 1 {
		// No occurrence has been started for this tenant yet. Defaulting to one
		// would make a tenant's FIRST barrier and its first post-revocation
		// barrier share an identity, so the re-admission would replay the
		// original attempt's recorded outcome instead of admitting the tenant
		// again.
		return ""
	}
	return fmt.Sprintf("b%s.a%d", binding.Revision[:min(12, len(binding.Revision))], publication.Attempt)
}

// projectionFingerprint is the 64-hex value the Gateway records with a receipt.
// It is derived from the same three revisions as the target, so an operator
// reading a receipt can tell which projection was acknowledged.
func (o *trinoPoolOperator) projectionFingerprint() string {
	projection := o.expectedProjection()
	digest := sha256.Sum256([]byte(strings.Join([]string{
		projection.Policy, projection.Password, projection.Group,
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

// trinoPoolPublicationID is deterministic in the tenant and the target, so a
// retry after a lost response addresses the same barrier instead of opening a
// second one.
func trinoPoolPublicationID(tenant, target string) string {
	digest := sha256.Sum256([]byte(tenant + "\x00" + target))
	return "pub." + hex.EncodeToString(digest[:])[:24]
}

// trinoPoolPublicationPlanHash is the barrier's immutable plan: this tenant,
// these principals, this configuration.
func trinoPoolPublicationPlanHash(binding trinoPoolTenantBinding, target string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(binding.Tenant + "\x00" + binding.Revision + "\x00" + target + "\x00"))
	for _, principal := range binding.Principals {
		_, _ = digest.Write([]byte(principal))
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// trinoPoolCommitIntent is what a commit MEANS, separate from the authority
// envelope that carries it.
type trinoPoolCommitIntent struct {
	Tenant        string
	Target        string
	PublicationID string
}
