//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
//  3. advance THE one live barrier by one step - open, one receipt, or commit.
//
// Two scheduling rules make that safe at fleet scale, and both are
// load-bearing rather than tuning:
//
//   - At most ONE barrier is live at a time, and it is driven to completion
//     rather than round-robined between steps. Every OPEN publication blocks
//     every member admission (the Gateway requires a joining member to
//     acknowledge the open publication's target revision, which a member
//     registered under a release id can never do), so N concurrent barriers are
//     N obstacles to the pool's own compute lifecycle.
//   - While a member is waiting to join, no new barrier is opened and the live
//     one is released, ONE per pass. Compute wins: a tenant waiting a few more
//     seconds is cheaper than a pool that cannot grow back.
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
	// ClearTrinoPoolPublicationBarrier is the durable half of abandoning an
	// attempt: without it the row keeps naming a finished publication and every
	// later pass selects that same dead attempt again.
	ClearTrinoPoolPublicationBarrier(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID string) error
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

// trinoPoolBarrierBasis is the configuration ONE attempt is admitting against.
//
// It is captured when the barrier opens and every receipt of that attempt is
// verified against it, so the receipts a commit rests on describe one coherent
// configuration rather than whatever happened to be current when each was
// taken. An attempt whose basis is gone (a leadership move) or no longer
// current (the projection moved) is released and reopened rather than
// completed on mixed evidence.
type trinoPoolBarrierBasis struct {
	Projection      trinoPoolProjectionRevisions
	CatalogRevision int64
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
	// The instances are read once and reused: the receipt step needs them, and
	// whether a candidate is waiting to be admitted decides whether a barrier
	// may be open at all.
	instances, err := o.store.ListTrinoPoolInstances(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("list pool instances: %w", err)
	}

	if progressed, err := o.revokeDepartedTenant(ctx, bindings, recorded); progressed || err != nil {
		return err
	}
	if progressed, err := o.publishChangedBindings(ctx, bindings, state); progressed || err != nil {
		return err
	}
	return o.advanceOneBarrier(ctx, bindings, recorded, state, instances)
}

// trinoPoolAwaitsMemberAdmission reports that a candidate is sitting at the
// Gateway's admission call.
//
// A candidate stays VALIDATING until its admission is accepted, so this is
// exactly "a member is blocked joining" - read from the durable instance rows
// rather than remembered from the last refusal, which a leadership move would
// lose and a durable admission backoff would delay by up to its whole wait.
func trinoPoolAwaitsMemberAdmission(instances []configstore.TrinoPoolInstance) bool {
	for _, instance := range instances {
		if trinopool.Phase(instance.Phase) == trinopool.PhaseValidating {
			return true
		}
	}
	return false
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
			Step:   o.step("tenant."+publication.OrgID, trinoPoolStepID("revoke", trinoPoolOccurrence(attempt))),
			Reason: "the warehouse is no longer served by this pool",
		}); err != nil {
			return true, o.dropAuthority(fmt.Errorf("revoke tenant %s: %w", publication.OrgID, err))
		}
		slog.Info("Trino pool tenant admission revoked.",
			"pool", o.config.PublicID, "tenant", publication.OrgID)
		o.forgetBarrierBasis(publication.PublicationID)
		return true, o.dropAuthority(o.publications.RecordTrinoPoolTenantRevoked(ctx, o.lease,
			o.config.PoolID, publication.OrgID, "the warehouse is no longer served by this pool"))
	}
	return false, nil
}

// publishChangedBindings publishes one tenant's principal set when it differs
// from the DURABLE record of what was last published.
//
// Every publication is a NEW durable occurrence, taken before the call. The
// Gateway resolves a step identity it has already recorded by returning that
// step's result and applying NOTHING, and a tenant's revision is a digest of
// its principal set - so a login that is added and then removed again produces
// a set, and therefore a body, byte-identical to an earlier one. Under a
// constant step identity that publication is a replay: the removed login stays
// bound to the tenant at the Gateway, and the next tenant to be given that
// principal is refused for a conflict it cannot see. The same applies to
// re-publishing after a revocation.
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
	binding := eligible[int(o.bindingCursor%uint64(len(eligible)))]

	attempt, err := o.publications.BeginTrinoPoolPublicationAttempt(ctx, o.lease, o.config.PoolID, binding.Tenant)
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("start a publication for %s: %w", binding.Tenant, err))
	}
	admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, binding.Tenant,
		trinogateway.PublishPrincipalsRequest{
			Step:       o.step("tenant."+binding.Tenant, trinoPoolStepID("principals", trinoPoolOccurrence(attempt), binding.Revision)),
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
		"principals", len(binding.Principals), "attempt", attempt, "state", admission.State)
	if err := o.publications.RecordTrinoPoolTenantPrincipals(ctx, o.lease,
		o.config.PoolID, binding.Tenant, binding.Revision); err != nil {
		return true, o.dropAuthority(err)
	}
	return true, o.dropAuthority(o.publications.ClearTrinoPoolPublicationFailure(ctx, o.lease,
		o.config.PoolID, binding.Tenant))
}

// advanceOneBarrier moves THE live barrier one step, or opens one when there is
// none and nothing is waiting on the pool's compute.
//
// Three properties matter at fleet scale, where a pool serves thousands of
// warehouses:
//
//   - A tenant's target is ITS OWN, not a snapshot of the whole fleet's
//     configuration. An admitted tenant is finished until its own principals
//     change; provisioning a NEW warehouse must not re-admit every existing
//     one, which at one external step per five-second tick would have delayed
//     that new tenant by hours.
//   - ONE barrier is live at a time and is driven to completion. Rotating
//     between steps spread each tenant's attempt over the whole fleet's
//     rotation - long enough for ordinary membership churn to invalidate it
//     before it could commit - and left one open publication per eligible
//     tenant, every one of which blocks a member from joining.
//   - Fairness comes from the durable backoff, not from rotating mid-attempt:
//     a tenant whose step failed releases its barrier and serves its wait while
//     the next tenant runs.
func (o *trinoPoolOperator) advanceOneBarrier(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	recorded []configstore.TrinoPoolPublication,
	state map[string]configstore.TrinoPoolPublication,
	instances []configstore.TrinoPoolInstance,
) error {
	now := nowUTC()
	holder, live := trinoPoolBarrierHolder(recorded)

	if trinoPoolAwaitsMemberAdmission(instances) {
		// Compute wins. Every OPEN publication refuses the joining member, and
		// below the serving floor that is not a delay but a pool that cannot
		// grow back. One release per pass keeps the work bounded however many
		// barriers an older version left behind.
		if live {
			return o.releaseBarrier(ctx, holder, "a pool member is waiting to join")
		}
		return nil
	}

	if live {
		binding, known := trinoPoolBindingFor(bindings, holder.OrgID)
		switch {
		case !known || len(binding.Principals) == 0 || holder.PrincipalRevision != binding.Revision:
			return o.releaseBarrier(ctx, holder, "the tenant's binding changed during the attempt")
		case o.tenantIsCurrent(binding, holder):
			return o.releaseBarrier(ctx, holder, "the tenant is already admitted at this intent")
		case holder.NextAttemptAt != nil && now.Before(*holder.NextAttemptAt):
			return o.releaseBarrier(ctx, holder, "the tenant is serving out the wait its last failure earned")
		}
		err := o.stepLiveBarrier(ctx, binding, holder, instances)
		if err == nil {
			return o.dropAuthority(o.publications.ClearTrinoPoolPublicationFailure(ctx, o.lease,
				o.config.PoolID, binding.Tenant))
		}
		if o.fenced {
			// Authority loss is not this tenant's problem and must not be
			// recorded as one.
			return err
		}
		// Record the wait this tenant earned, then report the failure. The next
		// pass releases its barrier and moves on to somebody else.
		delay := trinoPoolRetryDelay(holder.Attempts)
		if recordErr := o.publications.RecordTrinoPoolPublicationFailure(ctx, o.lease,
			o.config.PoolID, binding.Tenant, now.Add(delay), err.Error()); recordErr != nil {
			return o.dropAuthority(recordErr)
		}
		return err
	}

	if o.expectedProjection().Policy == "" {
		// Nothing has been published yet, so there is no configuration for a
		// member to acknowledge. Opening a barrier against an unknown
		// projection would admit a tenant against nothing.
		return nil
	}
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
	return o.openBarrier(ctx, binding)
}

// trinoPoolBarrierHolder returns the tenant whose durable row names a live
// barrier. A row names one exactly while its attempt is in flight: the open
// records it, and the commit, the revocation and the release all clear it.
func trinoPoolBarrierHolder(recorded []configstore.TrinoPoolPublication) (configstore.TrinoPoolPublication, bool) {
	for _, publication := range recorded {
		if publication.PublicationID != "" && publication.State != configstore.TrinoPublicationRevoked {
			return publication, true
		}
	}
	return configstore.TrinoPoolPublication{}, false
}

func trinoPoolBindingFor(bindings []trinoPoolTenantBinding, tenant string) (trinoPoolTenantBinding, bool) {
	for _, binding := range bindings {
		if binding.Tenant == tenant {
			return binding, true
		}
	}
	return trinoPoolTenantBinding{}, false
}

// releaseBarrier retires the live attempt, at the Gateway AND in the durable
// record, so the next pass is free to do something else.
//
// Both halves are required. Abandoning without clearing leaves the row naming a
// finished publication, which every later pass selects again - the loop that
// kept re-abandoning one tenant's dead barrier while the barrier actually
// blocking a member's admission stayed open. Clearing without abandoning leaves
// an OPEN publication at the Gateway that nothing will ever close.
func (o *trinoPoolOperator) releaseBarrier(
	ctx context.Context,
	publication configstore.TrinoPoolPublication,
	reason string,
) error {
	slog.Info("Trino pool publication attempt released.",
		"pool", o.config.PublicID, "tenant", publication.OrgID,
		"publication", publication.PublicationID, "reason", reason)
	_, err := o.gateway.AbandonPublication(ctx, o.config.RoutingGroup, publication.PublicationID,
		o.step("publication."+publication.OrgID, trinoPoolStepID("abandon", publication.PublicationID)))
	switch {
	case err == nil, trinogateway.IsNotFound(err):
		// Abandoned, or the Gateway never had it: either way it is finished.
	case isTrinoPoolCommittedPublication(err):
		// An opened admission gate is never retracted. The tenant IS admitted;
		// the checkpoint simply never landed, so it is read back and recorded
		// rather than lost.
		current, readErr := o.gateway.GetPublication(ctx, o.config.RoutingGroup, publication.PublicationID)
		if readErr != nil {
			return o.dropAuthority(fmt.Errorf("read back publication %s: %w", publication.PublicationID, readErr))
		}
		binding := trinoPoolTenantBinding{Tenant: publication.OrgID}
		return o.recordAdmitted(ctx, binding, current.TargetRevision, current)
	default:
		return o.dropAuthority(fmt.Errorf("abandon publication %s: %w", publication.PublicationID, err))
	}
	o.forgetBarrierBasis(publication.PublicationID)
	return o.dropAuthority(o.publications.ClearTrinoPoolPublicationBarrier(ctx, o.lease,
		o.config.PoolID, publication.OrgID))
}

// isTrinoPoolCommittedPublication reports the Gateway refusing to abandon a
// publication because it has already admitted the tenant.
func isTrinoPoolCommittedPublication(err error) bool {
	return errors.Is(err, trinogateway.ErrIrreversible)
}

// openBarrier starts a NEW attempt for this tenant.
//
// The occurrence counter is durable and monotone, and every step identity
// carries the publication it belongs to, so the new attempt shares nothing with
// any it replaces: a new publication, a new open step, new receipts and a new
// commit. Reusing an identity would resolve the previous attempt's recorded
// outcome and leave this intent unapplied.
func (o *trinoPoolOperator) openBarrier(ctx context.Context, binding trinoPoolTenantBinding) error {
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
	attempt, err := o.publications.BeginTrinoPoolPublicationAttempt(ctx, o.lease, o.config.PoolID, binding.Tenant)
	if err != nil {
		return o.dropAuthority(fmt.Errorf("start a publication attempt for %s: %w", binding.Tenant, err))
	}
	target := trinoPoolTargetRevision(binding, attempt)
	publicationID := trinoPoolPublicationID(binding.Tenant, target)
	// The identity is recorded BEFORE the Gateway call, so a lost response is
	// resolved by reading that publication back instead of opening a second
	// barrier - which the Gateway refuses anyway, leaving the first one open
	// forever.
	if err := o.publications.RecordTrinoPoolPublicationOpen(ctx, o.lease,
		o.config.PoolID, binding.Tenant, publicationID, target); err != nil {
		return o.dropAuthority(fmt.Errorf("record publication intent for %s: %w", binding.Tenant, err))
	}
	o.rememberBarrierBasis(publicationID)
	opened, err := o.issueOpenPublication(ctx, binding, target, publicationID, poolState.MembershipGeneration)
	if err != nil {
		return err
	}
	slog.Info("Trino pool publication opened.",
		"pool", o.config.PublicID, "tenant", binding.Tenant, "publication", publicationID,
		"target", target, "attempt", attempt, "required", len(opened.RequiredMembers))
	return nil
}

func (o *trinoPoolOperator) issueOpenPublication(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	target, publicationID string,
	membershipGeneration int64,
) (trinogateway.Publication, error) {
	opened, err := o.gateway.OpenPublication(ctx, o.config.RoutingGroup, trinogateway.OpenPublicationRequest{
		Step:                         o.step("publication."+binding.Tenant, trinoPoolStepID("open", publicationID)),
		PublicationID:                publicationID,
		Tenant:                       binding.Tenant,
		TargetRevision:               target,
		ExpectedMembershipGeneration: membershipGeneration,
		PayloadHash:                  trinoPoolPublicationPlanHash(binding, target),
	})
	if err != nil {
		return trinogateway.Publication{}, o.dropAuthority(fmt.Errorf("open publication for %s: %w", binding.Tenant, err))
	}
	return opened, nil
}

// stepLiveBarrier performs the ONE next step of the live attempt.
func (o *trinoPoolOperator) stepLiveBarrier(
	ctx context.Context,
	binding trinoPoolTenantBinding,
	publication configstore.TrinoPoolPublication,
	instances []configstore.TrinoPoolInstance,
) error {
	poolState, err := o.gateway.GetPool(ctx, o.config.RoutingGroup)
	if err != nil {
		return fmt.Errorf("read pool state for %s: %w", o.config.PublicID, err)
	}
	if poolState.ServingMembers < int64(o.config.Spec.MinServing) {
		slog.Debug("Trino pool publication is waiting for the serving floor.",
			"pool", o.config.PublicID, "tenant", binding.Tenant, "serving", poolState.ServingMembers)
		return nil
	}

	current, err := o.gateway.GetPublication(ctx, o.config.RoutingGroup, publication.PublicationID)
	if err != nil {
		if !trinogateway.IsNotFound(err) {
			return fmt.Errorf("read publication %s: %w", publication.PublicationID, err)
		}
		// Recorded but absent on the Gateway: the open never landed. Re-issuing
		// it under the SAME identity is the resolution, not a second barrier.
		current, err = o.issueOpenPublication(ctx, binding, publication.TargetRevision,
			publication.PublicationID, poolState.MembershipGeneration)
		if err != nil {
			return err
		}
		o.rememberBarrierBasis(publication.PublicationID)
		return nil
	}

	switch current.Phase {
	case "ADMITTED":
		return o.recordAdmitted(ctx, binding, publication.TargetRevision, current)
	case "ABANDONED":
		// Finished business, not a fault to escalate: membership changes during
		// a deployment, and abandoning is how the protocol lets an attempt that
		// can no longer commit get out of the way. Clearing the durable pointer
		// is what lets the next pass open a new attempt, under a new occurrence.
		o.forgetBarrierBasis(publication.PublicationID)
		return o.dropAuthority(o.publications.ClearTrinoPoolPublicationBarrier(ctx, o.lease,
			o.config.PoolID, binding.Tenant))
	}
	if current.MembershipGeneration != poolState.MembershipGeneration {
		// The membership this barrier was opened against has moved on: a member
		// joined, failed or was replaced. Its commit can never satisfy the
		// Gateway's generation check.
		return o.releaseBarrier(ctx, publication, "the pool membership changed during the attempt")
	}
	basis, ok := o.barrierBasisFor(publication.PublicationID)
	if !ok {
		// This process did not open this attempt (a leadership move), so what
		// its existing receipts attest to is unknown. Completing it would rest a
		// commit on evidence nobody can describe.
		return o.releaseBarrier(ctx, publication, "the configuration this attempt was opened against is unknown")
	}
	if basis.Projection != o.expectedProjection() {
		// Receipts already recorded attest to the projection this attempt was
		// opened against. Finishing it against a newer one would commit on
		// mixed evidence; the tenant gets a fresh attempt on the current
		// projection instead, and an already-admitted tenant is untouched
		// because it has no attempt in flight.
		return o.releaseBarrier(ctx, publication, "the accepted projection changed during the attempt")
	}
	if len(current.MissingMembers) > 0 {
		return o.recordOneReceipt(ctx, binding, current, basis, instances)
	}
	return o.commitBarrier(ctx, binding, current, publication.TargetRevision, poolState.MembershipGeneration)
}

// retireOpenBarrierForAdmission releases the live tenant publication that is
// standing in the way of a member's admission.
//
// The Gateway requires a member joining during an open publication to
// acknowledge that publication's target revision. A member registers under its
// RELEASE id, so it can never satisfy a tenant barrier's target, and the two
// would wait for each other: the member for the barrier to close, the barrier
// for a membership that includes the member.
//
// The scheduler already suppresses new barriers and releases the live one while
// a candidate is waiting, so this is the belt on that brace: it runs when an
// admission has actually been refused, releases exactly ONE attempt, and
// records that release durably so the next refusal reaches the next one instead
// of re-abandoning the same finished publication.
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
	holder, live := trinoPoolBarrierHolder(recorded)
	if !live {
		return
	}
	slog.Info("Trino pool is releasing a tenant publication so a member can be admitted.",
		"pool", o.config.PublicID, "tenant", holder.OrgID,
		"publication", holder.PublicationID, "instance", instanceID, "reason", cause)
	if err := o.releaseBarrier(ctx, holder, "a pool member is waiting to join"); err != nil {
		slog.Warn("Trino pool could not release the open publication.",
			"pool", o.config.PublicID, "tenant", holder.OrgID, "error", err)
	}
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
	basis trinoPoolBarrierBasis,
	instances []configstore.TrinoPoolInstance,
) error {
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
		acknowledgement, err := o.acknowledge(ctx, instance, basis)
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
				// The step identity carries the PUBLICATION, so a receipt for
				// this attempt is a different operation from one for any other
				// attempt against the same member. Without it the second
				// attempt's receipt is the first one's step identity carrying a
				// different applied revision, which the Gateway refuses as a
				// changed intent - permanently, since the identity never moves
				// again. One ordinary membership change during a deployment
				// stranded a tenant that way.
				Step:            o.step("publication."+binding.Tenant, trinoPoolStepID("receipt", current.PublicationID, instanceID)),
				InstanceID:      instanceID,
				PodUID:          instance.CoordinatorPodUID,
				BootID:          instance.CoordinatorBootID,
				AppliedRevision: current.TargetRevision,
				AuthFingerprint: trinoPoolProjectionFingerprint(basis.Projection),
			}); err != nil {
			return o.dropAuthority(fmt.Errorf("record acknowledgement of %s for %s: %w",
				instanceID, binding.Tenant, err))
		}
		slog.Info("Trino pool member acknowledged a tenant's configuration.",
			"pool", o.config.PublicID, "tenant", binding.Tenant, "instance", instanceID,
			"target", current.TargetRevision)
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
			OperationID: "publication:" + binding.Tenant + ":" + current.PublicationID,
			PoolID:      o.config.PoolID,
			Kind:        configstore.TrinoPoolOperationPublish,
			IntentHash:  trinoPoolPublicationPlanHash(binding, target),
		},
		"commit",
		trinoPoolCommitIntent{Tenant: binding.Tenant, Target: target, PublicationID: current.PublicationID},
		func(ctx context.Context) (string, error) {
			result, err := o.gateway.CommitPublication(ctx, o.config.RoutingGroup, current.PublicationID,
				trinogateway.CommitPublicationRequest{
					Step:                         o.step("publication."+binding.Tenant, trinoPoolStepID("commit", current.PublicationID)),
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
	o.forgetBarrierBasis(publication.PublicationID)
	return o.dropAuthority(o.publications.RecordTrinoPoolPublicationCommitted(ctx, o.lease,
		o.config.PoolID, binding.Tenant, target, string(receipt)))
}

// acknowledge asks ONE member what it is actually serving, against the
// configuration THIS attempt was opened at.
func (o *trinoPoolOperator) acknowledge(
	ctx context.Context,
	instance configstore.TrinoPoolInstance,
	basis trinoPoolBarrierBasis,
) (trinoPoolAcknowledgement, error) {
	if o.acknowledgement == nil {
		return trinoPoolAcknowledgement{}, fmt.Errorf("this control plane cannot probe pool members")
	}
	if trinopool.Phase(instance.Phase) != trinopool.PhaseServing && trinopool.Phase(instance.Phase) != trinopool.PhaseAdmitted {
		return trinoPoolAcknowledgement{}, fmt.Errorf("member %s is %s", instance.InstanceID, instance.Phase)
	}
	acknowledgement, err := o.acknowledgement(ctx, instance.EndpointURL, basis.Projection, basis.CatalogRevision)
	if err != nil {
		return trinoPoolAcknowledgement{}, err
	}
	if !acknowledgement.ProjectionCurrent {
		return trinoPoolAcknowledgement{}, fmt.Errorf("member %s is not serving the current authorization and authentication projection", instance.InstanceID)
	}
	return acknowledgement, nil
}

// rememberBarrierBasis captures the configuration an attempt is admitting
// against. There is at most one live attempt, so this holds one entry.
func (o *trinoPoolOperator) rememberBarrierBasis(publicationID string) {
	if o.barrierBasis == nil {
		o.barrierBasis = map[string]trinoPoolBarrierBasis{}
	}
	o.barrierBasis[publicationID] = trinoPoolBarrierBasis{
		Projection:      o.expectedProjection(),
		CatalogRevision: o.publishedCatalogRevision(),
	}
}

func (o *trinoPoolOperator) barrierBasisFor(publicationID string) (trinoPoolBarrierBasis, bool) {
	basis, ok := o.barrierBasis[publicationID]
	return basis, ok
}

func (o *trinoPoolOperator) forgetBarrierBasis(publicationID string) {
	delete(o.barrierBasis, publicationID)
}

// tenantIsCurrent reports that this tenant's admission already covers its
// current intent.
//
// The intent is the tenant's OWN: the principal set it has now, admitted under
// the occurrence that is recorded for it. It deliberately does not include the
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
		publication.AdmittedTargetRevision == trinoPoolTargetRevision(binding, publication.Attempt)
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

// trinoPoolTargetRevision names ONE tenant's attempt: the principal set being
// admitted, and which occurrence is admitting it.
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
// deciding with the projection this attempt was opened at - which is strictly
// stronger than anything a name could assert, and costs nothing when nothing
// changed.
func trinoPoolTargetRevision(binding trinoPoolTenantBinding, attempt int64) string {
	if attempt < 1 || binding.Revision == "" {
		// No occurrence has been started for this tenant yet. Defaulting to one
		// would make a tenant's FIRST barrier and its first post-revocation
		// barrier share an identity, so the re-admission would replay the
		// original attempt's recorded outcome instead of admitting the tenant
		// again.
		return ""
	}
	return fmt.Sprintf("b%s.a%d", binding.Revision[:min(12, len(binding.Revision))], attempt)
}

// trinoPoolProjectionFingerprint is the 64-hex value the Gateway records with a
// receipt, derived from the projection the attempt was opened at, so an
// operator reading a receipt can tell which projection was acknowledged.
func trinoPoolProjectionFingerprint(projection trinoPoolProjectionRevisions) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		projection.Policy, projection.Password, projection.Group,
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

// trinoPoolOccurrence names one durable occurrence inside a step identity.
func trinoPoolOccurrence(attempt int64) string {
	return fmt.Sprintf("a%d", attempt)
}

// trinoPoolStepID joins the parts of a step identity, within the Gateway's
// 64-character column.
//
// A step identity must be STABLE (a retry has to resolve the recorded outcome)
// and UNIQUE per intent (a different intent under one identity is refused
// forever). Truncating would break uniqueness silently, so an identity that
// does not fit keeps its leading parts and carries a digest of the rest: still
// deterministic, still unique, and short enough that the Gateway stores it.
func trinoPoolStepID(parts ...string) string {
	const limit = 64
	joined := strings.Join(parts, ".")
	if len(joined) <= limit {
		return joined
	}
	digest := sha256.Sum256([]byte(joined))
	short := hex.EncodeToString(digest[:])[:24]
	head := parts[0]
	if len(head)+1+len(short) > limit {
		head = head[:limit-1-len(short)]
	}
	return head + "." + short
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
