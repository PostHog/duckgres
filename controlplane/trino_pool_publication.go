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
// Order per tick, at most one external step, and the order is the priority:
//
//  1. release the live barrier while a member is waiting to join - compute
//     first, because an open publication is what refuses the join;
//  2. advance THE one live barrier by one step - open, one receipt, or commit;
//  3. revoke a tenant that has disappeared from the projection (never skip it
//     silently: its logins would stay dispatchable);
//  4. publish a changed principal binding;
//  5. open a barrier for the next tenant that needs one.
//
// Finishing the live barrier BEFORE servicing new bindings is what bounds the
// wait. Servicing bindings first meant a fleet with a thousand pending tenants
// spent a thousand passes publishing before it advanced the barrier it already
// had open - and every one of those passes was a pass in which a joining member
// stayed refused. The bound now depends on the member count, not on how many
// tenants happen to be pending.
//
// Three scheduling rules make that safe at fleet scale, and all are
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
//   - A tenant with a request in flight is worked ONLY on that request. A lost
//     response is not a finished request: it may still be executing at the
//     Gateway, and moving to the next desired intent under a new step identity
//     would let the older one commit last.
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
	// BeginTrinoPoolPublicationIntent opens an occurrence for a request whose
	// outcome will be unknown until the Gateway answers, and records which kind
	// of request it stands for. ResolveTrinoPoolPublicationIntent closes it once
	// the answer is definite.
	BeginTrinoPoolPublicationIntent(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID, kind, payload string) (int64, error)
	ResolveTrinoPoolPublicationIntent(ctx context.Context, lease configstore.TrinoPoolLease, poolID, orgID string) error
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

// trinoPoolRevocationReason is fixed, so a revocation reissued under the same
// occurrence carries the identical body the Gateway journaled.
const trinoPoolRevocationReason = "the warehouse is no longer served by this pool"

// trinoPoolPendingRequest is the stored body of the request an occurrence
// stands for.
//
// It is kept so a reissue is BYTE-IDENTICAL to the original. Sending a
// different body under the same identity would work - the Gateway refuses it
// and that refusal is definite - but it makes a conflict the ordinary success
// path, and it leaves a tenant whose last login disappeared mid-flight with
// nothing to send at all. Storing the request removes both.
//
// Principal identifiers and the revision naming them. Never a credential.
type trinoPoolPendingRequest struct {
	Revision   string   `json:"revision,omitempty"`
	Principals []string `json:"principals,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

func (r trinoPoolPendingRequest) encode() (string, error) {
	encoded, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encode the pending request: %w", err)
	}
	return string(encoded), nil
}

func trinoPoolDecodePendingRequest(payload string) (trinoPoolPendingRequest, error) {
	var request trinoPoolPendingRequest
	if strings.TrimSpace(payload) == "" {
		return request, errors.New("the stored request is empty")
	}
	if err := json.Unmarshal([]byte(payload), &request); err != nil {
		return request, fmt.Errorf("decode the stored request: %w", err)
	}
	return request, nil
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

	// The live barrier comes first, and freeing a blocked member comes before
	// even that. Both are bounded by the MEMBER count; the binding work below is
	// bounded by the tenant count, and letting it go first let a fleet's worth
	// of pending tenants hold a barrier - and a candidate - open indefinitely.
	holder, live := trinoPoolBarrierHolder(recorded)
	if live {
		return o.serviceLiveBarrier(ctx, bindings, holder, instances)
	}
	if progressed, err := o.resolvePendingIntent(ctx, bindings, recorded); progressed || err != nil {
		return err
	}
	if progressed, err := o.revokeDepartedTenant(ctx, bindings, recorded); progressed || err != nil {
		return err
	}
	// Publishing a binding and opening the next barrier are two queues, and at
	// fleet scale both are long. Taking one of them first whenever it has work
	// starves the other: a thousand tenants waiting for a first publication
	// would push every admission behind all of them, and a thousand tenants
	// waiting for a barrier would do the same to the newest binding. They take
	// turns instead, so each drains at half the rate rather than one of them
	// not at all.
	publish := func() (bool, error) { return o.publishChangedBindings(ctx, bindings, state) }
	open := func() (bool, error) {
		if trinoPoolAwaitsMemberAdmission(instances) {
			// Nothing is open to release, and opening one now would refuse the
			// candidate that is waiting.
			return false, nil
		}
		return o.openNextBarrier(ctx, bindings, state)
	}
	o.tenantTurn++
	queues := [2]func() (bool, error){publish, open}
	if o.tenantTurn%2 == 1 {
		queues[0], queues[1] = queues[1], queues[0]
	}
	for _, queue := range queues {
		if progressed, err := queue(); progressed || err != nil {
			return err
		}
	}
	return nil
}

// openOccurrence opens a NEW occurrence for a tenant's next request.
//
// Only a tenant whose previous request reached a definite outcome gets here:
// one with a request still in flight is taken by resolvePendingIntent instead,
// which reissues THAT occurrence. The occurrence and what it stands for are
// recorded in ONE write, so a leader that dies between them cannot leave an
// occurrence nobody can attribute.
func (o *trinoPoolOperator) openOccurrence(
	ctx context.Context,
	orgID, kind string,
	request trinoPoolPendingRequest,
) (int64, error) {
	payload, err := request.encode()
	if err != nil {
		return 0, err
	}
	return o.publications.BeginTrinoPoolPublicationIntent(ctx, o.lease, o.config.PoolID, orgID, kind, payload)
}

// failTenantStep records the wait a failed request earned, and closes its
// occurrence when - and only when - the Gateway's answer was definite.
//
// A REFUSAL is definite. The Gateway runs a step in one transaction and every
// check throws before the journal write, so a refused call applied nothing and
// never will: that request is over, whatever is on the wire behind it. Holding
// the occurrence open for it pins the tenant on a body the Gateway has already
// rejected - and since every other queue skips a tenant with a request in
// flight, neither a corrected binding nor a revocation could ever be sent. The
// reachable case is a principal owned by another tenant, which does not heal on
// its own: a revocation leaves the Gateway's principal rows in place.
//
// An UNKNOWN outcome keeps the pin, which is what the occurrence exists for.
// Two answers count as unknown:
//
//   - no Gateway verdict at all (a transport error), which trinoPoolDecided
//     already distinguishes - it is the same question runDurableStep asks;
//   - ErrUnavailable, because a 503 from the Gateway's own handler and one from
//     anything in front of it are indistinguishable here, so the effect may yet
//     commit.
//
// A stale epoch is excluded for a different reason: the refusal is definite,
// but this process has lost the authority to write anything. The term ends and
// the next leader settles the occurrence by replaying it.
//
// Closing an occurrence NEVER moves the checkpoint: the tenant's recorded
// binding must keep describing what the Gateway actually accepted.
func (o *trinoPoolOperator) failTenantStep(
	ctx context.Context,
	orgID string,
	publication configstore.TrinoPoolPublication,
	cause error,
	what string,
	firstAttempt bool,
) error {
	// A refusal settles the occurrence only when nothing older can still be
	// executing under it, which is true exactly on the FIRST request of an
	// occurrence.
	//
	// On a REISSUE an earlier copy of the same request may still be on its way:
	// the Gateway rolls a refused call back before it journals anything, so the
	// step identity stays unclaimed and `publishTenantPrincipals` carries no
	// revision ordering of its own. Closing here would let the controller
	// advance to a newer intent, publish it, checkpoint it - and then have that
	// older copy land and overwrite the Gateway's binding with the superseded
	// principal set, which duckgres would never correct because it believes it
	// already published the newer one.
	if firstAttempt && trinoPoolRefusedDefinitively(cause) {
		if errors.Is(cause, trinogateway.ErrIntentChanged) {
			// With every reissue carrying the stored body this means something
			// else recorded this step with different content.
			slog.Error("Trino pool tenant occurrence carries content the Gateway did not record for it.",
				"pool", o.config.PublicID, "tenant", orgID,
				"occurrence", publication.Attempt, "error", cause)
		} else {
			slog.Warn("Trino pool tenant request was refused; closing its occurrence so the next intent can be sent.",
				"pool", o.config.PublicID, "tenant", orgID, "occurrence", publication.Attempt,
				"intent", publication.PendingIntent, "error", cause)
		}
		if err := o.publications.ResolveTrinoPoolPublicationIntent(ctx, o.lease,
			o.config.PoolID, orgID); err != nil {
			return o.dropAuthority(err)
		}
	}
	delay := trinoPoolRetryDelay(publication.Attempts)
	if err := o.publications.RecordTrinoPoolPublicationFailure(ctx, o.lease,
		o.config.PoolID, orgID, nowUTC().Add(delay), cause.Error()); err != nil {
		return o.dropAuthority(err)
	}
	return fmt.Errorf("%s: %w", what, cause)
}

// trinoPoolRefusedDefinitively reports an answer that settles the request: the
// Gateway decided, nothing was applied, and nothing in flight under that
// identity can apply later either.
//
// It reuses the existing decision test rather than an enumeration of codes, so
// a refusal the Gateway adds later is handled the same way it classifies every
// other one. The two exclusions are the answers that are not verdicts about the
// request: see failTenantStep.
func trinoPoolRefusedDefinitively(cause error) bool {
	if errors.Is(cause, trinogateway.ErrUnavailable) || errors.Is(cause, trinogateway.ErrStaleEpoch) {
		return false
	}
	// A typed Gateway error IS the verdict, whatever code it carries - the same
	// question runDurableStep asks - so a refusal the Gateway adds later needs
	// no change here.
	if trinoPoolDecided(cause) {
		return true
	}
	// The sentinels a refusal of THESE two calls can carry, for a caller that
	// hands back the sentinel without the typed envelope. Anything else stays
	// unknown, which keeps the occurrence pinned: the conservative direction.
	for _, refusal := range []error{
		trinogateway.ErrPrincipalConflict,
		trinogateway.ErrValidation,
		trinogateway.ErrIntentChanged,
		trinogateway.ErrNotFound,
		trinogateway.ErrAPIMode,
		trinogateway.ErrPoolDisabled,
		trinogateway.ErrIdentityConflict,
		trinogateway.ErrTenantNotAdmitted,
	} {
		if errors.Is(cause, refusal) {
			return true
		}
	}
	return false
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

// resolvePendingIntent finishes the ONE request a tenant already has in flight,
// before any tenant's desired intent is allowed to move.
//
// A lost response is not a finished request: it may still be executing at the
// Gateway and commit whenever it gets there. Two requests from the same
// controller under different step identities are unordered there - the row lock
// serializes arrival, not desire - so issuing the next intent while the
// previous one is unresolved lets the older one commit last: a binding nobody
// wants, or a revocation of a tenant that has since been re-enabled, while this
// control plane has checkpointed the newer intent and will never issue it
// again.
//
// Reissuing the SAME occurrence is what settles it. Either the Gateway has the
// step journaled - a definite answer, and the still-delayed duplicate can only
// replay it - or this call records it, and the delayed duplicate arrives under
// a journaled identity and applies nothing.
func (o *trinoPoolOperator) resolvePendingIntent(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	recorded []configstore.TrinoPoolPublication,
) (bool, error) {
	now := nowUTC()
	for _, publication := range recorded {
		if publication.PendingIntent == "" {
			continue
		}
		if publication.NextAttemptAt != nil && now.Before(*publication.NextAttemptAt) {
			// Serving out the wait its last failure earned. Another tenant may
			// proceed meanwhile; this one resumes when the wait elapses.
			continue
		}
		request, err := trinoPoolDecodePendingRequest(publication.PendingPayload)
		if err != nil {
			// The stored request is what makes the reissue possible, so an
			// unreadable one cannot be settled by replay. Closing the occurrence
			// is the only move left; it is also a bug, so it is loud.
			slog.Error("Trino pool cannot replay a tenant's request and is closing its occurrence.",
				"pool", o.config.PublicID, "tenant", publication.OrgID,
				"occurrence", publication.Attempt, "intent", publication.PendingIntent, "error", err)
			return true, o.dropAuthority(o.publications.ResolveTrinoPoolPublicationIntent(ctx, o.lease,
				o.config.PoolID, publication.OrgID))
		}
		if publication.PendingIntent == configstore.TrinoPublicationIntentRevoke {
			return true, o.reissueRevoke(ctx, publication, request)
		}
		// The STORED set, not the desired one. A tenant whose last login has
		// since gone away still has exactly this to replay, which is what makes
		// the occurrence settleable rather than abandoned - and an abandoned
		// occurrence is what let a delayed publication rebind principals nobody
		// wanted afterwards.
		return true, o.reissuePublication(ctx, publication, request)
	}
	return false, nil
}

// reissueRevoke settles a revocation whose outcome is unknown. It runs even
// when the tenant has come back: the revocation has to reach a definite outcome
// before the tenant can be published and admitted again, or it could commit
// afterwards and take a serving warehouse off the air with nothing left in the
// desired state to correct it.
func (o *trinoPoolOperator) reissueRevoke(
	ctx context.Context,
	publication configstore.TrinoPoolPublication,
	request trinoPoolPendingRequest,
) error {
	reason := request.Reason
	if _, err := o.gateway.RevokeTenant(ctx, o.config.RoutingGroup, publication.OrgID, trinogateway.RevokeTenantRequest{
		Step:   o.step("tenant."+publication.OrgID, trinoPoolStepID("revoke", trinoPoolOccurrence(publication.Attempt))),
		Reason: reason,
	}); err != nil {
		return o.failTenantStep(ctx, publication.OrgID, publication, err,
			fmt.Sprintf("settle the revocation of %s", publication.OrgID), false)
	}
	slog.Info("Trino pool tenant revocation settled.",
		"pool", o.config.PublicID, "tenant", publication.OrgID, "occurrence", publication.Attempt)
	o.forgetBarrierBasis(publication.PublicationID)
	return o.dropAuthority(o.publications.RecordTrinoPoolTenantRevoked(ctx, o.lease,
		o.config.PoolID, publication.OrgID, reason))
}

// reissuePublication settles a publication whose outcome is unknown by sending
// the STORED request again, byte for byte.
//
// An identical body under the same identity is an ordinary replay: if the
// earlier call committed, the Gateway returns its recorded result, and if it did
// not, this one records the identity so the delayed original applies nothing
// when it arrives. Neither outcome depends on a refusal, and a tenant whose
// desired binding has changed - or vanished - since is settled just the same.
// Its new binding, if any, goes out afterwards under a new occurrence.
func (o *trinoPoolOperator) reissuePublication(
	ctx context.Context,
	publication configstore.TrinoPoolPublication,
	request trinoPoolPendingRequest,
) error {
	admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, publication.OrgID,
		trinogateway.PublishPrincipalsRequest{
			Step:       o.step("tenant."+publication.OrgID, trinoPoolStepID("principals", trinoPoolOccurrence(publication.Attempt))),
			Revision:   request.Revision,
			Principals: request.Principals,
		})
	if err != nil {
		if o.fenced {
			return err
		}
		return o.failTenantStep(ctx, publication.OrgID, publication, err,
			fmt.Sprintf("settle the publication for %s", publication.OrgID), false)
	}
	slog.Info("Trino pool tenant binding settled.",
		"pool", o.config.PublicID, "tenant", publication.OrgID,
		"principals", len(request.Principals), "occurrence", publication.Attempt, "state", admission.State)
	if err := o.publications.RecordTrinoPoolTenantPrincipals(ctx, o.lease,
		o.config.PoolID, publication.OrgID, request.Revision); err != nil {
		return o.dropAuthority(err)
	}
	return o.dropAuthority(o.publications.ClearTrinoPoolPublicationFailure(ctx, o.lease,
		o.config.PoolID, publication.OrgID))
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
	now := nowUTC()
	for _, publication := range recorded {
		if present[publication.OrgID] || publication.State == configstore.TrinoPublicationRevoked {
			continue
		}
		if publication.PendingIntent != "" {
			// Its open occurrence is settled first, by the resolver.
			continue
		}
		if publication.NextAttemptAt != nil && now.Before(*publication.NextAttemptAt) {
			continue
		}
		reason := trinoPoolRevocationReason
		attempt, err := o.openOccurrence(ctx, publication.OrgID, configstore.TrinoPublicationIntentRevoke,
			trinoPoolPendingRequest{Reason: reason})
		if err != nil {
			return true, o.dropAuthority(fmt.Errorf("start a revocation for %s: %w", publication.OrgID, err))
		}
		if _, err := o.gateway.RevokeTenant(ctx, o.config.RoutingGroup, publication.OrgID, trinogateway.RevokeTenantRequest{
			// The occurrence is what makes a SECOND revocation - after the
			// tenant was re-enabled and admitted again - a new operation rather
			// than a replay that returns the first revocation's outcome and
			// leaves the tenant admitted. It is PINNED until the Gateway answers
			// definitively, so a revocation still executing there cannot commit
			// after the tenant has been re-enabled and re-admitted.
			Step:   o.step("tenant."+publication.OrgID, trinoPoolStepID("revoke", trinoPoolOccurrence(attempt))),
			Reason: reason,
		}); err != nil {
			return true, o.failTenantStep(ctx, publication.OrgID, publication, err,
				fmt.Sprintf("revoke tenant %s", publication.OrgID), true)
		}
		slog.Info("Trino pool tenant admission revoked.",
			"pool", o.config.PublicID, "tenant", publication.OrgID, "occurrence", attempt)
		o.forgetBarrierBasis(publication.PublicationID)
		return true, o.dropAuthority(o.publications.RecordTrinoPoolTenantRevoked(ctx, o.lease,
			o.config.PoolID, publication.OrgID, reason))
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
			publication.State != configstore.TrinoPublicationRevoked &&
			publication.PendingIntent == "" {
			continue
		}
		if publication.PendingIntent != "" {
			// Its open occurrence is settled first, by the resolver. Moving to
			// the next intent here is exactly what let an older request commit
			// last.
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
	publication := state[binding.Tenant]
	// The request is stored with the occurrence, so every later attempt at it -
	// by this leader or the next - sends these exact bytes.
	request := trinoPoolPendingRequest{Revision: binding.Revision, Principals: binding.Principals}
	attempt, err := o.openOccurrence(ctx, binding.Tenant, configstore.TrinoPublicationIntentPrincipals, request)
	if err != nil {
		return true, o.dropAuthority(fmt.Errorf("start a publication for %s: %w", binding.Tenant, err))
	}
	// The step identity is the OCCURRENCE, not the body. A publication whose
	// response was lost may still be executing at the Gateway; reissuing the
	// same identity is what makes that late arrival a journal replay instead of
	// a second effect that overwrites the binding published since.
	admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, binding.Tenant,
		trinogateway.PublishPrincipalsRequest{
			Step:       o.step("tenant."+binding.Tenant, trinoPoolStepID("principals", trinoPoolOccurrence(attempt))),
			Revision:   request.Revision,
			Principals: request.Principals,
		})
	if err != nil {
		if o.fenced {
			return true, err
		}
		return true, o.failTenantStep(ctx, binding.Tenant, publication, err,
			fmt.Sprintf("publish principals for %s", binding.Tenant), true)
	}
	slog.Info("Trino pool tenant binding published.",
		"pool", o.config.PublicID, "tenant", binding.Tenant,
		"principals", len(binding.Principals), "occurrence", attempt, "state", admission.State)
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
func (o *trinoPoolOperator) serviceLiveBarrier(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	holder configstore.TrinoPoolPublication,
	instances []configstore.TrinoPoolInstance,
) error {
	now := nowUTC()
	if trinoPoolAwaitsMemberAdmission(instances) {
		// Compute wins. Every OPEN publication refuses the joining member, and
		// below the serving floor that is not a delay but a pool that cannot
		// grow back. One release per pass keeps the work bounded however many
		// barriers an older version left behind.
		return o.releaseBarrier(ctx, holder, "a pool member is waiting to join")
	}
	binding, known := trinoPoolBindingFor(bindings, holder.OrgID)
	switch {
	case !known || len(binding.Principals) == 0 || holder.PrincipalRevision != binding.Revision:
		return o.releaseBarrier(ctx, holder, "the tenant's binding changed during the attempt")
	case holder.PendingIntent != "":
		// A request for this tenant is in flight, so what it will be admitted
		// FOR is not settled yet.
		return o.releaseBarrier(ctx, holder, "the tenant has a request in flight")
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
		// Authority loss is not this tenant's problem and must not be recorded
		// as one.
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

// openNextBarrier opens a barrier for the next tenant that needs one. There is
// no live barrier when this runs, so at most one is ever open.
func (o *trinoPoolOperator) openNextBarrier(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	state map[string]configstore.TrinoPoolPublication,
) (bool, error) {
	now := nowUTC()
	if o.expectedProjection().Policy == "" {
		// Nothing has been published yet, so there is no configuration for a
		// member to acknowledge. Opening a barrier against an unknown
		// projection would admit a tenant against nothing.
		return false, nil
	}
	eligible := make([]trinoPoolTenantBinding, 0, len(bindings))
	for _, binding := range bindings {
		publication := state[binding.Tenant]
		if len(binding.Principals) == 0 || publication.PrincipalRevision != binding.Revision {
			// The binding has to be published before the barrier can mean
			// anything: the admission it opens is for that principal set.
			continue
		}
		if publication.PendingIntent != "" {
			// A request for this tenant is still in flight. Admitting it now
			// would be admitting it for a binding that is not settled.
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
		return false, nil
	}
	// Rotate the starting point so no tenant owns the front of the queue.
	o.barrierCursor++
	binding := eligible[int(o.barrierCursor%uint64(len(eligible)))]
	return true, o.openBarrier(ctx, binding)
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
		// The admitted target names the BINDING it admitted, and the occurrence
		// that admitted it. Only the binding decides currency: the occurrence
		// counter also moves for this tenant's publications and revocations, and
		// comparing it would re-admit a tenant whose admission is perfectly
		// current every time one of those happened.
		strings.HasPrefix(publication.AdmittedTargetRevision, trinoPoolTargetPrefix(binding))
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
	return fmt.Sprintf("%sa%d", trinoPoolTargetPrefix(binding), attempt)
}

// trinoPoolTargetPrefix names the BINDING a target admits, exactly - the
// Gateway's revision alphabet takes 64 characters and a binding revision is 32,
// so nothing has to be truncated into an ambiguous prefix.
func trinoPoolTargetPrefix(binding trinoPoolTenantBinding) string {
	return "b" + binding.Revision + "."
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
