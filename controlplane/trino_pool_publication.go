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
		present[binding.Tenant] = true
	}
	for _, publication := range recorded {
		if present[publication.OrgID] || publication.State == configstore.TrinoPublicationRevoked {
			continue
		}
		if _, err := o.gateway.RevokeTenant(ctx, o.config.RoutingGroup, publication.OrgID, trinogateway.RevokeTenantRequest{
			Step:   o.step("tenant."+publication.OrgID, "revoke"),
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
	for _, binding := range bindings {
		if len(binding.Principals) == 0 {
			// A tenant with no projectable login has nothing to admit. The
			// Gateway rejects an empty set, and inventing one would bind a
			// principal that cannot authenticate.
			continue
		}
		if state[binding.Tenant].PrincipalRevision == binding.Revision &&
			state[binding.Tenant].State != configstore.TrinoPublicationRevoked {
			continue
		}
		admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, binding.Tenant,
			trinogateway.PublishPrincipalsRequest{
				Step:       o.step("tenant."+binding.Tenant, "principals."+binding.Revision),
				Revision:   binding.Revision,
				Principals: binding.Principals,
			})
		if err != nil {
			return true, fmt.Errorf("publish principals for %s: %w", binding.Tenant, err)
		}
		slog.Info("Trino pool tenant binding published.",
			"pool", o.config.PublicID, "tenant", binding.Tenant,
			"principals", len(binding.Principals), "state", admission.State)
		return true, o.dropAuthority(o.publications.RecordTrinoPoolTenantPrincipals(ctx, o.lease,
			o.config.PoolID, binding.Tenant, binding.Revision))
	}
	return false, nil
}

// advanceOneBarrier moves a single tenant one step closer to being admitted.
func (o *trinoPoolOperator) advanceOneBarrier(
	ctx context.Context,
	bindings []trinoPoolTenantBinding,
	state map[string]configstore.TrinoPoolPublication,
) error {
	target := o.tenantTargetRevision()
	if target == "" {
		// Nothing has been published yet, so there is no configuration for a
		// member to acknowledge. Opening a barrier against an unknown target
		// would admit a tenant against nothing.
		return nil
	}
	for _, binding := range bindings {
		publication := state[binding.Tenant]
		if len(binding.Principals) == 0 || publication.PrincipalRevision != binding.Revision {
			// The binding has to be published before the barrier can mean
			// anything: the admission it opens is for that principal set.
			continue
		}
		if publication.AdmittedTargetRevision == target && publication.State == configstore.TrinoPublicationAdmitted {
			continue
		}
		return o.stepBarrier(ctx, binding, publication, target)
	}
	return nil
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
		// A barrier that was abandoned cannot be reused, and re-deriving the
		// same identity would replay into it forever. This needs an operator:
		// silently minting a second identity for the same intent is how a
		// publication ends up applied twice.
		return fmt.Errorf("publication %s for tenant %s was abandoned; it must be investigated before the tenant can be admitted",
			publicationID, binding.Tenant)
	}
	if len(current.MissingMembers) > 0 {
		return o.recordOneReceipt(ctx, binding, *current, target)
	}
	return o.commitBarrier(ctx, binding, *current, target, poolState.MembershipGeneration)
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

// tenantTargetRevision names the configuration a member must be serving before
// a tenant may be admitted against it: the published catalog revision and the
// authorization/authentication projection, together.
//
// One string, because the Gateway compares it verbatim between the barrier and
// every receipt. Splitting the two facts apart would let a member acknowledge
// the catalog while serving last week's password file.
func (o *trinoPoolOperator) tenantTargetRevision() string {
	projection := o.expectedProjection()
	if projection.Policy == "" || projection.Password == "" || projection.Group == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		projection.Policy, projection.Password, projection.Group,
	}, "\x00")))
	return fmt.Sprintf("c%d.p%s", o.publishedCatalogRevision(), hex.EncodeToString(digest[:])[:16])
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
