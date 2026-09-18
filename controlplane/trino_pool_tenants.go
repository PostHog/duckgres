//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
)

// Tenant principal publication.
//
// The Gateway's pooled admission restriction refuses to dispatch work whose
// claimed principal belongs to a tenant that is not admitted. It cannot derive
// which principals belong to which tenant: a warehouse's logins are one flat
// namespace produced by THIS controller's projection - a root login carrying no
// separator at all, plus qualified per-user names - and they are not a function
// of the tenant identifier. So the controller states them.
//
// The binding is published from the SAME projection that writes the
// coordinator's password file. If the two ever disagreed, the gate would either
// refuse a login Trino authenticates or admit one it rejects.

// trinoPoolTenantStore is the org projection the binding is derived from.
type trinoPoolTenantStore interface {
	ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error)
}

// publishTenantBindings republishes every changed tenant binding for this pool.
//
// It is driven by the binding REVISION, not by a timer: an unchanged tenant is
// not republished, and a tenant whose logins changed is republished before the
// next admission decision can be made on a stale set. Republishing is also how
// a removed login stops being admitted, because the Gateway replaces the set
// whole.
func (o *trinoPoolOperator) publishTenantBindings(ctx context.Context) error {
	if !o.config.Pool.TenantAdmission || o.tenants == nil {
		// The gate is off for this pool, so a binding would restrict nothing.
		// Publishing anyway would still be harmless, but it would suggest a
		// guarantee the pool is not making.
		return nil
	}
	orgs, err := o.tenants.ListTrinoEnabledOrgs()
	if err != nil {
		return fmt.Errorf("list tenants for pool %s: %w", o.config.PublicID, err)
	}

	var failures []error
	for _, binding := range trinoPoolBindingsFor(orgs, o.config.PoolID) {
		if o.publishedBindings[binding.Tenant] == binding.Revision {
			continue
		}
		if len(binding.Principals) == 0 {
			// A tenant with no projectable login has nothing to admit. Publishing
			// an empty set is rejected by the Gateway, and inventing one would
			// bind a principal that cannot authenticate.
			continue
		}
		admission, err := o.gateway.PublishTenantPrincipals(ctx, o.config.RoutingGroup, binding.Tenant,
			trinogateway.PublishPrincipalsRequest{
				Step:       o.step("tenant."+binding.Tenant, "principals."+binding.Revision),
				Revision:   binding.Revision,
				Principals: binding.Principals,
			})
		if err != nil {
			if errors.Is(err, trinogateway.ErrPrincipalConflict) {
				// Two tenants claim one principal. That is an ambiguity nothing
				// downstream may resolve by guessing, so it is surfaced and the
				// tenant stays unpublished rather than overwriting the other.
				slog.Error("Trino pool principal binding conflicts with another tenant.",
					"pool", o.config.PublicID, "tenant", binding.Tenant, "error", err)
			}
			failures = append(failures, fmt.Errorf("publish principals for %s: %w", binding.Tenant, err))
			continue
		}
		if o.publishedBindings == nil {
			o.publishedBindings = map[string]string{}
		}
		o.publishedBindings[binding.Tenant] = binding.Revision
		slog.Info("Trino pool tenant binding published.",
			"pool", o.config.PublicID, "tenant", binding.Tenant,
			"principals", len(binding.Principals), "state", admission.State)
	}
	// One tenant's failure must not stop the others: a pool serving many
	// warehouses cannot be held back by one bad row.
	return errors.Join(failures...)
}
