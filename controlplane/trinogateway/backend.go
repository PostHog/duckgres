package trinogateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// Legacy backend registration, which pooled registration depends on.
//
// PoolLifecycleService.registerMember looks the backend up by name and takes
// the member's endpoint from THAT record rather than from the request body.
// So a pooled instance needs a Gateway backend registration to exist before it
// can be registered as a member. This is a real prerequisite, not an internal
// detail, and it is implemented here rather than left as a hidden assumption
// that would surface as POOL_NOT_FOUND on the first spawn.
//
// The registration is created with active=false and is NEVER activated through
// the legacy route: `/gateway/backend/activate/{name}` would make the backend
// eligible for routing immediately, bypassing the whole certified-admission
// path. Eligibility comes only from the pooled protocol's admit call.

// Backend is the legacy ProxyBackendConfiguration record.
type Backend struct {
	Name         string `json:"name"`
	ProxyTo      string `json:"proxyTo"`
	ExternalURL  string `json:"externalUrl,omitempty"`
	RoutingGroup string `json:"routingGroup"`
	// Active is the legacy routing switch. For a pooled member it stays false:
	// the pooled protocol decides eligibility, and a backend flipped active by
	// this path would serve tenant traffic with no certificate at all.
	Active bool `json:"active"`
}

// ListBackends reads every registered backend.
func (c *Client) ListBackends(ctx context.Context) ([]Backend, error) {
	var backends []Backend
	if err := c.do(ctx, http.MethodGet, "/gateway/backend/all", nil, &backends); err != nil {
		return nil, err
	}
	return backends, nil
}

// EnsureInactiveBackend creates the backend registration a pooled member needs,
// or verifies that an existing one matches. It refuses to touch a registration
// that is currently active or points somewhere else: that would either
// hijack another cluster's record or silently re-point live routing.
func (c *Client) EnsureInactiveBackend(ctx context.Context, backend Backend) error {
	if backend.Name == "" || backend.ProxyTo == "" || backend.RoutingGroup == "" {
		return errors.New("gateway backend registration requires a name, endpoint and routing group")
	}
	if backend.Active {
		return errors.New("a pooled backend registration must be created inactive")
	}

	existing, err := c.ListBackends(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range existing {
		if candidate.Name != backend.Name {
			continue
		}
		// Already registered. Anything other than an exact, inactive match is
		// a conflict the operator has to resolve, not something to overwrite.
		if candidate.Active {
			return fmt.Errorf("%w: backend %s is already active", ErrIdentityConflict, backend.Name)
		}
		if candidate.ProxyTo != backend.ProxyTo || candidate.RoutingGroup != backend.RoutingGroup {
			return fmt.Errorf("%w: backend %s is registered with a different endpoint or routing group",
				ErrIdentityConflict, backend.Name)
		}
		return nil
	}
	return c.do(ctx, http.MethodPost, "/gateway/backend/modify/add", backend, nil)
}

// DeleteBackend removes a backend registration. It is called only after the
// Gateway has irreversibly retired the member that used it, so no routing
// decision can still reference it.
func (c *Client) DeleteBackend(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("deleting a gateway backend requires a name")
	}
	return c.doRaw(ctx, http.MethodPost, "/gateway/backend/modify/delete", []byte(name), "text/plain", nil)
}
