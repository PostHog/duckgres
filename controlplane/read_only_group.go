//go:build kubernetes

package controlplane

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

// registerReadOnlyGroup mounts the read-only discovery endpoints on their
// OWN gin group: token-only auth (no SSO, no roles) accepting the scoped
// read-only secret OR the admin internal secret. The read-only secret
// grants nothing outside this group — an external writer's pod compromise
// must not escalate to the provisioning/admin surface. ALL discovery
// routes go through this function so TestReadOnlyGroupTopology can pin
// the exact surface the discovery credential reaches.
func registerReadOnlyGroup(engine *gin.Engine, readOnlyTokens, adminTokens admin.TokenSet, store provisioning.Store) {
	discoveryAPI := engine.Group("/api/v1",
		admin.AnyTokenAuthMiddleware(readOnlyTokens, adminTokens),
	)
	provisioning.RegisterDiscoveryAPI(discoveryAPI, store)
}

// validateDistinctReadOnlySecret refuses a read-only secret (or fallback)
// that collides with the internal secret (or its fallbacks). A shared value
// silently un-scopes the credential: every external-writer pod would then
// hold a token that validates in the ADMIN token set — provisioning,
// password resets, impersonation — which is exactly what the discovery
// secret exists to prevent. Failing startup is the only honest behavior;
// the operator copy-pasted the wrong value and nothing downstream can tell.
func validateDistinctReadOnlySecret(discovery string, discoveryFallbacks []string, internal string, internalFallbacks []string) error {
	adminValues := make(map[string]struct{}, 1+len(internalFallbacks))
	if internal != "" {
		adminValues[internal] = struct{}{}
	}
	for _, v := range internalFallbacks {
		if v != "" {
			adminValues[v] = struct{}{}
		}
	}
	check := func(v, which string) error {
		if v == "" {
			return nil
		}
		if _, clash := adminValues[v]; clash {
			return fmt.Errorf("%s equals the internal secret (or one of its fallbacks): the read-only credential would silently grant admin access — use a distinct value", which)
		}
		return nil
	}
	if err := check(discovery, "read-only secret"); err != nil {
		return err
	}
	for _, v := range discoveryFallbacks {
		if err := check(v, "a read-only secret fallback"); err != nil {
			return err
		}
	}
	return nil
}
