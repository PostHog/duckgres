//go:build kubernetes

package controlplane

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/posthog/duckgres/controlplane/admin"
)

// adminSSOVerifierFromEnv builds the ALB OIDC verifier from the environment.
// It returns a nil verifier when DUCKGRES_ADMIN_SSO_ISSUER is unset. A nil
// verifier disables the SSO path: AuthMiddleware then ignores SSO headers and
// accepts only bearer tokens.
func adminSSOVerifierFromEnv(defaultRegion string) (*admin.ALBOIDCVerifier, error) {
	issuer := os.Getenv("DUCKGRES_ADMIN_SSO_ISSUER")
	if issuer == "" {
		slog.Warn("Admin SSO is not configured (DUCKGRES_ADMIN_SSO_ISSUER unset); the admin API accepts only bearer tokens. Set the SSO envs when the admin ingress authenticates through an AWS ALB.")
		return nil, nil
	}
	region := os.Getenv("DUCKGRES_ADMIN_SSO_REGION")
	if region == "" {
		region = defaultRegion
	}
	if region == "" {
		return nil, fmt.Errorf("DUCKGRES_ADMIN_SSO_ISSUER is set but no AWS region is available: set DUCKGRES_ADMIN_SSO_REGION or DUCKGRES_AWS_REGION")
	}
	clientID := os.Getenv("DUCKGRES_ADMIN_SSO_CLIENT_ID")
	if clientID == "" {
		slog.Warn("DUCKGRES_ADMIN_SSO_CLIENT_ID is unset; the admin SSO verifier skips the client claim check. Set it to the Cognito app client ID.")
	}
	verifier, err := admin.NewALBOIDCVerifier(region, issuer, clientID)
	if err != nil {
		return nil, err
	}
	slog.Info("Admin SSO signature verification enabled.", "issuer", issuer, "region", region)
	return verifier, nil
}
