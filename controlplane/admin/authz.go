//go:build kubernetes

package admin

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// Role is the coarse authorization tier for an admin-UI request.
type Role string

const (
	// RoleViewer can read/monitor everything but cannot mutate config, run
	// impersonated queries, or read the audit log.
	RoleViewer Role = "viewer"
	// RoleAdmin can do everything: edit the config store, impersonate users,
	// and read the audit log.
	RoleAdmin Role = "admin"
)

const (
	// ctxIdentityKey holds the resolved *Identity in the gin context.
	ctxIdentityKey = "duckgres_identity"

	// albOIDCDataHeader is the signed JWT the AWS ALB injects after a
	// successful Cognito (Google Workspace) authentication. It carries the
	// user's claims (email, groups). The ALB strips any client-supplied copy,
	// so on an internal-scheme LB reachable only over the tailnet it is the
	// trust boundary for operator identity.
	albOIDCDataHeader = "X-Amzn-Oidc-Data"
	// albOIDCIdentityHeader carries just the subject/email; used as a fallback
	// when the full data JWT is absent.
	albOIDCIdentityHeader = "X-Amzn-Oidc-Identity"
)

// ssoEmailDomain is the only email domain accepted on the SSO path. SSO emails
// outside this domain are treated as unauthenticated (defense in depth on top
// of the ALB/Cognito allow-list and the operators table).
const ssoEmailDomain = "@posthog.com"

// RoleResolver maps an authenticated SSO email to a Role. It is injected into
// AuthMiddleware so the auth layer stays free of any config-store dependency;
// the control plane wires one backed by the operators table. A nil resolver
// (or one that returns RoleViewer) means the caller is a viewer.
type RoleResolver func(email string) Role

// Identity is the resolved caller for an admin-UI request.
type Identity struct {
	Email string `json:"email"`
	Role  Role   `json:"role"`
	// Source records how the identity was established (for audit): "sso" or
	// "internal-secret".
	Source string `json:"source"`
}

// IdentityFromContext returns the resolved Identity, or nil if unauthenticated.
func IdentityFromContext(c *gin.Context) *Identity {
	v, ok := c.Get(ctxIdentityKey)
	if !ok {
		return nil
	}
	id, _ := v.(*Identity)
	return id
}

// AuthMiddleware authenticates a request and resolves its Role. A valid
// TokenSet bearer token (header/cookie) is the service-to-service / break-glass
// path and always maps to admin. Otherwise the ALB-injected Cognito JWT yields
// the caller's email, and resolve (the operators-table lookup) maps that email
// to a Role. Unauthenticated requests are rejected 401.
func AuthMiddleware(tokens TokenSet, resolve RoleResolver) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Internal secret (header or login cookie) -> admin (break-glass).
		if tokens.Valid(requestAdminToken(c)) {
			c.Set(ctxIdentityKey, &Identity{Email: "internal-secret", Role: RoleAdmin, Source: "internal-secret"})
			c.Next()
			return
		}
		// 2. ALB/Cognito SSO identity: extract the email, then resolve its role.
		email := emailFromOIDC(c)
		if email == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		role := RoleViewer
		if resolve != nil {
			role = resolve(email)
		}
		c.Set(ctxIdentityKey, &Identity{Email: email, Role: role, Source: "sso"})
		c.Next()
	}
}

// emailFromOIDC extracts the caller's email from the ALB OIDC data JWT (falling
// back to the `sub` claim, then to the identity-only header). It returns "" when
// no usable SSO identity is present OR when the email fails domain hardening.
//
// Domain hardening: only @posthog.com emails are accepted, and a JWT
// email_verified claim that is explicitly false rejects the identity. This is
// defense in depth on top of the ALB/Cognito allow-list — a stray non-corporate
// or unverified identity never becomes a logged-in (even viewer) caller.
//
// The JWT signature is NOT verified here: the request only reaches this pod via
// the internal-scheme ALB (which signs and injects the header and strips
// client-supplied copies) over a tailnet-restricted network. Verifying the
// ALB's regional public key by `kid` is a hardening follow-up tracked in the
// design doc.
func emailFromOIDC(c *gin.Context) string {
	raw := c.GetHeader(albOIDCDataHeader)
	if raw == "" {
		// Fallback: identity-only header (email/subject). The data JWT is absent,
		// so there is no email_verified claim to consult — domain check still applies.
		if email := c.GetHeader(albOIDCIdentityHeader); acceptableSSOEmail(email, true) {
			return strings.ToLower(strings.TrimSpace(email))
		}
		return ""
	}
	claims, err := decodeJWTClaims(raw)
	if err != nil {
		slog.Warn("admin: failed to decode ALB OIDC data header", "error", err)
		return ""
	}
	email := stringClaim(claims, "email")
	if email == "" {
		email = stringClaim(claims, "sub")
	}
	// email_verified, when present, must not be false.
	verified := true
	if v, ok := claims["email_verified"]; ok {
		if b, ok := v.(bool); ok {
			verified = b
		}
	}
	if !acceptableSSOEmail(email, verified) {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(email))
}

// acceptableSSOEmail enforces the domain + verification hardening rules.
func acceptableSSOEmail(email string, verified bool) bool {
	if !verified {
		return false
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(email)), ssoEmailDomain)
}

// RoleGate enforces the viewer/admin split:
//   - mutating verbs (POST/PUT/PATCH/DELETE) require admin;
//   - GET is allowed for any authenticated caller EXCEPT paths flagged
//     adminOnlyGET (e.g. the audit log), which require admin.
//
// Individual handlers that need finer control (impersonation) re-check the role
// from the context.
func RoleGate(adminOnlyGET ...string) gin.HandlerFunc {
	adminGET := make(map[string]struct{}, len(adminOnlyGET))
	for _, p := range adminOnlyGET {
		adminGET[p] = struct{}{}
	}
	return func(c *gin.Context) {
		id := IdentityFromContext(c)
		if id == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		mutating := c.Request.Method == http.MethodPost ||
			c.Request.Method == http.MethodPut ||
			c.Request.Method == http.MethodPatch ||
			c.Request.Method == http.MethodDelete
		_, adminGETPath := adminGET[c.FullPath()]
		if (mutating || adminGETPath) && id.Role != RoleAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "admin role required"})
			return
		}
		c.Next()
	}
}

// RequireAdmin is a per-route gate for admin-only endpoints (e.g. the audit
// log read). Unlike RoleGate's path allow-list, it travels with the route
// registration, so moving/renaming the route cannot silently downgrade it.
func RequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := IdentityFromContext(c)
		if id == nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		if id.Role != RoleAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "admin role required"})
			return
		}
		c.Next()
	}
}

// meHandler returns the caller's identity + role so the SPA can tailor its UI.
func meHandler(c *gin.Context) {
	id := IdentityFromContext(c)
	if id == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	c.JSON(http.StatusOK, id)
}

// decodeJWTClaims base64url-decodes the payload segment of a JWT into a claims
// map. It does not verify the signature (see identityFromOIDC).
func decodeJWTClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, errMalformedJWT
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// Some encoders pad; try the padded variant.
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, err
		}
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}

var errMalformedJWT = &jwtError{"malformed JWT"}

type jwtError struct{ msg string }

func (e *jwtError) Error() string { return e.msg }

func stringClaim(claims map[string]any, key string) string {
	if v, ok := claims[key].(string); ok {
		return v
	}
	return ""
}
