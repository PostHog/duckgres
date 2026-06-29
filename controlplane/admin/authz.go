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

// SSOConfig controls how the ALB/Cognito identity maps to a Role.
type SSOConfig struct {
	// AdminGroup is the Google Workspace / Cognito group whose members get the
	// admin role. When empty, SSO users default to VIEWER (fail closed) unless
	// AllowAllSSO is set. The break-glass internal-secret path is unaffected.
	AdminGroup string
	// AllowAllSSO, when true AND AdminGroup is empty, grants admin to every
	// SSO-authenticated user (single-tier dev convenience). It must be opted
	// into explicitly (DUCKGRES_ADMIN_SSO_ALLOW_ALL=true) so a missing group
	// config in production cannot silently grant admin to everyone.
	AllowAllSSO bool
}

// Identity is the resolved caller for an admin-UI request.
type Identity struct {
	Email  string   `json:"email"`
	Groups []string `json:"groups"`
	Role   Role     `json:"role"`
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
// path and always maps to admin. Otherwise the ALB-injected Cognito JWT is
// decoded into an Identity. Unauthenticated requests are rejected 401.
func AuthMiddleware(tokens TokenSet, sso SSOConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Internal secret (header or login cookie) -> admin.
		if tokens.Valid(requestAdminToken(c)) {
			c.Set(ctxIdentityKey, &Identity{Email: "internal-secret", Role: RoleAdmin, Source: "internal-secret"})
			c.Next()
			return
		}
		// 2. ALB/Cognito SSO identity.
		if id := identityFromOIDC(c, sso); id != nil {
			c.Set(ctxIdentityKey, id)
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
	}
}

// identityFromOIDC decodes the ALB OIDC data JWT (or falls back to the identity
// header) into an Identity with a resolved Role. Returns nil if no SSO identity
// is present.
//
// The JWT signature is NOT verified here: the request only reaches this pod via
// the internal-scheme ALB (which signs and injects the header and strips
// client-supplied copies) over a tailnet-restricted network. Verifying the
// ALB's regional public key by `kid` is a hardening follow-up tracked in the
// design doc; it does not change the role mapping below.
func identityFromOIDC(c *gin.Context, sso SSOConfig) *Identity {
	raw := c.GetHeader(albOIDCDataHeader)
	if raw == "" {
		// Fallback: identity-only header (email/subject), no groups.
		if email := c.GetHeader(albOIDCIdentityHeader); email != "" {
			return resolveRole(&Identity{Email: email, Source: "sso"}, sso)
		}
		return nil
	}
	claims, err := decodeJWTClaims(raw)
	if err != nil {
		slog.Warn("admin: failed to decode ALB OIDC data header", "error", err)
		return nil
	}
	id := &Identity{
		Email:  stringClaim(claims, "email"),
		Groups: groupsClaim(claims),
		Source: "sso",
	}
	if id.Email == "" {
		id.Email = stringClaim(claims, "sub")
	}
	return resolveRole(id, sso)
}

// resolveRole assigns admin/viewer based on group membership.
func resolveRole(id *Identity, sso SSOConfig) *Identity {
	if sso.AdminGroup == "" {
		if sso.AllowAllSSO {
			// Explicit opt-in (DUCKGRES_ADMIN_SSO_ALLOW_ALL): single-tier dev mode.
			id.Role = RoleAdmin
			slog.Warn("admin: DUCKGRES_ADMIN_SSO_GROUP unset and ALLOW_ALL set — granting admin to all SSO users", "email", id.Email)
			return id
		}
		// Fail closed: without a configured admin group, SSO users are viewers.
		// (The internal-secret / break-glass path still grants admin.)
		id.Role = RoleViewer
		slog.Warn("admin: DUCKGRES_ADMIN_SSO_GROUP unset — SSO user defaults to viewer (set the group, or DUCKGRES_ADMIN_SSO_ALLOW_ALL for dev)", "email", id.Email)
		return id
	}
	id.Role = RoleViewer
	for _, g := range id.Groups {
		if strings.EqualFold(strings.TrimSpace(g), sso.AdminGroup) {
			id.Role = RoleAdmin
			break
		}
	}
	return id
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

// groupsClaim extracts group membership from the common claim shapes Cognito /
// Google emit: a JSON array, or a space/comma-separated string. Checks several
// claim names.
func groupsClaim(claims map[string]any) []string {
	for _, key := range []string{"cognito:groups", "custom:groups", "groups"} {
		v, ok := claims[key]
		if !ok {
			continue
		}
		switch t := v.(type) {
		case []any:
			out := make([]string, 0, len(t))
			for _, g := range t {
				if s, ok := g.(string); ok {
					out = append(out, s)
				}
			}
			if len(out) > 0 {
				return out
			}
		case string:
			return strings.FieldsFunc(t, func(r rune) bool { return r == ' ' || r == ',' })
		}
	}
	return nil
}
