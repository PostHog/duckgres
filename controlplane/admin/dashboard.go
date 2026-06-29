//go:build kubernetes

package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"html/template"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

//go:embed static/*
var staticFS embed.FS

var dashboardTmpl *template.Template

const adminTokenCookieName = "duckgres_admin_token"

func init() {
	dashboardTmpl = template.Must(template.ParseFS(staticFS, "static/*.html"))
}

// TokenSet is the set of currently accepted admin bearer tokens: the primary
// internal secret plus any still-trusted previous secrets (rotation
// fallbacks). Mirrors posthog's SECRET_KEY + SECRET_KEY_FALLBACKS convention:
// clients always send the primary; the server accepts any member of the set,
// so a secret rotation is a phased roll with no auth-mismatch window.
//
// Tokens are stored and compared as SHA-256 digests: tokens are arbitrary
// operator-supplied strings, and subtle.ConstantTimeCompare short-circuits on
// a length mismatch, so comparing raw values would leak token lengths.
// Fixed-length digests keep every comparison the same shape regardless of
// the candidate, the provided value, or which (if any) token matches.
type TokenSet struct {
	digests [][sha256.Size]byte
}

// NewTokenSet builds a TokenSet from the primary secret and rotation
// fallbacks, dropping empty strings so an unset slot can never validate.
func NewTokenSet(primary string, fallbacks []string) TokenSet {
	digests := make([][sha256.Size]byte, 0, 1+len(fallbacks))
	for _, t := range append([]string{primary}, fallbacks...) {
		if t != "" {
			digests = append(digests, sha256.Sum256([]byte(t)))
		}
	}
	return TokenSet{digests: digests}
}

// Valid reports whether got matches any accepted token. Every candidate
// digest is compared with subtle.ConstantTimeCompare and there is no early
// exit on a match.
func (ts TokenSet) Valid(got string) bool {
	if got == "" {
		return false
	}
	gotSum := sha256.Sum256([]byte(got))
	match := 0
	for i := range ts.digests {
		match |= subtle.ConstantTimeCompare(gotSum[:], ts.digests[i][:])
	}
	return match == 1
}

// Count returns the number of accepted tokens (for operability logging only —
// never log the values themselves).
func (ts TokenSet) Count() int {
	return len(ts.digests)
}

func APIAuthMiddleware(tokens TokenSet) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !tokens.Valid(requestAdminToken(c)) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid admin token"})
			return
		}
		c.Next()
	}
}

// RegisterLogin wires the break-glass internal-secret login on the Gin engine.
// The React SPA (served by RegisterUI) owns "/" and all app routes; the primary
// auth path in production is ALB/Cognito SSO. This cookie login remains as a
// service-to-service / break-glass path (e.g. local port-forward or an env
// without SSO wired): POST /login sets the admin-token cookie, which
// AuthMiddleware accepts and maps to the admin role.
func RegisterLogin(r *gin.Engine, tokens TokenSet) {
	r.GET("/login", func(c *gin.Context) {
		// The admin token is deliberately NOT accepted via URL query parameters:
		// URL-borne secrets persist in browser history and any future proxy/access
		// logs. Submit the POST /login form or send the X-Duckgres-Internal-Secret
		// header instead.
		renderLoginPage(c, normalizeNextPath(c.Query("next")), "")
	})
	r.POST("/login", loginHandler(tokens))
}

func loginHandler(tokens TokenSet) gin.HandlerFunc {
	return func(c *gin.Context) {
		next := normalizeNextPath(c.PostForm("next"))
		token := c.PostForm("token")
		if !tokens.Valid(token) {
			renderLoginPage(c, next, "Invalid admin token")
			return
		}
		// The cookie stores the token the user provided. During a rotation a
		// cookie minted from a fallback secret keeps validating until the
		// fallback is dropped (re-login required then).
		setAdminTokenCookie(c, token)
		c.Redirect(http.StatusSeeOther, next)
	}
}

func renderLoginPage(c *gin.Context, next, errMsg string) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusUnauthorized)
	data := struct {
		Next  string
		Error string
	}{
		Next:  normalizeNextPath(next),
		Error: errMsg,
	}
	if err := dashboardTmpl.ExecuteTemplate(c.Writer, "login.html", data); err != nil {
		c.String(http.StatusInternalServerError, "template error: %v", err)
	}
}

func requestAdminToken(c *gin.Context) string {
	// Primary: X-Duckgres-Internal-Secret header (service-to-service)
	if secret := c.GetHeader("X-Duckgres-Internal-Secret"); secret != "" {
		return secret
	}
	// Fallback: cookie (dashboard UI)
	if cookie, err := c.Cookie(adminTokenCookieName); err == nil {
		return cookie
	}
	return ""
}

func setAdminTokenCookie(c *gin.Context, token string) {
	// HttpOnly + SameSite=Strict. Secure is set when the request arrived over
	// HTTPS (the TLS ALB ingress sets X-Forwarded-Proto=https); a plain-HTTP
	// port-forward leaves it false. Browsers treat http://localhost as a secure
	// context, so port-forward login still works either way.
	c.SetSameSite(http.SameSiteStrictMode)
	secure := c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetCookie(adminTokenCookieName, token, 7*24*60*60, "/", "", secure, true)
}

func normalizeNextPath(next string) string {
	if next == "" || !strings.HasPrefix(next, "/") {
		return "/"
	}
	return next
}
