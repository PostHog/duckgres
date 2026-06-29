//go:build kubernetes

package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"html/template"
	"net/http"
	"net/url"
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

// RegisterDashboard serves the admin dashboard on the Gin engine.
func RegisterDashboard(r *gin.Engine, tokens TokenSet) {
	// The models explorer is the primary dashboard surface: a sidebar of every
	// config-store model, a table of its rows, and a detail view per row. "/"
	// and "/models" both land here. The legacy per-entity pages stay reachable.
	r.GET("/", dashboardPageHandler("models.html", tokens))
	r.GET("/models", dashboardPageHandler("models.html", tokens))
	r.GET("/orgs", dashboardPageHandler("orgs.html", tokens))
	r.GET("/workers", dashboardPageHandler("workers.html", tokens))
	r.GET("/sessions", dashboardPageHandler("sessions.html", tokens))
	r.POST("/login", loginHandler(tokens))
}

func dashboardPageHandler(templateName string, tokens TokenSet) gin.HandlerFunc {
	return func(c *gin.Context) {
		// The admin token is deliberately NOT accepted via URL query parameters:
		// URL-borne secrets persist in browser history and any future proxy/access
		// logs. Authenticate via the POST /login form or the
		// X-Duckgres-Internal-Secret header instead.
		if !tokens.Valid(requestAdminToken(c)) {
			renderLoginPage(c, loginNextURI(c.Request.URL), "")
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, templateName, nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	}
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
	// HttpOnly + SameSite=Strict; not Secure because the dashboard is served
	// plain-HTTP on a network-policy-restricted port reached via port-forward.
	c.SetSameSite(http.SameSiteStrictMode)
	c.SetCookie(adminTokenCookieName, token, 7*24*60*60, "/", "", false, true)
}

// loginNextURI returns the request URI to round-trip through the login form's
// hidden "next" field, with any ?token= query key (the pre-#721 URL auth flow,
// e.g. a stale bookmark) stripped — the login page must never embed the secret
// or redirect it back into the URL bar / browser history.
func loginNextURI(u *url.URL) string {
	q := u.Query()
	if _, ok := q["token"]; !ok {
		return u.RequestURI()
	}
	q.Del("token")
	cloned := *u
	cloned.RawQuery = q.Encode()
	return cloned.RequestURI()
}

func normalizeNextPath(next string) string {
	if next == "" || !strings.HasPrefix(next, "/") {
		return "/"
	}
	return next
}
