//go:build kubernetes

package admin

import (
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

func APIAuthMiddleware(adminToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !validAdminToken(requestAdminToken(c), adminToken) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid admin token"})
			return
		}
		c.Next()
	}
}

// RegisterDashboard serves the admin dashboard on the Gin engine.
func RegisterDashboard(r *gin.Engine, adminToken string) {
	r.GET("/", dashboardPageHandler("index.html", adminToken))
	r.GET("/orgs", dashboardPageHandler("orgs.html", adminToken))
	r.GET("/workers", dashboardPageHandler("workers.html", adminToken))
	r.GET("/sessions", dashboardPageHandler("sessions.html", adminToken))
	r.GET("/settings", dashboardPageHandler("settings.html", adminToken))
	r.POST("/login", loginHandler(adminToken))
}

func dashboardPageHandler(templateName, adminToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// The admin token is deliberately NOT accepted via URL query parameters:
		// URL-borne secrets persist in browser history and any future proxy/access
		// logs. Authenticate via the POST /login form or the
		// X-Duckgres-Internal-Secret header instead.
		if !validAdminToken(requestAdminToken(c), adminToken) {
			renderLoginPage(c, loginNextURI(c.Request.URL), "")
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, templateName, nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	}
}

func loginHandler(adminToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		next := normalizeNextPath(c.PostForm("next"))
		token := c.PostForm("token")
		if !validAdminToken(token, adminToken) {
			renderLoginPage(c, next, "Invalid admin token")
			return
		}
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

func validAdminToken(got, want string) bool {
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
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
