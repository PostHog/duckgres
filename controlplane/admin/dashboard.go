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
		if redirectPath, ok := maybeSetAdminCookieFromQuery(c, adminToken); ok {
			c.Redirect(http.StatusSeeOther, redirectPath)
			return
		}
		if !validAdminToken(requestAdminToken(c), adminToken) {
			renderLoginPage(c, c.Request.URL.RequestURI(), "")
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
	auth := c.GetHeader("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
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
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(adminTokenCookieName, token, 7*24*60*60, "/", "", false, true)
}

func maybeSetAdminCookieFromQuery(c *gin.Context, adminToken string) (string, bool) {
	token := c.Query("token")
	if token == "" || !validAdminToken(token, adminToken) {
		return "", false
	}
	setAdminTokenCookie(c, token)
	q := cloneWithoutToken(c.Request.URL.Query())
	redirectPath := c.Request.URL.Path
	if encoded := q.Encode(); encoded != "" {
		redirectPath += "?" + encoded
	}
	return redirectPath, true
}

func cloneWithoutToken(values url.Values) url.Values {
	cloned := url.Values{}
	for k, v := range values {
		if k == "token" {
			continue
		}
		copied := make([]string, len(v))
		copy(copied, v)
		cloned[k] = copied
	}
	return cloned
}

func normalizeNextPath(next string) string {
	if next == "" || !strings.HasPrefix(next, "/") {
		return "/"
	}
	return next
}
