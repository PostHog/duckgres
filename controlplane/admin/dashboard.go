//go:build kubernetes

package admin

import (
	"embed"
	"html/template"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed static/*
var staticFS embed.FS

var dashboardTmpl *template.Template

func init() {
	dashboardTmpl = template.Must(template.ParseFS(staticFS, "static/*.html"))
}

// RegisterDashboard serves the admin dashboard on the Gin engine.
func RegisterDashboard(r *gin.Engine) {
	r.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, "index.html", nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	})
	r.GET("/teams", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, "teams.html", nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	})
	r.GET("/workers", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, "workers.html", nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	})
	r.GET("/sessions", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, "sessions.html", nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	})
	r.GET("/settings", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.ExecuteTemplate(c.Writer, "settings.html", nil); err != nil {
			c.String(http.StatusInternalServerError, "template error: %v", err)
		}
	})
}
