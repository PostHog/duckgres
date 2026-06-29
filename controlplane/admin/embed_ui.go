//go:build kubernetes

package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// uiDistFS holds the built React SPA. The real Vite bundle is committed to
// controlplane/admin/ui/dist and embedded here, so `go build` works without a
// node toolchain — but it must be regenerated (`just ui-build`) and re-committed
// when the UI changes. The Dockerfile / Dockerfile.controlplane node stages
// rebuild it before `go build` so the shipped image is never stale. `all:`
// includes Vite's hashed asset files.
//
//go:embed all:ui/dist
var uiDistFS embed.FS

// RegisterUI serves the embedded SPA on the engine: real files (index.html,
// /assets/*) are served directly; any other non-API GET falls back to
// index.html so client-side routing works. The bundle carries no secrets, so it
// is served unauthenticated — every data call under /api/v1 is auth-gated.
func RegisterUI(engine *gin.Engine) error {
	sub, err := fs.Sub(uiDistFS, "ui/dist")
	if err != nil {
		return err
	}
	index, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return err
	}
	fileServer := http.FileServer(http.FS(sub))

	engine.NoRoute(func(c *gin.Context) {
		p := c.Request.URL.Path
		// Never SPA-fallback an API path — return a JSON 404 instead of HTML.
		if p == "/api" || strings.HasPrefix(p, "/api/") {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		// Serve a real static asset if it exists.
		if p != "/" {
			if f, err := sub.Open(strings.TrimPrefix(p, "/")); err == nil {
				_ = f.Close()
				fileServer.ServeHTTP(c.Writer, c.Request)
				return
			}
		}
		// SPA fallback.
		c.Data(http.StatusOK, "text/html; charset=utf-8", index)
	})
	return nil
}
