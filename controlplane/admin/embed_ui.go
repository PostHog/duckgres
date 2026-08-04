//go:build kubernetes

package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// uiDistFS holds the built React SPA. The Vite bundle is a BUILD ARTIFACT, not
// committed: ui/dist is gitignored except a .gitkeep that keeps this embed
// target present so `go build` compiles without a node toolchain. The real
// bundle is produced by `just ui-build` locally and by the node stage in
// Dockerfile / Dockerfile.controlplane before `go build`, so a shipped image
// always has the fresh UI. `all:` includes the .gitkeep and Vite's hashed
// asset files.
//
//go:embed all:ui/dist
var uiDistFS embed.FS

// uiNotBuiltPage is served when ui/dist has no real build (only .gitkeep) — a
// bare `go build`/run without the node build step. CI and the Docker images
// always build the bundle, so this only appears in a local backend-only run.
const uiNotBuiltPage = `<!doctype html><html lang="en"><head><meta charset="utf-8">` +
	`<title>Duckgres Admin</title></head>` +
	`<body style="font-family:system-ui;background:#111;color:#ddd;padding:2rem">` +
	`<h1>Duckgres Admin</h1><p>UI bundle not built. Run <code>just ui-build</code> ` +
	`(controlplane/admin/ui), or use the Docker image which builds it.</p></body></html>`

// RegisterUI serves the embedded SPA on the engine: real files (index.html,
// /assets/*) are served directly; any other non-API GET falls back to
// index.html so client-side routing works. The bundle carries no secrets, so it
// is served unauthenticated — every data call under /api/v1 is auth-gated.
// When no real build is embedded (only .gitkeep), a graceful "not built" notice
// is served so the control plane still starts.
func RegisterUI(engine *gin.Engine) error {
	sub, err := fs.Sub(uiDistFS, "ui/dist")
	if err != nil {
		return err
	}
	index, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		index = []byte(uiNotBuiltPage)
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
