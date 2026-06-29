// Command devserver serves the admin dashboard's static assets off disk and
// reverse-proxies the API to a port-forwarded control plane, so the UI can be
// iterated on locally without rebuilding/redeploying the control-plane image.
//
// It mirrors the embedded serving contract exactly: the browser talks to a
// single origin (this server), static files are served from ./static, and any
// /api/, /login, or /health request is proxied to the target CP with the
// internal-secret header injected server-side. Because it's same-origin from
// the browser's point of view there is no CORS to configure and the secret
// never reaches the page's JavaScript — the UI uses relative /api/v1 paths and
// works byte-for-byte the same whether embedded or served here.
//
// Usage:
//
//	# 1. port-forward the control plane's admin API (separate terminal):
//	kubectl --context posthog-mw-dev -n <ns> port-forward deploy/duckgres-control-plane 8080:8080
//
//	# 2. run the dev server pointing at it:
//	DUCKGRES_INTERNAL_SECRET=<secret> go run ./controlplane/admin/devserver
//	# then open http://127.0.0.1:5173
//
// Edit controlplane/admin/static/*.html and just refresh the browser — no
// rebuild. Once happy, the same files are what //go:embed bakes into the CP.
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:5173", "address to serve the UI on")
	target := flag.String("target", "http://127.0.0.1:8080", "base URL of the port-forwarded control plane admin API")
	staticDir := flag.String("static", defaultStaticDir(), "directory of static UI assets to serve off disk")
	secret := flag.String("secret", os.Getenv("DUCKGRES_INTERNAL_SECRET"), "internal secret injected as X-Duckgres-Internal-Secret on proxied requests (defaults to $DUCKGRES_INTERNAL_SECRET)")
	flag.Parse()

	if *secret == "" {
		log.Println("WARNING: no internal secret set — proxied API requests will be rejected with 401. Set --secret or $DUCKGRES_INTERNAL_SECRET.")
	}

	targetURL, err := url.Parse(*target)
	if err != nil {
		log.Fatalf("invalid --target %q: %v", *target, err)
	}

	absStatic, err := filepath.Abs(*staticDir)
	if err != nil {
		log.Fatalf("resolve --static %q: %v", *staticDir, err)
	}
	if _, err := os.Stat(filepath.Join(absStatic, "models.html")); err != nil {
		log.Fatalf("static dir %q has no models.html (run from repo root, or pass --static): %v", absStatic, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	baseDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		baseDirector(req)
		req.Host = targetURL.Host
		// Inject the service-to-service secret server-side; never trust or
		// forward a browser cookie/credential through the dev proxy.
		req.Header.Del("Cookie")
		if *secret != "" {
			req.Header.Set("X-Duckgres-Internal-Secret", *secret)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if isProxiedPath(r.URL.Path) {
			proxy.ServeHTTP(w, r)
			return
		}
		serveStatic(w, r, absStatic)
	})

	log.Printf("duckgres admin UI dev server: http://%s  →  proxying /api,/login,/health to %s", *listen, targetURL)
	log.Printf("serving static assets from %s (edit + refresh, no rebuild)", absStatic)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
}

// isProxiedPath reports whether a request should be forwarded to the control
// plane rather than served from the local static dir.
func isProxiedPath(p string) bool {
	return strings.HasPrefix(p, "/api/") || p == "/login" || p == "/health"
}

// serveStatic serves UI assets, routing "/" and "/models" to the models
// explorer page (matching the CP's RegisterDashboard route table).
func serveStatic(w http.ResponseWriter, r *http.Request, dir string) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	switch name {
	case "", "models":
		name = "models.html"
	case "orgs", "workers", "sessions", "settings":
		name = name + ".html"
	}
	// Constrain to the static dir; reject path traversal.
	clean := filepath.Clean(filepath.Join(dir, name))
	if !strings.HasPrefix(clean, dir+string(os.PathSeparator)) && clean != dir {
		http.NotFound(w, r)
		return
	}
	if info, err := os.Stat(clean); err != nil || info.IsDir() {
		http.ServeFile(w, r, filepath.Join(dir, "models.html"))
		return
	}
	http.ServeFile(w, r, clean)
}

// defaultStaticDir resolves the static dir relative to the repo layout so the
// server can be run from the repo root with no flags.
func defaultStaticDir() string {
	return filepath.Join("controlplane", "admin", "static")
}
