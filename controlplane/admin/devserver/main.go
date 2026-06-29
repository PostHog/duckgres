// Command devserver serves the admin dashboard's static assets off disk and
// reverse-proxies the API to a control plane, so the UI can be iterated on
// locally without rebuilding/redeploying the control-plane image.
//
// The browser talks to a single origin (this server): static files come from
// ./static and any /api/, /login, or /health request is proxied to the control
// plane with the internal-secret header injected server-side. Same-origin →
// no CORS, and the secret never reaches page JS. The UI uses relative /api/v1
// paths, so it runs byte-for-byte the same whether embedded or served here.
//
// peepernetes-style usage: one --context drives everything (secret +
// port-forward + banner). Run one per environment:
//
//	go run ./controlplane/admin/devserver --context mw-prod-us-admin --listen 127.0.0.1:5173
//	go run ./controlplane/admin/devserver --context mw-dev-admin     --listen 127.0.0.1:5174
//
// The banner is red when the context name contains "prod". Edit
// controlplane/admin/static/*.html and refresh — no rebuild. Without --context
// it falls back to an external --target + --secret.
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:5173", "address to serve the UI on")
	staticDir := flag.String("static", defaultStaticDir(), "directory of static UI assets to serve off disk")
	kubeContext := flag.String("context", "", "kube context to serve: the devserver fetches the internal secret AND port-forwards the control plane for this context, and labels the banner with it (red when it contains \"prod\"). When set, it drives everything and --target/--secret are ignored.")
	namespace := flag.String("namespace", "duckgres", "kube namespace (with --context)")
	deploy := flag.String("deploy", "duckgres-control-plane", "control-plane deployment to port-forward (with --context)")
	remotePort := flag.Int("remote-port", 8080, "control-plane admin API container port (with --context)")
	target := flag.String("target", "http://127.0.0.1:8080", "CP admin API base URL (used only when --context is empty)")
	secret := flag.String("secret", os.Getenv("DUCKGRES_INTERNAL_SECRET"), "internal secret (used only when --context is empty; defaults to $DUCKGRES_INTERNAL_SECRET)")
	clusterLabel := flag.String("cluster-label", "", "override the banner label (default: --context, or the current kube context)")
	flag.Parse()

	targetStr, secretStr, clusterCtx := *target, *secret, *clusterLabel

	if *kubeContext != "" {
		// One --context drives the secret, the port-forward, and the banner.
		if clusterCtx == "" {
			clusterCtx = *kubeContext
		}
		s, err := kubeSecret(*kubeContext, *namespace, "duckgres-tokens", "internal-secret")
		if err != nil {
			log.Fatalf("fetch internal secret for context %q: %v", *kubeContext, err)
		}
		secretStr = s

		localPort, err := freeLocalPort()
		if err != nil {
			log.Fatalf("pick local port: %v", err)
		}
		pf := startPortForward(*kubeContext, *namespace, *deploy, localPort, *remotePort)
		defer func() { _ = pf.Process.Kill() }()
		// Kill the kubectl child on SIGINT/SIGTERM so we don't leak port-forwards.
		go func() {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch
			_ = pf.Process.Kill()
			os.Exit(0)
		}()
		targetStr = fmt.Sprintf("http://127.0.0.1:%d", localPort)
		if err := waitForListen(localPort, 30*time.Second); err != nil {
			log.Fatalf("port-forward %s/%s (context %q) not ready: %v", *namespace, *deploy, *kubeContext, err)
		}
		log.Printf("context %q → port-forward %s/%s %d:%d", *kubeContext, *namespace, *deploy, localPort, *remotePort)
	} else if clusterCtx == "" {
		clusterCtx = currentKubeContext()
	}

	if secretStr == "" {
		log.Println("WARNING: no internal secret — proxied API requests will be rejected with 401.")
	}
	if clusterCtx != "" {
		log.Printf("banner label %q (prod=%v)", clusterCtx, isProdContext(clusterCtx))
	}

	targetURL, err := url.Parse(targetStr)
	if err != nil {
		log.Fatalf("invalid target %q: %v", targetStr, err)
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
		if secretStr != "" {
			req.Header.Set("X-Duckgres-Internal-Secret", secretStr)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Answer /api/v1/cluster-info locally from the kube context (the deployed
		// CP has no such endpoint — the banner is a dev-only cue). Empty label =>
		// UI hides the banner.
		if r.URL.Path == "/api/v1/cluster-info" {
			serveClusterInfo(w, clusterCtx)
			return
		}
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

// kubeSecret reads a base64 secret value via kubectl and decodes it.
func kubeSecret(kubeContext, ns, name, key string) (string, error) {
	out, err := exec.Command("kubectl", "--context", kubeContext, "-n", ns,
		"get", "secret", name, "-o", "jsonpath={.data."+key+"}").Output()
	if err != nil {
		return "", err
	}
	dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(out)))
	if err != nil {
		return "", fmt.Errorf("decode secret %s/%s: %w", name, key, err)
	}
	return string(dec), nil
}

// startPortForward launches `kubectl port-forward deploy/<deploy> local:remote`
// for the given context and returns the running command.
func startPortForward(kubeContext, ns, deploy string, localPort, remotePort int) *exec.Cmd {
	cmd := exec.Command("kubectl", "--context", kubeContext, "-n", ns, "port-forward",
		"deploy/"+deploy, fmt.Sprintf("%d:%d", localPort, remotePort))
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("start port-forward: %v", err)
	}
	return cmd
}

// freeLocalPort returns an unused localhost TCP port.
func freeLocalPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitForListen blocks until something accepts on the local port (the
// port-forward is up) or the timeout elapses.
func waitForListen(port int, timeout time.Duration) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("nothing listening on %s after %s", addr, timeout)
}

// serveClusterInfo answers /api/v1/cluster-info with the cluster-context label
// (the kube context being served) and whether it denotes prod.
func serveClusterInfo(w http.ResponseWriter, label string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"label": label,
		"prod":  isProdContext(label),
	})
}

// isProdContext reports whether a kube-context name denotes production.
func isProdContext(ctx string) bool {
	return strings.Contains(strings.ToLower(ctx), "prod")
}

// currentKubeContext returns `kubectl config current-context`, or "" if kubectl
// is unavailable / no context is set (banner simply hidden).
func currentKubeContext() string {
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// isProxiedPath reports whether a request should be forwarded to the control
// plane rather than served from the local static dir.
func isProxiedPath(p string) bool {
	return strings.HasPrefix(p, "/api/") || p == "/login" || p == "/health"
}

// dashboardPages maps a request path (route or bare filename) to the static
// asset that serves it. The values are literals, so the filename handed to
// http.ServeFile never derives from the request — no path traversal is possible
// regardless of what the client sends. Routes mirror the CP's
// RegisterDashboard table; "/" and "/models" both land on the models explorer.
var dashboardPages = map[string]string{
	"":            "models.html",
	"models":      "models.html",
	"login":       "login.html",
	"models.html": "models.html",
	"login.html":  "login.html",
}

// serveStatic serves the dashboard's static assets. Any unrecognized path falls
// back to the models explorer (single-page-style entry).
func serveStatic(w http.ResponseWriter, r *http.Request, dir string) {
	file, ok := dashboardPages[strings.TrimPrefix(r.URL.Path, "/")]
	if !ok {
		file = "models.html"
	}
	http.ServeFile(w, r, filepath.Join(dir, file))
}

// defaultStaticDir resolves the static dir relative to the repo layout so the
// server can be run from the repo root with no flags.
func defaultStaticDir() string {
	return filepath.Join("controlplane", "admin", "static")
}
