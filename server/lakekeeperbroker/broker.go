// Package lakekeeperbroker is a tiny in-process HTTP server that bridges
// DuckDB's OAuth2 client_credentials expectations to a Kubernetes-projected
// ServiceAccount token. The DuckDB iceberg extension knows how to fetch a
// bearer token via OAuth2 (POST grant_type=client_credentials), but not how
// to load one from disk. The broker handles the disk read and hands the
// token back wrapped in an OAuth2 response shape that DuckDB accepts.
//
// Lifecycle:
//   - The worker starts the broker at boot, before any client session can run.
//   - kubelet refreshes the projected SA token on its own schedule (default
//     well under 1h); each /token request re-reads the file so we always
//     hand DuckDB the current token.
//   - The activation payload's LakekeeperOAuth2ServerURI points at
//     http://127.0.0.1:<broker-port>/token, so the broker is reachable only
//     from inside the worker pod.
//
// Why an in-process broker rather than a sidecar:
//   - One process to lifecycle and observe.
//   - No extra container in the duckling pod spec.
//   - The token file is mounted via a projected volume; the broker is the
//     only consumer.
package lakekeeperbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Broker is an HTTP server that wraps a projected SA token file as an
// OAuth2 client_credentials response.
type Broker struct {
	tokenPath string
	srv       *http.Server
	listener  net.Listener

	// expiresInSeconds is what we return in the OAuth2 response. DuckDB's
	// iceberg extension uses this to decide when to re-fetch. A short
	// value (default 60) keeps DuckDB in sync with kubelet's projected
	// token rotation; a long value reduces /token QPS but risks DuckDB
	// using a stale token after kubelet rotates.
	expiresInSeconds int

	mu      sync.Mutex
	started bool
}

// New builds a Broker that reads its token from tokenPath. The path must
// be readable at request time, not just at construction — the file is
// rotated by kubelet on the projected-token TTL schedule.
func New(tokenPath string) *Broker {
	return &Broker{
		tokenPath:        tokenPath,
		expiresInSeconds: 60,
	}
}

// WithExpiresIn overrides the expires_in (seconds) value in the OAuth2
// response. Default 60. Lower values increase /token QPS but reduce the
// window in which DuckDB could be holding a stale token after kubelet
// rotation.
func (b *Broker) WithExpiresIn(s int) *Broker {
	b.expiresInSeconds = s
	return b
}

// ListenAndServe starts the broker on the given loopback addr (e.g.
// "127.0.0.1:9876"). Returns once the listener is bound; the HTTP server
// runs in a background goroutine. Use Shutdown to stop it cleanly.
//
// Binding loopback-only is intentional — only the local worker process
// should ever hit /token, and exposing the wrapped SA token to any
// non-loopback caller would be a real credential leak.
func (b *Broker) ListenAndServe(addr string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return errors.New("broker already started")
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") && !strings.HasPrefix(addr, "[::1]:") {
		return fmt.Errorf("broker addr %q must bind to loopback; non-loopback would leak the SA token", addr)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("broker listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/token", b.handleToken)
	mux.HandleFunc("/health", b.handleHealth)
	b.listener = ln
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	b.srv = srv
	b.started = true
	// Capture srv in the closure rather than b.srv — Shutdown nil-clears
	// the struct field, so a goroutine that reads b.srv directly can
	// race with Shutdown and panic.
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "lakekeeper broker: serve: %v\n", err)
		}
	}()
	return nil
}

// Addr returns the bound address, useful when the caller passed a :0 port
// and wants to know what was picked.
func (b *Broker) Addr() string {
	if b.listener == nil {
		return ""
	}
	return b.listener.Addr().String()
}

// Shutdown stops the broker gracefully. Safe to call multiple times.
func (b *Broker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	srv := b.srv
	b.srv = nil
	b.started = false
	b.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// tokenResponse is the JSON shape DuckDB's iceberg extension expects from
// an OAuth2 client_credentials response.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type errorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

func (b *Broker) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		b.respondError(w, http.StatusMethodNotAllowed, "invalid_request", "POST only")
		return
	}
	// Read the projected SA token. The kubelet refreshes this file on its
	// own schedule; each request gets the freshest copy.
	raw, err := os.ReadFile(b.tokenPath)
	if err != nil {
		// 503 rather than 500 — file-not-yet-mounted is transient.
		b.respondError(w, http.StatusServiceUnavailable, "server_error",
			fmt.Sprintf("read token file: %v", err))
		return
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		b.respondError(w, http.StatusServiceUnavailable, "server_error", "token file empty")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(tokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   b.expiresInSeconds,
	})
}

func (b *Broker) handleHealth(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(b.tokenPath); err != nil {
		b.respondError(w, http.StatusServiceUnavailable, "unhealthy",
			fmt.Sprintf("token path %q: %v", b.tokenPath, err))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (b *Broker) respondError(w http.ResponseWriter, status int, code, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code, ErrorDescription: desc})
}

// DefaultPort is the loopback port we bind to in the worker pod. Picked
// to avoid common app ports; mirrored in the activation payload's
// LakekeeperOAuth2ServerURI value.
const DefaultPort = 9876

// DefaultAddr is the loopback address for the broker.
const DefaultAddr = "127.0.0.1:9876"

// DefaultTokenPath is the conventional mount path for the projected SA
// token. The duckling pod spec must mount a projected volume here with
// audience matching Lakekeeper's authentication.kubernetes.audiences.
const DefaultTokenPath = "/var/run/secrets/lakekeeper/token"

// DefaultOAuth2ServerURI is the value written to ManagedWarehouseIceberg.
// LakekeeperOAuth2ServerURI when the provisioner runs in OIDC mode. The
// worker activator ships it to the worker, which uses it as DuckDB's
// OAUTH2_SERVER_URI — the worker's own broker is what DuckDB ends up
// POSTing to.
const DefaultOAuth2ServerURI = "http://127.0.0.1:9876/token"
