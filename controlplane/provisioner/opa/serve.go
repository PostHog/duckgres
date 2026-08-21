package opa

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"sync/atomic"
)

// Bundle is a single immutable snapshot of policy + data that OPA's
// bundle plugin downloads via HTTP GET. The bytes are the gzip'd tarball
// produced by BundleBuilder.BuildBundle; the ETag is content-addressed
// so OPA's `If-None-Match` short-circuits unchanged-poll downloads.
//
// Bundle values are immutable after construction. The byte slice is
// unexported so no caller (including the store) can mutate it post-Set
// and desync the served bytes from the precomputed ETag. Read access
// is via WriteTo (write to an io.Writer) and Len (size in bytes); the
// ETag stays exported because Go strings are immutable by language
// rule, so re-exposing it is safe.
type Bundle struct {
	bytes []byte
	ETag  string
}

// NewBundle wraps a freshly built bundle in a Bundle, computing a strong
// ETag from the SHA-256 of the bytes. The quoting follows RFC 7232 §2.3
// (strong ETag is a quoted string with no W/ prefix), which OPA's bundle
// plugin echoes back verbatim in subsequent If-None-Match headers.
//
// The input slice is copied so the resulting Bundle is independent of
// caller-side mutation. One allocation per bundle update; bundle updates
// are reconcile-tick rare, so the cost is negligible vs. the safety
// gain.
func NewBundle(b []byte) Bundle {
	sum := sha256.Sum256(b)
	cp := make([]byte, len(b))
	copy(cp, b)
	return Bundle{
		bytes: cp,
		ETag:  `"` + hex.EncodeToString(sum[:]) + `"`,
	}
}

// WriteTo writes the bundle bytes to w. Returns the number of bytes
// written and any write error. Implements io.WriterTo so the handler
// can stream into the response writer without copying.
func (b Bundle) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.bytes)
	return int64(n), err
}

// Len returns the size of the bundle in bytes (for Content-Length).
func (b Bundle) Len() int {
	return len(b.bytes)
}

// BundleStore is the concurrent-safe holder the provisioner uses to swap
// in a new Bundle when the configstore changes. Reads via Current are
// lock-free; writes via Set are atomic-pointer swaps. The store is empty
// until the first Set; in that bootstrap window Handler returns 503 so
// OPA's bundle plugin retries on its next poll and Trino keeps running
// against the chart-baked bootstrap deny-all policy.
type BundleStore struct {
	current atomic.Pointer[Bundle]
}

// Set replaces the current bundle with b. Concurrent readers see the new
// bundle on their next Current call without locking. Idempotent: callers
// may Set on every reconcile tick even when b is byte-equal to the
// existing bundle (the HTTP ETag check makes that case cheap on the wire).
func (s *BundleStore) Set(b Bundle) {
	s.current.Store(&b)
}

// Current returns the most recently Set bundle, or (Bundle{}, false) if
// no bundle has been Set yet.
func (s *BundleStore) Current() (Bundle, bool) {
	p := s.current.Load()
	if p == nil {
		return Bundle{}, false
	}
	return *p, true
}

// Handler is the HTTP handler the provisioner exposes for OPA's bundle
// plugin to poll. Configure each customer-Trino OPA sidecar with
//
//	services.<name>.url      = <provisioner base URL>
//	services.<name>.credentials.bearer.token = <shared secret>
//	bundles.trino.service    = <name>
//	bundles.trino.resource   = <path mounted by Handler, e.g. /bundles/trino>
//	bundles.trino.polling.min_delay_seconds = 10
//	bundles.trino.polling.max_delay_seconds = 30
//
// OPA polls the configured URL; on success it activates the bundle
// atomically and bumps `data.trino` + `data.group_catalogs`. On 304 it
// keeps the active bundle. On 503 / network errors it retries with
// exponential backoff while keeping the active bundle in place (so a
// provisioner outage does NOT collapse Trino's existing authorization
// state — OPA serves the last good bundle indefinitely until the
// provisioner is back).
//
// Auth is REQUIRED. The bundle bytes contain `data.group_catalogs`,
// which enumerates every Trino-enabled team and their catalog names --
// effectively the customer roster. Anyone who can reach this endpoint
// must authenticate. Auth is called before any state lookup; return
// false to reject the request with 401. A typical implementation does
// a constant-time comparison of `Authorization: Bearer <token>` against
// a shared secret mounted from the same K8s Secret OPA uses for its
// bundle service credentials (see opa.BearerTokenAuth).
//
// Construct with NewHandler, which panics if Auth is nil. The runtime
// check in ServeHTTP fails closed (deny all) in the same case as
// belt-and-braces defense in depth: even if someone bypasses the
// constructor by literal-initialising Handler{}, the endpoint refuses
// to serve.
type Handler struct {
	Store *BundleStore
	Auth  func(r *http.Request) bool
}

// NewHandler constructs a bundle Handler. Both store and auth are
// required; passing nil for either is a programmer error and panics.
// Tests that aren't exercising the auth path should pass an explicit
// permissive function (e.g. `AllowAllForTest`) so the choice is
// visible at the call site rather than hidden in a default.
func NewHandler(store *BundleStore, auth func(*http.Request) bool) *Handler {
	if store == nil {
		panic("opa: NewHandler requires a non-nil BundleStore")
	}
	if auth == nil {
		panic("opa: NewHandler requires a non-nil Auth -- the bundle exposes the customer roster, unauthenticated access is never correct")
	}
	return &Handler{Store: store, Auth: auth}
}

// BearerTokenAuth returns an Auth function that requires
// `Authorization: Bearer <token>` to match the given token in
// constant time once the header length matches. token must be
// non-empty; passing "" panics to surface a misconfiguration loudly
// rather than serve unauthenticated.
//
// Single-token by design: the bundle bearer token is generated once at
// cluster bootstrap and is stable for the process lifetime. Rotating it
// (with a multi-token grace window so in-flight OPA polls using the
// previous token still validate) is a rotation-API concern and lands
// with that PR — not built speculatively here.
//
// Timing properties: once the Authorization header is the same length
// as the expected `"Bearer " + token` string, the byte comparison is
// constant-time (crypto/subtle.ConstantTimeCompare). The length check
// itself is observable, but for fixed-length random tokens that reveals
// nothing useful.
func BearerTokenAuth(token string) func(*http.Request) bool {
	if token == "" {
		panic("opa: BearerTokenAuth requires a non-empty token")
	}
	expected := []byte("Bearer " + token)
	return func(r *http.Request) bool {
		got := r.Header.Get("Authorization")
		if got == "" {
			return false
		}
		if len(got) != len(expected) {
			// Dummy constant-time compare so the length-mismatch path's
			// wall-clock doesn't degenerate relative to the equal-length
			// path.
			_ = subtle.ConstantTimeCompare(expected, expected)
			return false
		}
		return subtle.ConstantTimeCompare([]byte(got), expected) == 1
	}
}

// ServeHTTP implements the bundle endpoint contract. GET-only (HEAD also
// honoured for liveness pokes by other clients; OPA itself only uses
// GET). Responds with the gzip tarball, ETag header for poll caching,
// and 304 Not Modified when the client's If-None-Match matches the
// current ETag.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Belt-and-braces: NewHandler enforces non-nil Store/Auth, but a
	// literal Handler{Store: ..., Auth: nil} or Handler{Auth: ..., Store: nil}
	// construction bypasses that. Fail closed on either.
	if h.Store == nil {
		http.Error(w, "server misconfigured: no bundle store", http.StatusInternalServerError)
		return
	}
	if h.Auth == nil {
		http.Error(w, "server misconfigured: no auth", http.StatusInternalServerError)
		return
	}
	if !h.Auth(r) {
		// Plain 401, no WWW-Authenticate challenge -- OPA's bundle
		// plugin uses its preconfigured credentials, not a discovery
		// flow, so a challenge would be noise in the logs.
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	b, ok := h.Store.Current()
	if !ok {
		// Provisioner hasn't built the first bundle yet. OPA's bundle
		// plugin treats 503 as a transient retry, and Trino keeps the
		// chart-baked bootstrap deny-all in place. Fail-closed during
		// bootstrap.
		http.Error(w, "bundle not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("ETag", b.ETag)
	if match := r.Header.Get("If-None-Match"); match != "" && match == b.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", itoa(b.Len()))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = b.WriteTo(w)
}

// itoa avoids pulling in strconv for a single call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
