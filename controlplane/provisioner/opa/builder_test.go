package opa

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-policy-agent/opa/bundle"
)

// TestBuildBundleRoundTrip builds a bundle, parses it back through OPA's
// bundle reader, and asserts that the round-trip preserves the policy
// source and data document.
func TestBuildBundleRoundTrip(t *testing.T) {
	gc := GroupCatalogs{
		"org_42":   {"org_42_iceberg": true},
		"org_43":   {"org_43_iceberg": true},
		AdminGroup: {"org_42_iceberg": true, "org_43_iceberg": true},
	}

	raw, err := NewBuilder().BuildBundle(gc)
	if err != nil {
		t.Fatalf("BuildBundle: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("BuildBundle returned empty bytes")
	}

	parsed, err := bundle.NewReader(bytes.NewReader(raw)).Read()
	if err != nil {
		t.Fatalf("bundle.Read: %v", err)
	}

	if parsed.Manifest.Revision != bundleRevision {
		t.Errorf("manifest revision: want %q, got %q", bundleRevision, parsed.Manifest.Revision)
	}

	// Policy file must be present and contain the package declaration.
	if len(parsed.Modules) != 1 {
		t.Fatalf("want exactly 1 module in bundle, got %d", len(parsed.Modules))
	}
	if !strings.Contains(string(parsed.Modules[0].Raw), "package trino") {
		t.Errorf("module does not contain `package trino`")
	}

	// data.group_catalogs must round-trip cleanly.
	groupCatalogs, ok := parsed.Data["group_catalogs"].(map[string]interface{})
	if !ok {
		t.Fatalf("data.group_catalogs is not a map: %T", parsed.Data["group_catalogs"])
	}
	if _, ok := groupCatalogs["org_42"]; !ok {
		t.Error("group_catalogs missing entry for group org_42")
	}
	owned, ok := groupCatalogs["org_42"].(map[string]interface{})
	if !ok {
		t.Fatalf("group_catalogs[org_42] is not a map: %T", groupCatalogs["org_42"])
	}
	if owned["org_42_iceberg"] != true {
		t.Errorf("group_catalogs[org_42][org_42_iceberg]: want true, got %v", owned["org_42_iceberg"])
	}
}

// TestBuildBundleEmptyInput verifies that a nil / empty GroupCatalogs still
// produces a well-formed bundle. The bootstrap state (before the provisioner
// has published its first real bundle) must be deny-everything, which is
// what an empty group_catalogs gives us.
func TestBuildBundleEmptyInput(t *testing.T) {
	for _, gc := range []GroupCatalogs{nil, {}} {
		raw, err := NewBuilder().BuildBundle(gc)
		if err != nil {
			t.Fatalf("BuildBundle(empty): %v", err)
		}
		parsed, err := bundle.NewReader(bytes.NewReader(raw)).Read()
		if err != nil {
			t.Fatalf("bundle.Read: %v", err)
		}
		got, ok := parsed.Data["group_catalogs"].(map[string]interface{})
		if !ok {
			t.Fatalf("group_catalogs not a map: %T", parsed.Data["group_catalogs"])
		}
		if len(got) != 0 {
			t.Errorf("group_catalogs from empty input should be empty, got %v", got)
		}
	}
}

// --------------------------------------------------------------------------
// Bundle service (pull-based distribution).
//
// These exercise the contract OPA's bundle plugin relies on:
//   - GET returns 200 + gzip bytes + strong ETag on first poll
//   - GET with matching If-None-Match returns 304 (no body)
//   - GET before the provisioner has Set a bundle returns 503 (transient,
//     OPA retries; Trino keeps its bootstrap deny-all in place)
//   - non-GET/HEAD returns 405
//   - the optional Auth hook rejects requests with 401
// --------------------------------------------------------------------------

func TestBundleStoreAndHandler200(t *testing.T) {
	gc := GroupCatalogs{"org_42": {"org_42_iceberg": true}}
	raw, err := NewBuilder().BuildBundle(gc)
	if err != nil {
		t.Fatalf("BuildBundle: %v", err)
	}

	store := &BundleStore{}
	store.Set(NewBundle(raw))

	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/bundles/trino", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: want 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/gzip" {
		t.Errorf("content-type: want application/gzip, got %q", got)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Error("ETag header missing")
	}
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("ETag should be a quoted strong validator, got %q", etag)
	}
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	if !bytes.Equal(buf.Bytes(), raw) {
		t.Errorf("body bytes mismatch (got %d, want %d)", buf.Len(), len(raw))
	}
}

func TestBundleHandler304OnIfNoneMatch(t *testing.T) {
	gc := GroupCatalogs{"org_42": {"org_42_iceberg": true}}
	raw, _ := NewBuilder().BuildBundle(gc)
	b := NewBundle(raw)
	store := &BundleStore{}
	store.Set(b)

	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("If-None-Match", b.ETag)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("status: want 304, got %d", resp.StatusCode)
	}
	buf := new(bytes.Buffer)
	n, _ := buf.ReadFrom(resp.Body)
	if n != 0 {
		t.Errorf("304 should have empty body, got %d bytes", n)
	}
}

func TestBundleHandler200OnEtagMiss(t *testing.T) {
	gc := GroupCatalogs{"org_42": {"org_42_iceberg": true}}
	raw, _ := NewBuilder().BuildBundle(gc)
	store := &BundleStore{}
	store.Set(NewBundle(raw))

	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("If-None-Match", `"not-the-current-etag"`)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("etag miss: want 200, got %d", resp.StatusCode)
	}
}

func TestBundleHandler503BeforeFirstBundle(t *testing.T) {
	store := &BundleStore{} // no Set yet
	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("bootstrap status: want 503 (so OPA retries and Trino stays on deny-all), got %d", resp.StatusCode)
	}
}

func TestBundleHandlerRejectsNonGET(t *testing.T) {
	store := &BundleStore{}
	raw, _ := NewBuilder().BuildBundle(GroupCatalogs{"org_42": {"org_42_iceberg": true}})
	store.Set(NewBundle(raw))

	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, srv.URL, nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("Do %s: %v", method, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s: want 405, got %d", method, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); !strings.Contains(got, "GET") {
			t.Errorf("%s: Allow header should list GET, got %q", method, got)
		}
	}
}

func TestBundleHandlerBearerTokenAuth(t *testing.T) {
	store := &BundleStore{}
	raw, _ := NewBuilder().BuildBundle(GroupCatalogs{"org_42": {"org_42_iceberg": true}})
	store.Set(NewBundle(raw))

	srv := httptest.NewServer(NewHandler(store, BearerTokenAuth("hunter2")))
	defer srv.Close()

	cases := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic hunter2", http.StatusUnauthorized},
		{"wrong token", "Bearer wrong", http.StatusUnauthorized},
		{"token prefix attack", "Bearer hunter", http.StatusUnauthorized},
		{"token suffix attack", "Bearer hunter2x", http.StatusUnauthorized},
		{"correct token", "Bearer hunter2", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Errorf("status: want %d, got %d", tc.wantStatus, resp.StatusCode)
			}
		})
	}
}

// NewHandler refuses nil dependencies -- the bundle exposes the customer
// roster, so an unauthenticated endpoint is never correct.
func TestNewHandlerRejectsNilArgs(t *testing.T) {
	store := &BundleStore{}
	auth := allowAllForTest

	cases := []struct {
		name string
		fn   func()
	}{
		{"nil store", func() { _ = NewHandler(nil, auth) }},
		{"nil auth", func() { _ = NewHandler(store, nil) }},
		{"both nil", func() { _ = NewHandler(nil, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("NewHandler(%s) should panic", tc.name)
				}
			}()
			tc.fn()
		})
	}
}

// Literal Handler{} construction bypasses NewHandler -- the runtime
// guard in ServeHTTP must still fail closed.
func TestHandlerLiteralWithNilAuthFailsClosed(t *testing.T) {
	store := &BundleStore{}
	raw, _ := NewBuilder().BuildBundle(GroupCatalogs{"org_42": {"org_42_iceberg": true}})
	store.Set(NewBundle(raw))

	srv := httptest.NewServer(&Handler{Store: store}) // no Auth set
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("literal Handler with nil Auth: want 500 (fail-closed), got %d", resp.StatusCode)
	}
}

func TestBearerTokenAuthRejectsEmptyToken(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("BearerTokenAuth(\"\") should panic")
		}
	}()
	_ = BearerTokenAuth("")
}

func TestBundleStoreSetOverwrites(t *testing.T) {
	store := &BundleStore{}

	// First bundle.
	raw1, _ := NewBuilder().BuildBundle(GroupCatalogs{"org_42": {"org_42_iceberg": true}})
	b1 := NewBundle(raw1)
	store.Set(b1)
	got, ok := store.Current()
	if !ok || got.ETag != b1.ETag {
		t.Fatalf("Current after first Set: want ETag %q, got %q (ok=%v)", b1.ETag, got.ETag, ok)
	}

	// Second bundle with different content -> different ETag.
	raw2, _ := NewBuilder().BuildBundle(GroupCatalogs{
		"org_42": {"org_42_iceberg": true},
		"org_43": {"org_43_iceberg": true},
	})
	b2 := NewBundle(raw2)
	if b1.ETag == b2.ETag {
		t.Fatal("bundles with different content should not share an ETag (sha256 collision?)")
	}
	store.Set(b2)
	got, ok = store.Current()
	if !ok || got.ETag != b2.ETag {
		t.Errorf("Current after second Set: want ETag %q, got %q (ok=%v)", b2.ETag, got.ETag, ok)
	}
}

// NewBundle's defensive copy means caller-side mutation of the input
// slice after construction cannot affect the stored bundle.
func TestNewBundleIsolatesInputSlice(t *testing.T) {
	raw := []byte("policy-bytes-v1")
	b := NewBundle(raw)
	originalETag := b.ETag
	// Mutate the caller's slice.
	raw[0] = 0xff
	// Bundle ETag and serialized bytes must be unaffected.
	if b.ETag != originalETag {
		t.Errorf("ETag changed after caller mutated input slice: was %q, now %q", originalETag, b.ETag)
	}
	var got bytes.Buffer
	if _, err := b.WriteTo(&got); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if got.Bytes()[0] == 0xff {
		t.Error("Bundle.bytes aliased caller's input slice; defensive copy failed")
	}
}

// The Bundle's internal byte slice is unexported, so post-construction
// mutation via the returned value from Current() is structurally
// impossible. This test pins the API surface: if anyone re-exports the
// field or adds an accessor that returns a mutable []byte, it must come
// with a fresh threat-model review (the served bytes must not desync
// from the precomputed ETag).
func TestBundleHasNoExportedMutableByteAccess(t *testing.T) {
	// Construct a bundle, store it, mutate everything we can reach.
	store := &BundleStore{}
	raw, _ := NewBuilder().BuildBundle(GroupCatalogs{"org_42": {"org_42_iceberg": true}})
	b := NewBundle(raw)
	store.Set(b)

	// Mutate the local Bundle's accessible state. ETag is a string
	// (immutable by Go's type system); bytes is unexported. The compiler
	// guarantees the only way to ship new bytes is via NewBundle (which
	// copies). If this test ever fails to compile because someone added
	// `b.Bytes = ...` or `b.SetBytes(...)`, *that* is the signal.
	_ = b.ETag // exported but immutable
	_ = b.Len()

	// Serve through the handler and confirm the bytes match raw exactly
	// (i.e., that the store still holds the bundle the caller built).
	srv := httptest.NewServer(NewHandler(store, allowAllForTest))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	var got bytes.Buffer
	_, _ = got.ReadFrom(resp.Body)
	if !bytes.Equal(got.Bytes(), raw) {
		t.Error("served bytes diverged from constructed bytes -- has the immutability invariant changed?")
	}
}

func TestNewBundleETagIsContentAddressed(t *testing.T) {
	// Identical bytes -> identical ETag (content-addressed, not random).
	bytes1 := []byte("hello world")
	b1 := NewBundle(bytes1)
	b2 := NewBundle(append([]byte{}, bytes1...))
	if b1.ETag != b2.ETag {
		t.Errorf("ETag should be content-addressed; got %q vs %q for identical bytes", b1.ETag, b2.ETag)
	}
	// Different bytes -> different ETag.
	b3 := NewBundle([]byte("hello world!"))
	if b1.ETag == b3.ETag {
		t.Errorf("ETag should differ for different bytes; got %q for both", b1.ETag)
	}
	// ETag is RFC 7232 strong-validator shape: a quoted string.
	if !strings.HasPrefix(b1.ETag, `"`) || !strings.HasSuffix(b1.ETag, `"`) {
		t.Errorf("ETag should be a quoted string, got %q", b1.ETag)
	}
}
