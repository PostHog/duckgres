package opa

import (
	"bytes"
	"context"
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
	uc := UserCatalogs{
		"42":           {"org_42_iceberg": true},
		"43":           {"org_43_iceberg": true},
		AdminPrincipal: {"org_42_iceberg": true, "org_43_iceberg": true},
	}

	raw, err := NewBuilder().BuildBundle(uc)
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

	// data.user_catalogs must round-trip cleanly.
	userCatalogs, ok := parsed.Data["user_catalogs"].(map[string]interface{})
	if !ok {
		t.Fatalf("data.user_catalogs is not a map: %T", parsed.Data["user_catalogs"])
	}
	if _, ok := userCatalogs["42"]; !ok {
		t.Error("user_catalogs missing entry for user 42")
	}
	owned, ok := userCatalogs["42"].(map[string]interface{})
	if !ok {
		t.Fatalf("user_catalogs[42] is not a map: %T", userCatalogs["42"])
	}
	if owned["org_42_iceberg"] != true {
		t.Errorf("user_catalogs[42][org_42_iceberg]: want true, got %v", owned["org_42_iceberg"])
	}
}

// TestBuildBundleEmptyInput verifies that a nil / empty UserCatalogs still
// produces a well-formed bundle. The bootstrap state (before the provisioner
// has published its first real bundle) must be deny-everything, which is
// what an empty user_catalogs gives us.
func TestBuildBundleEmptyInput(t *testing.T) {
	for _, uc := range []UserCatalogs{nil, {}} {
		raw, err := NewBuilder().BuildBundle(uc)
		if err != nil {
			t.Fatalf("BuildBundle(empty): %v", err)
		}
		parsed, err := bundle.NewReader(bytes.NewReader(raw)).Read()
		if err != nil {
			t.Fatalf("bundle.Read: %v", err)
		}
		got, ok := parsed.Data["user_catalogs"].(map[string]interface{})
		if !ok {
			t.Fatalf("user_catalogs not a map: %T", parsed.Data["user_catalogs"])
		}
		if len(got) != 0 {
			t.Errorf("user_catalogs from empty input should be empty, got %v", got)
		}
	}
}

// TestPushBundle drives PushBundle against an httptest server, then
// asserts on the request the OPA sidecar would have seen.
func TestPushBundle(t *testing.T) {
	uc := UserCatalogs{"42": {"org_42_iceberg": true}}
	raw, err := NewBuilder().BuildBundle(uc)
	if err != nil {
		t.Fatalf("BuildBundle: %v", err)
	}

	var seenBody []byte
	var seenAuth string
	var seenContentType string
	var seenMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		seenAuth = r.Header.Get("Authorization")
		seenContentType = r.Header.Get("Content-Type")
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		seenBody = buf.Bytes()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	endpoint := server.URL + "/v1/bundles/trino"
	if err := PushBundle(context.Background(), endpoint, raw,
		WithHTTPClient(server.Client()),
		WithBearerToken("hunter2"),
	); err != nil {
		t.Fatalf("PushBundle: %v", err)
	}

	if seenMethod != http.MethodPut {
		t.Errorf("method: want PUT, got %q", seenMethod)
	}
	if seenContentType != "application/gzip" {
		t.Errorf("content-type: want application/gzip, got %q", seenContentType)
	}
	if seenAuth != "Bearer hunter2" {
		t.Errorf("auth header: want Bearer hunter2, got %q", seenAuth)
	}
	if !bytes.Equal(seenBody, raw) {
		t.Errorf("body bytes mismatch (got %d, want %d)", len(seenBody), len(raw))
	}
}

// TestPushBundleNon2xx propagates server errors as Go errors with the body
// included for postmortem.
func TestPushBundleNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("policy compile failed: foo"))
	}))
	defer server.Close()

	err := PushBundle(context.Background(), server.URL, []byte("not actually a bundle"),
		WithHTTPClient(server.Client()),
	)
	if err == nil {
		t.Fatal("PushBundle should fail on 400")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status 400, got: %v", err)
	}
	if !strings.Contains(err.Error(), "policy compile failed") {
		t.Errorf("error should include response body, got: %v", err)
	}
}

// TestPushBundleEmptyArgs guards against accidentally pushing zero bytes
// or to an empty URL.
func TestPushBundleEmptyArgs(t *testing.T) {
	if err := PushBundle(context.Background(), "", []byte{0x1}); err == nil {
		t.Error("PushBundle should reject empty endpoint")
	}
	if err := PushBundle(context.Background(), "http://example/", nil); err == nil {
		t.Error("PushBundle should reject empty bundle")
	}
}
