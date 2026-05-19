package lakekeeperbroker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTempToken(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	if err := os.WriteFile(p, []byte(contents), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return p
}

func startBroker(t *testing.T, tokenPath string) *Broker {
	t.Helper()
	b := New(tokenPath)
	if err := b.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})
	return b
}

func TestTokenEndpointReturnsFileContents(t *testing.T) {
	const want = "eyJ.fake.jwt"
	p := writeTempToken(t, want+"\n") // trailing newline gets trimmed
	b := startBroker(t, p)

	resp, err := http.PostForm("http://"+b.Addr()+"/token", url.Values{
		"grant_type": {"client_credentials"},
	})
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control = %q, want no-store", cc)
	}
	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if tr.AccessToken != want {
		t.Errorf("access_token = %q, want %q", tr.AccessToken, want)
	}
	if tr.TokenType != "Bearer" {
		t.Errorf("token_type = %q, want Bearer", tr.TokenType)
	}
	if tr.ExpiresIn <= 0 {
		t.Errorf("expires_in = %d, want positive", tr.ExpiresIn)
	}
}

func TestTokenEndpointReReadsFileEachRequest(t *testing.T) {
	// Verifies that kubelet token rotation is picked up — the broker
	// must not cache the token contents in memory.
	p := writeTempToken(t, "first-token")
	b := startBroker(t, p)

	get := func() string {
		resp, err := http.PostForm("http://"+b.Addr()+"/token", nil)
		if err != nil {
			t.Fatalf("POST /token: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		var tr tokenResponse
		if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return tr.AccessToken
	}
	if got := get(); got != "first-token" {
		t.Fatalf("first = %q", got)
	}
	if err := os.WriteFile(p, []byte("second-token"), 0600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if got := get(); got != "second-token" {
		t.Fatalf("second = %q, want second-token (broker should re-read on each request)", got)
	}
}

func TestTokenEndpointGETIsRejected(t *testing.T) {
	p := writeTempToken(t, "abc")
	b := startBroker(t, p)
	resp, err := http.Get("http://" + b.Addr() + "/token")
	if err != nil {
		t.Fatalf("GET /token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestTokenEndpointMissingFileReturns503(t *testing.T) {
	b := startBroker(t, "/nonexistent/path")
	resp, err := http.PostForm("http://"+b.Addr()+"/token", nil)
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestTokenEndpointEmptyFileReturns503(t *testing.T) {
	p := writeTempToken(t, "   \n\t ")
	b := startBroker(t, p)
	resp, err := http.PostForm("http://"+b.Addr()+"/token", nil)
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("empty token file should 503, got %d", resp.StatusCode)
	}
}

func TestHealthEndpoint(t *testing.T) {
	p := writeTempToken(t, "abc")
	b := startBroker(t, p)
	resp, err := http.Get("http://" + b.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHealthEndpointReports503WhenTokenMissing(t *testing.T) {
	b := startBroker(t, "/no/such/file")
	resp, err := http.Get("http://" + b.Addr() + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestRefusesToBindNonLoopback(t *testing.T) {
	b := New("irrelevant")
	if err := b.ListenAndServe("0.0.0.0:0"); err == nil {
		t.Fatal("expected error binding to non-loopback")
	} else if !strings.Contains(err.Error(), "loopback") {
		t.Errorf("error should mention loopback: %v", err)
	}
}

func TestStartTwiceErrors(t *testing.T) {
	p := writeTempToken(t, "abc")
	b := New(p)
	if err := b.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatalf("first ListenAndServe: %v", err)
	}
	t.Cleanup(func() { _ = b.Shutdown(context.Background()) })
	if err := b.ListenAndServe("127.0.0.1:0"); err == nil {
		t.Fatal("expected error on double-start")
	}
}

func TestExpiresInOverride(t *testing.T) {
	p := writeTempToken(t, "abc")
	b := New(p).WithExpiresIn(300)
	if err := b.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatalf("ListenAndServe: %v", err)
	}
	t.Cleanup(func() { _ = b.Shutdown(context.Background()) })
	resp, err := http.PostForm("http://"+b.Addr()+"/token", nil)
	if err != nil {
		t.Fatalf("POST /token: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var tr tokenResponse
	_ = json.NewDecoder(resp.Body).Decode(&tr)
	if tr.ExpiresIn != 300 {
		t.Errorf("expires_in = %d, want 300", tr.ExpiresIn)
	}
}
