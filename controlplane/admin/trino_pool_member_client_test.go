//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A pool member serves plain HTTP behind the Gateway's TLS. Every request,
// including a statement continuation, must declare that forwarded hop, and
// the HTTPS nextUri the coordinator hands back must be followed on the plain
// Service rather than dialled as https://<host>:443.
func TestTrinoPoolMemberClientDeclaresForwardedHTTPSAndRewritesContinuations(t *testing.T) {
	var sawPaths []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPaths = append(sawPaths, r.Method+" "+r.URL.Path)
		if r.Header.Get("X-Forwarded-Proto") != "https" || r.Header.Get("X-Forwarded-Port") != "443" {
			t.Errorf("%s %s: missing forwarded HTTPS headers", r.Method, r.URL.Path)
			http.Error(w, "insecure", http.StatusForbidden)
			return
		}
		host := strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
			_ = json.NewEncoder(w).Encode(map[string]any{
				// What a coordinator with process-forwarded=true returns.
				"nextUri": "https://" + host + ":443/v1/statement/executing/q1/abc/1",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statement/executing/q1/abc/1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]any{{"node-1", "http://10.0.0.1:8080", "471", true, "active"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := NewTrinoPoolMemberClient(srv.URL, func() (string, string) { return trinoObserverUser, "pw" }).(*trinoCoordinatorHTTPClient)
	rows, err := client.runStatement(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("runStatement: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %v, want the continuation page's one row", rows)
	}
	want := []string{"POST /v1/statement", "GET /v1/statement/executing/q1/abc/1"}
	if strings.Join(sawPaths, ",") != strings.Join(want, ",") {
		t.Fatalf("requests = %v, want %v", sawPaths, want)
	}
}

// A continuation that leaves the member (another host, or a non-statement
// path) is refused rather than followed with the observer's credentials.
func TestTrinoPoolMemberClientRefusesForeignContinuation(t *testing.T) {
	client := NewTrinoPoolMemberClient("http://cell-1.trino-cells.svc.cluster.local:8080", func() (string, string) { return "u", "p" }).(*trinoCoordinatorHTTPClient)
	for _, next := range []string{
		"https://elsewhere.example:443/v1/statement/executing/q/1",
		"https://cell-1.trino-cells.svc.cluster.local:443/v1/query/q",
		"https://user@cell-1.trino-cells.svc.cluster.local:443/v1/statement/executing/q/1",
	} {
		if _, err := client.continuationURL(next); err == nil {
			t.Errorf("continuationURL(%q) accepted a continuation outside the member", next)
		}
	}
	got, err := client.continuationURL("https://cell-1.trino-cells.svc.cluster.local:443/v1/statement/executing/q/1?x=1")
	if err != nil || got != "http://cell-1.trino-cells.svc.cluster.local:8080/v1/statement/executing/q/1?x=1" {
		t.Fatalf("continuationURL = %q, %v", got, err)
	}
}

// The fixed-coordinator client is unchanged: no forwarded headers, and the
// nextUri is followed verbatim.
func TestTrinoCoordinatorClientSendsNoForwardedHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forwarded-Proto") != "" {
			t.Errorf("fixed coordinator client sent X-Forwarded-Proto")
		}
		_, _ = w.Write([]byte(`{"nodeVersion":{"version":"471"},"environment":"e","coordinator":true}`))
	}))
	defer srv.Close()
	client := newTrinoCoordinatorClient(srv.URL, "pw", "")
	if _, err := client.ServerInfo(context.Background()); err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if next, _ := client.continuationURL("https://x:443/v1/statement/1"); next != "https://x:443/v1/statement/1" {
		t.Fatalf("fixed client rewrote a continuation: %q", next)
	}
}
