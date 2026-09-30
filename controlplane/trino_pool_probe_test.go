//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTrinoPoolProbeRejectsForeignContinuation(t *testing.T) {
	foreignCalls := 0
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignCalls++; w.WriteHeader(200) }))
	defer foreign.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": foreign.URL + "/v1/statement/stolen"})
	}))
	defer server.Close()
	client := trinoPoolSQLClient{baseURL: server.URL, client: server.Client(), username: "user", password: "secret"}
	if _, err := client.statement(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("foreign nextUri accepted")
	}
	if foreignCalls != 0 {
		t.Fatal("credentials reached foreign server")
	}
}

func TestTrinoPoolProbeRejectsAdjacentPath(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = io.WriteString(w, `{}`) }))
	defer server.Close()
	client := trinoPoolSQLClient{baseURL: server.URL, client: server.Client(), username: "user", password: "secret"}
	if _, err := client.read(context.Background(), http.MethodGet, server.URL+"/v1/statement-unrelated", ""); err == nil || calls != 0 {
		t.Fatal("adjacent response path accepted")
	}
}

func TestTrinoPoolProbeTransportRejectsRedirectsAndBadResponses(t *testing.T) {
	foreignCalls := 0
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignCalls++; w.WriteHeader(200) }))
	defer foreign.Close()
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		redirect bool
	}{
		{"redirect", 302, "", true},
		{"unauthorized", 401, "private error", false},
		{"bad json", 200, "not-json", false},
		{"SQL error", 200, `{"error":{"message":"private error"}}`, false},
		{"oversize", 200, strings.Repeat("x", (1<<20)+1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.redirect {
					w.Header().Set("Location", foreign.URL+"/v1/statement/stolen")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			hc := newTrinoPoolHTTPClient("")
			hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
			client := trinoPoolSQLClient{baseURL: server.URL, client: hc, username: "user", password: "secret"}
			if _, err := client.statement(context.Background(), "SELECT 1"); err == nil {
				t.Fatal("invalid coordinator response accepted")
			}
		})
	}
	if foreignCalls != 0 {
		t.Fatal("redirect leaked credentials")
	}
}
