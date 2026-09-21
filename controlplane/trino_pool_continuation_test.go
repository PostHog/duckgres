//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner"
)

func TestPoolCandidateConsumesForwardedHTTPSContinuation(t *testing.T) {
	for _, explicitPort := range []bool{false, true} {
		name := "implicit HTTPS port"
		if explicitPort {
			name = "explicit HTTPS port"
		}
		t.Run(name, func(t *testing.T) {
			coordinator := newFakeCoordinator(t)
			pages := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "observer" || password != "test-password" || r.Header.Get("X-Forwarded-Proto") != "https" || r.Header.Get("X-Forwarded-Port") != "443" {
					t.Error("probe lost authentication or forwarded origin headers")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				next := func(path string) {
					host := strings.Split(r.Host, ":")[0]
					if explicitPort {
						host += ":443"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": "https://" + host + path})
				}
				switch r.URL.Path {
				case "/v1/statement":
					next("/v1/statement/queued/query/token/1")
				case "/v1/statement/queued/query/token/1":
					pages++
					next("/v1/statement/executing/query/token/2")
				case "/v1/statement/executing/query/token/2":
					pages++
					_ = json.NewEncoder(w).Encode(map[string]any{"data": coordinator.nodes})
				default:
					coordinator.server.Config.Handler.ServeHTTP(w, r)
				}
			}))
			defer server.Close()
			validation, err := validateTrinoPoolCandidate(context.Background(), server.Client(), server.URL,
				func() (string, string) { return "observer", "test-password" },
				trinoPoolObservation{ReadyWorkers: 2, CoordinatorImage: fakeCoordinatorImage},
				trinoPoolExpectation{Image: fakeCoordinatorImage, CatalogRevision: 42, InternalHTTP: true,
					ProjectionDigest: provisioner.TrinoProjectionDigest(fakePolicyRevision, fakePasswordRevision, fakeGroupRevision)})
			if err != nil {
				t.Fatalf("candidate could not consume its forwarded result pages: %v", err)
			}
			if pages != 2 || validation.ReadyWorkers != 2 {
				t.Fatalf("pages=%d, workers=%d, want two of each", pages, validation.ReadyWorkers)
			}
		})
	}
}

func TestPoolProbeRejectsUntrustedContinuationOrigins(t *testing.T) {
	for _, target := range []string{
		"https://foreign.example.test/v1/statement/result",
		"https://%s:444/v1/statement/result",
		"http://%s:443/v1/statement/result",
		"ftp://%s/v1/statement/result",
		"//%s/v1/statement/result",
		"https://user:secret@%s/v1/statement/result",
		"https://%s/v1/statement/result?secret=value",
		"https://%s/v1/statement/result?",
		"https://%s/v1/statement/result#fragment",
		"https://%s/v1/statement-adjacent",
		"https://%s/v1/info",
		"https://%s/v1/statement",
	} {
		t.Run(target, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				host := strings.Split(r.Host, ":")[0]
				_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": strings.ReplaceAll(target, "%s", host)})
			}))
			defer server.Close()
			client := rolloutSQLClient{baseURL: server.URL, client: server.Client(), username: "observer", password: "test-password", internalHTTP: true}
			_, err := client.statement(context.Background(), "SELECT 1")
			if err == nil || err.Error() != "invalid coordinator response endpoint" || requests != 1 {
				t.Fatalf("unsafe continuation accepted: requests=%d, err=%v", requests, err)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "test-password") || strings.Contains(err.Error(), "example.test") {
				t.Fatalf("failure exposed response-controlled or credential text: %v", err)
			}
		})
	}
}

func TestPoolProbeDoesNotFollowHTTPRedirect(t *testing.T) {
	foreignRequests := 0
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { foreignRequests++ }))
	defer foreign.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/statement" {
			_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": "https://" + strings.Split(r.Host, ":")[0] + "/v1/statement/result"})
			return
		}
		http.Redirect(w, r, foreign.URL+"/v1/statement/result", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client := rolloutSQLClient{baseURL: server.URL, client: newRolloutHTTPClient(""), username: "observer", password: "test-password", internalHTTP: true}
	if _, err := client.statement(context.Background(), "SELECT 1"); err == nil || foreignRequests != 0 {
		t.Fatalf("redirect followed: calls=%d, error=%v", foreignRequests, err)
	}
}

func TestLegacyProbeDoesNotAcceptForwardedHTTPSPort(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": "https://" + strings.Split(r.Host, ":")[0] + "/v1/statement/result"})
	}))
	defer server.Close()
	client := rolloutSQLClient{baseURL: server.URL, client: server.Client(), username: "observer", password: "test-password"}
	_, err := client.statement(context.Background(), "SELECT 1")
	if err == nil || err.Error() != "invalid coordinator response endpoint" || requests != 1 {
		t.Fatalf("legacy probe accepted a different HTTPS port: requests=%d, error=%v", requests, err)
	}
}

func TestPoolNodeInventoryFailureKeepsSafeCause(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": "https://foreign.example.test/v1/statement/result"})
	}))
	defer server.Close()
	client := rolloutSQLClient{baseURL: server.URL, client: server.Client(), username: "observer", password: "test-password", internalHTTP: true}
	_, err := registeredWorkerCount(context.Background(), client, "node-1")
	if !errors.Is(err, errTrinoPoolCandidateNotReady) || !strings.Contains(err.Error(), "invalid coordinator response endpoint") {
		t.Fatalf("inventory failure lost its safe underlying cause: %v", err)
	}
}
