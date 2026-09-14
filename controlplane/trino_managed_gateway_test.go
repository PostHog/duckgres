//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const managedTestIncarnation = "11111111-1111-4111-8111-111111111111"
const managedTestTargetIncarnation = "22222222-2222-4222-8222-222222222222"

func managedTestRoute() trinoManagedGatewayRoute {
	return trinoManagedGatewayRoute{RoutingGroup: "cell-a", Generation: 3, BackendName: "cell-a-blue", BackendIncarnation: managedTestIncarnation}
}

func managedTestRollout() trinoManagedGatewayRollout {
	return trinoManagedGatewayRollout{RoutingGroup: "cell-a", OperationID: "operation-a", Phase: "CLAIMED", Version: 0,
		Plan: trinoManagedGatewayPlan{PlanHash: strings.Repeat("a", 64), ExpectedRouteGeneration: 3, SourceBackend: "cell-a-blue", SourceIncarnation: managedTestIncarnation, TargetBackend: "cell-a-green"}}
}

func managedTestClient(t *testing.T, handler http.Handler) *trinoManagedGateway {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := newTrinoManagedGateway(server.URL, "", "rollout", strings.Repeat("t", 48))
	if err != nil {
		t.Fatal(err)
	}
	transport := client.client.Transport.(*http.Transport)
	transport.TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	return client
}

func TestManagedGatewayReadsStableAuthenticatedSnapshot(t *testing.T) {
	calls := 0
	client := managedTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		user, password, ok := r.BasicAuth()
		if !ok || user != "rollout" || password != strings.Repeat("t", 48) || r.Header.Get("X-Gateway-Transaction-Admin-Token") != password || r.Method != http.MethodGet {
			t.Error("unexpected request authority")
		}
		if strings.Contains(r.URL.Path, "/routes/") {
			_ = json.NewEncoder(w).Encode(managedTestRoute())
		} else {
			_ = json.NewEncoder(w).Encode(managedTestRollout())
		}
	}))
	got, err := client.Observe(context.Background(), "cell-a")
	if err != nil || got == nil || got.Route != managedTestRoute() || got.Rollout == nil || *got.Rollout != managedTestRollout() || calls != 4 {
		t.Fatalf("stable observation failed: result=%+v calls=%d err=%v", got, calls, err)
	}
	if client.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("private Gateway path must explicitly opt out of the proxy")
	}
}

func TestManagedGatewayRejectsUnstableAndInvalidSnapshots(t *testing.T) {
	for _, mode := range []string{"route_changed", "operation_changed", "operation_appeared", "negative_version", "foreign_group", "invalid_uuid", "null", "trailing", "oversize", "redirect", "http_error"} {
		t.Run(mode, func(t *testing.T) {
			routes, operations := 0, 0
			client := managedTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "null":
					_, _ = w.Write([]byte("null"))
					return
				case "trailing":
					_, _ = w.Write([]byte(`{} {}`))
					return
				case "oversize":
					_, _ = w.Write([]byte(strings.Repeat(" ", (1<<20)+1)))
					return
				case "redirect":
					http.Redirect(w, r, "https://untrusted.example.test", http.StatusFound)
					return
				case "http_error":
					http.Error(w, "sensitive upstream body", http.StatusInternalServerError)
					return
				}
				if strings.Contains(r.URL.Path, "/routes/") {
					routes++
					route := managedTestRoute()
					if mode == "route_changed" && routes > 1 {
						route.Generation++
					}
					if mode == "foreign_group" {
						route.RoutingGroup = "cell-b"
					}
					if mode == "invalid_uuid" {
						route.BackendIncarnation = "invalid"
					}
					_ = json.NewEncoder(w).Encode(route)
				} else {
					operations++
					if mode == "operation_appeared" && operations == 1 {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					op := managedTestRollout()
					if mode == "negative_version" {
						op.Version = -1
					}
					if mode == "operation_changed" && operations > 1 {
						op.Version++
					}
					_ = json.NewEncoder(w).Encode(op)
				}
			}))
			if _, err := client.Observe(context.Background(), "cell-a"); err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("invalid snapshot accepted or error leaked details: %v", err)
			}
		})
	}
}

func TestManagedGatewayAllowsNoOperationAndReadsBackendProcess(t *testing.T) {
	client := managedTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/routes/"):
			_ = json.NewEncoder(w).Encode(managedTestRoute())
		case strings.Contains(r.URL.Path, "/rollouts/"):
			w.WriteHeader(http.StatusNotFound)
		default:
			_, _ = w.Write([]byte(`{"name":"cell-a-blue","incarnation":"` + managedTestIncarnation + `","state":"ACTIVE","nodeId":"node-a","coordinatorId":"process-a","generation":4,"activeQueries":2}`))
		}
	}))
	got, err := client.Observe(context.Background(), "cell-a")
	if err != nil || got.Rollout != nil {
		t.Fatalf("missing operation must be allowed for ordinary reads: %v", err)
	}
	backend, err := client.Backend(context.Background(), "cell-a-blue")
	if err != nil || backend.NodeID != "node-a" || backend.CoordinatorID != "process-a" {
		t.Fatalf("backend process read failed: %v", err)
	}
}

func TestManagedGatewayRejectsUnsafeConfigurationAndPath(t *testing.T) {
	for _, endpoint := range []string{"http://example.test", "https://user:password@example.test", "https://example.test/path", "https://example.test?token=secret", "https://example.test#fragment"} {
		if _, err := newTrinoManagedGateway(endpoint, "", "rollout", strings.Repeat("t", 48)); err == nil || strings.Contains(err.Error(), "password") {
			t.Fatalf("unsafe URL accepted or leaked: %v", err)
		}
	}
	client := managedTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid path reached server") }))
	for _, name := range []string{"../cell-a", "cell-a/other", "cell a", ""} {
		if _, err := client.Observe(context.Background(), name); err == nil {
			t.Error("invalid group accepted")
		}
		if _, err := client.Backend(context.Background(), name); err == nil {
			t.Error("invalid backend accepted")
		}
	}
}
