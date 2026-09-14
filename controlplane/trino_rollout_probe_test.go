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
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestTrinoRolloutReadinessProbeAuthenticatesAndConsumesMetadata(t *testing.T) {
	var server *httptest.Server
	canaryCalls, pages := 0, 0
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || password != "test-password" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/v1/info":
			_ = json.NewEncoder(w).Encode(map[string]any{"coordinator": true, "starting": false, "nodeId": "node-test", "coordinatorId": "abcde"})
		case "/v1/statement":
			sql, _ := io.ReadAll(r.Body)
			if strings.Contains(string(sql), "system.runtime.nodes") {
				if user != "observer" {
					t.Error("node read did not use observer")
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]any{{"node-test", "https://192.0.2.10:8443", true, "active"}, {"worker-test", "https://192.0.2.11:8443", false, "active"}}})
			} else {
				canaryCalls++
				if user != "canary-test" || string(sql) != `SELECT schema_name FROM "org_canary_test".information_schema.schemata` {
					t.Error("unexpected canary identity or SQL")
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": server.URL + "/v1/statement/result"})
			}
		case "/v1/statement/result":
			pages++
			_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]any{{"information_schema"}}})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	slot := rolloutReadinessSlot{cell: "cell-test", coordinatorURL: server.URL, client: server.Client(), observer: func() (string, string) { return "observer", "test-password" }, canary: rolloutCanaryCredential{Cell: "cell-test", OrgID: "canary-org", Principal: "canary-test", Password: "test-password"}}
	probe := newRolloutProbe(func(context.Context, rolloutCanaryCredential, string) (bool, error) { return true, nil })
	facts, err := probe(context.Background(), slot)
	if err != nil {
		t.Fatal(err)
	}
	if facts.RegisteredWorkers != 1 || facts.NodeID != "node-test" || canaryCalls != 1 || pages != 1 {
		t.Fatalf("incomplete probe: %+v", facts)
	}
	denied := newRolloutProbe(func(context.Context, rolloutCanaryCredential, string) (bool, error) { return false, nil })
	if _, err := denied(context.Background(), slot); err == nil {
		t.Fatal("wrong-cell or disabled canary accepted")
	}
	if canaryCalls != 1 {
		t.Fatal("ineligible canary credentials were sent")
	}
}

func TestTrinoRolloutReadinessProbeRejectsForeignContinuation(t *testing.T) {
	foreignCalls := 0
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignCalls++; w.WriteHeader(200) }))
	defer foreign.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"nextUri": foreign.URL + "/v1/statement/stolen"})
	}))
	defer server.Close()
	client := rolloutSQLClient{baseURL: server.URL, client: server.Client(), username: "user", password: "secret"}
	if _, err := client.statement(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("foreign nextUri accepted")
	}
	if foreignCalls != 0 {
		t.Fatal("credentials reached foreign server")
	}
}

func TestTrinoRolloutReadinessProbeRejectsMissingMetadata(t *testing.T) {
	for _, response := range []string{`{}`, `{"data":[]}`, `{"data":[[42]]}`, `{"data":[[""]]}`, `{"data":[["schema","extra"]]}`} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/info" {
					_, _ = io.WriteString(w, `{"coordinator":true,"starting":false,"nodeId":"node-test","coordinatorId":"abcde"}`)
					return
				}
				user, _, _ := r.BasicAuth()
				if user == "observer" {
					_, _ = io.WriteString(w, `{"data":[["node-test","https://192.0.2.10:8443",true,"active"],["worker-test","https://192.0.2.11:8443",false,"active"]]}`)
					return
				}
				_, _ = io.WriteString(w, response)
			}))
			defer server.Close()
			slot := rolloutReadinessSlot{cell: "cell-test", coordinatorURL: server.URL, client: server.Client(), observer: func() (string, string) { return "observer", "test-password" }, canary: rolloutCanaryCredential{Principal: "canary-test", Password: "test-password"}}
			probe := newRolloutProbe(func(context.Context, rolloutCanaryCredential, string) (bool, error) { return true, nil })
			if _, err := probe(context.Background(), slot); err == nil {
				t.Fatal("missing catalog metadata accepted")
			}
		})
	}
}

func TestTrinoRolloutReadinessProbeRejectsAdjacentPath(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = io.WriteString(w, `{}`) }))
	defer server.Close()
	client := rolloutSQLClient{baseURL: server.URL, client: server.Client(), username: "user", password: "secret"}
	if _, err := client.read(context.Background(), http.MethodGet, server.URL+"/v1/statement-unrelated", ""); err == nil || calls != 0 {
		t.Fatal("adjacent response path accepted")
	}
}

func TestTrinoRolloutReadinessRejectsCoordinatorFromOtherSlot(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/info" {
			_, _ = io.WriteString(w, `{"coordinator":true,"starting":false,"nodeId":"other-node","coordinatorId":"abcde"}`)
			return
		}
		user, _, _ := r.BasicAuth()
		if user == "observer" {
			_, _ = io.WriteString(w, `{"data":[["other-node","https://192.0.2.20:8443",true,"active"],["other-worker","https://192.0.2.21:8443",false,"active"]]}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[["information_schema"]]}`)
	}))
	defer server.Close()
	coordinator, worker := rolloutTestPod("coordinator", "coordinator"), rolloutTestPod("worker", "worker")
	coordinator.Status.PodIP, worker.Status.PodIP = "192.0.2.10", "192.0.2.11"
	slot := rolloutReadinessSlot{cell: "cell-test", color: "green", namespace: "test", coordinatorURL: server.URL, client: server.Client(), observer: func() (string, string) { return "observer", "test-password" }, canary: rolloutCanaryCredential{Principal: "canary-test", Password: "test-password"}}
	h := &trinoRolloutReadinessHandler{token: "test-capability", kube: fake.NewSimpleClientset(&coordinator, &worker), slots: map[string]rolloutReadinessSlot{"cell-test/green": slot}, limit: make(chan struct{}, 1), timeout: time.Second, probe: newRolloutProbe(func(context.Context, rolloutCanaryCredential, string) (bool, error) { return true, nil })}
	r := httptest.NewRequest(http.MethodGet, rolloutReadinessPrefix+"cell-test/green", nil)
	r.Header.Set(rolloutCapabilityHeader, h.token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("healthy other-slot coordinator certified the requested slot")
	}
}

func TestTrinoRolloutReadinessTransportRejectsRedirectsAndBadResponses(t *testing.T) {
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
			hc := newRolloutHTTPClient("")
			hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
			client := rolloutSQLClient{baseURL: server.URL, client: hc, username: "user", password: "secret"}
			if _, err := client.statement(context.Background(), "SELECT 1"); err == nil {
				t.Fatal("invalid coordinator response accepted")
			}
		})
	}
	if foreignCalls != 0 {
		t.Fatal("redirect leaked credentials")
	}
}
