package trinogateway

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func poolBackend() Backend {
	return Backend{
		Name: "pool-001-i-0007", ProxyTo: "https://i-0007.pool.invalid:8443",
		RoutingGroup: "pool-001", Active: false,
	}
}

// Pooled registration reads the endpoint from the Gateway's own backend
// record, so the record has to exist first. Without this the first spawn fails
// with POOL_NOT_FOUND and the prerequisite stays invisible.
func TestEnsureInactiveBackendCreatesTheRegistration(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gateway/backend/all" {
			writeJSON(t, w, http.StatusOK, []Backend{})
			return
		}
		writeJSON(t, w, http.StatusOK, map[string]any{})
	})
	if err := client.EnsureInactiveBackend(context.Background(), poolBackend()); err != nil {
		t.Fatalf("ensure backend: %v", err)
	}
	created := (*captured)[1]
	if created.method != http.MethodPost || created.path != "/gateway/backend/modify/add" {
		t.Fatalf("request = %s %s", created.method, created.path)
	}
	// Creating it active would make it eligible for tenant routing immediately,
	// with no certificate and no admission - exactly what the pooled protocol
	// exists to prevent.
	if created.body["active"] != false {
		t.Fatalf("backend was registered with active=%v", created.body["active"])
	}
	for _, field := range []string{"name", "proxyTo", "routingGroup"} {
		if _, present := created.body[field]; !present {
			t.Errorf("registration is missing %q", field)
		}
	}
}

func TestEnsureInactiveBackendIsIdempotent(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, []Backend{poolBackend()})
	})
	if err := client.EnsureInactiveBackend(context.Background(), poolBackend()); err != nil {
		t.Fatalf("ensure backend: %v", err)
	}
	if len(*captured) != 1 {
		t.Fatalf("an existing registration triggered %d requests", len(*captured))
	}
}

// Adopting somebody else's registration would re-point live routing or hijack
// another cluster's record.
func TestEnsureInactiveBackendRefusesAConflictingRegistration(t *testing.T) {
	cases := map[string]Backend{
		"already active": {Name: "pool-001-i-0007", ProxyTo: "https://i-0007.pool.invalid:8443", RoutingGroup: "pool-001", Active: true},
		"other endpoint": {Name: "pool-001-i-0007", ProxyTo: "https://somebody-else.invalid:8443", RoutingGroup: "pool-001"},
		"other group":    {Name: "pool-001-i-0007", ProxyTo: "https://i-0007.pool.invalid:8443", RoutingGroup: "adhoc"},
	}
	for name, existing := range cases {
		t.Run(name, func(t *testing.T) {
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				writeJSON(t, w, http.StatusOK, []Backend{existing})
			})
			if err := client.EnsureInactiveBackend(context.Background(), poolBackend()); !errors.Is(err, ErrIdentityConflict) {
				t.Fatalf("error = %v, want ErrIdentityConflict", err)
			}
		})
	}
}

func TestEnsureInactiveBackendRefusesToRegisterAnActiveBackend(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, []Backend{})
	})
	active := poolBackend()
	active.Active = true
	if err := client.EnsureInactiveBackend(context.Background(), active); err == nil {
		t.Fatal("an active pooled registration was accepted")
	}
}
