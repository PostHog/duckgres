//go:build kubernetes

package controlplane

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

type managedProbeTransport func(*http.Request) (*http.Response, error)

func (f managedProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestManagedReadinessDefaultHTTPSPort(t *testing.T) {
	calls := 0
	client := rolloutSQLClient{baseURL: "https://coordinator.example:443", username: "observer", password: "fixture-password", client: &http.Client{Transport: managedProbeTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
	})}}
	for _, tc := range []struct {
		endpoint string
		valid    bool
	}{{"https://coordinator.example/v1/info", true}, {"https://coordinator.example:443/v1/info", true}, {"https://coordinator.example:8443/v1/info", false}, {"https://other.example/v1/info", false}} {
		_, err := client.read(context.Background(), http.MethodGet, tc.endpoint, "")
		if (err == nil) != tc.valid {
			t.Fatalf("invalid normalized origin result for %s: %v", tc.endpoint, err)
		}
	}
	if calls != 2 {
		t.Fatal("foreign origin reached transport")
	}
}

func TestManagedRegistryModesAreExplicitAndClosed(t *testing.T) {
	for _, mode := range []string{"paused", "gateway-shared", "unknown"} {
		input := strings.Replace(testTrinoRegistryJSON, `"routing_group":"cell-test"`, `"routing_group":"group-test","catalog_management":"`+mode+`"`, 1)
		cells, err := parseTrinoCellRegistry([]byte(input))
		if mode == "unknown" {
			if err == nil {
				t.Fatal("unknown mode accepted")
			}
			continue
		}
		if err != nil || cells[0].CatalogManagement != mode || cells[0].RoutingGroup != "group-test" {
			t.Fatalf("explicit mode lost logical/group identity: %v", err)
		}
		if _, err := parseTrinoCellRegistry([]byte(strings.Replace(input, `"id":"green"`, `"id":"other"`, 1))); err == nil {
			t.Fatal("managed mode accepted non-paired slots")
		}
	}
}

func TestManagedWiringPausedNeedsNoGatewayOrCanaries(t *testing.T) {
	for _, key := range []string{"DUCKGRES_TRINO_MANAGED_GATEWAY_URL", "DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME", "DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", "DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE"} {
		t.Setenv(key, "")
	}
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{ID: "registered:cell-test", PublicID: "cell-test", RoutingGroup: "group-test", CatalogManagement: "paused"}}}
	if handler, err := buildTrinoManagedFleet(fleet, nil, nil); err != nil || handler != nil {
		t.Fatal("paused bridge required future credentials")
	}
	fleet[0].Cell.CatalogManagement = "gateway-shared"
	if _, err := buildTrinoManagedFleet(fleet, nil, nil); err == nil {
		t.Fatal("managed mode silently omitted readiness")
	}
	if _, err := buildTrinoManagedFleet(fleet, nil, &trinoRolloutReadinessHandler{token: strings.Repeat("t", 48)}); err == nil {
		t.Fatal("managed mode silently omitted Gateway credentials")
	}
}

func TestManagedOptionsRejectUnregisteredActiveRoute(t *testing.T) {
	reader := &managedReaderFake{observation: trinoManagedGatewayObservation{Route: trinoManagedGatewayRoute{RoutingGroup: "group-test", BackendName: "other-blue"}}}
	opts := managedCatalogOptions(nil, reader, "group-test", map[string]provisioner.TrinoCatalogClient{}, nil)
	if _, err := opts.Active(context.Background()); err == nil {
		t.Fatal("foreign active backend selected")
	}
}

type managedSelectorCatalog struct{ provisioner.TrinoCatalogClient }

func TestManagedOptionsUseCurrentRouteAndMatchingWarmPlan(t *testing.T) {
	blue, green := &managedSelectorCatalog{}, &managedSelectorCatalog{}
	reader := &managedReaderFake{observation: trinoManagedGatewayObservation{Route: trinoManagedGatewayRoute{RoutingGroup: "group-test", Generation: 7, BackendName: "group-test-blue", BackendIncarnation: "blue-incarnation"}}, backend: trinoManagedGatewayBackend{BackendName: "group-test-blue", State: "ACTIVE", Incarnation: "blue-incarnation"}}
	opts := managedCatalogOptions(nil, reader, "group-test", map[string]provisioner.TrinoCatalogClient{"group-test-blue": blue, "group-test-green": green}, nil)
	active, err := opts.Active(context.Background())
	if err != nil || active.Catalog != blue {
		t.Fatal("valid blue route was rejected")
	}
	reader.observation.Route.BackendName = "group-test-green"
	reader.observation.Route.BackendIncarnation = "green-incarnation"
	reader.backend = trinoManagedGatewayBackend{BackendName: "group-test-green", State: "ACTIVE", Incarnation: "green-incarnation"}
	active, err = opts.Active(context.Background())
	if err != nil || active.Catalog != green {
		t.Fatal("post-cutover route did not select green")
	}
	reader.backend.Incarnation = "old-green"
	if _, err := opts.Active(context.Background()); err == nil {
		t.Fatal("stale backend incarnation accepted")
	}
	reader.observation.Route = trinoManagedGatewayRoute{RoutingGroup: "group-test", Generation: 7, BackendName: "group-test-blue", BackendIncarnation: "blue-incarnation"}
	reader.observation.Rollout = &trinoManagedGatewayRollout{OperationID: "operation", Plan: trinoManagedGatewayPlan{PlanHash: strings.Repeat("a", 64), ExpectedRouteGeneration: 7, SourceBackend: "group-test-blue", SourceIncarnation: "blue-incarnation", TargetBackend: "group-test-green"}}
	freeze := &configstore.TrinoCellFreeze{OperationID: "operation", PlanHash: strings.Repeat("a", 64), TargetBackend: "group-test-green"}
	for _, phase := range []string{"CLAIMED", "WARMED", "VERIFIED", "CUTOVER"} {
		reader.observation.Rollout.Phase = phase
		target, err := opts.Target(context.Background(), freeze)
		switch phase {
		case "CLAIMED":
			if err != nil || target != nil {
				t.Fatal("claimed operation probed unstarted target")
			}
		case "CUTOVER":
			if err == nil {
				t.Fatal("late uncertified target accepted after cutover")
			}
		default:
			if err != nil || target == nil || target.Catalog != green {
				t.Fatal("matching warm target rejected")
			}
		}
	}
	reader.observation.Rollout.Phase = "WARMED"
	reader.observation.Route.Generation++
	if _, err := opts.Target(context.Background(), freeze); err == nil {
		t.Fatal("changed source route certified")
	}
}

func TestManagedRolloutFilesWithoutFleetFailStartupValidation(t *testing.T) {
	for _, tc := range []struct{ token, canary string }{{"requested", ""}, {"", "requested"}, {"requested", "requested"}} {
		t.Setenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", tc.token)
		t.Setenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE", tc.canary)
		if _, err := buildTrinoRolloutReadiness(nil, nil); err == nil {
			t.Fatal("requested rollout configuration was ignored without a fleet")
		}
	}
}
