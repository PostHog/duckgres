//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

type rolloutEligibilityStub struct{}

func (rolloutEligibilityStub) TrinoRolloutCanaryEligible(context.Context, string, string, string) (bool, error) {
	return true, nil
}

func TestTrinoRolloutReadinessConfigurationExplicitAndPrivate(t *testing.T) {
	t.Setenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", "")
	t.Setenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE", "")
	if h, err := buildTrinoRolloutReadiness(nil, rolloutEligibilityStub{}); err != nil || h != nil {
		t.Fatal("default configuration changed")
	}
	dir := t.TempDir()
	tokenPath, canaryPath := filepath.Join(dir, "token"), filepath.Join(dir, "canaries.json")
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("x", 48)), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", tokenPath)
	if _, err := buildTrinoRolloutReadiness(nil, rolloutEligibilityStub{}); err == nil {
		t.Fatal("partial explicit configuration accepted")
	}
	t.Setenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE", canaryPath)
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{PublicID: "cell-test", RoutingGroup: "group-test", Namespace: "test", Backends: []trinoRegisteredBackend{{ID: "blue", CoordinatorURL: "https://blue.example.test"}, {ID: "green", CoordinatorURL: "https://green.example.test"}}}, Kubernetes: fake.NewSimpleClientset()}}
	for _, tc := range []struct {
		data  string
		valid bool
	}{
		{`{"canaries":[{"cell":"cell-test","orgID":"canary-org","principal":"canary-test","password":"secret-test"}]}`, true},
		{`{"canaries":[{"cell":"unknown","orgID":"canary-org","principal":"canary-test","password":"secret-test"}]}`, false},
		{`{"canaries":[{"cell":"cell-test","orgID":"canary-org","principal":"canary-test","password":""}]}`, false},
		{`{"canaries":[]}`, false},
	} {
		if err := os.WriteFile(canaryPath, []byte(tc.data), 0600); err != nil {
			t.Fatal(err)
		}
		h, err := buildTrinoRolloutReadiness(fleet, rolloutEligibilityStub{})
		if (err == nil) != tc.valid {
			t.Fatalf("valid=%v, error=%v", tc.valid, err)
		}
		if tc.valid && len(h.slots) != 2 {
			t.Fatal("stopped color omitted from observation")
		}
		if tc.valid {
			r := httptest.NewRequest(http.MethodGet, rolloutReadinessPrefix+"group-test/green", nil)
			r.Header.Set(rolloutCapabilityHeader, h.token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			var response rolloutReadinessResponse
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Cell != "cell-test" || response.RoutingGroup != "group-test" || response.BackendName != "group-test-green" {
				t.Fatal("wire contract lost the distinct logical cell and routing group")
			}
		}
		if err != nil && strings.Contains(err.Error(), "secret-test") {
			t.Fatal("configuration error leaked password")
		}
	}
}

func TestTrinoRolloutReadinessScopesFixedCells(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("x", 48)), 0600); err != nil {
		t.Fatal(err)
	}
	legacy := &trinoWiring{Cell: trinoCell{ID: "legacy"}}
	pool := &trinoWiring{Cell: trinoCell{PublicID: "pool-test", Mode: trinoPoolModeShared}}
	fixed := &trinoWiring{
		Cell: trinoCell{PublicID: "fixed-test", RoutingGroup: "group-test", Namespace: "test",
			Backends: []trinoRegisteredBackend{{ID: "blue", CoordinatorURL: "https://blue.example.test"}, {ID: "green", CoordinatorURL: "https://green.example.test"}}},
		Kubernetes: fake.NewSimpleClientset(),
	}
	validCanary := `{"canaries":[{"cell":"fixed-test","orgID":"canary-org","principal":"canary-test","password":"test-password"}]}`
	for _, tc := range []struct {
		name        string
		fleet       trinoFleet
		canaries    string
		noToken     bool
		wantHandler bool
		wantError   bool
	}{
		{name: "pool token does not enable fixed readiness", fleet: trinoFleet{pool}},
		{name: "pool with legacy single", fleet: trinoFleet{legacy, pool}},
		{name: "pool without either readiness setting", fleet: trinoFleet{pool}, noToken: true},
		{name: "token without any registered cells", wantError: true},
		{name: "legacy single token is incomplete", fleet: trinoFleet{legacy}, wantError: true},
		{name: "fixed token is incomplete", fleet: trinoFleet{fixed}, wantError: true},
		{name: "mixed token is incomplete", fleet: trinoFleet{pool, fixed}, wantError: true},
		{name: "mixed fixed without backends rejected", fleet: trinoFleet{pool, &trinoWiring{Cell: trinoCell{PublicID: "empty-test", Mode: "fixed"}}}, wantError: true},
		{name: "mixed unknown mode rejected", fleet: trinoFleet{pool, &trinoWiring{Cell: trinoCell{PublicID: "unknown-test", Mode: "unknown"}}}, wantError: true},
		{name: "fixed valid", fleet: trinoFleet{fixed}, canaries: validCanary, wantHandler: true},
		{name: "mixed valid", fleet: trinoFleet{legacy, pool, fixed}, canaries: validCanary, wantHandler: true},
		{name: "explicit pool canary rejected", fleet: trinoFleet{pool}, canaries: `{"canaries":[]}`, wantError: true},
		{name: "mixed malformed canary rejected", fleet: trinoFleet{pool, fixed}, canaries: `{`, wantError: true},
		{name: "mixed canary without token rejected", fleet: trinoFleet{pool, fixed}, canaries: validCanary, noToken: true, wantError: true},
		{name: "mixed extra pool canary rejected", fleet: trinoFleet{pool, fixed},
			canaries: `{"canaries":[{"cell":"fixed-test","orgID":"canary-org","principal":"canary-test","password":"test-password"},{"cell":"pool-test","orgID":"pool-org","principal":"pool-canary","password":"test-password"}]}`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", tokenPath)
			if tc.noToken {
				t.Setenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE", "")
			}
			t.Setenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE", "")
			if tc.canaries != "" {
				path := filepath.Join(t.TempDir(), "canaries.json")
				if err := os.WriteFile(path, []byte(tc.canaries), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE", path)
			}
			handler, err := buildTrinoRolloutReadiness(tc.fleet, rolloutEligibilityStub{})
			if (err != nil) != tc.wantError || (handler != nil) != tc.wantHandler {
				t.Fatalf("handler=%v error=%v; want handler=%v error=%v", handler != nil, err, tc.wantHandler, tc.wantError)
			}
			if handler != nil {
				if len(handler.slots) != 2 {
					t.Fatalf("fixed readiness includes unexpected slots: %d", len(handler.slots))
				}
				for _, slot := range handler.slots {
					if slot.cell != fixed.Cell.PublicID {
						t.Fatalf("fixed readiness includes cell %q", slot.cell)
					}
				}
			}
		})
	}
}
