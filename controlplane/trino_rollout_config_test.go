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
