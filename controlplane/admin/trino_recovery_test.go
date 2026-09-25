//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type recoveryTrinoStore struct {
	fakeTrinoOrgStore
	instance      *configstore.TrinoPoolInstance
	pool          *configstore.TrinoPool
	instances     []configstore.TrinoPoolInstance
	request       *configstore.TrinoPoolRecovery
	writes        int
	requestedPool string
	requestErr    error
}

func (s *recoveryTrinoStore) GetTrinoPoolInstance(_ context.Context, id string) (*configstore.TrinoPoolInstance, error) {
	if s.instance != nil && s.instance.InstanceID == id {
		return s.instance, nil
	}
	return nil, nil
}
func (s *recoveryTrinoStore) GetTrinoPool(_ context.Context, id string) (*configstore.TrinoPool, error) {
	if s.pool != nil && s.pool.PoolID == id {
		return s.pool, nil
	}
	return nil, nil
}
func (s *recoveryTrinoStore) ListTrinoPoolInstances(context.Context, string) ([]configstore.TrinoPoolInstance, error) {
	return s.instances, nil
}
func (s *recoveryTrinoStore) GetTrinoPoolRecovery(context.Context, string, string) (*configstore.TrinoPoolRecovery, error) {
	return s.request, nil
}
func (s *recoveryTrinoStore) RequestTrinoPoolRecovery(_ context.Context, pool, _ string, req configstore.TrinoPoolRecovery) (*configstore.TrinoPoolRecovery, error) {
	s.writes++
	s.requestedPool = pool
	if s.requestErr != nil {
		return nil, s.requestErr
	}
	s.request = &req
	return &req, nil
}
func recoveryTrinoAPI() (*TrinoAPI, *recoveryTrinoStore) {
	store := &recoveryTrinoStore{
		instance:  &configstore.TrinoPoolInstance{InstanceID: "instance-a", PoolID: "registered:pool-a", Phase: "DRAINING", GatewayState: "DRAINING", GatewayGeneration: 7, GatewayIncarnation: "incarnation-a", CoordinatorPodUID: "pod-a", CoordinatorBootID: "boot-a", CoordinatorNodeID: "node-a", CoordinatorID: "coordinator-a", BlueprintSnapshot: `{"secret":"do-not-expose"}`, EndpointURL: "http://private.example.test"},
		pool:      &configstore.TrinoPool{PoolID: "registered:pool-a", APIMode: configstore.TrinoPoolAPIModeShared, MinServing: 2, DesiredInstances: 2},
		instances: []configstore.TrinoPoolInstance{{PoolID: "registered:pool-a", Phase: "SERVING"}, {PoolID: "registered:pool-a", Phase: "SERVING"}, {PoolID: "registered:pool-a", Phase: "DRAINING"}},
	}
	api := NewTrinoFleetAPI([]TrinoCell{{ID: "pool-a", StoredID: "registered:pool-a"}}, []TrinoCoordinatorClient{&fakeTrinoCoordinator{}}, store, nil)
	return api, store
}

const recoveryPath = "/api/v1/trino/instances/instance-a/recovery?cell=pool-a"
const recoveryBody = `{"operation_id":"recovery-test-a","expected_generation":7,"incarnation":"incarnation-a","pod_uid":"pod-a","boot_id":"boot-a","node_id":"node-a","coordinator_id":"coordinator-a","reason":"Discard retained results after independent verification","destructive_authorization":true}`

func TestTrinoRecoveryPreviewIsReadOnlyAndSanitized(t *testing.T) {
	api, store := recoveryTrinoAPI()
	store.instance.LastError = "private.example.test password=do-not-expose"
	code, data := doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), http.MethodGet, recoveryPath, "")
	if code != http.StatusOK {
		t.Fatalf("preview: %d %#v", code, data)
	}
	encoded, _ := json.Marshal(data)
	for _, forbidden := range []string{"do-not-expose", "private.example.test", "blueprint", "endpoint_url"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("preview exposes %s", forbidden)
		}
	}
	instance := data["instance"].(map[string]any)
	if instance["expected_generation"] != float64(7) || instance["phase"] != "DRAINING" || instance["pod_uid"] != "pod-a" {
		t.Fatalf("identity omitted: %#v", instance)
	}
	capacity := data["capacity"].(map[string]any)
	if capacity["stored_serving"] != float64(2) || capacity["min_serving"] != float64(2) || data["live_work_verified"] != false || store.writes != 0 {
		t.Fatalf("unsafe preview: %#v writes=%d", data, store.writes)
	}
	if instance["last_error"] != "" {
		t.Fatalf("arbitrary error exposed: %#v", instance)
	}
	store.request = &configstore.TrinoPoolRecovery{OperationID: "recovery-test-a"}
	store.instance.LastError = "Recovery is blocked; inspect the control-plane logs before retrying."
	code, data = doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), http.MethodGet, recoveryPath, "")
	if code != http.StatusOK || data["instance"].(map[string]any)["last_error"] != store.instance.LastError || store.writes != 0 {
		t.Fatalf("safe recovery status omitted: %d %#v", code, data)
	}
}

func TestTrinoRecoveryRequiresAdminAndSelectedPool(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		api, store := recoveryTrinoAPI()
		code, _ := doTrinoJSON(t, trinoTestRouter(api, RoleViewer), method, recoveryPath, recoveryBody)
		if code != http.StatusForbidden || store.writes != 0 {
			t.Fatalf("viewer %s: %d", method, code)
		}
		for _, path := range []string{strings.Replace(recoveryPath, "instance-a", "unknown", 1), strings.Replace(recoveryPath, "pool-a", "unknown", 1)} {
			code, _ = doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), method, path, recoveryBody)
			if code != http.StatusNotFound || store.writes != 0 {
				t.Fatalf("unknown %s: %d", path, code)
			}
		}
		store.instance.PoolID = "registered:another-pool"
		code, _ = doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), method, recoveryPath, recoveryBody)
		if code != http.StatusNotFound || store.writes != 0 {
			t.Fatalf("cross-pool %s: %d", method, code)
		}
	}
}

func TestTrinoRecoveryRequestRequiresExplicitIntent(t *testing.T) {
	for _, body := range []string{`{`, `{}`, strings.Replace(recoveryBody, `"destructive_authorization":true`, `"destructive_authorization":false`, 1), strings.Replace(recoveryBody, `"reason":"Discard retained results after independent verification"`, `"reason":" "`, 1), strings.Replace(recoveryBody, `"pod_uid":"pod-a"`, `"pod_uid":""`, 1), strings.Replace(recoveryBody, `"expected_generation":7`, `"expected_generation":0`, 1), recoveryBody + `{}`, strings.Replace(recoveryBody, `"reason":`, `"requested_by":"forged@example.com","reason":`, 1)} {
		api, store := recoveryTrinoAPI()
		code, _ := doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), http.MethodPost, recoveryPath, body)
		if code != http.StatusBadRequest || store.writes != 0 {
			t.Fatalf("invalid request accepted: %d writes=%d body=%s", code, store.writes, body)
		}
	}
}

func TestTrinoRecoveryRequestBindsActorAndPreservesConflicts(t *testing.T) {
	api, store := recoveryTrinoAPI()
	router := trinoTestRouter(api, RoleAdmin)
	code, data := doTrinoJSON(t, router, http.MethodPost, recoveryPath, recoveryBody)
	if code != http.StatusAccepted || store.request == nil || store.request.RequestedBy != "operator@posthog.com" || store.requestedPool != "registered:pool-a" || store.request.InstanceID != "instance-a" || store.request.OperationID != "recovery-test-a" {
		t.Fatalf("request: %d %#v %#v", code, data, store.request)
	}
	store.instance.Phase = "FAILURE_RETIRED"
	store.instance.GatewayGeneration = 10
	code, _ = doTrinoJSON(t, router, http.MethodPost, recoveryPath, recoveryBody)
	if code != http.StatusAccepted || store.request.ExpectedGeneration != 7 {
		t.Fatalf("original intent cannot replay after progress: %d", code)
	}
	for _, test := range []struct {
		err    error
		status int
	}{
		{configstore.ErrTrinoPoolConflict, http.StatusConflict},
		{configstore.ErrTrinoPoolRecoveryConflict, http.StatusConflict},
		{configstore.ErrTrinoPoolRecoveryInvalid, http.StatusBadRequest},
		{errors.New("private database endpoint"), http.StatusInternalServerError},
	} {
		store.requestErr = test.err
		code, data = doTrinoJSON(t, router, http.MethodPost, recoveryPath, recoveryBody)
		if code != test.status || strings.Contains(data["error"].(string), "private database endpoint") {
			t.Fatalf("store error response: %d %#v", code, data)
		}
	}
}
