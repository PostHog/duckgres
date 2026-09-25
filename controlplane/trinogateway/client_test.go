package trinogateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type capturedRequest struct {
	method string
	path   string
	body   map[string]any
	header http.Header
}

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *[]capturedRequest) {
	t.Helper()
	var captured []capturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := capturedRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone()}
		if payload, err := io.ReadAll(r.Body); err == nil && len(payload) > 0 {
			_ = json.Unmarshal(payload, &record.body)
		}
		captured = append(captured, record)
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(Config{BaseURL: server.URL, AdminToken: "0123456789abcdef0123456789abcdef", AllowPlaintext: true})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client, &captured
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func gatewayError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("X-Trino-Gateway-Error", code)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(code))
}

// memberResponse is the real Java-serialized member fixture, adjusted for the
// phase and generation a given test needs. Building it from the fixture keeps
// the handler's responses in the shape the Gateway actually emits.
func memberResponse(t *testing.T, phase string, generation int64) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "member.json"))
	if err != nil {
		t.Fatalf("read member fixture: %v", err)
	}
	var response map[string]any
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("decode member fixture: %v", err)
	}
	response["phase"], response["generation"] = phase, generation
	return response
}

// The Gateway's existing admin credential is reused; this adds no new secret.
func TestClientSendsTheExistingAdminCredential(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse(t, "PREPARING", 1))
	})
	if _, err := client.RegisterMember(context.Background(), "pool-1", RegisterMemberRequest{
		Step:       Step{OperationID: "op-1", StepID: "register", ControllerEpoch: 7},
		InstanceID: "i-1", BackendName: "pool-1-i-1", URL: "https://i-1.invalid:8443",
		PodUID: "pod-uid", BootID: "boot-id", ConfigRevision: "r-42",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	request := (*captured)[0]
	if request.method != http.MethodPost || request.path != "/gateway/v1/pools/pool-1/members" {
		t.Fatalf("request = %s %s", request.method, request.path)
	}
	if request.header.Get("Authorization") != "Bearer 0123456789abcdef0123456789abcdef" {
		t.Fatalf("missing bearer credential: %q", request.header.Get("Authorization"))
	}
	if request.header.Get("X-Gateway-Transaction-Admin-Token") != "0123456789abcdef0123456789abcdef" {
		t.Fatal("missing admin token header")
	}
}

// Field spellings are the cross-repo contract; a typo here fails closed at
// runtime and would only show up in a live deployment.
func TestRegisterMemberBodyMatchesTheContract(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse(t, "PREPARING", 1))
	})
	if _, err := client.RegisterMember(context.Background(), "pool-1", RegisterMemberRequest{
		Step:       Step{OperationID: "op-1", StepID: "register", ControllerEpoch: 7},
		InstanceID: "i-1", BackendName: "pool-1-i-1", URL: "https://i-1.invalid:8443",
		PodUID: "pod-uid", BootID: "boot-id", ConfigRevision: "r-42",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	body := (*captured)[0].body
	// Exactly the fields PoolLifecycleService.registerMember reads.
	for _, field := range []string{"operationId", "stepId", "controllerEpoch", "instanceId", "backendName", "url", "podUid", "bootId", "configRevision"} {
		if _, present := body[field]; !present {
			t.Errorf("request body is missing %q", field)
		}
	}
	// The Gateway computes the guard's payload hash from the canonicalized body
	// itself. Sending one would change that body and therefore the hash, making
	// an identical replay look like a changed intent.
	if _, present := body["payloadHash"]; present {
		t.Error("the client sent a payloadHash the Gateway computes itself")
	}
	// The endpoint comes from the Gateway's own backend registration; there is
	// no externalUrl input.
	if _, present := body["externalUrl"]; present {
		t.Error("the client sent externalUrl, which the Gateway does not read")
	}
}

func TestAdmitIsASingleCallCarryingTheReceipt(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse(t, "ACTIVE", 4))
	})
	member, err := client.AdmitMember(context.Background(), "pool-1", "i-1", admitRequest())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if member.Phase != "ACTIVE" || member.Generation != 4 {
		t.Fatalf("member = %+v", member)
	}
	request := (*captured)[0]
	// Admission is ONE call with a nested receipt. There is no /certificate and
	// no /activate route; posting to either would 404 in production while every
	// mock-based test kept passing.
	if request.path != "/gateway/v1/pools/pool-1/members/i-1/admit" {
		t.Fatalf("path = %s", request.path)
	}
	if request.body["expectedGeneration"] != float64(3) {
		t.Fatalf("expectedGeneration = %v", request.body["expectedGeneration"])
	}
	receipt, ok := request.body["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("receipt is not a nested object: %v", request.body["receipt"])
	}
	// Every one of these is read with a required-text accessor on the Java
	// side: an empty value is a 400, not a default.
	for _, field := range []string{"certificateHash", "configRevision", "authRevision", "podUid", "bootId", "nodeId", "coordinatorId", "readyWorkers", "checks"} {
		if _, present := receipt[field]; !present {
			t.Errorf("receipt is missing %q", field)
		}
	}
}

func admitRequest() AdmitMemberRequest {
	return AdmitMemberRequest{
		Step:               Step{OperationID: "op-1", StepID: "admit", ControllerEpoch: 7},
		ExpectedGeneration: 3,
		Receipt: ValidationReceipt{
			CertificateHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
			ConfigRevision:  "r-42", AuthRevision: "auth-9",
			PodUID: "pod-uid", BootID: "boot-id", NodeID: "node-1", CoordinatorID: "abcde",
			ReadyWorkers: 4,
			Checks:       []string{CheckImage, CheckWorkers, CheckCatalogRevision, CheckAuthRevision, CheckOperationalConnection},
		},
	}
}

// Terminal conflicts are surfaced to the operator, never hot-retried: retrying
// a stale epoch or a changed intent cannot succeed and hides a real fault.
func TestGatewayConflictsAreTypedAndTerminal(t *testing.T) {
	cases := map[string]error{
		"POOL_STALE_EPOCH":         ErrStaleEpoch,
		"POOL_INTENT_CHANGED":      ErrIntentChanged,
		"POOL_STALE_GENERATION":    ErrStaleGeneration,
		"POOL_PHASE":               ErrPhase,
		"POOL_IRREVERSIBLE":        ErrIrreversible,
		"POOL_SERVING_FLOOR":       ErrServingFloor,
		"POOL_SURGE_BUDGET":        ErrSurgeBudget,
		"POOL_NOT_CERTIFIED":       ErrNotCertified,
		"POOL_PUBLICATION_BARRIER": ErrPublicationBarrier,
		"POOL_MEMBERSHIP_CHANGED":  ErrMembershipChanged,
		"POOL_RECEIPTS_INCOMPLETE": ErrReceiptsIncomplete,
		"POOL_EVIDENCE_REQUIRED":   ErrEvidenceRequired,
		"POOL_DISABLED":            ErrPoolDisabled,
		"POOL_VALIDATION":          ErrValidation,
		"POOL_NOT_FOUND":           ErrNotFound,
		"POOL_IDENTITY_CONFLICT":   ErrIdentityConflict,
		"POOL_APIMODE":             ErrAPIMode,
		"POOL_NOT_DRAINED":         ErrNotDrained,
		"POOL_REPAIR_BUDGET":       ErrRepairBudget,
	}
	for code, expected := range cases {
		t.Run(code, func(t *testing.T) {
			status := http.StatusConflict
			switch code {
			case "POOL_DISABLED", "POOL_NOT_FOUND":
				status = http.StatusNotFound
			case "POOL_VALIDATION":
				status = http.StatusBadRequest
			}
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gatewayError(w, status, code)
			})
			_, err := client.AdmitMember(context.Background(), "pool-1", "i-1", admitRequest())
			if !errors.Is(err, expected) {
				t.Fatalf("error = %v, want %v", err, expected)
			}
			if Retryable(err) {
				t.Fatalf("%s was classified as retryable", code)
			}
		})
	}
}

// A 503 is a transient condition, not a verdict about the operation.
func TestUnavailableIsRetryable(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gatewayError(w, http.StatusServiceUnavailable, "ROUTING_STATE_UNAVAILABLE")
	})
	_, err := client.GetPool(context.Background(), "pool-1")
	if err == nil || !Retryable(err) {
		t.Fatalf("error = %v, want a retryable failure", err)
	}
}

// A replayed step returns the RECORDED result. The operator must be able to
// tell that apart from a fresh mutation so it does not double-count effects.
func TestReplayedResponsesAreReported(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		response := memberResponse(t, "ACTIVE", 4)
		response["replayed"] = true
		writeJSON(t, w, http.StatusOK, response)
	})
	member, err := client.AdmitMember(context.Background(), "pool-1", "i-1", admitRequest())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !member.Replayed {
		t.Fatal("a replayed response was reported as a fresh mutation")
	}
}

// A lost response is resolved by reading the SAME operation back, never by
// minting a fresh operation id.
func TestOperationReadBackResolvesALostResponse(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"protocolVersion": 1, "operationId": "op-1",
			"steps": []any{map[string]any{
				"stepId": "admit", "payloadHash": "abc", "controllerEpoch": 7,
				"outcome": "OK", "recordedAt": "2026-09-18T00:00:00Z",
				"result": map[string]any{"phase": "ACTIVE"},
			}},
		})
	})
	history, err := client.GetOperation(context.Background(), "pool-1", "op-1")
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if len(history.Steps) != 1 || history.Steps[0].StepID != "admit" || history.Steps[0].Outcome != "OK" {
		t.Fatalf("history = %+v", history)
	}
	if (*captured)[0].path != "/gateway/v1/pools/pool-1/operations/op-1" {
		t.Fatalf("path = %s", (*captured)[0].path)
	}
}

// A plaintext or credential-bearing origin must be refused: this client carries
// an admin token.
func TestClientRejectsUnsafeConfiguration(t *testing.T) {
	cases := map[string]Config{
		"plaintext origin":      {BaseURL: "http://gateway.invalid", AdminToken: "0123456789abcdef0123456789abcdef"},
		"credentials in origin": {BaseURL: "https://user:pass@gateway.invalid", AdminToken: "0123456789abcdef0123456789abcdef", AllowPlaintext: true},
		"short token":           {BaseURL: "https://gateway.invalid", AdminToken: "too-short"},
		"missing token":         {BaseURL: "https://gateway.invalid"},
		"path in origin":        {BaseURL: "https://gateway.invalid/api", AdminToken: "0123456789abcdef0123456789abcdef"},
	}
	for name, config := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClient(config); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestDrainReconciliationProtocol(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.URL.Query().Get("after") != "query+1" {
				t.Error("cursor not escaped")
			}
			writeJSON(t, w, http.StatusOK, []map[string]any{{"queryId": "query-2", "admissionCount": 7}})
		} else {
			writeJSON(t, w, http.StatusOK, map[string]any{"reconciled": 1})
		}
	})
	candidates, err := client.GetDrainCandidates(context.Background(), "pool-1", "member-1", "query+1")
	if err != nil || len(candidates) != 1 || candidates[0].AdmissionCount != 7 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	result, err := client.ReconcileQueries(context.Background(), "pool-1", "member-1", ReconcileQueriesRequest{
		Step: Step{OperationID: "op", StepID: "proof", ControllerEpoch: 3, OwnerIdentity: "controller"}, ExpectedGeneration: 9, NodeID: "node", CoordinatorID: "process", Queries: candidates})
	if err != nil || result.Reconciled != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	r := (*captured)[1]
	if r.path != "/gateway/v1/pools/pool-1/members/member-1/reconcile-queries" || r.body["expectedGeneration"] != float64(9) || r.body["nodeId"] != "node" || r.body["coordinatorId"] != "process" || r.body["controllerEpoch"] != float64(3) || r.body["ownerIdentity"] != "controller" {
		t.Fatalf("wrong wire contract: %+v", r)
	}
}
