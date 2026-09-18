package trinogateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

func memberResponse(phase string, generation int64) map[string]any {
	return map[string]any{
		"protocolVersion": 1, "poolId": "pool-1", "instanceId": "i-1",
		"incarnation": "11111111-1111-4111-8111-111111111111", "backendName": "pool-1-i-1",
		"phase": phase, "generation": generation, "controllerEpoch": 7,
		"membershipGeneration": 19, "replayed": false,
	}
}

// The Gateway's existing admin credential is reused; this adds no new secret.
func TestClientSendsTheExistingAdminCredential(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse("PREPARING", 1))
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
		writeJSON(t, w, http.StatusOK, memberResponse("PREPARING", 1))
	})
	if _, err := client.RegisterMember(context.Background(), "pool-1", RegisterMemberRequest{
		Step:       Step{OperationID: "op-1", StepID: "register", ControllerEpoch: 7},
		InstanceID: "i-1", BackendName: "pool-1-i-1", URL: "https://i-1.invalid:8443",
		ExternalURL: "https://i-1.external.invalid:8443",
		PodUID:      "pod-uid", BootID: "boot-id", ConfigRevision: "r-42",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	body := (*captured)[0].body
	for _, field := range []string{"operationId", "stepId", "controllerEpoch", "instanceId", "backendName", "url", "externalUrl", "podUid", "bootId", "configRevision"} {
		if _, present := body[field]; !present {
			t.Errorf("request body is missing %q", field)
		}
	}
}

func TestActivateSendsTheGenerationCAS(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse("ACTIVE", 4))
	})
	member, err := client.ActivateMember(context.Background(), "pool-1", "i-1", MemberStepRequest{
		Step: Step{OperationID: "op-1", StepID: "activate", ControllerEpoch: 7}, ExpectedGeneration: 3,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if member.Phase != "ACTIVE" || member.Generation != 4 {
		t.Fatalf("member = %+v", member)
	}
	body := (*captured)[0].body
	if body["expectedGeneration"] != float64(3) {
		t.Fatalf("expectedGeneration = %v", body["expectedGeneration"])
	}
}

// The certificate reports checks DUCKGRES performed. Claiming an
// auth-projection check the coordinator cannot acknowledge would be a lie the
// Gateway would record verbatim.
func TestCertificateReportsOnlyPerformedChecks(t *testing.T) {
	client, captured := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, memberResponse("PREPARING", 2))
	})
	if _, err := client.CertifyMember(context.Background(), "pool-1", "i-1", CertificateRequest{
		Step: Step{OperationID: "op-1", StepID: "certificate", ControllerEpoch: 7}, ExpectedGeneration: 1,
		ConfigRevision: "r-42", PodUID: "pod-uid", BootID: "boot-id",
		NodeID: "node-1", CoordinatorID: "abcde", ReadyWorkers: 4,
		Checks:          []string{CheckImage, CheckWorkers, CheckCatalogRevision, CheckOperationalConnection},
		CertificateHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	}); err != nil {
		t.Fatalf("certify: %v", err)
	}
	checks, _ := (*captured)[0].body["checks"].([]any)
	for _, check := range checks {
		if check == "auth-revision" {
			t.Fatal("certificate claimed an auth-revision check nothing can acknowledge")
		}
	}
	if len(checks) != 4 {
		t.Fatalf("checks = %v", checks)
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
	}
	for code, expected := range cases {
		t.Run(code, func(t *testing.T) {
			status := http.StatusConflict
			if code == "POOL_DISABLED" {
				status = http.StatusNotFound
			}
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gatewayError(w, status, code)
			})
			_, err := client.ActivateMember(context.Background(), "pool-1", "i-1", MemberStepRequest{
				Step: Step{OperationID: "op-1", StepID: "activate", ControllerEpoch: 7}, ExpectedGeneration: 1,
			})
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
		response := memberResponse("ACTIVE", 4)
		response["replayed"] = true
		writeJSON(t, w, http.StatusOK, response)
	})
	member, err := client.ActivateMember(context.Background(), "pool-1", "i-1", MemberStepRequest{
		Step: Step{OperationID: "op-1", StepID: "activate", ControllerEpoch: 7}, ExpectedGeneration: 3,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
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
				"stepId": "activate", "payloadHash": "abc", "controllerEpoch": 7,
				"outcome": "OK", "recordedAt": "2026-09-18T00:00:00Z",
				"result": map[string]any{"phase": "ACTIVE"},
			}},
		})
	})
	operation, err := client.GetOperation(context.Background(), "pool-1", "op-1")
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if len(operation.Steps) != 1 || operation.Steps[0].StepID != "activate" || operation.Steps[0].Outcome != "OK" {
		t.Fatalf("operation = %+v", operation)
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
