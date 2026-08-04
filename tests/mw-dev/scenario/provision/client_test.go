package provision

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientProvisionSendsExpectedRequestAndPreservesPassword(t *testing.T) {
	var gotRequest map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/orgs/scenario-org/provision" {
			t.Fatalf("path = %s, want provision path", r.URL.Path)
		}
		if got := r.Header.Get("X-Duckgres-Internal-Secret"); got != "internal-secret" {
			t.Fatalf("internal secret header = %q, want internal-secret", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":   "provisioning started",
			"org":      "scenario-org",
			"username": "root",
			"password": "root-password",
			"bucket":   "scenario-bucket",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BaseURL:        server.URL,
		InternalSecret: "internal-secret",
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	resp, err := client.Provision(context.Background(), "scenario-org", map[string]any{
		"database_name": "scenario_db",
		"metadata_store": map[string]any{
			"type": "cnpg-shard",
		},
		"ducklake": map[string]any{
			"enabled": true,
		},
	})
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if gotRequest["database_name"] != "scenario_db" {
		t.Fatalf("database_name = %#v, want scenario_db", gotRequest["database_name"])
	}
	metadataStore, ok := gotRequest["metadata_store"].(map[string]any)
	if !ok || metadataStore["type"] != "cnpg-shard" {
		t.Fatalf("metadata_store = %#v, want type cnpg-shard", gotRequest["metadata_store"])
	}
	if resp.Password != "root-password" {
		t.Fatalf("password = %q, want root-password", resp.Password)
	}
	if resp.Bucket != "scenario-bucket" {
		t.Fatalf("bucket = %q, want scenario-bucket", resp.Bucket)
	}
}

func TestRedactForArtifactRemovesPasswordsAndSecrets(t *testing.T) {
	payload := map[string]any{
		"provision_response": ProvisionResponse{
			Org:      "scenario-org",
			Username: "root",
			Password: "root-password",
		},
		"nested": map[string]any{
			"internal_secret":     "internal-secret",
			"password_aws_secret": "aws-secret-name",
			"safe":                "keep-me",
		},
		"tokens": []any{
			map[string]any{"token": "api-token"},
		},
	}

	redacted := RedactForArtifact(payload)
	raw, err := json.Marshal(redacted)
	if err != nil {
		t.Fatalf("marshal redacted payload: %v", err)
	}
	text := string(raw)
	for _, secret := range []string{"root-password", "internal-secret", "aws-secret-name", "api-token"} {
		if strings.Contains(text, secret) {
			t.Fatalf("redacted payload still contains %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, "keep-me") {
		t.Fatalf("redacted payload should retain safe values: %s", text)
	}
	if !strings.Contains(text, "redacted") {
		t.Fatalf("redacted payload should include redaction marker %q: %s", RedactedValue, text)
	}
}

func TestClientWaitWarehouseReadyPollsUntilReady(t *testing.T) {
	states := []string{WarehouseStatePending, WarehouseStateProvisioning, WarehouseStateReady}
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/api/v1/orgs/scenario-org/warehouse/status" {
			t.Fatalf("path = %s, want status path", r.URL.Path)
		}
		state := states[polls]
		polls++
		response := map[string]any{
			"org_id":               "scenario-org",
			"state":                state,
			"status_message":       "status " + state,
			"s3_state":             state,
			"metadata_store_state": state,
			"identity_state":       state,
			"secrets_state":        state,
		}
		if state == WarehouseStateReady {
			response["connection"] = map[string]any{
				"host":     "warehouse.example",
				"port":     5432,
				"database": "scenario_db",
				"username": "root",
			}
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	var sleeps []time.Duration
	status, err := client.WaitWarehouseReady(context.Background(), "scenario-org", WaitOptions{
		PollInterval: 25 * time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("WaitWarehouseReady returned error: %v", err)
	}
	if status.State != WarehouseStateReady {
		t.Fatalf("state = %q, want ready", status.State)
	}
	if status.Connection == nil || status.Connection.Host != "warehouse.example" {
		t.Fatalf("connection = %+v, want warehouse.example", status.Connection)
	}
	if polls != 3 {
		t.Fatalf("polls = %d, want 3", polls)
	}
	if len(sleeps) != 2 || sleeps[0] != 25*time.Millisecond || sleeps[1] != 25*time.Millisecond {
		t.Fatalf("sleeps = %#v, want two 25ms sleeps", sleeps)
	}
}

func TestClientWaitWarehouseReadyFailsFastOnFailed(t *testing.T) {
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		polls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org_id":         "scenario-org",
			"state":          WarehouseStateFailed,
			"status_message": "composition failed",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.WaitWarehouseReady(context.Background(), "scenario-org", WaitOptions{
		Sleep: func(context.Context, time.Duration) error {
			t.Fatal("sleep should not be called for failed state")
			return nil
		},
	})
	if !errors.Is(err, ErrWarehouseFailed) {
		t.Fatalf("WaitWarehouseReady error = %v, want ErrWarehouseFailed", err)
	}
	if polls != 1 {
		t.Fatalf("polls = %d, want 1", polls)
	}
	if !strings.Contains(err.Error(), "composition failed") {
		t.Fatalf("error = %v, want status message", err)
	}
}

func TestClientDeprovisionSendsExpectedRequest(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/orgs/scenario-org/deprovision" {
			t.Fatalf("path = %s, want deprovision path", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "deprovisioning started",
			"org":    "scenario-org",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	resp, err := client.Deprovision(context.Background(), "scenario-org")
	if err != nil {
		t.Fatalf("Deprovision returned error: %v", err)
	}
	if !called {
		t.Fatal("expected deprovision endpoint to be called")
	}
	if resp.Org != "scenario-org" || resp.Status != "deprovisioning started" {
		t.Fatalf("deprovision response = %+v", resp)
	}
}

func TestClientClassifiesHTTPFailuresAndRedactsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":    "password root-password token api-token Authorization: Bearer bearer-token",
			"password": "root-password",
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.Provision(context.Background(), "scenario-org", map[string]any{
		"database_name": "scenario_db",
	})
	if !errors.Is(err, ErrUnexpectedStatus) {
		t.Fatalf("Provision error = %v, want ErrUnexpectedStatus", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Provision error = %T %v, want APIError", err, err)
	}
	if apiErr.ErrorClass() != ErrorClassProvisionAPI {
		t.Fatalf("error class = %q, want %q", apiErr.ErrorClass(), ErrorClassProvisionAPI)
	}
	for _, secret := range []string{"root-password", "api-token", "bearer-token"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked %q: %v", secret, err)
		}
	}
}

func TestClientWaitPreservesContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org_id": "scenario-org",
			"state":  WarehouseStateProvisioning,
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = client.WaitWarehouseReady(ctx, "scenario-org", WaitOptions{
		Sleep: func(context.Context, time.Duration) error {
			cancel()
			return context.Canceled
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitWarehouseReady error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("WaitWarehouseReady error = %v, should not be ErrWaitTimeout", err)
	}
}

func TestClientWaitWarehouseDeletedReportsTimeout(t *testing.T) {
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		polls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org_id": "scenario-org",
			"state":  WarehouseStateDeleting,
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.WaitWarehouseDeleted(context.Background(), "scenario-org", WaitOptions{
		MaxAttempts: 2,
		Sleep:       func(context.Context, time.Duration) error { return nil },
	})
	if !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("WaitWarehouseDeleted error = %v, want ErrWaitTimeout", err)
	}
	if polls != 2 {
		t.Fatalf("polls = %d, want 2", polls)
	}
}
