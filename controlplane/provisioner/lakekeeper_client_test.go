package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(handler http.Handler) (*LakekeeperClient, func()) {
	srv := httptest.NewServer(handler)
	return NewLakekeeperClient(srv.URL), srv.Close
}

func TestInfo_OK(t *testing.T) {
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/management/v1/info" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"version":"0.11.6","bootstrapped":true,"server-id":"sid","authz-backend":"allow-all"}`)
	}))
	defer stop()

	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !info.Bootstrapped || info.Version != "0.11.6" || info.AuthzBackend != "allow-all" {
		t.Fatalf("unexpected info: %+v", info)
	}
}

func TestBootstrap_AlreadyBootstrappedIsIdempotent(t *testing.T) {
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":{"message":"already bootstrapped"}}`)
	}))
	defer stop()

	if err := c.Bootstrap(context.Background()); err != nil {
		t.Fatalf("expected idempotent success on 409, got: %v", err)
	}
}

func TestBootstrap_ServerErrorPropagates(t *testing.T) {
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	}))
	defer stop()

	err := c.Bootstrap(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got: %v", err)
	}
	if apiErr.Status != http.StatusInternalServerError || apiErr.Path != "/management/v1/bootstrap" {
		t.Fatalf("unexpected APIError: %+v", apiErr)
	}
}

func TestEnsureWarehouse_CreatesWhenAbsent(t *testing.T) {
	var sawCreate bool
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/management/v1/warehouse":
			_, _ = io.WriteString(w, `{"warehouses":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/management/v1/warehouse":
			sawCreate = true
			var req CreateWarehouseRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if req.WarehouseName != "org-acme" {
				t.Fatalf("warehouse name = %q, want org-acme", req.WarehouseName)
			}
			if !req.StorageProfile.STSEnabled || req.StorageProfile.Flavor != "s3-compat" {
				t.Fatalf("storage profile must enable STS + s3-compat flavor, got %+v", req.StorageProfile)
			}
			if req.StorageProfile.RemoteSigningEnabled {
				t.Fatalf("remote-signing-enabled must be false for DuckDB-compatible vending")
			}
			_, _ = io.WriteString(w, `{"warehouse-id":"wh-uuid","name":"org-acme","status":"active"}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer stop()

	req := CreateWarehouseRequest{
		WarehouseName: "org-acme",
		StorageProfile: WarehouseStorageProfile{
			Type: "s3", Bucket: "warehouse", KeyPrefix: "org-acme",
			Endpoint: "http://minio:9000", Region: "us-east-1",
			PathStyleAccess: true, Flavor: "s3-compat",
			STSEnabled: true, RemoteSigningEnabled: false,
		},
		StorageCredential: WarehouseStorageCredential{
			Type: "s3", CredentialType: "access-key",
			AWSAccessKeyID: "minioadmin", AWSSecretAccessKey: "minioadmin",
		},
	}
	wh, err := c.EnsureWarehouse(context.Background(), req)
	if err != nil {
		t.Fatalf("EnsureWarehouse: %v", err)
	}
	if !sawCreate {
		t.Fatalf("create POST was not sent")
	}
	if wh.Name != "org-acme" || wh.WarehouseID != "wh-uuid" {
		t.Fatalf("unexpected warehouse: %+v", wh)
	}
}

func TestEnsureWarehouse_ResolvesRaceOn409(t *testing.T) {
	// Simulate concurrent reconcilers: first GET sees empty list, POST loses
	// the race (409), follow-up GET returns the winner's warehouse.
	step := 0
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && step == 0:
			step = 1
			_, _ = io.WriteString(w, `{"warehouses":[]}`)
		case r.Method == http.MethodPost && step == 1:
			step = 2
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"error":{"message":"warehouse exists"}}`)
		case r.Method == http.MethodGet && step == 2:
			_, _ = io.WriteString(w, `{"warehouses":[{"warehouse-id":"winner","name":"org-acme","status":"active"}]}`)
		default:
			t.Fatalf("unexpected request at step %d: %s %s", step, r.Method, r.URL.Path)
		}
	}))
	defer stop()

	wh, err := c.EnsureWarehouse(context.Background(), CreateWarehouseRequest{WarehouseName: "org-acme"})
	if err != nil {
		t.Fatalf("EnsureWarehouse should resolve 409 via re-list: %v", err)
	}
	if wh.WarehouseID != "winner" {
		t.Fatalf("expected winner warehouse, got %+v", wh)
	}
}

func TestEnsureWarehouse_NoOpWhenPresent(t *testing.T) {
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			t.Fatalf("create should not be called when warehouse already exists")
		}
		_, _ = io.WriteString(w, `{"warehouses":[{"warehouse-id":"existing","name":"org-acme","status":"active"}]}`)
	}))
	defer stop()

	wh, err := c.EnsureWarehouse(context.Background(), CreateWarehouseRequest{WarehouseName: "org-acme"})
	if err != nil {
		t.Fatalf("EnsureWarehouse: %v", err)
	}
	if wh.WarehouseID != "existing" {
		t.Fatalf("expected existing warehouse, got %+v", wh)
	}
}

func TestBearerHeaderIsSetWhenConfigured(t *testing.T) {
	var sawAuth string
	c, stop := newTestClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"version":"x","bootstrapped":true}`)
	}))
	defer stop()
	c.WithBearer("abc.def.ghi")

	if _, err := c.Info(context.Background()); err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !strings.HasPrefix(sawAuth, "Bearer abc") {
		t.Fatalf("Authorization header = %q, want Bearer abc...", sawAuth)
	}
}
