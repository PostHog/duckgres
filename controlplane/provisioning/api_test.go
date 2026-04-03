package provisioning

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

type fakeStore struct {
	orgs       map[string]*configstore.Org
	warehouses map[string]*configstore.ManagedWarehouse
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		orgs:       make(map[string]*configstore.Org),
		warehouses: make(map[string]*configstore.ManagedWarehouse),
	}
}

func (s *fakeStore) GetOrg(orgID string) (*configstore.Org, error) {
	org, ok := s.orgs[orgID]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return org, nil
}

func (s *fakeStore) CreateOrgUser(orgID, username, passwordHash string) error {
	return nil
}

func (s *fakeStore) UpdateOrgUserPassword(orgID, username, passwordHash string) error {
	return nil
}

func (s *fakeStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	w, ok := s.warehouses[orgID]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	clone := *w
	return &clone, nil
}

func (s *fakeStore) CreatePendingWarehouse(orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error {
	// Auto-create org if needed (mirrors production behavior)
	if _, ok := s.orgs[orgID]; !ok {
		s.orgs[orgID] = &configstore.Org{Name: orgID, DatabaseName: databaseName}
	}
	existing, ok := s.warehouses[orgID]
	if ok && existing.State != configstore.ManagedWarehouseStateFailed && existing.State != configstore.ManagedWarehouseStateDeleted {
		return errors.New("warehouse already exists in non-terminal state")
	}
	clone := *warehouse
	clone.OrgID = orgID
	clone.State = configstore.ManagedWarehouseStatePending
	clone.WarehouseDatabaseState = configstore.ManagedWarehouseStatePending
	clone.MetadataStoreState = configstore.ManagedWarehouseStatePending
	clone.S3State = configstore.ManagedWarehouseStatePending
	clone.IdentityState = configstore.ManagedWarehouseStatePending
	clone.SecretsState = configstore.ManagedWarehouseStatePending
	s.warehouses[orgID] = &clone
	return nil
}

func (s *fakeStore) IsDatabaseNameAvailable(name string) (bool, error) {
	for _, org := range s.orgs {
		if org.DatabaseName == name {
			return false, nil
		}
	}
	return true, nil
}

func (s *fakeStore) SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error {
	w, ok := s.warehouses[orgID]
	if !ok {
		return gorm.ErrRecordNotFound
	}
	if w.State != expectedState {
		return fmt.Errorf("warehouse %q not in expected state %q", orgID, expectedState)
	}
	w.State = configstore.ManagedWarehouseStateDeleting
	return nil
}

func newTestRouter(store Store) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterAPI(r.Group("/api/v1"), store)
	return r
}

func TestProvisionCreatesWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "analytics-db",
		"metadata_store": {
			"type": "aurora",
			"aurora": {"min_acu": 0.5, "max_acu": 2}
		}
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	w := store.warehouses["analytics"]
	if w == nil {
		t.Fatal("expected warehouse to be created")
	}
	if w.State != configstore.ManagedWarehouseStatePending {
		t.Fatalf("expected state pending, got %q", w.State)
	}
	if w.AuroraMinACU != 0.5 {
		t.Fatalf("expected min_acu 0.5, got %f", w.AuroraMinACU)
	}
	if w.AuroraMaxACU != 2 {
		t.Fatalf("expected max_acu 2, got %f", w.AuroraMaxACU)
	}
}

func TestProvisionAutoCreatesOrg(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "aurora", "aurora": {"max_acu": 1}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/new-org/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if _, ok := store.orgs["new-org"]; !ok {
		t.Fatal("expected org to be auto-created")
	}
	if store.warehouses["new-org"] == nil {
		t.Fatal("expected warehouse to be created")
	}
}

func TestProvisionRejectsEmptyBody(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestProvisionRejectsZeroMaxACU(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"metadata_store": {"type": "aurora", "aurora": {"min_acu": 0.5, "max_acu": 0}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestProvisionRejectsExistingNonTerminal(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateProvisioning,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "aurora", "aurora": {"max_acu": 1}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestProvisionAllowsRetryAfterFailure(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateFailed,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "aurora", "aurora": {"min_acu": 0, "max_acu": 2}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses["analytics"].AuroraMaxACU != 2 {
		t.Fatalf("expected max_acu 2, got %f", store.warehouses["analytics"].AuroraMaxACU)
	}
}

func TestDeprovisionReadyWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateReady,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses["analytics"].State != configstore.ManagedWarehouseStateDeleting {
		t.Fatalf("expected deleting state, got %q", store.warehouses["analytics"].State)
	}
}

func TestDeprovisionFailedWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateFailed,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses["analytics"].State != configstore.ManagedWarehouseStateDeleting {
		t.Fatalf("expected deleting state, got %q", store.warehouses["analytics"].State)
	}
}

func TestDeprovisionProvisioningWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateProvisioning,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses["analytics"].State != configstore.ManagedWarehouseStateDeleting {
		t.Fatalf("expected deleting state, got %q", store.warehouses["analytics"].State)
	}
}

func TestDeprovisionRejectsPendingWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStatePending,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestGetWarehouseStatus(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID:              "analytics",
		State:              configstore.ManagedWarehouseStateProvisioning,
		S3State:            configstore.ManagedWarehouseStateReady,
		MetadataStoreState: configstore.ManagedWarehouseStatePending,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/analytics/warehouse/status", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp warehouseStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected provisioning state, got %q", resp.State)
	}
	if resp.S3State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected s3 ready, got %q", resp.S3State)
	}
}

func TestGetWarehouseNotFound(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/unknown/warehouse/status", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}
