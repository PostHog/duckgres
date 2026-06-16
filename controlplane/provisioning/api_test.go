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

const testOrgID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"

type fakeStore struct {
	orgs                  map[string]*configstore.Org
	users                 map[configstore.OrgUserKey]string
	warehouses            map[string]*configstore.ManagedWarehouse
	provisionUserFailHook error // set non-nil to simulate user-step failure inside Provision
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		orgs:       make(map[string]*configstore.Org),
		users:      make(map[configstore.OrgUserKey]string),
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
	key := configstore.OrgUserKey{OrgID: orgID, Username: username}
	s.users[key] = passwordHash
	return nil
}

func (s *fakeStore) UpdateOrgUserPassword(orgID, username, passwordHash string) error {
	key := configstore.OrgUserKey{OrgID: orgID, Username: username}
	if _, exists := s.users[key]; !exists {
		return fmt.Errorf("user %q not found in org %q", username, orgID)
	}
	s.users[key] = passwordHash
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
		return ErrWarehouseNonTerminal
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

// provisionUserFailHook, when non-nil, triggers a simulated failure
// during the user-creation step of Provision. The fake uses it to
// exercise rollback semantics — see TestProvisionTransactionRollsBack.
// Set to a non-nil error to fail; set to nil (default) for success.
//
// Real prod failure modes (DB write failure, conflict between checks)
// are handled by the *gormStore* implementation's transaction
// boundary; the fake fakes the boundary so tests can verify the
// handler treats partial failure as a complete rollback.
func (s *fakeStore) setProvisionUserFailHook(err error) { s.provisionUserFailHook = err }

func (s *fakeStore) Provision(req ProvisionRequest) error {
	// Pre-check: warehouse already exists in non-terminal state? Mirrors
	// createPendingWarehouseTx so the handler's 409 branch is exercised.
	if existing, ok := s.warehouses[req.OrgID]; ok &&
		existing.State != configstore.ManagedWarehouseStateFailed &&
		existing.State != configstore.ManagedWarehouseStateDeleted {
		return ErrWarehouseNonTerminal
	}

	// Stage the writes into shadow maps so a mid-step failure can roll
	// back without leaving partial state.
	shadowOrg := s.orgs[req.OrgID]
	shadowWarehouse := s.warehouses[req.OrgID]
	shadowUserHash, hadUser := s.users[configstore.OrgUserKey{OrgID: req.OrgID, Username: "root"}]

	// 1. Warehouse + Org
	if _, ok := s.orgs[req.OrgID]; !ok {
		s.orgs[req.OrgID] = &configstore.Org{Name: req.OrgID, DatabaseName: req.DatabaseName}
	}
	clone := *req.Warehouse
	clone.OrgID = req.OrgID
	clone.State = configstore.ManagedWarehouseStatePending
	clone.WarehouseDatabaseState = configstore.ManagedWarehouseStatePending
	clone.MetadataStoreState = configstore.ManagedWarehouseStatePending
	clone.S3State = configstore.ManagedWarehouseStatePending
	clone.IdentityState = configstore.ManagedWarehouseStatePending
	clone.SecretsState = configstore.ManagedWarehouseStatePending
	s.warehouses[req.OrgID] = &clone

	// 2. User — with injection hook for the rollback test
	if s.provisionUserFailHook != nil {
		// Roll back step 1
		if shadowOrg == nil {
			delete(s.orgs, req.OrgID)
		} else {
			s.orgs[req.OrgID] = shadowOrg
		}
		if shadowWarehouse == nil {
			delete(s.warehouses, req.OrgID)
		} else {
			s.warehouses[req.OrgID] = shadowWarehouse
		}
		return s.provisionUserFailHook
	}
	s.users[configstore.OrgUserKey{OrgID: req.OrgID, Username: "root"}] = req.RootUserHash

	// Reference the shadow vars so the linter doesn't complain about
	// declared-and-unused on the success path.
	_, _ = shadowUserHash, hadUser
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

// TestProvisionRejectsAurora locks in that the removed "aurora" metadata-store
// backend is no longer provisionable — the only creatable backends are
// cnpg-shard and external.
func TestProvisionRejectsAurora(t *testing.T) {
	store := newFakeStore()
	store.orgs[testOrgID] = &configstore.Org{Name: testOrgID}
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "analytics-db",
		"metadata_store": {"type": "aurora"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (aurora removed): %s", rec.Code, rec.Body.String())
	}
	if store.warehouses[testOrgID] != nil {
		t.Fatal("aurora request must not create a warehouse")
	}
}

func TestProvisionAutoCreatesOrg(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if _, ok := store.orgs[testOrgID]; !ok {
		t.Fatal("expected org to be auto-created")
	}
	if store.warehouses[testOrgID] == nil {
		t.Fatal("expected warehouse to be created")
	}
}

func TestProvisionRejectsEmptyBody(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestProvisionRejectsExistingNonTerminal(t *testing.T) {
	store := newFakeStore()
	store.orgs[testOrgID] = &configstore.Org{Name: testOrgID}
	store.warehouses[testOrgID] = &configstore.ManagedWarehouse{
		OrgID: testOrgID,
		State: configstore.ManagedWarehouseStateProvisioning,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestProvisionAllowsRetryAfterFailure(t *testing.T) {
	store := newFakeStore()
	store.orgs[testOrgID] = &configstore.Org{Name: testOrgID}
	store.users[configstore.OrgUserKey{OrgID: testOrgID, Username: "root"}] = "old-hash"
	store.warehouses[testOrgID] = &configstore.ManagedWarehouse{
		OrgID: testOrgID,
		State: configstore.ManagedWarehouseStateFailed,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses[testOrgID].MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("expected cnpg-shard warehouse after retry, got kind %q", store.warehouses[testOrgID].MetadataStore.Kind)
	}
}

func TestProvisionAllowsRetryAfterDeleted(t *testing.T) {
	store := newFakeStore()
	store.orgs[testOrgID] = &configstore.Org{Name: testOrgID}
	store.users[configstore.OrgUserKey{OrgID: testOrgID, Username: "root"}] = "old-hash"
	store.warehouses[testOrgID] = &configstore.ManagedWarehouse{
		OrgID: testOrgID,
		State: configstore.ManagedWarehouseStateDeleted,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
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

func TestProvisionTransactionRollsBackOnUserFailure(t *testing.T) {
	// Pattern A's whole point: when a downstream write inside the
	// transactional Provision fails, the upstream writes must roll
	// back too. The fake exposes a hook that simulates a user-step
	// failure; the caller's retry must see a clean starting state, not
	// a half-provisioned warehouse blocking re-creation.
	store := newFakeStore()
	store.setProvisionUserFailHook(errors.New("simulated DB write failure"))
	router := newTestRouter(store)

	body := []byte(`{"database_name": "team-7-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (transactional failure): %s", rec.Code, rec.Body.String())
	}

	// After rollback: no warehouse row, no user row, no Org row.
	// Retry would treat this as a brand-new provision.
	if _, ok := store.warehouses[testOrgID]; ok {
		t.Errorf("expected warehouse to be rolled back, got %+v", store.warehouses[testOrgID])
	}
	if _, ok := store.users[configstore.OrgUserKey{OrgID: testOrgID, Username: "root"}]; ok {
		t.Errorf("expected user row to be absent after rollback")
	}
	if _, ok := store.orgs[testOrgID]; ok {
		t.Errorf("expected org row to be rolled back, got %+v", store.orgs[testOrgID])
	}

	// Now clear the hook and retry — should succeed with a clean
	// fresh attempt (proving the rolled-back state didn't poison the
	// retry).
	store.setProvisionUserFailHook(nil)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202: %s", rec2.Code, rec2.Body.String())
	}
	if _, ok := store.warehouses[testOrgID]; !ok {
		t.Errorf("expected warehouse to be created on retry")
	}
}

func TestResetPasswordRequiresReadyWarehouse(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.users[configstore.OrgUserKey{OrgID: "analytics", Username: "root"}] = "old-hash"
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateDeleted,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/reset-password", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

func TestProvisionCnpgShard(t *testing.T) {
	store := newFakeStore()
	store.orgs[testOrgID] = &configstore.Org{Name: testOrgID}
	router := newTestRouter(store)

	// cnpg-shard takes no sizing and auto-enables iceberg.
	body := []byte(`{"database_name": "shardco-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses[testOrgID]
	if w == nil {
		t.Fatal("expected warehouse to be created")
		return
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Errorf("metadata store kind = %q, want cnpg-shard", w.MetadataStore.Kind)
	}
	if !w.Iceberg.Enabled || w.Iceberg.Backend != configstore.IcebergBackendLakekeeper {
		t.Errorf("expected iceberg enabled with lakekeeper backend, got %+v", w.Iceberg)
	}
}

func TestProvisionIcebergExternal(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "extice-db",
		"metadata_store": {"type": "external", "external": {
			"endpoint": "rds.example.us-east-1.rds.amazonaws.com",
			"password_aws_secret": "duckling-example-rds-password",
			"user": "postgres", "database": "postgres"
		}},
		"data_store": {"type": "external", "bucket_name": "posthog-duckling-example", "region": "us-east-1"},
		"iceberg": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses[testOrgID]
	if w == nil {
		t.Fatal("expected warehouse to be created")
		return
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindExternal {
		t.Errorf("metadata store kind = %q, want external", w.MetadataStore.Kind)
	}
	if w.MetadataStore.Endpoint != "rds.example.us-east-1.rds.amazonaws.com" || w.MetadataStore.PasswordAWSSecret != "duckling-example-rds-password" {
		t.Errorf("external creds not persisted: %+v", w.MetadataStore)
	}
	if w.MetadataStore.Username != "postgres" || w.MetadataStore.DatabaseName != "postgres" {
		t.Errorf("user/database not persisted: %+v", w.MetadataStore)
	}
	if w.DataStore.Kind != "external" || w.DataStore.BucketName != "posthog-duckling-example" || w.DataStore.Region != "us-east-1" {
		t.Errorf("data store not persisted: %+v", w.DataStore)
	}
	if !w.Iceberg.Enabled || w.Iceberg.Backend != configstore.IcebergBackendLakekeeper {
		t.Errorf("expected iceberg enabled with lakekeeper backend, got %+v", w.Iceberg)
	}
	// Decoupled: iceberg without a ducklake flag is iceberg-ONLY (no implicit DuckLake).
	if w.DuckLake.Enabled {
		t.Errorf("iceberg+external without a ducklake flag must NOT enable DuckLake; got ducklake=%v", w.DuckLake.Enabled)
	}
}

// TestProvisionRejectsNoCatalog verifies the ≥1-catalog gate: a duckling with
// neither ducklake nor iceberg is rejected.
func TestProvisionRejectsNoCatalog(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	body := []byte(`{"database_name":"nc-db","metadata_store":{"type":"cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if _, ok := store.warehouses[testOrgID]; ok {
		t.Error("warehouse must not be created with no catalog enabled")
	}
}

// TestProvisionCnpgDuckLakeAndIceberg verifies the fully-decoupled combo:
// cnpg-shard with both catalogs.
func TestProvisionCnpgDuckLakeAndIceberg(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	body := []byte(`{"database_name":"both-db","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true},"iceberg":{"enabled":true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	w := store.warehouses[testOrgID]
	if w == nil || w.MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("expected cnpg-shard warehouse, got %+v", w)
	}
	if !w.DuckLake.Enabled || !w.Iceberg.Enabled {
		t.Errorf("expected both ducklake and iceberg enabled; got ducklake=%v iceberg=%v", w.DuckLake.Enabled, w.Iceberg.Enabled)
	}
}

func TestProvisionDuckLakeExternal(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	// ducklake on, no iceberg → DuckLake-only on external.
	body := []byte(`{
		"database_name": "extdl-db",
		"metadata_store": {"type": "external", "external": {
			"endpoint": "rds.example.us-east-1.rds.amazonaws.com",
			"password_aws_secret": "duckling-example-rds-password"
		}},
		"data_store": {"type": "external", "bucket_name": "posthog-duckling-example", "region": "us-east-1"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses[testOrgID]
	if w == nil {
		t.Fatal("expected warehouse to be created")
		return
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindExternal {
		t.Errorf("metadata store kind = %q, want external", w.MetadataStore.Kind)
	}
	if !w.DuckLake.Enabled || w.Iceberg.Enabled {
		t.Errorf("ducklake-only+external: want ducklake on, iceberg off; got ducklake=%v iceberg=%v", w.DuckLake.Enabled, w.Iceberg.Enabled)
	}
}

func TestProvisionExternalRequiresEndpointAndSecret(t *testing.T) {
	for name, body := range map[string]string{
		"missing external block": `{"database_name":"e-db","ducklake":{"enabled":true},"metadata_store":{"type":"external"}}`,
		"missing endpoint":       `{"database_name":"e-db","ducklake":{"enabled":true},"metadata_store":{"type":"external","external":{"password_aws_secret":"s"}}}`,
		"missing secret":         `{"database_name":"e-db","ducklake":{"enabled":true},"metadata_store":{"type":"external","external":{"endpoint":"h"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			router := newTestRouter(store)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader([]byte(body)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if _, ok := store.warehouses[testOrgID]; ok {
				t.Error("warehouse must not be created when required external fields are missing")
			}
		})
	}
}

func TestProvisionExternalDataStoreRequiresBucket(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	// data_store.type=external without bucket_name → 400.
	body := []byte(`{"database_name":"e-db","ducklake":{"enabled":true},"metadata_store":{"type":"external","external":{"endpoint":"h","password_aws_secret":"s"}},"data_store":{"type":"external"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestProvisionRejectsUnsupportedMetadataStore(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "x-db", "metadata_store": {"type": "neon"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestProvisionRejectsInvalidOrgID(t *testing.T) {
	for _, bad := range []string{
		"analytics",
		"ben.iceberg",
		"Ben-Iceberg",
		"ben_iceberg",
		"-bad",
		"bad-",
		"f47ac10b58cc4372a5670e02b2c3d479",
		"f47ac10b-58cc-4372-a567-0e02b2c3d47z",
	} {
		t.Run(bad, func(t *testing.T) {
			store := newFakeStore()
			router := newTestRouter(store)
			body := []byte(`{"database_name":"d","metadata_store":{"type":"cnpg-shard"},"iceberg":{"enabled":true}}`)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+bad+"/provision", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("orgID %q: status = %d, want 400: %s", bad, rec.Code, rec.Body.String())
			}
			if _, ok := store.warehouses[bad]; ok {
				t.Errorf("warehouse must not be created for invalid org id %q", bad)
			}
		})
	}
}

func TestProvisionAcceptsCanonicalHyphenatedUUIDOrgID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	body := []byte(`{"database_name":"d","metadata_store":{"type":"cnpg-shard"},"iceberg":{"enabled":true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+testOrgID+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("canonical hyphenated UUID org id should be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
}
