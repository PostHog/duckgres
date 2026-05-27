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
	orgs                  map[string]*configstore.Org
	users                 map[configstore.OrgUserKey]string
	warehouses            map[string]*configstore.ManagedWarehouse
	trino                 map[string]*configstore.ManagedWarehouseTrino
	provisionUserFailHook error // set non-nil to simulate user-step failure inside Provision
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		orgs:       make(map[string]*configstore.Org),
		users:      make(map[configstore.OrgUserKey]string),
		warehouses: make(map[string]*configstore.ManagedWarehouse),
		trino:      make(map[string]*configstore.ManagedWarehouseTrino),
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
	shadowTrino, hadTrino := s.trino[req.OrgID]

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

	// 3. Optional Trino
	if req.Trino != nil {
		s.trino[req.OrgID] = &configstore.ManagedWarehouseTrino{
			OrgID:   req.OrgID,
			Enabled: true,
			Tier:    req.Trino.Tier,
			State:   configstore.ManagedWarehouseStatePending,
		}
	}

	// Reference the shadow vars so the linter doesn't complain about
	// declared-and-unused on the success path.
	_, _, _, _ = shadowUserHash, hadUser, shadowTrino, hadTrino
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

func (s *fakeStore) EnableTrino(orgID string, settings configstore.TrinoSettings) error {
	s.trino[orgID] = &configstore.ManagedWarehouseTrino{
		OrgID:   orgID,
		Enabled: true,
		Tier:    settings.Tier,
	}
	return nil
}

func (s *fakeStore) DisableTrino(orgID string) error {
	if row, ok := s.trino[orgID]; ok {
		row.Enabled = false
	}
	return nil
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

// TestProvisionRejectsAurora locks in that DuckLake (aurora) is no longer
// provisionable — even with a valid sizing block — now that cnpg-shard is the
// only creatable backend. Existing DuckLake deployments are unaffected; this
// only gates new creation.
func TestProvisionRejectsAurora(t *testing.T) {
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

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.warehouses["analytics"]; ok {
		t.Error("warehouse must not be created for a DuckLake (aurora) metadata store")
	}
}

func TestProvisionAutoCreatesOrg(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "cnpg-shard"}}`)
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

func TestProvisionRejectsExistingNonTerminal(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateProvisioning,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "cnpg-shard"}}`)
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
	store.users[configstore.OrgUserKey{OrgID: "analytics", Username: "root"}] = "old-hash"
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateFailed,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if store.warehouses["analytics"].MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("expected cnpg-shard warehouse after retry, got kind %q", store.warehouses["analytics"].MetadataStore.Kind)
	}
}

func TestProvisionAllowsRetryAfterDeleted(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.users[configstore.OrgUserKey{OrgID: "analytics", Username: "root"}] = "old-hash"
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateDeleted,
	}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
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

func TestProvisionEnablesTrinoWhenRequested(t *testing.T) {
	store := newFakeStore()
	// Trino enable requires a numeric org id (posthog team_id).
	store.orgs["42"] = &configstore.Org{Name: "42"}
	router := newTestRouter(store)

	// cnpg-shard is the only provisionable backend post-#627. The
	// provision handler auto-enables Iceberg (Lakekeeper) for any
	// cnpg-shard warehouse, which is the prerequisite for Trino.
	body := []byte(`{
		"database_name": "team-42-db",
		"metadata_store": {"type": "cnpg-shard"},
		"trino": {"enabled": true, "tier": "free"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/42/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	row := store.trino["42"]
	if row == nil || !row.Enabled || row.Tier != "free" {
		t.Fatalf("expected trino row with Enabled=true, Tier=free; got %+v", row)
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

	body := []byte(`{"database_name": "team-7-db", "metadata_store": {"type": "cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/7/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (transactional failure): %s", rec.Code, rec.Body.String())
	}

	// After rollback: no warehouse row, no user row, no Trino row,
	// no Org row. Retry would treat this as a brand-new provision.
	if _, ok := store.warehouses["7"]; ok {
		t.Errorf("expected warehouse to be rolled back, got %+v", store.warehouses["7"])
	}
	if _, ok := store.users[configstore.OrgUserKey{OrgID: "7", Username: "root"}]; ok {
		t.Errorf("expected user row to be absent after rollback")
	}
	if _, ok := store.orgs["7"]; ok {
		t.Errorf("expected org row to be rolled back, got %+v", store.orgs["7"])
	}

	// Now clear the hook and retry — should succeed with a clean
	// fresh attempt (proving the rolled-back state didn't poison the
	// retry).
	store.setProvisionUserFailHook(nil)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/7/provision", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202: %s", rec2.Code, rec2.Body.String())
	}
	if _, ok := store.warehouses["7"]; !ok {
		t.Errorf("expected warehouse to be created on retry")
	}
}

func TestProvisionRejectsTrinoWithNonNumericOrgID(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "analytics-db",
		"metadata_store": {"type": "cnpg-shard"},
		"trino": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 with non-numeric org id: %s", rec.Code, rec.Body.String())
	}
}

func TestProvisionWithoutTrinoLeavesTrinoRowUnset(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if _, ok := store.trino["analytics"]; ok {
		t.Fatalf("expected no trino row when request omits trino block")
	}
}

func TestEnableTrinoStandaloneEndpoint(t *testing.T) {
	store := newFakeStore()
	store.orgs["42"] = &configstore.Org{Name: "42"}
	router := newTestRouter(store)

	body := []byte(`{"enabled": true, "tier": "growth"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/42/trino", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	row := store.trino["42"]
	if row == nil || !row.Enabled || row.Tier != "growth" {
		t.Fatalf("expected trino row enabled with tier growth; got %+v", row)
	}
}

func TestEnableTrinoRejectsNonNumericOrgID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"enabled": true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/trino", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 with non-numeric org id: %s", rec.Code, rec.Body.String())
	}
}

func TestEnableTrinoRejectsEnabledFalse(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"enabled": false}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/trino", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestDisableTrinoEndpoint(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.trino["analytics"] = &configstore.ManagedWarehouseTrino{OrgID: "analytics", Enabled: true, Tier: "free"}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/orgs/analytics/trino", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	row := store.trino["analytics"]
	if row == nil || row.Enabled {
		t.Fatalf("expected trino row to be present but disabled; got %+v", row)
	}
}

func TestDisableTrinoOnMissingRowIsNoOp(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/orgs/unknown/trino", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
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
	store.orgs["shardco"] = &configstore.Org{Name: "shardco"}
	router := newTestRouter(store)

	// No aurora block — cnpg-shard takes no sizing and auto-enables iceberg.
	body := []byte(`{"database_name": "shardco-db", "metadata_store": {"type": "cnpg-shard"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/shardco/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses["shardco"]
	if w == nil {
		t.Fatal("expected warehouse to be created")
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Errorf("metadata store kind = %q, want cnpg-shard", w.MetadataStore.Kind)
	}
	if !w.Iceberg.Enabled || w.Iceberg.Backend != configstore.IcebergBackendLakekeeper {
		t.Errorf("expected iceberg enabled with lakekeeper backend, got %+v", w.Iceberg)
	}
	if w.AuroraMaxACU != 0 {
		t.Errorf("cnpg-shard must not set aurora sizing, got max_acu=%f", w.AuroraMaxACU)
	}
}

func TestProvisionRejectsExternalMetadataStore(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "ext-db", "metadata_store": {"type": "external"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/extco/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.warehouses["extco"]; ok {
		t.Error("warehouse must not be created for an external metadata store")
	}
}

func TestProvisionRejectsUnsupportedMetadataStore(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "x-db", "metadata_store": {"type": "neon"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/xco/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}
