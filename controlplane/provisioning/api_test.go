package provisioning

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

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

	// 1. Warehouse + Org. Mirrors createPendingWarehouseTx: a NEW org
	// requires default_team_id (rejected before any write); an existing org
	// keeps its stored value when the field is omitted (set-only, never wipe).
	if _, ok := s.orgs[req.OrgID]; !ok {
		if req.DefaultTeamID == 0 {
			return ErrDefaultTeamIDRequired
		}
		s.orgs[req.OrgID] = &configstore.Org{Name: req.OrgID, DatabaseName: req.DatabaseName}
	}
	if req.DefaultTeamID != 0 {
		teamID := req.DefaultTeamID
		s.orgs[req.OrgID].DefaultTeamID = &teamID
	}
	clone := *req.Warehouse
	clone.OrgID = req.OrgID
	clone.State = configstore.ManagedWarehouseStatePending
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
	return newTestRouterWithBucketSuffix(store, "")
}

func newTestRouterWithBucketSuffix(store Store, bucketSuffix string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterAPI(r.Group("/api/v1"), store, bucketSuffix)
	return r
}

// TestProvisionRejectsAurora locks in that the removed "aurora" metadata-store
// backend is no longer provisionable — the only creatable backends are
// cnpg-shard and external.
func TestProvisionRejectsAurora(t *testing.T) {
	store := newFakeStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "analytics-db",
		"metadata_store": {"type": "aurora"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/analytics/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (aurora removed): %s", rec.Code, rec.Body.String())
	}
	if store.warehouses["analytics"] != nil {
		t.Fatal("aurora request must not create a warehouse")
	}
}

func TestProvisionAutoCreatesOrg(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "test-db", "default_team_id": 1, "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
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

// TestProvisionPersistsDefaultTeamID checks the default_team_id in the
// provision body is threaded through to the org record.
func TestProvisionPersistsDefaultTeamID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "team-db", "default_team_id": 12345, "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/team-org/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	org, ok := store.orgs["team-org"]
	if !ok {
		t.Fatal("expected org to be auto-created")
	}
	if org.DefaultTeamID == nil {
		t.Fatal("expected default_team_id to be set, got nil")
	}
	if *org.DefaultTeamID != 12345 {
		t.Fatalf("default_team_id = %d, want %d", *org.DefaultTeamID, 12345)
	}
}

// TestProvisionNewOrgRequiresDefaultTeamID locks in the mandatory contract:
// provisioning a NEW org without default_team_id is rejected with 400 and
// creates nothing (no org, no warehouse) — every org carries its default team
// id from birth.
func TestProvisionNewOrgRequiresDefaultTeamID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "noteam-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/noteam-org/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "default_team_id") {
		t.Fatalf("error body should name default_team_id: %s", rec.Body.String())
	}
	if _, ok := store.orgs["noteam-org"]; ok {
		t.Fatal("org must not be created when default_team_id is missing")
	}
	if store.warehouses["noteam-org"] != nil {
		t.Fatal("warehouse must not be created when default_team_id is missing")
	}
}

// TestReprovisionExistingOrgKeepsDefaultTeamID: re-provisioning an EXISTING org
// without default_team_id stays valid (not the new-org 400) and keeps the
// stored value — the field is set-only, never wiped by omission.
func TestReprovisionExistingOrgKeepsDefaultTeamID(t *testing.T) {
	store := newFakeStore()
	teamID := int64(777)
	store.orgs["backfilled"] = &configstore.Org{Name: "backfilled", DefaultTeamID: &teamID}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "backfilled-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/backfilled/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	org := store.orgs["backfilled"]
	if org.DefaultTeamID == nil || *org.DefaultTeamID != teamID {
		t.Fatalf("default_team_id must survive re-provision without the field, got %v", org.DefaultTeamID)
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

	body := []byte(`{"database_name": "test-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
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

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
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

	body := []byte(`{"database_name": "analytics-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
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

func TestProvisionTransactionRollsBackOnUserFailure(t *testing.T) {
	// Pattern A's whole point: when a downstream write inside the
	// transactional Provision fails, the upstream writes must roll
	// back too. The fake exposes a hook that simulates a user-step
	// failure; the caller's retry must see a clean starting state, not
	// a half-provisioned warehouse blocking re-creation.
	store := newFakeStore()
	store.setProvisionUserFailHook(errors.New("simulated DB write failure"))
	router := newTestRouter(store)

	body := []byte(`{"database_name": "team-7-db", "default_team_id": 7, "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/7/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (transactional failure): %s", rec.Code, rec.Body.String())
	}

	// After rollback: no warehouse row, no user row, no Org row.
	// Retry would treat this as a brand-new provision.
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

	// cnpg-shard takes no sizing.
	body := []byte(`{"database_name": "shardco-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
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
		return
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		t.Errorf("metadata store kind = %q, want cnpg-shard", w.MetadataStore.Kind)
	}
	if !w.DuckLake.Enabled {
		t.Errorf("expected ducklake enabled, got %+v", w.DuckLake)
	}
}

// TestProvisionRequiresDuckLakeEnabled verifies the catalog gate: DuckLake is
// the only catalog (Iceberg support was removed), so any provision request
// without ducklake.enabled=true — including a legacy iceberg-only body — is
// rejected with the explicit error message.
func TestProvisionRequiresDuckLakeEnabled(t *testing.T) {
	for name, body := range map[string]string{
		"no catalog block":         `{"database_name":"nc-db","metadata_store":{"type":"cnpg-shard"}}`,
		"ducklake disabled":        `{"database_name":"nc-db","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":false}}`,
		"legacy iceberg-only body": `{"database_name":"nc-db","metadata_store":{"type":"cnpg-shard"},"iceberg":{"enabled":true}}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			router := newTestRouter(store)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/ncco/provision", bytes.NewReader([]byte(body)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "ducklake.enabled must be true") {
				t.Fatalf("error = %s, want \"ducklake.enabled must be true\"", rec.Body.String())
			}
			if _, ok := store.warehouses["ncco"]; ok {
				t.Error("warehouse must not be created without ducklake enabled")
			}
		})
	}
}

func TestProvisionDuckLakeExternal(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{
		"database_name": "extdl-db",
		"default_team_id": 1,
		"metadata_store": {"type": "external", "external": {
			"endpoint": "rds.example.us-east-1.rds.amazonaws.com",
			"password_aws_secret": "duckling-example-rds-password",
			"user": "postgres", "database": "postgres"
		}},
		"data_store": {"type": "external", "bucket_name": "posthog-duckling-example", "region": "us-east-1"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/extdl/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses["extdl"]
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
	if !w.DuckLake.Enabled {
		t.Errorf("ducklake+external: want ducklake on; got ducklake=%v", w.DuckLake.Enabled)
	}
}

// When a bucket suffix is configured, a fresh per-org s3bucket gets the
// CP-owned name pinned on the warehouse (→ Duckling CR spec.dataStore.bucketName)
// and returned in the provision response, so callers persist it instead of
// re-deriving. UUID org IDs are hyphen-compacted to fit the S3 63-char cap.
func TestProvisionComputesS3BucketName(t *testing.T) {
	store := newFakeStore()
	router := newTestRouterWithBucketSuffix(store, "mw-prod-us")

	org := "0194d640-5db4-0000-6cde-48d6114c0f99"
	wantBucket := "posthog-duckling-0194d6405db400006cde48d6114c0f99-mw-prod-us"
	body := []byte(`{
		"database_name": "db",
		"default_team_id": 1,
		"metadata_store": {"type": "cnpg-shard"},
		"data_store": {"type": "s3bucket"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+org+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	w := store.warehouses[org]
	if w == nil {
		t.Fatal("expected warehouse to be created")
		return
	}
	if w.DataStore.BucketName != wantBucket {
		t.Errorf("warehouse DataStore.BucketName = %q, want %q", w.DataStore.BucketName, wantBucket)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["bucket"] != wantBucket {
		t.Errorf("response bucket = %v, want %q", resp["bucket"], wantBucket)
	}
}

// Without a configured suffix the CP doesn't name buckets — the warehouse keeps
// an empty bucket name and the composition derives it (legacy behavior).
func TestProvisionNoBucketSuffixLeavesNameEmpty(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store) // empty suffix

	body := []byte(`{
		"database_name": "db",
		"default_team_id": 1,
		"metadata_store": {"type": "cnpg-shard"},
		"data_store": {"type": "s3bucket"},
		"ducklake": {"enabled": true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/someorg/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if w := store.warehouses["someorg"]; w == nil || w.DataStore.BucketName != "" {
		t.Errorf("want empty DataStore.BucketName, got %+v", w)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if _, ok := resp["bucket"]; ok {
		t.Errorf("response should omit bucket when CP naming is disabled, got %v", resp["bucket"])
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
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/eco/provision", bytes.NewReader([]byte(body)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if _, ok := store.warehouses["eco"]; ok {
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
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/eco/provision", bytes.NewReader(body))
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

	body := []byte(`{"database_name": "x-db", "metadata_store": {"type": "neon"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/xco/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestProvisionRejectsInvalidOrgID(t *testing.T) {
	for _, bad := range []string{"my.org", "My-Org", "my_org", "-bad", "bad-"} {
		t.Run(bad, func(t *testing.T) {
			store := newFakeStore()
			router := newTestRouter(store)
			body := []byte(`{"database_name":"d","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`)
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

func TestProvisionRejectsOverlongSlugOrgID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	org := strings.Repeat("a", 36)
	body := []byte(`{
		"database_name":"d",
		"metadata_store":{"type":"external","external":{"endpoint":"h","password_aws_secret":"s"}},
		"data_store":{"type":"external","bucket_name":"posthog-duckling-example"},
		"ducklake":{"enabled":true}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/"+org+"/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "canonical UUID or a slug") {
		t.Fatalf("error = %s, want org id slug length error", rec.Body.String())
	}
	if _, ok := store.warehouses[org]; ok {
		t.Error("warehouse must not be created for overlong slug org id")
	}
}

func TestProvisionAcceptsHyphenatedOrgID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	body := []byte(`{"database_name":"d","default_team_id":1,"metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/my-org-cnpg/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("hyphenated org id should be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestProvisionRejectsStringDefaultTeamID pins the wire contract:
// default_team_id is a JSON NUMBER (PostHog's integer Team.id). A quoted
// string is a decode-time 400 naming the field, and creates nothing.
func TestProvisionRejectsStringDefaultTeamID(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "strteam-db", "default_team_id": "12345", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/strteam-org/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "default_team_id") {
		t.Fatalf("error should name default_team_id: %s", rec.Body.String())
	}
	if _, ok := store.orgs["strteam-org"]; ok {
		t.Fatal("org must not be created on a malformed default_team_id")
	}
}
