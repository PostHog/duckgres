package provisioning

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

type fakeStore struct {
	orgs                  map[string]*configstore.Org
	users                 map[configstore.OrgUserKey]string
	warehouses            map[string]*configstore.ManagedWarehouse
	teams                 map[string]map[int64]*configstore.OrgTeam
	provisionUserFailHook error // set non-nil to simulate user-step failure inside Provision
	// lastProvision records the ProvisionRequest the handler dispatched, so
	// tests can assert the request-field → store-request threading (the real
	// billing-row/schema writes are covered by the Postgres-backed tests).
	lastProvision *ProvisionRequest

	listWarehousesErr error // set non-nil to fail ListWarehousesByStates
	listOrgTeamsErr   error // set non-nil to fail ListOrgTeamsByOrgIDs
	latestChangeErr   error // set non-nil to fail LatestConfigChange
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		orgs:       make(map[string]*configstore.Org),
		users:      make(map[configstore.OrgUserKey]string),
		warehouses: make(map[string]*configstore.ManagedWarehouse),
		teams:      make(map[string]map[int64]*configstore.OrgTeam),
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

// seedTeam plants one team row (nil billing/backfill unless set by the caller).
func (s *fakeStore) seedTeam(team configstore.OrgTeam) {
	if s.teams[team.OrgID] == nil {
		s.teams[team.OrgID] = make(map[int64]*configstore.OrgTeam)
	}
	clone := team
	s.teams[team.OrgID][team.TeamID] = &clone
}

func (s *fakeStore) ListOrgTeams(orgID string) ([]configstore.OrgTeam, error) {
	if _, ok := s.orgs[orgID]; !ok {
		return nil, gorm.ErrRecordNotFound
	}
	var out []configstore.OrgTeam
	for _, t := range s.teams[orgID] {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TeamID < out[j].TeamID })
	return out, nil
}

// UpsertOrgTeam mirrors configstore.UpsertOrgTeamTx's contract in memory:
// unknown org → gorm.ErrRecordNotFound, schema held by another team →
// ErrOrgTeamSchemaConflict, presence-aware merge otherwise.
func (s *fakeStore) UpsertOrgTeam(orgID string, up configstore.OrgTeamUpsert) (*configstore.OrgTeam, error) {
	if _, ok := s.orgs[orgID]; !ok {
		return nil, gorm.ErrRecordNotFound
	}
	for _, t := range s.teams[orgID] {
		if t.SchemaName == up.SchemaName && t.TeamID != up.TeamID {
			return nil, configstore.ErrOrgTeamSchemaConflict
		}
	}
	if s.teams[orgID] == nil {
		s.teams[orgID] = make(map[int64]*configstore.OrgTeam)
	}
	existing, ok := s.teams[orgID][up.TeamID]
	if !ok {
		backfill := up.BackfillEnabled == nil || *up.BackfillEnabled
		row := &configstore.OrgTeam{
			OrgID:           orgID,
			TeamID:          up.TeamID,
			SchemaName:      up.SchemaName,
			Enabled:         up.Enabled == nil || *up.Enabled,
			BackfillEnabled: &backfill,
		}
		applyFakeTableNames(row, up)
		if up.EarliestEventDateSet {
			row.EarliestEventDate = up.EarliestEventDate
		}
		s.teams[orgID][up.TeamID] = row
		clone := *row
		return &clone, nil
	}
	existing.SchemaName = up.SchemaName
	if up.Enabled != nil {
		existing.Enabled = *up.Enabled
	}
	if up.BackfillEnabled != nil {
		existing.BackfillEnabled = up.BackfillEnabled
	}
	applyFakeTableNames(existing, up)
	if up.EarliestEventDateSet {
		existing.EarliestEventDate = up.EarliestEventDate
	}
	clone := *existing
	return &clone, nil
}

func applyFakeTableNames(row *configstore.OrgTeam, up configstore.OrgTeamUpsert) {
	set := func(dst **string, src *string) {
		if src == nil {
			return
		}
		if *src == "" {
			*dst = nil
			return
		}
		v := *src
		*dst = &v
	}
	set(&row.EventsTableName, up.EventsTableName)
	set(&row.PersonsTableName, up.PersonsTableName)
	set(&row.SchemaDataImportsName, up.SchemaDataImportsName)
}

// DeleteOrgTeam mirrors configstore.DeleteOrgTeamTx's rules in memory: 404 for
// a missing row, refusal on the last team, billing handover to the remaining
// team with the oldest created_at.
func (s *fakeStore) DeleteOrgTeam(orgID string, teamID int64) (*configstore.OrgTeamDeleteResult, error) {
	target, ok := s.teams[orgID][teamID]
	if !ok {
		return nil, configstore.ErrOrgTeamNotFound
	}
	if len(s.teams[orgID]) == 1 {
		return nil, configstore.ErrLastOrgTeam
	}
	delete(s.teams[orgID], teamID)
	res := &configstore.OrgTeamDeleteResult{
		WasBilling: target.IsBillingTeam != nil && *target.IsBillingTeam,
	}
	if res.WasBilling {
		var successor *configstore.OrgTeam
		for _, t := range s.teams[orgID] {
			if successor == nil || t.CreatedAt.Before(successor.CreatedAt) ||
				(t.CreatedAt.Equal(successor.CreatedAt) && t.TeamID < successor.TeamID) {
				successor = t
			}
		}
		billing := true
		successor.IsBillingTeam = &billing
		res.NewBillingTeamID = successor.TeamID
	}
	return res, nil
}

func (s *fakeStore) Provision(req ProvisionRequest) error {
	reqCopy := req
	s.lastProvision = &reqCopy
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

func (s *fakeStore) ListWarehousesByStates(states []configstore.ManagedWarehouseProvisioningState) ([]configstore.ManagedWarehouse, error) {
	if s.listWarehousesErr != nil {
		return nil, s.listWarehousesErr
	}
	want := make(map[configstore.ManagedWarehouseProvisioningState]struct{}, len(states))
	for _, st := range states {
		want[st] = struct{}{}
	}
	// Deterministic order, mirroring the production ORDER BY org_id.
	ids := make([]string, 0, len(s.warehouses))
	for id := range s.warehouses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []configstore.ManagedWarehouse
	for _, id := range ids {
		if _, ok := want[s.warehouses[id].State]; ok {
			out = append(out, *s.warehouses[id])
		}
	}
	return out, nil
}

func (s *fakeStore) ListOrgTeamsByOrgIDs(orgIDs []string) ([]configstore.OrgTeam, error) {
	if s.listOrgTeamsErr != nil {
		return nil, s.listOrgTeamsErr
	}
	var out []configstore.OrgTeam
	for _, id := range orgIDs {
		ids := make([]int64, 0, len(s.teams[id]))
		for tid := range s.teams[id] {
			ids = append(ids, tid)
		}
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		for _, tid := range ids {
			out = append(out, *s.teams[id][tid])
		}
	}
	return out, nil
}

// LatestConfigChange mirrors production: max updated_at across ALL
// warehouse, org, and org-team rows regardless of state.
func (s *fakeStore) LatestConfigChange() (time.Time, error) {
	if s.latestChangeErr != nil {
		return time.Time{}, s.latestChangeErr
	}
	var latest time.Time
	for _, w := range s.warehouses {
		if w.UpdatedAt.After(latest) {
			latest = w.UpdatedAt
		}
	}
	for _, o := range s.orgs {
		if o.UpdatedAt.After(latest) {
			latest = o.UpdatedAt
		}
	}
	for _, byTeam := range s.teams {
		for _, t := range byTeam {
			if t.UpdatedAt.After(latest) {
				latest = t.UpdatedAt
			}
		}
	}
	return latest, nil
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
	w.StatusMessage = "Deprovisioning..."
	return nil
}

func newTestRouter(store Store) *gin.Engine {
	return newTestRouterWithBucketSuffix(store, "")
}

func newTestRouterWithBucketSuffix(store Store, bucketSuffix string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterAPI(r.Group("/api/v1"), store, bucketSuffix)
	// Mirror prod topology (multitenant.go): discovery is a separate group
	// on the same base path, so both surfaces stay reachable in tests.
	RegisterDiscoveryAPI(r.Group("/api/v1"), store)
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
	// The status_message flips to a live "Deprovisioning..." so a client polling
	// warehouse/status sees progress rather than the stale ready message.
	if got := store.warehouses["analytics"].StatusMessage; got != "Deprovisioning..." {
		t.Fatalf("expected status_message %q, got %q", "Deprovisioning...", got)
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

// --- Org team CRUD (provisioning API) ---

func doJSON(t *testing.T, router *gin.Engine, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestOrgTeamUpsertValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		body       string
		wantStatus int
		wantSubstr string
	}{
		"missing team_id":        {`{"schema_name":"team_1"}`, http.StatusBadRequest, "team_id"},
		"negative team_id":       {`{"team_id":-1,"schema_name":"team_1"}`, http.StatusBadRequest, "team_id"},
		"missing schema_name":    {`{"team_id":1}`, http.StatusBadRequest, "schema_name"},
		"uppercase schema_name":  {`{"team_id":1,"schema_name":"Team_1"}`, http.StatusBadRequest, "lowercase"},
		"hyphenated schema_name": {`{"team_id":1,"schema_name":"team-1"}`, http.StatusBadRequest, "lowercase"},
		"digit-led schema_name":  {`{"team_id":1,"schema_name":"1team"}`, http.StatusBadRequest, "lowercase"},
		"overlong schema_name":   {`{"team_id":1,"schema_name":"` + strings.Repeat("a", 64) + `"}`, http.StatusBadRequest, "63"},
		"invalid earliest_event_date": {
			`{"team_id":1,"schema_name":"team_1","earliest_event_date":"17-04-2023"}`,
			http.StatusBadRequest, "earliest_event_date",
		},
		"null backfill_enabled": {
			`{"team_id":1,"schema_name":"team_1","backfill_enabled":null}`,
			http.StatusBadRequest, "backfill_enabled",
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			store.orgs["acme"] = &configstore.Org{Name: "acme"}
			router := newTestRouter(store)
			rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams", tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("error should mention %q: %s", tc.wantSubstr, rec.Body.String())
			}
		})
	}
}

func TestOrgTeamUpsertCreatesAndLists(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7","backfill_enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var team configstore.OrgTeam
	if err := json.Unmarshal(rec.Body.Bytes(), &team); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if team.TeamID != 7 || team.SchemaName != "team_7" || !team.Enabled {
		t.Fatalf("created team = %+v, want team 7, schema team_7, enabled default true", team)
	}
	if team.BackfillEnabled == nil || !*team.BackfillEnabled {
		t.Fatalf("backfill_enabled = %v, want true", team.BackfillEnabled)
	}

	rec = doJSON(t, router, http.MethodGet, "/api/v1/orgs/acme/teams", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var listing struct {
		Teams []configstore.OrgTeam `json:"teams"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if len(listing.Teams) != 1 || listing.Teams[0].TeamID != 7 {
		t.Fatalf("listing = %+v, want the created team", listing.Teams)
	}
}

func TestOrgTeamListUnknownOrg404(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)
	rec := doJSON(t, router, http.MethodGet, "/api/v1/orgs/nope/teams", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
}

// TestOrgTeamUpsertGrandfathersExistingRow pins the grandfather contract: an
// upsert of an existing (org, team) MAY overwrite schema_name and the legacy
// table names — the PostHog backfill replaces the migration's "team_<id>"
// placeholder through this exact path.
func TestOrgTeamUpsertGrandfathersExistingRow(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 7, SchemaName: "team_7", Enabled: true})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"legacy_wh","events_table_name":"legacy_events","persons_table_name":"legacy_persons","schema_data_imports_name":"legacy_imports"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	stored := store.teams["acme"][7]
	if stored.SchemaName != "legacy_wh" {
		t.Fatalf("schema_name = %q, want overwritten legacy_wh", stored.SchemaName)
	}
	if stored.EventsTableName == nil || *stored.EventsTableName != "legacy_events" ||
		stored.PersonsTableName == nil || *stored.PersonsTableName != "legacy_persons" ||
		stored.SchemaDataImportsName == nil || *stored.SchemaDataImportsName != "legacy_imports" {
		t.Fatalf("legacy table names not stored: %+v", stored)
	}
	if !stored.Enabled {
		t.Fatal("omitted enabled must preserve the stored value on update")
	}
}

// TestOrgTeamUpsertBackfillDefaultsTrue pins the NOT NULL DEFAULT TRUE stance
// of backfill_enabled (migration 000028): a create that omits the field gets
// TRUE, matching the Django BooleanField(default=True) the column mirrors.
func TestOrgTeamUpsertBackfillDefaultsTrue(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"backfill_enabled":true`) {
		t.Fatalf("omitted backfill_enabled must default to true: %s", rec.Body.String())
	}
}

// TestOrgTeamUpsertEarliestEventDateTriState pins the tri-state wire contract
// of PostHog's cached earliest-event date on the upsert: a value sets it (and
// reads back as "YYYY-MM-DD"), an absent key preserves it, an explicit null
// clears it (serialized as null so the sensor sees "not yet resolved").
func TestOrgTeamUpsertEarliestEventDateTriState(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7","earliest_event_date":"2023-04-17"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("set: status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"earliest_event_date":"2023-04-17"`) {
		t.Fatalf("response must carry the date as YYYY-MM-DD: %s", rec.Body.String())
	}

	// Omitted key preserves the stored value.
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("preserve: status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := store.teams["acme"][7].EarliestEventDate; got == nil || got.String() != "2023-04-17" {
		t.Fatalf("omitted earliest_event_date must be preserved, got %v", got)
	}

	// The 9999-12-31 "no event history" sentinel (PostHog's
	// NO_HISTORY_SENTINEL) is stored verbatim — duckgres never interprets it.
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7","earliest_event_date":"9999-12-31"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("sentinel: status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := store.teams["acme"][7].EarliestEventDate; got == nil || got.String() != "9999-12-31" {
		t.Fatalf("sentinel date not stored, got %v", got)
	}

	// Explicit null clears back to NULL, serialized as null.
	rec = doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":7,"schema_name":"team_7","earliest_event_date":null}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("clear: status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := store.teams["acme"][7].EarliestEventDate; got != nil {
		t.Fatalf("earliest_event_date = %v, want NULL after explicit null", got)
	}
	if !strings.Contains(rec.Body.String(), `"earliest_event_date":null`) {
		t.Fatalf("cleared date must serialize as null: %s", rec.Body.String())
	}
}

func TestOrgTeamUpsertDuplicateSchema409(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 1, SchemaName: "team_1", Enabled: true})
	router := newTestRouter(store)

	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/teams",
		`{"team_id":2,"schema_name":"team_1"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "schema_name") {
		t.Fatalf("error should name schema_name: %s", rec.Body.String())
	}
}

func TestOrgTeamDeleteRules(t *testing.T) {
	billing := true
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	t.Run("last team refused", func(t *testing.T) {
		store := newFakeStore()
		store.orgs["acme"] = &configstore.Org{Name: "acme"}
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 1, SchemaName: "team_1", Enabled: true, IsBillingTeam: &billing})
		router := newTestRouter(store)

		rec := doJSON(t, router, http.MethodDelete, "/api/v1/orgs/acme/teams/1", "")
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "deleting the org") {
			t.Fatalf("error should state that deleting the org is the only way: %s", rec.Body.String())
		}
		if store.teams["acme"][1] == nil {
			t.Fatal("last team must not be deleted")
		}
	})

	t.Run("billing team delete promotes oldest remaining", func(t *testing.T) {
		store := newFakeStore()
		store.orgs["acme"] = &configstore.Org{Name: "acme"}
		b := billing
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 1, SchemaName: "team_1", Enabled: true, IsBillingTeam: &b, CreatedAt: base})
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 3, SchemaName: "team_3", Enabled: true, CreatedAt: base.Add(2 * time.Hour)})
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 2, SchemaName: "team_2", Enabled: true, CreatedAt: base.Add(time.Hour)})
		router := newTestRouter(store)

		rec := doJSON(t, router, http.MethodDelete, "/api/v1/orgs/acme/teams/1", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// Team 2 has the oldest created_at among the remaining rows.
		if resp["new_billing_team_id"] != float64(2) {
			t.Fatalf("new_billing_team_id = %v, want 2", resp["new_billing_team_id"])
		}
		if got := store.teams["acme"][2]; got.IsBillingTeam == nil || !*got.IsBillingTeam {
			t.Fatalf("team 2 must carry the billing mark, got %+v", got)
		}
	})

	t.Run("non-billing delete has no handover", func(t *testing.T) {
		store := newFakeStore()
		store.orgs["acme"] = &configstore.Org{Name: "acme"}
		b := billing
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 1, SchemaName: "team_1", Enabled: true, IsBillingTeam: &b, CreatedAt: base})
		store.seedTeam(configstore.OrgTeam{OrgID: "acme", TeamID: 2, SchemaName: "team_2", Enabled: true, CreatedAt: base.Add(time.Hour)})
		router := newTestRouter(store)

		rec := doJSON(t, router, http.MethodDelete, "/api/v1/orgs/acme/teams/2", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "new_billing_team_id") {
			t.Fatalf("non-billing delete must not report a handover: %s", rec.Body.String())
		}
	})

	t.Run("unknown team 404", func(t *testing.T) {
		store := newFakeStore()
		store.orgs["acme"] = &configstore.Org{Name: "acme"}
		router := newTestRouter(store)
		rec := doJSON(t, router, http.MethodDelete, "/api/v1/orgs/acme/teams/9", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("non-numeric team id 400", func(t *testing.T) {
		store := newFakeStore()
		store.orgs["acme"] = &configstore.Org{Name: "acme"}
		router := newTestRouter(store)
		rec := doJSON(t, router, http.MethodDelete, "/api/v1/orgs/acme/teams/abc", "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
		}
	})
}

// --- provision team_id/schema_name resolution ---

func TestProvisionTeamIDAndSchemaName(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := `{"database_name":"d","team_id":42,"schema_name":"custom_wh","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/neworg/provision", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if store.lastProvision == nil {
		t.Fatal("Provision not dispatched")
	}
	if store.lastProvision.DefaultTeamID != 42 || store.lastProvision.SchemaName != "custom_wh" {
		t.Fatalf("provision request = %+v, want team 42 schema custom_wh", store.lastProvision)
	}
}

func TestProvisionTeamIDFieldsMustAgree(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := `{"database_name":"d","team_id":42,"default_team_id":7,"metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/neworg/provision", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "disagree") {
		t.Fatalf("error should state the fields disagree: %s", rec.Body.String())
	}
	if store.lastProvision != nil {
		t.Fatal("nothing must be provisioned on disagreement")
	}
}

func TestProvisionAgreeingTeamIDFieldsAccepted(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := `{"database_name":"d","team_id":42,"default_team_id":42,"metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/neworg/provision", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if store.lastProvision.DefaultTeamID != 42 {
		t.Fatalf("effective team = %d, want 42", store.lastProvision.DefaultTeamID)
	}
}

func TestProvisionSchemaNameRequiresTeamID(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	router := newTestRouter(store)

	body := `{"database_name":"d","schema_name":"custom_wh","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/acme/provision", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "schema_name requires team_id") {
		t.Fatalf("error should say schema_name requires team_id: %s", rec.Body.String())
	}
}

func TestProvisionInvalidSchemaNameRejected(t *testing.T) {
	store := newFakeStore()
	router := newTestRouter(store)

	body := `{"database_name":"d","team_id":42,"schema_name":"Bad-Name","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}`
	rec := doJSON(t, router, http.MethodPost, "/api/v1/orgs/neworg/provision", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}
