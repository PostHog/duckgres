package provisioning

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func i64(v int64) *int64 { return &v }

func boolPtrD(v bool) *bool { return &v }

func strPtrD(v string) *string { return &v }

// seedTeam adds one duckgres_org_teams row to the fake (delegates to the
// fake's own seeding helper).
func seedTeam(s *fakeStore, orgID string, t configstore.OrgTeam) {
	t.OrgID = orgID
	s.seedTeam(t)
}

// seedWarehouse adds an org + warehouse pair shaped like a provisioned
// external-metadata tenant (the kind whose row actually carries connection
// details today — see the metadata-store note in discovery.go: cnpg rows
// carry only the kind until the CR-status backfill exists).
func seedWarehouse(s *fakeStore, orgID string, teamID *int64, state configstore.ManagedWarehouseProvisioningState, updatedAt time.Time) {
	s.orgs[orgID] = &configstore.Org{
		Name:         orgID,
		DatabaseName: "db_" + orgID,
		UpdatedAt:    updatedAt,
	}
	if teamID != nil {
		seedTeam(s, orgID, configstore.OrgTeam{
			TeamID:     *teamID,
			SchemaName: fmt.Sprintf("team_%d", *teamID),
			Enabled:    true,
			UpdatedAt:  updatedAt,
		})
	}
	s.warehouses[orgID] = &configstore.ManagedWarehouse{
		OrgID:        orgID,
		DucklingName: orgID,
		State:        state,
		UpdatedAt:    updatedAt,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         configstore.MetadataStoreKindExternal,
			Endpoint:     "duckling-" + orgID + ".rds.example",
			Port:         5432,
			DatabaseName: "duckling_" + orgID,
			Username:     "duckling_" + orgID,
			// The one genuinely sensitive row field — must NEVER appear in
			// the discovery payload (see TestListWarehousesPayloadNoSecrets).
			PasswordAWSSecret: "rds-master-secret-value",
		},
		MetadataStoreCredentials: configstore.SecretRef{
			Namespace: "ducklings",
			Name:      "duckling-" + orgID + "-password",
			Key:       "password",
		},
		DataStore: configstore.ManagedWarehouseDataStore{
			BucketName: "bucket-" + orgID,
		},
	}
}

func getJSON(t *testing.T, router http.Handler, path string, out any) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
			t.Fatalf("decode %s: %v (body: %s)", path, err, w.Body.String())
		}
	}
	return w.Code
}

func TestDiscoveryStateClassification(t *testing.T) {
	// The state enum is open (models.go): a state added without being
	// classified in discovery.go silently reads as fleet-wide tenant
	// removal to every discovery consumer. This test is the tripwire —
	// when adding a ManagedWarehouseState* constant, add it here AND to
	// exactly one of discoveryStates / discoveryExcludedStates.
	all := []configstore.ManagedWarehouseProvisioningState{
		configstore.ManagedWarehouseStatePending,
		configstore.ManagedWarehouseStateProvisioning,
		configstore.ManagedWarehouseStateReady,
		configstore.ManagedWarehouseStateFailed,
		configstore.ManagedWarehouseStateDeleting,
		configstore.ManagedWarehouseStateDeleted,
		configstore.ManagedWarehouseStateResharding,
	}
	classified := make(map[configstore.ManagedWarehouseProvisioningState]int)
	for _, st := range discoveryStates {
		classified[st]++
	}
	for _, st := range discoveryExcludedStates {
		classified[st]++
	}
	for _, st := range all {
		if classified[st] != 1 {
			t.Errorf("state %q classified %d times; every state must appear in exactly one of discoveryStates/discoveryExcludedStates", st, classified[st])
		}
	}
	if len(classified) != len(all) {
		t.Errorf("classification lists carry %d states, test knows %d — update both", len(classified), len(all))
	}
}

func TestListWarehousesStates(t *testing.T) {
	s := newFakeStore()
	now := time.Now()
	seedWarehouse(s, "ready-org", i64(2), configstore.ManagedWarehouseStateReady, now)
	seedWarehouse(s, "resharding-org", i64(3), configstore.ManagedWarehouseStateResharding, now)
	seedWarehouse(s, "pending-org", i64(4), configstore.ManagedWarehouseStatePending, now)
	seedWarehouse(s, "provisioning-org", i64(5), configstore.ManagedWarehouseStateProvisioning, now)
	seedWarehouse(s, "deleting-org", i64(6), configstore.ManagedWarehouseStateDeleting, now)
	seedWarehouse(s, "deleted-org", i64(7), configstore.ManagedWarehouseStateDeleted, now)
	seedWarehouse(s, "failed-org", i64(8), configstore.ManagedWarehouseStateFailed, now)

	var resp discoveryResponse
	if code := getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(resp.Warehouses) != 2 {
		t.Fatalf("want 2 warehouses (ready+resharding), got %d", len(resp.Warehouses))
	}
	byOrg := map[string]discoveryWarehouse{}
	for _, w := range resp.Warehouses {
		byOrg[w.OrgID] = w
	}
	// Ready is writable; resharding is listed (NOT hidden — hiding reads as
	// tenant removal downstream) but fenced.
	if !byOrg["ready-org"].Writable {
		t.Error("ready warehouse must be writable")
	}
	rw, ok := byOrg["resharding-org"]
	if !ok {
		t.Fatal("resharding warehouse must be listed")
	}
	if rw.Writable {
		t.Error("resharding warehouse must not be writable")
	}
}

func TestListWarehousesPayload(t *testing.T) {
	s := newFakeStore()
	seedWarehouse(s, "org-a", i64(50689), configstore.ManagedWarehouseStateReady, time.Now())

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	w := resp.Warehouses[0]

	if len(w.Teams) != 1 || w.Teams[0].TeamID != 50689 || w.Teams[0].SchemaName != "team_50689" {
		t.Fatalf("teams from duckgres_org_teams rows, got %+v", w.Teams)
	}
	if !w.Teams[0].Enabled {
		t.Fatalf("team must serve enabled=true, got %+v", w.Teams[0])
	}
	if w.Teams[0].EventsTable != "team_50689.events" {
		t.Fatalf("derived events_table wrong: %+v", w.Teams[0])
	}
	ms := w.MetadataStore
	if ms.Endpoint != "duckling-org-a.rds.example" || ms.Port != 5432 ||
		ms.Database != "duckling_org-a" || ms.Username != "duckling_org-a" ||
		ms.Kind != configstore.MetadataStoreKindExternal {
		t.Fatalf("metadata store passthrough wrong: %+v", ms)
	}
	ref := ms.PasswordSecretRef
	if ref.Namespace != "ducklings" || ref.Name != "duckling-org-a-password" || ref.Key != "password" {
		t.Fatalf("secret ref passthrough wrong: %+v", ref)
	}
	if w.Bucket != "bucket-org-a" {
		t.Fatalf("bucket passthrough wrong: %q", w.Bucket)
	}
}

func TestListWarehousesPayloadNoSecrets(t *testing.T) {
	// The row's PasswordAWSSecret is seeded with a sentinel VALUE; neither
	// that value nor any password-ish KEY (other than password_secret_ref)
	// may appear anywhere in the serialized payload. Catches both a direct
	// leak and a future "simplify by serializing the model" refactor.
	s := newFakeStore()
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, time.Now())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/warehouses", nil)
	w := httptest.NewRecorder()
	newTestRouter(s).ServeHTTP(w, req)
	body := w.Body.String()

	if strings.Contains(body, "rds-master-secret-value") {
		t.Fatal("payload leaks the PasswordAWSSecret value")
	}
	var v any
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var walk func(any)
	walk = func(n any) {
		switch n := n.(type) {
		case map[string]any:
			for k, val := range n {
				if k != "password_secret_ref" && strings.Contains(k, "password") {
					t.Errorf("payload carries password-ish key %q", k)
				}
				walk(val)
			}
		case []any:
			for _, val := range n {
				walk(val)
			}
		}
	}
	walk(v)
}

func TestListWarehousesBrokenTeamRows(t *testing.T) {
	s := newFakeStore()
	now := time.Now()
	seedWarehouse(s, "org-ok", i64(2), configstore.ManagedWarehouseStateReady, now)
	// Warehouse with NO team rows at all (every org gets its first team row
	// at provision; zero rows is a data inconsistency — degrade to empty
	// teams, never hide the warehouse).
	seedWarehouse(s, "org-noteams", nil, configstore.ManagedWarehouseStateReady, now)

	beforeMissing := testutil.ToFloat64(discoveryBrokenTeamRows.WithLabelValues("teams_missing"))

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	if len(resp.Warehouses) != 2 {
		t.Fatalf("both warehouses must be listed, got %d", len(resp.Warehouses))
	}
	for _, w := range resp.Warehouses {
		switch w.OrgID {
		case "org-ok":
			if len(w.Teams) != 1 {
				t.Errorf("org-ok should have 1 team, got %d", len(w.Teams))
			}
		default:
			if w.Teams == nil || len(w.Teams) != 0 {
				t.Errorf("%s should have empty (non-null) teams, got %+v", w.OrgID, w.Teams)
			}
		}
	}
	if got := testutil.ToFloat64(discoveryBrokenTeamRows.WithLabelValues("teams_missing")) - beforeMissing; got != 1 {
		t.Errorf("teams_missing counter delta = %v, want 1", got)
	}
}

func TestDuplicateTeamAcrossOrgsCounted(t *testing.T) {
	s := newFakeStore()
	now := time.Now()
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, now)
	seedWarehouse(s, "org-b", i64(2), configstore.ManagedWarehouseStateReady, now)

	before := testutil.ToFloat64(discoveryBrokenTeamRows.WithLabelValues("team_conflict"))
	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	// Both warehouses still carry the team (the listing doesn't arbitrate),
	// but the conflict is counted so it can be alerted on.
	if got := testutil.ToFloat64(discoveryBrokenTeamRows.WithLabelValues("team_conflict")) - before; got != 1 {
		t.Errorf("team_conflict counter delta = %v, want 1", got)
	}
}

func TestListWarehousesConfigGeneration(t *testing.T) {
	s := newFakeStore()
	older := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, older)
	// A DELETED warehouse's newer updated_at must still advance the
	// generation: leaving the discoverable set IS a change a poller must
	// not skip (the whole point of the unfiltered marker).
	seedWarehouse(s, "org-gone", i64(3), configstore.ManagedWarehouseStateDeleted, newer)

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	if resp.ConfigGeneration != newer.Unix() {
		t.Fatalf("config_generation = %d, want %d (max over ALL rows incl. excluded states)", resp.ConfigGeneration, newer.Unix())
	}
}

func TestListWarehousesEmpty(t *testing.T) {
	var resp discoveryResponse
	if code := getJSON(t, newTestRouter(newFakeStore()), "/api/v1/warehouses", &resp); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if resp.Warehouses == nil || len(resp.Warehouses) != 0 {
		t.Fatalf("empty fleet must serialize as [], got %+v", resp.Warehouses)
	}
	if resp.ConfigGeneration != 0 {
		t.Fatalf("empty fleet generation must be 0, got %d", resp.ConfigGeneration)
	}
}

func TestDiscoveryTransientErrorsFailTheRequest(t *testing.T) {
	// Transient store failures must 500 the WHOLE request — a polling
	// consumer keeps its last-known-good state. Serving a 200 with a
	// tenant's team silently absent is indistinguishable from removal and,
	// after consumer-side damping, silently stops ingestion for the team.
	for name, breakStore := range map[string]func(*fakeStore){
		"list_warehouses": func(s *fakeStore) { s.listWarehousesErr = errors.New("db down") },
		"list_org_teams":  func(s *fakeStore) { s.listOrgTeamsErr = errors.New("db down") },
		"latest_change":   func(s *fakeStore) { s.latestChangeErr = errors.New("db down") },
	} {
		t.Run(name, func(t *testing.T) {
			s := newFakeStore()
			seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, time.Now())
			breakStore(s)
			router := newTestRouter(s)
			for _, path := range []string{"/api/v1/warehouses", "/api/v1/warehouse-team-ids"} {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != http.StatusInternalServerError {
					t.Errorf("%s: want 500 on transient store error, got %d", path, w.Code)
				}
			}
		})
	}
}

func TestTeamIDsProjection(t *testing.T) {
	s := newFakeStore()
	now := time.Now()
	seedWarehouse(s, "org-a", i64(50689), configstore.ManagedWarehouseStateReady, now)
	seedWarehouse(s, "org-b", i64(2), configstore.ManagedWarehouseStateReady, now)
	// Resharding teams stay included: upstream ingestion keeps running
	// during a duckling reshard; dropping the team here would (post-damping)
	// silently stop ingesting it.
	seedWarehouse(s, "org-c", i64(81505), configstore.ManagedWarehouseStateResharding, now)
	// Duplicate team across orgs must not duplicate in the array.
	seedWarehouse(s, "org-dup", i64(2), configstore.ManagedWarehouseStateReady, now)
	// A warehouse with no team rows contributes nothing (and is counted by
	// the shared assembly — see TestListWarehousesBrokenTeamRows).
	seedWarehouse(s, "org-noteams", nil, configstore.ManagedWarehouseStateReady, now)
	// Excluded states contribute nothing.
	seedWarehouse(s, "org-pending", i64(7), configstore.ManagedWarehouseStatePending, now)
	// DISABLED teams stay in the projection: enabled is duckgres's
	// query-serving switch (migration 000024), not an ingestion signal —
	// see TestDisabledTeamStaysInProjection.
	seedTeam(s, "org-a", configstore.OrgTeam{TeamID: 424242, SchemaName: "disabled_team", Enabled: false, UpdatedAt: now})

	var ids []int64
	if code := getJSON(t, newTestRouter(s), "/api/v1/warehouse-team-ids", &ids); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	want := []int64{2, 50689, 81505, 424242}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v (sorted, deduped)", ids, want)
		}
	}
}

func TestDisabledTeamStaysInProjection(t *testing.T) {
	// enabled is the per-team QUERY-SERVING switch (migration 000024) —
	// not an ingestion signal. A disabled team must stay in the values
	// projection (else a serving hold silently stops its ingestion:
	// permanent event loss) and in the full listing with the flag.
	s := newFakeStore()
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, time.Now())
	seedTeam(s, "org-a", configstore.OrgTeam{TeamID: 3, SchemaName: "held_team", Enabled: false})

	var ids []int64
	getJSON(t, newTestRouter(s), "/api/v1/warehouse-team-ids", &ids)
	found := false
	for _, id := range ids {
		if id == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("disabled team must stay in the values projection, got %v", ids)
	}
}

func TestDisabledTeamServedFlagged(t *testing.T) {
	// enabled=false flows through the FULL listing as an informational
	// flag; consumers derive nothing destructive from it.
	s := newFakeStore()
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, time.Now())
	seedTeam(s, "org-a", configstore.OrgTeam{TeamID: 3, SchemaName: "paused_team", Enabled: false})

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	if len(resp.Warehouses[0].Teams) != 2 {
		t.Fatalf("disabled team must stay in the full listing, got %+v", resp.Warehouses[0].Teams)
	}
	for _, tm := range resp.Warehouses[0].Teams {
		if tm.TeamID == 3 && tm.Enabled {
			t.Fatal("team 3 must carry enabled=false")
		}
	}
}

func TestResolvedTableNames(t *testing.T) {
	// The CP serves RESOLVED table locations — the derivation from
	// schema_name + grandfathered overrides happens once, in
	// resolveTeamTables, so consumers never guess the qualification rule.
	s := newFakeStore()
	seedWarehouse(s, "org-a", nil, configstore.ManagedWarehouseStateReady, time.Now())
	seedTeam(s, "org-a", configstore.OrgTeam{
		TeamID:          2,
		SchemaName:      "posthog",
		Enabled:         true,
		EventsTableName: strPtrD("legacy_events"),
	})

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	tm := resp.Warehouses[0].Teams[0]
	// Override resolves within the team's schema; unset fields derive.
	if tm.EventsTable != "posthog.legacy_events" || tm.PersonsTable != "posthog.persons" || tm.DataImportsSchema != "posthog_data_imports" {
		t.Fatalf("resolved table names wrong: %+v", tm)
	}
}

func TestConfigGenerationAdvancesOnTeamChange(t *testing.T) {
	// A team-row edit (enable/disable, schema repair) is a routing change a
	// poller must not skip — the generation must cover duckgres_org_teams.
	s := newFakeStore()
	older := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	seedWarehouse(s, "org-a", i64(2), configstore.ManagedWarehouseStateReady, older)
	s.teams["org-a"][2].UpdatedAt = newer

	var resp discoveryResponse
	getJSON(t, newTestRouter(s), "/api/v1/warehouses", &resp)
	if resp.ConfigGeneration != newer.Unix() {
		t.Fatalf("config_generation = %d, want %d (team-row updated_at)", resp.ConfigGeneration, newer.Unix())
	}
}

func TestTeamIDsEmptyFleetIsEmptyArray(t *testing.T) {
	// millpond's include-values source refuses empty arrays — but the
	// ENDPOINT must still serialize [] (not null) for an empty fleet; the
	// refusal is the consumer's decision.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/warehouse-team-ids", nil)
	w := httptest.NewRecorder()
	newTestRouter(newFakeStore()).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if body := w.Body.String(); body != "[]" {
		t.Fatalf("empty fleet must serialize as [], got %q", body)
	}
}
