//go:build kubernetes

package admin

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

type fakeAPIStore struct {
	teams      map[string]*configstore.Team
	users      map[string]*configstore.TeamUser
	warehouses map[string]*configstore.ManagedWarehouse
}

func newFakeAPIStore() *fakeAPIStore {
	return &fakeAPIStore{
		teams:      make(map[string]*configstore.Team),
		users:      make(map[string]*configstore.TeamUser),
		warehouses: make(map[string]*configstore.ManagedWarehouse),
	}
}

func (s *fakeAPIStore) ListTeams() ([]configstore.Team, error) {
	teams := make([]configstore.Team, 0, len(s.teams))
	for _, team := range s.teams {
		teams = append(teams, *copyTeam(team))
	}
	return teams, nil
}

func (s *fakeAPIStore) CreateTeam(team *configstore.Team) error {
	if _, ok := s.teams[team.Name]; ok {
		return errors.New("duplicate team")
	}
	clone := copyTeam(team)
	clone.Warehouse = nil
	s.teams[team.Name] = clone
	return nil
}

func (s *fakeAPIStore) GetTeam(name string) (*configstore.Team, error) {
	team, ok := s.teams[name]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return copyTeam(team), nil
}

func (s *fakeAPIStore) UpdateTeam(name string, updates configstore.Team) (*configstore.Team, bool, error) {
	team, ok := s.teams[name]
	if !ok {
		return nil, false, nil
	}
	team.MaxWorkers = updates.MaxWorkers
	team.MinWorkers = updates.MinWorkers
	team.MemoryBudget = updates.MemoryBudget
	team.IdleTimeoutS = updates.IdleTimeoutS
	return copyTeam(team), true, nil
}

func (s *fakeAPIStore) DeleteTeam(name string) (bool, error) {
	if _, ok := s.teams[name]; !ok {
		return false, nil
	}
	delete(s.teams, name)
	delete(s.warehouses, name)
	return true, nil
}

func (s *fakeAPIStore) ListUsers() ([]configstore.TeamUser, error) {
	users := make([]configstore.TeamUser, 0, len(s.users))
	for _, user := range s.users {
		clone := *user
		users = append(users, clone)
	}
	return users, nil
}

func (s *fakeAPIStore) CreateUser(user *configstore.TeamUser) error {
	if _, ok := s.users[user.Username]; ok {
		return errors.New("duplicate user")
	}
	clone := *user
	s.users[user.Username] = &clone
	return nil
}

func (s *fakeAPIStore) GetUser(username string) (*configstore.TeamUser, error) {
	user, ok := s.users[username]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	clone := *user
	return &clone, nil
}

func (s *fakeAPIStore) UpdateUser(username, passwordHash, teamName string) (*configstore.TeamUser, bool, error) {
	user, ok := s.users[username]
	if !ok {
		return nil, false, nil
	}
	if passwordHash != "" {
		user.Password = passwordHash
	}
	if teamName != "" {
		user.TeamName = teamName
	}
	clone := *user
	return &clone, true, nil
}

func (s *fakeAPIStore) DeleteUser(username string) (bool, error) {
	if _, ok := s.users[username]; !ok {
		return false, nil
	}
	delete(s.users, username)
	return true, nil
}

func (s *fakeAPIStore) GetManagedWarehouse(teamName string) (*configstore.ManagedWarehouse, error) {
	warehouse, ok := s.warehouses[teamName]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return copyWarehouse(warehouse), nil
}

func (s *fakeAPIStore) UpsertManagedWarehouse(teamName string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error) {
	team, ok := s.teams[teamName]
	if !ok {
		return nil, false, nil
	}
	clone := copyWarehouse(warehouse)
	clone.TeamName = teamName
	s.warehouses[teamName] = clone
	team.Warehouse = copyWarehouse(clone)
	return copyWarehouse(clone), true, nil
}

func (s *fakeAPIStore) GetGlobalConfig() (configstore.GlobalConfig, error) {
	return configstore.GlobalConfig{}, nil
}

func (s *fakeAPIStore) SaveGlobalConfig(cfg *configstore.GlobalConfig) error {
	return nil
}

func (s *fakeAPIStore) GetDuckLakeConfig() (configstore.DuckLakeConfig, error) {
	return configstore.DuckLakeConfig{}, nil
}

func (s *fakeAPIStore) SaveDuckLakeConfig(cfg *configstore.DuckLakeConfig) error {
	return nil
}

func (s *fakeAPIStore) GetRateLimitConfig() (configstore.RateLimitConfig, error) {
	return configstore.RateLimitConfig{}, nil
}

func (s *fakeAPIStore) SaveRateLimitConfig(cfg *configstore.RateLimitConfig) error {
	return nil
}

func (s *fakeAPIStore) GetQueryLogConfig() (configstore.QueryLogConfig, error) {
	return configstore.QueryLogConfig{}, nil
}

func (s *fakeAPIStore) SaveQueryLogConfig(cfg *configstore.QueryLogConfig) error {
	return nil
}

func copyWarehouse(warehouse *configstore.ManagedWarehouse) *configstore.ManagedWarehouse {
	if warehouse == nil {
		return nil
	}
	clone := *warehouse
	return &clone
}

func copyTeam(team *configstore.Team) *configstore.Team {
	if team == nil {
		return nil
	}
	clone := *team
	if team.Warehouse != nil {
		clone.Warehouse = copyWarehouse(team.Warehouse)
	}
	if len(team.Users) > 0 {
		clone.Users = make([]configstore.TeamUser, len(team.Users))
		copy(clone.Users, team.Users)
	}
	return &clone
}

func newTestAPIRouter(store apiStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), store, nil)
	return r
}

func seedTeamWithWarehouse(store *fakeAPIStore, name string) {
	warehouse := &configstore.ManagedWarehouse{
		TeamName: name,
		Aurora: configstore.ManagedWarehouseAurora{
			Region:       "us-east-1",
			Endpoint:     fmt.Sprintf("%s.cluster.example", name),
			Port:         5432,
			DatabaseName: name + "_warehouse",
			Username:     "warehouse_user",
		},
		S3: configstore.ManagedWarehouseS3{
			Provider:   "aws",
			Region:     "us-east-1",
			Bucket:     name + "-bucket",
			PathPrefix: name + "/ducklake/",
		},
		WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
			Namespace:          "duckgres",
			ServiceAccountName: name + "-worker",
			IAMRoleARN:         "arn:aws:iam::123456789012:role/" + name + "-worker",
		},
		AuroraCredentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      name + "-aurora",
			Key:       "dsn",
		},
		S3Credentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      name + "-s3",
			Key:       "credentials",
		},
		RuntimeConfig: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      name + "-runtime",
			Key:       "duckgres.yaml",
		},
		State:         configstore.ManagedWarehouseStateReady,
		AuroraState:   configstore.ManagedWarehouseStateReady,
		S3State:       configstore.ManagedWarehouseStateReady,
		IdentityState: configstore.ManagedWarehouseStateReady,
		SecretsState:  configstore.ManagedWarehouseStateReady,
	}
	store.teams[name] = &configstore.Team{
		Name:      name,
		Warehouse: copyWarehouse(warehouse),
	}
	store.warehouses[name] = warehouse
}

func TestGetTeamIncludesWarehouse(t *testing.T) {
	store := newFakeAPIStore()
	seedTeamWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/teams/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var team configstore.Team
	if err := json.Unmarshal(rec.Body.Bytes(), &team); err != nil {
		t.Fatalf("unmarshal team: %v", err)
	}
	if team.Warehouse == nil {
		t.Fatal("expected warehouse in team response")
	}
	if team.Warehouse.Aurora.DatabaseName != "analytics_warehouse" {
		t.Fatalf("expected analytics_warehouse, got %q", team.Warehouse.Aurora.DatabaseName)
	}
}

func TestListTeamsIncludesWarehouse(t *testing.T) {
	store := newFakeAPIStore()
	seedTeamWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/teams", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var teams []configstore.Team
	if err := json.Unmarshal(rec.Body.Bytes(), &teams); err != nil {
		t.Fatalf("unmarshal teams: %v", err)
	}
	if len(teams) != 1 {
		t.Fatalf("expected 1 team, got %d", len(teams))
	}
	if teams[0].Warehouse == nil {
		t.Fatal("expected nested warehouse in team list response")
	}
}

func TestGetWarehouseReturnsNotFoundWhenMissing(t *testing.T) {
	store := newFakeAPIStore()
	store.teams["analytics"] = &configstore.Team{Name: "analytics"}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/teams/analytics/warehouse", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestPutWarehouseUpsertsForExistingTeam(t *testing.T) {
	store := newFakeAPIStore()
	store.teams["analytics"] = &configstore.Team{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"team_name": "wrong-team",
		"aurora": {
			"region": "us-east-1",
			"endpoint": "analytics.cluster.example",
			"port": 5432,
			"database_name": "analytics_warehouse",
			"username": "warehouse_user"
		},
		"s3": {
			"provider": "aws",
			"region": "us-east-1",
			"bucket": "analytics-bucket",
			"path_prefix": "analytics/ducklake/",
			"endpoint": "s3.us-east-1.amazonaws.com",
			"use_ssl": true,
			"url_style": "vhost"
		},
		"worker_identity": {
			"namespace": "duckgres",
			"service_account_name": "analytics-worker",
			"iam_role_arn": "arn:aws:iam::123456789012:role/analytics-worker"
		},
		"aurora_credentials": {
			"namespace": "duckgres",
			"name": "analytics-aurora",
			"key": "dsn"
		},
		"s3_credentials": {
			"namespace": "duckgres",
			"name": "analytics-s3",
			"key": "credentials"
		},
		"runtime_config": {
			"namespace": "duckgres",
			"name": "analytics-runtime",
			"key": "duckgres.yaml"
		},
		"state": "ready",
		"status_message": "ready",
		"aurora_state": "ready",
		"s3_state": "ready",
		"identity_state": "ready",
		"secrets_state": "ready"
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/teams/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	warehouse := store.warehouses["analytics"]
	if warehouse == nil {
		t.Fatal("expected stored warehouse")
	}
	if warehouse.TeamName != "analytics" {
		t.Fatalf("expected team_name analytics, got %q", warehouse.TeamName)
	}
	if warehouse.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime secret analytics-runtime, got %q", warehouse.RuntimeConfig.Name)
	}
}

func TestPutWarehouseRejectsUnknownTeam(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/teams/unknown/warehouse", bytes.NewReader([]byte(`{"state":"ready"}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}
