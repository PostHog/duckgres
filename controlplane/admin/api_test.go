//go:build kubernetes

package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

type fakeAPIStore struct {
	orgs       map[string]*configstore.Org
	users      map[string]*configstore.OrgUser
	warehouses map[string]*configstore.ManagedWarehouse
}

func newFakeAPIStore() *fakeAPIStore {
	return &fakeAPIStore{
		orgs:       make(map[string]*configstore.Org),
		users:      make(map[string]*configstore.OrgUser),
		warehouses: make(map[string]*configstore.ManagedWarehouse),
	}
}

func (s *fakeAPIStore) ListOrgs() ([]configstore.Org, error) {
	orgs := make([]configstore.Org, 0, len(s.orgs))
	for _, org := range s.orgs {
		orgs = append(orgs, *copyOrg(org))
	}
	return orgs, nil
}

func (s *fakeAPIStore) CreateOrg(org *configstore.Org) error {
	if _, ok := s.orgs[org.Name]; ok {
		return errors.New("duplicate org")
	}
	clone := copyOrg(org)
	clone.Warehouse = nil
	s.orgs[org.Name] = clone
	return nil
}

func (s *fakeAPIStore) GetOrg(name string) (*configstore.Org, error) {
	org, ok := s.orgs[name]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return copyOrg(org), nil
}

func (s *fakeAPIStore) UpdateOrg(name string, updates configstore.Org) (*configstore.Org, bool, error) {
	org, ok := s.orgs[name]
	if !ok {
		return nil, false, nil
	}
	org.MaxWorkers = updates.MaxWorkers
	org.MemoryBudget = updates.MemoryBudget
	org.IdleTimeoutS = updates.IdleTimeoutS
	return copyOrg(org), true, nil
}

func (s *fakeAPIStore) DeleteOrg(name string) (bool, error) {
	if _, ok := s.orgs[name]; !ok {
		return false, nil
	}
	delete(s.orgs, name)
	delete(s.warehouses, name)
	return true, nil
}

func (s *fakeAPIStore) ListUsers() ([]configstore.OrgUser, error) {
	users := make([]configstore.OrgUser, 0, len(s.users))
	for _, user := range s.users {
		clone := *user
		users = append(users, clone)
	}
	return users, nil
}

func (s *fakeAPIStore) CreateUser(user *configstore.OrgUser) error {
	key := user.OrgID + "/" + user.Username
	if _, ok := s.users[key]; ok {
		return errors.New("duplicate user")
	}
	clone := *user
	s.users[key] = &clone
	return nil
}

func (s *fakeAPIStore) GetUser(orgID, username string) (*configstore.OrgUser, error) {
	key := orgID + "/" + username
	user, ok := s.users[key]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	clone := *user
	return &clone, nil
}

func (s *fakeAPIStore) UpdateUser(orgID, username, passwordHash string) (*configstore.OrgUser, bool, error) {
	key := orgID + "/" + username
	user, ok := s.users[key]
	if !ok {
		return nil, false, nil
	}
	if passwordHash != "" {
		user.Password = passwordHash
	}
	clone := *user
	return &clone, true, nil
}

func (s *fakeAPIStore) DeleteUser(orgID, username string) (bool, error) {
	key := orgID + "/" + username
	if _, ok := s.users[key]; !ok {
		return false, nil
	}
	delete(s.users, key)
	return true, nil
}

func (s *fakeAPIStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	warehouse, ok := s.warehouses[orgID]
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return copyWarehouse(warehouse), nil
}

func (s *fakeAPIStore) UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error) {
	org, ok := s.orgs[orgID]
	if !ok {
		return nil, false, nil
	}
	clone := copyWarehouse(warehouse)
	clone.OrgID = orgID
	s.warehouses[orgID] = clone
	org.Warehouse = copyWarehouse(clone)
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

func copyOrg(org *configstore.Org) *configstore.Org {
	if org == nil {
		return nil
	}
	clone := *org
	if org.Warehouse != nil {
		clone.Warehouse = copyWarehouse(org.Warehouse)
	}
	if len(org.Users) > 0 {
		clone.Users = make([]configstore.OrgUser, len(org.Users))
		copy(clone.Users, org.Users)
	}
	return &clone
}

func newTestAPIRouter(store apiStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), store, nil)
	return r
}

func seedOrgWithWarehouse(store *fakeAPIStore, name string) {
	warehouse := &configstore.ManagedWarehouse{
		OrgID: name,
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Region:       "us-east-1",
			Endpoint:     fmt.Sprintf("%s.cluster.example", name),
			Port:         5432,
			DatabaseName: name + "_warehouse",
			Username:     "warehouse_user",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
			Engine:       "postgres",
			Region:       "us-east-1",
			Endpoint:     fmt.Sprintf("%s-metadata.cluster.example", name),
			Port:         5432,
			DatabaseName: name + "_metadata",
			Username:     "metadata_user",
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
		WarehouseDatabaseCredentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      name + "-warehouse-db",
			Key:       "dsn",
		},
		MetadataStoreCredentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      name + "-metadata",
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
		State:                  configstore.ManagedWarehouseStateReady,
		WarehouseDatabaseState: configstore.ManagedWarehouseStateReady,
		MetadataStoreState:     configstore.ManagedWarehouseStateReady,
		S3State:                configstore.ManagedWarehouseStateReady,
		IdentityState:          configstore.ManagedWarehouseStateReady,
		SecretsState:           configstore.ManagedWarehouseStateReady,
	}
	store.orgs[name] = &configstore.Org{
		Name:      name,
		Warehouse: copyWarehouse(warehouse),
	}
	store.warehouses[name] = warehouse
}

func TestGetOrgIncludesWarehouse(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var org configstore.Org
	if err := json.Unmarshal(rec.Body.Bytes(), &org); err != nil {
		t.Fatalf("unmarshal org: %v", err)
	}
	if org.Warehouse == nil {
		t.Fatal("expected warehouse in org response")
	}
	if org.Warehouse.WarehouseDatabase.DatabaseName != "analytics_warehouse" {
		t.Fatalf("expected analytics_warehouse, got %q", org.Warehouse.WarehouseDatabase.DatabaseName)
	}
	if org.Warehouse.MetadataStore.Kind != "dedicated_rds" {
		t.Fatalf("expected metadata store kind dedicated_rds, got %q", org.Warehouse.MetadataStore.Kind)
	}
}

func TestListOrgsIncludesWarehouse(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var orgs []configstore.Org
	if err := json.Unmarshal(rec.Body.Bytes(), &orgs); err != nil {
		t.Fatalf("unmarshal orgs: %v", err)
	}
	if len(orgs) != 1 {
		t.Fatalf("expected 1 org, got %d", len(orgs))
	}
	if orgs[0].Warehouse == nil {
		t.Fatal("expected nested warehouse in org list response")
	}
}

func TestGetWarehouseReturnsNotFoundWhenMissing(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/analytics/warehouse", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestPutWarehouseUpsertsForExistingOrg(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"warehouse_database": {
			"region": "us-east-1",
			"endpoint": "analytics.cluster.example",
			"port": 5432,
			"database_name": "analytics_warehouse",
			"username": "warehouse_user"
		},
		"metadata_store": {
			"kind": "dedicated_rds",
			"engine": "postgres",
			"region": "us-east-1",
			"endpoint": "analytics-metadata.cluster.example",
			"port": 5432,
			"database_name": "ducklake_metadata",
			"username": "metadata_user"
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
		"warehouse_database_credentials": {
			"namespace": "duckgres",
			"name": "analytics-warehouse-db",
			"key": "dsn"
		},
		"metadata_store_credentials": {
			"namespace": "duckgres",
			"name": "analytics-metadata",
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
		"warehouse_database_state": "ready",
		"metadata_store_state": "ready",
		"s3_state": "ready",
		"identity_state": "ready",
		"secrets_state": "ready"
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
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
	if warehouse.OrgID != "analytics" {
		t.Fatalf("expected org_id analytics, got %q", warehouse.OrgID)
	}
	if warehouse.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime secret analytics-runtime, got %q", warehouse.RuntimeConfig.Name)
	}
	if warehouse.WarehouseDatabaseCredentials.Name != "analytics-warehouse-db" {
		t.Fatalf("expected warehouse db secret analytics-warehouse-db, got %q", warehouse.WarehouseDatabaseCredentials.Name)
	}
	if warehouse.MetadataStore.DatabaseName != "ducklake_metadata" {
		t.Fatalf("expected metadata db ducklake_metadata, got %q", warehouse.MetadataStore.DatabaseName)
	}
}

func TestPutWarehouseRejectsUnknownOrg(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/unknown/warehouse", bytes.NewReader([]byte(`{"state":"ready"}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestPutWarehouseRejectsServerManagedFields(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"org_id": "wrong-org",
		"created_at": "2026-03-18T10:00:00Z",
		"warehouse_database": {
			"database_name": "analytics_warehouse"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPutWarehouseAllowsCustomProvisioningStates(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"state": "awaiting-human-approval",
		"warehouse_database_state": "queued-for-bootstrap",
		"metadata_store_state": "vendor-pending",
		"s3_state": "bucket-handshake",
		"identity_state": "iam-review",
		"secrets_state": "waiting-external-secret"
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
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
	if warehouse.State != "awaiting-human-approval" {
		t.Fatalf("expected custom overall state, got %q", warehouse.State)
	}
	if warehouse.WarehouseDatabaseState != "queued-for-bootstrap" {
		t.Fatalf("expected custom warehouse db state, got %q", warehouse.WarehouseDatabaseState)
	}
	if warehouse.MetadataStoreState != "vendor-pending" {
		t.Fatalf("expected custom metadata state, got %q", warehouse.MetadataStoreState)
	}
}

func TestCreateOrgRejectsNestedWarehousePayload(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{
		"name": "analytics",
		"max_workers": 4,
		"warehouse": {
			"state": "ready"
		}
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.orgs["analytics"]; ok {
		t.Fatal("expected org create to be rejected when warehouse payload is present")
	}
}

func TestUpdateOrgRejectsNestedWarehousePayload(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics", MaxWorkers: 2}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"max_workers": 4,
		"warehouse": {
			"state": "ready"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if store.orgs["analytics"].MaxWorkers != 2 {
		t.Fatalf("expected org update to be rejected, max_workers = %d", store.orgs["analytics"].MaxWorkers)
	}
}

func TestGetOrgOmitsMinWorkers(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:       "analytics",
		MaxWorkers: 2,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"min_workers"`)) {
		t.Fatalf("expected org response to omit min_workers, got %s", rec.Body.String())
	}
}

func TestManagedWarehouseUpsertColumnsExcludeCreatedAt(t *testing.T) {
	columns := managedWarehouseUpsertColumns()

	if slices.Contains(columns, "created_at") {
		t.Fatal("expected created_at to be excluded from managed warehouse upserts")
	}
	if slices.Contains(columns, "org_id") {
		t.Fatal("expected org_id to be excluded from managed warehouse upserts")
	}
	if !slices.Contains(columns, "updated_at") {
		t.Fatal("expected updated_at to be included in managed warehouse upserts")
	}
	if !slices.Contains(columns, "warehouse_database_database_name") {
		t.Fatal("expected warehouse_database_database_name to be included in managed warehouse upserts")
	}
	if !slices.Contains(columns, "metadata_store_database_name") {
		t.Fatal("expected metadata_store_database_name to be included in managed warehouse upserts")
	}
}
