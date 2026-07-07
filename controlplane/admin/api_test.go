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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/notifications"
	"gorm.io/gorm"
)

type fakeAPIStore struct {
	orgs       map[string]*configstore.Org
	users      map[string]*configstore.OrgUser
	warehouses map[string]*configstore.ManagedWarehouse
}

type adminCapturedNotification struct {
	name  string
	orgID string
	props map[string]any
}

type adminFakeNotifier struct {
	events []adminCapturedNotification
}

func (f *adminFakeNotifier) Notify(event notifications.Event) {
	f.events = append(f.events, adminCapturedNotification{name: event.Name, orgID: event.OrgID, props: event.Props})
}
func (f *adminFakeNotifier) Close() {}

func installAdminFakeNotifier(t *testing.T) *adminFakeNotifier {
	t.Helper()
	fake := &adminFakeNotifier{}
	notifications.SetDefault(fake)
	t.Cleanup(func() { notifications.SetDefault(nil) })
	return fake
}

func (f *adminFakeNotifier) count(name string) int {
	n := 0
	for _, event := range f.events {
		if event.name == name {
			n++
		}
	}
	return n
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
	org.MaxVCPUs = updates.MaxVCPUs
	// Mirrors gormAPIStore: written unconditionally so "" clears (the handler
	// presence-merge already preserved omitted fields).
	org.DefaultWorkerCPU = updates.DefaultWorkerCPU
	org.DefaultWorkerMemory = updates.DefaultWorkerMemory
	org.DefaultWorkerTTL = updates.DefaultWorkerTTL
	org.DefaultWorkerMinHotIdle = updates.DefaultWorkerMinHotIdle
	if updates.HostnameAlias != nil {
		if *updates.HostnameAlias == "" {
			org.HostnameAlias = nil
		} else {
			alias := *updates.HostnameAlias
			org.HostnameAlias = &alias
		}
	}
	return copyOrg(org), true, nil
}

func (s *fakeAPIStore) DeleteOrg(name string) (bool, error) {
	if _, ok := s.orgs[name]; !ok {
		return false, nil
	}
	// Only a non-terminal warehouse row blocks deletion; a "deleted" row (infra
	// torn down by the provisioner) is cascaded away so the name is released.
	if wh, ok := s.warehouses[name]; ok && wh.State != configstore.ManagedWarehouseStateDeleted {
		return false, errWarehouseStillExists
	}
	delete(s.warehouses, name)
	delete(s.orgs, name)
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

func (s *fakeAPIStore) UpdateUser(orgID, username, passwordHash string, passthrough *bool, maxVCPUs *int) (*configstore.OrgUser, bool, error) {
	key := orgID + "/" + username
	user, ok := s.users[key]
	if !ok {
		return nil, false, nil
	}
	if passwordHash != "" {
		user.Password = passwordHash
	}
	if passthrough != nil {
		user.Passthrough = *passthrough
	}
	if maxVCPUs != nil {
		user.MaxVCPUs = *maxVCPUs
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

func (s *fakeAPIStore) MutateManagedWarehouse(orgID string, mutate func(*configstore.ManagedWarehouse) error) (*configstore.ManagedWarehouse, bool, error) {
	org, ok := s.orgs[orgID]
	if !ok {
		return nil, false, nil
	}
	var warehouse configstore.ManagedWarehouse
	if existing, ok := s.warehouses[orgID]; ok {
		warehouse = *existing
	}
	if err := mutate(&warehouse); err != nil {
		return nil, true, err
	}
	clone := copyWarehouse(&warehouse)
	clone.OrgID = orgID
	s.warehouses[orgID] = clone
	org.Warehouse = copyWarehouse(clone)
	return copyWarehouse(clone), true, nil
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
	registerAPIWithStore(r.Group("/api/v1"), store, nil, nil)
	return r
}

func seedOrgWithWarehouse(store *fakeAPIStore, name string) {
	warehouse := &configstore.ManagedWarehouse{
		OrgID:        name,
		DucklingName: name,
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Endpoint: fmt.Sprintf("%s.cluster.example", name),
			Port:     5432,
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
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
			Namespace:  "duckgres",
			IAMRoleARN: "arn:aws:iam::123456789012:role/" + name + "-worker",
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
		State:              configstore.ManagedWarehouseStateReady,
		MetadataStoreState: configstore.ManagedWarehouseStateReady,
		S3State:            configstore.ManagedWarehouseStateReady,
		IdentityState:      configstore.ManagedWarehouseStateReady,
		SecretsState:       configstore.ManagedWarehouseStateReady,
	}
	store.orgs[name] = &configstore.Org{
		Name:      name,
		Warehouse: copyWarehouse(warehouse),
	}
	store.warehouses[name] = warehouse
}

func TestCreateUserIgnoresRemovedDefaultCatalogField(t *testing.T) {
	// default_catalog was removed with Iceberg support. The users endpoints do
	// not reject unknown JSON fields, so a legacy body carrying it still
	// creates the user — the field is just silently ignored.
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{
		"org_id": "analytics",
		"username": "reader",
		"password": "secret",
		"default_catalog": "iceberg"
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if store.users["analytics/reader"] == nil {
		t.Fatal("expected user to be created")
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("default_catalog")) {
		t.Fatalf("response must not echo removed default_catalog field: %s", rec.Body.String())
	}
}

func TestCreateUserAcceptsMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{
		"org_id": "analytics",
		"username": "analyst",
		"password": "secret",
		"max_vcpus": 8
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	user := store.users["analytics/analyst"]
	if user == nil {
		t.Fatal("expected user to be created")
	}
	if user.MaxVCPUs != 8 {
		t.Fatalf("MaxVCPUs = %d, want 8", user.MaxVCPUs)
	}
}

func TestCreateUserRejectsNegativeMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{
		"org_id": "analytics",
		"username": "analyst",
		"password": "secret",
		"max_vcpus": -1
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if len(store.users) != 0 {
		t.Fatalf("expected no users to be created, got %d", len(store.users))
	}
}

func TestUpdateUserMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	store.users["analytics/analyst"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "analyst",
		Password: "hash",
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/analyst", bytes.NewReader([]byte(`{"max_vcpus":12}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := store.users["analytics/analyst"].MaxVCPUs; got != 12 {
		t.Fatalf("MaxVCPUs = %d, want 12", got)
	}
}

func TestUpdateUserMaxVCPUsCanClearToZero(t *testing.T) {
	store := newFakeAPIStore()
	store.users["analytics/analyst"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "analyst",
		Password: "hash",
		MaxVCPUs: 12,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/analyst", bytes.NewReader([]byte(`{"max_vcpus":0}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := store.users["analytics/analyst"].MaxVCPUs; got != 0 {
		t.Fatalf("MaxVCPUs = %d, want 0", got)
	}
}

func TestUpdateUserMaxVCPUsNullClearsToZero(t *testing.T) {
	store := newFakeAPIStore()
	store.users["analytics/analyst"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "analyst",
		Password: "hash",
		MaxVCPUs: 12,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/analyst", bytes.NewReader([]byte(`{"max_vcpus":null}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := store.users["analytics/analyst"].MaxVCPUs; got != 0 {
		t.Fatalf("MaxVCPUs = %d, want 0", got)
	}
}

func TestUpdateUserOmittingMaxVCPUsPreservesIt(t *testing.T) {
	store := newFakeAPIStore()
	store.users["analytics/analyst"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "analyst",
		Password: "hash",
		MaxVCPUs: 12,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/analyst", bytes.NewReader([]byte(`{"passthrough":true}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := store.users["analytics/analyst"].MaxVCPUs; got != 12 {
		t.Fatalf("MaxVCPUs = %d, want preserved 12", got)
	}
}

func TestUpdateUserRejectsNegativeMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	store.users["analytics/analyst"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "analyst",
		Password: "hash",
		MaxVCPUs: 12,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/analyst", bytes.NewReader([]byte(`{"max_vcpus":-1}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := store.users["analytics/analyst"].MaxVCPUs; got != 12 {
		t.Fatalf("MaxVCPUs changed to %d, want preserved 12", got)
	}
}

func TestUpdateUserIgnoresRemovedDefaultCatalogField(t *testing.T) {
	// default_catalog was removed with Iceberg support: a legacy update body
	// carrying it still succeeds (unknown fields are ignored on the users
	// endpoints) and other fields in the same body are applied.
	store := newFakeAPIStore()
	store.users["analytics/reader"] = &configstore.OrgUser{
		OrgID:    "analytics",
		Username: "reader",
		Password: "hash",
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/users/reader", bytes.NewReader([]byte(`{"default_catalog":"iceberg","passthrough":true}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !store.users["analytics/reader"].Passthrough {
		t.Fatal("expected passthrough=true to be applied alongside the ignored field")
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("default_catalog")) {
		t.Fatalf("response must not echo removed default_catalog field: %s", rec.Body.String())
	}
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
	if org.Warehouse.MetadataStore.DatabaseName != "analytics_metadata" {
		t.Fatalf("expected analytics_metadata, got %q", org.Warehouse.MetadataStore.DatabaseName)
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
		"duckling_name": "analytics",
		"warehouse_database": {
			"endpoint": "analytics.cluster.example",
			"port": 5432
		},
		"metadata_store": {
			"kind": "dedicated_rds",
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
		return
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

func TestPutWarehouseMergesPartialUpdateIntoExistingWarehouse(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	body := []byte(`{
		"pgbouncer": {
			"enabled": true
		}
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
	if !warehouse.PgBouncer.Enabled {
		t.Fatal("expected pgbouncer to be enabled")
	}
	if warehouse.MetadataStore.DatabaseName != "analytics_metadata" {
		t.Fatalf("expected metadata db analytics_metadata, got %q", warehouse.MetadataStore.DatabaseName)
	}
	if warehouse.S3.Bucket != "analytics-bucket" {
		t.Fatalf("expected s3 bucket analytics-bucket, got %q", warehouse.S3.Bucket)
	}
	if warehouse.MetadataStoreCredentials.Name != "analytics-metadata" {
		t.Fatalf("expected metadata secret analytics-metadata, got %q", warehouse.MetadataStoreCredentials.Name)
	}
	if warehouse.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected state ready, got %q", warehouse.State)
	}
}

func TestPutWarehouseDisablesPgBouncerWhenSetToFalse(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	store.warehouses["analytics"].PgBouncer = configstore.ManagedWarehousePgBouncer{Enabled: true}
	router := newTestAPIRouter(store)

	body := []byte(`{"pgbouncer": {"enabled": false}}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if store.warehouses["analytics"].PgBouncer.Enabled {
		t.Fatal("expected pgbouncer to be disabled after PUT with enabled=false")
	}
}

func TestPutWarehouseRejectsRemovedIcebergField(t *testing.T) {
	// Iceberg support was removed: "iceberg" is no longer a whitelisted field
	// on the warehouse PUT, so the strict decode rejects it as unknown.
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	body := []byte(`{"iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPutWarehousePreservesNestedFieldsOnPartialUpdate(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	// Send only one inner field. Every other metadata_store field must stay
	// as seeded — confirms the merge is nested-aware, not whole-struct replace.
	body := []byte(`{"metadata_store": {"database_name": "renamed_metadata"}}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	got := store.warehouses["analytics"].MetadataStore
	if got.DatabaseName != "renamed_metadata" {
		t.Fatalf("database_name = %q, want renamed_metadata", got.DatabaseName)
	}
	if got.Endpoint != "analytics-metadata.cluster.example" {
		t.Fatalf("endpoint = %q, want analytics-metadata.cluster.example (nested fields were wiped)", got.Endpoint)
	}
	if got.Port != 5432 {
		t.Fatalf("port = %d, want 5432", got.Port)
	}
	if got.Kind != "dedicated_rds" {
		t.Fatalf("kind = %q, want dedicated_rds", got.Kind)
	}
	if got.Username != "metadata_user" {
		t.Fatalf("username = %q, want metadata_user", got.Username)
	}
}

func TestPutWarehouseRejectsOversizedBody(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	// Pad the body past the 1 MiB cap inside a valid top-level field so the
	// reader errors on size rather than JSON parsing.
	oversized := strings.Repeat("a", (1<<20)+1024)
	body := []byte(`{"status_message": "` + oversized + `"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPutWarehouseRejectsSecretRefsOutsideTenantScope(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"metadata_store": {
			"kind": "dedicated_rds",
			"endpoint": "analytics-metadata.cluster.example",
			"port": 5432,
			"database_name": "ducklake_metadata",
			"username": "metadata_user"
		},
		"worker_identity": {
			"namespace": "tenant-a"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-b",
			"name": "billing-metadata",
			"key": "dsn"
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

func TestPutWarehouseRejectsSecretRefsWithoutWorkerNamespace(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"duckling_name": "analytics",
		"worker_identity": {
			"iam_role_arn": "arn:aws:iam::123456789012:role/analytics-worker"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-b",
			"name": "analytics-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "worker_identity.namespace") {
		t.Fatalf("expected worker namespace validation error, got %s", rec.Body.String())
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
			"endpoint": "analytics.cluster.example"
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
		"duckling_name": "analytics",
		"state": "awaiting-human-approval",
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
		return
	}
	if warehouse.State != "awaiting-human-approval" {
		t.Fatalf("expected custom overall state, got %q", warehouse.State)
	}
	if warehouse.MetadataStoreState != "vendor-pending" {
		t.Fatalf("expected custom metadata state, got %q", warehouse.MetadataStoreState)
	}
}

func TestPutWarehouseRejectsCrossTenantSecretRefs(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"duckling_name": "analytics",
		"worker_identity": {
			"namespace": "tenant-analytics"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-billing",
			"name": "analytics-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "tenant-owned") {
		t.Fatalf("expected tenant-owned secret ref validation error, got %s", rec.Body.String())
	}
}

func TestPutWarehouseRejectsSecretRefWithoutExplicitNamespace(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"duckling_name": "analytics",
		"worker_identity": {
			"namespace": "tenant-analytics"
		},
		"metadata_store_credentials": {
			"name": "analytics-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "SecretRef.Namespace") {
		t.Fatalf("expected explicit namespace validation error, got %s", rec.Body.String())
	}
}

func TestPutWarehouseRejectsCrossTenantSecretReference(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"worker_identity": {
			"namespace": "tenant-a"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-b",
			"name": "analytics-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.warehouses["analytics"]; ok {
		t.Fatal("expected invalid warehouse payload to be rejected")
	}
}

func TestPutWarehouseRejectsSecretReferenceOutsideOrgPrefix(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"worker_identity": {
			"namespace": "tenant-a"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-a",
			"name": "shared-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.warehouses["analytics"]; ok {
		t.Fatal("expected invalid warehouse payload to be rejected")
	}
}

func TestPutWarehouseRejectsSecretReferenceWithoutOrgPrefix(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"duckling_name": "analytics",
		"worker_identity": {
			"namespace": "tenant-a"
		},
		"metadata_store_credentials": {
			"namespace": "tenant-a",
			"name": "shared-analytics-metadata",
			"key": "dsn"
		}
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics/warehouse", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "must start with") {
		t.Fatalf("expected org prefix validation error, got %s", rec.Body.String())
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

func TestDeleteOrgRejectsWhenWarehouseStillExists(t *testing.T) {
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "deprovision") {
		t.Fatalf("expected error to mention deprovision, got: %s", rec.Body.String())
	}
	if _, ok := store.orgs["analytics"]; !ok {
		t.Fatal("expected org to survive a rejected delete")
	}
	if _, ok := store.warehouses["analytics"]; !ok {
		t.Fatal("expected warehouse to survive a rejected delete")
	}
}

func TestDeleteOrgSucceedsAfterWarehouseRemoved(t *testing.T) {
	notifier := installAdminFakeNotifier(t)
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	delete(store.warehouses, "analytics")
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if _, ok := store.orgs["analytics"]; ok {
		t.Fatal("expected org to be deleted once the warehouse is gone")
	}
	if got := notifier.count("org_deleted"); got != 1 {
		t.Fatalf("org_deleted notifications = %d, want 1 (all: %+v)", got, notifier.events)
	}
}

func TestDeleteOrgCascadesDeletedWarehouse(t *testing.T) {
	notifier := installAdminFakeNotifier(t)
	store := newFakeAPIStore()
	seedOrgWithWarehouse(store, "analytics")
	// Simulate a completed deprovision: the provisioner leaves the warehouse
	// row behind in the terminal "deleted" state rather than removing it.
	store.warehouses["analytics"].State = configstore.ManagedWarehouseStateDeleted
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if _, ok := store.orgs["analytics"]; ok {
		t.Fatal("expected org to be deleted once its warehouse is fully deprovisioned")
	}
	if _, ok := store.warehouses["analytics"]; ok {
		t.Fatal("expected the deleted warehouse row to be cascaded away")
	}
	if got := notifier.count("org_deleted"); got != 1 {
		t.Fatalf("org_deleted notifications = %d, want 1 (all: %+v)", got, notifier.events)
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

func TestGetOrgOmitsMaxConnections(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name: "analytics",
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orgs/analytics", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"max_connections"`)) {
		t.Fatalf("expected org response to omit max_connections, got %s", rec.Body.String())
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
	if !slices.Contains(columns, "metadata_store_database_name") {
		t.Fatal("expected metadata_store_database_name to be included in managed warehouse upserts")
	}
	// Regression guards: image and duck_lake_version must be in the upsert
	// column list so the per-tenant pinning patch endpoint actually
	// persists. If either is missing, PATCH /orgs/:id/warehouse/pinning
	// silently no-ops and the matrix-build cutover breaks. Note the actual
	// Postgres column is `duck_lake_version` (GORM CamelCase→snake_case
	// splits on every uppercase-after-lowercase boundary), not the JSON-tag
	// shape `ducklake_version` callers see on the wire.
	if !slices.Contains(columns, "image") {
		t.Fatal("expected image to be included in managed warehouse upserts (tenant pinning)")
	}
	if !slices.Contains(columns, "duck_lake_version") {
		t.Fatal("expected duck_lake_version to be included in managed warehouse upserts (tenant pinning)")
	}
}

func TestPatchTenantPinningSetsImageAndDuckLakeVersion(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID:           "analytics",
		Image:           "old-image:1.0",
		DuckLakeVersion: "0.3",
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"image":"posthog/duckgres-worker:abc-duckdb1.5.1","ducklake_version":"0.4"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	got := store.warehouses["analytics"]
	if got.Image != "posthog/duckgres-worker:abc-duckdb1.5.1" {
		t.Errorf("image = %q, want posthog/duckgres-worker:abc-duckdb1.5.1", got.Image)
	}
	if got.DuckLakeVersion != "0.4" {
		t.Errorf("ducklake_version = %q, want 0.4", got.DuckLakeVersion)
	}
}

func TestPatchTenantPinningPreservesUntouchedField(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID:           "analytics",
		Image:           "existing-image:1.0",
		DuckLakeVersion: "0.4",
	}
	router := newTestAPIRouter(store)

	// Only set image; ducklake_version absent ⇒ preserve.
	body := []byte(`{"image":"new-image:2.0"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	got := store.warehouses["analytics"]
	if got.Image != "new-image:2.0" {
		t.Errorf("image = %q, want new-image:2.0", got.Image)
	}
	if got.DuckLakeVersion != "0.4" {
		t.Errorf("ducklake_version = %q, want 0.4 (untouched)", got.DuckLakeVersion)
	}
}

func TestPatchTenantPinningEmptyStringClearsField(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{
		OrgID:           "analytics",
		Image:           "pinned:1.0",
		DuckLakeVersion: "0.4",
	}
	router := newTestAPIRouter(store)

	// Explicit "" — distinct from absent — falls back to global default.
	body := []byte(`{"image":""}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	got := store.warehouses["analytics"]
	if got.Image != "" {
		t.Errorf("image = %q, want empty (cleared)", got.Image)
	}
	if got.DuckLakeVersion != "0.4" {
		t.Errorf("ducklake_version = %q, want 0.4 (untouched)", got.DuckLakeVersion)
	}
}

func TestPatchTenantPinningRejectsEmptyBody(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{OrgID: "analytics"}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPatchTenantPinningRejectsBadVersion(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{OrgID: "analytics"}
	router := newTestAPIRouter(store)

	cases := []string{
		`{"ducklake_version":"foo"}`,
		`{"ducklake_version":"1"}`,
		`{"ducklake_version":"1.0.0"}`,
		`{"ducklake_version":"1."}`,
		`{"ducklake_version":".1"}`,
	}
	for _, body := range cases {
		req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader([]byte(body)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want %d", body, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestPatchTenantPinningRejectsUnknownFields(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{Name: "analytics"}
	store.warehouses["analytics"] = &configstore.ManagedWarehouse{OrgID: "analytics"}
	router := newTestAPIRouter(store)

	body := []byte(`{"image":"x","not_a_real_field":42}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/analytics/warehouse/pinning", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPatchTenantPinningReturnsNotFoundForMissingOrg(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"image":"x"}`)
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/missing/warehouse/pinning", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestCreateOrgPersistsHostnameAlias(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"name":"tenant-alpha-id","database_name":"tenant_alpha","hostname_alias":"entirely-chief-wildcat"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	stored := store.orgs["tenant-alpha-id"]
	if stored == nil {
		t.Fatal("org not stored")
	}
	if stored.HostnameAlias == nil || *stored.HostnameAlias != "entirely-chief-wildcat" {
		t.Errorf("HostnameAlias = %v, want pointer to %q", stored.HostnameAlias, "entirely-chief-wildcat")
	}
}

func TestCreateOrgEmptyHostnameAliasIsTreatedAsNone(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"name":"plain","database_name":"plain","hostname_alias":""}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	stored := store.orgs["plain"]
	if stored == nil {
		t.Fatal("org not stored")
	}
	if stored.HostnameAlias != nil {
		t.Errorf("HostnameAlias = %v, want nil (empty string normalized to none)", stored.HostnameAlias)
	}
}

func TestUpdateOrgClearsHostnameAliasWithEmptyString(t *testing.T) {
	store := newFakeAPIStore()
	alias := "entirely-chief-wildcat"
	store.orgs["tenant-alpha-id"] = &configstore.Org{
		Name:          "tenant-alpha-id",
		DatabaseName:  "tenant_alpha",
		HostnameAlias: &alias,
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"hostname_alias":""}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/tenant-alpha-id", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["tenant-alpha-id"]
	if stored.HostnameAlias != nil {
		t.Errorf("HostnameAlias not cleared: %v", stored.HostnameAlias)
	}
}

func TestCreateOrgRejectsInvalidHostnameAlias(t *testing.T) {
	cases := []struct {
		name  string
		alias string
	}{
		{"contains dot (would silently fail SNI matching)", "foo.bar"},
		{"contains underscore", "foo_bar"},
		{"leading hyphen", "-foo"},
		{"trailing hyphen", "foo-"},
		{"contains slash", "foo/bar"},
		{"contains uppercase A-Z is fine actually", ""}, // skipped — uppercase is allowed
	}
	for _, tc := range cases {
		if tc.alias == "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeAPIStore()
			router := newTestAPIRouter(store)

			body := []byte(fmt.Sprintf(`{"name":"acme","database_name":"acme","hostname_alias":%q}`, tc.alias))
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("alias %q: status = %d, want %d: %s", tc.alias, rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if _, ok := store.orgs["acme"]; ok {
				t.Errorf("alias %q: org should NOT have been created", tc.alias)
			}
		})
	}
}

func TestCreateOrgAcceptsLongValidHostnameAlias(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	// 63 chars exactly — at the RFC 1035 DNS label limit.
	alias := strings.Repeat("a", 63)
	body := []byte(fmt.Sprintf(`{"name":"acme","database_name":"acme","hostname_alias":%q}`, alias))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("63-char alias should be accepted: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateOrgRejectsHostnameAliasOver63Chars(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	alias := strings.Repeat("a", 64) // one over the limit
	body := []byte(fmt.Sprintf(`{"name":"acme","database_name":"acme","hostname_alias":%q}`, alias))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("64-char alias should be rejected: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestUpdateOrgOmittingHostnameAliasPreservesIt(t *testing.T) {
	store := newFakeAPIStore()
	alias := "entirely-chief-wildcat"
	store.orgs["tenant-alpha-id"] = &configstore.Org{
		Name:          "tenant-alpha-id",
		DatabaseName:  "tenant_alpha",
		HostnameAlias: &alias,
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"max_workers":8}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/tenant-alpha-id", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["tenant-alpha-id"]
	if stored.HostnameAlias == nil || *stored.HostnameAlias != "entirely-chief-wildcat" {
		t.Errorf("HostnameAlias not preserved: %v", stored.HostnameAlias)
	}
}

func TestIsValidDuckLakeSpecVersion(t *testing.T) {
	cases := []struct {
		v    string
		want bool
	}{
		{"0.3", true},
		{"0.4", true},
		{"1.0", true},
		{"0.10", true},
		{"1.5", true},
		{"", false},
		{"1", false},
		{"1.", false},
		{".1", false},
		{"1.0.0", false},
		{"foo", false},
		{"1.x", false},
		{"-1.0", false},
	}
	for _, tc := range cases {
		if got := isValidDuckLakeSpecVersion(tc.v); got != tc.want {
			t.Errorf("isValidDuckLakeSpecVersion(%q) = %v, want %v", tc.v, got, tc.want)
		}
	}
}

func TestUpdateOrgMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:       "analytics",
		MaxWorkers: 2,
		MaxVCPUs:   5,
	}
	router := newTestAPIRouter(store)

	body := []byte(`{
		"max_vcpus": 10
	}`)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if store.orgs["analytics"].MaxVCPUs != 10 {
		t.Fatalf("expected org max_vcpus to be updated, got %d", store.orgs["analytics"].MaxVCPUs)
	}
	if store.orgs["analytics"].MaxWorkers != 2 {
		t.Fatalf("expected max_workers to be preserved, got %d", store.orgs["analytics"].MaxWorkers)
	}
}

func TestUpdateOrgMaxVCPUsCanClearToZero(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:       "analytics",
		MaxWorkers: 2,
		MaxVCPUs:   10,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader([]byte(`{"max_vcpus":0}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if store.orgs["analytics"].MaxVCPUs != 0 {
		t.Fatalf("expected org max_vcpus to be cleared, got %d", store.orgs["analytics"].MaxVCPUs)
	}
	if store.orgs["analytics"].MaxWorkers != 2 {
		t.Fatalf("expected max_workers to be preserved, got %d", store.orgs["analytics"].MaxWorkers)
	}
}

func TestUpdateOrgOmittingMaxVCPUsPreservesIt(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:       "analytics",
		MaxWorkers: 2,
		MaxVCPUs:   10,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader([]byte(`{"max_workers":3}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if store.orgs["analytics"].MaxVCPUs != 10 {
		t.Fatalf("expected org max_vcpus to be preserved, got %d", store.orgs["analytics"].MaxVCPUs)
	}
	if store.orgs["analytics"].MaxWorkers != 3 {
		t.Fatalf("expected max_workers to be updated, got %d", store.orgs["analytics"].MaxWorkers)
	}
}

func TestUpdateOrgMaxVCPUsNullClearsToZero(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:       "analytics",
		MaxWorkers: 2,
		MaxVCPUs:   10,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader([]byte(`{"max_vcpus":null}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if store.orgs["analytics"].MaxVCPUs != 0 {
		t.Fatalf("expected org max_vcpus to be cleared, got %d", store.orgs["analytics"].MaxVCPUs)
	}
	if store.orgs["analytics"].MaxWorkers != 2 {
		t.Fatalf("expected max_workers to be preserved, got %d", store.orgs["analytics"].MaxWorkers)
	}
}

func TestUpdateOrgRejectsNegativeMaxVCPUs(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["analytics"] = &configstore.Org{
		Name:     "analytics",
		MaxVCPUs: 10,
	}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/analytics", bytes.NewReader([]byte(`{"max_vcpus":-1}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if store.orgs["analytics"].MaxVCPUs != 10 {
		t.Fatalf("expected org max_vcpus to be preserved, got %d", store.orgs["analytics"].MaxVCPUs)
	}
}

// --- Org default worker profile (default_worker_cpu/memory/ttl) ---

func TestUpdateOrgSetsDefaultWorkerProfile(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme", DatabaseName: "acme"}
	router := newTestAPIRouter(store)

	body := []byte(`{"default_worker_cpu":"2","default_worker_memory":"8Gi","default_worker_ttl":"10m"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["acme"]
	if stored.DefaultWorkerCPU != "2" || stored.DefaultWorkerMemory != "8Gi" || stored.DefaultWorkerTTL != "10m" {
		t.Fatalf("stored default profile = %q/%q/%q, want 2/8Gi/10m",
			stored.DefaultWorkerCPU, stored.DefaultWorkerMemory, stored.DefaultWorkerTTL)
	}
	// The fields must round-trip in the response JSON so operators can read
	// back what they set (GET uses the same model serialization).
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["default_worker_cpu"] != "2" || resp["default_worker_memory"] != "8Gi" || resp["default_worker_ttl"] != "10m" {
		t.Fatalf("response JSON missing default worker profile fields: %s", rec.Body.String())
	}
}

func TestUpdateOrgClearsDefaultWorkerProfileWithEmptyStrings(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{
		Name: "acme", DatabaseName: "acme",
		DefaultWorkerCPU: "2", DefaultWorkerMemory: "8Gi", DefaultWorkerTTL: "10m",
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"default_worker_cpu":"","default_worker_memory":"","default_worker_ttl":""}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["acme"]
	if stored.DefaultWorkerCPU != "" || stored.DefaultWorkerMemory != "" || stored.DefaultWorkerTTL != "" {
		t.Fatalf("default profile not cleared: %q/%q/%q",
			stored.DefaultWorkerCPU, stored.DefaultWorkerMemory, stored.DefaultWorkerTTL)
	}
}

func TestUpdateOrgOmittingDefaultWorkerProfilePreservesIt(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{
		Name: "acme", DatabaseName: "acme",
		DefaultWorkerCPU: "2", DefaultWorkerMemory: "8Gi", DefaultWorkerTTL: "10m",
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"max_workers":4}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["acme"]
	if stored.DefaultWorkerCPU != "2" || stored.DefaultWorkerMemory != "8Gi" || stored.DefaultWorkerTTL != "10m" {
		t.Fatalf("default profile not preserved: %q/%q/%q",
			stored.DefaultWorkerCPU, stored.DefaultWorkerMemory, stored.DefaultWorkerTTL)
	}
}

func TestUpdateOrgSetsDefaultWorkerMinHotIdle(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme", DatabaseName: "acme"}
	router := newTestAPIRouter(store)

	body := []byte(`{"default_worker_min_hot_idle":2}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	stored := store.orgs["acme"]
	if stored.DefaultWorkerMinHotIdle != 2 {
		t.Fatalf("stored default_worker_min_hot_idle = %d, want 2", stored.DefaultWorkerMinHotIdle)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["default_worker_min_hot_idle"] != float64(2) {
		t.Fatalf("response JSON missing default_worker_min_hot_idle: %s", rec.Body.String())
	}
}

func TestUpdateOrgOmittingDefaultWorkerMinHotIdlePreservesIt(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{
		Name: "acme", DatabaseName: "acme", DefaultWorkerMinHotIdle: 2,
	}
	router := newTestAPIRouter(store)

	body := []byte(`{"max_workers":4}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := store.orgs["acme"].DefaultWorkerMinHotIdle; got != 2 {
		t.Fatalf("default_worker_min_hot_idle not preserved: got %d, want 2", got)
	}
}

func TestUpdateOrgRejectsNegativeDefaultWorkerMinHotIdle(t *testing.T) {
	store := newFakeAPIStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme", DatabaseName: "acme", DefaultWorkerMinHotIdle: 2}
	router := newTestAPIRouter(store)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader([]byte(`{"default_worker_min_hot_idle":-1}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := store.orgs["acme"].DefaultWorkerMinHotIdle; got != 2 {
		t.Fatalf("invalid payload mutated default_worker_min_hot_idle: got %d", got)
	}
}

func TestUpdateOrgRejectsInvalidDefaultWorkerProfile(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"garbage cpu quantity", `{"default_worker_cpu":"lots"}`},
		{"zero cpu", `{"default_worker_cpu":"0"}`},
		{"negative memory", `{"default_worker_memory":"-8Gi"}`},
		{"garbage memory quantity", `{"default_worker_memory":"big"}`},
		{"garbage ttl duration", `{"default_worker_ttl":"whenever"}`},
		{"negative ttl", `{"default_worker_ttl":"-5m"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeAPIStore()
			store.orgs["acme"] = &configstore.Org{Name: "acme", DatabaseName: "acme", DefaultWorkerCPU: "2"}
			router := newTestAPIRouter(store)

			req := httptest.NewRequest(http.MethodPut, "/api/v1/orgs/acme", bytes.NewReader([]byte(tc.body)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if store.orgs["acme"].DefaultWorkerCPU != "2" {
				t.Fatal("invalid payload must not mutate the stored org")
			}
		})
	}
}

func TestCreateOrgAcceptsDefaultWorkerProfile(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"name":"acme","database_name":"acme","default_worker_cpu":"2","default_worker_memory":"8Gi","default_worker_ttl":"75m"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	stored := store.orgs["acme"]
	if stored.DefaultWorkerCPU != "2" || stored.DefaultWorkerMemory != "8Gi" || stored.DefaultWorkerTTL != "75m" {
		t.Fatalf("stored default profile = %q/%q/%q, want 2/8Gi/75m",
			stored.DefaultWorkerCPU, stored.DefaultWorkerMemory, stored.DefaultWorkerTTL)
	}
}

func TestCreateOrgAcceptsDefaultWorkerMinHotIdle(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"name":"acme","database_name":"acme","default_worker_min_hot_idle":1}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if got := store.orgs["acme"].DefaultWorkerMinHotIdle; got != 1 {
		t.Fatalf("stored default_worker_min_hot_idle = %d, want 1", got)
	}
}

func TestCreateOrgRejectsInvalidDefaultWorkerTTL(t *testing.T) {
	store := newFakeAPIStore()
	router := newTestAPIRouter(store)

	body := []byte(`{"name":"acme","database_name":"acme","default_worker_ttl":"10 minutes"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := store.orgs["acme"]; ok {
		t.Fatal("org should NOT have been created")
	}
}
