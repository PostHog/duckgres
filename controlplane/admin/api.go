//go:build kubernetes

package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errWarehousePayloadNotAllowed = errors.New("warehouse payload must be updated via /orgs/:name/warehouse")

// WorkerStatus represents a worker's current status for the API.
type WorkerStatus struct {
	ID             int    `json:"id"`
	Org            string `json:"org"`
	ActiveSessions int    `json:"active_sessions"`
	Status         string `json:"status"`
}

// SessionStatus represents an active session for the API.
type SessionStatus struct {
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Org      string `json:"org"`
}

// ClusterStatus aggregates cluster state for the dashboard.
type ClusterStatus struct {
	TotalOrgs     int         `json:"total_orgs"`
	TotalWorkers  int         `json:"total_workers"`
	TotalSessions int         `json:"total_sessions"`
	Orgs          []OrgStatus `json:"orgs"`
}

// OrgStatus is a per-org summary.
type OrgStatus struct {
	Name           string `json:"name"`
	Workers        int    `json:"workers"`
	ActiveSessions int    `json:"active_sessions"`
	MaxWorkers     int    `json:"max_workers"`
	MemoryBudget   string `json:"memory_budget"`
}

// OrgStackInfo provides info about an org's live state.
// Implemented by the controlplane.OrgRouter via adapter.
type OrgStackInfo interface {
	// AllOrgStats returns per-org worker and session counts.
	AllOrgStats() []OrgStatus
	// AllWorkerStatuses returns all workers across orgs.
	AllWorkerStatuses() []WorkerStatus
	// AllSessionStatuses returns all active sessions across orgs.
	AllSessionStatuses() []SessionStatus
}

// RegisterAPI registers all admin REST endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store *configstore.ConfigStore, info OrgStackInfo) {
	registerAPIWithStore(r, newGormAPIStore(store), info)
}

func registerAPIWithStore(r *gin.RouterGroup, store apiStore, info OrgStackInfo) {
	h := &apiHandler{store: store, info: info}

	// Orgs CRUD
	r.GET("/orgs", h.listOrgs)
	r.POST("/orgs", h.createOrg)
	r.GET("/orgs/:name", h.getOrg)
	r.PUT("/orgs/:name", h.updateOrg)
	r.DELETE("/orgs/:name", h.deleteOrg)
	r.GET("/orgs/:name/warehouse", h.getManagedWarehouse)
	r.PUT("/orgs/:name/warehouse", h.putManagedWarehouse)

	// Users CRUD
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/users/:username", h.getUser)
	r.PUT("/users/:username", h.updateUser)
	r.DELETE("/users/:username", h.deleteUser)

	// Workers (read-only)
	r.GET("/workers", h.listWorkers)

	// Sessions (read-only)
	r.GET("/sessions", h.listSessions)

	// Config singletons
	r.GET("/config/global", h.getGlobalConfig)
	r.PUT("/config/global", h.updateGlobalConfig)
	r.GET("/config/ducklake", h.getDuckLakeConfig)
	r.PUT("/config/ducklake", h.updateDuckLakeConfig)
	r.GET("/config/ratelimit", h.getRateLimitConfig)
	r.PUT("/config/ratelimit", h.updateRateLimitConfig)
	r.GET("/config/querylog", h.getQueryLogConfig)
	r.PUT("/config/querylog", h.updateQueryLogConfig)

	// Overview
	r.GET("/status", h.getClusterStatus)
}

type apiStore interface {
	ListOrgs() ([]configstore.Org, error)
	CreateOrg(org *configstore.Org) error
	GetOrg(name string) (*configstore.Org, error)
	UpdateOrg(name string, updates configstore.Org) (*configstore.Org, bool, error)
	DeleteOrg(name string) (bool, error)

	ListUsers() ([]configstore.OrgUser, error)
	CreateUser(user *configstore.OrgUser) error
	GetUser(username string) (*configstore.OrgUser, error)
	UpdateUser(username, passwordHash, orgID string) (*configstore.OrgUser, bool, error)
	DeleteUser(username string) (bool, error)

	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error)

	GetGlobalConfig() (configstore.GlobalConfig, error)
	SaveGlobalConfig(cfg *configstore.GlobalConfig) error
	GetDuckLakeConfig() (configstore.DuckLakeConfig, error)
	SaveDuckLakeConfig(cfg *configstore.DuckLakeConfig) error
	GetRateLimitConfig() (configstore.RateLimitConfig, error)
	SaveRateLimitConfig(cfg *configstore.RateLimitConfig) error
	GetQueryLogConfig() (configstore.QueryLogConfig, error)
	SaveQueryLogConfig(cfg *configstore.QueryLogConfig) error
}

type gormAPIStore struct {
	store *configstore.ConfigStore
}

func newGormAPIStore(store *configstore.ConfigStore) apiStore {
	return &gormAPIStore{store: store}
}

func (s *gormAPIStore) db() *gorm.DB {
	return s.store.DB()
}

func (s *gormAPIStore) ListOrgs() ([]configstore.Org, error) {
	var orgs []configstore.Org
	if err := s.db().Preload("Users").Preload("Warehouse").Find(&orgs).Error; err != nil {
		return nil, err
	}
	return orgs, nil
}

func (s *gormAPIStore) CreateOrg(org *configstore.Org) error {
	org.Warehouse = nil
	return s.db().Omit("Warehouse").Create(org).Error
}

func (s *gormAPIStore) GetOrg(name string) (*configstore.Org, error) {
	var org configstore.Org
	if err := s.db().Preload("Users").Preload("Warehouse").First(&org, "name = ?", name).Error; err != nil {
		return nil, err
	}
	return &org, nil
}

func (s *gormAPIStore) UpdateOrg(name string, updates configstore.Org) (*configstore.Org, bool, error) {
	result := s.db().Model(&configstore.Org{}).Where("name = ?", name).Updates(map[string]interface{}{
		"max_workers":    updates.MaxWorkers,
		"memory_budget":  updates.MemoryBudget,
		"idle_timeout_s": updates.IdleTimeoutS,
	})
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	org, err := s.GetOrg(name)
	if err != nil {
		return nil, true, err
	}
	return org, true, nil
}

func (s *gormAPIStore) DeleteOrg(name string) (bool, error) {
	returnRows := int64(0)
	err := s.db().Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("org_id = ?", name).Delete(&configstore.OrgUser{}).Error; err != nil {
			return err
		}
		result := tx.Where("name = ?", name).Delete(&configstore.Org{})
		if result.Error != nil {
			return result.Error
		}
		returnRows = result.RowsAffected
		return nil
	})
	if err != nil {
		return false, err
	}
	return returnRows > 0, nil
}

func (s *gormAPIStore) ListUsers() ([]configstore.OrgUser, error) {
	var users []configstore.OrgUser
	if err := s.db().Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

func (s *gormAPIStore) CreateUser(user *configstore.OrgUser) error {
	return s.db().Create(user).Error
}

func (s *gormAPIStore) GetUser(username string) (*configstore.OrgUser, error) {
	var user configstore.OrgUser
	if err := s.db().First(&user, "username = ?", username).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *gormAPIStore) UpdateUser(username, passwordHash, orgID string) (*configstore.OrgUser, bool, error) {
	updates := map[string]interface{}{}
	if passwordHash != "" {
		updates["password"] = passwordHash
	}
	if orgID != "" {
		updates["org_id"] = orgID
	}
	result := s.db().Model(&configstore.OrgUser{}).Where("username = ?", username).Updates(updates)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	user, err := s.GetUser(username)
	if err != nil {
		return nil, true, err
	}
	return user, true, nil
}

func (s *gormAPIStore) DeleteUser(username string) (bool, error) {
	result := s.db().Where("username = ?", username).Delete(&configstore.OrgUser{})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *gormAPIStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	var warehouse configstore.ManagedWarehouse
	if err := s.db().First(&warehouse, "org_id = ?", orgID).Error; err != nil {
		return nil, err
	}
	return &warehouse, nil
}

func (s *gormAPIStore) UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error) {
	var count int64
	if err := s.db().Model(&configstore.Org{}).Where("name = ?", orgID).Count(&count).Error; err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	warehouse.OrgID = orgID
	warehouse.UpdatedAt = time.Now().UTC()
	if err := s.db().Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "org_id"}},
		DoUpdates: clause.AssignmentColumns(managedWarehouseUpsertColumns()),
	}).Create(warehouse).Error; err != nil {
		return nil, true, err
	}
	stored, err := s.GetManagedWarehouse(orgID)
	if err != nil {
		return nil, true, err
	}
	return stored, true, nil
}

func managedWarehouseUpsertColumns() []string {
	return []string{
		"warehouse_database_region",
		"warehouse_database_endpoint",
		"warehouse_database_port",
		"warehouse_database_database_name",
		"warehouse_database_username",
		"metadata_store_kind",
		"metadata_store_engine",
		"metadata_store_region",
		"metadata_store_endpoint",
		"metadata_store_port",
		"metadata_store_database_name",
		"metadata_store_username",
		"s3_provider",
		"s3_region",
		"s3_bucket",
		"s3_path_prefix",
		"s3_endpoint",
		"s3_use_ssl",
		"s3_url_style",
		"worker_identity_namespace",
		"worker_identity_service_account_name",
		"worker_identity_iam_role_arn",
		"warehouse_database_credentials_namespace",
		"warehouse_database_credentials_name",
		"warehouse_database_credentials_key",
		"metadata_store_credentials_namespace",
		"metadata_store_credentials_name",
		"metadata_store_credentials_key",
		"s3_credentials_namespace",
		"s3_credentials_name",
		"s3_credentials_key",
		"runtime_config_namespace",
		"runtime_config_name",
		"runtime_config_key",
		"state",
		"status_message",
		"warehouse_database_state",
		"warehouse_database_status_message",
		"metadata_store_state",
		"metadata_store_status_message",
		"s3_state",
		"s3_status_message",
		"identity_state",
		"identity_status_message",
		"secrets_state",
		"secrets_status_message",
		"ready_at",
		"failed_at",
		"updated_at",
	}
}

func (s *gormAPIStore) GetGlobalConfig() (configstore.GlobalConfig, error) {
	var cfg configstore.GlobalConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.GlobalConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveGlobalConfig(cfg *configstore.GlobalConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetDuckLakeConfig() (configstore.DuckLakeConfig, error) {
	var cfg configstore.DuckLakeConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.DuckLakeConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveDuckLakeConfig(cfg *configstore.DuckLakeConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetRateLimitConfig() (configstore.RateLimitConfig, error) {
	var cfg configstore.RateLimitConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.RateLimitConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveRateLimitConfig(cfg *configstore.RateLimitConfig) error {
	return s.db().Save(cfg).Error
}

func (s *gormAPIStore) GetQueryLogConfig() (configstore.QueryLogConfig, error) {
	var cfg configstore.QueryLogConfig
	if err := s.db().First(&cfg, 1).Error; err != nil {
		return configstore.QueryLogConfig{}, err
	}
	return cfg, nil
}

func (s *gormAPIStore) SaveQueryLogConfig(cfg *configstore.QueryLogConfig) error {
	return s.db().Save(cfg).Error
}

type apiHandler struct {
	store apiStore
	info  OrgStackInfo
}

type managedWarehouseRequest struct {
	WarehouseDatabase              configstore.ManagedWarehouseDatabase          `json:"warehouse_database"`
	MetadataStore                  configstore.ManagedWarehouseMetadataStore     `json:"metadata_store"`
	S3                             configstore.ManagedWarehouseS3                `json:"s3"`
	WorkerIdentity                 configstore.ManagedWarehouseWorkerIdentity    `json:"worker_identity"`
	WarehouseDatabaseCredentials   configstore.SecretRef                         `json:"warehouse_database_credentials"`
	MetadataStoreCredentials       configstore.SecretRef                         `json:"metadata_store_credentials"`
	S3Credentials                  configstore.SecretRef                         `json:"s3_credentials"`
	RuntimeConfig                  configstore.SecretRef                         `json:"runtime_config"`
	State                          configstore.ManagedWarehouseProvisioningState `json:"state"`
	StatusMessage                  string                                        `json:"status_message"`
	WarehouseDatabaseState         configstore.ManagedWarehouseProvisioningState `json:"warehouse_database_state"`
	WarehouseDatabaseStatusMessage string                                        `json:"warehouse_database_status_message"`
	MetadataStoreState             configstore.ManagedWarehouseProvisioningState `json:"metadata_store_state"`
	MetadataStoreStatusMessage     string                                        `json:"metadata_store_status_message"`
	S3State                        configstore.ManagedWarehouseProvisioningState `json:"s3_state"`
	S3StatusMessage                string                                        `json:"s3_status_message"`
	IdentityState                  configstore.ManagedWarehouseProvisioningState `json:"identity_state"`
	IdentityStatusMessage          string                                        `json:"identity_status_message"`
	SecretsState                   configstore.ManagedWarehouseProvisioningState `json:"secrets_state"`
	SecretsStatusMessage           string                                        `json:"secrets_status_message"`
	ReadyAt                        *time.Time                                    `json:"ready_at"`
	FailedAt                       *time.Time                                    `json:"failed_at"`
}

func (r managedWarehouseRequest) toManagedWarehouse() configstore.ManagedWarehouse {
	return configstore.ManagedWarehouse{
		WarehouseDatabase:              r.WarehouseDatabase,
		MetadataStore:                  r.MetadataStore,
		S3:                             r.S3,
		WorkerIdentity:                 r.WorkerIdentity,
		WarehouseDatabaseCredentials:   r.WarehouseDatabaseCredentials,
		MetadataStoreCredentials:       r.MetadataStoreCredentials,
		S3Credentials:                  r.S3Credentials,
		RuntimeConfig:                  r.RuntimeConfig,
		State:                          r.State,
		StatusMessage:                  r.StatusMessage,
		WarehouseDatabaseState:         r.WarehouseDatabaseState,
		WarehouseDatabaseStatusMessage: r.WarehouseDatabaseStatusMessage,
		MetadataStoreState:             r.MetadataStoreState,
		MetadataStoreStatusMessage:     r.MetadataStoreStatusMessage,
		S3State:                        r.S3State,
		S3StatusMessage:                r.S3StatusMessage,
		IdentityState:                  r.IdentityState,
		IdentityStatusMessage:          r.IdentityStatusMessage,
		SecretsState:                   r.SecretsState,
		SecretsStatusMessage:           r.SecretsStatusMessage,
		ReadyAt:                        r.ReadyAt,
		FailedAt:                       r.FailedAt,
	}
}

func decodeStrictWarehouseRequest(c *gin.Context, dst *managedWarehouseRequest) error {
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// --- Orgs ---

func (h *apiHandler) listOrgs(c *gin.Context) {
	orgs, err := h.store.ListOrgs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, orgs)
}

func (h *apiHandler) createOrg(c *gin.Context) {
	var org configstore.Org
	if err := c.ShouldBindJSON(&org); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateOrgMutationPayload(&org); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if org.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if err := h.store.CreateOrg(&org); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, org)
}

func (h *apiHandler) getOrg(c *gin.Context) {
	name := c.Param("name")
	org, err := h.store.GetOrg(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, org)
}

func (h *apiHandler) updateOrg(c *gin.Context) {
	name := c.Param("name")
	var updates configstore.Org
	if err := c.ShouldBindJSON(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateOrgMutationPayload(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	org, ok, err := h.store.UpdateOrg(name, updates)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, org)
}

func (h *apiHandler) deleteOrg(c *gin.Context) {
	name := c.Param("name")
	ok, err := h.store.DeleteOrg(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}

func validateOrgMutationPayload(org *configstore.Org) error {
	if org != nil && org.Warehouse != nil {
		return errWarehousePayloadNotAllowed
	}
	return nil
}

func (h *apiHandler) getManagedWarehouse(c *gin.Context) {
	warehouse, err := h.store.GetManagedWarehouse(c.Param("name"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "managed warehouse not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, warehouse)
}

func (h *apiHandler) putManagedWarehouse(c *gin.Context) {
	orgID := c.Param("name")
	var req managedWarehouseRequest
	if err := decodeStrictWarehouseRequest(c, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	warehouse := req.toManagedWarehouse()
	stored, ok, err := h.store.UpsertManagedWarehouse(orgID, &warehouse)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// --- Users ---

func (h *apiHandler) listUsers(c *gin.Context) {
	users, err := h.store.ListUsers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *apiHandler) createUser(c *gin.Context) {
	// Use a raw struct because OrgUser.Password has json:"-"
	var raw struct {
		Username string `json:"username"`
		Password string `json:"password"`
		OrgID    string `json:"org_id"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if raw.Username == "" || raw.OrgID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username and org_id are required"})
		return
	}
	if raw.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password is required"})
		return
	}
	hash, err := configstore.HashPassword(raw.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	user := configstore.OrgUser{
		Username: raw.Username,
		Password: hash,
		OrgID:    raw.OrgID,
	}
	if err := h.store.CreateUser(&user); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *apiHandler) getUser(c *gin.Context) {
	username := c.Param("username")
	user, err := h.store.GetUser(username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) updateUser(c *gin.Context) {
	username := c.Param("username")
	var raw struct {
		Password string `json:"password"`
		OrgID    string `json:"org_id"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	passwordHash := ""
	if raw.Password != "" {
		hash, err := configstore.HashPassword(raw.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
			return
		}
		passwordHash = hash
	}
	user, ok, err := h.store.UpdateUser(username, passwordHash, raw.OrgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) deleteUser(c *gin.Context) {
	username := c.Param("username")
	ok, err := h.store.DeleteUser(username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": username})
}

// --- Workers ---

func (h *apiHandler) listWorkers(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []WorkerStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllWorkerStatuses())
}

// --- Sessions ---

func (h *apiHandler) listSessions(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, []SessionStatus{})
		return
	}
	c.JSON(http.StatusOK, h.info.AllSessionStatuses())
}

// --- Config Singletons ---

func (h *apiHandler) getGlobalConfig(c *gin.Context) {
	cfg, err := h.store.GetGlobalConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateGlobalConfig(c *gin.Context) {
	var cfg configstore.GlobalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveGlobalConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getDuckLakeConfig(c *gin.Context) {
	cfg, err := h.store.GetDuckLakeConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateDuckLakeConfig(c *gin.Context) {
	var cfg configstore.DuckLakeConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveDuckLakeConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getRateLimitConfig(c *gin.Context) {
	cfg, err := h.store.GetRateLimitConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateRateLimitConfig(c *gin.Context) {
	var cfg configstore.RateLimitConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveRateLimitConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) getQueryLogConfig(c *gin.Context) {
	cfg, err := h.store.GetQueryLogConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

func (h *apiHandler) updateQueryLogConfig(c *gin.Context) {
	var cfg configstore.QueryLogConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	cfg.ID = 1
	if err := h.store.SaveQueryLogConfig(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

// --- Status ---

func (h *apiHandler) getClusterStatus(c *gin.Context) {
	if h.info == nil {
		c.JSON(http.StatusOK, ClusterStatus{})
		return
	}

	orgStats := h.info.AllOrgStats()
	totalWorkers := 0
	totalSessions := 0
	for _, os := range orgStats {
		totalWorkers += os.Workers
		totalSessions += os.ActiveSessions
	}

	c.JSON(http.StatusOK, ClusterStatus{
		TotalOrgs:     len(orgStats),
		TotalWorkers:  totalWorkers,
		TotalSessions: totalSessions,
		Orgs:          orgStats,
	})
}
