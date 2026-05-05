//go:build kubernetes

package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errWarehousePayloadNotAllowed = errors.New("warehouse payload must be updated via /orgs/:id/warehouse")

// maxWarehousePutBodyBytes caps the admin PUT body. Warehouse payloads are
// under 10 KB in practice; 1 MiB leaves room for future fields while keeping
// the handler from loading unbounded input into memory.
const maxWarehousePutBodyBytes = 1 << 20

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
	Protocol string `json:"protocol"`
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
	r.GET("/orgs/:id", h.getOrg)
	r.PUT("/orgs/:id", h.updateOrg)
	r.DELETE("/orgs/:id", h.deleteOrg)
	r.GET("/orgs/:id/warehouse", h.getManagedWarehouse)
	r.PUT("/orgs/:id/warehouse", h.putManagedWarehouse)
	// Focused endpoint for pinning a tenant to a specific worker image and
	// DuckLake spec version — the operator workflow we want to be able to
	// run without ever touching the config-store DB directly.
	r.PATCH("/orgs/:id/warehouse/pinning", h.patchTenantPinning)

	// Users CRUD
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/orgs/:id/users/:username", h.getUser)
	r.PUT("/orgs/:id/users/:username", h.updateUser)
	r.DELETE("/orgs/:id/users/:username", h.deleteUser)

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
	GetUser(orgID, username string) (*configstore.OrgUser, error)
	UpdateUser(orgID, username, passwordHash string, passthrough *bool) (*configstore.OrgUser, bool, error)
	DeleteUser(orgID, username string) (bool, error)

	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error)
	// MutateManagedWarehouse loads the existing warehouse (or a zero value if
	// none), calls mutate to apply changes, and persists the result — all
	// inside a single transaction with a row-level lock on the warehouse row.
	// Closes the read-modify-write race that plain Get+Upsert is exposed to
	// when concurrent PUTs target the same org. Returns (nil, false, nil) if
	// the org doesn't exist.
	MutateManagedWarehouse(orgID string, mutate func(*configstore.ManagedWarehouse) error) (*configstore.ManagedWarehouse, bool, error)

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
	fields := map[string]interface{}{
		"max_workers":    updates.MaxWorkers,
		"memory_budget":  updates.MemoryBudget,
		"idle_timeout_s": updates.IdleTimeoutS,
	}
	// Only update resource fields when explicitly provided to avoid clearing
	// previously-set values when the caller omits them from the JSON payload.
	if updates.WorkerCPURequest != "" {
		fields["worker_cpu_request"] = updates.WorkerCPURequest
	}
	if updates.WorkerMemoryRequest != "" {
		fields["worker_memory_request"] = updates.WorkerMemoryRequest
	}
	result := s.db().Model(&configstore.Org{}).Where("name = ?", name).Updates(fields)
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

func (s *gormAPIStore) GetUser(orgID, username string) (*configstore.OrgUser, error) {
	var user configstore.OrgUser
	if err := s.db().First(&user, "org_id = ? AND username = ?", orgID, username).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *gormAPIStore) UpdateUser(orgID, username, passwordHash string, passthrough *bool) (*configstore.OrgUser, bool, error) {
	updates := map[string]interface{}{}
	if passwordHash != "" {
		updates["password"] = passwordHash
	}
	if passthrough != nil {
		updates["passthrough"] = *passthrough
	}
	if len(updates) == 0 {
		// Nothing to change — return the current row so callers can still
		// distinguish "user not found" from "no-op update".
		user, err := s.GetUser(orgID, username)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		return user, true, nil
	}
	result := s.db().Model(&configstore.OrgUser{}).Where("org_id = ? AND username = ?", orgID, username).Updates(updates)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	user, err := s.GetUser(orgID, username)
	if err != nil {
		return nil, true, err
	}
	return user, true, nil
}

func (s *gormAPIStore) DeleteUser(orgID, username string) (bool, error) {
	result := s.db().Where("org_id = ? AND username = ?", orgID, username).Delete(&configstore.OrgUser{})
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

func (s *gormAPIStore) MutateManagedWarehouse(orgID string, mutate func(*configstore.ManagedWarehouse) error) (*configstore.ManagedWarehouse, bool, error) {
	var (
		stored    *configstore.ManagedWarehouse
		orgExists bool
	)
	err := s.db().Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&configstore.Org{}).Where("name = ?", orgID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		orgExists = true

		// SELECT ... FOR UPDATE: blocks concurrent mutators on the same row
		// until this transaction commits. A missing row is not an error —
		// PUT on a brand-new warehouse lands in the same path.
		var warehouse configstore.ManagedWarehouse
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&warehouse, "org_id = ?", orgID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := mutate(&warehouse); err != nil {
			return err
		}

		warehouse.OrgID = orgID
		warehouse.UpdatedAt = time.Now().UTC()
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "org_id"}},
			DoUpdates: clause.AssignmentColumns(managedWarehouseUpsertColumns()),
		}).Create(&warehouse).Error; err != nil {
			return err
		}

		var reloaded configstore.ManagedWarehouse
		if err := tx.First(&reloaded, "org_id = ?", orgID).Error; err != nil {
			return err
		}
		stored = &reloaded
		return nil
	})
	if err != nil {
		return nil, orgExists, err
	}
	return stored, orgExists, nil
}

func managedWarehouseUpsertColumns() []string {
	return []string{
		"image",
		// GORM's default naming strategy splits CamelCase on every
		// uppercase-after-lowercase boundary, so `DuckLakeVersion` lands as
		// `duck_lake_version` in Postgres — NOT `ducklake_version`. The JSON
		// tag is `ducklake_version` but that only controls API I/O, not the
		// DB column name. Mismatching this against the actual column makes
		// the ON CONFLICT … DO UPDATE clause throw 42703.
		"duck_lake_version",
		"aurora_min_acu",
		"aurora_max_acu",
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
		"pgbouncer_enabled",
		"s3_provider",
		"s3_region",
		"s3_bucket",
		"s3_path_prefix",
		"s3_endpoint",
		"s3_use_ssl",
		"s3_url_style",
		"s3_delta_catalog_enabled",
		"s3_delta_catalog_path",
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

// managedWarehouseRequest is the whitelist of fields a caller may set on the
// PUT endpoint. It's only used for strict decode (DisallowUnknownFields) — the
// actual merge is performed by json.Unmarshal directly onto a ManagedWarehouse
// (see putManagedWarehouse). For that to work, every `json:` tag here must
// match the corresponding `json:` tag on configstore.ManagedWarehouse. If you
// add a field here without a matching tag on ManagedWarehouse, strict decode
// will accept it and the merge will silently drop it.
type managedWarehouseRequest struct {
	Image                          string                                        `json:"image"`
	DuckLakeVersion                string                                        `json:"ducklake_version"`
	WarehouseDatabase              configstore.ManagedWarehouseDatabase          `json:"warehouse_database"`
	MetadataStore                  configstore.ManagedWarehouseMetadataStore     `json:"metadata_store"`
	PgBouncer                      configstore.ManagedWarehousePgBouncer         `json:"pgbouncer"`
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

// decodeStrictWarehouseRequest validates a PUT body by decoding it into
// managedWarehouseRequest with DisallowUnknownFields. This whitelists which
// top-level fields a caller may set; the actual merge is performed separately
// by unmarshaling the same body onto an existing ManagedWarehouse (see
// putManagedWarehouse) so missing keys — at any nesting level — preserve
// whatever the stored row already holds.
func decodeStrictWarehouseRequest(body []byte, dst *managedWarehouseRequest) error {
	dec := json.NewDecoder(bytes.NewReader(body))
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
	name := c.Param("id")
	org, err := h.store.GetOrg(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, org)
}

func (h *apiHandler) updateOrg(c *gin.Context) {
	name := c.Param("id")
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
	name := c.Param("id")
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
	warehouse, err := h.store.GetManagedWarehouse(c.Param("id"))
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

// warehouseBadRequestError marks an error from the mutate closure as caused by
// bad caller input rather than a store-level failure. The handler maps it to
// 400, not 500.
type warehouseBadRequestError struct{ err error }

func (e warehouseBadRequestError) Error() string { return e.err.Error() }
func (e warehouseBadRequestError) Unwrap() error { return e.err }

func (h *apiHandler) putManagedWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxWarehousePutBodyBytes))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Strict decode rejects unknown top-level fields and malformed JSON. We
	// don't use the decoded value directly; it just gates which keys the body
	// is allowed to carry.
	var req managedWarehouseRequest
	if err := decodeStrictWarehouseRequest(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// MutateManagedWarehouse locks the row inside a transaction, runs the
	// closure, and commits — closing the race where two concurrent PUTs would
	// otherwise Get + modify different snapshots and silently clobber each
	// other. The closure does the merge and validation on the locked row.
	stored, ok, err := h.store.MutateManagedWarehouse(orgID, func(w *configstore.ManagedWarehouse) error {
		// json.Unmarshal only overwrites fields whose keys appear in the body
		// — top-level AND nested. Callers can PATCH one inner field (e.g.
		// `{"metadata_store":{"database_name":"x"}}`) without wiping siblings.
		if err := json.Unmarshal(body, w); err != nil {
			return warehouseBadRequestError{err}
		}
		cfgView := &configstore.ManagedWarehouseConfig{
			OrgID:                        orgID,
			WarehouseDatabase:            w.WarehouseDatabase,
			MetadataStore:                w.MetadataStore,
			S3:                           w.S3,
			WorkerIdentity:               w.WorkerIdentity,
			WarehouseDatabaseCredentials: w.WarehouseDatabaseCredentials,
			MetadataStoreCredentials:     w.MetadataStoreCredentials,
			S3Credentials:                w.S3Credentials,
			RuntimeConfig:                w.RuntimeConfig,
		}
		if err := configstore.ValidateManagedWarehouseSecretRefs(orgID, "", cfgView); err != nil {
			return warehouseBadRequestError{err}
		}
		return nil
	})
	if err != nil {
		var badReq warehouseBadRequestError
		if errors.As(err, &badReq) {
			c.JSON(http.StatusBadRequest, gin.H{"error": badReq.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// tenantPinningRequest carries the two columns that determine which worker
// image and DuckLake spec version a tenant pins to. Both fields are
// optional — omitting one preserves the stored value, mirroring the partial
// patch semantics of putManagedWarehouse.
//
// We use *string instead of string so the JSON decoder can distinguish
// "field absent" (preserve) from "field present and empty" (clear). Clearing
// the image falls back to the global default; clearing ducklake_version
// likewise falls back to the global default. We don't allow both to be nil
// after the patch — that's a no-op which is almost certainly a caller bug.
type tenantPinningRequest struct {
	Image           *string `json:"image,omitempty"`
	DuckLakeVersion *string `json:"ducklake_version,omitempty"`
}

const maxPinningPatchBodyBytes = 4 << 10 // 4 KiB; the body is two strings

// patchTenantPinning sets the image and/or ducklake_version columns on a
// tenant's managed_warehouses row without touching the rest of the
// warehouse config. This replaces the operational shape of running
// `UPDATE duckgres_managed_warehouses SET image=..., ducklake_version=...`
// against the config store directly — which is fine for one-offs but
// creates audit + concurrency risks at any scale.
//
// Returns 200 with the updated warehouse on success, 400 for malformed
// requests (unknown fields, both fields absent, invalid version format),
// 404 if the org has no managed warehouse row, and 500 on store errors.
func (h *apiHandler) patchTenantPinning(c *gin.Context) {
	orgID := c.Param("id")

	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxPinningPatchBodyBytes))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req tenantPinningRequest
	if err := dec.Decode(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Image == nil && req.DuckLakeVersion == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one of image or ducklake_version must be set"})
		return
	}
	if req.DuckLakeVersion != nil && *req.DuckLakeVersion != "" {
		if !isValidDuckLakeSpecVersion(*req.DuckLakeVersion) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ducklake_version must be a major.minor string like \"0.4\" or \"1.0\""})
			return
		}
	}

	stored, ok, err := h.store.MutateManagedWarehouse(orgID, func(w *configstore.ManagedWarehouse) error {
		if req.Image != nil {
			w.Image = *req.Image
		}
		if req.DuckLakeVersion != nil {
			w.DuckLakeVersion = *req.DuckLakeVersion
		}
		return nil
	})
	if err != nil {
		var badReq warehouseBadRequestError
		if errors.As(err, &badReq) {
			c.JSON(http.StatusBadRequest, gin.H{"error": badReq.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// isValidDuckLakeSpecVersion accepts the same major.minor format that
// server/ducklake.versionLessThan parses (e.g., "0.4", "1.0", "0.10"). We
// don't import server/ducklake here to avoid a cross-cutting dependency
// from the admin API onto a server subpackage; the check is just a shape
// gate to catch typos before the value gets persisted.
func isValidDuckLakeSpecVersion(v string) bool {
	dot := -1
	for i := 0; i < len(v); i++ {
		if v[i] == '.' {
			if dot >= 0 {
				return false // multiple dots
			}
			dot = i
			continue
		}
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return dot > 0 && dot < len(v)-1
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
		Username    string `json:"username"`
		Password    string `json:"password"`
		OrgID       string `json:"org_id"`
		Passthrough bool   `json:"passthrough"`
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
		Username:    raw.Username,
		Password:    hash,
		OrgID:       raw.OrgID,
		Passthrough: raw.Passthrough,
	}
	if err := h.store.CreateUser(&user); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *apiHandler) getUser(c *gin.Context) {
	orgID := c.Param("id")
	username := c.Param("username")
	user, err := h.store.GetUser(orgID, username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) updateUser(c *gin.Context) {
	orgID := c.Param("id")
	username := c.Param("username")
	// Passthrough is *bool so omitting it preserves the stored value; sending
	// `false` explicitly clears the flag.
	var raw struct {
		Password    string `json:"password"`
		Passthrough *bool  `json:"passthrough,omitempty"`
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
	user, ok, err := h.store.UpdateUser(orgID, username, passwordHash, raw.Passthrough)
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
	orgID := c.Param("id")
	username := c.Param("username")
	ok, err := h.store.DeleteUser(orgID, username)
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
