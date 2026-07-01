//go:build kubernetes

package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"k8s.io/apimachinery/pkg/api/resource"
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
	CPU            string `json:"cpu"`
	Memory         string `json:"memory"`
	TTLSeconds     int    `json:"ttl_seconds"`
}

// SessionStatus represents an active session for the API.
type SessionStatus struct {
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Org      string `json:"org"`
	User     string `json:"user"`
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
// fetcher (may be nil) aggregates per-CP live state (sessions/workers) across
// replicas so the dashboard shows cluster-wide numbers instead of one CP's slice.
func RegisterAPI(r *gin.RouterGroup, store *configstore.ConfigStore, info OrgStackInfo, fetcher PeerFetcher) {
	registerAPIWithStore(r, newGormAPIStore(store), info, fetcher)
	// Generic read-only models explorer (sidebar + table + detail UI). Reads
	// the concrete store directly because it needs the runtime schema name and
	// raw DB for tables the typed apiStore interface doesn't surface.
	registerModelsAPI(r, store)
	// Admin-only Operators management (the admin-console access list). Each
	// route self-gates with RequireAdmin; mutations are audited via the group.
	registerOperatorsAPI(r, store)
}

func registerAPIWithStore(r *gin.RouterGroup, store apiStore, info OrgStackInfo, fetcher PeerFetcher) {
	h := &apiHandler{store: store, info: info, fetcher: fetcher}

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
	UpdateUser(orgID, username, passwordHash string, passthrough *bool, defaultCatalog *string, maxVCPUs *int) (*configstore.OrgUser, bool, error)
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
		"max_workers": updates.MaxWorkers,
		"max_vcpus":   updates.MaxVCPUs,
		// Org default worker profile: written unconditionally so an explicit
		// empty string CLEARS the default (the handler's presence-merge keeps
		// omitted fields at their stored values before this runs).
		"default_worker_cpu":          updates.DefaultWorkerCPU,
		"default_worker_memory":       updates.DefaultWorkerMemory,
		"default_worker_ttl":          updates.DefaultWorkerTTL,
		"default_worker_min_hot_idle": updates.DefaultWorkerMinHotIdle,
	}
	// HostnameAlias is *string: nil = preserve, "" = clear (NULL), "x" = set.
	// NULL releases the unique-index slot so other orgs can take that alias.
	if updates.HostnameAlias != nil {
		if *updates.HostnameAlias == "" {
			fields["hostname_alias"] = nil
		} else {
			fields["hostname_alias"] = *updates.HostnameAlias
		}
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

func (s *gormAPIStore) UpdateUser(orgID, username, passwordHash string, passthrough *bool, defaultCatalog *string, maxVCPUs *int) (*configstore.OrgUser, bool, error) {
	updates := map[string]interface{}{}
	if passwordHash != "" {
		updates["password"] = passwordHash
	}
	if passthrough != nil {
		updates["passthrough"] = *passthrough
	}
	if defaultCatalog != nil {
		updates["default_catalog"] = *defaultCatalog
	}
	if maxVCPUs != nil {
		updates["max_vcpus"] = *maxVCPUs
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
		"warehouse_database_endpoint",
		"warehouse_database_port",
		"metadata_store_kind",
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
		"iceberg_enabled",
		"iceberg_region",
		"iceberg_namespace",
		"worker_identity_namespace",
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
		"metadata_store_state",
		"s3_state",
		"iceberg_state",
		"identity_state",
		"secrets_state",
		"ready_at",
		"failed_at",
		"updated_at",
	}
}

type apiHandler struct {
	store   apiStore
	info    OrgStackInfo
	fetcher PeerFetcher // nil = no cross-CP aggregation (single-CP or tests)
}

// managedWarehouseRequest is the whitelist of fields a caller may set on the
// PUT endpoint. It's only used for strict decode (DisallowUnknownFields) — the
// actual merge is performed by json.Unmarshal directly onto a ManagedWarehouse
// (see putManagedWarehouse). For that to work, every `json:` tag here must
// match the corresponding `json:` tag on configstore.ManagedWarehouse. If you
// add a field here without a matching tag on ManagedWarehouse, strict decode
// will accept it and the merge will silently drop it.
type managedWarehouseRequest struct {
	Image                        string                                        `json:"image"`
	DuckLakeVersion              string                                        `json:"ducklake_version"`
	DucklingName                 string                                        `json:"duckling_name"`
	WarehouseDatabase            configstore.ManagedWarehouseDatabase          `json:"warehouse_database"`
	MetadataStore                configstore.ManagedWarehouseMetadataStore     `json:"metadata_store"`
	PgBouncer                    configstore.ManagedWarehousePgBouncer         `json:"pgbouncer"`
	S3                           configstore.ManagedWarehouseS3                `json:"s3"`
	Iceberg                      configstore.ManagedWarehouseIceberg           `json:"iceberg"`
	WorkerIdentity               configstore.ManagedWarehouseWorkerIdentity    `json:"worker_identity"`
	WarehouseDatabaseCredentials configstore.SecretRef                         `json:"warehouse_database_credentials"`
	MetadataStoreCredentials     configstore.SecretRef                         `json:"metadata_store_credentials"`
	S3Credentials                configstore.SecretRef                         `json:"s3_credentials"`
	RuntimeConfig                configstore.SecretRef                         `json:"runtime_config"`
	State                        configstore.ManagedWarehouseProvisioningState `json:"state"`
	StatusMessage                string                                        `json:"status_message"`
	MetadataStoreState           configstore.ManagedWarehouseProvisioningState `json:"metadata_store_state"`
	S3State                      configstore.ManagedWarehouseProvisioningState `json:"s3_state"`
	IcebergState                 configstore.ManagedWarehouseProvisioningState `json:"iceberg_state"`
	IdentityState                configstore.ManagedWarehouseProvisioningState `json:"identity_state"`
	SecretsState                 configstore.ManagedWarehouseProvisioningState `json:"secrets_state"`
	ReadyAt                      *time.Time                                    `json:"ready_at"`
	FailedAt                     *time.Time                                    `json:"failed_at"`
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
	// Normalize empty hostname_alias to NULL on insert so the unique index
	// doesn't reject a second org with an explicit empty string. Centralizing
	// the rule at the handler layer keeps any future store impl from having
	// to repeat it.
	if org.HostnameAlias != nil && *org.HostnameAlias == "" {
		org.HostnameAlias = nil
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
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var updates configstore.Org
	if err := json.Unmarshal(body, &updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateOrgMutationPayload(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	existing, err := h.store.GetOrg(name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	merged := *existing
	if _, ok := fields["max_workers"]; ok {
		merged.MaxWorkers = updates.MaxWorkers
	}
	if _, ok := fields["max_vcpus"]; ok {
		merged.MaxVCPUs = updates.MaxVCPUs
	}
	// Org default worker profile: present-in-payload wins, including an
	// explicit "" which clears the default.
	if _, ok := fields["default_worker_cpu"]; ok {
		merged.DefaultWorkerCPU = updates.DefaultWorkerCPU
	}
	if _, ok := fields["default_worker_memory"]; ok {
		merged.DefaultWorkerMemory = updates.DefaultWorkerMemory
	}
	if _, ok := fields["default_worker_ttl"]; ok {
		merged.DefaultWorkerTTL = updates.DefaultWorkerTTL
	}
	if _, ok := fields["default_worker_min_hot_idle"]; ok {
		merged.DefaultWorkerMinHotIdle = updates.DefaultWorkerMinHotIdle
	}
	if _, ok := fields["hostname_alias"]; ok {
		merged.HostnameAlias = updates.HostnameAlias
	}
	org, ok, err := h.store.UpdateOrg(name, merged)
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
	if org == nil {
		return nil
	}
	if org.Warehouse != nil {
		return errWarehousePayloadNotAllowed
	}
	if org.HostnameAlias != nil {
		if err := validateHostnameAlias(*org.HostnameAlias); err != nil {
			return err
		}
	}
	if err := validateOrgDefaultWorkerProfile(org); err != nil {
		return err
	}
	if org.DefaultWorkerMinHotIdle < 0 {
		return fmt.Errorf("default_worker_min_hot_idle: value %d must be >= 0", org.DefaultWorkerMinHotIdle)
	}
	if org.MaxVCPUs < 0 {
		return fmt.Errorf("max_vcpus: value %d must be >= 0", org.MaxVCPUs)
	}
	return nil
}

// validateOrgDefaultWorkerProfile rejects garbage default-worker-profile
// values at the API boundary so they can never enter the config store (the
// control plane tolerates bad rows by ignoring them, but a 400 here surfaces
// the typo to the operator instead of a silently-ignored default). Empty
// strings are allowed: they mean "unset" on create / "clear" on update.
func validateOrgDefaultWorkerProfile(org *configstore.Org) error {
	for _, f := range []struct{ name, raw string }{
		{"default_worker_cpu", org.DefaultWorkerCPU},
		{"default_worker_memory", org.DefaultWorkerMemory},
	} {
		if f.raw == "" {
			continue
		}
		q, err := resource.ParseQuantity(f.raw)
		if err != nil {
			return fmt.Errorf("%s: invalid quantity %q", f.name, f.raw)
		}
		if q.Sign() <= 0 {
			return fmt.Errorf("%s: quantity %q must be positive", f.name, f.raw)
		}
	}
	if raw := org.DefaultWorkerTTL; raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("default_worker_ttl: invalid duration %q (use a Go duration like \"30m\")", raw)
		}
		if d < 0 {
			return fmt.Errorf("default_worker_ttl: duration %q must be >= 0", raw)
		}
	}
	return nil
}

// validateHostnameAlias enforces that an alias is a valid single DNS label
// (RFC 1035): alphanumeric + hyphens, no leading/trailing hyphen, 1–63
// characters. The empty string is allowed (means "clear" on update / "no
// alias" on create — see handler-level normalization). Aliases that violate
// this would silently fail SNI matching (`sni_kubernetes.go:23` rejects
// multi-label prefixes), so the validation lives at admission time to surface
// the typo as a 400 instead of a mysteriously unreachable tenant.
func validateHostnameAlias(alias string) error {
	if alias == "" {
		return nil
	}
	if len(alias) > 63 {
		return errors.New("hostname_alias must be at most 63 characters (DNS label limit)")
	}
	for i, r := range alias {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		isHyphen := r == '-'
		if !isAlnum && !isHyphen {
			return fmt.Errorf("hostname_alias contains invalid character %q (allowed: A-Z, a-z, 0-9, hyphen)", r)
		}
		if isHyphen && (i == 0 || i == len(alias)-1) {
			return errors.New("hostname_alias must not start or end with a hyphen")
		}
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
		if w.DucklingName == "" {
			return warehouseBadRequestError{errors.New("duckling_name cannot be empty")}
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
		Username       string `json:"username"`
		Password       string `json:"password"`
		OrgID          string `json:"org_id"`
		Passthrough    bool   `json:"passthrough"`
		DefaultCatalog string `json:"default_catalog"`
		MaxVCPUs       int    `json:"max_vcpus"`
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
	if catalog, err := validateDefaultCatalog(raw.DefaultCatalog); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	} else {
		raw.DefaultCatalog = catalog
	}
	if raw.MaxVCPUs < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "max_vcpus must be >= 0"})
		return
	}
	hash, err := configstore.HashPassword(raw.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	user := configstore.OrgUser{
		Username:       raw.Username,
		Password:       hash,
		OrgID:          raw.OrgID,
		Passthrough:    raw.Passthrough,
		DefaultCatalog: raw.DefaultCatalog,
		MaxVCPUs:       raw.MaxVCPUs,
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
		Password       string  `json:"password"`
		Passthrough    *bool   `json:"passthrough,omitempty"`
		DefaultCatalog *string `json:"default_catalog,omitempty"`
		MaxVCPUs       *int    `json:"max_vcpus,omitempty"`
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	maxVCPUs := raw.MaxVCPUs
	if rawMaxVCPUs, ok := fields["max_vcpus"]; ok && bytes.Equal(bytes.TrimSpace(rawMaxVCPUs), []byte("null")) {
		zero := 0
		maxVCPUs = &zero
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
	if raw.DefaultCatalog != nil {
		catalog, err := validateDefaultCatalog(*raw.DefaultCatalog)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		raw.DefaultCatalog = &catalog
	}
	if maxVCPUs != nil && *maxVCPUs < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "max_vcpus must be >= 0"})
		return
	}
	user, ok, err := h.store.UpdateUser(orgID, username, passwordHash, raw.Passthrough, raw.DefaultCatalog, maxVCPUs)
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

func validateDefaultCatalog(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw == "iceberg" {
		return raw, nil
	}
	return "", errors.New("default_catalog must be empty or iceberg")
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
	workers := []WorkerStatus{}
	if h.info != nil {
		workers = h.info.AllWorkerStatuses()
	}
	// A worker is owned by exactly one CP (disjoint union); dedup makes it idempotent.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/workers")
		mergePeer(&workers, bodies, func(e []WorkerStatus) []WorkerStatus { return e })
		workers = dedupeBy(workers, func(w WorkerStatus) int { return w.ID })
	}
	c.JSON(http.StatusOK, workers)
}

// --- Sessions ---

func (h *apiHandler) listSessions(c *gin.Context) {
	sessions := []SessionStatus{}
	if h.info != nil {
		sessions = h.info.AllSessionStatuses()
	}
	// A session lives on exactly one CP (disjoint union); dedup makes it idempotent.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/sessions")
		mergePeer(&sessions, bodies, func(e []SessionStatus) []SessionStatus { return e })
		sessions = dedupeBy(sessions, func(s SessionStatus) int { return s.WorkerID })
	}
	c.JSON(http.StatusOK, sessions)
}

// --- Status ---

func (h *apiHandler) getClusterStatus(c *gin.Context) {
	orgStats := []OrgStatus{}
	if h.info != nil {
		orgStats = h.info.AllOrgStats()
	}
	// Per-org active-session counts are per-CP; merge every CP's slice so the
	// Overview cards reflect the whole cluster instead of one replica's view.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/status")
		orgStats = mergeOrgStats(orgStats, bodies)
	}

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
