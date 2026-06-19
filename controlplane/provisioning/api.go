package provisioning

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
	"gorm.io/gorm"
)

// isUniqueViolation reports whether err comes from a Postgres
// 23505 unique-constraint violation. The pgx/jackc driver surfaces
// the SQLSTATE through a method on the returned error; we match
// against that without importing pgconn directly (mirrors the
// pattern in controlplane/provisioner/postgres_admin.go).
//
// Mapped to HTTP 409 by the provision handler so callers see a
// clear "your input conflicts with existing state" rather than a
// generic 500.
func isUniqueViolation(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "23505"
}

// ducklingOrgIDPattern constrains org IDs that get a provisioned warehouse to a
// single DNS-1123 label (lowercase alphanumerics + hyphens, start/end
// alphanumeric). This is the shape every derived name needs:
//   - the SNI prefix <org>.<managed-suffix> is a single DNS label already;
//   - the Duckling CR / IAM role / S3 bucket / Lakekeeper CR names use the org
//     ID verbatim (lowercased), so it must be a valid k8s/AWS name;
//   - the Postgres identifier maps any non-[a-z0-9_] char to '_', which is only
//     injective when the source charset excludes everything but hyphens.
//
// Validating here keeps the org ID → resource-name mappings collision-free.
var ducklingOrgIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// Store defines the config store operations needed by the provisioning API.
type Store interface {
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	GetOrg(orgID string) (*configstore.Org, error)
	// Provision is the all-or-nothing entrypoint for POST /provision —
	// wraps warehouse + root-user writes in a single configstore
	// transaction so partial failure rolls back cleanly. Use this for the
	// public provision endpoint; the older per-step methods below are kept
	// for the standalone surfaces (reset-password).
	Provision(req ProvisionRequest) error
	CreatePendingWarehouse(orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error
	CreateOrgUser(orgID, username, passwordHash string) error
	UpdateOrgUserPassword(orgID, username, passwordHash string) error
	SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error
	IsDatabaseNameAvailable(name string) (bool, error)
}

// RegisterAPI registers provisioning endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store Store) {
	h := &handler{store: store}
	r.POST("/orgs/:id/provision", h.provisionWarehouse)
	r.POST("/orgs/:id/deprovision", h.deprovisionWarehouse)
	r.GET("/orgs/:id/warehouse/status", h.getWarehouseStatus)
	r.POST("/orgs/:id/reset-password", h.resetPassword)
	r.GET("/database-name/check", h.checkDatabaseName)
}

type handler struct {
	store Store
}

// warehouseStatusResponse is the public-facing view of warehouse state.
// Exposes lifecycle status and, when ready, connection details (without password).
type warehouseStatusResponse struct {
	OrgID              string                                        `json:"org_id"`
	State              configstore.ManagedWarehouseProvisioningState `json:"state"`
	StatusMessage      string                                        `json:"status_message"`
	S3State            configstore.ManagedWarehouseProvisioningState `json:"s3_state"`
	MetadataStoreState configstore.ManagedWarehouseProvisioningState `json:"metadata_store_state"`
	IdentityState      configstore.ManagedWarehouseProvisioningState `json:"identity_state"`
	SecretsState       configstore.ManagedWarehouseProvisioningState `json:"secrets_state"`
	ReadyAt            *time.Time                                    `json:"ready_at,omitempty"`
	FailedAt           *time.Time                                    `json:"failed_at,omitempty"`
	Connection         *connectionDetails                            `json:"connection,omitempty"`
}

// connectionDetails is returned in status (without password) and in provision/reset-password (with password).
type connectionDetails struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

type provisionRequest struct {
	DatabaseName  string                 `json:"database_name"`
	MetadataStore *provisionMetadataReq  `json:"metadata_store,omitempty"`
	DataStore     *provisionDataStoreReq `json:"data_store,omitempty"`
	DuckLake      *provisionDuckLakeReq  `json:"ducklake,omitempty"`
	Iceberg       *provisionIcebergReq   `json:"iceberg,omitempty"`
}

type provisionMetadataReq struct {
	Type string `json:"type"`
	// External is required when Type == "external": a pre-existing Postgres
	// referenced by host + an AWS Secrets Manager secret for the password.
	External *provisionExternalReq `json:"external,omitempty"`
}

// provisionDuckLakeReq toggles the DuckLake catalog. Independent of Iceberg and
// of the metadata-store type: enable DuckLake, Iceberg, or both. At least one
// catalog must be enabled.
type provisionDuckLakeReq struct {
	Enabled bool `json:"enabled"`
}

// provisionExternalReq describes a pre-existing (external) Postgres metadata
// store. Endpoint (RDS host) and PasswordAWSSecret (the AWS Secrets Manager
// secret NAME holding the password) are required; User/Database default to
// "postgres" when omitted.
type provisionExternalReq struct {
	Endpoint          string `json:"endpoint"`
	PasswordAWSSecret string `json:"password_aws_secret"`
	User              string `json:"user,omitempty"`
	Database          string `json:"database,omitempty"`
}

// provisionDataStoreReq selects the object store. Type "s3bucket" (or omitted)
// provisions a fresh per-org bucket; "external" reuses an existing bucket and
// then requires BucketName. Region is optional (composition default applies).
type provisionDataStoreReq struct {
	Type       string `json:"type"`
	BucketName string `json:"bucket_name,omitempty"`
	Region     string `json:"region,omitempty"`
}

// provisionIcebergReq toggles the per-tenant Lakekeeper Iceberg catalog. For
// external metadata stores it's optional (enabled → iceberg+external, omitted
// → ducklake+external); for cnpg-shard it's implied and always enabled.
type provisionIcebergReq struct {
	Enabled   bool   `json:"enabled"`
	Namespace string `json:"namespace,omitempty"`
}

// icebergNamespace returns the requested Iceberg namespace, or "" to let the
// XRD default ("main") apply.
func icebergNamespace(req *provisionIcebergReq) string {
	if req == nil {
		return ""
	}
	return req.Namespace
}

// resolveDataStore validates and normalizes the data-store request into the
// stored intent. Nil or "s3bucket" provisions a fresh per-org bucket;
// "external" reuses an existing bucket and requires a bucket name.
func resolveDataStore(req *provisionDataStoreReq) (configstore.ManagedWarehouseDataStore, error) {
	if req == nil || req.Type == "" || req.Type == "s3bucket" {
		return configstore.ManagedWarehouseDataStore{Kind: "s3bucket"}, nil
	}
	if req.Type == "external" {
		if req.BucketName == "" {
			return configstore.ManagedWarehouseDataStore{}, errors.New("data_store.type 'external' requires data_store.bucket_name")
		}
		return configstore.ManagedWarehouseDataStore{
			Kind:       "external",
			BucketName: req.BucketName,
			Region:     req.Region,
		}, nil
	}
	return configstore.ManagedWarehouseDataStore{}, fmt.Errorf("data_store.type must be \"s3bucket\" or \"external\" (got %q)", req.Type)
}

func (h *handler) provisionWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	var req provisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !ducklingOrgIDPattern.MatchString(orgID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "org id must be a DNS-1123 label (lowercase alphanumerics and hyphens, starting and ending alphanumeric) so the derived resource names are valid and collision-free"})
		return
	}

	if req.DatabaseName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "database_name is required"})
		return
	}

	if req.MetadataStore == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "metadata_store is required"})
		return
	}

	// Catalogs are decoupled from the metadata backend: a duckling can run
	// DuckLake, Iceberg, or both, on any of the three metadata stores. At least
	// one catalog must be enabled (a warehouse with neither has nothing to
	// attach).
	ducklakeEnabled := req.DuckLake != nil && req.DuckLake.Enabled
	icebergEnabled := req.Iceberg != nil && req.Iceberg.Enabled
	if !ducklakeEnabled && !icebergEnabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one of ducklake.enabled or iceberg.enabled must be true"})
		return
	}

	ds, derr := resolveDataStore(req.DataStore)
	if derr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": derr.Error()})
		return
	}

	warehouse := &configstore.ManagedWarehouse{
		DataStore: ds,
		DuckLake:  configstore.ManagedWarehouseDuckLake{Enabled: ducklakeEnabled},
	}
	if icebergEnabled {
		warehouse.Iceberg = configstore.ManagedWarehouseIceberg{
			Enabled:   true,
			Backend:   configstore.IcebergBackendLakekeeper,
			Namespace: icebergNamespace(req.Iceberg),
		}
	}

	// Metadata backend (the Postgres that hosts the DuckLake catalog and/or the
	// Lakekeeper PG). Provisioning shape differs per type; the catalog choice
	// above is orthogonal.
	switch req.MetadataStore.Type {
	case configstore.MetadataStoreKindCnpgShard:
		// No per-claim config — the composition picks the active shard from
		// chart values and provisions the per-tenant role+database there.
		warehouse.MetadataStore.Kind = configstore.MetadataStoreKindCnpgShard

	case configstore.MetadataStoreKindExternal:
		// A pre-existing Postgres. Endpoint (RDS host) + the AWS Secrets Manager
		// secret name for the password are required; user/database default to
		// "postgres" at the XRD.
		ext := req.MetadataStore.External
		if ext == nil || ext.Endpoint == "" || ext.PasswordAWSSecret == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "metadata_store.type 'external' requires metadata_store.external.endpoint and metadata_store.external.password_aws_secret"})
			return
		}
		warehouse.MetadataStore = configstore.ManagedWarehouseMetadataStore{
			Kind:              configstore.MetadataStoreKindExternal,
			Endpoint:          ext.Endpoint,
			Username:          ext.User,
			DatabaseName:      ext.Database,
			PasswordAWSSecret: ext.PasswordAWSSecret,
		}

	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("metadata_store.type must be %q or %q (got %q)", configstore.MetadataStoreKindCnpgShard, configstore.MetadataStoreKindExternal, req.MetadataStore.Type)})
		return
	}

	// Generate the root password. The plaintext is returned in this
	// response only — it is never stored, only the bcrypt hash is
	// persisted via the transactional Provision below.
	plainPassword, err := configstore.GeneratePassword()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate password"})
		return
	}
	hash, err := configstore.HashPassword(plainPassword)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}

	// One transaction wraps warehouse + root user. Failure of any
	// sub-step rolls the others back so the caller's retry sees the same
	// starting state (no half-provisioned row blocking re-creation).
	if err := h.store.Provision(ProvisionRequest{
		OrgID:        orgID,
		DatabaseName: req.DatabaseName,
		Warehouse:    warehouse,
		RootUserHash: hash,
	}); err != nil {
		// The warehouse-already-exists conflict is the only error
		// shape that maps to 409. Everything else (DB write failure,
		// OnConflict surprise) is internal. The sentinel here
		// replaces an earlier `strings.Contains` match, so that
		// rewording the error message can't silently break the 409
		// branch.
		if errors.Is(err, ErrWarehouseNonTerminal) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		if isUniqueViolation(err) {
			// Most likely: database_name already in use by another
			// org. Map to 409 with a clear message; the underlying
			// error still includes the constraint name for ops.
			c.JSON(http.StatusConflict, gin.H{"error": "provision conflicts with existing state (likely database_name in use): " + err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "provision failed: " + err.Error()})
		return
	}

	analytics.Default().Capture("warehouse_provisioned", orgID, map[string]any{
		"database_name":    req.DatabaseName,
		"metadata_store":   string(req.MetadataStore.Type),
		"ducklake_enabled": ducklakeEnabled,
		"iceberg_enabled":  icebergEnabled,
	})

	c.JSON(http.StatusAccepted, gin.H{
		"status":   "provisioning started",
		"org":      orgID,
		"username": "root",
		"password": plainPassword,
	})
}

func (h *handler) deprovisionWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	// Try CAS from each deprovisionable state. Order doesn't matter —
	// only one will match. This avoids a read-then-write TOCTOU race.
	deprovisionableStates := []configstore.ManagedWarehouseProvisioningState{
		configstore.ManagedWarehouseStateReady,
		configstore.ManagedWarehouseStateFailed,
		configstore.ManagedWarehouseStateProvisioning,
	}

	var err error
	for _, state := range deprovisionableStates {
		if err = h.store.SetWarehouseDeleting(orgID, state); err == nil {
			analytics.Default().Capture("warehouse_deprovisioned", orgID, nil)
			c.JSON(http.StatusAccepted, gin.H{"status": "deprovisioning started", "org": orgID})
			return
		}
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "warehouse not found"})
		return
	}
	c.JSON(http.StatusConflict, gin.H{"error": "warehouse must be in ready, failed, or provisioning state to deprovision"})
}

func (h *handler) getWarehouseStatus(c *gin.Context) {
	orgID := c.Param("id")

	warehouse, err := h.store.GetManagedWarehouse(orgID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "warehouse not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	resp := warehouseStatusResponse{
		OrgID:              warehouse.OrgID,
		State:              warehouse.State,
		StatusMessage:      warehouse.StatusMessage,
		S3State:            warehouse.S3State,
		MetadataStoreState: warehouse.MetadataStoreState,
		IdentityState:      warehouse.IdentityState,
		SecretsState:       warehouse.SecretsState,
		ReadyAt:            warehouse.ReadyAt,
		FailedAt:           warehouse.FailedAt,
	}

	if warehouse.State == configstore.ManagedWarehouseStateReady {
		org, err := h.store.GetOrg(orgID)
		if err == nil {
			resp.Connection = &connectionDetails{
				Host:     warehouse.WarehouseDatabase.Endpoint,
				Port:     warehouse.WarehouseDatabase.Port,
				Database: org.DatabaseName,
				Username: "root",
			}
		}
	}

	c.JSON(http.StatusOK, resp)
}

func (h *handler) resetPassword(c *gin.Context) {
	orgID := c.Param("id")

	warehouse, err := h.store.GetManagedWarehouse(orgID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "warehouse not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if warehouse.State != configstore.ManagedWarehouseStateReady {
		c.JSON(http.StatusConflict, gin.H{"error": "warehouse must be in ready state to reset password"})
		return
	}

	plainPassword, err := configstore.GeneratePassword()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate password"})
		return
	}
	hash, err := configstore.HashPassword(plainPassword)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	if err := h.store.UpdateOrgUserPassword(orgID, "root", hash); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "root user not found"})
		return
	}

	analytics.Default().Capture("warehouse_password_reset", orgID, map[string]any{
		"username": "root",
	})

	c.JSON(http.StatusOK, gin.H{
		"username": "root",
		"password": plainPassword,
	})
}

func (h *handler) checkDatabaseName(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name query parameter is required"})
		return
	}

	available, err := h.store.IsDatabaseNameAvailable(name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check database name"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"name": name, "available": available})
}
