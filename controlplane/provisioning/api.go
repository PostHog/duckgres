package provisioning

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
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

const (
	// maxDucklingSlugOrgIDLength is the public Duckgres provisioning contract
	// for non-UUID org IDs. It is intentionally stricter than most individual
	// downstream limits: with the current managed-warehouse suffix "mw-prod-us",
	// the S3 bucket name is:
	//
	//   posthog-duckling-<slug>-mw-prod-us
	//
	// S3 caps bucket names at 63 chars, leaving 35 chars for <slug>. That cap
	// also leaves enough room for the other request-driven names derived from
	// org ID today (Duckling k8s names, IAM roles, PgBouncer names,
	// and Postgres identifiers). Canonical UUID org IDs are allowed separately
	// because configstore.DucklingBucketName compacts them from 36 to 32 chars
	// before building the S3 bucket name. If the managed bucket suffix grows
	// beyond "mw-prod-us", lower this cap or validate the suffix at startup.
	maxDucklingSlugOrgIDLength = 35
)

// ducklingOrgIDPattern constrains provisionable org IDs to a single DNS-1123
// label (lowercase alphanumerics + hyphens, start/end alphanumeric). This keeps
// SNI labels valid and makes the Postgres hyphen-to-underscore mapping
// collision-free for names Duckgres derives from org ID.
var ducklingOrgIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// canonicalDucklingUUIDPattern matches the UUID-shaped org IDs PostHog sends.
// These are longer than maxDucklingSlugOrgIDLength but safe because Duckgres
// compacts UUID hyphens only for S3 bucket naming.
var canonicalDucklingUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func validateDucklingOrgID(orgID string) error {
	if !ducklingOrgIDPattern.MatchString(orgID) {
		return errors.New("org id must be a DNS-1123 label (lowercase alphanumerics and hyphens, starting and ending alphanumeric) so the derived resource names are valid and collision-free")
	}
	if !canonicalDucklingUUIDPattern.MatchString(orgID) && len(orgID) > maxDucklingSlugOrgIDLength {
		return fmt.Errorf("org id must be a canonical UUID or a slug of at most %d characters", maxDucklingSlugOrgIDLength)
	}
	return nil
}

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
// bucketSuffix is the env suffix used to compute the control-plane-owned
// per-org s3bucket name at provision time (empty ⇒ the CP doesn't name buckets
// and the composition derives).
func RegisterAPI(r *gin.RouterGroup, store Store, bucketSuffix string) {
	h := &handler{store: store, bucketSuffix: bucketSuffix}
	r.POST("/orgs/:id/provision", h.provisionWarehouse)
	r.POST("/orgs/:id/deprovision", h.deprovisionWarehouse)
	r.GET("/orgs/:id/warehouse/status", h.getWarehouseStatus)
	r.POST("/orgs/:id/reset-password", h.resetPassword)
	r.GET("/database-name/check", h.checkDatabaseName)
}

type handler struct {
	store Store
	// bucketSuffix is the env suffix (e.g. "mw-prod-us") used to compute the
	// CP-owned s3bucket name; empty disables CP naming. See
	// configstore.DucklingBucketName.
	bucketSuffix string
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
	// Bucket is the authoritative per-org S3 bucket name the CP provisioned.
	// Empty for external data stores or ducklings provisioned before CP-owned
	// naming whose row hasn't been backfilled yet.
	Bucket string `json:"bucket,omitempty"`
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
	DatabaseName string `json:"database_name"`
	// DefaultTeamID links the org to its default PostHog team id. REQUIRED
	// when the provision creates a NEW org (400 otherwise — every org carries
	// its team id from birth; pull-based compute billing keys usage buckets by
	// it). Optional on re-provision of an existing org: absent/empty keeps the
	// stored value, never wipes it.
	DefaultTeamID string                 `json:"default_team_id,omitempty"`
	MetadataStore *provisionMetadataReq  `json:"metadata_store,omitempty"`
	DataStore     *provisionDataStoreReq `json:"data_store,omitempty"`
	DuckLake      *provisionDuckLakeReq  `json:"ducklake,omitempty"`
}

type provisionMetadataReq struct {
	Type string `json:"type"`
	// External is required when Type == "external": a pre-existing Postgres
	// referenced by host + an AWS Secrets Manager secret for the password.
	External *provisionExternalReq `json:"external,omitempty"`
}

// provisionDuckLakeReq toggles the DuckLake catalog. Independent of the
// metadata-store type; it must be enabled (a warehouse without a catalog has
// nothing to attach).
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

	if err := validateDucklingOrgID(orgID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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

	// The catalog is decoupled from the metadata backend: a duckling runs
	// DuckLake on any of the metadata stores. It must be enabled (a warehouse
	// without a catalog has nothing to attach).
	ducklakeEnabled := req.DuckLake != nil && req.DuckLake.Enabled
	if !ducklakeEnabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ducklake.enabled must be true"})
		return
	}

	ds, derr := resolveDataStore(req.DataStore)
	if derr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": derr.Error()})
		return
	}

	// Control-plane-owned bucket naming: for a fresh per-org bucket (s3bucket
	// with no caller-supplied name) compute the name here, once, and pin it on
	// the warehouse. It flows to the Duckling CR's spec.dataStore.bucketName
	// (so the composition provisions exactly this bucket instead of deriving
	// one) and back to the caller in the response below — so nothing downstream
	// re-derives the name. No-op when bucketSuffix is unset (the composition
	// derives, legacy behavior) or when the caller passed an explicit name.
	if ds.Kind == "s3bucket" && ds.BucketName == "" {
		ds.BucketName = configstore.DucklingBucketName(orgID, h.bucketSuffix)
	}

	warehouse := &configstore.ManagedWarehouse{
		DataStore: ds,
		DuckLake:  configstore.ManagedWarehouseDuckLake{Enabled: ducklakeEnabled},
		// Stamp the canonical Duckling CR name now so lookups never have to
		// re-derive it. lower(orgID) mirrors provisioner.ducklingName (org IDs
		// are validated DNS-1123 labels, so lowercasing is the whole transform);
		// we inline it rather than import provisioner into this package.
		DucklingName: strings.ToLower(orgID),
	}
	if warehouse.DucklingName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "duckling_name is required"})
		return
	}
	// Metadata backend (the Postgres that hosts the DuckLake catalog).
	// Provisioning shape differs per type; the catalog choice above is
	// orthogonal.
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
		OrgID:         orgID,
		DatabaseName:  req.DatabaseName,
		DefaultTeamID: req.DefaultTeamID,
		Warehouse:     warehouse,
		RootUserHash:  hash,
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
		// Creating a NEW org requires default_team_id — a caller input
		// problem, not a server failure. Decided in the store (only it knows
		// whether the org exists), surfaced here as 400.
		if errors.Is(err, ErrDefaultTeamIDRequired) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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

	// The handler only kicks off provisioning (the response is 202 Accepted);
	// the warehouse is not usable yet. The terminal outcome —
	// warehouse_provision_success / warehouse_provision_failed — is emitted by
	// the async provisioner controller when the warehouse reaches Ready / Failed.
	analytics.Default().Capture("warehouse_provision_begin", orgID, map[string]any{
		"database_name":    req.DatabaseName,
		"metadata_store":   string(req.MetadataStore.Type),
		"ducklake_enabled": ducklakeEnabled,
	})

	resp := gin.H{
		"status":   "provisioning started",
		"org":      orgID,
		"username": "root",
		"password": plainPassword,
	}
	// Return the authoritative bucket name synchronously with the password, so
	// callers persist the name the CP provisioned instead of re-deriving it.
	// Empty (CP naming disabled, or external data store) is omitted.
	if warehouse.DataStore.BucketName != "" {
		resp["bucket"] = warehouse.DataStore.BucketName
	}
	c.JSON(http.StatusAccepted, resp)
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
			// As with provisioning, this only starts the teardown. The terminal
			// warehouse_deprovision_success / warehouse_deprovision_failed events
			// are emitted by the async provisioner controller as it deletes the
			// underlying resources.
			analytics.Default().Capture("warehouse_deprovision_begin", orgID, nil)
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
		Bucket:             warehouse.DataStore.BucketName,
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
