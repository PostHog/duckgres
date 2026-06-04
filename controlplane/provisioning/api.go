package provisioning

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
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
	// wraps warehouse + root-user + optional Trino-opt-in writes in a
	// single configstore transaction so partial failure rolls back
	// cleanly. Use this for the public provision endpoint; the older
	// per-step methods below are kept for the standalone surfaces
	// (reset-password, enable/disable trino on an existing org).
	Provision(req ProvisionRequest) error
	CreatePendingWarehouse(orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error
	CreateOrgUser(orgID, username, passwordHash string) error
	UpdateOrgUserPassword(orgID, username, passwordHash string) error
	SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error
	IsDatabaseNameAvailable(name string) (bool, error)

	// Trino lifecycle. EnableTrino is idempotent — re-enabling updates
	// settings without flipping through a disabled state. DisableTrino
	// leaves the row in place so the provisioner observes the transition
	// and cleans up the catalog + password file entry on next reconcile.
	EnableTrino(orgID string, settings configstore.TrinoSettings) error
	DisableTrino(orgID string) error
}

// RegisterAPI registers provisioning endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store Store) {
	h := &handler{store: store}
	r.POST("/orgs/:id/provision", h.provisionWarehouse)
	r.POST("/orgs/:id/deprovision", h.deprovisionWarehouse)
	r.GET("/orgs/:id/warehouse/status", h.getWarehouseStatus)
	r.POST("/orgs/:id/reset-password", h.resetPassword)
	r.POST("/orgs/:id/trino", h.enableTrino)
	r.DELETE("/orgs/:id/trino", h.disableTrino)
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

	// Trino is the opt-in flag for the customer-facing Trino cluster.
	// Optional — when nil, the warehouse is provisioned with the existing
	// PG-only behavior. When non-nil and Enabled=true, the provisioning
	// handler additionally writes a ManagedWarehouseTrino row so the
	// provisioner picks it up on the next reconcile.
	Trino *provisionTrinoReq `json:"trino,omitempty"`
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

// provisionTrinoReq is the per-request Trino opt-in. Mirrored by the
// standalone POST /orgs/:id/trino body (trinoRequest below) so both
// surfaces accept the same shape.
type provisionTrinoReq struct {
	// Enabled flips Trino on for this org. False (or omitted) is a no-op:
	// existing rows are not affected, so the provision endpoint can be
	// retried with Trino={Enabled:false} without disabling a previously
	// enabled org.
	Enabled bool `json:"enabled"`

	// Tier picks the resource-group limits applied to the org. Empty
	// string is the default tier.
	Tier string `json:"tier,omitempty"`
}

// trinoRequest is the body shape for POST /orgs/:id/trino (standalone
// enable on an existing org). Mirrors provisionTrinoReq so callers can
// use one schema for both surfaces.
type trinoRequest struct {
	Enabled bool   `json:"enabled"`
	Tier    string `json:"tier,omitempty"`
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

	// orgID was already validated as a DNS-1123 label at the top of the handler
	// (ducklingOrgIDPattern) — that's all the Trino catalog/group naming needs,
	// since TrinoCatalogName sanitizes it injectively (org_<sanitize(Name)>_iceberg).
	// No extra per-Trino constraint; org names are not numeric team_ids.
	var trinoSettings *configstore.TrinoSettings
	if req.Trino != nil && req.Trino.Enabled {
		trinoSettings = &configstore.TrinoSettings{Tier: req.Trino.Tier}
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

	// One transaction wraps warehouse + root user + optional Trino
	// opt-in. Failure of any sub-step rolls the others back so the
	// caller's retry sees the same starting state (no half-provisioned
	// row blocking re-creation).
	if err := h.store.Provision(ProvisionRequest{
		OrgID:        orgID,
		DatabaseName: req.DatabaseName,
		Warehouse:    warehouse,
		RootUserHash: hash,
		Trino:        trinoSettings,
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

	c.JSON(http.StatusAccepted, gin.H{
		"status":   "provisioning started",
		"org":      orgID,
		"username": "root",
		"password": plainPassword,
	})
}

// enableTrino handles POST /orgs/:id/trino — opting an existing org
// (provisioned previously without Trino) into the customer Trino cluster.
// Idempotent: re-enabling updates the tier without flipping through a
// disabled state.
//
// Note this does NOT require the org's ManagedWarehouse to exist — the
// Trino provisioner gates on its own readiness signal (Iceberg-Ready in
// Lakekeeper), not on the warehouse top-level state. Callers that need
// the warehouse first should call /provision before /trino.
func (h *handler) enableTrino(c *gin.Context) {
	orgID := c.Param("id")
	if orgID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "org id is required"})
		return
	}
	if !ducklingOrgIDPattern.MatchString(orgID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "org id must be a DNS-1123 label (lowercase alphanumerics + hyphens); got: " + orgID})
		return
	}

	var req trinoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !req.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "enabled must be true; use DELETE to disable"})
		return
	}

	// Preflight: the FK on ManagedWarehouseTrino requires the Org row
	// to exist. Without this check, EnableTrino's INSERT hits a
	// foreign-key violation and we'd return a 500 with the raw Postgres
	// error. 404 is the right shape: "the resource you're trying to
	// modify doesn't exist." Callers that need to /provision first
	// see the clear 404 here rather than parsing an FK error string.
	if _, err := h.store.GetOrg(orgID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found; call /provision first"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.store.EnableTrino(orgID, configstore.TrinoSettings{Tier: req.Tier}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"status": "trino enable queued",
		"org":    orgID,
		"tier":   req.Tier,
	})
}

// disableTrino handles DELETE /orgs/:id/trino — opting the org out of
// the customer Trino cluster. The row is kept (with Enabled=false) so
// the provisioner observes the transition and removes the catalog +
// password file entry on its next reconcile tick. Idempotent.
func (h *handler) disableTrino(c *gin.Context) {
	orgID := c.Param("id")
	if orgID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "org id is required"})
		return
	}
	if err := h.store.DisableTrino(orgID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"status": "trino disable queued",
		"org":    orgID,
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
			// Also disable Trino so the reconcile loop tears down
			// the customer-Trino projections (catalog, password/
			// group file entries, OPA bundle ownership, resource
			// group). Without this, deprovisioning a warehouse
			// leaves the Trino row enabled forever — the CASCADE
			// only fires when the Org row itself is deleted, and
			// `reconcileDeleting` doesn't touch the Org row. We'd
			// otherwise keep projecting the deprovisioned org's
			// credentials into Trino indefinitely.
			//
			// Best-effort: failure to disable Trino doesn't abort
			// the warehouse deprovision (the warehouse state is
			// already moved to Deleting). Operator can retry by
			// calling DELETE /orgs/:id/trino directly.
			if disableErr := h.store.DisableTrino(orgID); disableErr != nil {
				// Log via the response — there's no slog on the
				// gin handler, but the caller will see the warning
				// alongside the 202. Soft-fail so the warehouse
				// deprovision still proceeds.
				c.JSON(http.StatusAccepted, gin.H{
					"status":  "deprovisioning started",
					"org":     orgID,
					"warning": "failed to disable trino in the same call; retry DELETE /orgs/:id/trino: " + disableErr.Error(),
				})
				return
			}
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
