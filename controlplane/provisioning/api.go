package provisioning

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

// Store defines the config store operations needed by the provisioning API.
type Store interface {
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	GetOrg(orgID string) (*configstore.Org, error)
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
	DatabaseName  string                `json:"database_name"`
	MetadataStore *provisionMetadataReq `json:"metadata_store,omitempty"`

	// Trino is the opt-in flag for the customer-facing Trino cluster.
	// Optional — when nil, the warehouse is provisioned with the existing
	// PG-only behavior. When non-nil and Enabled=true, the provisioning
	// handler additionally writes a ManagedWarehouseTrino row so the
	// provisioner picks it up on the next reconcile.
	Trino *provisionTrinoReq `json:"trino,omitempty"`
}

type provisionMetadataReq struct {
	Type string `json:"type"`
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

func (h *handler) provisionWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	var req provisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
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

	// The control plane provisions exactly one metadata-store backend for new
	// warehouses: cnpg-shard (the per-tenant Lakekeeper Iceberg catalog on the
	// shared CloudNativePG shard). DuckLake (aurora) and external are no longer
	// provisionable.
	//
	// This gate is on *creation* only. Existing DuckLake and external ducklings
	// keep running untouched: the worker activator still reads their Duckling
	// CR / config-store metadata and attaches DuckLake, the controller still
	// reconciles their existing CRs (reconcilePending skips Create when the CR
	// already exists, and DucklingClient.Create still understands aurora), and
	// admin read/mutate/deprovision are unaffected.
	warehouse := &configstore.ManagedWarehouse{}
	switch req.MetadataStore.Type {
	case configstore.MetadataStoreKindCnpgShard:
		// cnpg-shard takes no aurora sizing and, per the Duckling XRD, must
		// always have Iceberg enabled — so enable it here (Lakekeeper backend)
		// rather than making the caller pass it redundantly. The composition
		// picks the active shard from chart values; there is no per-claim
		// placement knob.
		warehouse.MetadataStore.Kind = configstore.MetadataStoreKindCnpgShard
		warehouse.Iceberg = configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		}
	case configstore.MetadataStoreKindAurora:
		c.JSON(http.StatusBadRequest, gin.H{"error": "DuckLake (metadata_store.type 'aurora') is no longer provisionable; only 'cnpg-shard' is. Existing DuckLake deployments are unaffected."})
		return
	case "external":
		c.JSON(http.StatusBadRequest, gin.H{"error": "external metadata stores are not provisionable via the control plane; existing external deployments are unaffected"})
		return
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("metadata_store.type must be %q (got %q); 'aurora'/DuckLake and 'external' are no longer provisionable", configstore.MetadataStoreKindCnpgShard, req.MetadataStore.Type)})
		return
	}

	if err := h.store.CreatePendingWarehouse(orgID, req.DatabaseName, warehouse); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	// Create root user with a generated password. The plaintext is returned
	// in this response only — it is never stored, only the bcrypt hash is persisted.
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
	if err := h.store.CreateOrgUser(orgID, "root", hash); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create root user"})
		return
	}

	// Optionally enable Trino in the same handler. Failures here roll
	// back to a 500 — partial state (warehouse + user created but trino
	// row missing) would mean the caller's next retry conflicts on the
	// warehouse row but the trino opt-in was silently lost. We'd rather
	// surface that so the caller knows to retry.
	if req.Trino != nil && req.Trino.Enabled {
		if err := h.store.EnableTrino(orgID, configstore.TrinoSettings{Tier: req.Trino.Tier}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enable trino: " + err.Error()})
			return
		}
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

	var req trinoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !req.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "enabled must be true; use DELETE to disable"})
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
