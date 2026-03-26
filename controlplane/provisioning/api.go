package provisioning

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

// Store defines the config store operations needed by the provisioning API.
type Store interface {
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	CreatePendingWarehouse(orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error
	SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error
}

// RegisterAPI registers provisioning endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store Store) {
	h := &handler{store: store}
	r.POST("/orgs/:id/provision", h.provisionWarehouse)
	r.POST("/orgs/:id/deprovision", h.deprovisionWarehouse)
	r.GET("/orgs/:id/warehouse/status", h.getWarehouseStatus)
}

type handler struct {
	store Store
}

// warehouseStatusResponse is the public-facing view of warehouse state.
// Only exposes lifecycle status — no infrastructure secrets or internal config.
type warehouseStatusResponse struct {
	OrgID              string                                    `json:"org_id"`
	State              configstore.ManagedWarehouseProvisioningState `json:"state"`
	StatusMessage      string                                    `json:"status_message"`
	S3State            configstore.ManagedWarehouseProvisioningState `json:"s3_state"`
	MetadataStoreState configstore.ManagedWarehouseProvisioningState `json:"metadata_store_state"`
	IdentityState      configstore.ManagedWarehouseProvisioningState `json:"identity_state"`
	SecretsState       configstore.ManagedWarehouseProvisioningState `json:"secrets_state"`
	ReadyAt            *time.Time                                `json:"ready_at,omitempty"`
	FailedAt           *time.Time                                `json:"failed_at,omitempty"`
}

type provisionRequest struct {
	DatabaseName  string                `json:"database_name,omitempty"`
	MetadataStore *provisionMetadataReq `json:"metadata_store,omitempty"`
}

type provisionMetadataReq struct {
	Type   string              `json:"type"`
	Aurora *provisionAuroraReq `json:"aurora,omitempty"`
}

type provisionAuroraReq struct {
	MinACU float64 `json:"min_acu"`
	MaxACU float64 `json:"max_acu"`
}

func (h *handler) provisionWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	var req provisionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.MetadataStore == nil || req.MetadataStore.Aurora == nil || req.MetadataStore.Aurora.MaxACU <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "metadata_store.aurora.max_acu must be greater than 0"})
		return
	}

	// Default database name to org ID if not specified
	databaseName := req.DatabaseName
	if databaseName == "" {
		databaseName = orgID
	}

	warehouse := &configstore.ManagedWarehouse{
		AuroraMinACU: req.MetadataStore.Aurora.MinACU,
		AuroraMaxACU: req.MetadataStore.Aurora.MaxACU,
	}

	if err := h.store.CreatePendingWarehouse(orgID, databaseName, warehouse); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "provisioning started", "org": orgID})
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

	c.JSON(http.StatusOK, warehouseStatusResponse{
		OrgID:              warehouse.OrgID,
		State:              warehouse.State,
		StatusMessage:      warehouse.StatusMessage,
		S3State:            warehouse.S3State,
		MetadataStoreState: warehouse.MetadataStoreState,
		IdentityState:      warehouse.IdentityState,
		SecretsState:       warehouse.SecretsState,
		ReadyAt:            warehouse.ReadyAt,
		FailedAt:           warehouse.FailedAt,
	})
}
