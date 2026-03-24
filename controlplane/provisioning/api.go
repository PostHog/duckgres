package provisioning

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

// Store defines the config store operations needed by the provisioning API.
type Store interface {
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	CreatePendingWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) error
	SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error
}

// RegisterAPI registers provisioning endpoints on the given router group.
func RegisterAPI(r *gin.RouterGroup, store Store) {
	h := &handler{store: store}
	r.POST("/orgs/:id/provision", h.provisionWarehouse)
	r.POST("/orgs/:id/deprovision", h.deprovisionWarehouse)
	r.GET("/orgs/:id/warehouse", h.getWarehouseStatus)
}

type handler struct {
	store Store
}

type provisionRequest struct {
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

	warehouse := &configstore.ManagedWarehouse{}
	if req.MetadataStore != nil && req.MetadataStore.Aurora != nil {
		warehouse.AuroraMinACU = req.MetadataStore.Aurora.MinACU
		warehouse.AuroraMaxACU = req.MetadataStore.Aurora.MaxACU
	}

	if err := h.store.CreatePendingWarehouse(orgID, warehouse); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "provisioning started", "org": orgID})
}

func (h *handler) deprovisionWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	// Try CAS from ready -> deleting, then from failed -> deleting.
	// This avoids a read-then-write TOCTOU race.
	err := h.store.SetWarehouseDeleting(orgID, configstore.ManagedWarehouseStateReady)
	if err != nil {
		err = h.store.SetWarehouseDeleting(orgID, configstore.ManagedWarehouseStateFailed)
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "warehouse not found"})
			return
		}
		c.JSON(http.StatusConflict, gin.H{"error": "warehouse must be in ready or failed state to deprovision"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "deprovisioning started", "org": orgID})
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

	c.JSON(http.StatusOK, warehouse)
}
