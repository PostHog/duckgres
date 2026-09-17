package provisioning

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

var ErrTrinoCellSelectionRequired = errors.New("select an initial Trino cell before enabling Trino")
var ErrTrinoCellNotConfigured = errors.New("the assigned Trino cell is not configured")

func (h *handler) admitTrino(c *gin.Context, orgID string) bool {
	if h.trinoAdmission == nil {
		return true
	}
	if err := h.trinoAdmission(orgID); err != nil {
		if errors.Is(err, ErrTrinoCellSelectionRequired) || errors.Is(err, ErrTrinoCellNotConfigured) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot verify Trino cell assignment"})
		}
		return false
	}
	return true
}

// Option configures optional deployment-specific provisioning gates.
type Option func(*handler)

// WithTrinoBackendValidator checks backend availability before any provisioning
// writes. Without a validator, Hoglake enablement is deliberately unavailable.
func WithTrinoBackendValidator(validate func(configstore.TrinoBackend) error) Option {
	return func(h *handler) { h.trinoBackendValidator = validate }
}

// Resolve before checking deployment availability, including omitted-field clients.
// The transactional store repeats resolution to detect a concurrent first enable.
func (h *handler) resolveTrinoBackend(c *gin.Context, orgID string, requested configstore.TrinoBackend) (configstore.TrinoBackend, bool) {
	if requested != "" && !requested.Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "backend must be ducklake or hoglake"})
		return "", false
	}
	row, err := h.store.GetManagedWarehouseTrino(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino backend"})
		return "", false
	}
	backend, err := configstore.ResolveTrinoBackend(row, requested)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return "", false
	}
	if backend == configstore.TrinoBackendDuckLake {
		return backend, true
	}
	if h.trinoBackendValidator == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "managed Hoglake provisioning is not configured"})
		return "", false
	}
	if err := h.trinoBackendValidator(backend); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "requested Trino backend is not configured"})
		return "", false
	}
	return backend, true
}
