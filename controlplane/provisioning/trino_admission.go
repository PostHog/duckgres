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

func (h *handler) admitTrinoBackend(c *gin.Context, backend configstore.TrinoBackend) bool {
	if backend != "" && !backend.Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "backend must be ducklake or hoglake"})
		return false
	}
	// Preserve the backend for existing omitted-field clients.
	if backend == "" {
		return true
	}
	if h.trinoBackendValidator != nil {
		if err := h.trinoBackendValidator(backend); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "requested Trino backend is not configured"})
			return false
		}
	} else if backend == configstore.TrinoBackendHoglake {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "managed Hoglake provisioning is not configured"})
		return false
	}
	return true
}
