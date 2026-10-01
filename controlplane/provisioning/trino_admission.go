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

// newClientTrinoBackend is the backend a client with no selection receives:
// Hoglake where the deployment runs managed Hoglake, DuckLake everywhere else.
// Without the fallback, a deployment with no Hoglake could not enable Trino
// for any new org, and onboarding (which enables Trino in the provision call)
// failed outright. DuckLake serves the org's existing warehouse.
func (h *handler) newClientTrinoBackend() configstore.TrinoBackend {
	if h.trinoBackendValidator != nil && h.trinoBackendValidator(configstore.TrinoBackendHoglake) == nil {
		return configstore.TrinoBackendHoglake
	}
	return configstore.TrinoBackendDuckLake
}

// Resolve before checking deployment availability, including omitted-field clients.
// The transactional store repeats resolution to detect a concurrent first enable.
func (h *handler) resolveTrinoBackend(c *gin.Context, orgID string, requested configstore.TrinoBackend) (configstore.TrinoBackend, bool) {
	if requested != "" && !requested.Valid() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "backend must be ducklake or hoglake"})
		return "", false
	}
	// An explicit Hoglake request on a deployment without it is "not
	// configured", not a selection conflict with the DuckLake fallback.
	if requested == configstore.TrinoBackendHoglake && h.newClientTrinoBackend() != configstore.TrinoBackendHoglake {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "managed Hoglake provisioning is not configured"})
		return "", false
	}
	row, err := h.store.GetManagedWarehouseTrino(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino backend"})
		return "", false
	}
	backend, err := configstore.ResolveTrinoBackend(row, requested, h.newClientTrinoBackend())
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

// WithTrinoDefaultCell supplies the validated deployment placement. The store
// persists it atomically with enablement and preserves any existing assignment.
func WithTrinoDefaultCell(cellID string) Option {
	return func(h *handler) { h.trinoDefaultCell = cellID }
}
