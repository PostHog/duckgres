//go:build kubernetes

package admin

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// operatorStore is the slice of the config store the operators handler needs.
// Narrowing to an interface (satisfied by *configstore.ConfigStore) lets the
// handler — including the last-admin guard — be unit-tested with a fake, the
// same way apiStore backs the rest of the admin API.
type operatorStore interface {
	ListOperators() ([]configstore.Operator, error)
	OperatorRole(email string) (string, error)
	CountAdmins() (int64, error)
	UpsertOperator(email, role, addedBy string) error
	DeleteOperator(email string) (bool, error)
}

// operatorsHandler serves the admin-only Operators management API. Operators
// are the admin-console access list: each row maps an @posthog.com SSO email to
// a role (admin|viewer), which AuthMiddleware resolves per-request.
type operatorsHandler struct {
	store operatorStore
}

// registerOperatorsAPI wires the Operators management endpoints. Every route is
// admin-only (RequireAdmin per-route, so the gate travels with the route and a
// rename can't silently downgrade it). Mutations also flow through the audited
// /api/v1 group, so operator changes are recorded automatically.
func registerOperatorsAPI(r *gin.RouterGroup, store operatorStore) {
	h := &operatorsHandler{store: store}
	r.GET("/operators", RequireAdmin(), h.listOperators)
	r.POST("/operators", RequireAdmin(), h.upsertOperator)
	r.DELETE("/operators/:email", RequireAdmin(), h.deleteOperator)
}

func (h *operatorsHandler) listOperators(c *gin.Context) {
	ops, err := h.store.ListOperators()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"operators": ops})
}

type operatorUpsertRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}

func (h *operatorsHandler) upsertOperator(c *gin.Context) {
	var req operatorUpsertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	role := strings.TrimSpace(req.Role)
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email is required"})
		return
	}
	if !strings.HasSuffix(email, ssoEmailDomain) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email must end in " + ssoEmailDomain})
		return
	}
	if role != string(RoleAdmin) && role != string(RoleViewer) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "role must be \"admin\" or \"viewer\""})
		return
	}

	// existing is the operator's current role ("" if brand-new); we read it up
	// front so we can (a) enforce the last-admin guard and (b) record a
	// human-readable "role X → admin" audit detail.
	existing, err := h.store.OperatorRole(email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Last-admin guard: refuse to demote the final remaining admin to viewer.
	// This is a pre-check + write, not a single atomic statement — acceptable
	// for this tiny, low-traffic, internal-only table where a racing second
	// admin-demotion is implausible and the worst case (zero admins) is still
	// recoverable via the break-glass internal-secret path.
	if role == string(RoleViewer) && existing == string(RoleAdmin) {
		admins, err := h.store.CountAdmins()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if admins <= 1 {
			c.JSON(http.StatusConflict, gin.H{"error": "cannot demote the last remaining admin"})
			return
		}
	}

	// Audit detail: "granted admin role" for a new operator, "role viewer →
	// admin" for a role change, or a plain confirmation for an idempotent set.
	switch {
	case existing == "":
		setAuditDetail(c, "granted "+role+" role (new operator)")
	case existing != role:
		setAuditDetail(c, "role "+existing+" → "+role)
	default:
		setAuditDetail(c, "role unchanged ("+role+")")
	}

	addedBy := ""
	if id := IdentityFromContext(c); id != nil {
		addedBy = id.Email
	}
	if err := h.store.UpsertOperator(email, role, addedBy); err != nil {
		if errors.Is(err, configstore.ErrInvalidOperatorRole) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, configstore.Operator{Email: email, Role: role, AddedBy: addedBy})
}

func (h *operatorsHandler) deleteOperator(c *gin.Context) {
	email := strings.ToLower(strings.TrimSpace(c.Param("email")))
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "email is required"})
		return
	}

	// Last-admin guard: refuse to delete the final remaining admin (see the
	// upsert guard for the pre-check + op rationale).
	role, err := h.store.OperatorRole(email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if role == string(RoleAdmin) {
		admins, err := h.store.CountAdmins()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if admins <= 1 {
			c.JSON(http.StatusConflict, gin.H{"error": "cannot remove the last remaining admin"})
			return
		}
	}
	if role != "" {
		setAuditDetail(c, "removed "+role+" operator")
	}

	deleted, err := h.store.DeleteOperator(email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !deleted {
		c.JSON(http.StatusNotFound, gin.H{"error": "operator not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true})
}
