//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

type trinoRecoveryStore interface {
	GetTrinoPool(context.Context, string) (*configstore.TrinoPool, error)
	GetTrinoPoolInstance(context.Context, string) (*configstore.TrinoPoolInstance, error)
	ListTrinoPoolInstances(context.Context, string) ([]configstore.TrinoPoolInstance, error)
	GetTrinoPoolRecovery(context.Context, string, string) (*configstore.TrinoPoolRecovery, error)
	RequestTrinoPoolRecovery(context.Context, string, string, configstore.TrinoPoolRecovery) (*configstore.TrinoPoolRecovery, error)
}

type trinoRecoveryRequest struct {
	OperationID              string `json:"operation_id"`
	ExpectedGeneration       int64  `json:"expected_generation"`
	Incarnation              string `json:"incarnation"`
	PodUID                   string `json:"pod_uid"`
	BootID                   string `json:"boot_id"`
	NodeID                   string `json:"node_id"`
	CoordinatorID            string `json:"coordinator_id"`
	Reason                   string `json:"reason"`
	DestructiveAuthorization bool   `json:"destructive_authorization"`
}

type trinoRecoveryInstanceSummary struct {
	InstanceID     string    `json:"instance_id"`
	Phase          string    `json:"phase"`
	GatewayState   string    `json:"gateway_state"`
	PhaseChangedAt time.Time `json:"phase_changed_at"`
}

func (a *TrinoAPI) handleRecoveryInstances(c *gin.Context) {
	identity := IdentityFromContext(c)
	if identity == nil || identity.Role != RoleAdmin || strings.TrimSpace(identity.Email) == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return
	}
	store, ok := a.orgs.(trinoRecoveryStore)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Trino recovery is unavailable"})
		return
	}
	ctx := c.Request.Context()
	pool, err := store.GetTrinoPool(ctx, a.cell.storedID())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino pool"})
		return
	}
	if pool == nil || pool.APIMode != configstore.TrinoPoolAPIModeShared {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown shared Trino pool"})
		return
	}
	instances, err := store.ListTrinoPoolInstances(ctx, pool.PoolID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino instances"})
		return
	}
	rows := make([]trinoRecoveryInstanceSummary, 0)
	for _, instance := range instances {
		if instance.PoolID != pool.PoolID || trinopool.Phase(instance.Phase).Terminal() {
			continue
		}
		rows = append(rows, trinoRecoveryInstanceSummary{
			InstanceID: instance.InstanceID, Phase: instance.Phase,
			GatewayState: instance.GatewayState, PhaseChangedAt: instance.PhaseChangedAt,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].InstanceID < rows[j].InstanceID })
	c.JSON(http.StatusOK, gin.H{"cell": a.cell.ID, "instances": rows})
}

func recoveryRequestView(request *configstore.TrinoPoolRecovery) any {
	if request == nil {
		return nil
	}
	return gin.H{
		"operation_id": request.OperationID, "instance_id": request.InstanceID,
		"expected_generation": request.ExpectedGeneration, "incarnation": request.Incarnation,
		"pod_uid": request.PodUID, "boot_id": request.BootID, "node_id": request.NodeID,
		"coordinator_id": request.CoordinatorID, "requested_by": request.RequestedBy,
		"reason": request.Reason, "destructive_authorization": request.DestructiveAuthorization,
		"created_at": request.CreatedAt,
	}
}

func (a *TrinoAPI) recoveryTarget(c *gin.Context) (trinoRecoveryStore, *configstore.TrinoPool, *configstore.TrinoPoolInstance) {
	identity := IdentityFromContext(c)
	if identity == nil || identity.Role != RoleAdmin || strings.TrimSpace(identity.Email) == "" {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return nil, nil, nil
	}
	store, ok := a.orgs.(trinoRecoveryStore)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Trino recovery is unavailable"})
		return nil, nil, nil
	}
	instance, err := store.GetTrinoPoolInstance(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino instance"})
		return nil, nil, nil
	}
	if instance == nil || instance.PoolID != a.cell.storedID() {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown Trino instance"})
		return nil, nil, nil
	}
	pool, err := store.GetTrinoPool(c.Request.Context(), instance.PoolID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino pool"})
		return nil, nil, nil
	}
	if pool == nil || pool.APIMode != configstore.TrinoPoolAPIModeShared {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown shared Trino pool"})
		return nil, nil, nil
	}
	return store, pool, instance
}

func (a *TrinoAPI) handleRecoveryPreview(c *gin.Context) {
	store, pool, instance := a.recoveryTarget(c)
	if store == nil {
		return
	}
	ctx := c.Request.Context()
	instances, err := store.ListTrinoPoolInstances(ctx, pool.PoolID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino capacity"})
		return
	}
	request, err := store.GetTrinoPoolRecovery(ctx, pool.PoolID, instance.InstanceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot read Trino recovery"})
		return
	}
	serving := 0
	for _, candidate := range instances {
		if candidate.PoolID == pool.PoolID && trinopool.Phase(candidate.Phase).Serving() {
			serving++
		}
	}
	lastError := ""
	if request != nil && instance.LastError == configstore.TrinoPoolRecoveryBlockedMessage {
		lastError = instance.LastError
	}
	// This snapshot is deliberately not a probe or an authorization to delete.
	c.JSON(http.StatusOK, gin.H{
		"cell": a.cell.ID,
		"instance": gin.H{
			"instance_id": instance.InstanceID, "phase": instance.Phase,
			"gateway_state": instance.GatewayState, "expected_generation": instance.GatewayGeneration,
			"incarnation": instance.GatewayIncarnation, "pod_uid": instance.CoordinatorPodUID,
			"boot_id": instance.CoordinatorBootID, "node_id": instance.CoordinatorNodeID,
			"coordinator_id": instance.CoordinatorID, "phase_changed_at": instance.PhaseChangedAt,
			"last_error": lastError,
		},
		"capacity":           gin.H{"stored_serving": serving, "min_serving": pool.MinServing, "desired_instances": pool.DesiredInstances, "frozen": pool.Frozen},
		"live_work_verified": false,
		"request":            recoveryRequestView(request),
	})
}

func (a *TrinoAPI) handleRequestRecovery(c *gin.Context) {
	store, pool, instance := a.recoveryTarget(c)
	if store == nil {
		return
	}
	var req trinoRecoveryRequest
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery request"})
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery request"})
		return
	}
	if !req.DestructiveAuthorization || req.ExpectedGeneration <= 0 || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 256 || strings.TrimSpace(req.OperationID) == "" || len(req.OperationID) > 128 || strings.TrimSpace(req.Incarnation) == "" || strings.TrimSpace(req.PodUID) == "" || strings.TrimSpace(req.BootID) == "" || strings.TrimSpace(req.NodeID) == "" || strings.TrimSpace(req.CoordinatorID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "explicit destructive authorization, reason, operation ID, and exact instance identity are required"})
		return
	}
	request, err := store.RequestTrinoPoolRecovery(c.Request.Context(), pool.PoolID, instance.InstanceID, configstore.TrinoPoolRecovery{
		OperationID: req.OperationID, PoolID: pool.PoolID, InstanceID: instance.InstanceID,
		ExpectedGeneration: req.ExpectedGeneration, Incarnation: req.Incarnation, PodUID: req.PodUID,
		BootID: req.BootID, NodeID: req.NodeID, CoordinatorID: req.CoordinatorID,
		RequestedBy: IdentityFromContext(c).Email, Reason: req.Reason, DestructiveAuthorization: req.DestructiveAuthorization,
	})
	if err != nil {
		switch {
		case errors.Is(err, configstore.ErrTrinoPoolRecoveryConflict), errors.Is(err, configstore.ErrTrinoPoolConflict), errors.Is(err, configstore.ErrTrinoPoolIntentChanged):
			c.JSON(http.StatusConflict, gin.H{"error": "recovery conflicts with the recorded intent or current instance state"})
		case errors.Is(err, configstore.ErrTrinoPoolRecoveryInvalid):
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery request"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot request Trino recovery"})
		}
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"request": recoveryRequestView(request)})
}
