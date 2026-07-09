//go:build kubernetes

package admin

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Reshard admin API: start/inspect/cancel metadata-store migrations (see
// docs/design/resharding.md). The operation itself is executed by the
// provisioner-side ReshardRunner; these handlers only create/read op rows and
// the verbose log the console polls.
//
// Registered inside the audited /api/v1 group: RoleGate makes the POSTs
// admin-only, AuditMiddleware records them. The external target password is
// EPHEMERAL: validated, handed to the local runner in-process
// (claim-on-create), and never persisted to the op row, the log, or the audit
// detail.

// ReshardStore is the config-store surface these handlers need.
type ReshardStore interface {
	CreateReshardOperation(op *configstore.ReshardOperation) error
	GetReshardOperation(id int64) (*configstore.ReshardOperation, error)
	ListReshardOperationsForOrg(orgID string, limit int) ([]configstore.ReshardOperation, error)
	ListReshardOperations(limit int) ([]configstore.ReshardOperation, error)
	ListReshardLog(opID, afterID int64, limit int) ([]configstore.ReshardLogEntry, error)
	RequestReshardCancel(id int64) (bool, error)
	FinishPendingReshardOperation(id int64, state configstore.ReshardState, errMsg string) (bool, error)
	AppendReshardLog(opID int64, level, message string) error
	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	ListExternalMetadataStores() ([]configstore.ExternalMetadataStoreInfo, error)
}

// ReshardPasswordStash hands the runner the ephemeral external password for
// an op created on this CP (claim-on-create handoff). Satisfied by
// *provisioner.ReshardRunner.
type ReshardPasswordStash interface {
	StashExternalPassword(opID int64, password string)
}

// reshardShardNamePattern mirrors the Duckling XRD's cnpgShard pattern.
var reshardShardNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

type reshardTargetRequest struct {
	Type string `json:"type"` // "cnpg-shard" | "external"

	// cnpg-shard target
	CnpgShard string `json:"cnpg_shard"`

	// external target (endpoint + SM secret NAME persisted; password ephemeral)
	Endpoint          string `json:"endpoint"`
	User              string `json:"user"`
	Database          string `json:"database"`
	PasswordAWSSecret string `json:"password_aws_secret"`
	Password          string `json:"password"`
}

type startReshardRequest struct {
	Target              reshardTargetRequest `json:"target"`
	DrainTimeoutSeconds int64                `json:"drain_timeout_seconds"`
	// 0 = runner default (15m): how long the cutover waits for the
	// composition to converge before rolling back.
	CutoverTimeoutSeconds int64 `json:"cutover_timeout_seconds"`
}

// RegisterReshardAPI wires the reshard endpoints. lister may be nil (duckling
// client unavailable) — starting a reshard then 503s; reads still work.
// stash may be nil (no local runner) — external targets then 503. cluster may
// be nil (non-k8s) — shard discovery then falls back to the shards tenants
// already occupy.
func RegisterReshardAPI(r *gin.RouterGroup, store ReshardStore, lister DucklingMetadataLister, stash ReshardPasswordStash, cluster kubernetes.Interface) {
	h := &reshardHandler{store: store, lister: lister, stash: stash, cluster: cluster}
	r.POST("/orgs/:id/reshard", h.start)
	r.GET("/orgs/:id/reshards", h.listForOrg)
	r.GET("/reshards", h.listAll)
	r.GET("/reshards/:opid", h.get)
	r.GET("/reshards/:opid/log", h.log)
	r.GET("/reshards/targets", h.targets)
	r.POST("/reshards/:opid/cancel", h.cancel)
}

type reshardHandler struct {
	store   ReshardStore
	lister  DucklingMetadataLister
	stash   ReshardPasswordStash
	cluster kubernetes.Interface
}

// cnpgShardsNamespace is where the shared CNPG metadata shards run; instance
// pods carry the operator's cnpg.io/cluster=<shard> label.
const cnpgShardsNamespace = "cnpg-shards"

// targets returns everything the reshard form can offer as a destination:
// every cnpg shard (including EMPTY ones no tenant occupies yet — discovered
// from the CNPG instance pods via the cluster-topology read the Nodes view
// already uses; an RBAC Forbidden degrades to the shards tenants occupy, read
// from the duckling statuses) and every external metadata store a live
// warehouse references (endpoint + SM secret NAME only — never a password).
func (h *reshardHandler) targets(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), ducklingMetadataTimeout)
	defer cancel()

	shardSet := map[string]struct{}{}
	clusterAvailable := false
	if h.cluster != nil {
		pods, err := h.cluster.CoreV1().Pods(cnpgShardsNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "cnpg.io/cluster",
		})
		switch {
		case err == nil:
			clusterAvailable = true
			for i := range pods.Items {
				if shard := pods.Items[i].Labels["cnpg.io/cluster"]; shard != "" {
					shardSet[shard] = struct{}{}
				}
			}
		case apierrors.IsForbidden(err):
			// Same degrade contract as the cluster topology endpoints: the
			// e2e CP has no cluster-scoped RBAC; fall back to occupied shards.
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	if h.lister != nil {
		stores, err := h.lister.CRMetadataStores(ctx)
		if err == nil {
			for _, ms := range stores {
				if ms.Kind == configstore.MetadataStoreKindCnpgShard {
					if shard := cnpgShardFromEndpoint(ms.Endpoint); shard != "" {
						shardSet[shard] = struct{}{}
					}
				}
			}
		}
	}
	shards := make([]string, 0, len(shardSet))
	for s := range shardSet {
		shards = append(shards, s)
	}
	sort.Strings(shards)

	external, err := h.store.ListExternalMetadataStores()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if external == nil {
		external = []configstore.ExternalMetadataStoreInfo{}
	}

	c.JSON(http.StatusOK, gin.H{
		"shards": shards,
		// cluster_discovery=false means the shard list only contains shards
		// tenants already occupy (RBAC degrade / non-k8s) — a brand-new empty
		// shard would be missing and needs manual entry.
		"cluster_discovery": clusterAvailable,
		"external_stores":   external,
	})
}

func (h *reshardHandler) start(c *gin.Context) {
	orgID := c.Param("id")
	var req startReshardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body: " + err.Error()})
		return
	}

	wh, err := h.store.GetManagedWarehouse(orgID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org has no managed warehouse"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if wh.State != configstore.ManagedWarehouseStateReady {
		c.JSON(http.StatusConflict, gin.H{"error": "warehouse must be in ready state to reshard (currently " + string(wh.State) + ")"})
		return
	}
	sourceKind := wh.MetadataStore.Kind
	if sourceKind == "" {
		sourceKind = configstore.MetadataStoreKindCnpgShard
	}
	if sourceKind != configstore.MetadataStoreKindCnpgShard && sourceKind != configstore.MetadataStoreKindExternal {
		c.JSON(http.StatusConflict, gin.H{"error": "unsupported source metadata-store kind " + sourceKind})
		return
	}

	op := &configstore.ReshardOperation{
		OrgID:                 orgID,
		DucklingName:          wh.DucklingName,
		SourceKind:            sourceKind,
		DrainTimeoutSeconds:   req.DrainTimeoutSeconds,
		CutoverTimeoutSeconds: req.CutoverTimeoutSeconds,
	}
	if op.CutoverTimeoutSeconds < 0 {
		op.CutoverTimeoutSeconds = 0
	}
	if op.DucklingName == "" {
		op.DucklingName = orgID
	}
	if op.DrainTimeoutSeconds <= 0 {
		op.DrainTimeoutSeconds = 1800
	}
	if sourceKind == configstore.MetadataStoreKindExternal {
		op.SourceEndpoint = wh.MetadataStore.Endpoint
		op.SourceUser = wh.MetadataStore.Username
		op.SourceDatabase = wh.MetadataStore.DatabaseName
		op.SourcePasswordSecret = wh.MetadataStore.PasswordAWSSecret
	}

	currentShard := h.currentShard(c, op.DucklingName)
	op.FromShard = currentShard

	switch req.Target.Type {
	case configstore.MetadataStoreKindCnpgShard:
		shard := strings.TrimSpace(req.Target.CnpgShard)
		if len(shard) > 63 || !reshardShardNamePattern.MatchString(shard) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "target.cnpg_shard must match " + reshardShardNamePattern.String() + " (e.g. shard-002)"})
			return
		}
		if sourceKind == configstore.MetadataStoreKindCnpgShard && currentShard != "" && shard == currentShard {
			c.JSON(http.StatusBadRequest, gin.H{"error": "target shard equals the org's current shard (" + currentShard + ")"})
			return
		}
		op.TargetKind = configstore.MetadataStoreKindCnpgShard
		op.ToShard = shard
	case configstore.MetadataStoreKindExternal:
		if sourceKind == configstore.MetadataStoreKindExternal {
			c.JSON(http.StatusBadRequest, gin.H{"error": "external → external is not supported"})
			return
		}
		if strings.TrimSpace(req.Target.Endpoint) == "" || strings.TrimSpace(req.Target.PasswordAWSSecret) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "external target requires endpoint and password_aws_secret"})
			return
		}
		if req.Target.Password == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "external target requires the password (sent once, never stored — the runner uses it for the copy; password_aws_secret must contain the same value)"})
			return
		}
		if h.stash == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no reshard runner on this control plane — external targets unavailable"})
			return
		}
		op.TargetKind = configstore.MetadataStoreKindExternal
		op.TargetEndpoint = strings.TrimSpace(req.Target.Endpoint)
		op.TargetPasswordSecret = strings.TrimSpace(req.Target.PasswordAWSSecret)
		op.TargetUser = strings.TrimSpace(req.Target.User)
		op.TargetDatabase = strings.TrimSpace(req.Target.Database)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "target.type must be cnpg-shard or external"})
		return
	}

	if err := h.store.CreateReshardOperation(op); err != nil {
		if errors.Is(err, configstore.ErrReshardConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "org already has an active reshard operation"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if op.TargetKind == configstore.MetadataStoreKindExternal && h.stash != nil {
		// Ephemeral handoff to the LOCAL runner — never persisted. The local
		// runner claims pending ops within its poll tick; only a crash before
		// the claim loses the password (the runner then fails the op with a
		// clear re-run message).
		h.stash.StashExternalPassword(op.ID, req.Target.Password)
	}
	_ = h.store.AppendReshardLog(op.ID, "info", "operation created by "+actorForAudit(c)+": "+describeReshard(op))
	// Audit detail carries no secrets.
	setAuditDetail(c, "reshard "+describeReshard(op))

	c.JSON(http.StatusAccepted, op)
}

func describeReshard(op *configstore.ReshardOperation) string {
	src := op.SourceKind
	if op.FromShard != "" {
		src += " " + op.FromShard
	} else if op.SourceEndpoint != "" {
		src += " " + op.SourceEndpoint
	}
	dst := op.TargetKind
	if op.ToShard != "" {
		dst += " " + op.ToShard
	} else if op.TargetEndpoint != "" {
		dst += " " + op.TargetEndpoint
	}
	return "org " + op.OrgID + ": " + src + " → " + dst
}

// currentShard resolves the org's live shard from the duckling CR status
// (best-effort — empty when the lister is unavailable).
func (h *reshardHandler) currentShard(c *gin.Context, ducklingName string) string {
	if h.lister == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), ducklingMetadataTimeout)
	defer cancel()
	stores, err := h.lister.CRMetadataStores(ctx)
	if err != nil {
		return ""
	}
	ms, ok := stores[strings.ToLower(ducklingName)]
	if !ok {
		ms, ok = stores[ducklingName]
	}
	if !ok || ms.Kind != "cnpg-shard" {
		return ""
	}
	return cnpgShardFromEndpoint(ms.Endpoint)
}

// listAll returns operations across every org, newest first — the console's
// global Reshards page.
func (h *reshardHandler) listAll(c *gin.Context) {
	ops, err := h.store.ListReshardOperations(parseIntDefault(c.Query("limit"), 100))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if ops == nil {
		ops = []configstore.ReshardOperation{}
	}
	c.JSON(http.StatusOK, gin.H{"operations": ops})
}

func (h *reshardHandler) listForOrg(c *gin.Context) {
	ops, err := h.store.ListReshardOperationsForOrg(c.Param("id"), parseIntDefault(c.Query("limit"), 50))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if ops == nil {
		ops = []configstore.ReshardOperation{}
	}
	c.JSON(http.StatusOK, gin.H{"operations": ops})
}

func (h *reshardHandler) get(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("opid"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid operation id"})
		return
	}
	op, err := h.store.GetReshardOperation(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no such reshard operation"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, op)
}

func (h *reshardHandler) log(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("opid"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid operation id"})
		return
	}
	afterID, _ := strconv.ParseInt(c.Query("after_id"), 10, 64)
	entries, err := h.store.ListReshardLog(id, afterID, parseIntDefault(c.Query("limit"), 500))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []configstore.ReshardLogEntry{}
	}
	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

func (h *reshardHandler) cancel(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("opid"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid operation id"})
		return
	}
	op, err := h.store.GetReshardOperation(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no such reshard operation"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if op.State.Terminal() {
		c.JSON(http.StatusConflict, gin.H{"error": "operation already " + string(op.State)})
		return
	}

	// A pending (unclaimed) op finishes immediately; a running one gets the
	// cancel flag and the runner rolls back from wherever it is.
	if op.State == configstore.ReshardStatePending {
		if done, err := h.store.FinishPendingReshardOperation(id, configstore.ReshardStateCancelled, "cancelled before start"); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		} else if done {
			_ = h.store.AppendReshardLog(id, "warn", "cancelled by "+actorForAudit(c)+" before the runner claimed it")
			setAuditDetail(c, "reshard cancel op "+strconv.FormatInt(id, 10)+" (pending)")
			c.JSON(http.StatusOK, gin.H{"state": configstore.ReshardStateCancelled})
			return
		}
		// Raced with a claim — fall through to the flag path.
	}
	if _, err := h.store.RequestReshardCancel(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = h.store.AppendReshardLog(id, "warn", "cancel requested by "+actorForAudit(c))
	setAuditDetail(c, "reshard cancel op "+strconv.FormatInt(id, 10))
	c.JSON(http.StatusAccepted, gin.H{"cancel_requested": true})
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

// actorForAudit renders the requesting identity for log lines (the audit
// middleware records the structured entry separately).
func actorForAudit(c *gin.Context) string {
	if id := IdentityFromContext(c); id != nil && id.Email != "" {
		return id.Email
	}
	return "internal-secret"
}
