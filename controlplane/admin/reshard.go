//go:build kubernetes

package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Reshard admin API: start/inspect/cancel metadata-store migrations (see
// docs/design/resharding.md). The operation itself is executed by a DEDICATED
// per-operation runner pod (duckgres-reshard-op-<id>, spawned here on start);
// these handlers only create/read op rows, spawn the pod, and serve the
// verbose log the console polls.
//
// Registered inside the audited /api/v1 group: RoleGate makes the POSTs
// admin-only, AuditMiddleware records them. The external target password is
// EPHEMERAL: validated, held in THIS replica's in-memory stash, pulled once by
// the runner pod over the internal-secret-only password endpoint, and never
// persisted to the op row, the log, the audit detail, or any pod spec.

// ReshardStore is the config-store surface these handlers need.
type ReshardStore interface {
	CreateReshardOperation(op *configstore.ReshardOperation) error
	SetReshardOperationPasswordURL(id int64, url string) error
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

// ReshardPodSpawner spawns the dedicated runner pod for a just-created
// operation. Satisfied by *controlplane.ReshardPodSpawner. nil when this CP
// has no kubernetes API — starting a reshard is then refused (nothing would
// ever execute it).
type ReshardPodSpawner interface {
	// SpawnReshardPod creates the duckgres-reshard-op-<id> pod (no wait for
	// readiness; the pod claims the op row itself).
	SpawnReshardPod(ctx context.Context, op *configstore.ReshardOperation) error
	// PasswordURLForOp renders the URL (pointing at THIS replica) the runner
	// pod pulls the ephemeral external target password from. Empty when the
	// replica cannot determine its own pod IP.
	PasswordURLForOp(opID int64) string
}

// reshardPasswordStash holds the ephemeral external-target passwords by op id,
// in THIS replica's memory only, until the runner pod pulls them or the op
// turns terminal. Never persisted anywhere.
type reshardPasswordStash struct {
	mu   sync.Mutex
	byOp map[int64]string
}

func newReshardPasswordStash() *reshardPasswordStash {
	return &reshardPasswordStash{byOp: map[int64]string{}}
}

func (s *reshardPasswordStash) put(opID int64, password string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byOp[opID] = password
}

func (s *reshardPasswordStash) get(opID int64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pw, ok := s.byOp[opID]
	return pw, ok
}

func (s *reshardPasswordStash) delete(opID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byOp, opID)
}

// pruneTerminal lazily drops stashed passwords whose operation reached a
// terminal state (called from the start + password handlers; reshards are rare
// so the stash stays tiny).
func (s *reshardPasswordStash) pruneTerminal(store ReshardStore) {
	s.mu.Lock()
	ids := make([]int64, 0, len(s.byOp))
	for id := range s.byOp {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		op, err := store.GetReshardOperation(id)
		if err != nil {
			continue
		}
		if op.State.Terminal() {
			s.delete(id)
		}
	}
}

// reshardShardNamePattern mirrors the Duckling XRD's cnpgShard pattern.
var reshardShardNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// rdsManagedSecretNamePattern matches AWS RDS-managed master-password secret
// names: the `rds!db-…`/`rds!cluster-…` names RDS creates for
// manage_master_user_password, and the `rds/<db>/master` alias console paths.
// The external-secrets IAM policy only allows `secretsmanager:GetSecretValue`
// on a per-env prefix allowlist (posthog-*, duckling-*, … — see the
// external-secrets-pod-identity terraform module in posthog-cloud-infra); no
// environment allows `rds…`, so a reshard pointed at one of these would pass
// the catalog copy and then hang the cutover on an ESO AccessDenied. Reject
// it up front.
var rdsManagedSecretNamePattern = regexp.MustCompile(`^rds[!/]`)

// esoReadableSecretPrefixes is the positive allowlist of Secrets Manager name
// prefixes the external-secrets IAM role can GetSecretValue on. The policy is
// identical across every managed-warehouse environment (dev / prod-us /
// prod-eu) — see the external-secrets-pod-identity terraform module in
// posthog-cloud-infra — so these two prefixes are hardcoded rather than
// resolved per-env. A cnpg→external reshard whose SM secret name is outside
// this set would pass the catalog copy and then hang the destructive cutover on
// an ESO AccessDenied, so the start handler rejects it up front (Fix 1).
var esoReadableSecretPrefixes = []string{"posthog-", "duckling-"}

// hasESOReadablePrefix reports whether name begins with a prefix the
// external-secrets role is allowed to read.
func hasESOReadablePrefix(name string) bool {
	for _, p := range esoReadableSecretPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// externalProbeTimeout bounds the pre-flight connection check to the external
// target so a black-holed endpoint can't hang the admin HTTP handler; a timeout
// counts as a connect failure → 400.
const externalProbeTimeout = 8 * time.Second

// ExternalTargetProber verifies the CP can actually reach + authenticate
// against a cnpg→external reshard target Postgres before the op is created —
// the destructive cnpg→ext flip (Crossplane DELETEs the source role/DB) happens
// long after submit, so a doomed credential/endpoint is far cheaper to catch
// here than after the flip. Satisfied in production by an adapter over
// provisioner.PGCatalogCopier (wired in controlplane/multitenant.go); nil in
// tests / non-k8s builds, in which case the check is SKIPPED (the runner's copy
// step still catches a bad credential later — this is a fail-fast optimization,
// not the only line of defense).
type ExternalTargetProber interface {
	// Probe opens a bounded connection to endpoint (host[:port], default port
	// 5432) as user/database with password over the given sslmode and runs a
	// trivial liveness query. It MUST NOT echo the password in its error.
	Probe(ctx context.Context, endpoint, user, database, password, sslMode string) error
}

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
// client unavailable) — shard discovery degrades; reads still work. spawner
// may be nil (no kubernetes API) — starting a reshard then 503s (nothing would
// execute the op). cluster may be nil (non-k8s) — shard discovery then falls
// back to the shards tenants already occupy. prober may be nil (tests /
// non-k8s) — the external target pre-flight connection check is then SKIPPED
// (see ExternalTargetProber).
func RegisterReshardAPI(r *gin.RouterGroup, store ReshardStore, lister DucklingMetadataLister, spawner ReshardPodSpawner, cluster kubernetes.Interface, prober ExternalTargetProber) {
	h := &reshardHandler{store: store, lister: lister, spawner: spawner, cluster: cluster, prober: prober, stash: newReshardPasswordStash()}
	r.POST("/orgs/:id/reshard", h.start)
	r.GET("/orgs/:id/reshards", h.listForOrg)
	r.GET("/reshards", h.listAll)
	r.GET("/reshards/:opid", h.get)
	r.GET("/reshards/:opid/log", h.log)
	r.GET("/reshards/targets", h.targets)
	r.POST("/reshards/:opid/cancel", h.cancel)
	// Internal-secret-only: the runner pod's one-shot ephemeral password pull.
	r.GET("/reshards/:opid/password", h.password)
}

type reshardHandler struct {
	store   ReshardStore
	lister  DucklingMetadataLister
	spawner ReshardPodSpawner
	cluster kubernetes.Interface
	prober  ExternalTargetProber
	stash   *reshardPasswordStash
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
	// The op executes in a dedicated runner pod; without a spawner nothing
	// would ever run it, so refuse up front (any target kind).
	if h.spawner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no reshard pod spawner on this control plane (kubernetes API unavailable)"})
		return
	}
	h.stash.pruneTerminal(h.store)

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
		targetEndpoint := strings.TrimSpace(req.Target.Endpoint)
		targetSecret := strings.TrimSpace(req.Target.PasswordAWSSecret)
		if targetEndpoint == "" || targetSecret == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "external target requires endpoint and password_aws_secret"})
			return
		}
		// Fix 1 — the SM secret NAME must be ESO-readable. Detect the more
		// specific RDS-managed-master shape first (its message is more helpful),
		// then fall back to the general positive-allowlist message.
		switch {
		case rdsManagedSecretNamePattern.MatchString(targetSecret):
			c.JSON(http.StatusBadRequest, gin.H{"error": "password_aws_secret looks like an RDS-managed master secret (rds!… / rds/…) — the external-secrets role cannot read those, so the cutover would hang on an ESO AccessDenied. Create a Secrets Manager secret whose name the ESO policy allows (e.g. duckling-<name>-…-rds-password) with the raw password string as its value, and use that name"})
			return
		case !hasESOReadablePrefix(targetSecret):
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("password_aws_secret %q is not readable by the external-secrets role: its IAM policy only allows Secrets Manager names starting with %s, so the cutover would hang on an ESO AccessDenied. Create a secret whose name starts with one of those (convention: duckling-<name>-<env>-<region>-rds-password) holding the raw password string, and use that name", targetSecret, strings.Join(esoReadableSecretPrefixes, " or "))})
			return
		}
		if req.Target.Password == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "external target requires the password (sent once, never stored — the runner uses it for the copy; password_aws_secret must contain the same value)"})
			return
		}
		targetUser := strings.TrimSpace(req.Target.User)
		if targetUser == "" {
			targetUser = "postgres"
		}
		targetDatabase := strings.TrimSpace(req.Target.Database)
		if targetDatabase == "" {
			targetDatabase = "postgres"
		}
		// Fix 2 — pre-flight connection check. Prove the CP can reach the target
		// Postgres AND that the endpoint/user/database/password are valid before
		// the op is created — the destructive cnpg→ext flip happens much later,
		// so a doomed target is far cheaper to catch here than after the flip.
		// External RDS uses sslmode=require (matches the runner's copy of the
		// external target). Skipped when no prober is configured (tests / non-k8s
		// — the runner's copy still catches a bad credential later). The error is
		// redacted: it never echoes the password.
		if h.prober != nil {
			probeCtx, cancel := context.WithTimeout(c.Request.Context(), externalProbeTimeout)
			err := h.prober.Probe(probeCtx, targetEndpoint, targetUser, targetDatabase, req.Target.Password, "require")
			cancel()
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("cannot connect to the external target %s/%s as %s with the provided password: %v", targetEndpoint, targetDatabase, targetUser, err)})
				return
			}
		}
		op.TargetKind = configstore.MetadataStoreKindExternal
		op.TargetEndpoint = targetEndpoint
		op.TargetPasswordSecret = targetSecret
		op.TargetUser = strings.TrimSpace(req.Target.User)
		op.TargetDatabase = strings.TrimSpace(req.Target.Database)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "target.type must be cnpg-shard or external"})
		return
	}

	// Create the op PENDING, then spawn its dedicated runner pod. The pod
	// claims the row itself (pending → running via the standard claim CAS);
	// the leader reconciler backstops a pod that dies or never starts.
	if createErr := h.store.CreateReshardOperation(op); createErr != nil {
		if errors.Is(createErr, configstore.ErrReshardConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "org already has an active reshard operation"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": createErr.Error()})
		return
	}

	// Ephemeral external-target password handoff: stash it in THIS replica's
	// memory and record the pull URL (which points at this replica's pod IP)
	// on the op row so the runner pod — and a reconciler respawn of it — can
	// fetch it over the internal-secret-only password endpoint. The URL is
	// persisted; the password never is.
	if op.TargetKind == configstore.MetadataStoreKindExternal {
		url := h.spawner.PasswordURLForOp(op.ID)
		if url == "" {
			_, _ = h.store.FinishPendingReshardOperation(op.ID, configstore.ReshardStateFailed, "control plane could not determine its own pod IP for the password handoff")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "control plane could not determine its own pod IP for the ephemeral password handoff"})
			return
		}
		if err := h.store.SetReshardOperationPasswordURL(op.ID, url); err != nil {
			_, _ = h.store.FinishPendingReshardOperation(op.ID, configstore.ReshardStateFailed, "recording the password handoff URL failed: "+err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "recording the password handoff URL failed: " + err.Error()})
			return
		}
		op.PasswordURL = url
		h.stash.put(op.ID, req.Target.Password)
	}

	if err := h.spawner.SpawnReshardPod(c.Request.Context(), op); err != nil {
		h.stash.delete(op.ID)
		_, _ = h.store.FinishPendingReshardOperation(op.ID, configstore.ReshardStateFailed, "spawning the reshard runner pod failed: "+err.Error())
		_ = h.store.AppendReshardLog(op.ID, "error", "spawning the reshard runner pod failed: "+err.Error())
		c.JSON(http.StatusBadGateway, gin.H{"error": "spawning the reshard runner pod failed: " + err.Error()})
		return
	}

	_ = h.store.AppendReshardLog(op.ID, "info", "operation created by "+actorForAudit(c)+": "+describeReshard(op)+
		"; runner pod "+reshardPodNameForLog(op.ID)+" spawned")
	// Audit detail carries no secrets.
	setAuditDetail(c, "reshard "+describeReshard(op))

	c.JSON(http.StatusAccepted, op)
}

// reshardPodNameForLog mirrors controlplane.ReshardPodName without the import
// (admin must not depend on the controlplane package).
func reshardPodNameForLog(opID int64) string {
	return "duckgres-reshard-op-" + strconv.FormatInt(opID, 10)
}

// password serves the runner pod's ONE-SHOT pull of the ephemeral external
// target password. Internal-secret callers ONLY (the runner pod authenticates
// with the same internal secret the CP holds) — an SSO admin must never read a
// tenant credential, so Source is checked, not just the admin role. 404 for
// anything unavailable (unknown op, terminal op, stash on another replica or
// already gone). The password value is never logged or audited; the ACCESS is
// recorded in the op log.
func (h *reshardHandler) password(c *gin.Context) {
	id := IdentityFromContext(c)
	if id == nil || id.Source != "internal-secret" {
		c.JSON(http.StatusForbidden, gin.H{"error": "internal secret required"})
		return
	}
	opID, err := strconv.ParseInt(c.Param("opid"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid operation id"})
		return
	}
	h.stash.pruneTerminal(h.store)
	op, err := h.store.GetReshardOperation(opID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "password not available"})
		return
	}
	if op.State.Terminal() {
		h.stash.delete(opID)
		c.JSON(http.StatusNotFound, gin.H{"error": "password not available"})
		return
	}
	pw, ok := h.stash.get(opID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "password not available"})
		return
	}
	_ = h.store.AppendReshardLog(opID, "info", "runner pod fetched the ephemeral external target password")
	c.JSON(http.StatusOK, gin.H{"password": pw})
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
