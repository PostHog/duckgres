//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// provisionEventProps builds the common property set for warehouse provision
// lifecycle events from a warehouse record. Mirrors the properties the admin
// API attaches to warehouse_provision_begin so success/failed can be broken
// down the same way. Only metadata — never credentials or secret values.
func provisionEventProps(w *configstore.ManagedWarehouse) map[string]any {
	return map[string]any{
		"metadata_store":   string(w.MetadataStore.Kind),
		"ducklake_enabled": w.DuckLake.Enabled,
	}
}

// captureProvisionFailed emits warehouse_provision_failed with a stable reason
// category, but only when the terminal Failed transition actually landed
// (updateErr == nil). Emitting on a failed CAS would let a stuck warehouse
// re-fire the event every reconcile tick.
func captureProvisionFailed(w *configstore.ManagedWarehouse, reason string, updateErr error) {
	if updateErr != nil {
		return
	}
	props := provisionEventProps(w)
	props["reason"] = reason
	analytics.Default().Capture("warehouse_provision_failed", w.OrgID, props)
}

// ducklingCRName returns the Duckling CR name for a warehouse row: the stored
// duckling_name, which is authoritative (the control plane never derives it).
// The column is NOT NULL and backfilled in the database, so the org-ID
// fallback only covers zero-value test fixtures and legacy in-flight rows
// constructed in memory before the column existed.
func ducklingCRName(w *configstore.ManagedWarehouse) string {
	if w.DucklingName != "" {
		return w.DucklingName
	}
	return w.OrgID
}

// WarehouseStore is the subset of configstore.ConfigStore that the controller needs.
type WarehouseStore interface {
	ListWarehousesByStates(states []configstore.ManagedWarehouseProvisioningState) ([]configstore.ManagedWarehouse, error)
	UpdateWarehouseState(orgID string, expectedState configstore.ManagedWarehouseProvisioningState, updates map[string]interface{}) error
}

// MetadataProbe is the signature for an end-to-end metadata-store probe. The
// production implementation is ProbeMetadataStore; tests inject a stub.
type MetadataProbe func(ctx context.Context, endpoint, user, password, database, sslMode string) error

// Controller polls the config store for actionable warehouses and reconciles
// their state against Duckling CRs in Kubernetes.
type Controller struct {
	store        WarehouseStore
	duckling     *DucklingClient
	pollInterval time.Duration
	probe        MetadataProbe

	// bucketSuffix is the env suffix for CP-owned s3bucket naming
	// (DUCKGRES_DUCKLING_BUCKET_SUFFIX, e.g. "mw-prod-us"). When set, the
	// controller backfills spec.dataStore.bucketName onto ready s3bucket
	// ducklings that predate CP-owned naming. Empty ⇒ backfill is a no-op and
	// the composition keeps deriving the name.
	bucketSuffix string

	// cnpgShardFieldUnsupported latches when a cnpg-shard backfill read-back
	// shows the API server pruned spec.metadataStore.cnpgShard — i.e. the
	// cluster's Duckling XRD predates the field. Cluster-wide condition, so
	// one latch disables the backfill (not per-org) until restart; the
	// reconcile loop is single-goroutine so no locking. See reconcileCnpgShard.
	cnpgShardFieldUnsupported bool
}

// NewController creates a provisioning controller. Returns an error if the
// Kubernetes client cannot be initialized (e.g., not running in-cluster).
func NewController(store WarehouseStore, pollInterval time.Duration) (*Controller, error) {
	dc, err := NewDucklingClient()
	if err != nil {
		return nil, fmt.Errorf("create duckling client: %w", err)
	}
	return &Controller{
		store:        store,
		duckling:     dc,
		pollInterval: pollInterval,
		probe:        ProbeMetadataStore,
	}, nil
}

// NewControllerWithClient creates a Controller with a pre-built DucklingClient (for testing).
func NewControllerWithClient(store WarehouseStore, dc *DucklingClient, pollInterval time.Duration) *Controller {
	return &Controller{
		store:        store,
		duckling:     dc,
		pollInterval: pollInterval,
		probe:        ProbeMetadataStore,
	}
}

// SetProbe overrides the metadata-store probe. Used by tests.
func (c *Controller) SetProbe(p MetadataProbe) {
	c.probe = p
}

// DucklingClient exposes the controller's Duckling CR client so co-located
// components (the reshard runner) share one client instead of re-resolving
// in-cluster config.
func (c *Controller) DucklingClient() *DucklingClient {
	return c.duckling
}

// WithBucketSuffix sets the env suffix used to compute and backfill the
// CP-owned per-org s3bucket name onto existing ready ducklings. Empty leaves
// backfill disabled (composition keeps deriving). Returns the controller for
// chaining.
func (c *Controller) WithBucketSuffix(suffix string) *Controller {
	c.bucketSuffix = suffix
	return c
}

// Run starts the reconciliation loop. Blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	slog.Info("Provisioning controller started.", "poll_interval", c.pollInterval)
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	// Run once immediately at startup
	c.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Provisioning controller stopped.")
			return
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

// actionableStates are the warehouse states the controller acts on.
// Ready is included so we can drift-correct user-mutable CR fields (today
// just pgbouncer.enabled) without waiting for the Duckling to be recreated.
var actionableStates = []configstore.ManagedWarehouseProvisioningState{
	configstore.ManagedWarehouseStatePending,
	configstore.ManagedWarehouseStateProvisioning,
	configstore.ManagedWarehouseStateReady,
	configstore.ManagedWarehouseStateDeleting,
}

func (c *Controller) reconcile(ctx context.Context) {
	warehouses, err := c.store.ListWarehousesByStates(actionableStates)
	if err != nil {
		slog.Warn("Provisioning controller: failed to list warehouses.", "error", err)
		return
	}

	for _, w := range warehouses {
		if ctx.Err() != nil {
			return
		}
		switch w.State {
		case configstore.ManagedWarehouseStatePending:
			c.reconcilePending(ctx, &w)
		case configstore.ManagedWarehouseStateProvisioning:
			c.reconcileProvisioning(ctx, &w)
		case configstore.ManagedWarehouseStateReady:
			c.reconcileReady(ctx, &w)
		case configstore.ManagedWarehouseStateDeleting:
			c.reconcileDeleting(ctx, &w)
		}
	}
}

func (c *Controller) reconcilePending(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "pending")

	now := time.Now().UTC()

	// Check if a Duckling CR already exists (e.g., controller restart)
	_, err := c.duckling.Get(ctx, ducklingCRName(w))
	if err == nil {
		// CR exists — transition directly to provisioning
		log.Info("Duckling CR already exists, transitioning to provisioning.")
		if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStatePending, map[string]interface{}{
			"state":                   configstore.ManagedWarehouseStateProvisioning,
			"status_message":          "Provisioning in progress...",
			"provisioning_started_at": now,
		}); err != nil {
			log.Warn("Failed to update state to provisioning.", "error", err)
		}
		return
	}

	// Create the Duckling CR
	log.Info("Creating Duckling CR.",
		"pgbouncer_enabled", w.PgBouncer.Enabled)
	if err := c.duckling.Create(ctx, ducklingCRName(w), CreateOptions{
		MetadataStoreType:         w.MetadataStore.Kind,
		PgBouncerEnabled:          w.PgBouncer.Enabled,
		ExternalEndpoint:          w.MetadataStore.Endpoint,
		ExternalPasswordAWSSecret: w.MetadataStore.PasswordAWSSecret,
		ExternalUser:              w.MetadataStore.Username,
		ExternalDatabase:          w.MetadataStore.DatabaseName,
		DataStoreType:             w.DataStore.Kind,
		DataStoreBucket:           w.DataStore.BucketName,
		DataStoreRegion:           w.DataStore.Region,
		DuckLakeEnabled:           w.DuckLake.Enabled,
	}); err != nil {
		log.Error("Failed to create Duckling CR.", "error", err)
		_ = c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStatePending, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateFailed,
			"status_message": fmt.Sprintf("Failed to create Duckling CR: %v", err),
			"failed_at":      now,
		})
		return
	}

	if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStatePending, map[string]interface{}{
		"state":                   configstore.ManagedWarehouseStateProvisioning,
		"status_message":          "Provisioning in progress...",
		"provisioning_started_at": now,
	}); err != nil {
		log.Warn("Failed to update state to provisioning.", "error", err)
	}
}

func (c *Controller) reconcileProvisioning(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "provisioning")

	// Use ProvisioningStartedAt if set (tracks when we entered provisioning state),
	// fall back to CreatedAt for warehouses created before this field existed.
	startedAt := w.CreatedAt
	if w.ProvisioningStartedAt != nil {
		startedAt = *w.ProvisioningStartedAt
	}

	// Check for timeout (30 minutes)
	if time.Since(startedAt) > 30*time.Minute {
		log.Warn("Provisioning timed out.")
		err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateFailed,
			"status_message": "Provisioning timed out after 30 minutes",
			"failed_at":      time.Now().UTC(),
		})
		captureProvisionFailed(w, "provisioning_timeout", err)
		return
	}

	status, err := c.duckling.Get(ctx, ducklingCRName(w))
	if err != nil {
		log.Warn("Failed to get Duckling CR status.", "error", err)
		return
	}

	// Check for Crossplane failure — only fail on persistent sync errors.
	// Crossplane resources commonly flap Synced=False transiently (e.g., IAM
	// eventual consistency, metadata-store endpoint DNS propagation), so we only transition
	// to failed if 10+ minutes have passed, giving transient errors time to resolve.
	if status.SyncedFalseMessage != "" && time.Since(startedAt) > 10*time.Minute {
		log.Warn("Crossplane sync failure.", "message", status.SyncedFalseMessage)
		err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateFailed,
			"status_message": fmt.Sprintf("Crossplane error: %s", status.SyncedFalseMessage),
			"failed_at":      time.Now().UTC(),
		})
		captureProvisionFailed(w, "crossplane_sync_failure", err)
		return
	}

	// Track per-component readiness. A component is ready only once the XR's
	// Ready condition is True — NOT merely because its status field
	// (bucketName, endpoint, password, iamRoleArn) is populated. The
	// composition sets those fields as soon as it *renders* the composed
	// resource, which happens well before that resource actually reconciles, so
	// keying readiness on field presence alone reported a component (notably S3)
	// green while its underlying managed resource was still failing — e.g. a
	// bucket stuck on BucketNotEmpty. Ready is Crossplane's aggregate "all
	// composed resources reconciled" signal, so gate on it and recompute every
	// tick: a component that regresses is revoked, not latched on.
	updates := map[string]interface{}{}

	setComponent := func(key string, current configstore.ManagedWarehouseProvisioningState, present bool) bool {
		desired := configstore.ManagedWarehouseStateProvisioning
		if present && status.ReadyCondition {
			desired = configstore.ManagedWarehouseStateReady
		}
		if current != desired {
			updates[key] = desired
		}
		return desired == configstore.ManagedWarehouseStateReady
	}

	s3Ready := setComponent("s3_state", w.S3State, status.DataStore.BucketName != "")
	metaReady := setComponent("metadata_store_state", w.MetadataStoreState, status.MetadataStore.Endpoint != "")
	secretsReady := setComponent("secrets_state", w.SecretsState, status.MetadataStore.Password != "")
	identReady := setComponent("identity_state", w.IdentityState, status.IAMRoleARN != "")

	if s3Ready && metaReady && secretsReady && identReady && status.ReadyCondition {
		// End-to-end probe: AWS reports the metadata store (RDS) Available before
		// its DNS record has propagated to in-cluster CoreDNS, and even longer
		// before pgbouncer's resolver picks it up. Flipping to Ready on
		// AWS-Available alone produces a 3-5 minute window where worker
		// activations fail with "DuckLake migration check failed → DNS
		// lookup failed". Confirm the path workers will actually use works
		// before we tell the rest of the system the warehouse is usable.
		//
		// PgBouncer is opt-in per duckling: when enabled, workers connect
		// through the pgbouncer Service (plaintext); when disabled they
		// connect to the RDS endpoint directly (TLS required). The probe
		// has to match that — otherwise we'd be testing a path nobody uses.
		probe := c.probe
		if probe == nil {
			probe = ProbeMetadataStore
		}
		var probeEndpoint, probeSSLMode string
		if w.PgBouncer.Enabled {
			probeEndpoint = status.MetadataStore.PgBouncerEndpoint
			probeSSLMode = "disable"
		} else {
			probeEndpoint = status.MetadataStore.Endpoint
			probeSSLMode = "require"
		}
		if err := probe(ctx,
			probeEndpoint,
			status.MetadataStore.User,
			status.MetadataStore.Password,
			status.MetadataStore.Database,
			probeSSLMode,
		); err != nil {
			log.Info("Infrastructure provisioned but end-to-end probe still failing — staying in provisioning.",
				"pgbouncer_enabled", w.PgBouncer.Enabled, "error", err)
			updates["status_message"] = "Provisioning in progress..."
		} else {
			now := time.Now().UTC()
			updates["state"] = configstore.ManagedWarehouseStateReady
			updates["status_message"] = "Infrastructure ready"
			updates["ready_at"] = now
			log.Info("Infrastructure ready, transitioning to ready.", "pgbouncer_enabled", w.PgBouncer.Enabled)
		}
	} else if !status.ReadyCondition {
		// Still provisioning and the XR is not Ready — surface *why*. Prefer the
		// concrete provider error from a failing composed resource (e.g. the S3
		// bucket's BucketNotEmpty) over the XR's generic "Unready resources:
		// <name>" roll-up, so the admin UI shows the actual blocker instead of a
		// stale message. Guarded to the not-Ready branch so it never overwrites
		// the probe-wait / ready messages set above.
		if msg := c.diagnoseNotReady(ctx, w, status); msg != "" && msg != w.StatusMessage {
			updates["status_message"] = msg
		}
	}

	if len(updates) > 0 {
		if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, updates); err != nil {
			log.Warn("Failed to update warehouse state.", "error", err)
		} else if updates["state"] == configstore.ManagedWarehouseStateReady {
			// Terminal success: the warehouse just flipped to Ready and is now
			// usable. Guarded on a successful CAS from Provisioning, so this
			// fires exactly once — the next tick routes a Ready warehouse to
			// reconcileReady, not here.
			analytics.Default().Capture("warehouse_provision_success", w.OrgID, provisionEventProps(w))
		}
	}

}

// statusMessageMaxLen bounds the diagnostic status message so a verbose provider
// error can't overflow the status_message column (VARCHAR(1024)).
const statusMessageMaxLen = 1000

// diagnoseNotReady builds a human-facing explanation of why a still-provisioning
// warehouse is not Ready. It asks Kubernetes for the failing composed resources
// and joins their provider errors (the actual blocker, e.g. a bucket's
// BucketNotEmpty); if none report an error — or the lookup fails — it falls back
// to the XR's own Ready-condition roll-up ("Unready resources: <name>"). Returns
// "" when there is nothing more specific to say.
func (c *Controller) diagnoseNotReady(ctx context.Context, w *configstore.ManagedWarehouse, status *DucklingStatus) string {
	errs, err := c.duckling.ComposedResourceErrors(ctx, ducklingCRName(w))
	if err == nil && len(errs) > 0 {
		parts := make([]string, 0, len(errs))
		for _, e := range errs {
			parts = append(parts, fmt.Sprintf("%s %s: %s", e.Kind, e.Name, e.Message))
		}
		return truncate(strings.Join(parts, "; "), statusMessageMaxLen)
	}
	return truncate(status.ReadyFalseMessage, statusMessageMaxLen)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

// reconcileReady handles drift correction for Ready warehouses. The
// post-create-mutable spec field is metadataStore.pgbouncer.enabled; if an
// operator flips it in the config store (admin API), we patch the CR so the
// Crossplane composition provisions / tears down the affected resource.
//
// Scope is intentionally narrow: we do NOT reconcile ACU, image, or other
// spec fields. Those aren't user-mutable via the admin API today, and
// aggressive drift correction would conflict with manual kubectl patches.
func (c *Controller) reconcileReady(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "ready")

	currentPgB, err := c.duckling.GetPgBouncerEnabled(ctx, ducklingCRName(w))
	if err != nil {
		if apierrors.IsNotFound(err) {
			// CR is gone but the warehouse is still marked Ready. Don't try
			// to patch; leave the state machine to catch up.
			log.Warn("Duckling CR not found for Ready warehouse — skipping drift check.")
			return
		}
		log.Warn("Failed to read Duckling CR for drift check.", "error", err)
		return
	}

	if currentPgB != w.PgBouncer.Enabled {
		log.Info("PgBouncer drift detected, patching Duckling CR.",
			"desired", w.PgBouncer.Enabled, "current", currentPgB)
		if err := c.duckling.SetPgBouncerEnabled(ctx, ducklingCRName(w), w.PgBouncer.Enabled); err != nil {
			log.Warn("Failed to patch Duckling CR pgbouncer.enabled.", "error", err)
		}
	}

	// Backfill the CP-owned s3bucket name onto ducklings provisioned before the
	// control plane started supplying it (spec.dataStore carries only
	// {type: s3bucket}). The name computed here is exactly what the composition
	// has been deriving into status, so the patch moves it from a derived output
	// into a durable input without changing the actual bucket. No-op once set.
	c.reconcileBucketName(ctx, w, log)

	// Backfill the composition-pinned cnpg shard onto the Duckling spec
	// (spec.metadataStore.cnpgShard) for cnpg-shard ducklings provisioned
	// before the field existed. Same derived-output → durable-input move as
	// the bucket name: the value written is exactly the CR's own
	// status.metadataStore.assignedShard, so the composition re-renders
	// identically and nothing moves. Once every duckling carries it, the
	// shard a tenant lives on is explicit, schema-validated spec — the
	// prerequisite for shard-migration cutovers, which patch this field to a
	// DIFFERENT shard (an explicitly operator-driven step, never done here).
	c.reconcileCnpgShard(ctx, w, log)

	// Mirror the CR-status metadata-store connection details into the
	// warehouse row so machine consumers (the provisioning API's discovery
	// endpoints) can serve them without a Kubernetes dependency. Same
	// derived-output → durable-copy move as the two backfills above.
	c.reconcileMetadataStoreRow(ctx, w, log)
}

// metadataStoreRowUpdates computes the drift between the warehouse row's
// metadata-store block and the Duckling CR status — the row is a MIRROR of
// the CR for cnpg tenants (the provision handler writes only the kind; the
// composition picks the shard and publishes endpoint/database/user + the
// credential Secret ref in status). Pure function so the sync logic is
// testable without a cluster.
//
// external tenants: endpoint/database/user are provision-time INPUTS on the
// row — never overwritten from status; only the credential Secret ref (which
// has no provisioning-time writer for either kind) is mirrored.
//
// Empty status fields are never synced over non-empty row values, and an
// empty map means "no drift" — the caller must skip the write entirely so a
// steady-state reconcile tick doesn't bump updated_at (which would churn the
// discovery config_generation every tick).
//
// A status whose metadata-store TYPE contradicts the row kind (both
// non-empty) produces no updates at all: that's identity drift (the condition
// recordSource refuses a reshard over), and mirroring across it would stamp
// one store's connection details onto a row claiming the other — worse than
// serving nothing. Either side empty is "unknown", not "mismatched" — older
// compositions publish no status type, and a kind-less row must not silently
// strand the secret-ref mirror.
func metadataStoreRowUpdates(w *configstore.ManagedWarehouse, status *DucklingStatus) map[string]interface{} {
	updates := map[string]interface{}{}

	if status.MetadataStore.Type != "" && w.MetadataStore.Kind != "" &&
		status.MetadataStore.Type != w.MetadataStore.Kind {
		return updates
	}

	if w.MetadataStore.Kind == configstore.MetadataStoreKindCnpgShard && status.MetadataStore.Endpoint != "" {
		if w.MetadataStore.Endpoint != status.MetadataStore.Endpoint {
			updates["metadata_store_endpoint"] = status.MetadataStore.Endpoint
		}
		if status.MetadataStore.Database != "" && w.MetadataStore.DatabaseName != status.MetadataStore.Database {
			updates["metadata_store_database_name"] = status.MetadataStore.Database
		}
		if status.MetadataStore.User != "" && w.MetadataStore.Username != status.MetadataStore.User {
			updates["metadata_store_username"] = status.MetadataStore.User
		}
		if w.MetadataStore.Port == 0 {
			// The CR status carries no port; cnpg pooler serves 5432.
			updates["metadata_store_port"] = 5432
		}
	}

	ref := status.MetadataCredentialSecretRef
	if ref.Name != "" {
		if ref.Namespace == "" {
			// The composition publishes same-namespace refs without an
			// explicit namespace; Duckling CRs live in "ducklings". A
			// non-ducklings namespace, if ever published, is mirrored
			// verbatim — consumers' RBAC fails closed on it.
			ref.Namespace = ducklingNamespace
		}
		if ref.Key == "" {
			// Key-less refs are a real mid-migration shape (#972's harness
			// defaults them the same way); every credential secret uses the
			// conventional "password" key.
			ref.Key = "password"
		}
		if w.MetadataStoreSecretRef.Namespace != ref.Namespace {
			updates["metadata_store_secret_ref_namespace"] = ref.Namespace
		}
		if w.MetadataStoreSecretRef.Name != ref.Name {
			updates["metadata_store_secret_ref_name"] = ref.Name
		}
		if w.MetadataStoreSecretRef.Key != ref.Key {
			updates["metadata_store_secret_ref_key"] = ref.Key
		}
	}
	return updates
}

// reconcileMetadataStoreRow syncs the row's metadata-store mirror from CR
// status. Runs ONLY from reconcileReady, and the write itself is the
// state-CAS UpdateWarehouseState(…, Ready, …) — so a warehouse that left
// ready mid-tick (a reshard starting) is never written: during resharding
// the reshard runner has exclusive ownership of these columns, and the
// post-flip transition back to ready lets the next tick mirror the new
// store.
func (c *Controller) reconcileMetadataStoreRow(ctx context.Context, w *configstore.ManagedWarehouse, log *slog.Logger) {
	// Unresolved read: the mirror needs connection fields and the Secret
	// REFERENCE only — resolving would add a Secret GET (and a plaintext
	// credential read) per ready tick for no consumer.
	status, err := c.duckling.GetStatusUnresolved(ctx, ducklingCRName(w))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		log.Warn("Failed to read Duckling CR status for metadata-store row sync.", "error", err)
		return
	}

	if status.MetadataStore.Type != "" && w.MetadataStore.Kind != "" &&
		status.MetadataStore.Type != w.MetadataStore.Kind {
		// The pure function suppresses all updates on this contradiction;
		// surface it — it's the same identity drift recordSource refuses a
		// reshard over, and it needs an operator, not silence.
		log.Warn("Metadata-store identity drift: CR status type contradicts the row kind — mirror suppressed.",
			"status_type", status.MetadataStore.Type, "row_kind", w.MetadataStore.Kind)
		return
	}

	updates := metadataStoreRowUpdates(w, status)
	if len(updates) == 0 {
		return
	}
	log.Info("Mirroring metadata-store connection details from CR status into the warehouse row.",
		"fields", len(updates))
	if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateReady, updates); err != nil {
		if errors.Is(err, configstore.ErrWarehouseStateMismatch) {
			// Left ready mid-tick (reshard starting) — the runner owns the
			// columns now; the post-flip tick will re-sync.
			return
		}
		log.Warn("Failed to mirror metadata-store details into the warehouse row.", "error", err)
	}
}

// reconcileBucketName backfills spec.dataStore.bucketName (and the config-store
// row) with the control-plane-owned per-org bucket name for ready s3bucket
// ducklings. Idempotent and self-limiting: a no-op when no suffix is
// configured, when the data store isn't s3bucket, or once the CR already
// carries the (matching) name. Never overwrites a CR that already has a
// DIFFERENT explicit name — that would be an operator-set override we must not
// stomp.
func (c *Controller) reconcileBucketName(ctx context.Context, w *configstore.ManagedWarehouse, log *slog.Logger) {
	if c.bucketSuffix == "" {
		return
	}
	// Empty Kind is the historical default for the s3bucket path; "external"
	// carries its own bucket name and must be left alone.
	if w.DataStore.Kind != "" && w.DataStore.Kind != "s3bucket" {
		return
	}
	desired := configstore.DucklingBucketName(w.OrgID, c.bucketSuffix)
	if desired == "" {
		return
	}

	current, err := c.duckling.GetDataStoreBucketName(ctx, ducklingCRName(w))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		log.Warn("Failed to read Duckling CR dataStore.bucketName for backfill.", "error", err)
		return
	}
	if current != "" && current != desired {
		log.Warn("Duckling CR dataStore.bucketName differs from the computed name; leaving it.",
			"cr", current, "computed", desired)
		return
	}

	if current == "" {
		log.Info("Backfilling CP-owned bucket name onto Duckling CR.", "bucket", desired)
		if err := c.duckling.SetDataStoreBucketName(ctx, ducklingCRName(w), desired); err != nil {
			log.Warn("Failed to patch Duckling CR dataStore.bucketName.", "error", err)
			return
		}
	}

	// Mirror the authoritative name onto the config-store row so the provision
	// response and warehouse status surface it without re-deriving.
	if w.DataStore.BucketName != desired {
		if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateReady, map[string]interface{}{
			"data_store_bucket_name": desired,
		}); err != nil {
			log.Warn("Failed to persist dataStore bucket name to config store.", "error", err)
		}
	}
}

// reconcileCnpgShard backfills spec.metadataStore.cnpgShard with the CR's own
// pinned status.metadataStore.assignedShard for ready cnpg-shard ducklings
// that don't carry the field yet. Idempotent and self-limiting: a no-op for
// non-cnpg metadata stores, while the pin isn't stamped yet, and once the
// spec carries ANY value — an existing (different) spec value is an
// operator-set migration override we must never stomp.
//
// The read-back after the patch detects a cluster whose Duckling XRD predates
// the field (the API server then silently prunes it): that disables the
// backfill for the rest of this controller's lifetime — cluster-wide
// condition, one WARN, no per-tick patch churn.
func (c *Controller) reconcileCnpgShard(ctx context.Context, w *configstore.ManagedWarehouse, log *slog.Logger) {
	if c.cnpgShardFieldUnsupported {
		return
	}
	if w.MetadataStore.Kind != configstore.MetadataStoreKindCnpgShard {
		return
	}

	specShard, assignedShard, err := c.duckling.GetCnpgShardState(ctx, ducklingCRName(w))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		log.Warn("Failed to read Duckling CR cnpg shard state for backfill.", "error", err)
		return
	}
	if specShard != "" {
		// Already backfilled, or an operator-set migration override.
		return
	}
	if assignedShard == "" {
		// Composition hasn't stamped the pin yet.
		return
	}

	log.Info("Backfilling pinned cnpg shard onto Duckling CR spec.", "shard", assignedShard)
	if err := c.duckling.SetMetadataStoreCnpgShard(ctx, ducklingCRName(w), assignedShard); err != nil {
		log.Warn("Failed to patch Duckling CR metadataStore.cnpgShard.", "error", err)
		return
	}
	readBack, _, err := c.duckling.GetCnpgShardState(ctx, ducklingCRName(w))
	if err != nil {
		log.Warn("Failed to read back Duckling CR cnpg shard after backfill.", "error", err)
		return
	}
	if readBack != assignedShard {
		c.cnpgShardFieldUnsupported = true
		log.Warn("Duckling XRD pruned spec.metadataStore.cnpgShard — the cluster's XRD predates the field; disabling the shard backfill until this control plane restarts.",
			"wrote", assignedShard, "readBack", readBack)
	}
}

func (c *Controller) reconcileDeleting(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "deleting")

	log.Info("Deleting Duckling CR.")
	if err := c.duckling.Delete(ctx, ducklingCRName(w)); err != nil {
		// Only proceed if the CR is already gone (NotFound). For other errors
		// (network, RBAC, etc.) we retry on the next reconcile pass to avoid
		// marking as deleted while AWS resources still exist.
		if !apierrors.IsNotFound(err) {
			log.Warn("Failed to delete Duckling CR, will retry.", "error", err)
			// Deletion has no terminal Failed state (unlike provisioning) — the
			// controller retries indefinitely. So warehouse_deprovision_failed
			// signals "a teardown attempt failed, will retry", and may fire once
			// per reconcile pass until the teardown succeeds. See the README
			// events table.
			analytics.Default().Capture("warehouse_deprovision_failed", w.OrgID, map[string]any{
				"reason": "duckling_delete_failed",
			})
			return
		}
	}

	if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateDeleting, map[string]interface{}{
		"state":          configstore.ManagedWarehouseStateDeleted,
		"status_message": "Resources deleted",
	}); err != nil {
		log.Warn("Failed to update state to deleted.", "error", err)
	} else {
		// Terminal success: all underlying resources are gone. Guarded on a
		// successful CAS from Deleting, so this fires exactly once.
		analytics.Default().Capture("warehouse_deprovision_success", w.OrgID, nil)
	}
}
