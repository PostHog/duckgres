//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"log/slog"
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
	_, err := c.duckling.Get(ctx, w.OrgID)
	if err == nil {
		// CR exists — transition directly to provisioning
		log.Info("Duckling CR already exists, transitioning to provisioning.")
		if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStatePending, map[string]interface{}{
			"state":                   configstore.ManagedWarehouseStateProvisioning,
			"status_message":          "Duckling CR exists, polling status",
			"provisioning_started_at": now,
		}); err != nil {
			log.Warn("Failed to update state to provisioning.", "error", err)
		}
		return
	}

	// Create the Duckling CR
	log.Info("Creating Duckling CR.",
		"pgbouncer_enabled", w.PgBouncer.Enabled)
	if err := c.duckling.Create(ctx, w.OrgID, CreateOptions{
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
		"status_message":          "Duckling CR created, waiting for resources",
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

	status, err := c.duckling.Get(ctx, w.OrgID)
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

	// Track per-component readiness based on Duckling CR status fields.
	// Infrastructure details (endpoint, password, bucket, etc.) are read directly
	// from the Duckling CR by the worker activator at activation time — only
	// state transitions are stored in the config store.
	updates := map[string]interface{}{}

	if status.DataStore.BucketName != "" && w.S3State != configstore.ManagedWarehouseStateReady {
		updates["s3_state"] = configstore.ManagedWarehouseStateReady
	}

	if status.MetadataStore.Endpoint != "" && w.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		updates["metadata_store_state"] = configstore.ManagedWarehouseStateReady
	}

	if status.MetadataStore.Password != "" && w.SecretsState != configstore.ManagedWarehouseStateReady {
		updates["secrets_state"] = configstore.ManagedWarehouseStateReady
	}

	if status.IAMRoleARN != "" && w.IdentityState != configstore.ManagedWarehouseStateReady {
		updates["identity_state"] = configstore.ManagedWarehouseStateReady
	}

	// Infrastructure is ready when all components are provisioned AND the
	// Crossplane Ready condition is True. The Ready condition ensures all
	// composed resources (including the metadata store) are fully reconciled,
	// not just that individual status fields are populated.
	s3Ready := w.S3State == configstore.ManagedWarehouseStateReady || updates["s3_state"] == configstore.ManagedWarehouseStateReady
	metaReady := w.MetadataStoreState == configstore.ManagedWarehouseStateReady || updates["metadata_store_state"] == configstore.ManagedWarehouseStateReady
	secretsReady := w.SecretsState == configstore.ManagedWarehouseStateReady || updates["secrets_state"] == configstore.ManagedWarehouseStateReady
	identReady := w.IdentityState == configstore.ManagedWarehouseStateReady || updates["identity_state"] == configstore.ManagedWarehouseStateReady

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
			updates["status_message"] = fmt.Sprintf("Waiting for metadata store reachability: %v", err)
		} else {
			now := time.Now().UTC()
			updates["state"] = configstore.ManagedWarehouseStateReady
			updates["status_message"] = "Infrastructure ready"
			updates["ready_at"] = now
			log.Info("Infrastructure ready, transitioning to ready.", "pgbouncer_enabled", w.PgBouncer.Enabled)
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

	currentPgB, err := c.duckling.GetPgBouncerEnabled(ctx, w.OrgID)
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
		if err := c.duckling.SetPgBouncerEnabled(ctx, w.OrgID, w.PgBouncer.Enabled); err != nil {
			log.Warn("Failed to patch Duckling CR pgbouncer.enabled.", "error", err)
		}
	}

	// Backfill the CP-owned s3bucket name onto ducklings provisioned before the
	// control plane started supplying it (spec.dataStore carries only
	// {type: s3bucket}). The name computed here is exactly what the composition
	// has been deriving into status, so the patch moves it from a derived output
	// into a durable input without changing the actual bucket. No-op once set.
	c.reconcileBucketName(ctx, w, log)
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

	current, err := c.duckling.GetDataStoreBucketName(ctx, w.OrgID)
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
		if err := c.duckling.SetDataStoreBucketName(ctx, w.OrgID, desired); err != nil {
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

func (c *Controller) reconcileDeleting(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "deleting")

	log.Info("Deleting Duckling CR.")
	if err := c.duckling.Delete(ctx, w.OrgID); err != nil {
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
