//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// WarehouseStore is the subset of configstore.ConfigStore that the controller needs.
type WarehouseStore interface {
	ListWarehousesByStates(states []configstore.ManagedWarehouseProvisioningState) ([]configstore.ManagedWarehouse, error)
	UpdateWarehouseState(orgID string, expectedState configstore.ManagedWarehouseProvisioningState, updates map[string]interface{}) error
	// UpdateIcebergConfig writes per-org Iceberg/Lakekeeper config without
	// CAS'ing on the top-level warehouse state. The Lakekeeper provisioner
	// uses this because Iceberg provisioning runs in parallel with the
	// top-level Duckling state machine — by the time we're ready to persist
	// the Lakekeeper endpoint, the warehouse may already have transitioned
	// to Ready, and a state-CAS update would silently no-op.
	UpdateIcebergConfig(orgID string, updates map[string]interface{}) error
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
		"pgbouncer_enabled", w.PgBouncer.Enabled,
		"iceberg_enabled", w.Iceberg.Enabled)
	if err := c.duckling.Create(ctx, w.OrgID, CreateOptions{
		MinACU:           w.AuroraMinACU,
		MaxACU:           w.AuroraMaxACU,
		PgBouncerEnabled: w.PgBouncer.Enabled,
		IcebergEnabled:   w.Iceberg.Enabled,
		IcebergNamespace: w.Iceberg.Namespace,
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
		_ = c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateFailed,
			"status_message": "Provisioning timed out after 30 minutes",
			"failed_at":      time.Now().UTC(),
		})
		return
	}

	status, err := c.duckling.Get(ctx, w.OrgID)
	if err != nil {
		log.Warn("Failed to get Duckling CR status.", "error", err)
		return
	}

	// Check for Crossplane failure — only fail on persistent sync errors.
	// Crossplane resources commonly flap Synced=False transiently (e.g., IAM
	// eventual consistency, Aurora cold start delays), so we only transition
	// to failed if 10+ minutes have passed, giving transient errors time to resolve.
	if status.SyncedFalseMessage != "" && time.Since(startedAt) > 10*time.Minute {
		log.Warn("Crossplane sync failure.", "message", status.SyncedFalseMessage)
		_ = c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateFailed,
			"status_message": fmt.Sprintf("Crossplane error: %s", status.SyncedFalseMessage),
			"failed_at":      time.Now().UTC(),
		})
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

	// Iceberg is opt-in per warehouse — only track its state when enabled.
	// When the composition writes status.iceberg.tableBucketArn, the bucket
	// (Workspace) is reconciled and we persist the ARN + region back to the
	// configstore so the worker activator can feed it into IcebergConfig.
	addIcebergStatusUpdates(updates, w, status)

	// Infrastructure is ready when all components are provisioned AND the
	// Crossplane Ready condition is True. The Ready condition ensures all
	// composed resources (including the Aurora instance) are fully reconciled,
	// not just that individual status fields are populated.
	s3Ready := w.S3State == configstore.ManagedWarehouseStateReady || updates["s3_state"] == configstore.ManagedWarehouseStateReady
	metaReady := w.MetadataStoreState == configstore.ManagedWarehouseStateReady || updates["metadata_store_state"] == configstore.ManagedWarehouseStateReady
	secretsReady := w.SecretsState == configstore.ManagedWarehouseStateReady || updates["secrets_state"] == configstore.ManagedWarehouseStateReady
	identReady := w.IdentityState == configstore.ManagedWarehouseStateReady || updates["identity_state"] == configstore.ManagedWarehouseStateReady
	// Iceberg is only required for Ready when the tenant opted in.
	icebergReady := !w.Iceberg.Enabled ||
		w.IcebergState == configstore.ManagedWarehouseStateReady ||
		updates["iceberg_state"] == configstore.ManagedWarehouseStateReady

	if s3Ready && metaReady && secretsReady && identReady && icebergReady && status.ReadyCondition {
		// End-to-end probe: AWS reports the Aurora cluster Available before
		// its DNS record has propagated to in-cluster CoreDNS, and even longer
		// before pgbouncer's resolver picks it up. Flipping to Ready on
		// AWS-Available alone produces a 3-5 minute window where worker
		// activations fail with "DuckLake migration check failed → DNS
		// lookup failed". Confirm the path workers will actually use works
		// before we tell the rest of the system the warehouse is usable.
		//
		// PgBouncer is opt-in per duckling: when enabled, workers connect
		// through the pgbouncer Service (plaintext); when disabled they
		// connect to the Aurora endpoint directly (TLS required). The probe
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
		}
	}
}

// reconcileReady handles drift correction for Ready warehouses. The
// post-create-mutable spec fields are metadataStore.pgbouncer.enabled and
// iceberg.enabled; if an operator flips either in the config store (admin
// API), we patch the CR so the Crossplane composition provisions / tears
// down the affected resource.
//
// Scope is intentionally narrow: we do NOT reconcile ACU, image, or other
// spec fields. Those aren't user-mutable via the admin API today, and
// aggressive drift correction would conflict with manual kubectl patches.
//
// iceberg.namespace is NOT drift-corrected — the XRD's CEL admission rule
// rejects post-create namespace changes, so a drift attempt would just hit
// a 422 from the API server. Namespace changes require warehouse re-creation.
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

	currentIceberg, err := c.duckling.GetIcebergEnabled(ctx, w.OrgID)
	if err != nil {
		log.Warn("Failed to read Duckling CR iceberg.enabled for drift check.", "error", err)
		return
	}
	if currentIceberg != w.Iceberg.Enabled {
		log.Info("Iceberg drift detected, patching Duckling CR.",
			"desired", w.Iceberg.Enabled, "current", currentIceberg)
		if err := c.duckling.SetIcebergEnabled(ctx, w.OrgID, w.Iceberg.Enabled); err != nil {
			log.Warn("Failed to patch Duckling CR iceberg.enabled.", "error", err)
		}
	}

	// Propagate the Duckling's status.iceberg.tableBucketArn back to the
	// configstore even after the warehouse has transitioned to Ready. This
	// covers the late-enable case: the iceberg block can be flipped on via
	// the admin API long after the warehouse first became Ready, and the
	// Crossplane composition will only emit the bucket Workspace + populate
	// status.iceberg.tableBucketArn after that flip. Without this, the ARN
	// would only land in the configstore if the warehouse was still in
	// Provisioning when the composition completed — never on a re-enable.
	if w.Iceberg.Enabled {
		status, err := c.duckling.Get(ctx, w.OrgID)
		if err != nil {
			log.Warn("Failed to read Duckling status for iceberg propagation.", "error", err)
			return
		}
		updates := map[string]interface{}{}
		addIcebergStatusUpdates(updates, w, status)
		if len(updates) > 0 {
			if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateReady, updates); err != nil {
				log.Warn("Failed to persist iceberg status to configstore.", "error", err)
			} else {
				log.Info("Iceberg status persisted to configstore.", "updates", updates)
			}
		}
	}
}

// addIcebergStatusUpdates copies the Duckling's reported iceberg status
// (ARN, region, namespace) into the configstore update map when fields
// differ. Idempotent. Called from both reconcileProvisioning (initial
// turn-up) and reconcileReady (late iceberg enable on an existing
// warehouse). Without the reconcileReady call site, a warehouse that
// became Ready before iceberg was opted in would never get its ARN
// propagated to the configstore — the worker activator would then run
// AttachIcebergCatalog with an empty TableBucket and skip the attach.
func addIcebergStatusUpdates(updates map[string]interface{}, w *configstore.ManagedWarehouse, status *DucklingStatus) {
	if !w.Iceberg.Enabled || status.Iceberg.TableBucketArn == "" {
		return
	}
	if w.Iceberg.TableBucketArn != status.Iceberg.TableBucketArn {
		updates["iceberg_table_bucket_arn"] = status.Iceberg.TableBucketArn
	}
	if w.Iceberg.Region != status.Iceberg.Region && status.Iceberg.Region != "" {
		updates["iceberg_region"] = status.Iceberg.Region
	}
	if w.Iceberg.Namespace != status.Iceberg.NamespaceName && status.Iceberg.NamespaceName != "" {
		updates["iceberg_namespace"] = status.Iceberg.NamespaceName
	}
	if w.IcebergState != configstore.ManagedWarehouseStateReady {
		updates["iceberg_state"] = configstore.ManagedWarehouseStateReady
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
			return
		}
	}

	if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateDeleting, map[string]interface{}{
		"state":          configstore.ManagedWarehouseStateDeleted,
		"status_message": "Resources deleted",
	}); err != nil {
		log.Warn("Failed to update state to deleted.", "error", err)
	}
}
