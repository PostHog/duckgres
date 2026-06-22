//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
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

	// Lakekeeper-side reconcile dependencies. All optional: if any is nil
	// (e.g. in older deployments or unit tests that don't exercise the
	// Lakekeeper path), reconcileLakekeeper is skipped silently.
	lakekeeperProvisioner *LakekeeperProvisioner
	lakekeeperInputs      LakekeeperInputsResolver

	// bucketSuffix is the env suffix for CP-owned s3bucket naming
	// (DUCKGRES_DUCKLING_BUCKET_SUFFIX, e.g. "mw-prod-us"). When set, the
	// controller backfills spec.dataStore.bucketName onto ready s3bucket
	// ducklings that predate CP-owned naming. Empty ⇒ backfill is a no-op and
	// the composition keeps deriving the name.
	bucketSuffix string
}

// LakekeeperInputsResolver is the function shape the controller uses to
// build ProvisioningInputs for a given warehouse. Resolves things the
// configstore doesn't carry directly: the admin Postgres DSN (from a
// K8s Secret managed by Crossplane), the PG host the Lakekeeper pod uses
// to reach the cluster, and the S3 storage profile.
//
// Defined as a function rather than a method on Controller so tests can
// substitute a fake and so the convention for sourcing the admin DSN
// (Crossplane-emitted Secret name pattern) lives outside this package.
type LakekeeperInputsResolver func(ctx context.Context, w *configstore.ManagedWarehouse) (ProvisioningInputs, error)

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

// WithLakekeeperProvisioner enables the Lakekeeper reconcile branch.
// Both p and inputs must be non-nil — partial wiring would silently
// disable the reconcile step and that's a misconfiguration we'd rather
// surface at startup than at the first activation.
func (c *Controller) WithLakekeeperProvisioner(p *LakekeeperProvisioner, inputs LakekeeperInputsResolver) *Controller {
	if p == nil {
		panic("WithLakekeeperProvisioner: provisioner is nil; call NewLakekeeperProvisioner first")
	}
	if inputs == nil {
		panic("WithLakekeeperProvisioner: inputs resolver is nil")
	}
	c.lakekeeperProvisioner = p
	c.lakekeeperInputs = inputs
	return c
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
		"pgbouncer_enabled", w.PgBouncer.Enabled,
		"iceberg_enabled", w.Iceberg.Enabled)
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
		IcebergEnabled:            w.Iceberg.Enabled,
		IcebergNamespace:          w.Iceberg.Namespace,
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
	// eventual consistency, metadata-store endpoint DNS propagation), so we only transition
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

	// Iceberg readiness for the Lakekeeper backend is owned by
	// reconcileLakekeeper, which writes iceberg_state=Ready directly once
	// the per-org Lakekeeper warehouse is provisioned. Nothing here needs
	// to propagate from the Crossplane Duckling status — the Lakekeeper
	// provisioner is the source of truth.

	// Infrastructure is ready when all components are provisioned AND the
	// Crossplane Ready condition is True. The Ready condition ensures all
	// composed resources (including the metadata store) are fully reconciled,
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
		}
	}

	// Provision the per-org Lakekeeper as part of turn-up, not only as a
	// post-Ready late-enable. A cnpg-shard Duckling is required by the XRD to
	// have iceberg.enabled=true, so its warehouse can never reach Ready until
	// iceberg_state flips — and for the Lakekeeper backend that only happens
	// once EnsureForOrg has stood the catalog up (there's no S3 Tables bucket
	// whose ARN would otherwise trigger it). reconcileLakekeeper is idempotent,
	// no-ops for non-Lakekeeper backends, and returns quietly until the
	// Duckling status carries the metadata creds and the Lakekeeper CR has
	// bootstrapped; the poll loop is the requeue. The iceberg_state=Ready it
	// writes is picked up by the readiness check on a subsequent tick.
	c.reconcileLakekeeper(ctx, w)
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

	// Backfill the CP-owned s3bucket name onto ducklings provisioned before the
	// control plane started supplying it (spec.dataStore carries only
	// {type: s3bucket}). The name computed here is exactly what the composition
	// has been deriving into status, so the patch moves it from a derived output
	// into a durable input without changing the actual bucket. No-op once set.
	c.reconcileBucketName(ctx, w, log)

	// Lakekeeper reconcile is independent of the Duckling state machine —
	// provisioning a Lakekeeper for an org doesn't depend on, and doesn't
	// block, the warehouse top-level state.
	c.reconcileLakekeeper(ctx, w)
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

// reconcileLakekeeper provisions a per-org Lakekeeper instance when the
// warehouse selects the lakekeeper backend and isn't yet provisioned. Called
// from both reconcileProvisioning (initial turn-up — required for cnpg-shard,
// which must have iceberg enabled and so can't reach Ready until the catalog
// is up) and reconcileReady (late-enable on an already-Ready warehouse).
// Idempotent: a warehouse with LakekeeperEndpoint already populated is a
// no-op. ErrBootstrapPending from the underlying EnsureForOrg is logged
// at debug and treated as "retry on the next tick" — the controller's
// poll loop is the requeue mechanism.
//
// Skipped silently when the controller wasn't built with
// WithLakekeeperProvisioner (e.g. in deployments where the operator
// isn't installed, or in tests).
func (c *Controller) reconcileLakekeeper(ctx context.Context, w *configstore.ManagedWarehouse) {
	if c.lakekeeperProvisioner == nil || c.lakekeeperInputs == nil {
		return
	}
	if !w.Iceberg.Enabled {
		return
	}
	if w.Iceberg.ResolvedBackend() != configstore.IcebergBackendLakekeeper {
		return
	}
	log := slog.With("org", w.OrgID, "phase", "lakekeeper")

	if w.Iceberg.LakekeeperEndpoint != "" {
		// Already provisioned. Converge the pod-shape fields (replicas, resource
		// requests, scrape annotations) onto the org's existing CR(s) via a
		// label-matched merge patch — no inputs needed, never recreates under a
		// new name. The operator rolls the Deployment only when the spec actually
		// changes. (The operator can't add fields that aren't in the CR, so a
		// spec change in the provisioner must be written back here — it won't
		// appear on its own.)
		if err := c.lakekeeperProvisioner.PatchPodShape(ctx, w.OrgID); err != nil {
			log.Warn("Lakekeeper CR pod-shape drift correction failed.", "error", err)
		}
		return
	}

	inputs, err := c.lakekeeperInputs(ctx, w)
	if err != nil {
		log.Warn("Failed to resolve lakekeeper provisioning inputs.", "error", err)
		return
	}
	if err := c.lakekeeperProvisioner.EnsureForOrg(ctx, w, inputs); err != nil {
		if errors.Is(err, ErrBootstrapPending) {
			log.Debug("Lakekeeper bootstrap still pending; next tick will retry.")
			return
		}
		log.Warn("Lakekeeper provisioning failed.", "error", err)
		return
	}
	log.Info("Lakekeeper provisioning completed.")
}

func (c *Controller) reconcileDeleting(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "deleting")

	// Resolve the Lakekeeper inputs BEFORE deleting the Duckling CR. The
	// inputs include the metadata-store admin DSN derived from the CR's
	// status; once the CR is gone the resolver can't reconstruct it, and
	// we'd lose the only way to drop the per-tenant lakekeeper_<org>
	// Postgres database. Best-effort resolution: when the inputs aren't
	// available (resolver unwired, CR never reconciled, dev/orbstack
	// without env config), the subsequent DeleteForOrg call falls back to
	// k8s-only teardown.
	var lkInputs ProvisioningInputs
	if c.lakekeeperProvisioner != nil && c.lakekeeperInputs != nil {
		if in, err := c.lakekeeperInputs(ctx, w); err != nil {
			log.Debug("Lakekeeper inputs unavailable at delete time; skipping PG cleanup.", "error", err)
		} else {
			lkInputs = in
		}
	}

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

	// Tear down the per-org Lakekeeper instance the control plane provisioned
	// out-of-band (CR + Secret + ServiceAccount in the lakekeeper namespace,
	// and — when this provisioner created them — the lakekeeper_<org>
	// Postgres database and role on the metadata store). The Crossplane
	// Duckling composition doesn't own the k8s pieces, so without an explicit
	// teardown they leak after the warehouse is gone. Idempotent and
	// NotFound-tolerant — a clean no-op for ducklings that never enabled
	// Iceberg. Skipped silently when the provisioner isn't wired (mirrors
	// reconcileLakekeeper). On error we return without marking the warehouse
	// deleted so the next reconcile pass retries.
	if c.lakekeeperProvisioner != nil {
		if err := c.lakekeeperProvisioner.DeleteForOrg(ctx, w.OrgID, lkInputs); err != nil {
			log.Warn("Failed to tear down Lakekeeper resources, will retry.", "error", err)
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
