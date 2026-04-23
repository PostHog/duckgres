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
}

// Controller polls the config store for actionable warehouses and reconciles
// their state against Duckling CRs in Kubernetes.
type Controller struct {
	store        WarehouseStore
	duckling     *DucklingClient
	pollInterval time.Duration
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
	}, nil
}

// NewControllerWithClient creates a Controller with a pre-built DucklingClient (for testing).
func NewControllerWithClient(store WarehouseStore, dc *DucklingClient, pollInterval time.Duration) *Controller {
	return &Controller{
		store:        store,
		duckling:     dc,
		pollInterval: pollInterval,
	}
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
	log.Info("Creating Duckling CR.", "pgbouncer_enabled", w.PgBouncer.Enabled)
	if err := c.duckling.Create(ctx, w.OrgID, CreateOptions{
		MinACU:           w.AuroraMinACU,
		MaxACU:           w.AuroraMaxACU,
		PgBouncerEnabled: w.PgBouncer.Enabled,
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

	// Infrastructure is ready when all components are provisioned AND the
	// Crossplane Ready condition is True. The Ready condition ensures all
	// composed resources (including the Aurora instance) are fully reconciled,
	// not just that individual status fields are populated.
	s3Ready := w.S3State == configstore.ManagedWarehouseStateReady || updates["s3_state"] == configstore.ManagedWarehouseStateReady
	metaReady := w.MetadataStoreState == configstore.ManagedWarehouseStateReady || updates["metadata_store_state"] == configstore.ManagedWarehouseStateReady
	secretsReady := w.SecretsState == configstore.ManagedWarehouseStateReady || updates["secrets_state"] == configstore.ManagedWarehouseStateReady
	identReady := w.IdentityState == configstore.ManagedWarehouseStateReady || updates["identity_state"] == configstore.ManagedWarehouseStateReady

	if s3Ready && metaReady && secretsReady && identReady && status.ReadyCondition {
		now := time.Now().UTC()
		updates["state"] = configstore.ManagedWarehouseStateReady
		updates["status_message"] = "Infrastructure ready"
		updates["ready_at"] = now
		log.Info("Infrastructure ready, transitioning to ready.")
	}

	if len(updates) > 0 {
		if err := c.store.UpdateWarehouseState(w.OrgID, configstore.ManagedWarehouseStateProvisioning, updates); err != nil {
			log.Warn("Failed to update warehouse state.", "error", err)
		}
	}
}

// reconcileReady handles drift correction for Ready warehouses. Today the
// only post-create-mutable spec field is metadataStore.pgbouncer.enabled;
// if an operator flips it in the config store (admin API), we patch the CR
// so the Crossplane composition provisions / tears down the pooler.
//
// Scope is intentionally narrow: we do NOT reconcile ACU, image, or other
// spec fields. Those aren't user-mutable via the admin API today, and
// aggressive drift correction would conflict with manual kubectl patches.
func (c *Controller) reconcileReady(ctx context.Context, w *configstore.ManagedWarehouse) {
	log := slog.With("org", w.OrgID, "phase", "ready")

	currentEnabled, err := c.duckling.GetPgBouncerEnabled(ctx, w.OrgID)
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
	if currentEnabled == w.PgBouncer.Enabled {
		return
	}

	log.Info("PgBouncer drift detected, patching Duckling CR.",
		"desired", w.PgBouncer.Enabled, "current", currentEnabled)
	if err := c.duckling.SetPgBouncerEnabled(ctx, w.OrgID, w.PgBouncer.Enabled); err != nil {
		log.Warn("Failed to patch Duckling CR pgbouncer.enabled.", "error", err)
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
