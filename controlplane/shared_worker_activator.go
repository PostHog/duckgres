//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/ducklake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// activeOrgLabelKey is the pod label that per-warehouse Cilium policies
// select on. The Crossplane Duckling composition pre-creates a
// CiliumClusterwideNetworkPolicy keyed on this label + the org name, so
// stamping the label at activation time is what causes the policy to
// snap onto the worker pod via label selector — no per-pod CRD writes.
const activeOrgLabelKey = "duckgres/active-org"

type SharedWorkerActivator struct {
	clientset              kubernetes.Interface
	defaultNamespace       string
	defaultSpecVersion     string
	stsBroker              *STSBroker
	runtimeStore           credentialExpiryStore
	lifecycle              *WorkerLifecycle // typed RefreshLease entry point; same epoch-bump CAS as direct BumpWorkerEpoch
	resolveOrgConfig       func(string) (*configstore.OrgConfig, error)
	resolveDucklingStatus  func(context.Context, string) (*provisioner.DucklingStatus, error)
	activateReservedWorker func(context.Context, *ManagedWorker, TenantActivationPayload) error
	setMigrating           func(orgID string)
	clearMigrating         func(orgID string)

	// migrationChecked caches per-org migration check results to avoid
	// redundant pgx roundtrips on every worker activation.
	// After the first check, the metadata store version is known and
	// won't change (migration is irreversible).
	migrationChecked sync.Map // orgID (string) → bool (needed)
}

// credentialExpiryStore is the slice of the runtime store the activator uses
// to record when STS-brokered S3 credentials expire on a worker.
// Owner-epoch bumping for credential refresh now flows through
// lifecycle.RefreshLease, which uses a separate workerLifecycleStore
// interface internally — this surface stays narrow.
// Defined as an interface so tests can substitute a fake without
// standing up Postgres.
type credentialExpiryStore interface {
	MarkCredentialsRefreshed(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, expiresAt time.Time) (bool, error)
}

type TenantActivationPayload struct {
	OrgID     string                `json:"org_id"`
	Usernames []string              `json:"usernames,omitempty"`
	DuckLake  server.DuckLakeConfig `json:"ducklake"`
	// S3CredentialsExpiresAt is the absolute expiration time of the STS
	// credentials embedded in DuckLake.{S3AccessKey,S3SecretKey,S3SessionToken}.
	// nil for non-STS payloads (config-store-driven warehouses use static
	// credentials with no expiration). When set, the activator persists it
	// onto the worker_records row after a successful activation so the
	// credential refresh scheduler can pick the worker up before the token
	// goes stale.
	S3CredentialsExpiresAt *time.Time `json:"s3_credentials_expires_at,omitempty"`
}

func NewSharedWorkerActivator(shared *K8sWorkerPool, stsBroker *STSBroker, defaultSpecVersion string, resolveOrgConfig func(string) (*configstore.OrgConfig, error)) *SharedWorkerActivator {
	if shared == nil {
		return nil
	}
	a := &SharedWorkerActivator{
		clientset:              shared.clientset,
		defaultNamespace:       shared.namespace,
		defaultSpecVersion:     defaultSpecVersion,
		stsBroker:              stsBroker,
		resolveOrgConfig:       resolveOrgConfig,
		activateReservedWorker: shared.ActivateReservedWorker,
	}
	// The K8s pool already wraps the underlying configstore via its
	// runtimeStore; the activator can borrow it for the credential-refresh
	// stamp. Tests that construct a pool without a runtime store get a nil
	// here, which is fine — the persist step is skipped (the stamp is best-
	// effort; the next scheduler tick will retry).
	if rs, ok := shared.runtimeStore.(credentialExpiryStore); ok {
		a.runtimeStore = rs
	}
	// Borrow the pool's lifecycle service via ensureLifecycle() so we
	// pick up the lazy initialization path the pool uses when
	// runtimeStore was wired after construction (test fixtures). With
	// PR 4's legacy-fallback deletion, the activator now refuses to
	// refresh credentials when lifecycle is nil — going through
	// ensureLifecycle gives us the same wiring discipline the
	// K8sWorkerPool itself uses.
	a.lifecycle = shared.ensureLifecycle()
	return a
}

func (a *SharedWorkerActivator) ActivateReservedWorker(ctx context.Context, worker *ManagedWorker) error {
	if worker == nil {
		return fmt.Errorf("worker is required for activation")
	}
	state := worker.SharedState()
	if state.Assignment == nil {
		return fmt.Errorf("worker %d has no org assignment", worker.ID)
	}

	org, err := a.lookupOrgConfig(state.Assignment.OrgID)
	if err != nil {
		return err
	}

	payload, err := a.BuildActivationRequest(ctx, org, state.Assignment)
	if err != nil {
		return err
	}

	// Check if this org needs DuckLake migration. The result is cached per org
	// to avoid redundant pgx roundtrips on every worker activation — once the
	// metadata store is checked, its version won't change (migration is
	// irreversible, and the first migrating worker upgrades it).
	if cached, ok := a.migrationChecked.Load(payload.OrgID); ok {
		if cached.(bool) {
			payload.DuckLake.Migrate = true
		}
	} else {
		// First activation for this org — run the check in the control plane.
		// The backup file is written to os.TempDir() since the CP may not have
		// a persistent /data mount.
		if needed, err := ducklake.CheckAndBackupMigration(payload.DuckLake, os.TempDir(), payload.DuckLake.SpecVersion); err != nil {
			slog.Warn("DuckLake migration check failed, proceeding without migration.",
				"org", payload.OrgID, "error", err)
		} else {
			a.migrationChecked.Store(payload.OrgID, needed)
			if needed {
				payload.DuckLake.Migrate = true
			}
		}
	}

	// Block new connections for this org while the migration runs.
	// The first worker to activate will perform the schema upgrade;
	// subsequent connections would fail against a partially-migrated catalog.
	if payload.DuckLake.Migrate && a.setMigrating != nil {
		a.setMigrating(payload.OrgID)
	}

	activate := func() error {
		if a.activateReservedWorker != nil {
			return a.activateReservedWorker(ctx, worker, payload)
		}
		return worker.ActivateTenant(ctx, server.WorkerActivationPayload{
			WorkerControlMetadata: server.WorkerControlMetadata{
				WorkerID:     worker.ID,
				OwnerEpoch:   worker.OwnerEpoch(),
				CPInstanceID: worker.OwnerCPInstanceID(),
			},
			OrgID:    payload.OrgID,
			DuckLake: payload.DuckLake,
		})
	}

	err = activate()

	// Stamp the active-org label on the worker pod so the per-warehouse
	// CiliumClusterwideNetworkPolicy (pre-created by the Crossplane
	// Duckling composition) snaps onto this pod via label selector.
	// Best-effort and idempotent: a failure here does not fail activation;
	// the worker still functions, it just won't have its per-tenant
	// network policy applied until something patches the label later.
	// See also docs on activeOrgLabelKey.
	if err == nil {
		a.stampActiveOrgLabel(ctx, worker.PodName(), payload.OrgID)
	}

	// Clear the migration lock after the worker finishes activation
	// (whether it succeeded or failed). New connections can proceed.
	if payload.DuckLake.Migrate {
		if a.clearMigrating != nil {
			a.clearMigrating(payload.OrgID)
		}
		if err == nil {
			// Migration succeeded — update cache so future workers for this
			// org skip the check and don't set AUTOMATIC_MIGRATION.
			a.migrationChecked.Store(payload.OrgID, false)
		}
	}

	// Stamp the STS expiration onto the worker_records row so the
	// credential-refresh scheduler can pick this worker up before its
	// session token goes stale. Best-effort: a failure here doesn't fail
	// the activation. The next scheduler tick will see a NULL or stale
	// expiry and refresh inline.
	if err == nil && payload.S3CredentialsExpiresAt != nil && a.runtimeStore != nil {
		if _, mErr := a.runtimeStore.MarkCredentialsRefreshed(
			worker.ID,
			worker.OwnerCPInstanceID(),
			worker.OwnerEpoch(),
			*payload.S3CredentialsExpiresAt,
		); mErr != nil {
			slog.Warn("Failed to record S3 credential expiry on activation.",
				"worker_id", worker.ID, "org", payload.OrgID, "error", mErr)
		}
	}

	return err
}

// stampActiveOrgLabel patches the worker pod with `duckgres/active-org=<org>`.
// Strategic-merge so re-activations (e.g. credential refresh) are no-ops
// once the label is set. Logs and swallows errors; the activation has
// already completed by the time we get here, and the label is a security
// optimisation rather than a functional requirement.
func (a *SharedWorkerActivator) stampActiveOrgLabel(ctx context.Context, podName, orgID string) {
	if a.clientset == nil || podName == "" || orgID == "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, activeOrgLabelKey, orgID)
	if _, err := a.clientset.CoreV1().Pods(a.defaultNamespace).Patch(
		ctx,
		podName,
		types.StrategicMergePatchType,
		[]byte(patch),
		metav1.PatchOptions{},
	); err != nil {
		slog.Warn("Failed to stamp active-org label on worker pod.",
			"pod", podName, "org", orgID, "error", err)
	}
}

// RefreshCredentials brokers a fresh STS session for the worker's org and
// re-sends ActivateTenant so the worker's DuckDB ducklake_s3 secret picks
// up the new (AccessKey, SecretKey, SessionToken). Used by the
// credential-refresh scheduler to keep long-running workers alive across
// the STS session-duration boundary without ever touching a session's
// pinned *sql.Conn.
//
// The owner_epoch is bumped atomically in the runtime store before the
// RPC; the in-memory ManagedWorker is updated to match, then the
// ActivateTenant RPC carries the new epoch so the worker's
// reuseExistingActivation guard accepts the payload. On RPC success the
// worker's in-memory state is replaced and MarkCredentialsRefreshed
// stamps the new expiry. Any of these steps can fail — a failure aborts
// without trampling the worker's existing (still-valid-for-now) creds,
// and the next scheduler tick retries.
func (a *SharedWorkerActivator) RefreshCredentials(ctx context.Context, worker *ManagedWorker) error {
	if worker == nil {
		return fmt.Errorf("worker is required for credential refresh")
	}
	if a.runtimeStore == nil {
		return fmt.Errorf("credential refresh requires a runtime store")
	}
	state := worker.SharedState()
	if state.Assignment == nil || state.Assignment.OrgID == "" {
		return fmt.Errorf("worker %d has no org assignment to refresh credentials for", worker.ID)
	}

	org, err := a.lookupOrgConfig(state.Assignment.OrgID)
	if err != nil {
		return err
	}
	payload, err := a.BuildActivationRequest(ctx, org, state.Assignment)
	if err != nil {
		return err
	}
	if payload.S3CredentialsExpiresAt == nil {
		// Static-cred warehouses don't need refresh — defensive guard so we
		// don't incorrectly mark expiry on rows that genuinely have none.
		return nil
	}

	if a.lifecycle == nil {
		return fmt.Errorf("refresh worker credentials requires a lifecycle service (worker %d)", worker.ID)
	}
	cpInstanceID := worker.OwnerCPInstanceID()
	// RefreshOwnerEpochAtomic holds the worker's epoch lock across the
	// durable RefreshLease CAS. Without this, a concurrent
	// ShutdownAll's OwnerEpoch() read could land between the durable
	// bump and the in-memory SetOwnerEpoch, building a stale lease
	// that CAS-misses (leaving the row in draining for the orphan
	// sweep to reconcile later). The lock is held during the DB
	// round-trip; that's the right trade-off — the round-trip is
	// O(10ms) and ShutdownAll iterates workers serially anyway.
	var newEpoch int64
	if err := worker.RefreshOwnerEpochAtomic(func(currentEpoch int64) (int64, error) {
		newLease, err := a.lifecycle.RefreshLease(configstore.NewWorkerLease(worker.ID, cpInstanceID, currentEpoch, worker.image), LifecycleOriginCredRefresh)
		if err != nil {
			return 0, err
		}
		newEpoch = newLease.OwnerEpoch()
		return newEpoch, nil
	}); err != nil {
		// Keep the legacy "bump owner epoch for refresh" wrapper wording
		// so log-grep / dashboard filters that pattern-match on the
		// previous string still work. ErrWorkerOwnerEpochMismatch is
		// preserved via %w.
		return fmt.Errorf("bump owner epoch for refresh: %w", err)
	}

	rpcPayload := server.WorkerActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			WorkerID:     worker.ID,
			OwnerEpoch:   newEpoch,
			CPInstanceID: cpInstanceID,
		},
		OrgID:    payload.OrgID,
		DuckLake: payload.DuckLake,
	}
	if err := worker.ActivateTenant(ctx, rpcPayload); err != nil {
		return fmt.Errorf("activate tenant for refresh: %w", err)
	}

	if a.runtimeStore != nil {
		// Guarded against a future store implementation that satisfies
		// workerLifecycleStore (for the RefreshLease above) but not
		// credentialExpiryStore. Production ConfigStore satisfies
		// both, but the two type assertions in NewSharedWorkerActivator
		// are independent and a partial mock could leave runtimeStore
		// nil while lifecycle is set. The expiry stamp is best-effort
		// — skipping it just means the next refresh tick sees a stale
		// expiry and refreshes again.
		if _, err := a.runtimeStore.MarkCredentialsRefreshed(
			worker.ID, cpInstanceID, newEpoch, *payload.S3CredentialsExpiresAt,
		); err != nil {
			// The worker has new creds in DuckDB; we just couldn't record the
			// new expiry. Next tick will see a stale expiry and refresh again.
			slog.Warn("Failed to record refreshed credential expiry.",
				"worker_id", worker.ID, "org", payload.OrgID, "error", err)
		}
	}
	return nil
}

func (a *SharedWorkerActivator) BuildActivationRequest(ctx context.Context, org *configstore.OrgConfig, assignment *WorkerAssignment) (TenantActivationPayload, error) {
	if org == nil || org.Warehouse == nil {
		return TenantActivationPayload{}, fmt.Errorf("org %q has no managed warehouse runtime", orgName(org))
	}
	if assignment == nil {
		return TenantActivationPayload{}, fmt.Errorf("worker assignment is required")
	}
	if err := configstore.ValidateManagedWarehouseSecretRefs(assignment.OrgID, a.defaultNamespace, org.Warehouse); err != nil {
		return TenantActivationPayload{}, err
	}

	var dl server.DuckLakeConfig
	var expiresAt *time.Time
	var err error

	// Try reading infrastructure details from the Duckling CR first (Crossplane-provisioned).
	// Fall back to the config store path for non-Crossplane warehouses (manual seed, MinIO, etc.).
	if a.resolveDucklingStatus != nil {
		dl, expiresAt, err = a.buildDuckLakeConfigFromDuckling(ctx, assignment.OrgID)
		if err != nil {
			slog.Warn("Duckling CR activation failed, falling back to config store.", "org", assignment.OrgID, "error", err)
		}
	}
	if a.resolveDucklingStatus == nil || err != nil {
		// Static-cred config-store warehouses (secret-ref S3 credentials)
		// return a nil expiresAt and the credential refresh scheduler skips
		// them; the "aws" provider path brokers STS creds and returns their
		// expiration so the scheduler keeps those workers fresh.
		dl, expiresAt, err = a.buildDuckLakeConfigFromConfigStore(ctx, org.Warehouse)
	}
	if err != nil {
		return TenantActivationPayload{}, err
	}

	usernames := make([]string, 0, len(org.Users))
	for username := range org.Users {
		usernames = append(usernames, username)
	}
	slices.Sort(usernames)

	// Resolve target DuckLake spec version.
	targetSpecVersion := org.Warehouse.DuckLakeVersion
	if targetSpecVersion == "" {
		targetSpecVersion = a.defaultSpecVersion
	}
	if targetSpecVersion == "" {
		targetSpecVersion = ducklake.DefaultSpecVersion
	}
	dl.SpecVersion = targetSpecVersion

	return TenantActivationPayload{
		OrgID:                  assignment.OrgID,
		Usernames:              usernames,
		DuckLake:               dl,
		S3CredentialsExpiresAt: expiresAt,
	}, nil
}

// buildDuckLakeConfigFromDuckling reads all infrastructure details from the Duckling CR
// and uses STS to broker S3 credentials. Used for Crossplane-provisioned ducklings.
// The returned *time.Time is the STS credentials' expiration; nil for paths that
// don't broker temporary credentials.
func (a *SharedWorkerActivator) buildDuckLakeConfigFromDuckling(ctx context.Context, orgID string) (server.DuckLakeConfig, *time.Time, error) {
	status, err := a.resolveDucklingStatus(ctx, orgID)
	if err != nil {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("resolve duckling CR %q: %w", orgID, err)
	}
	if status.DataStore.BucketName == "" {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has no data store bucket", orgID)
	}

	dl := server.DuckLakeConfig{
		ObjectStore: fmt.Sprintf("s3://%s/", status.DataStore.BucketName),
		S3Region:    status.DataStore.S3Region,
		S3UseSSL:    true,
		S3URLStyle:  "vhost",
	}

	// DuckLake is attached iff this tenant has it enabled. The CR's
	// spec.ducklake.enabled is authoritative (decoupled ducklings); legacy CRs
	// that predate the field fall back to the historical coupling — DuckLake on
	// for external, off for cnpg-shard. When on, the catalog lives in the
	// metadata Postgres.
	ducklakeEnabled := status.MetadataStore.Type != configstore.MetadataStoreKindCnpgShard
	if status.DuckLakeEnabled != nil {
		ducklakeEnabled = *status.DuckLakeEnabled
	}
	if ducklakeEnabled {
		if status.MetadataStore.Password == "" {
			return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has DuckLake enabled but no metadata store password", orgID)
		}
		host, port, viaPgBouncer, err := ducklingMetadataStoreAddress(status, orgID)
		if err != nil {
			return server.DuckLakeConfig{}, nil, err
		}
		dl.MetadataStore = buildDuckLakeMetadataStoreDSN(
			host,
			port,
			status.MetadataStore.User,
			status.MetadataStore.Password,
			status.MetadataStore.Database,
		)
		dl.ViaPgBouncer = viaPgBouncer
	}

	// Broker S3 credentials via STS AssumeRole
	if status.IAMRoleARN == "" {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has no IAM role ARN for worker activation", orgID)
	}
	if a.stsBroker == nil {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("STS broker is required for worker activation for org %q", orgID)
	}
	creds, err := a.stsBroker.AssumeRole(ctx, status.IAMRoleARN)
	if err != nil {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("STS AssumeRole for org %q: %w", orgID, err)
	}
	dl.S3Provider = "config"
	dl.S3AccessKey = creds.AccessKeyID
	dl.S3SecretKey = creds.SecretAccessKey
	dl.S3SessionToken = creds.SessionToken

	expiresAt := creds.Expiration
	return dl, &expiresAt, nil
}

func ducklingMetadataStoreAddress(status *provisioner.DucklingStatus, orgID string) (host string, port int, viaPgBouncer bool, err error) {
	// Prefer the PgBouncer endpoint when the Duckling exposes one — the
	// Crossplane composition sets status.metadataStore.pgbouncerEndpoint
	// (as "<host>:<port>") when a per-Duckling pooler is provisioned.
	if pgb := status.MetadataStore.PgBouncerEndpoint; pgb != "" {
		h, p, err := net.SplitHostPort(pgb)
		if err != nil {
			return "", 0, false, fmt.Errorf("parse pgbouncerEndpoint %q for org %q: %w", pgb, orgID, err)
		}
		portNum, err := strconv.Atoi(p)
		if err != nil {
			return "", 0, false, fmt.Errorf("parse pgbouncerEndpoint port %q for org %q: %w", p, orgID, err)
		}
		return h, portNum, true, nil
	}

	// No pooler — fall back to the direct RDS endpoint. Guard against an
	// empty endpoint here rather than letting a malformed DSN surface later
	// as an opaque connect error.
	if status.MetadataStore.Endpoint == "" {
		return "", 0, false, fmt.Errorf("duckling CR %q has no metadata store endpoint or pgbouncerEndpoint", orgID)
	}
	return status.MetadataStore.Endpoint, 5432, false, nil
}

// buildDuckLakeConfigFromConfigStore reads infrastructure details from the config store
// and K8s Secrets. Used for non-Crossplane warehouses (manual seed, MinIO, etc.).
func (a *SharedWorkerActivator) buildDuckLakeConfigFromConfigStore(ctx context.Context, warehouse *configstore.ManagedWarehouseConfig) (server.DuckLakeConfig, *time.Time, error) {
	metadataPassword, err := a.readSecretValue(ctx, warehouse.MetadataStoreCredentials)
	if err != nil {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("metadata store credentials: %w", err)
	}

	dl := server.DuckLakeConfig{
		MetadataStore: buildDuckLakeMetadataStoreDSN(
			warehouse.MetadataStore.Endpoint,
			warehouse.MetadataStore.Port,
			warehouse.MetadataStore.Username,
			metadataPassword,
			warehouse.MetadataStore.DatabaseName,
		),
		ObjectStore:         buildManagedWarehouseObjectStore(warehouse.S3),
		S3Endpoint:          warehouse.S3.Endpoint,
		S3Region:            warehouse.S3.Region,
		S3UseSSL:            warehouse.S3.UseSSL,
		S3URLStyle:          warehouse.S3.URLStyle,
		DeltaCatalogEnabled: warehouse.S3.DeltaCatalogEnabled,
		DeltaCatalogPath:    warehouse.S3.DeltaCatalogPath,
	}

	switch {
	case warehouse.S3Credentials.Name != "":
		accessKey, secretKey, sessionToken, err := a.readS3Credentials(ctx, warehouse.S3Credentials)
		if err != nil {
			return server.DuckLakeConfig{}, nil, fmt.Errorf("s3 credentials: %w", err)
		}
		dl.S3Provider = "config"
		dl.S3AccessKey = accessKey
		dl.S3SecretKey = secretKey
		// session_token is optional in the secret payload — long-term IAM
		// user keys don't have one. STS-vended temporary credentials
		// (AccessKeyId starting with ASIA…) require it: AWS rejects the
		// signing identity without the token. Letting the field through lets sandbox/CI fixtures
		// that source creds from STS use the same secret-ref schema as
		// production's long-term keys.
		dl.S3SessionToken = sessionToken
	case strings.EqualFold(warehouse.S3.Provider, "aws"):
		roleARN := warehouse.WorkerIdentity.IAMRoleARN
		if roleARN == "" {
			return server.DuckLakeConfig{}, nil, fmt.Errorf("managed warehouse %q requires worker_identity.iam_role_arn for worker activation", warehouse.OrgID)
		}
		if a.stsBroker == nil {
			return server.DuckLakeConfig{}, nil, fmt.Errorf("STS broker is required for worker activation for org %q", warehouse.OrgID)
		}
		creds, err := a.stsBroker.AssumeRole(ctx, roleARN)
		if err != nil {
			return server.DuckLakeConfig{}, nil, fmt.Errorf("STS AssumeRole for org %q: %w", warehouse.OrgID, err)
		}
		dl.S3Provider = "config"
		dl.S3AccessKey = creds.AccessKeyID
		dl.S3SecretKey = creds.SecretAccessKey
		dl.S3SessionToken = creds.SessionToken
		// STS-vended creds expire: surface the expiration so the
		// credential-refresh scheduler keeps this worker's secret fresh.
		// Without it the worker record's expiry stays NULL, the scheduler
		// never lists the worker as due, and any session outliving the
		// 1h token dies with ExpiredToken.
		expiresAt := creds.Expiration
		return dl, &expiresAt, nil
	}

	return dl, nil, nil
}

func BuildTenantActivationPayload(ctx context.Context, clientset kubernetes.Interface, defaultNamespace string, org *configstore.OrgConfig, stsBroker *STSBroker) (TenantActivationPayload, error) {
	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: defaultNamespace,
		stsBroker:        stsBroker,
	}
	assignment := &WorkerAssignment{
		OrgID: orgName(org),
	}
	return activator.BuildActivationRequest(ctx, org, assignment)
}

func (a *SharedWorkerActivator) readSecretValue(ctx context.Context, ref configstore.SecretRef) (string, error) {
	if strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.Key) == "" {
		return "", fmt.Errorf("secret ref requires name and key")
	}
	namespace := strings.TrimSpace(ref.Namespace)
	if namespace == "" {
		if strings.TrimSpace(a.defaultNamespace) != "" {
			return "", fmt.Errorf("secret ref requires explicit SecretRef.Namespace; set worker_identity.namespace and store the same namespace on the SecretRef instead of relying on shared fallback namespace %q", strings.TrimSpace(a.defaultNamespace))
		}
		return "", fmt.Errorf("secret ref requires explicit SecretRef.Namespace; set worker_identity.namespace and store the same namespace on the SecretRef")
	}
	secret, err := a.clientset.CoreV1().Secrets(namespace).Get(ctx, strings.TrimSpace(ref.Name), metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(ref.Key)
	if value, ok := secret.StringData[key]; ok && value != "" {
		return value, nil
	}
	raw, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s missing key %q", secret.Namespace, secret.Name, key)
	}
	return string(raw), nil
}

func (a *SharedWorkerActivator) readS3Credentials(ctx context.Context, ref configstore.SecretRef) (string, string, string, error) {
	value, err := a.readSecretValue(ctx, ref)
	if err != nil {
		return "", "", "", err
	}

	var payload struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
		SessionToken    string `json:"session_token"`
	}
	if err := json.Unmarshal([]byte(value), &payload); err != nil {
		return "", "", "", fmt.Errorf("parse s3 credential payload: %w", err)
	}
	if payload.AccessKeyID == "" || payload.SecretAccessKey == "" {
		return "", "", "", fmt.Errorf("s3 credential payload requires access_key_id and secret_access_key")
	}
	return payload.AccessKeyID, payload.SecretAccessKey, payload.SessionToken, nil
}

func buildDuckLakeMetadataStoreDSN(host string, port int, username, password, database string) string {
	return fmt.Sprintf(
		"postgres:host=%s port=%d user=%s password=%s dbname=%s",
		escapeDuckLakeConnStringValue(host),
		port,
		escapeDuckLakeConnStringValue(username),
		escapeDuckLakeConnStringValue(password),
		escapeDuckLakeConnStringValue(database),
	)
}

func buildManagedWarehouseObjectStore(s3 configstore.ManagedWarehouseS3) string {
	if s3.Bucket == "" {
		return ""
	}
	prefix := strings.Trim(s3.PathPrefix, "/")
	if prefix == "" {
		return fmt.Sprintf("s3://%s/", s3.Bucket)
	}
	return fmt.Sprintf("s3://%s/%s/", s3.Bucket, prefix)
}

func escapeDuckLakeConnStringValue(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		` `, `\ `,
		`'`, `\'`,
		`"`, `\"`,
		"\t", `\t`,
		"\n", `\n`,
		"\r", `\r`,
	)
	return replacer.Replace(value)
}

func (a *SharedWorkerActivator) lookupOrgConfig(orgID string) (*configstore.OrgConfig, error) {
	if a.resolveOrgConfig == nil {
		return nil, fmt.Errorf("org config resolver is not configured for org %s", orgID)
	}
	org, err := a.resolveOrgConfig(orgID)
	if err != nil {
		return nil, err
	}
	if org == nil {
		return nil, fmt.Errorf("org config resolver returned nil for org %s", orgID)
	}
	return org, nil
}

func orgName(org *configstore.OrgConfig) string {
	if org == nil {
		return ""
	}
	return org.Name
}

// MetadataPostgresURL resolves a standard postgres:// URL for an org's
// DuckLake metadata Postgres, for CP-side read-only queries (the storage
// sampler — see storage_meter.go). Mirrors the resolution the worker
// activation uses: prefer the Duckling CR (pgbouncer endpoint when present,
// else the direct metadata endpoint), fall back to the config-store warehouse
// shape. sslmode follows provisioner.ProbeMetadataStore's rules: "disable"
// through the duckling's pgbouncer (plaintext worker↔pooler; the pooler
// carries TLS to RDS), "require" against a direct RDS endpoint, "prefer" for
// config-store warehouses (matches the DuckDB ATTACH default there).
// Returns an error when the org has no DuckLake-enabled warehouse.
func (a *SharedWorkerActivator) MetadataPostgresURL(ctx context.Context, orgID string) (string, error) {
	if a.resolveDucklingStatus != nil {
		status, err := a.resolveDucklingStatus(ctx, orgID)
		if err == nil {
			ducklakeEnabled := status.MetadataStore.Type != configstore.MetadataStoreKindCnpgShard
			if status.DuckLakeEnabled != nil {
				ducklakeEnabled = *status.DuckLakeEnabled
			}
			if !ducklakeEnabled {
				return "", fmt.Errorf("org %q has DuckLake disabled", orgID)
			}
			if status.MetadataStore.Password == "" {
				return "", fmt.Errorf("duckling CR %q has no metadata store password", orgID)
			}
			host, port, viaPgBouncer, err := ducklingMetadataStoreAddress(status, orgID)
			if err != nil {
				return "", err
			}
			sslMode := "require"
			if viaPgBouncer {
				sslMode = "disable"
			}
			return metadataPostgresURL(host, port, status.MetadataStore.User, status.MetadataStore.Password, status.MetadataStore.Database, sslMode), nil
		}
		slog.Debug("Duckling CR metadata resolution failed, falling back to config store.", "org", orgID, "error", err)
	}

	org, err := a.lookupOrgConfig(orgID)
	if err != nil {
		return "", err
	}
	if org.Warehouse == nil {
		return "", fmt.Errorf("org %q has no managed warehouse", orgID)
	}
	password, err := a.readSecretValue(ctx, org.Warehouse.MetadataStoreCredentials)
	if err != nil {
		return "", fmt.Errorf("metadata store credentials for org %q: %w", orgID, err)
	}
	ms := org.Warehouse.MetadataStore
	return metadataPostgresURL(ms.Endpoint, ms.Port, ms.Username, password, ms.DatabaseName, "prefer"), nil
}

// metadataPostgresURL builds a postgres:// URL for database/sql ("pgx") from
// the same fields the DuckDB ATTACH DSN uses. url.URL handles credential
// escaping.
func metadataPostgresURL(host string, port int, username, password, database, sslMode string) string {
	if port == 0 {
		port = 5432
	}
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(username, password),
		Host:     net.JoinHostPort(host, strconv.Itoa(port)),
		Path:     "/" + database,
		RawQuery: "sslmode=" + sslMode + "&connect_timeout=5",
	}).String()
}
