//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/server"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type SharedWorkerActivator struct {
	clientset              kubernetes.Interface
	defaultNamespace       string
	defaultSpecVersion     string
	stsBroker              *STSBroker
	runtimeStore           credentialExpiryStore
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
// to record when STS-brokered S3 credentials expire on a worker, and to
// bump the owner epoch atomically when re-activating a worker for refresh.
// Defined as an interface so tests can substitute a fake without standing
// up Postgres.
type credentialExpiryStore interface {
	MarkCredentialsRefreshed(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, expiresAt time.Time) (bool, error)
	BumpWorkerEpoch(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (int64, error)
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
		if needed, err := server.CheckAndBackupDuckLakeMigration(payload.DuckLake, os.TempDir(), payload.DuckLake.SpecVersion); err != nil {
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

	currentEpoch := worker.OwnerEpoch()
	cpInstanceID := worker.OwnerCPInstanceID()
	newEpoch, err := a.runtimeStore.BumpWorkerEpoch(worker.ID, cpInstanceID, currentEpoch)
	if err != nil {
		return fmt.Errorf("bump owner epoch for refresh: %w", err)
	}
	worker.SetOwnerEpoch(newEpoch)

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

	if _, err := a.runtimeStore.MarkCredentialsRefreshed(
		worker.ID, cpInstanceID, newEpoch, *payload.S3CredentialsExpiresAt,
	); err != nil {
		// The worker has new creds in DuckDB; we just couldn't record the
		// new expiry. Next tick will see a stale expiry and refresh again.
		slog.Warn("Failed to record refreshed credential expiry.",
			"worker_id", worker.ID, "org", payload.OrgID, "error", err)
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
		// Config-store warehouses use static creds (no STS), so expiresAt
		// stays nil and the credential refresh scheduler skips them.
		dl, err = a.buildDuckLakeConfigFromConfigStore(ctx, org.Warehouse)
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
		targetSpecVersion = server.DefaultDuckLakeSpecVersion
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
	if status.MetadataStore.Password == "" {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has no metadata store password", orgID)
	}
	if status.DataStore.BucketName == "" {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has no data store bucket", orgID)
	}

	host, port, viaPgBouncer, err := ducklingMetadataStoreAddress(status, orgID)
	if err != nil {
		return server.DuckLakeConfig{}, nil, err
	}

	dl := server.DuckLakeConfig{
		MetadataStore: buildDuckLakeMetadataStoreDSN(
			host,
			port,
			status.MetadataStore.User,
			status.MetadataStore.Password,
			status.MetadataStore.Database,
		),
		ViaPgBouncer: viaPgBouncer,
		ObjectStore:  fmt.Sprintf("s3://%s/", status.DataStore.BucketName),
		S3Region:     status.DataStore.S3Region,
		S3UseSSL:     true,
		S3URLStyle:   "vhost",
	}

	// Broker S3 credentials via STS AssumeRole
	if status.IAMRoleARN == "" {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("duckling CR %q has no IAM role ARN for shared warm activation", orgID)
	}
	if a.stsBroker == nil {
		return server.DuckLakeConfig{}, nil, fmt.Errorf("STS broker is required for shared warm activation for org %q", orgID)
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

	// No pooler — fall back to the direct Aurora endpoint. Guard against an
	// empty endpoint here rather than letting a malformed DSN surface later
	// as an opaque connect error.
	if status.MetadataStore.Endpoint == "" {
		return "", 0, false, fmt.Errorf("duckling CR %q has no metadata store endpoint or pgbouncerEndpoint", orgID)
	}
	return status.MetadataStore.Endpoint, 5432, false, nil
}

// buildDuckLakeConfigFromConfigStore reads infrastructure details from the config store
// and K8s Secrets. Used for non-Crossplane warehouses (manual seed, MinIO, etc.).
func (a *SharedWorkerActivator) buildDuckLakeConfigFromConfigStore(ctx context.Context, warehouse *configstore.ManagedWarehouseConfig) (server.DuckLakeConfig, error) {
	metadataPassword, err := a.readSecretValue(ctx, warehouse.MetadataStoreCredentials)
	if err != nil {
		return server.DuckLakeConfig{}, fmt.Errorf("metadata store credentials: %w", err)
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
		accessKey, secretKey, err := a.readS3Credentials(ctx, warehouse.S3Credentials)
		if err != nil {
			return server.DuckLakeConfig{}, fmt.Errorf("s3 credentials: %w", err)
		}
		dl.S3Provider = "config"
		dl.S3AccessKey = accessKey
		dl.S3SecretKey = secretKey
	case strings.EqualFold(warehouse.S3.Provider, "aws"):
		roleARN := warehouse.WorkerIdentity.IAMRoleARN
		if roleARN == "" {
			return server.DuckLakeConfig{}, fmt.Errorf("managed warehouse %q requires worker_identity.iam_role_arn for shared warm activation", warehouse.OrgID)
		}
		if a.stsBroker == nil {
			return server.DuckLakeConfig{}, fmt.Errorf("STS broker is required for shared warm activation for org %q", warehouse.OrgID)
		}
		creds, err := a.stsBroker.AssumeRole(ctx, roleARN)
		if err != nil {
			return server.DuckLakeConfig{}, fmt.Errorf("STS AssumeRole for org %q: %w", warehouse.OrgID, err)
		}
		dl.S3Provider = "config"
		dl.S3AccessKey = creds.AccessKeyID
		dl.S3SecretKey = creds.SecretAccessKey
		dl.S3SessionToken = creds.SessionToken
	}

	return dl, nil
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

func (a *SharedWorkerActivator) readS3Credentials(ctx context.Context, ref configstore.SecretRef) (string, string, error) {
	value, err := a.readSecretValue(ctx, ref)
	if err != nil {
		return "", "", err
	}

	var payload struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
	}
	if err := json.Unmarshal([]byte(value), &payload); err != nil {
		return "", "", fmt.Errorf("parse s3 credential payload: %w", err)
	}
	if payload.AccessKeyID == "" || payload.SecretAccessKey == "" {
		return "", "", fmt.Errorf("s3 credential payload requires access_key_id and secret_access_key")
	}
	return payload.AccessKeyID, payload.SecretAccessKey, nil
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
