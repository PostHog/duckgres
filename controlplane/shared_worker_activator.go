//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/server"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type SharedWorkerActivator struct {
	clientset              kubernetes.Interface
	defaultNamespace       string
	stsBroker              *STSBroker
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

type TenantActivationPayload struct {
	OrgID     string                `json:"org_id"`
	Usernames []string              `json:"usernames,omitempty"`
	DuckLake  server.DuckLakeConfig `json:"ducklake"`
}

func NewSharedWorkerActivator(shared *K8sWorkerPool, stsBroker *STSBroker, resolveOrgConfig func(string) (*configstore.OrgConfig, error)) *SharedWorkerActivator {
	if shared == nil {
		return nil
	}
	return &SharedWorkerActivator{
		clientset:              shared.clientset,
		defaultNamespace:       shared.namespace,
		stsBroker:              stsBroker,
		resolveOrgConfig:       resolveOrgConfig,
		activateReservedWorker: shared.ActivateReservedWorker,
	}
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
		if needed, err := server.CheckAndBackupDuckLakeMigration(payload.DuckLake, os.TempDir()); err != nil {
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

	return err
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
	var err error

	// Try reading infrastructure details from the Duckling CR first (Crossplane-provisioned).
	// Fall back to the config store path for non-Crossplane warehouses (manual seed, MinIO, etc.).
	if a.resolveDucklingStatus != nil {
		dl, err = a.buildDuckLakeConfigFromDuckling(ctx, assignment.OrgID)
		if err != nil {
			slog.Warn("Duckling CR activation failed, falling back to config store.", "org", assignment.OrgID, "error", err)
		}
	}
	if a.resolveDucklingStatus == nil || err != nil {
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

	return TenantActivationPayload{
		OrgID:     assignment.OrgID,
		Usernames: usernames,
		DuckLake:  dl,
	}, nil
}

// buildDuckLakeConfigFromDuckling reads all infrastructure details from the Duckling CR
// and uses STS to broker S3 credentials. Used for Crossplane-provisioned ducklings.
func (a *SharedWorkerActivator) buildDuckLakeConfigFromDuckling(ctx context.Context, orgID string) (server.DuckLakeConfig, error) {
	status, err := a.resolveDucklingStatus(ctx, orgID)
	if err != nil {
		return server.DuckLakeConfig{}, fmt.Errorf("resolve duckling CR %q: %w", orgID, err)
	}
	if status.MetadataStore.Password == "" {
		return server.DuckLakeConfig{}, fmt.Errorf("duckling CR %q has no metadata store password", orgID)
	}
	if status.DataStore.BucketName == "" {
		return server.DuckLakeConfig{}, fmt.Errorf("duckling CR %q has no data store bucket", orgID)
	}

	dl := server.DuckLakeConfig{
		MetadataStore: buildDuckLakeMetadataStoreDSN(
			status.MetadataStore.Endpoint,
			5432, // Aurora always uses 5432
			status.MetadataStore.User,
			status.MetadataStore.Password,
			status.MetadataStore.Database,
		),
		ObjectStore: fmt.Sprintf("s3://%s/", status.DataStore.BucketName),
		S3Region:    status.DataStore.S3Region,
		S3UseSSL:    true,
		S3URLStyle:  "vhost",
	}

	// Broker S3 credentials via STS AssumeRole
	if status.IAMRoleARN == "" {
		return server.DuckLakeConfig{}, fmt.Errorf("duckling CR %q has no IAM role ARN for shared warm activation", orgID)
	}
	if a.stsBroker == nil {
		return server.DuckLakeConfig{}, fmt.Errorf("STS broker is required for shared warm activation for org %q", orgID)
	}
	creds, err := a.stsBroker.AssumeRole(ctx, status.IAMRoleARN)
	if err != nil {
		return server.DuckLakeConfig{}, fmt.Errorf("STS AssumeRole for org %q: %w", orgID, err)
	}
	dl.S3Provider = "config"
	dl.S3AccessKey = creds.AccessKeyID
	dl.S3SecretKey = creds.SecretAccessKey
	dl.S3SessionToken = creds.SessionToken

	return dl, nil
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
		ObjectStore: buildManagedWarehouseObjectStore(warehouse.S3),
		S3Endpoint:  warehouse.S3.Endpoint,
		S3Region:    warehouse.S3.Region,
		S3UseSSL:    warehouse.S3.UseSSL,
		S3URLStyle:  warehouse.S3.URLStyle,
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
