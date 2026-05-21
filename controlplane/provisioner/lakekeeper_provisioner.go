//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server/lakekeeperbroker"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LakekeeperProvisioner advances an org's Lakekeeper through a fixed pipeline:
//
//	create lakekeeper_<orgid> DB → ensure K8s Secret with creds →
//	ensure Lakekeeper CR → wait for operator bootstrap →
//	ensure Iceberg warehouse via REST → persist endpoint+client_id back to
//	the warehouse row.
//
// Every step is idempotent so EnsureForOrg can be called repeatedly. The
// pure dependencies (admin DSN, PG host, S3 config) are passed in by the
// caller so the provisioner doesn't bake in secret-lookup logic — PR2 wiring
// will compute them from the Duckling CR status + K8s Secrets.
type LakekeeperProvisioner struct {
	store     WarehouseStore
	k8s       *LakekeeperK8sClient
	image     string
	clientFor ClientFactory
}

// ClientFactory builds a LakekeeperClient for a base URL. Lets tests inject
// httptest.Server URLs without rebuilding the provisioner.
type ClientFactory func(baseURL string) *LakekeeperClient

// ProvisioningInputs are everything the provisioner needs about the org's
// environment that doesn't live in the warehouse row yet.
type ProvisioningInputs struct {
	// AdminDSN is a pgx-compatible DSN with permission to CREATE DATABASE
	// on the org's Aurora cluster. Caller resolves this from a K8s Secret
	// managed by Crossplane.
	AdminDSN string

	// PG host/port the Lakekeeper pod uses to reach the same cluster. Often
	// the same hostname as AdminDSN but routed differently (e.g. through
	// PgBouncer). Empty Port defaults to 5432.
	PGHost string
	PGPort int32

	// PGSSLMode the Lakekeeper pod sets when connecting. Default "require".
	// Set to "disable" for local/dev environments where PG has no TLS.
	PGSSLMode string

	// S3 storage profile for the warehouse Lakekeeper hands out. For prod
	// AWS, leave Endpoint empty and Flavor "aws". For MinIO / s3-compat,
	// set Endpoint and Flavor "s3-compat".
	S3 S3StorageConfig

	// KubernetesAuthAudiences enables the OIDC SA-token auth path on the
	// Lakekeeper CR. The duckling's projected SA token must carry one of
	// these audiences. Empty disables — Lakekeeper runs in allowall +
	// NetworkPolicy mode (the PR1+PR2 deployment shape).
	//
	// When set, the provisioner also writes a non-empty
	// LakekeeperOAuth2ServerURI to the warehouse row so the worker's
	// in-process broker becomes DuckDB's OAuth2 server.
	//
	// **Flag-day deploy ordering.** Once any org gets a non-empty value
	// here, its activation payload carries LakekeeperOAuth2ServerURI=
	// http://127.0.0.1:9876/token, which the worker's iceberg extension
	// tries to POST to. If the duckling pod spec hasn't yet been updated
	// to (a) mount the projected SA token at DUCKGRES_LAKEKEEPER_TOKEN_PATH
	// and (b) expose the broker on 9876, the POST hits a closed port and
	// the ATTACH fails. There is no path that clears the URI once written
	// (re-running with empty audiences would leave the old value behind).
	// Deploy order MUST be:
	//   1. Roll out the duckling pod spec change with the projected SA
	//      volume + env var.
	//   2. Apply the operator chart change that flips the CR's
	//      authentication.kubernetes.enabled (PR4 already plumbs this).
	//   3. Only then flip KubernetesAuthAudiences in the inputs resolver.
	KubernetesAuthAudiences []string
}

// S3StorageConfig captures the bucket + credentials Lakekeeper uses.
type S3StorageConfig struct {
	Bucket    string
	KeyPrefix string
	Endpoint  string
	Region    string
	// Flavor is "aws" or "s3-compat".
	Flavor string

	// Static creds — present for MinIO/dev. For prod AWS, leave empty and
	// Lakekeeper uses its pod IRSA identity (allow-direct-system-credentials
	// in the operator config).
	StaticAccessKeyID     string
	StaticAccessKeySecret string

	// RoleARN is the IAM role Lakekeeper assumes to vend scoped S3 credentials
	// to clients (sts-role-arn). Required for the AWS flavor when STS is on;
	// the per-org duckling role (Lakekeeper's own Pod Identity, self-assumed).
	// Empty for s3-compat (MinIO), where STS vending doesn't need a role.
	RoleARN string
}

// LakekeeperProvisionerOption tunes the provisioner.
type LakekeeperProvisionerOption func(*LakekeeperProvisioner)

// WithImage sets the Lakekeeper container image. Defaults to a pinned tag.
func WithImage(img string) LakekeeperProvisionerOption {
	return func(p *LakekeeperProvisioner) { p.image = img }
}

// WithClientFactory overrides how LakekeeperClient instances are built.
// Tests use this to point at httptest.Server.
func WithClientFactory(f ClientFactory) LakekeeperProvisionerOption {
	return func(p *LakekeeperProvisioner) { p.clientFor = f }
}

// DefaultLakekeeperImage is the pinned image we deploy by default.
// Bumps to this constant should be paired with a Lakekeeper-operator
// version compatibility check. The published tags carry a "v" prefix
// (quay.io/lakekeeper/catalog:v0.12.2) — without it the pull 404s.
//
// MUST be v0.12.x or later: the operator (lakekeeper_controller.go) emits the
// v0.12.x config scheme (LAKEKEEPER__STORAGE_CREDENTIAL_BACKEND__AWS__*).
// v0.11.x ignores those env vars, so e.g. system-identity credential vending
// silently stays disabled ("System identity credentials are disabled").
const DefaultLakekeeperImage = "quay.io/lakekeeper/catalog:v0.12.2"

// NewLakekeeperProvisioner builds a provisioner. The WarehouseStore is the
// same interface the existing controller uses for persistence.
func NewLakekeeperProvisioner(store WarehouseStore, k8s *LakekeeperK8sClient, opts ...LakekeeperProvisionerOption) *LakekeeperProvisioner {
	p := &LakekeeperProvisioner{
		store:     store,
		k8s:       k8s,
		image:     DefaultLakekeeperImage,
		clientFor: NewLakekeeperClient,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// ErrBootstrapPending signals "the operator hasn't finished initial bootstrap
// of the Lakekeeper CR yet". Callers should requeue rather than treat this
// as a failure.
var ErrBootstrapPending = errors.New("lakekeeper bootstrap pending")

// EnsureForOrg drives the pipeline. Idempotent: re-running on an
// already-provisioned org reads existing state and only emits writes for
// genuine drift.
func (p *LakekeeperProvisioner) EnsureForOrg(ctx context.Context, w *configstore.ManagedWarehouse, in ProvisioningInputs) error {
	if w == nil {
		return errors.New("EnsureForOrg: warehouse is nil")
	}
	if !isValidOrgIDLabel(w.OrgID) {
		return fmt.Errorf("EnsureForOrg: orgID %q is not a valid K8s label value", w.OrgID)
	}
	if err := validateInputs(in); err != nil {
		return err
	}

	dbName := lakekeeperDBName(w.OrgID)
	secretName := LakekeeperResourceName(w.OrgID)
	resourceName := LakekeeperResourceName(w.OrgID)
	// In-cluster Service DNS — operator names the Service after the CR.
	baseURL := fmt.Sprintf("http://%s.%s.svc:8181", resourceName, p.k8s.namespace)

	// 1. CREATE DATABASE lakekeeper_<orgid> if absent.
	if err := EnsureDatabase(ctx, in.AdminDSN, dbName); err != nil {
		return fmt.Errorf("ensure lakekeeper db: %w", err)
	}

	// 2. Resolve or generate the per-org credentials, and ensure the K8s
	// Secret holds them. On re-runs we read the existing Secret rather than
	// rotating its values — Lakekeeper's DB password would otherwise drift
	// from the Postgres user's actual password.
	creds, err := p.resolveOrGenerateSecret(ctx, w.OrgID, dbName, secretName)
	if err != nil {
		return fmt.Errorf("ensure lakekeeper secret: %w", err)
	}

	// 2b. Ensure the Postgres role exists with the password matching the
	// Secret. A freshly-created database has no users by default; without
	// this, the Lakekeeper pod's CreateAdminUser-style startup would fail
	// with "role does not exist". EnsureRole is idempotent and rotates the
	// password to match the Secret on re-runs.
	if err := EnsureRole(ctx, in.AdminDSN, creds.DBUser, creds.DBPassword, dbName); err != nil {
		return fmt.Errorf("ensure lakekeeper pg role: %w", err)
	}

	// 2c. Ensure the per-org ServiceAccount the Lakekeeper pod runs under.
	// Must exist before the CR so the operator's Deployment + migration Job
	// can mount it. Each org gets its own SA (in the shared namespace) so it
	// can carry a distinct EKS Pod Identity scoped to that org's bucket.
	if err := p.k8s.EnsureServiceAccount(ctx, w.OrgID); err != nil {
		return fmt.Errorf("ensure lakekeeper service account: %w", err)
	}

	// 3. Apply the Lakekeeper CR pointing at the org's PG + the Secret.
	pgPort := in.PGPort
	if pgPort == 0 {
		pgPort = 5432
	}
	if err := p.k8s.EnsureCR(ctx, LakekeeperCRSpec{
		OrgID:                   w.OrgID,
		Image:                   p.image,
		Replicas:                1,
		PGHost:                  in.PGHost,
		PGPort:                  pgPort,
		PGDatabase:              dbName,
		SecretName:              secretName,
		BaseURI:                 baseURL,
		PGSSLMode:               in.PGSSLMode,
		ServiceAccountName:      LakekeeperServiceAccountName(w.OrgID),
		KubernetesAuthAudiences: in.KubernetesAuthAudiences,
	}); err != nil {
		return fmt.Errorf("ensure lakekeeper cr: %w", err)
	}

	// 4. Check whether the operator has marked bootstrap complete. If not,
	// return ErrBootstrapPending so the outer reconcile loop can requeue
	// without blocking other orgs.
	if err := p.checkBootstrap(ctx, w.OrgID); err != nil {
		return err
	}

	// 5. Idempotently create the org's warehouse via Lakekeeper's REST API.
	// LakekeeperClient calls /management/v1/* which are at the server root,
	// NOT under /catalog. The /catalog prefix is only for the Iceberg REST
	// API that ducklings hit later — that's the value we persist in
	// LakekeeperEndpoint below.
	lkClient := p.clientFor(baseURL)
	whReq := CreateWarehouseRequest{
		WarehouseName: lakekeeperWarehouseName(w.OrgID),
		StorageProfile: WarehouseStorageProfile{
			Type:                 "s3",
			Bucket:               in.S3.Bucket,
			KeyPrefix:            in.S3.KeyPrefix,
			Endpoint:             in.S3.Endpoint,
			Region:               in.S3.Region,
			PathStyleAccess: in.S3.Flavor == "s3-compat",
			Flavor:          in.S3.Flavor,
			// STS credential vending is OFF: Lakekeeper would assume a role
			// and hand DuckDB short-lived creds, but its downscoping session
			// policy overflows AWS's packed-policy limit (PackedPolicyTooLarge),
			// and it's unnecessary — the duckling worker already holds STS creds
			// for this bucket (brokered by the control plane for DuckLake, same
			// per-org role). The worker attaches the catalog with its own S3
			// secret and Lakekeeper serves metadata only (using its pod identity
			// for catalog-metadata IO via allowDirectSystemCredentials).
			STSEnabled:           false,
			RemoteSigningEnabled: false,
		},
		StorageCredential: storageCredFor(in.S3),
	}
	wh, err := lkClient.EnsureWarehouse(ctx, whReq)
	if err != nil {
		return fmt.Errorf("ensure lakekeeper warehouse: %w", err)
	}

	// 6. Persist endpoint+credentials back into the warehouse row. Iceberg
	// state flips to Ready as part of the same write.
	//
	// OAUTH2_SERVER_URI is populated only when KubernetesAuthAudiences was
	// passed (PR4 OIDC mode). Without it, Lakekeeper runs in allowall +
	// NetworkPolicy mode and the worker emits an ATTACH with
	// AUTHORIZATION_TYPE 'none' — the empty URI value signals that path
	// downstream via server/iceberg.BuildLakekeeperAttachStmt.
	oauth2URI := ""
	if len(in.KubernetesAuthAudiences) > 0 {
		oauth2URI = lakekeeperbroker.DefaultOAuth2ServerURI
	}
	// NOTE: these are raw DB column names — GORM's Updates(map) uses keys
	// verbatim, bypassing struct field→column mapping. They MUST match the
	// columns AutoMigrate generated from ManagedWarehouseIceberg. GORM
	// snake-cases the field LakekeeperOAuth2ServerURI to "o_auth2_server_uri"
	// (it splits the OAuth2 acronym), so the column is
	// iceberg_lakekeeper_o_auth2_server_uri — NOT ...oauth2.... A mismatch
	// here fails the persist with "column does not exist" (SQLSTATE 42703).
	updates := map[string]interface{}{
		"iceberg_enabled":                                 true,
		"iceberg_backend":                                 configstore.IcebergBackendLakekeeper,
		"iceberg_lakekeeper_endpoint":                     baseURL + "/catalog",
		"iceberg_lakekeeper_warehouse":                    wh.Name,
		"iceberg_lakekeeper_client_id":                    oauthClientID(w.OrgID),
		"iceberg_lakekeeper_o_auth2_server_uri":           oauth2URI,
		"iceberg_lakekeeper_client_credentials_namespace": p.k8s.namespace,
		"iceberg_lakekeeper_client_credentials_name":      secretName,
		"iceberg_lakekeeper_client_credentials_key":       SecretKeyOAuth2ClientSecret,
		// Persist the S3 region so the worker's iceberg S3 secret (built from
		// the duckling's brokered creds) targets the right region for data IO.
		"iceberg_region": in.S3.Region,
		"iceberg_state":  configstore.ManagedWarehouseStateReady,
	}
	_ = creds // creds are written into the Secret; the row only references them
	if err := p.store.UpdateIcebergConfig(w.OrgID, updates); err != nil {
		return fmt.Errorf("persist lakekeeper config: %w", err)
	}
	return nil
}

// checkBootstrap reads the Lakekeeper CR status once. Returns nil when
// bootstrappedAt is non-empty, ErrBootstrapPending otherwise. The caller —
// typically the warehouse-reconcile loop — is responsible for requeueing on
// ErrBootstrapPending. We intentionally don't poll here: the outer loop
// iterates over many orgs per tick, and a per-org sleep would stall every
// other org behind a slow bootstrap. The operator's bootstrap typically
// completes in <10s once the Deployment is ready, well within a normal
// reconcile cadence.
func (p *LakekeeperProvisioner) checkBootstrap(ctx context.Context, orgID string) error {
	st, err := p.k8s.GetCR(ctx, orgID)
	if err != nil {
		return fmt.Errorf("get lakekeeper cr status: %w", err)
	}
	if st == nil || !st.Bootstrapped {
		return ErrBootstrapPending
	}
	return nil
}

// resolveOrGenerateSecret reads the existing per-org Secret if present, or
// generates fresh credentials and writes a new Secret. Generating ONLY on
// first run keeps the Postgres user's password stable across re-runs —
// rotating the Secret would silently de-sync it from the actual PG password.
func (p *LakekeeperProvisioner) resolveOrGenerateSecret(ctx context.Context, orgID, dbName, secretName string) (LakekeeperSecretData, error) {
	existing, err := p.k8s.kubernetes.CoreV1().Secrets(p.k8s.namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err == nil {
		return secretFromExisting(existing), nil
	}
	if !apierrors.IsNotFound(err) {
		return LakekeeperSecretData{}, fmt.Errorf("get existing secret: %w", err)
	}
	// Generate fresh credentials.
	data := LakekeeperSecretData{
		DBUser:             dbName, // Lakekeeper connects as a role named after its DB
		DBPassword:         mustRandomHex(32),
		EncryptionKey:      mustRandomHex(32), // 32 bytes ⇒ 64 hex chars; safely covers any 256-bit key expectation
		OAuth2ClientSecret: mustRandomHex(32),
	}
	if err := p.k8s.EnsureSecret(ctx, orgID, data); err != nil {
		return LakekeeperSecretData{}, err
	}
	return data, nil
}

// secretFromExisting decodes the per-org Secret keys back into a typed
// value. Reads only from Data — the K8s API server clears StringData on
// read and base64-decodes everything into Data, so checking StringData
// first would be dead code in production. The fake clientset echoes
// StringData back; assertSecretData in the unit tests covers both, so
// coverage is unchanged.
func secretFromExisting(s *corev1.Secret) LakekeeperSecretData {
	return LakekeeperSecretData{
		DBUser:             string(s.Data[SecretKeyDBUser]),
		DBPassword:         string(s.Data[SecretKeyDBPassword]),
		EncryptionKey:      string(s.Data[SecretKeyEncryptionKey]),
		OAuth2ClientSecret: string(s.Data[SecretKeyOAuth2ClientSecret]),
	}
}

func validateInputs(in ProvisioningInputs) error {
	if in.AdminDSN == "" {
		return errors.New("ProvisioningInputs.AdminDSN is required")
	}
	if in.PGHost == "" {
		return errors.New("ProvisioningInputs.PGHost is required")
	}
	if in.S3.Bucket == "" || in.S3.Region == "" {
		return errors.New("ProvisioningInputs.S3 Bucket+Region are required")
	}
	if in.S3.Flavor == "" {
		return errors.New("ProvisioningInputs.S3.Flavor is required (\"aws\" or \"s3-compat\")")
	}
	return nil
}

func storageCredFor(s3 S3StorageConfig) WarehouseStorageCredential {
	if s3.StaticAccessKeyID != "" {
		return WarehouseStorageCredential{
			Type:               "s3",
			CredentialType:     "access-key",
			AWSAccessKeyID:     s3.StaticAccessKeyID,
			AWSSecretAccessKey: s3.StaticAccessKeySecret,
		}
	}
	return WarehouseStorageCredential{
		Type:           "s3",
		CredentialType: "aws-system-identity",
	}
}

func lakekeeperDBName(orgID string) string {
	return "lakekeeper_" + lakekeeperResourceSuffix(orgID)
}

func lakekeeperWarehouseName(orgID string) string {
	return "org-" + lakekeeperResourceSuffix(orgID)
}

func oauthClientID(orgID string) string {
	return "duckling-" + lakekeeperResourceSuffix(orgID)
}

func mustRandomHex(byteLen int) string {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand on Linux/macOS doesn't fail in practice; if it does,
		// we cannot proceed safely.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}
