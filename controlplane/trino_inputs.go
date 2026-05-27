//go:build kubernetes

package controlplane

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	"golang.org/x/crypto/bcrypt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// newTrinoKubeClient builds an in-cluster typed kubernetes.Interface for
// the Trino provisioner's Secret + ConfigMap projections. Returns the
// in-cluster client error verbatim so the caller can log+skip rather
// than fatal-out a non-K8s deployment (matches the Lakekeeper-side
// best-effort pattern).
func newTrinoKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return kc, nil
}

// Env-only knobs that wire the customer-Trino provisioner branch. All
// are read at startup; rotation requires a restart (the Trino
// provisioner sub-component itself doesn't observe env changes).
const (
	// envTrinoProvisionerEnabled gates the whole sub-component. Off by
	// default so deployments without a customer Trino cluster carry no
	// extra reconcile cost. Accepted values: "1", "true", "yes" (case-
	// insensitive).
	envTrinoProvisionerEnabled = "DUCKGRES_TRINO_PROVISIONER_ENABLED"

	// envTrinoCoordinatorURL is the customer-Trino coordinator's REST
	// endpoint, e.g. http://trino-coordinator.trino-customer.svc:8080.
	// Required when the provisioner is enabled.
	envTrinoCoordinatorURL = "DUCKGRES_TRINO_COORDINATOR_URL"

	// envTrinoAdminPassword is the plaintext admin password the
	// provisioner presents over HTTP Basic to the customer-Trino
	// coordinator. Trino's file authenticator validates it against the
	// bcrypt hash in password.db.
	envTrinoAdminPassword = "DUCKGRES_TRINO_ADMIN_PASSWORD" //nolint:gosec // env name only

	// envTrinoAdminPasswordHash is the bcrypt hash of the same admin
	// password, projected verbatim into password.db. Must correspond to
	// envTrinoAdminPassword — mismatched values mean the provisioner
	// authenticates with one credential while password.db expects a
	// different one. Document the pairing in the runbook.
	envTrinoAdminPasswordHash = "DUCKGRES_TRINO_ADMIN_PASSWORD_HASH" //nolint:gosec // env name only

	// envTrinoBundleBearerToken is the shared bearer token the bundle
	// HTTP endpoint (opa.Handler) validates against. Must match
	// services.<name>.credentials.bearer.token in the OPA sidecar
	// config.
	envTrinoBundleBearerToken = "DUCKGRES_TRINO_BUNDLE_BEARER_TOKEN" //nolint:gosec // env name only

	// envTrinoIAMAccountID is the AWS account id used to assemble the
	// per-org iceberg.s3.aws-role-arn (the duckling-<orgid> role).
	// Required for AWS deployments; may be empty in dev clusters where
	// per-org S3 access isn't exercised.
	envTrinoIAMAccountID = "DUCKGRES_TRINO_IAM_ACCOUNT_ID"

	// envTrinoAWSRegion is the AWS region for the per-org IAM role
	// binding and Iceberg catalog config. Falls back to the per-org
	// ManagedWarehouseIceberg.Region when empty.
	envTrinoAWSRegion = "DUCKGRES_TRINO_AWS_REGION"

	// envTrinoNamespace overrides provisioner.TrinoCustomerNamespace
	// for the auth Secret + resource-groups ConfigMap projection. Empty
	// = default ("trino-customer").
	envTrinoNamespace = "DUCKGRES_TRINO_NAMESPACE"
)

// trinoProvisionerEnabled reports whether the control plane should
// wire the Trino provisioner branch into the provisioning controller.
// Off by default; matches lakekeeperProvisionerEnabled's gating style.
func trinoProvisionerEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(envTrinoProvisionerEnabled)))
	return v == "1" || v == "true" || v == "yes"
}

// trinoWiring carries the runtime objects multitenant.go has to mount
// onto the provisioning controller + the API server when the Trino
// branch is enabled. Returned together so the caller doesn't have to
// re-derive any of them.
type trinoWiring struct {
	Provisioner *provisioner.TrinoProvisioner
	BundleStore *opa.BundleStore
	// BundleHandler is the HTTP handler the API server mounts for the
	// customer-Trino OPA sidecar's bundle plugin to poll. It does its
	// own bearer-token auth (the bundle exposes the customer roster),
	// independent of the admin API's internal-secret auth.
	BundleHandler *opa.Handler
}

// buildTrinoWiring constructs everything the Trino branch needs from
// env vars + the supplied configstore-backed reads. Returns nil when
// the provisioner is disabled (callers should treat this as "skip the
// Trino branch entirely") and an error when it's enabled but the env
// is incomplete.
//
// The kubernetes.Interface is a typed client suitable for projecting
// Secrets and ConfigMaps into the customer-Trino namespace. In prod
// the caller builds it from rest.InClusterConfig(); in dev/test the
// caller can pass a fake (this function makes no assumptions about
// the cluster being real).
func buildTrinoWiring(store *configstore.ConfigStore, kc kubernetes.Interface) (*trinoWiring, error) {
	if !trinoProvisionerEnabled() {
		return nil, nil
	}

	// Required env. Fail fast on missing values rather than letting
	// the provisioner silently no-op every reconcile tick.
	coordinatorURL := strings.TrimSpace(os.Getenv(envTrinoCoordinatorURL))
	if coordinatorURL == "" {
		return nil, fmt.Errorf("%s is required when %s is set", envTrinoCoordinatorURL, envTrinoProvisionerEnabled)
	}
	adminPassword := os.Getenv(envTrinoAdminPassword)
	adminPasswordHash := os.Getenv(envTrinoAdminPasswordHash)
	bundleToken := os.Getenv(envTrinoBundleBearerToken)
	if adminPassword == "" || adminPasswordHash == "" || bundleToken == "" {
		return nil, errors.New("Trino provisioner enabled but credentials incomplete: " +
			envTrinoAdminPassword + ", " + envTrinoAdminPasswordHash + ", " +
			envTrinoBundleBearerToken + " must all be set (the bundle exposes the customer roster; unauthenticated is never correct)")
	}

	// Verify the plaintext and the bcrypt hash actually correspond.
	// The two env vars have to be a matched pair: the provisioner
	// authenticates with the plaintext over HTTP Basic, while Trino's
	// file authenticator validates against the bcrypt hash. A typo in
	// either makes every catalog operation fail at runtime with a
	// confusing 401. Catching it here turns a runtime mystery into a
	// loud startup failure.
	if err := bcrypt.CompareHashAndPassword([]byte(adminPasswordHash), []byte(adminPassword)); err != nil {
		return nil, fmt.Errorf("%s does not match the bcrypt of %s: %w (the two env vars must be a matched pair; regenerate or fix them in your secret)",
			envTrinoAdminPasswordHash, envTrinoAdminPassword, err)
	}

	catalogClient := provisioner.NewTrinoCatalogHTTPClient(coordinatorURL, opa.AdminPrincipal, adminPassword)
	bundleStore := &opa.BundleStore{}
	bundleHandler := opa.NewHandler(bundleStore, opa.BearerTokenAuth(bundleToken))

	trinoProv, err := provisioner.NewTrinoProvisioner(provisioner.TrinoProvisionerOpts{
		Store:             store,
		IcebergStore:      store,
		Kubernetes:        kc,
		Namespace:         strings.TrimSpace(os.Getenv(envTrinoNamespace)),
		Catalog:           catalogClient,
		BundleStore:       bundleStore,
		BundleBuilder:     opa.NewBuilder(),
		AdminPasswordHash: adminPasswordHash,
		IAMAccountID:      strings.TrimSpace(os.Getenv(envTrinoIAMAccountID)),
		AWSRegion:         strings.TrimSpace(os.Getenv(envTrinoAWSRegion)),
	})
	if err != nil {
		return nil, fmt.Errorf("construct Trino provisioner: %w", err)
	}

	return &trinoWiring{
		Provisioner:   trinoProv,
		BundleStore:   bundleStore,
		BundleHandler: bundleHandler,
	}, nil
}
