//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// newTrinoKubeClient builds an in-cluster typed kubernetes.Interface for
// the Trino provisioner's Secret + ConfigMap projections. Returns the
// in-cluster client error verbatim so the caller can log+skip rather
// than fatal-out a non-K8s deployment.
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

// Shared-pool provisioning settings. Credentials are bootstrapped in Kubernetes Secrets.
const (
	// envTrinoAWSRegion is the FALLBACK region for a per-org catalog whose
	// warehouse row carries no s3_region. The row is authoritative.
	envTrinoAWSRegion = "DUCKGRES_TRINO_AWS_REGION"

	// envTrinoTenantSecretMountPath overrides where the chart mounts the
	// tenant-password Secret inside the Trino pods. It is rendered into
	// every catalog's ducklake.metadata.connection-password-file, so it
	// MUST match the chart's volumeMount; a mismatch makes every catalog
	// fail to open a metadata connection. Empty ==
	// provisioner.DefaultTrinoTenantSecretMountPath.
	envTrinoTenantSecretMountPath = "DUCKGRES_TRINO_TENANT_SECRET_MOUNT_PATH" //nolint:gosec // env var name, not a credential

	// envTrinoS3MaxConnections overrides the per-catalog S3 connection
	// pool bound. Empty or unparseable == the provisioner default.
	envTrinoS3MaxConnections = "DUCKGRES_TRINO_S3_MAX_CONNECTIONS"

	// envTrinoFilesystemCacheEnabled enables caching for newly created catalogs.
	// Empty defaults to false; Trino nodes also need a configured cache manager.
	envTrinoFilesystemCacheEnabled = "DUCKGRES_TRINO_FILESYSTEM_CACHE_ENABLED"

	envTrinoManagedHoglakeURI = "DUCKGRES_TRINO_MANAGED_HOGLAKE_URI"
	envTrinoHoglakeDataPath   = "DUCKGRES_TRINO_HOGLAKE_DATA_PATH"
	envTrinoHoglakeNamespace  = "DUCKGRES_TRINO_HOGLAKE_NAMESPACE"
)

// trinoProvisionerEnabled recognizes explicit shared-pool configuration.
func trinoProvisionerEnabled() bool {
	return strings.TrimSpace(os.Getenv(envTrinoDefaultCell)) != "" || strings.TrimSpace(os.Getenv(envTrinoCellsFile)) != ""
}

// trinoCell separates durable ownership from the operator-visible identity.
// Pool instances share projections for explicitly assigned warehouses.
type trinoCell struct {
	// Mode identifies the shared-pool compute topology.
	Mode         string
	ID           string
	PublicID     string
	RoutingGroup string
	Namespace    string
	ClientURL    string
	// PoolCoordinatorPort is a shared-pool cell's per-instance coordinator
	// Service port. A pool has no CoordinatorURL; its observer reaches each
	// instance on this port instead.
	PoolCoordinatorPort int32
}

// consoleCell exposes the public pool identity without changing stored ownership.
func (c trinoCell) consoleCell() admin.TrinoCell {
	return admin.TrinoCell{
		ID:        c.PublicID,
		StoredID:  c.ID,
		ClientURL: c.ClientURL,
	}
}

// trinoWiring carries the runtime objects multitenant.go has to mount
// onto the provisioning controller + the API server when the Trino
// branch is enabled. Returned together so the caller doesn't have to
// re-derive any of them.
type trinoWiring struct {
	Kubernetes  kubernetes.Interface
	Provisioner *provisioner.TrinoProvisioner
	BundleStore *opa.BundleStore
	// BundleHandler is the HTTP handler the API server mounts for the
	// cell's OPA sidecar's bundle plugin to poll. Authenticated with the
	// bundle bearer token, which is generated once at cluster bootstrap
	// and stable for the process lifetime (rotation is a follow-up
	// rotation-API concern).
	BundleHandler *opa.Handler
	// Cell is the cell this wiring reconciles; carried out for startup
	// logging.
	Cell trinoCell
	// Console is what the admin console needs to observe this cell. Built
	// here rather than in multitenant.go so the observer credential is read
	// through the provisioner that owns it and nothing else has to know how
	// the coordinator is authenticated.
	Console   *trinoConsoleWiring
	Observers []admin.TrinoCoordinatorClient
}

// trinoConsoleWiring is the admin console's half of the Trino branch: the
// cell's identity plus a coordinator client authenticated as the OBSERVER
// principal.
//
// The observer is deliberately NOT the provisioner's admin principal. The
// admin can CREATE/DROP catalogs and, by policy, sees only its own queries;
// the observer sees every tenant's query metadata and holds no catalog at
// all. Keeping them separate means a leak of either credential yields one
// half of that authority, never both. See opa.ObserverPrincipal.
type trinoConsoleWiring struct {
	Cell     admin.TrinoCell
	Observer admin.TrinoCoordinatorClient
}

type trinoWiringStore interface {
	provisioner.TrinoStore
	provisioner.TrinoBootstrapSentinelStore
	provisioner.TrinoWarehouseStore
}

func buildTrinoCellWiring(store trinoWiringStore, kc kubernetes.Interface, ducklings provisioner.TrinoDucklingResolver, cell trinoCell, storageResolvers ...provisioner.TrinoDucklingResolver) (*trinoWiring, error) {

	filesystemCacheEnabled, err := trinoFilesystemCacheEnabled()
	if err != nil {
		return nil, err
	}

	managedHoglake, err := trinoManagedHoglakeConfig()
	if err != nil {
		return nil, err
	}

	if ducklings == nil {
		// Without it every catalog sits pending forever waiting on a
		// duckling status that nothing resolves — a silent, permanent
		// half-provisioned cell. Fail the rollout instead.
		return nil, fmt.Errorf("trino provisioner requires a duckling status resolver (the Duckling CR client is unavailable)")
	}

	catalogClient := newUnavailableTrinoCatalogClient(cell.ID)

	var storageResolver provisioner.TrinoDucklingResolver
	if len(storageResolvers) > 0 {
		storageResolver = storageResolvers[0]
	}
	bundleStore := &opa.BundleStore{}

	trinoProv, err := provisioner.NewTrinoProvisioner(provisioner.TrinoProvisionerOpts{
		Store:                  store,
		BootstrapSentinel:      store,
		Warehouses:             store,
		Ducklings:              ducklings,
		HoglakeDucklings:       storageResolver,
		Kubernetes:             kc,
		Namespace:              cell.Namespace,
		CellID:                 cell.ID,
		TenantSecretMountPath:  strings.TrimSpace(os.Getenv(envTrinoTenantSecretMountPath)),
		Catalog:                catalogClient,
		BundleStore:            bundleStore,
		BundleBuilder:          opa.NewBuilder(),
		AWSRegion:              strings.TrimSpace(os.Getenv(envTrinoAWSRegion)),
		S3MaxConnections:       envInt(envTrinoS3MaxConnections),
		FilesystemCacheEnabled: filesystemCacheEnabled,
		ManagedHoglake:         managedHoglake,
	})
	if err != nil {
		return nil, fmt.Errorf("construct Trino provisioner: %w", err)
	}

	// Synchronous bootstrap: generates (first install) or loads the
	// cluster credentials, seeds the catalog client's admin credential,
	// and returns the OPA bundle bearer token. Bounded by a generous
	// timeout — if Postgres or the K8s API server is unreachable for
	// minutes, that's a hard startup failure surfaced in the pod logs.
	bootstrapCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bundleToken, err := trinoProv.Bootstrap(bootstrapCtx)
	if err != nil {
		return nil, fmt.Errorf("bootstrap Trino cluster secrets: %w", err)
	}

	// Build the handler with the real token directly — no placeholder,
	// no post-construction swap, so no window where the endpoint serves
	// with a token a real client could match by accident.
	bundleHandler := opa.NewHandler(bundleStore, opa.BearerTokenAuth(bundleToken))
	lister, ok := store.(trinoPoolInstanceLister)
	if !ok {
		return nil, fmt.Errorf("trino cell %s cannot list pool instances", cell.PublicID)
	}
	observer := newTrinoPoolObserver(cell.ID, cell.Namespace, cell.PoolCoordinatorPort, lister, trinoProv.ObserverCredential)
	observers := []admin.TrinoCoordinatorClient{observer}

	return &trinoWiring{
		Kubernetes:    kc,
		Provisioner:   trinoProv,
		BundleStore:   bundleStore,
		BundleHandler: bundleHandler,
		Cell:          cell,
		Observers:     observers,
		Console: &trinoConsoleWiring{
			Cell: cell.consoleCell(),
			// Read the credential through the provisioner on every call
			// rather than capturing it here: the pair is regenerated if it
			// ever goes missing, and a captured copy would 401 forever
			// after that self-heal.
			Observer: observer,
		},
	}, nil
}

// The provisioner reads a tenant's duckling status through the control
// plane's existing Duckling CR reader, passed in by the caller.
//
// This is deliberately the SAME read the worker activation path performs
// (SharedWorkerActivator.buildDuckLakeConfigFromDuckling reaches the
// password through the same resolver, which resolves
// status.metadataStore.credentialSecretRef into a plaintext) rather than a
// parallel implementation, so Trino and the DuckDB workers can never end up
// authenticating a tenant's metadata store with two different credentials.
//
// A duckling with no status published yet returns (nil, nil) — a WAIT, which
// leaves the org provisioning until the composition catches up. Only a genuine
// read failure is an error.
//
// The provisioner takes the whole status rather than just the password because
// the catalog needs the object-store inputs too — bucket, region and the
// per-org IAM role — and those live ONLY here. The config store's
// ManagedWarehouse carries s3.* and worker_identity.* columns, but nothing
// populates them for a DuckLake warehouse: the bucket lands on
// data_store.bucket_name and the role is a Crossplane composition output
// published to the Duckling CR's status. Reading them from the same status the
// password comes from also keeps Trino and the DuckDB workers pointed at the
// same bucket under the same identity, which is the property that matters.

// envInt reads a non-negative integer env var, returning 0 for unset,
// blank, or unparseable values so the caller's own default applies.
func envInt(name string) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// trinoFilesystemCacheEnabled rejects invalid settings rather than silently
// benchmarking a different cache mode than the operator requested.
func trinoFilesystemCacheEnabled() (bool, error) {
	value := strings.TrimSpace(os.Getenv(envTrinoFilesystemCacheEnabled))
	if value == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", envTrinoFilesystemCacheEnabled)
	}
	return enabled, nil
}

func trinoManagedHoglakeConfig() (*provisioner.TrinoManagedHoglakeConfig, error) {
	uri := strings.TrimSpace(os.Getenv(envTrinoManagedHoglakeURI))
	path := strings.TrimSpace(os.Getenv(envTrinoHoglakeDataPath))
	namespace := strings.TrimSpace(os.Getenv(envTrinoHoglakeNamespace))
	if uri == "" && path == "" && namespace == "" {
		return nil, nil
	}
	if namespace == "" {
		namespace = "main"
	}
	config := &provisioner.TrinoManagedHoglakeConfig{URI: uri, DataPath: path, Namespace: namespace}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("managed Hoglake configuration: %w", err)
	}
	return config, nil
}
