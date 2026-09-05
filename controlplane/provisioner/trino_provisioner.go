//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TrinoProvisionerSource is the `X-Trino-Source` the provisioner stamps on
// its catalog-management statements. Trino records the header verbatim as
// the `source` column of system.runtime.queries and shows it in the query
// listing, so tagging it is what lets an operator tell control-plane
// traffic apart from tenant SQL — including filtering it out of the admin
// console's own live-query view. Untagged, every reconcile tick's SHOW
// CATALOGS looks like a mystery query from a privileged user.
const TrinoProvisionerSource = "duckgres-provisioner"

// TrinoCustomerNamespace is the K8s namespace where the shared Trino cell
// lives. The auth Secret, tenant-password Secret and resource-groups
// ConfigMap are projected into this namespace by the provisioner.
const TrinoCustomerNamespace = "trino-customer"

// TrinoAuthSecretName is the K8s Secret that holds the projected
// password.db + group.db. Mounted only into the coordinator pod, not
// workers — workers don't authenticate; the blast radius of a worker pod
// compromise stays away from the bcrypt hashes.
const TrinoAuthSecretName = "trino-auth"

// Trino auth secret data keys.
const (
	TrinoAuthSecretKeyPasswordDB = "password.db"
	TrinoAuthSecretKeyGroupDB    = "group.db"
)

// TrinoResourceGroupsConfigMapName is the K8s ConfigMap that holds the
// generated resource-groups.json file consumed by the Trino coordinator
// via resource-groups.config-file.
const TrinoResourceGroupsConfigMapName = "trino-resource-groups"

// TrinoResourceGroupsConfigMapKey is the data key under which the JSON
// file is stored. Matches the file name Trino expects via
// resource-groups.config-file mount.
const TrinoResourceGroupsConfigMapKey = "resource-groups.json"

// TrinoInternalCommunicationSecretName is the K8s Secret that holds the
// Trino node-to-node `internal-communication.shared-secret`. Projected
// as the TRINO_INTERNAL_COMMUNICATION_SHARED_SECRET env var into both
// coordinator and worker pods by the chart. Generated and self-owned
// by the provisioner (ensureClusterSecrets); the K8s Secret is the
// source of truth for its value.
const TrinoInternalCommunicationSecretName = "trino-internal-communication"

// TrinoInternalCommunicationSecretKey is the data key on the
// internal-communication Secret. Matches the env var the chart pulls
// via secretKeyRef in coordinator-deployment.yaml + worker-deployment.yaml.
const TrinoInternalCommunicationSecretKey = "shared-secret"

// TrinoOPABundleTokenSecretName is the K8s Secret that holds the
// bearer token the customer-Trino OPA sidecar presents when polling
// the duckgres provisioner's /bundles/trino endpoint. The provisioner
// generates it once and the K8s Secret is the source of truth; the
// chart's OPA sidecar reads the same Secret via tokenSecretName, so no
// cross-namespace Reflector is needed.
const TrinoOPABundleTokenSecretName = "trino-opa-bundle-token"

// TrinoOPABundleTokenSecretKey is the data key on the bundle-token
// Secret. Matches the OPA sidecar's services.<name>.credentials.bearer.token
// configuration (see chart's configmap-opa.yaml).
const TrinoOPABundleTokenSecretKey = "token"

// Additional keys on TrinoAuthSecretName beyond password.db / group.db.
// The auth Secret carries the projected file content (consumed by the
// coordinator's file-password-authenticator) plus the admin
// principal's plaintext + bcrypt hash (consumed by the provisioner's
// own catalog REST client). Keeping all four keys on one Secret reduces
// the chart-side mount surface; the admin pair is written/regenerated
// together by ensureAdminCredential, merged into the Secret so the
// per-tick password.db/group.db projection isn't clobbered.
const (
	TrinoAuthSecretKeyAdminPassword     = "admin-password"
	TrinoAuthSecretKeyAdminPasswordHash = "admin-password-hash" //nolint:gosec // K8s secret key name, not a hardcoded credential
)

// The observer principal's plaintext + bcrypt hash, consumed by the
// control plane's admin console when it reads the cell's live query and
// node state over the coordinator REST API.
//
// Deliberately a SECOND credential on the same Secret rather than a reuse
// of the admin pair: the admin principal can CREATE/DROP catalogs and sees
// only its own queries; the observer sees every tenant's query metadata
// and owns no catalog at all. Fusing them would make one leaked credential
// yield both halves. See opa.ObserverPrincipal for the full argument, and
// policy.rego's "Observer principal" section for the enforcement.
//
// Same lifecycle as the admin pair (ensureCredentialPair): regenerated if
// missing, because the provisioner owns both sides and nothing external
// consumes the value, so loss self-heals within one password-file refresh
// instead of wedging the cell.
const (
	TrinoAuthSecretKeyObserverPassword     = "observer-password"
	TrinoAuthSecretKeyObserverPasswordHash = "observer-password-hash" //nolint:gosec // K8s secret key name, not a hardcoded credential
)

// TrinoTenantSecretName is the K8s Secret holding ONE KEY PER
// TRINO-ENABLED ORG whose value is that org's DuckLake metadata-store
// password. The chart mounts it into the coordinator (and workers, which
// also open metadata connections) at TenantSecretMountPath, and each org's
// catalog names its own file through
// `ducklake.metadata.connection-password-file`.
//
// Why a file and not a catalog property: Trino echoes the full
// `CREATE CATALOG ... WITH (...)` statement into its query log and its web
// UI, and ships catalog properties to every worker. A password placed in a
// property is therefore readable by anyone who can see a query listing.
// The file indirection keeps the secret in a Secret and the catalog
// carrying only a path.
//
// The projection is AUTHORITATIVE, not additive: keys for orgs that are no
// longer enabled are removed on the next tick, so a disabled org's password
// stops being mounted even though the (now dropped) catalog no longer reads
// it.
const TrinoTenantSecretName = "trino-tenant-secrets" //nolint:gosec // K8s object name, not a credential

// DefaultTrinoTenantSecretMountPath is where the chart mounts
// TrinoTenantSecretName inside the Trino pods. The provisioner never reads
// this path itself — it only renders it into each catalog's
// `ducklake.metadata.connection-password-file` — so it MUST agree with the
// chart's volumeMount. Override with DUCKGRES_TRINO_TENANT_SECRET_MOUNT_PATH
// if the chart mounts it elsewhere.
const DefaultTrinoTenantSecretMountPath = "/etc/trino/tenant-secrets"

// defaultTrinoS3MaxConnections bounds the per-catalog S3 client pool. Every
// enabled org gets its own catalog and therefore its own pool; on a cell
// with hundreds of tenants the connector default (a much larger number)
// multiplies into an unusable file-descriptor and heap footprint on the
// coordinator. Small on purpose: a tenant that saturates 50 concurrent S3
// requests is already at its resource-group concurrency limit.
const defaultTrinoS3MaxConnections = 50

// metadataStoreDefaultPort is the Postgres port assumed when the warehouse
// row carries no explicit port (cnpg-shard rows are mirrored from a CR
// status that publishes no port; the pooler serves 5432).
const metadataStoreDefaultPort = 5432

// secretDataKeyPattern is the K8s Secret data-key grammar
// (`[-._a-zA-Z0-9]+`). Org IDs are validated as DNS-1123 labels at the
// provisioning endpoint, so they always satisfy it; the check exists so a
// future loosening of org-ID validation surfaces as one org's clear error
// rather than a rejected write that takes the WHOLE tenant Secret (and with
// it every other org's password) down with it.
var secretDataKeyPattern = regexp.MustCompile(`^[-._a-zA-Z0-9]+$`)

// managedCatalogRe is opa.ManagedCatalogPattern compiled for the Go side.
// It decides which catalogs on the coordinator this provisioner owns and
// may therefore DROP; anything else (system, jmx, a hand-made maintenance
// catalog) is left alone. Compiling the OPA constant rather than
// re-spelling the shape is what keeps the drop filter and the policy's
// admin authority from drifting apart.
var managedCatalogRe = regexp.MustCompile(opa.ManagedCatalogPattern)

// trinoCatalogIdentifier is the Trino catalog identifier grammar
// ([a-z0-9_]+). Anything outside this set in Org.Name is replaced with
// `_` before forming the catalog name.
var trinoCatalogIdentifier = regexp.MustCompile(`[^a-z0-9_]`)

// trinoSanitize lowercases and replaces non-[a-z0-9_] runs with `_`.
// Pure function so callers can recover the sanitized name without
// holding the provisioner.
func trinoSanitize(orgName string) string {
	lower := strings.ToLower(orgName)
	return trinoCatalogIdentifier.ReplaceAllString(lower, "_")
}

// TrinoCatalogName returns the catalog identifier for an org.
// Format: org_<sanitized>. The sanitization maps Org.Name to Trino
// identifier rules ([a-z0-9_]); any other characters collapse to
// underscores.
//
// For principals that satisfy ValidateDatabaseName the mapping is injective
// — that grammar allows only lowercase alphanumerics and hyphens, so the
// hyphen is the only character rewritten and no valid principal contains the
// underscore it becomes — which, with database_name's global unique index,
// makes distinct orgs' catalog names distinct by construction. Grandfathered
// rows predate the validation and can still converge; rejectPrincipalCollisions
// holds those orgs back rather than letting one read the other's catalog.
//
// The name carried an `_iceberg` suffix while the backing table format was
// Iceberg behind Lakekeeper. Warehouses are DuckLake now (migration 000014
// dropped every iceberg_* column), so the suffix went with it. The shape is
// pinned from three sides — this function, opa.ManagedCatalogPattern, and
// the regex literal inside policy.rego — and the pair of tests named in
// ManagedCatalogPattern's doc comment fails if any one of them moves alone.
func TrinoCatalogName(principal string) string {
	return "org_" + trinoSanitize(principal)
}

// TrinoGroupName returns the file-group-provider group label for an org,
// derived from its Trino principal.
// Format: org_<sanitized>. Matches the OPA `input.context.identity.groups`
// the customer Trino sends with each decision request.
func TrinoGroupName(principal string) string {
	return "org_" + trinoSanitize(principal)
}

// trinoUsernamePattern is the grammar a duckgres username must satisfy to be
// projected into the cell's auth files.
//
// This is an ALLOWLIST, and it is a security control rather than a tidiness
// one. duckgres validates a username as little more than "not empty" (see
// controlplane/validation.go), while password.db is `<user>:<hash>` per line
// and group.db is `<group>:<user>,<user>` per line. A username holding `:`,
// `,` or a newline would not merely render oddly -- it would let whoever can
// create org users append arbitrary lines to those files, including a line
// for the admin principal. Anything outside this grammar is therefore never
// written, and no amount of downstream escaping is relied on.
//
// `.` is excluded as well, so that `<org>.<user>` carries exactly the one
// separator TrinoPrincipalSeparator puts there and the org prefix stays
// recoverable by the resource-group selector (see orgCaptureRegex).
var trinoUsernamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]*$`)

// projectableTrinoUsername reports whether a duckgres username is safe to
// render into password.db / group.db.
func projectableTrinoUsername(username string) bool {
	return len(username) <= 255 && trinoUsernamePattern.MatchString(username)
}

// TrinoScopeGroupName returns the group label for a project-scoped login:
// one group per (org, team), carrying that team's schema scope in the OPA
// bundle.
//
// The `scope_` prefix keeps these out of TrinoGroupName's `org_<name>` space
// and TrinoTierGroupName's `tier_` space. That separation matters: a group in
// the `org_` space that the bundle happens not to scope reads the whole
// catalog, so a scope group whose name could collide with an org group would
// be a silent widening rather than a name clash.
func TrinoScopeGroupName(principal string, teamID int64) string {
	return fmt.Sprintf("scope_%s_team_%d", trinoSanitize(principal), teamID)
}

// TrinoResourceGroupName returns the resource-group selector key for
// an org. Sanitized like the catalog name so a `.` in orgName doesn't
// get re-interpreted as a hierarchy separator in Trino's resource-
// group path (and so the selector + subgroup names stay aligned across
// the catalog, group, and resource-group projections). Customer
// principals' Trino username == the principal (a DNS-1123 label), and we
// sanitize defensively so a non-identifier char can't reshape the path.
func TrinoResourceGroupName(principal string) string {
	return "root.tenants." + trinoSanitize(principal)
}

// TrinoCatalogClient is the REST surface the provisioner needs against
// the customer Trino cluster: enumerate, create, alter, drop catalogs.
// Concrete implementation in trinoCatalogHTTPClient below; the interface
// is exported so tests can inject a fake at the function boundary.
type TrinoCatalogClient interface {
	ListCatalogs(ctx context.Context) ([]string, error)
	CreateCatalog(ctx context.Context, name string, props map[string]string) error
	AlterCatalog(ctx context.Context, name string, props map[string]string) error
	DropCatalog(ctx context.Context, name string) error
}

// TrinoProvisionerOpts groups all the dependencies trino_provisioner.go
// needs. Each is required at construction time — partial wiring would
// cause silent reconcile no-ops, which we'd rather surface at startup.
type TrinoProvisionerOpts struct {
	// Store is the cross-cutting Trino read/write surface.
	Store TrinoStore

	// BootstrapSentinel is the configstore-backed "have the cluster
	// credentials ever been generated" bit. The K8s Secrets are the
	// source of truth for the credential VALUES; this sentinel only
	// lets ensureClusterSecrets tell first-boot (generate) from a
	// post-bootstrap missing Secret (fail loud — regenerating the
	// env-projected internal-communication secret would split-brain a
	// running Trino cluster). No credential bytes or refs are stored.
	BootstrapSentinel TrinoBootstrapSentinelStore

	// Warehouses is the per-org warehouse read surface. Every input to a
	// DuckLake catalog except the password comes from this row: the
	// metadata_store_* block (endpoint/port/database/user/kind), the s3_*
	// block (bucket/prefix/region) and worker_identity_iam_role_arn — the
	// same fields GET /api/v1/warehouses serves. Must return (nil, nil)
	// for an org with no warehouse row so the reconcile can WAIT rather
	// than fail an org that opted into Trino before it was provisioned.
	Warehouses TrinoWarehouseStore

	// TenantPasswords resolves one org's DuckLake metadata-store password.
	// Production wiring hands over the same Duckling CR read the worker
	// activation path uses (DucklingClient.Get resolves
	// status.metadataStore.credentialSecretRef into a plaintext), so
	// there is exactly ONE definition of "the tenant's metadata password"
	// in the control plane. Required: without it the tenant Secret cannot
	// be projected and every catalog would sit pending forever.
	Ducklings TrinoDucklingResolver

	// Kubernetes is used for the auth Secret, tenant-password Secret and
	// resource-groups ConfigMap projections in the Trino namespace.
	Kubernetes kubernetes.Interface

	// Namespace overrides TrinoCustomerNamespace. Empty == default.
	// Useful for dev clusters that namespace Trino differently.
	Namespace string

	// CellID is the Trino cell this provisioner reconciles. It claims
	// Trino-enabled orgs with no cell yet, reconciles the orgs stamped
	// with this cell, and IGNORES orgs stamped with any other cell.
	// Empty == configstore.DefaultTrinoCellID.
	CellID string

	// TenantSecretMountPath is the in-pod path the chart mounts
	// TrinoTenantSecretName at; each org's catalog points its
	// connection-password-file at <TenantSecretMountPath>/<orgID>. Empty ==
	// DefaultTrinoTenantSecretMountPath. It must match the chart or every
	// catalog fails to open a metadata connection.
	TenantSecretMountPath string

	// Catalog is the Trino REST client used to issue CREATE / ALTER /
	// DROP CATALOG. The provisioner authenticates as opa.AdminPrincipal;
	// the underlying HTTP client's Basic-auth credential is now set
	// from the bootstrapped admin plaintext at first Reconcile (the
	// caller passes in a SetCredentials-aware client, see
	// TrinoCatalogClient interface). For unit tests a fake catalog
	// client with no auth surface is fine.
	Catalog TrinoCatalogClient

	// BundleStore is the in-memory holder of the most recently built OPA
	// bundle. The provisioner Set()s into it on every reconcile tick.
	BundleStore *opa.BundleStore

	// BundleBuilder builds the OPA bundle (gzip tarball) from a
	// GroupCatalogs map. In production, pass opa.NewBuilder().
	BundleBuilder opa.BundleBuilder

	// AWSRegion is the FALLBACK region for a catalog whose warehouse row
	// carries no s3_region. The row is authoritative; this only covers
	// rows that predate the column being populated. Empty in dev / unit
	// tests, in which case a region-less warehouse stays pending rather
	// than producing a catalog the S3 client cannot address.
	AWSRegion string

	// S3MaxConnections overrides defaultTrinoS3MaxConnections for the
	// per-catalog S3 client pool. Zero == the default.
	S3MaxConnections int
}

// TrinoBootstrapSentinelStore is the narrow configstore surface the
// provisioner uses for the one-bit "ever bootstrapped" sentinel.
// Defined here (not importing configstore wholesale) so the provisioner
// stays test-substitutable. Production wiring is just
// `opts.BootstrapSentinel = configStore`.
//
//   - IsBootstrapped reports whether the cluster credentials for the
//     namespace have been generated at least once.
//   - MarkBootstrapped records that they have (idempotent). Called only
//     after all three K8s Secrets are confirmed present + valid.
type TrinoBootstrapSentinelStore interface {
	IsTrinoClusterBootstrapped(ctx context.Context, namespace string) (bool, error)
	MarkTrinoClusterBootstrapped(ctx context.Context, namespace string) error
}

// TrinoCatalogCredentialUpdater is the optional credential-rotation
// hook a catalog client exposes. Some HTTP-backed clients need to
// rebuild the cached Basic-auth header when the admin password
// changes; clients without runtime-mutable credentials (test fakes,
// bearer-token clients) implement no-op or omit the interface entirely
// (the provisioner type-asserts on each Reconcile).
type TrinoCatalogCredentialUpdater interface {
	SetCredentials(username, password string)
}

// TrinoStore is the read/write surface trino_provisioner.go uses for the
// per-reconcile projection inputs and outcomes. Defined as a narrow
// interface so unit tests can swap a fake and so the type doesn't drag the
// configstore import everywhere.
type TrinoStore interface {
	ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error)
	UpdateTrinoState(orgID string, upd configstore.TrinoStateUpdate) error
	// AssignTrinoCell claims an org with no cell into this provisioner's
	// cell. Only ever writes rows whose cell is still unset.
	AssignTrinoCell(orgID, cellID string) error
}

// TrinoWarehouseStore reads a single org's warehouse row to populate the
// per-org DuckLake catalog properties. Backed by configstore.ConfigStore's
// GetManagedWarehouseForTrino, whose contract this mirrors: (nil, nil) when
// the org has no warehouse row yet.
type TrinoWarehouseStore interface {
	GetManagedWarehouseForTrino(orgID string) (*configstore.ManagedWarehouse, error)
}

// TrinoTenantPasswordResolver returns the plaintext DuckLake metadata-store
// password for one org, or an error. An empty password with a nil error
// means "the org's duckling has not published a credential yet" — a WAIT,
// not a failure: the org stays provisioning and the next tick retries.
//
// The plaintext is handled in exactly two places: this call, and the write
// into TrinoTenantSecretName. It is never logged, never put in a catalog
// property, and never persisted in the config store.
type TrinoDucklingResolver func(ctx context.Context, orgID string) (*DucklingStatus, error)

// TrinoProvisioner owns the customer Trino cluster's projected state:
// cluster-level Secrets (internal-communication shared secret, auth
// password + group files, OPA bundle bearer token), per-org auth file
// projection, resource-groups JSON, OPA bundle, and catalog REST state.
//
// Per-tick deterministic projection: given the same inputs, every K8s
// write produces byte-equal output, so re-running a tick is a no-op
// against settled state. Cluster-secret GENERATION is the one
// non-deterministic step — gated on the bootstrap sentinel, so it only
// fires on first install; thereafter ensureClusterSecrets adopts the
// existing K8s Secrets.
type TrinoProvisioner struct {
	store                 TrinoStore
	bootstrapSentinel     TrinoBootstrapSentinelStore
	warehouses            TrinoWarehouseStore
	ducklings             TrinoDucklingResolver
	kubernetes            kubernetes.Interface
	namespace             string
	cellID                string
	catalog               TrinoCatalogClient
	bundleStore           *opa.BundleStore
	bundleBuilder         opa.BundleBuilder
	tenantSecretMountPath string
	awsRegion             string
	s3MaxConnections      int

	// adminPasswordHash is cached on each Reconcile from the
	// trino-auth K8s Secret and prepended to password.db on projection.
	// Empty until the first Reconcile.
	adminPasswordHash string

	// observerPassword / observerPasswordHash are the admin console's
	// Trino credential, cached from the same Secret. The hash is
	// projected into password.db; the plaintext is handed to the console
	// wiring through ObserverCredential and never logged, never written
	// to the config store, and never rendered into a catalog property.
	//
	// Guarded because ObserverCredential is read by the API layer on a
	// different goroutine from the reconcile loop that writes them.
	credMu               sync.RWMutex
	observerPassword     string
	observerPasswordHash string
}

// ObserverCredential returns the Trino username + plaintext password the
// admin console authenticates to the coordinator with. Empty password
// until the first Bootstrap has run.
//
// The console is a READ-ONLY consumer of this credential: policy.rego
// grants the observer cluster-wide query visibility (and kill) plus
// ReadSystemInformation, and nothing else -- notably no catalog access, so
// this credential cannot read a row of tenant data.
func (p *TrinoProvisioner) ObserverCredential() (username, password string) {
	p.credMu.RLock()
	defer p.credMu.RUnlock()
	return opa.ObserverPrincipal, p.observerPassword
}

// observerHash reads the cached bcrypt hash for the password.db
// projection.
func (p *TrinoProvisioner) observerHash() string {
	p.credMu.RLock()
	defer p.credMu.RUnlock()
	return p.observerPasswordHash
}

// setObserverCredential caches the minted pair for the projection step
// and for ObserverCredential.
func (p *TrinoProvisioner) setObserverCredential(plaintext, hash string) {
	p.credMu.Lock()
	defer p.credMu.Unlock()
	p.observerPassword, p.observerPasswordHash = plaintext, hash
}

// NewTrinoProvisioner constructs a TrinoProvisioner from required deps.
// Returns an error if any required dep is missing rather than panicking
// downstream on the first reconcile tick.
func NewTrinoProvisioner(opts TrinoProvisionerOpts) (*TrinoProvisioner, error) {
	if opts.Store == nil {
		return nil, errors.New("TrinoProvisioner: Store is required")
	}
	if opts.BootstrapSentinel == nil {
		return nil, errors.New("TrinoProvisioner: BootstrapSentinel is required")
	}
	if opts.Warehouses == nil {
		return nil, errors.New("TrinoProvisioner: Warehouses is required")
	}
	if opts.Ducklings == nil {
		return nil, errors.New("TrinoProvisioner: Ducklings is required")
	}
	if opts.Kubernetes == nil {
		return nil, errors.New("TrinoProvisioner: Kubernetes client is required")
	}
	if opts.Catalog == nil {
		return nil, errors.New("TrinoProvisioner: Catalog client is required")
	}
	if opts.BundleStore == nil {
		return nil, errors.New("TrinoProvisioner: BundleStore is required")
	}
	if opts.BundleBuilder == nil {
		return nil, errors.New("TrinoProvisioner: BundleBuilder is required")
	}
	ns := opts.Namespace
	if ns == "" {
		ns = TrinoCustomerNamespace
	}
	cell := opts.CellID
	if cell == "" {
		cell = configstore.DefaultTrinoCellID
	}
	mountPath := opts.TenantSecretMountPath
	if mountPath == "" {
		mountPath = DefaultTrinoTenantSecretMountPath
	}
	maxConns := opts.S3MaxConnections
	if maxConns <= 0 {
		maxConns = defaultTrinoS3MaxConnections
	}
	return &TrinoProvisioner{
		store:                 opts.Store,
		bootstrapSentinel:     opts.BootstrapSentinel,
		warehouses:            opts.Warehouses,
		ducklings:             opts.Ducklings,
		kubernetes:            opts.Kubernetes,
		namespace:             ns,
		cellID:                cell,
		catalog:               opts.Catalog,
		bundleStore:           opts.BundleStore,
		bundleBuilder:         opts.BundleBuilder,
		tenantSecretMountPath: strings.TrimRight(mountPath, "/"),
		awsRegion:             opts.AWSRegion,
		s3MaxConnections:      maxConns,
	}, nil
}

// CellID reports the Trino cell this provisioner owns. Exposed for
// startup logging and tests.
func (p *TrinoProvisioner) CellID() string { return p.cellID }

// Reconcile runs one full projection: cluster secrets → auth files →
// resource groups → OPA bundle → tenant passwords → catalogs. Errors in
// any one output are logged and surfaced but the next output still runs —
// a failing OPA push shouldn't leave the password file stale, and vice
// versa. The caller's next tick re-runs everything.
//
// Returns a multi-error wrapping every per-step error so callers can
// surface them in observability without losing detail. nil iff every
// step succeeded.
func (p *TrinoProvisioner) Reconcile(ctx context.Context) error {
	// 0. Cluster-level Secrets. Bootstrap-or-load on every tick (cheap
	//    after first run; the configstore row exists and the call
	//    returns it without touching K8s). Catalog REST calls in step 5
	//    authenticate as the admin principal whose password lives in
	//    the trino-auth Secret this step ensures exists — so this MUST
	//    run before any catalog REST call, otherwise cold-start hits a
	//    401 against an empty password.db.
	//
	//    Failure here is fatal for the tick: catalog reconciles would
	//    401 and auth projection would have nothing to project the
	//    admin lines from. The next tick retries.
	if _, err := p.ensureClusterSecrets(ctx); err != nil {
		return fmt.Errorf("ensure trino cluster secrets: %w", err)
	}

	allOrgs, err := p.store.ListTrinoEnabledOrgs()
	if err != nil {
		return fmt.Errorf("list trino-enabled orgs: %w", err)
	}

	// Narrow the fleet-wide listing to the orgs THIS cell owns, claiming
	// the unassigned ones on the way through. Everything downstream —
	// password file, group file, tenant Secret, resource groups, OPA
	// bundle, catalogs — is a projection of exactly this slice, so a
	// tenant that belongs to another cell is invisible to every step.
	orgs := p.claimCellOrgs(allOrgs)

	// Stable iteration order so logs and projections are deterministic
	// regardless of how the DB driver returned the rows.
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].OrgID < orgs[j].OrgID })

	// Orgs whose principals collide are held back from EVERY projection
	// below. This has to happen before the five steps, not inside the
	// catalog step: if a colliding org still reached the OPA bundle, its
	// group would be granted the shared catalog name and it could read the
	// other tenant's data through the catalog that org legitimately owns.
	projectable, collisions := rejectPrincipalCollisions(orgs)

	var errs []error

	// 1. Auth file projection (K8s Secret). Atomic Secret update.
	//    Runs BEFORE catalogs so the admin lines exist in password.db
	//    + group.db when the catalog client's first request reaches
	//    the coordinator on a cold-start tick.
	authErr := p.reconcileAuthSecret(ctx, projectable)
	if authErr != nil {
		errs = append(errs, fmt.Errorf("reconcile auth secret: %w", authErr))
	}

	// 2. Resource groups (K8s ConfigMap). Generated from the per-org
	//    tier; rebuilt every tick. Also runs before catalogs so the
	//    coordinator's resource-groups manager has a valid file when
	//    catalog creation kicks off queries on tier-specific groups.
	rgErr := p.reconcileResourceGroups(ctx)
	if rgErr != nil {
		errs = append(errs, fmt.Errorf("reconcile resource groups: %w", rgErr))
	}

	// 3. OPA bundle. GroupCatalogs keyed by Trino group name; Set into
	//    the in-memory store the bundle HTTP handler serves. Pre-
	//    catalog so the OPA sidecar's authorization decisions for the
	//    catalog reconcile's own queries see the up-to-date roster.
	opaErr := p.reconcileOPABundle(ctx, projectable)
	if opaErr != nil {
		errs = append(errs, fmt.Errorf("reconcile opa bundle: %w", opaErr))
	}

	// 4. Tenant metadata-store passwords (K8s Secret). One key per
	//    enabled org; keys for orgs that left the set are removed. Runs
	//    before catalogs because a catalog's
	//    `ducklake.metadata.connection-password-file` names a path that
	//    must already hold the org's password — a catalog created ahead
	//    of its key would fail every metadata connection.
	//
	//    Note the kubelet's own projection lag: a key written here is
	//    visible inside the pod within its sync period (tens of seconds),
	//    not instantly. A catalog created in the same tick as its key can
	//    therefore still fail its first connection; that surfaces as a
	//    per-org catalog error and the next tick retries against a Secret
	//    that has since landed.
	tenants, tenantErr := p.reconcileTenantSecrets(ctx, projectable)
	if tenantErr != nil {
		errs = append(errs, fmt.Errorf("reconcile tenant secrets: %w", tenantErr))
	}

	// 5. Catalogs (REST). Per-org idempotent CREATE; orgs disabled
	//    since last tick get DROP. Runs last so all the prerequisite
	//    state (admin auth file + resource-groups + OPA bundle + the
	//    tenant password Secret) is in place before the coordinator gets
	//    a REST call.
	//
	//    Skip catalogs if ANY of the projection steps failed: the
	//    auth-secret failure case is the killer — a Trino coordinator
	//    that just lost its password.db keys would 401 every catalog
	//    REST call from us, and we'd surface that as a misleading
	//    "catalog reconcile failed" error masking the real projection
	//    problem. Resource-groups + OPA bundle failures are less
	//    immediately broken but still mean the coordinator is in an
	//    inconsistent state from the chart's perspective. Better to
	//    retry the full prerequisite + catalog chain on the next tick
	//    than to push partial state.
	//
	//    On skip, every org gets attributed the projection error via
	//    writePerOrgStates' globalErr path (state -> Failed with the
	//    join-of-errors as StatusMessage); no per-org catalog outcome
	//    is recorded.
	globalErr := errors.Join(authErr, rgErr, opaErr, tenantErr)
	var catalogOutcomes map[string]catalogOutcome
	if globalErr == nil {
		var catErr error
		catalogOutcomes, catErr = p.reconcileCatalogs(ctx, projectable, tenants)
		if catErr != nil {
			errs = append(errs, fmt.Errorf("reconcile catalogs: %w", catErr))
		}
	} else {
		slog.Warn("trino reconcile: skipping catalog REST step because projection prerequisites failed",
			"projection_error", globalErr)
	}

	// Per-org state writes. The global steps' (auth / resource-groups /
	// OPA bundle / tenant Secret) outcomes are the same for every org
	// since each is a single K8s API write — wrap them once. Per-org
	// variance lives at the catalog step, which folds in the per-org
	// tenant-password outcomes.
	if len(collisions) > 0 {
		if catalogOutcomes == nil {
			catalogOutcomes = make(map[string]catalogOutcome, len(collisions))
		}
		for orgID, err := range collisions {
			// out.Err is checked before globalErr in writePerOrgStates,
			// so the collision is what the operator sees.
			catalogOutcomes[orgID] = catalogOutcome{Err: err}
			errs = append(errs, fmt.Errorf("org %s: %w", orgID, err))
		}
	}
	p.writePerOrgStates(orgs, catalogOutcomes, globalErr)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// rejectPrincipalCollisions splits orgs into those safe to project and those
// whose Trino catalog name, or whose Trino username, is not unique to them.
//
// Every Trino-facing name is trinoSanitize(principal), and sanitization is
// injective over principals that satisfy ValidateDatabaseName — that grammar
// is lowercase alphanumerics and hyphens, so the hyphen is the only
// character sanitization rewrites, and no valid principal contains the
// underscore it rewrites to. Grandfathered rows predate that validation
// though (see configstore.ValidateDatabaseName), so a stored name may hold
// an underscore or uppercase, and then "Acme_Corp" and "acme-corp" both
// derive catalog org_acme_corp.
//
// Left alone that is a cross-tenant read: the first org creates the catalog
// against its own metadata store, the second finds the name already present,
// is recorded as Existed, and is granted that catalog by the OPA bundle —
// so it queries the first org's data. Both orgs are therefore held back and
// reported Failed. Refusing to provision either is the only safe resolution:
// picking a winner by sort order would silently hand one tenant a catalog
// the other also believes it owns.
func rejectPrincipalCollisions(orgs []configstore.TrinoEnabledOrg) (projectable []configstore.TrinoEnabledOrg, collisions map[string]error) {
	byCatalog := make(map[string][]string, len(orgs))
	byPrincipal := make(map[string]map[string]bool, len(orgs))
	claim := func(principal, orgID string) {
		if byPrincipal[principal] == nil {
			byPrincipal[principal] = map[string]bool{}
		}
		byPrincipal[principal][orgID] = true
	}
	// The cell's own principals are claimed first, so a tenant that derives
	// either name is treated as contesting it and is held back. Neither is
	// reachable from a valid database_name, but the policy's whole admin
	// conjunction rests on the name being the provisioner's alone.
	claim(opa.AdminPrincipal, "")
	claim(opa.ObserverPrincipal, "")
	for _, o := range orgs {
		principal := o.TrinoPrincipal()
		if principal == "" {
			// No principal is a separate condition, reported by the
			// catalog step; it is not a collision.
			continue
		}
		name := TrinoCatalogName(principal)
		byCatalog[name] = append(byCatalog[name], o.OrgID)
		claim(principal, o.OrgID)
		for _, u := range o.Users {
			if !projectableTrinoUsername(u.Username) {
				continue
			}
			claim(o.TrinoUserPrincipal(u.Username), o.OrgID)
		}
	}

	contested := make(map[string]string, 0) // orgID -> catalog name
	for name, ids := range byCatalog {
		if len(ids) < 2 {
			continue
		}
		for _, id := range ids {
			contested[id] = name
		}
	}
	// A Trino username claimed by two orgs is a cross-tenant authentication
	// bug, not a cosmetic clash: password.db is one flat namespace per cell,
	// so the duplicate line lets one org's user authenticate against the
	// other's entry and land in the other's group. Valid database_names make
	// this unreachable — they are DNS labels, so `<org>.<user>` splits at its
	// only dot — but grandfathered rows predate that rule and may hold a dot,
	// which is exactly how `acme.analytics` the org and `acme` + `analytics`
	// the login come to claim one name.
	contestedPrincipal := map[string]string{} // orgID -> principal
	for principal, owners := range byPrincipal {
		if len(owners) < 2 {
			continue
		}
		for id := range owners {
			if id == "" {
				continue // the cell's own principal, not an org
			}
			contestedPrincipal[id] = principal
		}
	}
	if len(contested) == 0 && len(contestedPrincipal) == 0 {
		return orgs, nil
	}

	collisions = make(map[string]error, len(contested)+len(contestedPrincipal))
	projectable = make([]configstore.TrinoEnabledOrg, 0, len(orgs))
	for _, o := range orgs {
		name, bad := contested[o.OrgID]
		if !bad {
			if principal, dup := contestedPrincipal[o.OrgID]; dup {
				others := make([]string, 0, len(byPrincipal[principal]))
				for id := range byPrincipal[principal] {
					if id != o.OrgID {
						others = append(others, orgLabel(id))
					}
				}
				sort.Strings(others)
				collisions[o.OrgID] = fmt.Errorf(
					"Trino username %q is also claimed by %s; refusing to project either — "+
						"rename the org's database_name or the colliding login so the usernames differ",
					principal, strings.Join(others, ", "))
				slog.Error("Trino reconcile: refusing to project orgs whose Trino usernames collide.",
					"org", o.OrgID, "principal", principal, "colliding_with", others)
				continue
			}
			projectable = append(projectable, o)
			continue
		}
		others := make([]string, 0, len(byCatalog[name])-1)
		for _, id := range byCatalog[name] {
			if id != o.OrgID {
				others = append(others, id)
			}
		}
		sort.Strings(others)
		collisions[o.OrgID] = fmt.Errorf(
			"database_name %q derives catalog %s, which is also derived by org(s) %s; "+
				"refusing to provision either — rename one so the catalog names differ",
			o.TrinoPrincipal(), name, strings.Join(others, ", "))
		slog.Error("Trino reconcile: refusing to provision orgs whose catalog names collide.",
			"org", o.OrgID, "database_name", o.TrinoPrincipal(), "catalog", name, "colliding_with", others)
	}
	return projectable, collisions
}

// orgLabel names a principal's claimant in an operator-facing message. The
// empty org id is the cell itself (see the AdminPrincipal/ObserverPrincipal
// claims in rejectPrincipalCollisions), which has no org row to name.
func orgLabel(orgID string) string {
	if orgID == "" {
		return "this Trino cell's own operational principals"
	}
	return "org " + orgID
}

// claimCellOrgs filters the fleet-wide Trino-enabled listing down to the
// orgs this cell is responsible for, claiming any that have no cell yet.
//
// Three outcomes per row:
//
//   - cell == p.cellID    -> ours; reconcile it.
//   - cell == ""          -> unassigned; claim it (AssignTrinoCell) and
//     reconcile it. The claim is conditional in SQL, so if a second cell
//     claimed it first this write is a no-op and we DROP the org from
//     this tick rather than projecting a tenant we may not own.
//   - anything else       -> another cell's tenant; skip silently. Not an
//     error and not a state write: writing state for an org we don't own
//     would fight the owning cell's writer every tick.
//
// There is exactly one cell today. This function is the whole of "cell
// awareness" — no assignment policy, no rebalancing, no capacity model.
func (p *TrinoProvisioner) claimCellOrgs(orgs []configstore.TrinoEnabledOrg) []configstore.TrinoEnabledOrg {
	mine := make([]configstore.TrinoEnabledOrg, 0, len(orgs))
	for _, o := range orgs {
		switch o.CellID {
		case p.cellID:
			mine = append(mine, o)
		case "":
			if err := p.store.AssignTrinoCell(o.OrgID, p.cellID); err != nil {
				// Transient write failure — leave the org unassigned and
				// let the next tick claim it. Projecting it now would
				// mean serving a tenant whose ownership is unrecorded.
				slog.Warn("Trino reconcile: failed to claim org into cell.",
					"org", o.OrgID, "cell", p.cellID, "error", err)
				continue
			}
			slog.Info("Trino reconcile: org claimed into cell.", "org", o.OrgID, "cell", p.cellID)
			o.CellID = p.cellID
			mine = append(mine, o)
		default:
			slog.Debug("Trino reconcile: skipping org owned by another cell.",
				"org", o.OrgID, "org_cell", o.CellID, "this_cell", p.cellID)
		}
	}
	return mine
}

// Bootstrap is the public entry point, run SYNCHRONOUSLY at process
// startup (buildTrinoWiring calls it before constructing the bundle
// HTTP handler). It returns the OPA bundle bearer token so the caller
// can build the handler with the real token directly — there's no
// placeholder-then-swap window because the handler can't be constructed
// until this returns.
//
// Idempotent: re-running adopts the existing K8s Secrets and refreshes
// the in-memory admin-hash cache. A non-nil error is safe to surface to
// a startup-time fatal log — without these credentials the bundle
// endpoint can't authenticate and catalog REST can't authorize.
func (p *TrinoProvisioner) Bootstrap(ctx context.Context) (bundleToken string, err error) {
	return p.ensureClusterSecrets(ctx)
}

// ensureClusterSecrets makes the cluster-level K8s Secrets exist and
// returns the OPA bundle bearer token (for the startup handler): the
// internal-communication shared secret, the bundle token, and the two
// provisioner-owned credential pairs on trino-auth (admin, observer).
//
// The K8s Secret is the SINGLE source of truth for each credential
// value (mirrors ensureWorkerRPCSecret); the configstore holds only a
// one-bit "ever bootstrapped" sentinel. That sentinel is the gate that
// distinguishes the two reasons a Secret might be absent:
//
//   - NOT bootstrapped yet -> first boot -> GENERATE the value and
//     Create the Secret (adopting a racing replica's value on
//     AlreadyExists, never overwriting).
//   - bootstrapped already -> the Secret was deleted out-of-band ->
//     FAIL LOUD. Regenerating the internal-communication shared secret
//     would split-brain the running Trino cluster (it's env-projected
//     into long-lived pods); recovery needs an operator-coordinated
//     rollout (the rotation API, follow-up). We treat all three the
//     same way for a simple, safe invariant.
//
// Concurrency: no DB lock. K8s Create-or-AlreadyExists serializes the
// first-boot race across replicas (first writer wins each Secret, the
// rest adopt); the sentinel Mark is idempotent. A crash mid-set leaves
// convergent state (next tick adopts what exists, generates what's
// missing, then Marks) rather than an orphan.
//
// "All three the same way" below refers to the write-once family; the two
// credential PAIRS on trino-auth are the deliberate exception, documented
// at ensureCredentialPair.
func (p *TrinoProvisioner) ensureClusterSecrets(ctx context.Context) (bundleToken string, err error) {
	bootstrapped, err := p.bootstrapSentinel.IsTrinoClusterBootstrapped(ctx, p.namespace)
	if err != nil {
		// Transient (DB blip): retry next tick. Do NOT assume
		// not-bootstrapped — that could regenerate a live secret.
		return "", fmt.Errorf("read trino bootstrap sentinel: %w", err)
	}

	// internal-communication shared secret: write-once + immutable. The
	// provisioner doesn't consume its value at runtime (it's env-
	// projected to Trino pods by the chart), but it must exist.
	if _, err := p.ensureWriteOnceSecret(ctx, TrinoInternalCommunicationSecretName, TrinoInternalCommunicationSecretKey, bootstrapped); err != nil {
		return "", err
	}

	// OPA bundle bearer token: write-once + immutable. Returned so the
	// startup handler is built with the real value.
	bundleToken, err = p.ensureWriteOnceSecret(ctx, TrinoOPABundleTokenSecretName, TrinoOPABundleTokenSecretKey, bootstrapped)
	if err != nil {
		return "", err
	}

	// admin password + bcrypt hash: a matched pair on the mutable,
	// multi-owner trino-auth Secret (reconcileAuthSecret also writes
	// password.db/group.db there each tick). NOT sentinel-gated — it's
	// regenerated-if-missing at any time because it has no external
	// consumer (the provisioner owns both sides), so loss self-heals
	// rather than wedging. Written/validated together; merge retried on
	// conflict.
	adminPlaintext, adminHash, err := p.ensureAdminCredential(ctx)
	if err != nil {
		return "", err
	}

	// observer password + bcrypt hash: the admin console's read-only
	// Trino identity, on the same Secret with the same
	// regenerate-if-missing semantics and for the same reason (no
	// external consumer). Established AFTER the admin pair so the
	// first-boot Create of trino-auth is owned by one code path; this
	// call then merges into the Secret the admin pair just created.
	observerPlaintext, observerHash, err := p.ensureObserverCredential(ctx)
	if err != nil {
		return "", err
	}

	// All confirmed present + valid — only now record the
	// sentinel, so a partial first boot (some Secrets created, then a
	// crash) re-enters the generate path next tick rather than fail-loud.
	if !bootstrapped {
		if err := p.bootstrapSentinel.MarkTrinoClusterBootstrapped(ctx, p.namespace); err != nil {
			return "", fmt.Errorf("mark trino cluster bootstrapped: %w", err)
		}
	}

	p.adminPasswordHash = adminHash
	p.setObserverCredential(observerPlaintext, observerHash)
	// Push the admin plaintext into the catalog client if it supports
	// runtime credential updates (test fakes don't).
	if updater, ok := p.catalog.(TrinoCatalogCredentialUpdater); ok {
		updater.SetCredentials(opa.AdminPrincipal, adminPlaintext)
	}

	return bundleToken, nil
}

// ensureWriteOnceSecret makes a single-key, write-once Secret exist and
// returns its value. Never overwrites an existing value (so it can't
// self-inflict a rotation). Absence is interpreted via the bootstrapped
// gate: generate-and-create on first boot, fail-loud after.
func (p *TrinoProvisioner) ensureWriteOnceSecret(ctx context.Context, name, key string, bootstrapped bool) (string, error) {
	v, err := p.readSecretKey(ctx, name, key)
	if err == nil {
		// Present + non-empty: adopt, never overwrite. Enforce the
		// write-once invariant we depend on: if the Secret exists but
		// isn't immutable (operator pre-created it, or an older code
		// version made it mutable), promote it to immutable so its
		// value can't be edited in place afterward. This is FATAL on
		// failure (not best-effort): the bundle handler captures the
		// token once and later reconciles ignore token changes, so a
		// still-mutable write-once Secret could be edited out-of-band
		// and silently diverge consumers. Failing here makes the next
		// reconcile retry until immutability is actually established
		// before we treat the Secret as safely adopted.
		if perr := p.ensureSecretImmutable(ctx, name); perr != nil {
			return "", fmt.Errorf("adopt %s: could not establish write-once immutability (will retry): %w", name, perr)
		}
		return string(v), nil
	}
	var mse missingSecretError
	if !errors.As(err, &mse) {
		// Transient API error — retry next tick rather than treat as drift.
		return "", fmt.Errorf("ensure %s/%s: %w", name, key, err)
	}
	// Genuinely absent (or present-but-empty corruption).
	if bootstrapped {
		return "", fmt.Errorf(
			"trino cluster secret %s/%s is missing but the cluster is already bootstrapped (%s). "+
				"This is out-of-band deletion/corruption. Refusing to regenerate: a new internal-communication "+
				"shared secret would split-brain the running Trino cluster. Restore the Secret, or rotate via the "+
				"rotation API (which coordinates the required Trino pod rollout)",
			name, key, mse.reason)
	}
	// First boot: generate + create immutable. On a lost create race,
	// adopt the winning replica's value.
	value, genErr := configstore.GeneratePassword()
	if genErr != nil {
		return "", fmt.Errorf("generate %s/%s: %w", name, key, genErr)
	}
	if err := p.createManagedSecret(ctx, name, map[string][]byte{key: []byte(value)}, true /*immutable*/); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create %s: %w", name, err)
		}
		// Another replica created it first — adopt their value.
		adopted, readErr := p.readSecretKey(ctx, name, key)
		if readErr != nil {
			return "", fmt.Errorf("adopt %s/%s after create race: %w", name, key, readErr)
		}
		return string(adopted), nil
	}
	return value, nil
}

// ensureAdminCredential makes the __admin_provisioner password + bcrypt
// hash exist as a matched pair on the trino-auth Secret and returns
// (plaintext, hash).
//
// Unlike the write-once secrets, the admin credential is NOT sentinel-
// gated and is regenerated-if-missing at ANY time. That's safe because
// it has no external long-lived consumer: the provisioner controls both
// sides — it writes the bcrypt hash into password.db (which the
// coordinator's file-authenticator refreshes) AND authenticates its own
// catalog REST client with the plaintext (SetCredentials). So if the
// pair is lost (e.g. a stray wholesale write of trino-auth during a
// rolling upgrade wiped the admin keys), regenerating it self-heals
// within one password-file refresh window — no split-brain, no wedge.
// This is the key difference from the internal-communication secret,
// whose value IS env-projected to long-lived pods and therefore must
// fail loud rather than regenerate.
//
// The pair is always written/regenerated TOGETHER (like
// ensureWorkerRPCSecret's cert+key) so the two keys can't desync.
// trino-auth is mutable + multi-owner (reconcileAuthSecret writes
// password.db/group.db each tick), so the admin keys are MERGED in. The
// merge is retried on a 409 conflict so two replicas racing on first
// boot don't crash the loser — it re-reads and adopts the winner's pair.
// ensureCredentialPair is that algorithm, parameterized over which pair of
// keys on trino-auth it establishes. Both the admin and the observer
// principal use it; label names the credential in error messages.
func (p *TrinoProvisioner) ensureCredentialPair(ctx context.Context, label, plainKey, hashKey string) (plaintext, hash string, err error) {
	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		plainBytes, plainErr := p.readSecretKey(ctx, TrinoAuthSecretName, plainKey)
		hashBytes, hashErr := p.readSecretKey(ctx, TrinoAuthSecretName, hashKey)

		// Both present: validate the pair and ADOPT it. This is the
		// convergence point — every replica that finds a present pair
		// returns the same durable value, so even after a create/merge
		// race the next read settles everyone onto one pair.
		if plainErr == nil && hashErr == nil {
			if bcryptErr := bcrypt.CompareHashAndPassword(hashBytes, plainBytes); bcryptErr != nil {
				return "", "", fmt.Errorf(
					"trino-auth %s does not validate against %s (inconsistent pair, likely a "+
						"manual edit): %w", hashKey, plainKey, bcryptErr)
			}
			return string(plainBytes), string(hashBytes), nil
		}

		// A non-missing (transient) read error → surface for retry-next-tick.
		for _, e := range []error{plainErr, hashErr} {
			if e == nil {
				continue
			}
			var mse missingSecretError
			if !errors.As(e, &mse) {
				return "", "", fmt.Errorf("ensure trino-auth %s credential: %w", label, e)
			}
		}

		// Pair missing/incomplete → generate a candidate and try to
		// ESTABLISH it without overwriting a concurrent winner.
		newPlain, genErr := configstore.GeneratePassword()
		if genErr != nil {
			return "", "", fmt.Errorf("generate %s password: %w", label, genErr)
		}
		newHash, hashGenErr := configstore.HashPassword(newPlain)
		if hashGenErr != nil {
			return "", "", fmt.Errorf("hash %s password: %w", label, hashGenErr)
		}
		pairData := map[string][]byte{
			plainKey: []byte(newPlain),
			hashKey:  []byte(newHash),
		}

		// First boot: ensureClusterSecrets runs before reconcileAuthSecret,
		// so trino-auth typically doesn't exist yet. Create-once makes the
		// pair atomic: the winner owns it, racing replicas get
		// AlreadyExists and loop back to ADOPT (top of the loop) rather
		// than overwriting the winner's pair.
		createErr := p.createManagedSecret(ctx, TrinoAuthSecretName, pairData, false /*mutable: reconcileAuthSecret adds password.db/group.db*/)
		if createErr == nil {
			return newPlain, newHash, nil
		}
		if !apierrors.IsAlreadyExists(createErr) {
			return "", "", fmt.Errorf("create trino-auth %s credential: %w", label, createErr)
		}

		// trino-auth already exists. Re-read: if a pair is now present
		// (a racing replica won the Create, or it was set since our read),
		// the loop top will adopt it. If still absent (legacy upgrade:
		// trino-auth holds only password.db/group.db, no such keys), merge
		// our pair in. The merge can still race a concurrent merge, but the
		// loop re-reads and converges on the durable winner.
		if _, e := p.readSecretKey(ctx, TrinoAuthSecretName, plainKey); e == nil {
			continue // pair appeared — adopt on next iteration
		}
		if mergeErr := p.upsertSecretMerge(ctx, TrinoAuthSecretName, pairData); mergeErr != nil && !apierrors.IsConflict(mergeErr) {
			return "", "", fmt.Errorf("merge trino-auth %s credential: %w", label, mergeErr)
		}
		// Loop back: re-read and adopt whatever durably won.
	}
	return "", "", fmt.Errorf("ensure trino-auth %s credential: did not converge after %d attempts (will retry next reconcile)", label, maxAttempts)
}

// ensureAdminCredential establishes the provisioner's own
// catalog-management credential.
func (p *TrinoProvisioner) ensureAdminCredential(ctx context.Context) (plaintext, hash string, err error) {
	return p.ensureCredentialPair(ctx, "admin",
		TrinoAuthSecretKeyAdminPassword, TrinoAuthSecretKeyAdminPasswordHash)
}

// ensureObserverCredential establishes the admin console's read-only
// Trino credential. Same regenerate-if-missing semantics as the admin
// pair and for the same reason: the provisioner owns both sides, so a
// lost pair self-heals on the next tick instead of wedging. A rotation
// costs the console only a 401 until the coordinator's password-file
// refresh picks up the new hash, because the console reads the credential
// from this provisioner on each call rather than caching it at startup.
func (p *TrinoProvisioner) ensureObserverCredential(ctx context.Context) (plaintext, hash string, err error) {
	return p.ensureCredentialPair(ctx, "observer",
		TrinoAuthSecretKeyObserverPassword, TrinoAuthSecretKeyObserverPasswordHash)
}

// createManagedSecret creates a Secret with the standard managed
// labels. When immutable is true the apiserver rejects later in-place
// edits to its data (used for the write-once credentials). Returns the
// K8s error verbatim so callers can branch on apierrors.IsAlreadyExists
// to implement create-once-then-adopt: whoever wins the Create owns the
// value; racing replicas get AlreadyExists and adopt it rather than
// overwriting.
func (p *TrinoProvisioner) createManagedSecret(ctx context.Context, name string, data map[string][]byte, immutable bool) error {
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.namespace,
			Labels: map[string]string{
				"app":              "trino",
				"duckgres/managed": "true",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	if immutable {
		desired.Immutable = &immutable
	}
	_, err := p.kubernetes.CoreV1().Secrets(p.namespace).Create(ctx, desired, metav1.CreateOptions{})
	return err
}

// ensureSecretImmutable promotes an existing Secret to Immutable:true if
// it isn't already, so a write-once Secret that predates this code (or
// was pre-created mutably by an operator) can no longer be edited in
// place. Adding Immutable to a mutable Secret is an allowed Update (only
// the data of an already-immutable Secret is frozen); a no-op if it's
// already immutable. Returns nil when already immutable or successfully
// promoted; a conflict is benign (another replica is promoting it) and
// is swallowed.
func (p *TrinoProvisioner) ensureSecretImmutable(ctx context.Context, name string) error {
	secrets := p.kubernetes.CoreV1().Secrets(p.namespace)
	existing, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get secret %s for immutability check: %w", name, err)
	}
	if existing.Immutable != nil && *existing.Immutable {
		return nil // already immutable
	}
	immutable := true
	existing.Immutable = &immutable
	if _, err := secrets.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsConflict(err) {
			return nil // a concurrent promote won; fine
		}
		return fmt.Errorf("promote secret %s to immutable: %w", name, err)
	}
	return nil
}

// readSecretKey reads a single key from a Secret in the provisioner's
// configured namespace. Returns a missingSecretError (detectable via
// errors.As) when the Secret or key is absent, and a wrapped transient
// error for anything else (API timeout, RBAC, etc.) — callers that
// need to distinguish "genuinely missing" from "couldn't check right
// now" rely on that split.
func (p *TrinoProvisioner) readSecretKey(ctx context.Context, name, key string) ([]byte, error) {
	secret, err := p.kubernetes.CoreV1().Secrets(p.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, missingSecretError{namespace: p.namespace, name: name, key: key, reason: "secret not found"}
		}
		// Transient / non-NotFound (timeout, forbidden, conflict, ...).
		// Wrap verbatim so callers can choose to retry rather than treat
		// it as drift.
		return nil, fmt.Errorf("get secret %s/%s: %w", p.namespace, name, err)
	}
	v, ok := secret.Data[key]
	if !ok {
		return nil, missingSecretError{namespace: p.namespace, name: name, key: key, reason: "key absent from secret"}
	}
	// An empty value is corruption, not a valid credential — generated
	// credentials are never zero-length. Treat it as drift so a bad
	// restore / manual edit is surfaced rather than, e.g., installing an
	// empty bundle bearer token (which would later panic BearerTokenAuth)
	// or projecting an empty admin password.
	if len(v) == 0 {
		return nil, missingSecretError{namespace: p.namespace, name: name, key: key, reason: "key present but value is empty (corruption)"}
	}
	return v, nil
}

// missingSecretError signals that a managed Secret or one of its keys
// is genuinely absent or empty (as opposed to a transient API error).
// Detected via errors.As in the ensure* helpers to tell "generate on
// first boot" / "fail loud after bootstrap" from "retry transient".
type missingSecretError struct {
	namespace string
	name      string
	key       string
	reason    string
}

func (e missingSecretError) Error() string {
	return fmt.Sprintf("%s/%s key %q: %s", e.namespace, e.name, e.key, e.reason)
}

// catalogOutcome is the per-org result of the catalog reconcile step.
// Exactly one of (Created, Existed, Pending) is true when Err == nil;
// Err != nil means CREATE CATALOG itself failed and the org should
// land in Failed state with the error message.
type catalogOutcome struct {
	Created bool // catalog was just created this tick
	Existed bool // catalog already present, no action taken
	// Pending marks an org that was skipped because an input isn't ready
	// yet (no tenant password, no warehouse row, an incomplete connection
	// block). Provisioning, not a failure.
	Pending bool
	// PendingReason is the operator-facing explanation written into
	// status_message alongside a Pending outcome.
	PendingReason string
	Err           error // CREATE CATALOG error; mutually exclusive with the booleans
}

// writePerOrgStates writes the per-org state transition after one full
// reconcile tick. Per-org variance lives entirely at the catalog step;
// the global steps' failure (if any) applies uniformly to every
// Trino-enabled org.
//
// Priority of attribution (worst wins):
//
//  1. Per-org catalog error -> Failed + "catalog: <err>".
//  2. Global step error     -> Failed + "projection: <err>".
//  3. Catalog still pending -> Provisioning + the pending reason.
//  4. Everything succeeded  -> Ready + ReadyAt=now.
//
// Errors from UpdateTrinoState itself are logged and swallowed —
// failing to record state is non-fatal; the next reconcile tick will
// re-attempt.
func (p *TrinoProvisioner) writePerOrgStates(
	orgs []configstore.TrinoEnabledOrg,
	catalogOutcomes map[string]catalogOutcome,
	globalErr error,
) {
	now := time.Now().UTC()
	zero := time.Time{} // pointer-to-zero signals "clear failed_at" to UpdateTrinoState
	for _, o := range orgs {
		out := catalogOutcomes[o.OrgID]
		var (
			nextState configstore.ManagedWarehouseProvisioningState
			msg       string
		)
		switch {
		case out.Err != nil:
			nextState = configstore.ManagedWarehouseStateFailed
			msg = "catalog: " + out.Err.Error()
		case globalErr != nil:
			nextState = configstore.ManagedWarehouseStateFailed
			msg = "projection: " + globalErr.Error()
		case out.Pending:
			nextState = configstore.ManagedWarehouseStateProvisioning
			msg = out.PendingReason
			if msg == "" {
				msg = "waiting for warehouse provisioning to complete"
			}
		default:
			// Created or Existed + no global failure == Ready.
			nextState = configstore.ManagedWarehouseStateReady
			msg = ""
		}

		// Transition-aware timestamps, matching the surrounding
		// ManagedWarehouse pattern in controller.go: ready_at stamps
		// the first transition into Ready and is preserved on
		// subsequent ticks; failed_at stamps the transition into
		// Failed and is cleared on transition out so the column reads
		// as "currently failing since X" rather than "ever failed."
		upd := configstore.TrinoStateUpdate{
			State:         nextState,
			StatusMessage: msg,
		}
		if nextState == configstore.ManagedWarehouseStateReady && o.State != configstore.ManagedWarehouseStateReady {
			// Transitioning INTO Ready — stamp ready_at, clear any
			// stale failed_at from the previous Failed lifecycle.
			upd.ReadyAt = &now
			upd.FailedAt = &zero
		}
		if nextState == configstore.ManagedWarehouseStateFailed && o.State != configstore.ManagedWarehouseStateFailed {
			// Transitioning INTO Failed — stamp failed_at. Leave
			// ready_at as-is (it records the historic first-Ready
			// transition; the row's currently-failing-ness is
			// represented by state+failed_at).
			upd.FailedAt = &now
		}
		if nextState != configstore.ManagedWarehouseStateFailed && o.State == configstore.ManagedWarehouseStateFailed {
			// Transitioning OUT of Failed into a non-Ready state
			// (e.g. Provisioning while waiting on Iceberg) — clear
			// failed_at since the row is no longer failed.
			upd.FailedAt = &zero
		}

		if err := p.store.UpdateTrinoState(o.OrgID, upd); err != nil {
			slog.Warn("Trino reconcile: failed to write per-org state.",
				"org", o.OrgID, "error", err)
		}
	}
}

// tenantSecretProjection is the per-org result of the tenant-password
// step, consumed by reconcileCatalogs. An org appears in exactly one of
// the three maps.
type tenantSecretProjection struct {
	// projected holds the orgs whose password is now a key on the tenant
	// Secret. Only these orgs may get a catalog.
	projected map[string]bool
	// pending holds orgs whose password isn't available YET (the duckling
	// has not published a credential), keyed to a human reason.
	pending map[string]string
	// failed holds orgs whose password could not be resolved at all.
	failed map[string]error
	// statuses holds the duckling status each password came from, so the
	// catalog step can build a catalog from the SAME live composition
	// rather than re-reading it (or, worse, reading object-store fields
	// off the config store, where they are never populated).
	statuses map[string]*DucklingStatus
}

// reconcileTenantSecrets resolves every enabled org's DuckLake
// metadata-store password and writes them into ONE Secret in the Trino
// namespace, keyed by org id. The chart mounts that Secret and each org's
// catalog reads its own key as a file.
//
// The write is authoritative (replace, not merge): an org that left the
// enabled set loses its key on the very next tick, so a disabled tenant's
// password stops being mounted into the Trino pods even before its catalog
// is dropped.
//
// A per-org resolution failure does NOT fail the step — the other orgs'
// passwords still get projected, and the failing org is attributed its own
// error at the catalog step. Only the Secret WRITE failing is global,
// because at that point nobody's password landed.
func (p *TrinoProvisioner) reconcileTenantSecrets(ctx context.Context, orgs []configstore.TrinoEnabledOrg) (tenantSecretProjection, error) {
	proj := tenantSecretProjection{
		projected: make(map[string]bool, len(orgs)),
		pending:   make(map[string]string),
		failed:    make(map[string]error),
		statuses:  make(map[string]*DucklingStatus, len(orgs)),
	}
	data := make(map[string][]byte, len(orgs))
	for _, o := range orgs {
		if o.OrgID == "" {
			continue
		}
		if !secretDataKeyPattern.MatchString(o.OrgID) {
			proj.failed[o.OrgID] = fmt.Errorf(
				"org id %q is not a valid Kubernetes Secret data key ([-._a-zA-Z0-9]+); its metadata-store password cannot be projected", o.OrgID)
			continue
		}
		status, err := p.ducklings(ctx, o.OrgID)
		if err != nil {
			// Deliberately not wrapped with the org's infrastructure
			// detail beyond what the resolver said — this string ends up
			// in status_message, which operators read.
			proj.failed[o.OrgID] = fmt.Errorf("resolve metadata-store password: %w", err)
			continue
		}
		if status == nil || status.MetadataStore.Password == "" {
			proj.pending[o.OrgID] = "waiting for the duckling to publish a metadata-store credential"
			continue
		}
		data[o.OrgID] = []byte(status.MetadataStore.Password)
		proj.projected[o.OrgID] = true
		proj.statuses[o.OrgID] = status
	}

	if err := p.replaceSecret(ctx, TrinoTenantSecretName, data); err != nil {
		return tenantSecretProjection{}, err
	}
	return proj, nil
}

// reconcileCatalogs issues CREATE CATALOG for each Trino-enabled org
// (idempotent — SHOW CATALOGS first, skip if already present), and
// DROP CATALOG for any managed-name catalogs the cell has that aren't
// in the enabled set.
//
// We intentionally only touch catalogs whose name matches
// opa.ManagedCatalogPattern, so other catalogs (system, jmx, hand-rolled
// ones for the maintenance use case) survive untouched.
func (p *TrinoProvisioner) reconcileCatalogs(
	ctx context.Context,
	orgs []configstore.TrinoEnabledOrg,
	tenants tenantSecretProjection,
) (map[string]catalogOutcome, error) {
	outcomes := make(map[string]catalogOutcome, len(orgs))

	existing, err := p.catalog.ListCatalogs(ctx)
	if err != nil {
		// Listing failed — we can't safely attribute per-org outcomes,
		// so flag every org as failed-with-this-error so they don't
		// transition to ready spuriously.
		for _, o := range orgs {
			outcomes[o.OrgID] = catalogOutcome{Err: fmt.Errorf("list trino catalogs: %w", err)}
		}
		return outcomes, fmt.Errorf("list trino catalogs: %w", err)
	}
	existingSet := make(map[string]bool, len(existing))
	for _, c := range existing {
		existingSet[c] = true
	}

	// Wanted is every enabled org's catalog, INCLUDING the orgs whose
	// password is still pending. A password that is momentarily
	// unresolvable must not drop a working catalog out from under a
	// tenant's running queries.
	wanted := make(map[string]bool, len(orgs))
	for _, o := range orgs {
		if o.TrinoPrincipal() == "" {
			continue
		}
		wanted[TrinoCatalogName(o.TrinoPrincipal())] = true
	}

	var errs []error
	for _, o := range orgs {
		// No principal means no derivable catalog name. The listing query
		// already excludes these, so this is defensive: creating `org_`
		// would collide across every such org.
		if o.TrinoPrincipal() == "" {
			err := errors.New("org has no database_name, which is its Trino principal")
			errs = append(errs, fmt.Errorf("org %s: %w", o.OrgID, err))
			outcomes[o.OrgID] = catalogOutcome{Err: err}
			continue
		}
		name := TrinoCatalogName(o.TrinoPrincipal())

		// The password gate comes FIRST, before the already-exists
		// shortcut: a catalog whose password file is missing is a broken
		// catalog, and reporting the org ready because the catalog object
		// happens to exist would hide exactly the failure an operator
		// needs to see.
		if err := tenants.failed[o.OrgID]; err != nil {
			errs = append(errs, fmt.Errorf("org %s: %w", o.OrgID, err))
			outcomes[o.OrgID] = catalogOutcome{Err: err}
			continue
		}
		if reason, waiting := tenants.pending[o.OrgID]; waiting {
			outcomes[o.OrgID] = catalogOutcome{Pending: true, PendingReason: reason}
			continue
		}
		if !tenants.projected[o.OrgID] {
			outcomes[o.OrgID] = catalogOutcome{Pending: true, PendingReason: "metadata-store password not projected yet"}
			continue
		}

		if existingSet[name] {
			// Already there. Drift-correcting properties (ALTER) is
			// post-v1: the only property that can move under a live
			// catalog is the metadata endpoint after a reshard, and that
			// path fences the tenant separately.
			outcomes[o.OrgID] = catalogOutcome{Existed: true}
			continue
		}

		warehouse, err := p.warehouses.GetManagedWarehouseForTrino(o.OrgID)
		if err != nil {
			perOrgErr := fmt.Errorf("read warehouse config: %w", err)
			errs = append(errs, fmt.Errorf("org %s: %w", o.OrgID, perOrgErr))
			outcomes[o.OrgID] = catalogOutcome{Err: perOrgErr}
			continue
		}
		if warehouse == nil {
			outcomes[o.OrgID] = catalogOutcome{
				Pending:       true,
				PendingReason: "org has no managed warehouse row yet",
			}
			continue
		}
		duckling := tenants.statuses[o.OrgID]
		if missing := missingCatalogInputs(warehouse, duckling, p.awsRegion); len(missing) > 0 {
			// The org opted into Trino but its warehouse hasn't finished
			// provisioning (or a reshard blanked the connection block).
			// Rendering a catalog with an empty endpoint or bucket makes
			// Trino accept a catalog that fails every query, so wait.
			slog.Debug("Trino reconcile: warehouse not ready yet, skipping catalog create.",
				"org", o.OrgID, "missing", missing)
			outcomes[o.OrgID] = catalogOutcome{
				Pending:       true,
				PendingReason: "waiting for warehouse fields: " + strings.Join(missing, ", "),
			}
			continue
		}
		props := p.buildCatalogProperties(o.OrgID, warehouse, duckling)
		if err := p.catalog.CreateCatalog(ctx, name, props); err != nil {
			if p.tenantSecretNotMountedYet(err, o.OrgID) {
				// Not a failure: the Secret key is projected (checked
				// above) and the pods just have not seen it yet.
				slog.Info("Trino reconcile: tenant password file not visible in the Trino pods yet, retrying next tick.",
					"org", o.OrgID, "catalog", name)
				outcomes[o.OrgID] = catalogOutcome{
					Pending:       true,
					PendingReason: "waiting for the tenant password file to appear in the Trino pods",
				}
				continue
			}
			perOrgErr := fmt.Errorf("create catalog %s: %w", name, err)
			errs = append(errs, perOrgErr)
			outcomes[o.OrgID] = catalogOutcome{Err: perOrgErr}
			continue
		}
		slog.Info("Trino reconcile: catalog created.", "org", o.OrgID, "catalog", name)
		outcomes[o.OrgID] = catalogOutcome{Created: true}
	}

	for _, c := range existing {
		if !managedCatalogRe.MatchString(c) {
			continue
		}
		if wanted[c] {
			continue
		}
		if err := p.catalog.DropCatalog(ctx, c); err != nil {
			errs = append(errs, fmt.Errorf("drop stale catalog %s: %w", c, err))
			continue
		}
		slog.Info("Trino reconcile: catalog dropped.", "catalog", c)
	}

	if len(errs) > 0 {
		return outcomes, errors.Join(errs...)
	}
	return outcomes, nil
}

// missingCatalogInputs lists the warehouse fields a DuckLake catalog needs
// that this row does not carry yet. A non-empty result means WAIT (the
// warehouse is still being provisioned, or a reshard has blanked the
// connection block mid-cutover), not FAIL.
//
// fallbackRegion is the provisioner's configured AWS region, consulted only
// when the row has no s3_region of its own.
func missingCatalogInputs(w *configstore.ManagedWarehouse, d *DucklingStatus, fallbackRegion string) []string {
	var missing []string
	if d == nil {
		return append(missing, "duckling_status")
	}
	if d.MetadataStore.Endpoint == "" && d.MetadataStore.PgBouncerEndpoint == "" {
		missing = append(missing, "metadata_store_endpoint")
	}
	if d.MetadataStore.Database == "" {
		missing = append(missing, "metadata_store_database")
	}
	if d.MetadataStore.User == "" {
		missing = append(missing, "metadata_store_user")
	}
	if d.DataStore.BucketName == "" {
		missing = append(missing, "data_store_bucket_name")
	}
	if d.DataStore.S3Region == "" && fallbackRegion == "" {
		missing = append(missing, "data_store_s3_region")
	}
	if d.IAMRoleARN == "" {
		// Without the per-org role every catalog would fall back to the
		// Trino pod's ambient identity, which can assume EVERY tenant
		// role — that collapses the per-tenant S3 boundary entirely.
		// Waiting is the only safe answer.
		missing = append(missing, "iam_role_arn")
	}
	return missing
}

// buildCatalogProperties returns the Trino catalog property set for an
// org's DuckLake catalog. Callers MUST have checked missingCatalogInputs
// first; this function assumes every input is present.
//
// The property set, and why each one is here:
//
//	connector.name                                 the DuckLake connector
//	ducklake.metadata.connection-url               JDBC URL of the org's
//	                                               metadata Postgres, with an
//	                                               sslmode matching the store
//	                                               kind (in-cluster cnpg-shard
//	                                               traffic is not TLS-wrapped;
//	                                               an external RDS hop is)
//	ducklake.metadata.connection-user              the org's metadata role
//	ducklake.metadata.connection-password-file     a path into the mounted
//	                                               tenant Secret — NEVER the
//	                                               password itself, see
//	                                               TrinoTenantSecretName
//	ducklake.data-path                             the org's bucket (+ prefix)
//	fs.s3.enabled                                  Trino's native S3 file
//	                                               system. NOT
//	                                               fs.native-s3.enabled: that
//	                                               spelling was rejected at
//	                                               CREATE CATALOG and cost us
//	                                               a release (#681)
//	s3.region                                      row first, env fallback
//	s3.auth-type + s3.iam-role                     the per-org duckling role,
//	                                               assumed per catalog — the
//	                                               tenant S3 boundary
//	s3.max-connections                             bounded per-catalog pool
//
// NO SECRET APPEARS IN THIS MAP. Trino logs the full CREATE CATALOG
// statement and renders catalog properties in its web UI, and ships them to
// every worker; a password here would be readable by anyone who can see a
// query listing. If you add a property, ask whether it is safe in a log
// line before adding it.
func (p *TrinoProvisioner) buildCatalogProperties(orgID string, w *configstore.ManagedWarehouse, d *DucklingStatus) map[string]string {
	region := d.DataStore.S3Region
	if region == "" {
		region = p.awsRegion
	}
	return map[string]string{
		"connector.name":                    "ducklake",
		"ducklake.metadata.connection-url":  ducklakeMetadataJDBCURL(d),
		"ducklake.metadata.connection-user": d.MetadataStore.User,
		trinoDuckLakePasswordFileProperty:   p.tenantPasswordFilePath(orgID),
		"ducklake.data-path":                ducklakeDataPath(d.DataStore.BucketName, w.S3.PathPrefix),
		"fs.s3.enabled":                     "true",
		"s3.region":                         region,
		"s3.auth-type":                      "IAM_ROLE",
		"s3.iam-role":                       d.IAMRoleARN,
		"s3.max-connections":                strconv.Itoa(p.s3MaxConnections),
	}
}

// trinoDuckLakePasswordFileProperty is the DuckLake catalog property naming
// the in-pod file that holds one tenant's metadata-store password.
const trinoDuckLakePasswordFileProperty = "ducklake.metadata.connection-password-file"

// tenantSecretNotMountedYet reports whether err is Trino refusing the catalog
// because this org's password file is not visible inside the Trino pods yet.
//
// This is the ordinary onboarding race, not a failure. The provisioner writes
// the org's key onto the tenant Secret and creates the catalog in the same
// tick, but the pods read that Secret through a mounted volume, and the
// kubelet refreshes it on its own sync period — up to a minute or so later.
// For that window Trino rejects the catalog because the file genuinely is not
// there, and the next tick succeeds unaided (verified in prod: the org went
// Ready on its own once the mount caught up).
//
// Treating it as a failure is actively misleading: it stamps failed_at and
// parks a Trino configuration error in status_message for an org that is
// merely a few seconds early, which is noise for anything watching Trino org
// state. Reporting Pending says the true thing — still converging.
//
// The match is deliberately the single exact sentence Trino emits, built from
// our own property name and our own computed path, rather than a set of loose
// substrings. Catalog properties carry tenant-influenced values (bucket names,
// endpoints) that Trino echoes back in configuration errors, so a loose match
// could be tripped by an org's own data — the same trap
// isInstanceFatalError documents for DuckDB's echoed query text. If Trino ever
// rewords this, the match simply stops firing and the org fails loudly again,
// which is the pre-existing behavior rather than a new silent state.
func (p *TrinoProvisioner) tenantSecretNotMountedYet(err error, orgID string) bool {
	var stmtErr *TrinoStatementError
	if !errors.As(err, &stmtErr) {
		return false
	}
	return strings.Contains(stmtErr.Message, fmt.Sprintf(
		"Invalid configuration property %s: file does not exist: %s",
		trinoDuckLakePasswordFileProperty, p.tenantPasswordFilePath(orgID)))
}

// tenantPasswordFilePath is the in-pod path of one org's metadata-store
// password, i.e. the key <orgID> of the mounted TrinoTenantSecretName.
func (p *TrinoProvisioner) tenantPasswordFilePath(orgID string) string {
	return p.tenantSecretMountPath + "/" + orgID
}

// ducklakeMetadataJDBCURL renders the org's metadata Postgres as a JDBC URL,
// reading the address off the Duckling status so Trino dials exactly what the
// DuckDB workers dial. The config store's metadata_store columns are NOT the
// source: they are empty for orgs whose warehouse predates them being
// populated, which left those tenants permanently unprovisionable even though
// the live composition had every value.
//
// sslmode is derived from the store kind, never from the caller:
//
//   - cnpg-shard: `disable`. The pooler is reached over in-cluster
//     networking and serves no server certificate; requiring TLS makes
//     every catalog fail to connect.
//   - anything else (external RDS/Aurora, and any future kind): `require`.
//     Fail SAFE on an unknown kind — an unnecessary TLS handshake costs a
//     round trip, an omitted one puts a tenant credential on the wire.
func ducklakeMetadataJDBCURL(d *DucklingStatus) string {
	host, port := ducklingMetadataAddress(d)
	sslMode := "require"
	if d.MetadataStore.Type == string(configstore.MetadataStoreKindCnpgShard) {
		sslMode = "disable"
	}
	return fmt.Sprintf("jdbc:postgresql://%s:%d/%s?sslmode=%s",
		host, port, d.MetadataStore.Database, sslMode)
}

// ducklingMetadataAddress mirrors ducklingMetadataStoreAddress in the
// controlplane package: prefer the per-Duckling PgBouncer when the
// composition provisioned one, else the direct endpoint on the default port.
// It is duplicated rather than shared because controlplane imports this
// package, not the other way round; if the two ever disagree, THIS one is
// wrong.
func ducklingMetadataAddress(d *DucklingStatus) (string, int) {
	if pgb := d.MetadataStore.PgBouncerEndpoint; pgb != "" {
		if host, portStr, err := net.SplitHostPort(pgb); err == nil {
			if port, err := strconv.Atoi(portStr); err == nil {
				return host, port
			}
		}
	}
	return d.MetadataStore.Endpoint, metadataStoreDefaultPort
}

// ducklakeDataPath renders the org's object-store root. Mirrors the worker
// activation path's buildManagedWarehouseObjectStore byte-for-byte (same
// bucket, same prefix handling, same trailing slash) so Trino and DuckDB
// address exactly the same DuckLake data files. It is duplicated rather
// than shared because the controlplane package imports this one, not the
// other way round; if the two ever disagree, THIS one is wrong.
func ducklakeDataPath(bucket, pathPrefix string) string {
	prefix := strings.Trim(pathPrefix, "/")
	if prefix == "" {
		return fmt.Sprintf("s3://%s/", bucket)
	}
	return fmt.Sprintf("s3://%s/%s/", bucket, prefix)
}

// reconcileAuthSecret rebuilds password.db + group.db and atomically
// writes them onto the trino-auth K8s Secret. Other keys on the same
// Secret (admin-password, admin-password-hash — owned by
// ensureClusterSecrets) are preserved via upsertSecretMerge: this tick
// is only authoritative for the two file-projection keys.
//
// Mounted into the coordinator pod only (chart configuration in Stream
// F). Workers never see this Secret.
func (p *TrinoProvisioner) reconcileAuthSecret(ctx context.Context, orgs []configstore.TrinoEnabledOrg) error {
	passwordDB, groupDB := BuildTrinoAuthFiles(orgs, TrinoClusterPrincipals{
		AdminPasswordHash:    p.adminPasswordHash,
		ObserverPasswordHash: p.observerHash(),
	})
	return p.upsertSecretMerge(ctx, TrinoAuthSecretName, map[string][]byte{
		TrinoAuthSecretKeyPasswordDB: []byte(passwordDB),
		TrinoAuthSecretKeyGroupDB:    []byte(groupDB),
	})
}

// TrinoClusterPrincipals carries the bcrypt hashes for the cell's two
// non-tenant principals. A struct rather than two positional strings
// because they are the same type, adjacent, and swapping them would hand
// the console's identity catalog-management authority and the
// provisioner's identity none — a mistake the compiler could not catch.
//
// Both are provisioner-owned pairs on the trino-auth Secret; see
// TrinoAuthSecretKeyAdminPassword and TrinoAuthSecretKeyObserverPassword.
type TrinoClusterPrincipals struct {
	// AdminPasswordHash authenticates opa.AdminPrincipal, which performs
	// CREATE/DROP CATALOG and sees only its own queries.
	AdminPasswordHash string
	// ObserverPasswordHash authenticates opa.ObserverPrincipal, the admin
	// console's read-only identity: cluster-wide query visibility and
	// ReadSystemInformation, and no catalog access whatsoever.
	ObserverPasswordHash string
}

// BuildTrinoAuthFiles deterministically renders the (password.db,
// group.db) pair for a list of Trino-enabled orgs. Pure function so
// unit tests can exercise the projection without K8s + the rest of
// the reconcile path.
//
// Format conventions:
//
//	password.db: <principal>:<bcrypt hash from OrgUser.Password>
//	             Two kinds of tenant line. The org's own principal is its
//	             database_name (see TrinoEnabledOrg.TrinoPrincipal), so the
//	             tenant is known by the same name it uses for its DuckDB
//	             warehouse rather than a bare org UUID. Each of the org's
//	             duckgres logins additionally gets `<database_name>.<user>`
//	             (TrinoUserPrincipal). Hashes are copied through unchanged —
//	             they are already bcrypt in the configstore, and they are the
//	             SAME hashes pgwire authenticates with, so one password works
//	             on both engines and nothing has to be re-hashed or reset.
//	group.db:    <group_name>:<comma-separated users>
//	             NOTE: this is the opposite direction from password.db.
//	             Easy to get backwards, hence this comment.
//
//	             An org's unscoped principals share its `org_<name>` group,
//	             which the OPA bundle grants the whole catalog. A
//	             project-scoped login goes into a `scope_<name>_team_<id>`
//	             group INSTEAD — never both, because the org group is
//	             unscoped and membership in it would defeat the scope.
//
// Orgs without a principal are skipped entirely. An org with a principal but
// no RootPasswordHash still projects its per-user logins: the bare org
// principal is one credential among several now, not the only way in.
//
// cluster carries the bcrypt hashes for the two non-tenant principals.
// Each is prepended to both files when non-empty, regardless of orgs —
// OPA gates both on a username-AND-group conjunction (is_admin,
// is_observer), so a principal missing from either file has no authority
// at all. An empty hash skips that principal's lines entirely rather than
// projecting an un-authenticatable entry (acceptable in unit tests where
// the catalog client is a fake; never acceptable in production — see
// NewTrinoProvisioner).
//
// Neither operational principal joins a tier group: tier membership is
// what routes a query to a tenant's resource group, and neither of these
// submits tenant SQL (the admin issues catalog DDL under its own
// root.admin selector; the observer submits nothing at all, reading the
// coordinator's REST API instead).
func BuildTrinoAuthFiles(orgs []configstore.TrinoEnabledOrg, cluster TrinoClusterPrincipals) (passwordDB, groupDB string) {
	var pwLines, grpLines []string
	tierMembers := map[string][]string{}
	// Prepend so the operational principals are always present even if
	// every org row is filtered out (empty cluster bootstrap).
	for _, cp := range []struct{ principal, group, hash string }{
		{opa.AdminPrincipal, opa.AdminGroup, cluster.AdminPasswordHash},
		{opa.ObserverPrincipal, opa.ObserverGroup, cluster.ObserverPasswordHash},
	} {
		if cp.hash == "" {
			continue
		}
		pwLines = append(pwLines, fmt.Sprintf("%s:%s", cp.principal, cp.hash))
		grpLines = append(grpLines, fmt.Sprintf("%s:%s", cp.group, cp.principal))
	}
	for _, o := range orgs {
		principal := o.TrinoPrincipal()
		if principal == "" {
			continue
		}
		// The org's own principal: database_name authenticating with the
		// root hash. Kept for service-to-service use and for clients
		// configured before per-user logins existed.
		var orgGroupMembers []string
		if o.RootPasswordHash != "" {
			pwLines = append(pwLines, fmt.Sprintf("%s:%s", principal, o.RootPasswordHash))
			orgGroupMembers = append(orgGroupMembers, principal)
			tierMembers[normalizeTier(o.Tier)] = append(tierMembers[normalizeTier(o.Tier)], principal)
		}
		// Per-user logins. Each one authenticates as <org>.<user> with the
		// very same bcrypt hash it uses on pgwire.
		scopeMembers := map[string][]string{}
		for _, u := range o.Users {
			if u.PasswordHash == "" || !projectableTrinoUsername(u.Username) {
				// An unprojectable username costs that ONE login its Trino
				// access and nothing else. Holding the whole org back would
				// turn one odd name into an org-wide outage.
				if u.PasswordHash != "" {
					slog.Warn("Trino: skipping login whose username cannot be rendered into the auth files.",
						"org", o.OrgID, "user", u.Username)
				}
				continue
			}
			userPrincipal := o.TrinoUserPrincipal(u.Username)
			pwLines = append(pwLines, fmt.Sprintf("%s:%s", userPrincipal, u.PasswordHash))
			// A scoped login joins its scope group INSTEAD of the org group:
			// the org group is unscoped in the bundle, so putting a project
			// login in it would hand it the whole catalog.
			if group, ok := scopeGroupFor(o, u); ok {
				scopeMembers[group] = append(scopeMembers[group], userPrincipal)
			} else {
				orgGroupMembers = append(orgGroupMembers, userPrincipal)
			}
			tierMembers[normalizeTier(o.Tier)] = append(tierMembers[normalizeTier(o.Tier)], userPrincipal)
		}
		// group_name first, comma-separated users second. NOTE this is the
		// opposite direction from password.db; easy to get backwards.
		if len(orgGroupMembers) > 0 {
			sort.Strings(orgGroupMembers)
			grpLines = append(grpLines, fmt.Sprintf("%s:%s",
				TrinoGroupName(principal), strings.Join(orgGroupMembers, ",")))
		}
		for _, group := range sortedKeys(scopeMembers) {
			members := scopeMembers[group]
			sort.Strings(members)
			grpLines = append(grpLines, fmt.Sprintf("%s:%s", group, strings.Join(members, ",")))
		}
	}
	// Tier claims. These carry a tenant's tier to the resource-group
	// selectors, which match on userGroup — that is what keeps
	// resource-groups.json free of tenant names and therefore static. This
	// file IS reloaded (file.refresh-period on the group provider), so a
	// retier takes effect without a restart. Emitted in a fixed tier order
	// so the file is byte-stable across ticks.
	for _, tier := range []string{tierScale, tierGrowth, tierFree} {
		members := tierMembers[tier]
		if len(members) == 0 {
			continue
		}
		sort.Strings(members)
		grpLines = append(grpLines, fmt.Sprintf("%s:%s", TrinoTierGroupName(tier), strings.Join(members, ",")))
	}
	// Trailing newline so a file with one entry round-trips through
	// `cat password.db | head` cleanly; matters mostly for ops.
	if len(pwLines) > 0 {
		pwLines = append(pwLines, "")
	}
	if len(grpLines) > 0 {
		grpLines = append(grpLines, "")
	}
	return strings.Join(pwLines, "\n"), strings.Join(grpLines, "\n")
}

// scopeGroupFor returns the scope group a login belongs in, and whether it is
// scoped at all. A login is scoped iff the config store resolved a project
// policy for it AND that policy names a team, which is what the group is
// keyed on.
//
// A scoped login whose policy resolved to NO readable namespace still gets a
// group — an empty scope in the bundle, which reads nothing. That is the
// fail-closed shape configstore produces for a team that is missing or
// disabled, and it must survive the trip rather than degrading into "no scope
// group", which would put the login in the unscoped org group.
func scopeGroupFor(o configstore.TrinoEnabledOrg, u configstore.TrinoOrgUser) (string, bool) {
	if u.Scope == nil || u.TeamID == nil {
		return "", false
	}
	return TrinoScopeGroupName(o.TrinoPrincipal(), *u.TeamID), true
}

// sortedKeys returns a map's keys in sorted order, so every projection this
// file writes is byte-stable across ticks (an unstable file would rewrite the
// Secret every reconcile and re-trigger every coordinator's file refresh).
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// reconcileResourceGroups projects resource-groups.json into the
// trino-resource-groups ConfigMap.
//
// The content does NOT depend on the org list — see BuildTrinoResourceGroups
// — so this writes identical bytes every tick and the ConfigMap stops
// changing when a tenant is added. That is deliberate: Trino's file-backed
// manager parses the file once at injection and never reloads it, so a file
// that changed per tenant needed a coordinator restart to take effect. The
// projection stays in the reconcile loop rather than moving to the chart so
// the provisioner keeps sole ownership of the ConfigMap; two writers would
// fight over it.
func (p *TrinoProvisioner) reconcileResourceGroups(ctx context.Context) error {
	bytes, err := BuildTrinoResourceGroups()
	if err != nil {
		return fmt.Errorf("build resource-groups.json: %w", err)
	}
	return p.upsertConfigMap(ctx, TrinoResourceGroupsConfigMapName, map[string]string{
		TrinoResourceGroupsConfigMapKey: string(bytes),
	})
}

// resourceGroupSubGroup is the per-org subgroup serialized into
// resource-groups.json under root.tenants.<sanitized-org-name>
// (trinoSanitize(Org.Name), so ben-iceberg-cnpg → ben_iceberg_cnpg).
type resourceGroupSubGroup struct {
	Name                 string `json:"name"`
	SoftMemoryLimit      string `json:"softMemoryLimit"`
	HardConcurrencyLimit int    `json:"hardConcurrencyLimit"`
	MaxQueued            int    `json:"maxQueued"`
	SchedulingPolicy     string `json:"schedulingPolicy,omitempty"`
	SchedulingWeight     int    `json:"schedulingWeight,omitempty"`
	// JmxExport asks Trino to export this group's MBeans. It defaults to
	// FALSE in Trino, so per-group queue depth is invisible unless a group
	// opts in — the manager-level aggregate is all you otherwise get.
	JmxExport bool `json:"jmxExport,omitempty"`
	// Recursive: a tier group holds one templated ${org} child, so a tenant
	// lands at root.tenants.<tier>.<org> without being named in this file.
	SubGroups []resourceGroupSubGroup `json:"subGroups,omitempty"`
}

type resourceGroupTier struct {
	Name                 string                  `json:"name"`
	SoftMemoryLimit      string                  `json:"softMemoryLimit"`
	HardConcurrencyLimit int                     `json:"hardConcurrencyLimit"`
	MaxQueued            int                     `json:"maxQueued"`
	SubGroups            []resourceGroupSubGroup `json:"subGroups,omitempty"`
}

type resourceGroupRoot struct {
	Name                 string              `json:"name"`
	SoftMemoryLimit      string              `json:"softMemoryLimit"`
	HardConcurrencyLimit int                 `json:"hardConcurrencyLimit"`
	MaxQueued            int                 `json:"maxQueued"`
	SubGroups            []resourceGroupTier `json:"subGroups,omitempty"`
}

type resourceGroupSelector struct {
	// UserGroup matches against the caller's groups from the file group
	// provider, which is how a tenant's TIER reaches the selector without the
	// tenant appearing in this file by name.
	UserGroup string `json:"userGroup,omitempty"`
	User      string `json:"user"`
	Group     string `json:"group"`
}

type resourceGroupsFile struct {
	RootGroups []resourceGroupRoot     `json:"rootGroups"`
	Selectors  []resourceGroupSelector `json:"selectors"`
}

// tierLimits maps a tier name to per-org subgroup limits. v1 ships
// just three tiers; refine in post-v1 work once we have load data.
//
// Empty tier (default for orgs that didn't specify one) gets the
// "free" limits — the most conservative.
// Tier names. These are the tier column's values, the resource-group node
// names, and (via TrinoTierGroupName) the group.db claims that select them.
const (
	tierFree   = "free"
	tierGrowth = "growth"
	tierScale  = "scale"
)

// orgTemplateVariable is the resource-group name Trino expands from the
// selector's named capture; orgCaptureRegex is the capture that fills it.
// Together they let one templated node serve every tenant, which is what
// keeps this file free of tenant names — see BuildTrinoResourceGroups.
//
// The capture stops at the first `.` because a tenant principal is either the
// org's bare database_name (`acme`) or one of its per-user logins
// (`acme.analyst`, see configstore.TrinoUserPrincipal), and BOTH must resolve
// to the SAME leaf resource group. A `(?<org>.*)` capture -- what this was
// before per-user logins -- matches the whole username, so every user would
// get a private leaf carrying the full per-tenant limits, and an org with ten
// logins would quietly hold ten times its concurrency and memory budget. The
// selector is matched with Pattern.matcher(user).matches(), i.e. a full
// match, so the trailing group is required for qualified names to match at
// all; TestBuildTrinoResourceGroups_CapturesOrgFromQualifiedUsername pins
// both shapes.
const (
	orgTemplateVariable = "${org}"
	orgCaptureRegex     = `(?<org>[^.]+)(?:\..*)?`
)

// TrinoTierGroupName is the group.db claim that puts an org in a tier lane.
// Prefixed so it cannot collide with TrinoGroupName's org_<principal> claims,
// which the OPA bundle keys on — a tier group is deliberately absent from the
// bundle's group_catalogs, so it grants no catalog access.
func TrinoTierGroupName(tier string) string {
	return "tier_" + tier
}

// normalizeTier maps a stored tier value onto the three lanes. An unknown or
// empty tier becomes free — the smallest lane — so a bad value degrades a
// tenant's throughput rather than leaving it matching no selector at all,
// which Trino rejects outright.
func normalizeTier(tier string) string {
	switch tier {
	case tierGrowth, tierScale:
		return tier
	default:
		return tierFree
	}
}

func tierLimits(tier string) resourceGroupSubGroup {
	switch tier {
	case "growth":
		return resourceGroupSubGroup{
			SoftMemoryLimit:      "20%",
			HardConcurrencyLimit: 10,
			MaxQueued:            50,
		}
	case "scale":
		return resourceGroupSubGroup{
			SoftMemoryLimit:      "40%",
			HardConcurrencyLimit: 25,
			MaxQueued:            100,
		}
	default: // "free" or ""
		return resourceGroupSubGroup{
			SoftMemoryLimit:      "5%",
			HardConcurrencyLimit: 3,
			MaxQueued:            20,
		}
	}
}

// BuildTrinoResourceGroups renders resource-groups.json. Pure function for
// unit testing.
//
// The file is STATIC — it does not mention a single tenant, and its bytes do
// not change when one is added, removed or retiered. That is the whole point:
// Trino's file-backed resource-group manager parses this file once, at
// injection, and never reloads it (FileResourceGroupConfig has exactly one
// property, the path). A file naming tenants therefore meant every new tenant
// was rejected with "No matching resource group found with the configured
// selection rules" until the coordinator restarted — which would have undone
// the whole point of provisioning catalogs without one.
//
// Tree shape:
//
//	root
//	  ├─ admin
//	  │    └─ __admin_provisioner
//	  └─ tenants
//	       ├─ scale  └─ ${org}
//	       ├─ growth └─ ${org}
//	       └─ free   └─ ${org}
//
// ${org} is expanded by Trino from the named capture in the selector's user
// regex, so each tenant still gets its OWN leaf group — per-tenant fairness
// survives, it is just allocated at match time instead of written ahead.
//
// A tenant's tier reaches the selector through userGroup rather than through
// this file: the provisioner puts each org in a tier_<tier> group in group.db,
// which the file group provider DOES reload (file.refresh-period=60s), so a
// retier takes effect within a minute and still needs no restart.
//
// Selector order is first-match-wins. The two operational principals come
// first so neither the provisioner's catalog DDL nor the console's node query
// falls into a tenant lane, then the explicit tiers, then free as the
// catch-all — an org with an unknown or empty tier lands in the smallest lane
// rather than matching nothing, which would reject its queries outright.
func BuildTrinoResourceGroups() ([]byte, error) {
	// The templated leaf every tier shares.
	leaf := func(tier string) []resourceGroupSubGroup {
		l := tierLimits(tier)
		l.Name = orgTemplateVariable
		// Export the LEAF, which is the per-tenant group: Trino names the
		// MBean after the expanded group id (root.tenants.<tier>.<org>), so
		// this is what makes one tenant's queue depth, running count and
		// memory usage separable from another's. Without it only
		// InternalResourceGroupManager's cluster-wide aggregate exists, which
		// cannot answer "which tenant is queueing" or drive per-tenant
		// alerting. The tier groups above stay unexported — their totals are
		// derivable by summing the leaves.
		l.JmxExport = true
		return []resourceGroupSubGroup{l}
	}
	tenantTier := func(tier string) resourceGroupSubGroup {
		l := tierLimits(tier)
		l.Name = tier
		l.SubGroups = leaf(tier)
		return l
	}

	// Both operational principals get an explicit lane BEFORE the tenant
	// selectors. The last selector matches user `(?<org>.*)`, which matches
	// anything, so without these the provisioner and the observer would be
	// admitted as tenants into root.tenants.free.<principal>. Those leaves
	// are JmxExport=true, so each would appear as a phantom tenant in the
	// per-tenant resource-group metrics and in anything alerting off them.
	selectors := []resourceGroupSelector{{
		User:  opa.AdminPrincipal,
		Group: "root.admin." + opa.AdminPrincipal,
	}, {
		User:  opa.ObserverPrincipal,
		Group: "root.admin." + opa.ObserverPrincipal,
	}}
	for _, tier := range []string{tierScale, tierGrowth} {
		selectors = append(selectors, resourceGroupSelector{
			UserGroup: TrinoTierGroupName(tier),
			User:      orgCaptureRegex,
			Group:     "root.tenants." + tier + "." + orgTemplateVariable,
		})
	}
	// Catch-all: no tier group claimed, so the smallest lane.
	selectors = append(selectors, resourceGroupSelector{
		User:  orgCaptureRegex,
		Group: "root.tenants." + tierFree + "." + orgTemplateVariable,
	})

	// Admin tier limits are deliberately small. The provisioner only
	// issues DDL (SHOW / CREATE / DROP / ALTER CATALOG) which finishes
	// in milliseconds; oversizing the admin lane is wasted budget that
	// could be eating into the tenants tier.
	adminTier := resourceGroupTier{
		Name:                 "admin",
		SoftMemoryLimit:      "5%",
		HardConcurrencyLimit: 4,
		MaxQueued:            20,
		SubGroups: []resourceGroupSubGroup{{
			Name:                 opa.AdminPrincipal,
			SoftMemoryLimit:      "5%",
			HardConcurrencyLimit: 4,
			MaxQueued:            20,
		}, {
			// The observer runs one query: SELECT from system.runtime.nodes,
			// on the cluster page's refresh interval. Its own small lane so a
			// console left open cannot queue behind, or ahead of, the
			// provisioner's catalog DDL. Unexported for the same reason the
			// admin leaf is: it is not a tenant.
			Name:                 opa.ObserverPrincipal,
			SoftMemoryLimit:      "2%",
			HardConcurrencyLimit: 2,
			MaxQueued:            10,
		}},
	}

	cfg := resourceGroupsFile{
		RootGroups: []resourceGroupRoot{{
			Name:                 "root",
			SoftMemoryLimit:      "100%",
			HardConcurrencyLimit: 200,
			MaxQueued:            1000,
			SubGroups: []resourceGroupTier{
				adminTier,
				{
					Name:                 "tenants",
					SoftMemoryLimit:      "80%",
					HardConcurrencyLimit: 100,
					MaxQueued:            500,
					SubGroups: []resourceGroupSubGroup{
						tenantTier(tierScale),
						tenantTier(tierGrowth),
						tenantTier(tierFree),
					},
				},
			},
		}},
		Selectors: selectors,
	}
	out, err := json.MarshalIndent(&cfg, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal resource-groups.json: %w", err)
	}
	return out, nil
}

// reconcileOPABundle builds the GroupCatalogs map and Set()s a freshly
// built bundle into the in-memory BundleStore. The customer-Trino OPA
// sidecar polls opa.Handler (mounted by the caller on the provisioning
// HTTP server, backed by this same store) with If-None-Match; an
// unchanged input produces a byte-equal bundle, the ETag matches, and
// OPA's poll returns 304 without re-activating.
//
// Keying is by *group*, not by username — the policy authorises via
// `data.group_catalogs[group][catalog]`. Customer principals' groups
// are `org_<sanitized-org-name>` (TrinoGroupName — trinoSanitize, so
// ben-iceberg-cnpg → org_ben_iceberg_cnpg); the admin group owns every
// managed catalog so the provisioner's own SHOW CATALOGS idempotency
// check (run as opa.AdminPrincipal) is allowed.
//
// Project-scoped logins add a second kind of group, `scope_<org>_team_<id>`,
// which owns exactly the same catalog its org group does and additionally
// carries a GroupScope. That layering is what keeps this change off the
// tenant-isolation path: the catalog grant is the same grant, and the scope
// can only subtract from it (see the "Project scopes" section of
// policy.rego).
//
// ctx is currently unused (the builder is pure and the store Set is
// in-memory), but kept on the signature for parity with the other
// reconcile* steps and to permit instrumented builders later.
func (p *TrinoProvisioner) reconcileOPABundle(_ context.Context, orgs []configstore.TrinoEnabledOrg) error {
	gc := make(opa.GroupCatalogs, len(orgs)+1)
	gs := opa.GroupScopes{}
	adminCatalogs := make(map[string]bool, len(orgs))
	for _, o := range orgs {
		principal := o.TrinoPrincipal()
		if principal == "" {
			continue
		}
		catalog := TrinoCatalogName(principal)
		gc[TrinoGroupName(principal)] = map[string]bool{catalog: true}
		adminCatalogs[catalog] = true
		// A project-scoped login sits in its own group, which owns the SAME
		// catalog — the cross-tenant check is unchanged for it — and carries
		// a scope document that narrows it to that project's schemas. Groups
		// are per (org, team), so several logins on one team share one entry.
		for _, u := range o.Users {
			group, scoped := scopeGroupFor(o, u)
			if !scoped || !projectableTrinoUsername(u.Username) || u.PasswordHash == "" {
				continue
			}
			gc[group] = map[string]bool{catalog: true}
			gs[group] = opa.NewGroupScope(u.Scope.AllowedSchemas, u.Scope.AllowedRelations)
		}
	}
	if len(adminCatalogs) > 0 {
		// Admin owns every managed catalog so SHOW CATALOGS / catalog
		// management succeeds. Reads still require is_admin (the
		// admin-group claim alone grants nothing — see opa.AdminGroup
		// docstring).
		gc[opa.AdminGroup] = adminCatalogs
	}
	bundle, err := p.bundleBuilder.BuildBundle(gc, gs)
	if err != nil {
		return fmt.Errorf("build opa bundle: %w", err)
	}
	p.bundleStore.Set(opa.NewBundle(bundle))
	return nil
}

// upsertSecretMerge is the partial-owner Secret writer: it
// preserves keys on the existing Secret that this call doesn't
// overwrite. Used for Secrets owned jointly by multiple reconcile
// paths (notably trino-auth, which holds both the bootstrapped admin
// credential keys and the per-tick projected password.db / group.db).
//
// Concurrency note: the read-modify-write here is racy in the
// abstract — two concurrent provisioner replicas could each load the
// same ResourceVersion, both Update, and one would lose its delta.
// In practice the provisioner runs reconcile per replica but the
// bootstrap path is gated by the configstore advisory lock and the
// per-tick auth/rg/opa writes are byte-equal-deterministic, so a
// lost-write retries identically on the next tick. If the race ever
// matters, switch to apiserver patch with field-manager ownership.
func (p *TrinoProvisioner) upsertSecretMerge(ctx context.Context, name string, data map[string][]byte) error {
	secrets := p.kubernetes.CoreV1().Secrets(p.namespace)
	existing, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get secret %s for merge: %w", name, err)
		}
		// First write — create with only our keys.
		desired := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: p.namespace,
				Labels: map[string]string{
					"app":              "trino",
					"duckgres/managed": "true",
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: data,
		}
		if _, err := secrets.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create secret %s: %w", name, err)
			}
			// Lost the create race — fall through to the merge path.
			existing, err = secrets.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get secret %s after create race: %w", name, err)
			}
		} else {
			return nil
		}
	}

	// Merge data: preserve existing keys, overwrite ours.
	merged := make(map[string][]byte, len(existing.Data)+len(data))
	for k, v := range existing.Data {
		merged[k] = v
	}
	for k, v := range data {
		merged[k] = v
	}

	existing.Data = merged
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	existing.Labels["app"] = "trino"
	existing.Labels["duckgres/managed"] = "true"

	if _, err := secrets.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s (merge): %w", name, err)
	}
	return nil
}

// replaceSecret is the sole-owner Secret writer: the given data map
// becomes the Secret's ENTIRE contents, so keys this call doesn't name are
// deleted. That is the point for the tenant-password Secret — an org that
// left the enabled set must lose its key, and a merge-style writer would
// keep every password it ever wrote mounted forever.
//
// Never use this for a Secret with more than one writer (trino-auth has
// three; it uses upsertSecretMerge).
func (p *TrinoProvisioner) replaceSecret(ctx context.Context, name string, data map[string][]byte) error {
	secrets := p.kubernetes.CoreV1().Secrets(p.namespace)
	existing, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get secret %s for replace: %w", name, err)
		}
		if createErr := p.createManagedSecret(ctx, name, data, false /*mutable: rewritten every tick*/); createErr != nil {
			if !apierrors.IsAlreadyExists(createErr) {
				return fmt.Errorf("create secret %s: %w", name, createErr)
			}
			// Lost the create race — fall through and overwrite; the
			// data is a deterministic function of the same inputs, so
			// whichever replica writes last writes the same bytes.
			existing, err = secrets.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get secret %s after create race: %w", name, err)
			}
		} else {
			return nil
		}
	}

	existing.Data = data
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	existing.Labels["app"] = "trino"
	existing.Labels["duckgres/managed"] = "true"
	if _, err := secrets.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s (replace): %w", name, err)
	}
	return nil
}

// upsertConfigMap mirrors upsertSecretMerge for non-secret config (resource-
// groups.json is not sensitive — tier mappings + org ids).
func (p *TrinoProvisioner) upsertConfigMap(ctx context.Context, name string, data map[string]string) error {
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.namespace,
			Labels: map[string]string{
				"app":              "trino",
				"duckgres/managed": "true",
			},
		},
		Data: data,
	}
	cms := p.kubernetes.CoreV1().ConfigMaps(p.namespace)
	_, err := cms.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create configmap %s: %w", name, err)
	}
	existing, getErr := cms.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get configmap %s for update: %w", name, getErr)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if _, err := cms.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update configmap %s: %w", name, err)
	}
	return nil
}

// =====================================================================
// HTTP implementation of TrinoCatalogClient.
// =====================================================================

// trinoCatalogHTTPClient drives the Trino REST API for catalog
// management. Authentication is HTTP Basic (Trino's standard for the
// file password authenticator); the configured user is
// opa.AdminPrincipal, with the password living in a K8s Secret mounted
// only into the provisioner pod.
//
// One client per provisioner instance — the underlying http.Client is
// goroutine-safe; the username/password pair is guarded by mu and
// updated by SetCredentials at each cluster-secrets rotation. Reads
// per-request are uncontended in steady state (Trino REST calls are
// once per catalog reconcile, dwarfed by HTTP round-trip time).
type trinoCatalogHTTPClient struct {
	baseURL  string
	hc       *http.Client
	mu       sync.RWMutex
	username string
	password string
}

// NewTrinoCatalogHTTPClient builds an HTTP-backed TrinoCatalogClient.
// baseURL is the customer Trino coordinator endpoint. Prefer HTTPS: Trino
// only accepts password (Basic) authentication over a secure channel — over
// plain HTTP it routes to the insecure (passwordless) authenticator and
// rejects a Basic password outright ("Password not allowed for insecure
// authentication"). A plain-http baseURL therefore cannot authenticate the
// admin principal for catalog DDL.
//
// tlsServerName, when non-empty, is the name the coordinator's TLS certificate
// is verified against, overriding the baseURL host for verification (Go's
// crypto/tls ServerName). cert-manager issues the coordinator cert for the
// EXTERNAL hostname (e.g. trino.dw.dev.postwh.com), which doesn't match the
// in-cluster Service address the provisioner dials; this keeps FULL cert
// verification (chain + the overridden name) instead of disabling it. Empty =
// standard verification against the baseURL host (correct when baseURL already
// uses the cert hostname). Ignored for http URLs.
//
// Credentials are typically empty here and populated by the first
// Reconcile via SetCredentials (after ensureClusterSecrets bootstraps
// the admin password). Production callers MAY pre-supply credentials
// for the rare case where the bootstrap is known upfront (e.g. tests);
// the username defaults to opa.AdminPrincipal if empty.
func NewTrinoCatalogHTTPClient(baseURL, username, password, tlsServerName string) TrinoCatalogClient {
	if username == "" {
		username = opa.AdminPrincipal
	}
	hc := &http.Client{Timeout: 30 * time.Second}
	if tlsServerName != "" {
		// Clone the default transport to keep its proxy/dial/keepalive
		// defaults, then pin the TLS verification name. This is NOT
		// InsecureSkipVerify — the cert chain is still validated, just against
		// tlsServerName rather than the dialed host. Applies to every request
		// the client makes, including the /v1/statement nextUri follow-ups.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			ServerName: tlsServerName,
			MinVersion: tls.VersionTLS12,
		}
		hc.Transport = transport
	}
	return &trinoCatalogHTTPClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		hc:       hc,
		username: username,
		password: password,
	}
}

// SetCredentials updates the cached Basic-auth pair. Called from the
// provisioner's ensureClusterSecrets path after the bootstrap-or-load
// step resolves the live admin plaintext. Idempotent and goroutine-
// safe — concurrent readers see either the old or the new pair
// atomically (a single in-flight statement won't observe a mixed
// username/password).
func (c *trinoCatalogHTTPClient) SetCredentials(username, password string) {
	c.mu.Lock()
	c.username = username
	c.password = password
	c.mu.Unlock()
}

// credentials reads the cached pair under the read lock. Used by the
// per-request authn path.
func (c *trinoCatalogHTTPClient) credentials() (string, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.username, c.password
}

// trinoStatementResponse is the subset of the Trino /v1/statement
// response we need: nextUri for paging, plus a coarse status check.
// The full response is much larger; we ignore the rest.
type trinoStatementResponse struct {
	ID      string                   `json:"id"`
	NextURI string                   `json:"nextUri,omitempty"`
	Stats   map[string]interface{}   `json:"stats,omitempty"`
	Data    [][]interface{}          `json:"data,omitempty"`
	Error   *trinoStatementErrorBody `json:"error,omitempty"`
}

type trinoStatementErrorBody struct {
	Message   string `json:"message"`
	ErrorCode int    `json:"errorCode"`
	ErrorName string `json:"errorName"`
	ErrorType string `json:"errorType"`
}

// TrinoStatementError is a statement the coordinator rejected, carrying
// Trino's own error classification instead of flattening it into a string.
//
// It exists so callers can branch on WHAT Trino objected to rather than
// grepping a wrapped error chain. Error() reproduces the previous flattened
// text verbatim, so logs and status messages are unchanged.
type TrinoStatementError struct {
	ErrorName string
	ErrorType string
	Message   string
}

func (e *TrinoStatementError) Error() string {
	return fmt.Sprintf("trino: %s (%s): %s", e.ErrorName, e.ErrorType, e.Message)
}

// runStatement executes a single Trino statement via /v1/statement and
// drains the nextUri chain. Returns the accumulated data rows (may be
// empty for DDL).
func (c *trinoCatalogHTTPClient) runStatement(ctx context.Context, sql string) ([][]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/statement", strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("build statement request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	username, password := c.credentials()
	req.Header.Set("X-Trino-User", username)
	req.Header.Set("X-Trino-Source", TrinoProvisionerSource)
	req.Header.Set("Authorization", "Basic "+basicAuth(username, password))

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post statement: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("post statement: status %d: %s", resp.StatusCode, string(body))
	}

	return c.drainStatement(ctx, body)
}

// drainStatement reads the nextUri chain until the statement
// completes. Each hop is a GET; the final body carries the result
// data (already-accumulated rows from earlier hops are kept).
//
// Bounded to maxDrainHops to defend against a pathological Trino
// (or test fake) that perpetually returns a nextUri without ever
// completing. Catalog management statements are DDL and finish in a
// handful of hops in practice; 1000 is generous for any sane Trino.
// Each hop also honors ctx — a cancelled reconcile context aborts
// promptly rather than waiting for the next request to time out.
func (c *trinoCatalogHTTPClient) drainStatement(ctx context.Context, initial []byte) ([][]interface{}, error) {
	const maxDrainHops = 1000
	var all [][]interface{}
	body := initial
	for hop := 0; hop < maxDrainHops; hop++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("statement drain aborted: %w", err)
		}
		var r trinoStatementResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("parse statement response: %w (body=%q)", err, string(body))
		}
		if r.Error != nil {
			return nil, &TrinoStatementError{
				ErrorName: r.Error.ErrorName,
				ErrorType: r.Error.ErrorType,
				Message:   r.Error.Message,
			}
		}
		all = append(all, r.Data...)
		if r.NextURI == "" {
			return all, nil
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.NextURI, nil)
		if err != nil {
			return nil, fmt.Errorf("build nextUri request: %w", err)
		}
		username, password := c.credentials()
		req.Header.Set("X-Trino-User", username)
		req.Header.Set("X-Trino-Source", TrinoProvisionerSource)
		req.Header.Set("Authorization", "Basic "+basicAuth(username, password))
		resp, err := c.hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("get nextUri: %w", err)
		}
		body, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("get nextUri: status %d: %s", resp.StatusCode, string(body))
		}
	}
	return nil, fmt.Errorf("statement drain exceeded %d hops without completing", maxDrainHops)
}

// ListCatalogs runs SHOW CATALOGS and returns the catalog names.
func (c *trinoCatalogHTTPClient) ListCatalogs(ctx context.Context) ([]string, error) {
	rows, err := c.runStatement(ctx, "SHOW CATALOGS")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if len(r) == 0 {
			continue
		}
		if s, ok := r[0].(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// CreateCatalog issues CREATE CATALOG <name> USING <connector> WITH (...)
// with the given properties.
func (c *trinoCatalogHTTPClient) CreateCatalog(ctx context.Context, name string, props map[string]string) error {
	connector := props["connector.name"]
	if connector == "" {
		return fmt.Errorf("CreateCatalog %q: connector.name property is required", name)
	}
	// Copy props minus connector.name into withProps; do not mutate the
	// caller's map. (Earlier versions did `withProps := props; delete(withProps, "connector.name")`,
	// which silently shared backing storage with the caller and stripped
	// connector.name from their copy too.)
	withProps := make(map[string]string, len(props))
	for k, v := range props {
		if k == "connector.name" {
			continue
		}
		withProps[k] = v
	}
	sql := fmt.Sprintf("CREATE CATALOG %s USING %s%s", quoteTrinoIdentifier(name), connector, renderWithClause(withProps))
	_, err := c.runStatement(ctx, sql)
	return err
}

// AlterCatalog is not exercised in v1 (no property drift while
// Lakekeeper stays allowall — see plan), but the method is wired so
// post-v1 OAuth2 rotation has a place to go. Implemented as DROP +
// CREATE under the hood — Trino's ALTER CATALOG covers a narrow set
// of property updates, and the simplest sane fallback is recreate.
func (c *trinoCatalogHTTPClient) AlterCatalog(ctx context.Context, name string, props map[string]string) error {
	if err := c.DropCatalog(ctx, name); err != nil {
		return fmt.Errorf("alter catalog %q (drop step): %w", name, err)
	}
	return c.CreateCatalog(ctx, name, props)
}

// DropCatalog issues DROP CATALOG <name>.
func (c *trinoCatalogHTTPClient) DropCatalog(ctx context.Context, name string) error {
	_, err := c.runStatement(ctx, "DROP CATALOG "+quoteTrinoIdentifier(name))
	return err
}

// quoteTrinoIdentifier wraps the identifier in double quotes and
// escapes any embedded double quote per Trino's SQL grammar.
func quoteTrinoIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// renderWithClause renders a sorted-by-key `WITH ("k" = '...')` clause
// suitable for CREATE CATALOG. Sorted output makes the SQL
// deterministic for snapshot tests. Returns an empty string when there
// are no properties.
func renderWithClause(props map[string]string) string {
	if len(props) == 0 {
		return ""
	}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf(`"%s" = '%s'`, k, strings.ReplaceAll(props[k], "'", "''")))
	}
	return " WITH (" + strings.Join(parts, ", ") + ")"
}

// basicAuth produces the base64-encoded value for the Authorization
// header. Matches net/http's internal helper.
func basicAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

// Bundle distribution is pull-based (plan "Open Questions #6"): the
// provisioner Set()s built bundles into an opa.BundleStore; the
// customer-Trino OPA sidecar polls opa.Handler with If-None-Match.
// There is no provisioner-side push client. See reconcileOPABundle and
// TrinoProvisionerOpts.BundleStore.
