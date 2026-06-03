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
	"net/http"
	"regexp"
	"sort"
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

// TrinoCustomerNamespace is the K8s namespace where the customer-facing
// Trino cluster lives. The auth Secret + resource-groups ConfigMap are
// projected into this namespace by the provisioner.
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
// Format: org_<sanitized>_iceberg. The sanitization maps Org.Name
// to Trino identifier rules ([a-z0-9_]); any other characters
// collapse to underscores. Catalog name uniqueness across orgs
// depends on Org.Name uniqueness post-sanitization.
func TrinoCatalogName(orgName string) string {
	return "org_" + trinoSanitize(orgName) + "_iceberg"
}

// TrinoGroupName returns the file-group-provider group label for an org.
// Format: org_<sanitized>. Matches the OPA `input.context.identity.groups`
// the customer Trino sends with each decision request.
func TrinoGroupName(orgName string) string {
	return "org_" + trinoSanitize(orgName)
}

// TrinoResourceGroupName returns the resource-group selector key for
// an org. Sanitized like the catalog name so a `.` in orgName doesn't
// get re-interpreted as a hierarchy separator in Trino's resource-
// group path (and so the selector + subgroup names stay aligned across
// the catalog, group, and resource-group projections). Customer
// principals' Trino username == orgName (numeric team_id in v1), but
// the underlying type is still a string so we sanitize defensively.
func TrinoResourceGroupName(orgName string) string {
	return "root.tenants." + trinoSanitize(orgName)
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

	// IcebergStore is the per-org Iceberg config read surface.
	IcebergStore TrinoIcebergStore

	// Kubernetes is used for the auth Secret and resource-groups
	// ConfigMap projections in the customer Trino namespace.
	Kubernetes kubernetes.Interface

	// Namespace overrides TrinoCustomerNamespace. Empty == default.
	// Useful for dev clusters that namespace customer Trino differently.
	Namespace string

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

	// IAMAccountID is the AWS account id the per-org Iceberg catalogs
	// reference via iceberg.s3.aws-role-arn (the duckling-<orgid> role).
	// Required for AWS deployments; empty in dev / unit tests.
	IAMAccountID string

	// AWSRegion is the AWS region for the per-org S3 IAM role binding
	// and Iceberg catalog config. Empty in dev / unit tests.
	AWSRegion string
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

// TrinoStore is the read surface trino_provisioner.go uses for the per-
// reconcile projection inputs. Defined as a narrow interface so unit
// tests can swap a fake and so the type doesn't drag the configstore
// import everywhere.
type TrinoStore interface {
	ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error)
	UpdateTrinoState(orgID string, upd configstore.TrinoStateUpdate) error
}

// TrinoIcebergStore reads a single org's Iceberg config to populate the
// per-org catalog properties. Backed by configstore.ConfigStore.
type TrinoIcebergStore interface {
	GetManagedWarehouseIceberg(orgID string) (*configstore.ManagedWarehouseIceberg, error)
}

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
	store             TrinoStore
	bootstrapSentinel TrinoBootstrapSentinelStore
	icebergStore      TrinoIcebergStore
	kubernetes        kubernetes.Interface
	namespace         string
	catalog           TrinoCatalogClient
	bundleStore       *opa.BundleStore
	bundleBuilder     opa.BundleBuilder
	iamAccountID      string
	awsRegion         string

	// adminPasswordHash is cached on each Reconcile from the
	// trino-auth K8s Secret and prepended to password.db on projection.
	// Empty until the first Reconcile.
	adminPasswordHash string
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
	if opts.IcebergStore == nil {
		return nil, errors.New("TrinoProvisioner: IcebergStore is required")
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
	return &TrinoProvisioner{
		store:             opts.Store,
		bootstrapSentinel: opts.BootstrapSentinel,
		icebergStore:      opts.IcebergStore,
		kubernetes:        opts.Kubernetes,
		namespace:         ns,
		catalog:           opts.Catalog,
		bundleStore:       opts.BundleStore,
		bundleBuilder:     opts.BundleBuilder,
		iamAccountID:      opts.IAMAccountID,
		awsRegion:         opts.AWSRegion,
	}, nil
}

// Reconcile runs one full projection: catalogs → auth files → resource
// groups → OPA bundle. Errors in any one output are logged and surfaced
// but the next output still runs — a failing OPA push shouldn't leave
// the password file stale, and vice versa. The caller's next tick re-
// runs everything.
//
// Returns a multi-error wrapping every per-step error so callers can
// surface them in observability without losing detail. nil iff every
// step succeeded.
func (p *TrinoProvisioner) Reconcile(ctx context.Context) error {
	// 0. Cluster-level Secrets. Bootstrap-or-load on every tick (cheap
	//    after first run; the configstore row exists and the call
	//    returns it without touching K8s). Catalog REST calls in step 4
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

	orgs, err := p.store.ListTrinoEnabledOrgs()
	if err != nil {
		return fmt.Errorf("list trino-enabled orgs: %w", err)
	}

	// Stable iteration order so logs and projections are deterministic
	// regardless of how the DB driver returned the rows.
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].OrgID < orgs[j].OrgID })

	var errs []error

	// 1. Auth file projection (K8s Secret). Atomic Secret update.
	//    Runs BEFORE catalogs so the admin lines exist in password.db
	//    + group.db when the catalog client's first request reaches
	//    the coordinator on a cold-start tick.
	authErr := p.reconcileAuthSecret(ctx, orgs)
	if authErr != nil {
		errs = append(errs, fmt.Errorf("reconcile auth secret: %w", authErr))
	}

	// 2. Resource groups (K8s ConfigMap). Generated from the per-org
	//    tier; rebuilt every tick. Also runs before catalogs so the
	//    coordinator's resource-groups manager has a valid file when
	//    catalog creation kicks off queries on tier-specific groups.
	rgErr := p.reconcileResourceGroups(ctx, orgs)
	if rgErr != nil {
		errs = append(errs, fmt.Errorf("reconcile resource groups: %w", rgErr))
	}

	// 3. OPA bundle. GroupCatalogs keyed by Trino group name; Set into
	//    the in-memory store the bundle HTTP handler serves. Pre-
	//    catalog so the OPA sidecar's authorization decisions for the
	//    catalog reconcile's own queries see the up-to-date roster.
	opaErr := p.reconcileOPABundle(ctx, orgs)
	if opaErr != nil {
		errs = append(errs, fmt.Errorf("reconcile opa bundle: %w", opaErr))
	}

	// 4. Catalogs (REST). Per-org idempotent CREATE; orgs disabled
	//    since last tick get DROP. Runs last so all the prerequisite
	//    state (admin auth file + resource-groups + OPA bundle) is in
	//    place before the coordinator gets a REST call.
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
	globalErr := errors.Join(authErr, rgErr, opaErr)
	var catalogOutcomes map[string]catalogOutcome
	if globalErr == nil {
		var catErr error
		catalogOutcomes, catErr = p.reconcileCatalogs(ctx, orgs)
		if catErr != nil {
			errs = append(errs, fmt.Errorf("reconcile catalogs: %w", catErr))
		}
	} else {
		slog.Warn("trino reconcile: skipping catalog REST step because projection prerequisites failed",
			"projection_error", globalErr)
	}

	// Per-org state writes. The global steps' (auth/rg/opa) outcomes
	// are the same for every org since each is a single K8s API write
	// — wrap them once.
	p.writePerOrgStates(orgs, catalogOutcomes, globalErr)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
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

// ensureClusterSecrets makes the three cluster-level K8s Secrets exist
// and returns the OPA bundle bearer token (for the startup handler).
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

	// All three confirmed present + valid — only now record the
	// sentinel, so a partial first boot (some Secrets created, then a
	// crash) re-enters the generate path next tick rather than fail-loud.
	if !bootstrapped {
		if err := p.bootstrapSentinel.MarkTrinoClusterBootstrapped(ctx, p.namespace); err != nil {
			return "", fmt.Errorf("mark trino cluster bootstrapped: %w", err)
		}
	}

	p.adminPasswordHash = adminHash
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
func (p *TrinoProvisioner) ensureAdminCredential(ctx context.Context) (plaintext, hash string, err error) {
	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		plainBytes, plainErr := p.readSecretKey(ctx, TrinoAuthSecretName, TrinoAuthSecretKeyAdminPassword)
		hashBytes, hashErr := p.readSecretKey(ctx, TrinoAuthSecretName, TrinoAuthSecretKeyAdminPasswordHash)

		// Both present: validate the pair and ADOPT it. This is the
		// convergence point — every replica that finds a present pair
		// returns the same durable value, so even after a create/merge
		// race the next read settles everyone onto one pair.
		if plainErr == nil && hashErr == nil {
			if bcryptErr := bcrypt.CompareHashAndPassword(hashBytes, plainBytes); bcryptErr != nil {
				return "", "", fmt.Errorf(
					"trino-auth admin-password-hash does not validate against admin-password (inconsistent pair, likely a "+
						"manual edit): %w", bcryptErr)
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
				return "", "", fmt.Errorf("ensure trino-auth admin credential: %w", e)
			}
		}

		// Pair missing/incomplete → generate a candidate and try to
		// ESTABLISH it without overwriting a concurrent winner.
		newPlain, genErr := configstore.GeneratePassword()
		if genErr != nil {
			return "", "", fmt.Errorf("generate admin password: %w", genErr)
		}
		newHash, hashGenErr := configstore.HashPassword(newPlain)
		if hashGenErr != nil {
			return "", "", fmt.Errorf("hash admin password: %w", hashGenErr)
		}
		adminData := map[string][]byte{
			TrinoAuthSecretKeyAdminPassword:     []byte(newPlain),
			TrinoAuthSecretKeyAdminPasswordHash: []byte(newHash),
		}

		// First boot: ensureClusterSecrets runs before reconcileAuthSecret,
		// so trino-auth typically doesn't exist yet. Create-once makes the
		// admin keys atomic: the winner owns them, racing replicas get
		// AlreadyExists and loop back to ADOPT (top of the loop) rather
		// than overwriting the winner's pair.
		createErr := p.createManagedSecret(ctx, TrinoAuthSecretName, adminData, false /*mutable: reconcileAuthSecret adds password.db/group.db*/)
		if createErr == nil {
			return newPlain, newHash, nil
		}
		if !apierrors.IsAlreadyExists(createErr) {
			return "", "", fmt.Errorf("create trino-auth admin credential: %w", createErr)
		}

		// trino-auth already exists. Re-read: if a pair is now present
		// (a racing replica won the Create, or it was set since our read),
		// the loop top will adopt it. If still absent (legacy upgrade:
		// trino-auth holds only password.db/group.db, no admin keys), merge
		// our pair in. The merge can still race a concurrent merge, but the
		// loop re-reads and converges on the durable winner.
		if _, e := p.readSecretKey(ctx, TrinoAuthSecretName, TrinoAuthSecretKeyAdminPassword); e == nil {
			continue // pair appeared — adopt on next iteration
		}
		if mergeErr := p.upsertSecretMerge(ctx, TrinoAuthSecretName, adminData); mergeErr != nil && !apierrors.IsConflict(mergeErr) {
			return "", "", fmt.Errorf("merge trino-auth admin credential: %w", mergeErr)
		}
		// Loop back: re-read and adopt whatever durably won.
	}
	return "", "", fmt.Errorf("ensure trino-auth admin credential: did not converge after %d attempts (will retry next reconcile)", maxAttempts)
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
	Created bool  // catalog was just created this tick
	Existed bool  // catalog already present, no action taken
	Pending bool  // skipped because Iceberg isn't ready yet (provisioning, not a failure)
	Err     error // CREATE CATALOG error; mutually exclusive with the booleans
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
//  3. Catalog still pending -> Provisioning + "waiting for iceberg".
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
			msg = "waiting for iceberg / Lakekeeper to become ready"
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

// reconcileCatalogs issues CREATE CATALOG for each Trino-enabled org
// (idempotent — SHOW CATALOGS first, skip if already present), and
// DROP CATALOG for any `org_*_iceberg` catalogs the customer cluster
// has that aren't in the enabled set.
//
// We intentionally only touch catalogs whose name matches the
// org_*_iceberg prefix so other catalogs (system, jmx, hand-rolled
// ones for the maintenance use case) survive untouched.
func (p *TrinoProvisioner) reconcileCatalogs(ctx context.Context, orgs []configstore.TrinoEnabledOrg) (map[string]catalogOutcome, error) {
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

	wanted := make(map[string]bool, len(orgs))
	for _, o := range orgs {
		wanted[TrinoCatalogName(o.OrgID)] = true
	}

	var errs []error
	for _, o := range orgs {
		name := TrinoCatalogName(o.OrgID)
		if existingSet[name] {
			// Already there. Drift-correct properties (ALTER) is post-v1
			// — Lakekeeper auth stays allowall so catalog properties
			// don't drift; revisit when OAuth2 lands.
			outcomes[o.OrgID] = catalogOutcome{Existed: true}
			continue
		}
		iceberg, err := p.icebergStore.GetManagedWarehouseIceberg(o.OrgID)
		if err != nil {
			perOrgErr := fmt.Errorf("read iceberg config: %w", err)
			errs = append(errs, fmt.Errorf("org %s: %w", o.OrgID, perOrgErr))
			outcomes[o.OrgID] = catalogOutcome{Err: perOrgErr}
			continue
		}
		if iceberg == nil || iceberg.LakekeeperEndpoint == "" || iceberg.LakekeeperWarehouse == "" {
			// Org opted into Trino but Iceberg isn't fully ready yet —
			// both the REST endpoint AND the warehouse name are
			// required (iceberg.rest-catalog.warehouse rendered as ''
			// makes Trino reject CREATE CATALOG every tick, silently).
			// Skip; the next reconcile picks it up once Lakekeeper
			// provisioning completes.
			slog.Debug("Trino reconcile: iceberg not ready yet, skipping catalog create.",
				"org", o.OrgID,
				"endpoint_set", iceberg != nil && iceberg.LakekeeperEndpoint != "",
				"warehouse_set", iceberg != nil && iceberg.LakekeeperWarehouse != "")
			outcomes[o.OrgID] = catalogOutcome{Pending: true}
			continue
		}
		props := p.buildCatalogProperties(o.OrgID, iceberg)
		if err := p.catalog.CreateCatalog(ctx, name, props); err != nil {
			perOrgErr := fmt.Errorf("create catalog %s: %w", name, err)
			errs = append(errs, perOrgErr)
			outcomes[o.OrgID] = catalogOutcome{Err: perOrgErr}
			continue
		}
		slog.Info("Trino reconcile: catalog created.", "org", o.OrgID, "catalog", name)
		outcomes[o.OrgID] = catalogOutcome{Created: true}
	}

	for _, c := range existing {
		if !strings.HasPrefix(c, "org_") || !strings.HasSuffix(c, "_iceberg") {
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

// buildCatalogProperties returns the Trino catalog property set for an
// org's Iceberg catalog. Lakekeeper stays allowall in v1, so no OAuth2
// credentials are embedded here — just the endpoint, the warehouse
// name, and the per-org S3 IAM role.
//
// Property names follow Trino's current file-system / Iceberg-REST
// surface (matching the working maintenance chart at
// charts/charts/trino/files/trino_maintenance.py):
//
//	fs.s3.enabled=true              enable the unified S3 file system
//	s3.region=<region>              forwarded to the AWS SDK
//	s3.iam-role=<arn>               per-org duckling-<orgid> role
//
// The OLD `iceberg.s3.aws-role-arn` / `iceberg.s3.region` properties
// referenced in the design plan are stale for current Trino — they
// silently get ignored by some versions and outright rejected by
// others, both of which break the catalog. Keep this list in sync
// with the maintenance chart.
func (p *TrinoProvisioner) buildCatalogProperties(orgID string, ic *configstore.ManagedWarehouseIceberg) map[string]string {
	props := map[string]string{
		"connector.name":                 "iceberg",
		"iceberg.catalog.type":           "rest",
		"iceberg.rest-catalog.uri":       ic.LakekeeperEndpoint,
		"iceberg.rest-catalog.warehouse": ic.LakekeeperWarehouse,
		"fs.s3.enabled":                  "true",
	}
	if p.iamAccountID != "" {
		props["s3.iam-role"] = fmt.Sprintf("arn:aws:iam::%s:role/duckling-%s", p.iamAccountID, orgID)
	}
	if p.awsRegion != "" {
		props["s3.region"] = p.awsRegion
	} else if ic.Region != "" {
		props["s3.region"] = ic.Region
	}
	return props
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
	passwordDB, groupDB := BuildTrinoAuthFiles(orgs, p.adminPasswordHash)
	return p.upsertSecretMerge(ctx, TrinoAuthSecretName, map[string][]byte{
		TrinoAuthSecretKeyPasswordDB: []byte(passwordDB),
		TrinoAuthSecretKeyGroupDB:    []byte(groupDB),
	})
}

// BuildTrinoAuthFiles deterministically renders the (password.db,
// group.db) pair for a list of Trino-enabled orgs. Pure function so
// unit tests can exercise the projection without K8s + the rest of
// the reconcile path.
//
// Format conventions:
//
//	password.db: <team_id>:<bcrypt hash from OrgUser.Password>
//	             One line per org. Hash is copied through unchanged
//	             — it's already bcrypt in the configstore.
//	group.db:    <group_name>:<comma-separated users>
//	             NOTE: this is the opposite direction from password.db.
//	             For v1 (one user per org) the value is the single
//	             team_id. Easy to get backwards, hence this comment.
//
// Orgs without a RootPasswordHash are skipped silently (the listing
// query already filters to (org, root-user) pairs, so this is just
// defensive against future changes).
//
// adminPasswordHash is the bcrypt for opa.AdminPrincipal — the
// provisioner's own catalog-management identity. When non-empty, the
// admin entry is unconditionally prepended to both files (regardless of
// orgs). The plan ("Internal admin principal for catalog management")
// requires the admin in *both* password.db and group.db so OPA's
// is_admin = (admin username AND admin group) conjunction holds; the
// provisioner cannot do CREATE/DROP CATALOG otherwise. Empty hash skips
// the admin lines (acceptable in unit tests where the catalog client is
// a fake; never acceptable in production — see NewTrinoProvisioner).
func BuildTrinoAuthFiles(orgs []configstore.TrinoEnabledOrg, adminPasswordHash string) (passwordDB, groupDB string) {
	var pwLines, grpLines []string
	if adminPasswordHash != "" {
		// Prepend so the admin is always present even if every org row
		// is filtered out (empty cluster bootstrap).
		pwLines = append(pwLines, fmt.Sprintf("%s:%s", opa.AdminPrincipal, adminPasswordHash))
		grpLines = append(grpLines, fmt.Sprintf("%s:%s", opa.AdminGroup, opa.AdminPrincipal))
	}
	for _, o := range orgs {
		if o.RootPasswordHash == "" || o.OrgID == "" {
			continue
		}
		pwLines = append(pwLines, fmt.Sprintf("%s:%s", o.OrgID, o.RootPasswordHash))
		// group_name first, comma-separated users second. For v1 this
		// is one user per group (team_id only).
		grpLines = append(grpLines, fmt.Sprintf("%s:%s", TrinoGroupName(o.OrgID), o.OrgID))
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

// reconcileResourceGroups builds resource-groups.json and projects it
// into the trino-resource-groups ConfigMap. The Trino coordinator
// reads the file via the mounted ConfigMap; Trino's resource-groups
// plugin picks up file changes (no reload needed when only data
// changes).
func (p *TrinoProvisioner) reconcileResourceGroups(ctx context.Context, orgs []configstore.TrinoEnabledOrg) error {
	bytes, err := BuildTrinoResourceGroups(orgs)
	if err != nil {
		return fmt.Errorf("build resource-groups.json: %w", err)
	}
	return p.upsertConfigMap(ctx, TrinoResourceGroupsConfigMapName, map[string]string{
		TrinoResourceGroupsConfigMapKey: string(bytes),
	})
}

// resourceGroupSubGroup is the per-org subgroup serialized into
// resource-groups.json under root.tenants.<team_id>.
type resourceGroupSubGroup struct {
	Name                 string `json:"name"`
	SoftMemoryLimit      string `json:"softMemoryLimit"`
	HardConcurrencyLimit int    `json:"hardConcurrencyLimit"`
	MaxQueued            int    `json:"maxQueued"`
	SchedulingPolicy     string `json:"schedulingPolicy,omitempty"`
	SchedulingWeight     int    `json:"schedulingWeight,omitempty"`
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
	User  string `json:"user"`
	Group string `json:"group"`
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

// BuildTrinoResourceGroups deterministically renders the
// resource-groups.json file from a list of Trino-enabled orgs. Pure
// function for unit testing.
//
// Tree shape:
//
//	root
//	  └─ tenants
//	       ├─ 42        (team_id)
//	       ├─ 43
//	       └─ ...
//
// Selectors map Trino username (== team_id for customer principals) to
// the root.tenants.<team_id> group. The admin principal lives under a
// sibling root.admin.<admin_user> path so its catalog-DDL traffic is
// isolated from tenant traffic and so it matches a selector at all
// (without an admin selector, Trino's resource-group manager rejects
// the admin's SHOW/CREATE/DROP CATALOG queries with "Query is not
// associated with any resource group" — which would silently break
// every reconcile tick).
func BuildTrinoResourceGroups(orgs []configstore.TrinoEnabledOrg) ([]byte, error) {
	subgroups := make([]resourceGroupSubGroup, 0, len(orgs))
	// Admin selector first so its match wins fast on the reconcile-DDL
	// path; tenant selectors follow. (Selector order is first-match-
	// wins in Trino's file-backed manager. With disjoint usernames the
	// order is functionally irrelevant, but keeping admin first
	// documents the intent and makes the file readable.)
	selectors := []resourceGroupSelector{{
		User:  opa.AdminPrincipal,
		Group: "root.admin." + opa.AdminPrincipal,
	}}
	for _, o := range orgs {
		if o.OrgID == "" {
			continue
		}
		sg := tierLimits(o.Tier)
		// Subgroup name is sanitized to match TrinoResourceGroupName's
		// final path component. The selector's User field stays raw —
		// it matches against Trino's `current_user`, which is the
		// auth-time username (raw orgID).
		sg.Name = trinoSanitize(o.OrgID)
		subgroups = append(subgroups, sg)
		selectors = append(selectors, resourceGroupSelector{
			User:  o.OrgID,
			Group: TrinoResourceGroupName(o.OrgID),
		})
	}

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
					SubGroups:            subgroups,
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
// are `org_<team_id>` (TrinoGroupName); the admin group owns every
// managed catalog so the provisioner's own SHOW CATALOGS idempotency
// check (run as opa.AdminPrincipal) is allowed.
//
// ctx is currently unused (the builder is pure and the store Set is
// in-memory), but kept on the signature for parity with the other
// reconcile* steps and to permit instrumented builders later.
func (p *TrinoProvisioner) reconcileOPABundle(_ context.Context, orgs []configstore.TrinoEnabledOrg) error {
	gc := make(opa.GroupCatalogs, len(orgs)+1)
	adminCatalogs := make(map[string]bool, len(orgs))
	for _, o := range orgs {
		if o.OrgID == "" {
			continue
		}
		catalog := TrinoCatalogName(o.OrgID)
		gc[TrinoGroupName(o.OrgID)] = map[string]bool{catalog: true}
		adminCatalogs[catalog] = true
	}
	if len(adminCatalogs) > 0 {
		// Admin owns every managed catalog so SHOW CATALOGS / catalog
		// management succeeds. Reads still require is_admin (the
		// admin-group claim alone grants nothing — see opa.AdminGroup
		// docstring).
		gc[opa.AdminGroup] = adminCatalogs
	}
	bundle, err := p.bundleBuilder.BuildBundle(gc)
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
			return nil, fmt.Errorf("trino: %s (%s): %s", r.Error.ErrorName, r.Error.ErrorType, r.Error.Message)
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
