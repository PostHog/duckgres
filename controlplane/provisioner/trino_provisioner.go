//go:build kubernetes

package provisioner

import (
	"context"
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
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
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
	// Store is the cross-cutting Trino read/write surface. Two methods
	// matter: ListTrinoEnabledOrgs for the projection inputs, and
	// GetManagedWarehouseTrino for per-org status reads if needed.
	Store TrinoStore

	// IcebergStore is the per-org Iceberg config read surface (used to
	// build catalog properties pointing at the per-org Lakekeeper). We
	// reuse the existing configstore.ConfigStore-backed reader here.
	IcebergStore TrinoIcebergStore

	// Kubernetes is used for the auth Secret and resource-groups
	// ConfigMap projections in the customer Trino namespace.
	Kubernetes kubernetes.Interface

	// Namespace overrides TrinoCustomerNamespace. Empty == default.
	// Useful for dev clusters that namespace customer Trino differently.
	Namespace string

	// Catalog is the Trino REST client used to issue CREATE / ALTER /
	// DROP CATALOG. The provisioner authenticates as opa.AdminPrincipal;
	// the underlying HTTP client owns the admin Basic-auth credential
	// (loaded from a provisioner-only K8s Secret, not from configstore).
	Catalog TrinoCatalogClient

	// BundleStore is the in-memory holder of the most recently built OPA
	// bundle. The provisioner Set()s into it on every reconcile tick;
	// the customer-Trino OPA sidecar's bundle plugin polls an HTTP
	// endpoint (opa.Handler) backed by this same store, so distribution
	// is pull-based (see plan "Open Questions #6"). Owned and exposed by
	// the caller (typically multitenant.go) so the same store can be
	// mounted onto the provisioning HTTP server.
	BundleStore *opa.BundleStore

	// BundleBuilder builds the OPA bundle (gzip tarball) from a
	// GroupCatalogs map. In production, pass opa.NewBuilder().
	BundleBuilder opa.BundleBuilder

	// AdminPasswordHash is the bcrypt hash of the password the
	// provisioner authenticates with against the customer Trino cluster
	// as opa.AdminPrincipal. Projected verbatim into password.db (Trino
	// file authenticator expects bcrypt hashes). Required in production;
	// empty in unit tests (where the catalog client is a fake and
	// auth.db content doesn't reach Trino).
	AdminPasswordHash string

	// IAMAccountID is the AWS account id the per-org Iceberg catalogs
	// reference via iceberg.s3.aws-role-arn (the duckling-<orgid> role).
	// Required for AWS deployments; empty in dev / unit tests.
	IAMAccountID string

	// AWSRegion is the AWS region for the per-org S3 IAM role binding
	// and Iceberg catalog config. Empty in dev / unit tests.
	AWSRegion string
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

// TrinoProvisioner owns the four outputs the customer Trino cluster
// depends on for per-org configuration: catalog REST state, password +
// group files, resource-groups JSON, and OPA bundle.
//
// All four outputs are regenerated on every reconcile tick from the same
// configstore-derived input. Same input → same output (deterministic
// projection), so a re-run is a no-op against settled state.
type TrinoProvisioner struct {
	store             TrinoStore
	icebergStore      TrinoIcebergStore
	kubernetes        kubernetes.Interface
	namespace         string
	catalog           TrinoCatalogClient
	bundleStore       *opa.BundleStore
	bundleBuilder     opa.BundleBuilder
	adminPasswordHash string
	iamAccountID      string
	awsRegion         string
}

// NewTrinoProvisioner constructs a TrinoProvisioner from required deps.
// Returns an error if any required dep is missing rather than panicking
// downstream on the first reconcile tick.
func NewTrinoProvisioner(opts TrinoProvisionerOpts) (*TrinoProvisioner, error) {
	if opts.Store == nil {
		return nil, errors.New("TrinoProvisioner: Store is required")
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
		icebergStore:      opts.IcebergStore,
		kubernetes:        opts.Kubernetes,
		namespace:         ns,
		catalog:           opts.Catalog,
		bundleStore:       opts.BundleStore,
		bundleBuilder:     opts.BundleBuilder,
		adminPasswordHash: opts.AdminPasswordHash,
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
	orgs, err := p.store.ListTrinoEnabledOrgs()
	if err != nil {
		return fmt.Errorf("list trino-enabled orgs: %w", err)
	}

	// Stable iteration order so logs and projections are deterministic
	// regardless of how the DB driver returned the rows.
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].OrgID < orgs[j].OrgID })

	var errs []error

	// 1. Catalogs (REST). Per-org idempotent CREATE; orgs disabled since
	// last tick get DROP. Failing one org doesn't block the others. The
	// per-org outcome (catalog exists / pending / errored) is captured
	// so the trailing state-write step can attribute per-org status
	// correctly — the other three steps below are global (one K8s API
	// write each), so their failure attaches to every org uniformly.
	catalogOutcomes, catErr := p.reconcileCatalogs(ctx, orgs)
	if catErr != nil {
		errs = append(errs, fmt.Errorf("reconcile catalogs: %w", catErr))
	}

	// 2. Auth file projection (K8s Secret). Atomic Secret update.
	authErr := p.reconcileAuthSecret(ctx, orgs)
	if authErr != nil {
		errs = append(errs, fmt.Errorf("reconcile auth secret: %w", authErr))
	}

	// 3. Resource groups (K8s ConfigMap). Generated from the per-org
	// tier; rebuilt every tick.
	rgErr := p.reconcileResourceGroups(ctx, orgs)
	if rgErr != nil {
		errs = append(errs, fmt.Errorf("reconcile resource groups: %w", rgErr))
	}

	// 4. OPA bundle. GroupCatalogs keyed by Trino group name; Set into
	// the in-memory store the bundle HTTP handler serves.
	opaErr := p.reconcileOPABundle(ctx, orgs)
	if opaErr != nil {
		errs = append(errs, fmt.Errorf("reconcile opa bundle: %w", opaErr))
	}

	// Per-org state writes. The global steps' (auth/rg/opa) outcomes
	// are the same for every org since each is a single K8s API write
	// — wrap them once.
	globalErr := errors.Join(authErr, rgErr, opaErr)
	p.writePerOrgStates(orgs, catalogOutcomes, globalErr)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// catalogOutcome is the per-org result of the catalog reconcile step.
// Exactly one of (Created, Existed, Pending) is true when Err == nil;
// Err != nil means CREATE CATALOG itself failed and the org should
// land in Failed state with the error message.
type catalogOutcome struct {
	Created bool   // catalog was just created this tick
	Existed bool   // catalog already present, no action taken
	Pending bool   // skipped because Iceberg isn't ready yet (provisioning, not a failure)
	Err     error  // CREATE CATALOG error; mutually exclusive with the booleans
}

// writePerOrgStates writes the per-org state transition after one full
// reconcile tick. Per-org variance lives entirely at the catalog step;
// the global steps' failure (if any) applies uniformly to every
// Trino-enabled org.
//
// Priority of attribution (worst wins):
//
//   1. Per-org catalog error -> Failed + "catalog: <err>".
//   2. Global step error     -> Failed + "projection: <err>".
//   3. Catalog still pending -> Provisioning + "waiting for iceberg".
//   4. Everything succeeded  -> Ready + ReadyAt=now.
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
// writes the trino-auth K8s Secret. Idempotent — same orgs in same
// order produce byte-equal files.
//
// Mounted into the coordinator pod only (chart configuration in Stream
// F). Workers never see this Secret.
func (p *TrinoProvisioner) reconcileAuthSecret(ctx context.Context, orgs []configstore.TrinoEnabledOrg) error {
	passwordDB, groupDB := BuildTrinoAuthFiles(orgs, p.adminPasswordHash)
	return p.upsertSecret(ctx, TrinoAuthSecretName, map[string][]byte{
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
//   password.db: <team_id>:<bcrypt hash from OrgUser.Password>
//                One line per org. Hash is copied through unchanged
//                — it's already bcrypt in the configstore.
//   group.db:    <group_name>:<comma-separated users>
//                NOTE: this is the opposite direction from password.db.
//                For v1 (one user per org) the value is the single
//                team_id. Easy to get backwards, hence this comment.
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
	Name                  string `json:"name"`
	SoftMemoryLimit       string `json:"softMemoryLimit"`
	HardConcurrencyLimit  int    `json:"hardConcurrencyLimit"`
	MaxQueued             int    `json:"maxQueued"`
	SchedulingPolicy      string `json:"schedulingPolicy,omitempty"`
	SchedulingWeight      int    `json:"schedulingWeight,omitempty"`
}

type resourceGroupTier struct {
	Name                  string                  `json:"name"`
	SoftMemoryLimit       string                  `json:"softMemoryLimit"`
	HardConcurrencyLimit  int                     `json:"hardConcurrencyLimit"`
	MaxQueued             int                     `json:"maxQueued"`
	SubGroups             []resourceGroupSubGroup `json:"subGroups,omitempty"`
}

type resourceGroupRoot struct {
	Name                  string              `json:"name"`
	SoftMemoryLimit       string              `json:"softMemoryLimit"`
	HardConcurrencyLimit  int                 `json:"hardConcurrencyLimit"`
	MaxQueued             int                 `json:"maxQueued"`
	SubGroups             []resourceGroupTier `json:"subGroups,omitempty"`
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
//   root
//     └─ tenants
//          ├─ 42        (team_id)
//          ├─ 43
//          └─ ...
//
// Selectors map Trino username (== team_id for customer principals) to
// the root.tenants.<team_id> group.
func BuildTrinoResourceGroups(orgs []configstore.TrinoEnabledOrg) ([]byte, error) {
	subgroups := make([]resourceGroupSubGroup, 0, len(orgs))
	selectors := make([]resourceGroupSelector, 0, len(orgs))
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

	cfg := resourceGroupsFile{
		RootGroups: []resourceGroupRoot{{
			Name:                 "root",
			SoftMemoryLimit:      "100%",
			HardConcurrencyLimit: 200,
			MaxQueued:            1000,
			SubGroups: []resourceGroupTier{{
				Name:                 "tenants",
				SoftMemoryLimit:      "80%",
				HardConcurrencyLimit: 100,
				MaxQueued:            500,
				SubGroups:            subgroups,
			}},
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

// upsertSecret performs a get-or-create followed by a wholesale Data
// replacement. Mirrors the EnsureSecret pattern in lakekeeper_k8s.go;
// we don't reuse the helper there because that one is shaped around
// Lakekeeper's specific key set.
func (p *TrinoProvisioner) upsertSecret(ctx context.Context, name string, data map[string][]byte) error {
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
	secrets := p.kubernetes.CoreV1().Secrets(p.namespace)
	_, err := secrets.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create secret %s: %w", name, err)
	}
	existing, getErr := secrets.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get secret %s for update: %w", name, getErr)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if _, err := secrets.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s: %w", name, err)
	}
	return nil
}

// upsertConfigMap mirrors upsertSecret for non-secret config (resource-
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
// goroutine-safe but the cached Basic-auth header is set once at
// construction.
type trinoCatalogHTTPClient struct {
	baseURL  string
	hc       *http.Client
	username string
	password string
}

// NewTrinoCatalogHTTPClient builds an HTTP-backed TrinoCatalogClient.
// baseURL is the customer Trino coordinator endpoint (typically the
// in-cluster Service: http://trino:8080). Username defaults to
// opa.AdminPrincipal if empty.
func NewTrinoCatalogHTTPClient(baseURL, username, password string) TrinoCatalogClient {
	if username == "" {
		username = opa.AdminPrincipal
	}
	return &trinoCatalogHTTPClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		hc:       &http.Client{Timeout: 30 * time.Second},
		username: username,
		password: password,
	}
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
	req.Header.Set("X-Trino-User", c.username)
	req.Header.Set("Authorization", "Basic "+basicAuth(c.username, c.password))

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
		req.Header.Set("X-Trino-User", c.username)
		req.Header.Set("Authorization", "Basic "+basicAuth(c.username, c.password))
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
