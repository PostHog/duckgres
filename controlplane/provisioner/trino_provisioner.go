//go:build kubernetes

package provisioner

import (
	"bytes"
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

// TrinoAdminProvisionerUser is the reserved Trino principal the
// provisioner uses for catalog management. OPA distinguishes this
// principal from customer users to scope CREATE/ALTER/DROP CATALOG.
const TrinoAdminProvisionerUser = "__admin_provisioner"

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

// TrinoResourceGroupName returns the resource-group selector key for an
// org. Customer principals' Trino username == orgName (numeric team_id);
// selectors match on that, scoping the org into root.tenants.<orgName>.
func TrinoResourceGroupName(orgName string) string {
	return "root.tenants." + orgName
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

// OPABundleClient is the surface the provisioner uses to push a generated
// bundle to the customer Trino cluster's OPA sidecar via the bundle API.
// One pod hosts the OPA sidecar (alongside the coordinator); the push is
// over loopback / NetworkPolicy-restricted egress. Interface lets tests
// substitute a fake.
type OPABundleClient interface {
	PushBundle(ctx context.Context, bundle []byte) error
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
	// DROP CATALOG. The provisioner authenticates as
	// TrinoAdminProvisionerUser; the underlying HTTP client owns the
	// admin Basic-auth credential.
	Catalog TrinoCatalogClient

	// OPA is the bundle push client. Bundle bytes come from
	// BundleBuilder.
	OPA OPABundleClient

	// BundleBuilder builds the OPA bundle from the user→catalogs map.
	// Construct with opa.NewStubBuilder() until the real Rego-backed
	// builder ships in a sibling PR (which swaps in a real constructor
	// without touching this call site).
	BundleBuilder opa.BundleBuilder

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
	store         TrinoStore
	icebergStore  TrinoIcebergStore
	kubernetes    kubernetes.Interface
	namespace     string
	catalog       TrinoCatalogClient
	opa           OPABundleClient
	bundleBuilder opa.BundleBuilder
	iamAccountID  string
	awsRegion     string
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
	if opts.OPA == nil {
		return nil, errors.New("TrinoProvisioner: OPA client is required")
	}
	if opts.BundleBuilder == nil {
		return nil, errors.New("TrinoProvisioner: BundleBuilder is required")
	}
	ns := opts.Namespace
	if ns == "" {
		ns = TrinoCustomerNamespace
	}
	return &TrinoProvisioner{
		store:         opts.Store,
		icebergStore:  opts.IcebergStore,
		kubernetes:    opts.Kubernetes,
		namespace:     ns,
		catalog:       opts.Catalog,
		opa:           opts.OPA,
		bundleBuilder: opts.BundleBuilder,
		iamAccountID:  opts.IAMAccountID,
		awsRegion:     opts.AWSRegion,
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
	// last tick get DROP. Failing one org doesn't block the others.
	if err := p.reconcileCatalogs(ctx, orgs); err != nil {
		errs = append(errs, fmt.Errorf("reconcile catalogs: %w", err))
	}

	// 2. Auth file projection (K8s Secret). Atomic Secret update.
	if err := p.reconcileAuthSecret(ctx, orgs); err != nil {
		errs = append(errs, fmt.Errorf("reconcile auth secret: %w", err))
	}

	// 3. Resource groups (K8s ConfigMap). Generated from the per-org
	// tier; rebuilt every tick.
	if err := p.reconcileResourceGroups(ctx, orgs); err != nil {
		errs = append(errs, fmt.Errorf("reconcile resource groups: %w", err))
	}

	// 4. OPA bundle (push via OPA bundle API). user_catalogs map keyed
	// by team_id, value is the org's owned catalog set.
	if err := p.reconcileOPABundle(ctx, orgs); err != nil {
		errs = append(errs, fmt.Errorf("reconcile opa bundle: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// reconcileCatalogs issues CREATE CATALOG for each Trino-enabled org
// (idempotent — SHOW CATALOGS first, skip if already present), and
// DROP CATALOG for any `org_*_iceberg` catalogs the customer cluster
// has that aren't in the enabled set.
//
// We intentionally only touch catalogs whose name matches the
// org_*_iceberg prefix so other catalogs (system, jmx, hand-rolled
// ones for the maintenance use case) survive untouched.
func (p *TrinoProvisioner) reconcileCatalogs(ctx context.Context, orgs []configstore.TrinoEnabledOrg) error {
	existing, err := p.catalog.ListCatalogs(ctx)
	if err != nil {
		return fmt.Errorf("list trino catalogs: %w", err)
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
			continue
		}
		iceberg, err := p.icebergStore.GetManagedWarehouseIceberg(o.OrgID)
		if err != nil {
			errs = append(errs, fmt.Errorf("org %s: read iceberg config: %w", o.OrgID, err))
			continue
		}
		if iceberg == nil || iceberg.LakekeeperEndpoint == "" {
			// Org opted into Trino but Iceberg isn't ready in Lakekeeper
			// yet. Skip silently — the next reconcile picks it up once
			// the Lakekeeper provisioner finishes.
			slog.Debug("Trino reconcile: iceberg not ready yet, skipping catalog create.",
				"org", o.OrgID)
			continue
		}
		props := p.buildCatalogProperties(o.OrgID, iceberg)
		if err := p.catalog.CreateCatalog(ctx, name, props); err != nil {
			errs = append(errs, fmt.Errorf("create catalog %s: %w", name, err))
			continue
		}
		slog.Info("Trino reconcile: catalog created.", "org", o.OrgID, "catalog", name)
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
		return errors.Join(errs...)
	}
	return nil
}

// buildCatalogProperties returns the Trino catalog property set for an
// org's Iceberg catalog. Lakekeeper stays allowall in v1, so no OAuth2
// credentials are embedded here — just the endpoint, the warehouse
// name, and the per-org S3 IAM role.
func (p *TrinoProvisioner) buildCatalogProperties(orgID string, ic *configstore.ManagedWarehouseIceberg) map[string]string {
	props := map[string]string{
		"connector.name":              "iceberg",
		"iceberg.catalog.type":        "rest",
		"iceberg.rest-catalog.uri":    ic.LakekeeperEndpoint,
		"iceberg.rest-catalog.warehouse": ic.LakekeeperWarehouse,
	}
	if p.iamAccountID != "" {
		props["iceberg.s3.aws-role-arn"] = fmt.Sprintf("arn:aws:iam::%s:role/duckling-%s", p.iamAccountID, orgID)
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
	passwordDB, groupDB := BuildTrinoAuthFiles(orgs)
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
func BuildTrinoAuthFiles(orgs []configstore.TrinoEnabledOrg) (passwordDB, groupDB string) {
	var pwLines, grpLines []string
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
		sg.Name = o.OrgID
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

// reconcileOPABundle builds the user_catalogs map and asks the bundle
// builder to render a bundle, then pushes it via the OPA bundle API.
//
// While the bundle builder is the stub (returns "{}"), this step still
// exercises the code path end-to-end so a regression landing alongside
// the real builder's merge is loud, not silent.
func (p *TrinoProvisioner) reconcileOPABundle(ctx context.Context, orgs []configstore.TrinoEnabledOrg) error {
	uc := make(opa.UserCatalogs, len(orgs))
	for _, o := range orgs {
		if o.OrgID == "" {
			continue
		}
		uc[o.OrgID] = map[string]bool{TrinoCatalogName(o.OrgID): true}
	}
	bundle, err := p.bundleBuilder.BuildBundle(uc)
	if err != nil {
		return fmt.Errorf("build opa bundle: %w", err)
	}
	if err := p.opa.PushBundle(ctx, bundle); err != nil {
		return fmt.Errorf("push opa bundle: %w", err)
	}
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
// HTTP implementations of TrinoCatalogClient + OPABundleClient.
// =====================================================================

// trinoCatalogHTTPClient drives the Trino REST API for catalog
// management. Authentication is HTTP Basic (Trino's standard for the
// file password authenticator); the configured user is
// TrinoAdminProvisionerUser, with the password living in a K8s Secret
// mounted only into the provisioner pod.
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
// TrinoAdminProvisionerUser if empty.
func NewTrinoCatalogHTTPClient(baseURL, username, password string) TrinoCatalogClient {
	if username == "" {
		username = TrinoAdminProvisionerUser
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
func (c *trinoCatalogHTTPClient) drainStatement(ctx context.Context, initial []byte) ([][]interface{}, error) {
	var all [][]interface{}
	body := initial
	for {
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
	withProps := props
	delete(withProps, "connector.name")
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

// =====================================================================
// OPA bundle HTTP client.
// =====================================================================

// opaBundleHTTPClient pushes a bundle to the OPA sidecar's bundle API
// (PUT /v1/policies or the bundle service endpoint; specific path is
// configurable). The bundle bytes are uploaded as the request body.
//
// For v1 the OPA sidecar runs alongside the customer Trino coordinator
// pod, so the URL is typically http://localhost:8181 from the
// provisioner's perspective (port-forward in dev) or
// http://trino-coordinator-opa:8181 from the provisioner pod in K8s.
type opaBundleHTTPClient struct {
	baseURL string
	hc      *http.Client
}

// NewOPABundleHTTPClient builds an OPABundleClient. baseURL is the OPA
// sidecar's bundle API root.
func NewOPABundleHTTPClient(baseURL string) OPABundleClient {
	return &opaBundleHTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		hc:      &http.Client{Timeout: 30 * time.Second},
	}
}

// PushBundle uploads the bundle bytes to the OPA bundle endpoint.
// We use PUT /v1/policies/duckgres so the bundle replaces the
// previously-pushed one rather than appending. OPA's bundle API path
// can vary by configuration; the path may need to match the real
// builder PR's PushBundle helper before merge.
func (c *opaBundleHTTPClient) PushBundle(ctx context.Context, bundle []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/v1/policies/duckgres", bytes.NewReader(bundle))
	if err != nil {
		return fmt.Errorf("build opa request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("opa push: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("opa push: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
