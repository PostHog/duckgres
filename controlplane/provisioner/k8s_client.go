//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var ducklingGVR = schema.GroupVersionResource{
	Group:    "k8s.posthog.com",
	Version:  "v1alpha1",
	Resource: "ducklings",
}

const ducklingNamespace = "ducklings"

// DucklingStatus holds the parsed status from a Duckling CR.
// The Duckling composition provisions AWS infrastructure (S3, IAM) and the
// per-tenant metadata Postgres, but not K8s workloads — those are managed by
// the duckgres Helm chart.
type DucklingStatus struct {
	MetadataStore struct {
		Type              string
		Endpoint          string
		PgBouncerEndpoint string
		Password          string
		User              string
		Database          string
	}
	DataStore struct {
		Type       string
		BucketName string
		S3Region   string
	}
	// Iceberg is populated when spec.iceberg.enabled=true. The
	// composition provisions a per-org Lakekeeper instance; the
	// Lakekeeper provisioner extension drives readiness off the
	// Lakekeeper CR itself, so we only need namespace/region here.
	Iceberg struct {
		NamespaceName string
		Region        string
	}
	IAMRoleARN         string
	ReadyCondition     bool
	SyncedFalseMessage string

	// DuckLakeEnabled is spec.ducklake.enabled, read present/absent: nil when
	// the CR predates the decoupled ducklake field (the worker activator then
	// falls back to the legacy type-based default — DuckLake on for
	// external, off for cnpg-shard). Non-nil for decoupled ducklings.
	DuckLakeEnabled *bool
}

// DucklingClient wraps a Kubernetes dynamic client for Duckling CR operations.
type DucklingClient struct {
	client dynamic.Interface
}

// NewDucklingClient creates a DucklingClient using in-cluster config.
func NewDucklingClient() (*DucklingClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	dc, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return &DucklingClient{client: dc}, nil
}

// NewDucklingClientWithDynamic creates a DucklingClient with a provided dynamic.Interface (for testing).
func NewDucklingClientWithDynamic(client dynamic.Interface) *DucklingClient {
	return &DucklingClient{client: client}
}

// ducklingName is the k8s/AWS resource name derived from an org ID, used for
// the Duckling CR, the IAM role (duckling-<name>), the S3 bucket, the
// Lakekeeper CR/SA/Secret, etc. Org IDs are validated as DNS-1123 labels at
// provision time (lowercase alphanumerics + hyphens), so this only lowercases:
// hyphens are preserved, keeping in-cluster names human-readable and injective
// with the org ID.
//
// (It used to strip hyphens to keep per-org Aurora RDS cluster identifiers
// short, but the control plane no longer provisions per-org Aurora clusters,
// and stripping was lossy — "a-b" and "ab" collided.)
func ducklingName(orgID string) string {
	return strings.ToLower(orgID)
}

// pgIdentSanitizeRe matches characters not allowed in an unquoted Postgres
// identifier fragment.
var pgIdentSanitizeRe = regexp.MustCompile(`[^a-z0-9_]`)

// pgIdentSuffix sanitizes an org ID into a valid unquoted Postgres identifier
// fragment: lowercase, with every non-[a-z0-9_] character (notably hyphens)
// mapped to '_'. Postgres identifiers can't contain hyphens unquoted, so PG
// object names can't preserve them the way k8s names do. This mirrors the
// Crossplane composition's $pgIdent transform for cnpg-shard, so the external
// (provisioner-created) and cnpg-shard (composition-created) Lakekeeper
// databases follow the same convention. Injective for org IDs restricted to
// [a-z0-9-], which the provision-time validation guarantees.
func pgIdentSuffix(orgID string) string {
	return pgIdentSanitizeRe.ReplaceAllString(strings.ToLower(orgID), "_")
}

// legacyDucklingName is the pre-hyphen-preservation transform (hyphens
// stripped). Retained ONLY so lookups can still find Duckling CRs created
// before ducklingName started preserving hyphens — e.g. a CR named
// "018d351a9ff70000eaff4628875ad045" for org "018d351a-9ff7-0000-eaff-...".
// New CRs are always created under ducklingName (hyphen-preserving).
func legacyDucklingName(orgID string) string {
	return strings.ReplaceAll(strings.ToLower(orgID), "-", "")
}

// CreateOptions carries per-org knobs that shape the generated Duckling CR.
type CreateOptions struct {
	// MetadataStoreType selects the Duckling's metadata-store backend. The
	// control plane creates cnpg-shard and external; any other value (including
	// empty) is rejected by Create.
	MetadataStoreType string
	PgBouncerEnabled  bool

	// External metadata store (MetadataStoreType == "external"). Endpoint and
	// ExternalPasswordAWSSecret are required for that type; User/Database
	// default to "postgres" at the XRD level when empty.
	ExternalEndpoint          string
	ExternalPasswordAWSSecret string
	ExternalUser              string
	ExternalDatabase          string

	// DataStoreType selects spec.dataStore.type. Empty defaults to "s3bucket"
	// (the composition provisions a fresh per-org bucket). "external" reuses an
	// existing bucket — DataStoreBucket is then required, DataStoreRegion
	// optional (XRD/composition default applies when empty).
	DataStoreType   string
	DataStoreBucket string
	DataStoreRegion string

	// IcebergEnabled toggles spec.iceberg.enabled on the Duckling CR. The
	// composition only provisions the per-tenant Lakekeeper Iceberg catalog
	// when this is true; flipping it post-create is handled by the controller's
	// Ready-state drift logic.
	IcebergEnabled bool
	// IcebergNamespace is the Iceberg namespace within the tenant's catalog.
	// Empty falls back to the XRD default ("main").
	IcebergNamespace string

	// DuckLakeEnabled toggles spec.ducklake.enabled. Independent of Iceberg and
	// of the metadata-store type; at least one of DuckLakeEnabled/IcebergEnabled
	// must be true (Create rejects a CR with neither).
	DuckLakeEnabled bool
}

// Create creates a Duckling CR for the given org.
func (d *DucklingClient) Create(ctx context.Context, orgID string, opts CreateOptions) error {
	name := ducklingName(orgID)

	var metadataStore map[string]interface{}
	switch opts.MetadataStoreType {
	case configstore.MetadataStoreKindCnpgShard:
		// The cnpg-shard metadata store is the per-tenant Postgres on the shared
		// CloudNativePG shard, provisioned via provider-sql. It carries no
		// per-claim config — the composition reads the active shard from chart
		// values. It hosts the DuckLake catalog and/or the Lakekeeper PG
		// depending on the catalog flags; a CR with neither catalog has nothing
		// to attach, so refuse it.
		if !opts.IcebergEnabled && !opts.DuckLakeEnabled {
			return fmt.Errorf("create duckling CR %q: metadata store type %q requires at least one of ducklake or iceberg enabled", name, configstore.MetadataStoreKindCnpgShard)
		}
		// No pgbouncer block: cnpg-shard tenants reach Postgres through the
		// shard's own session-mode Pooler, not a per-Duckling PgBouncer.
		metadataStore = map[string]interface{}{"type": configstore.MetadataStoreKindCnpgShard}
	case configstore.MetadataStoreKindExternal:
		// A pre-existing Postgres (e.g. RDS), referenced by endpoint + an AWS
		// Secrets Manager secret name for the password (resolved by the
		// composition via ESO). Backs a DuckLake catalog (iceberg disabled) or
		// the Lakekeeper catalog (iceberg enabled). User/Database are omitted
		// when empty so the XRD defaults ("postgres") apply.
		if opts.ExternalEndpoint == "" || opts.ExternalPasswordAWSSecret == "" {
			return fmt.Errorf("create duckling CR %q: metadata store type %q requires endpoint and passwordAwsSecret", name, configstore.MetadataStoreKindExternal)
		}
		external := map[string]interface{}{
			"endpoint":          opts.ExternalEndpoint,
			"passwordAwsSecret": opts.ExternalPasswordAWSSecret,
		}
		if opts.ExternalUser != "" {
			external["user"] = opts.ExternalUser
		}
		if opts.ExternalDatabase != "" {
			external["database"] = opts.ExternalDatabase
		}
		metadataStore = map[string]interface{}{
			"type":     configstore.MetadataStoreKindExternal,
			"external": external,
		}
		if opts.PgBouncerEnabled {
			metadataStore["pgbouncer"] = map[string]interface{}{
				"enabled": true,
			}
		}
	default:
		return fmt.Errorf("create duckling CR %q: unsupported metadata store type %q (control plane creates only %q or %q)",
			name, opts.MetadataStoreType, configstore.MetadataStoreKindCnpgShard, configstore.MetadataStoreKindExternal)
	}

	var dataStore map[string]interface{}
	switch opts.DataStoreType {
	case "external":
		// Reuse an existing bucket (the composition provisions none).
		if opts.DataStoreBucket == "" {
			return fmt.Errorf("create duckling CR %q: dataStore type %q requires a bucket name", name, "external")
		}
		external := map[string]interface{}{"bucketName": opts.DataStoreBucket}
		if opts.DataStoreRegion != "" {
			external["region"] = opts.DataStoreRegion
		}
		dataStore = map[string]interface{}{"type": "external", "external": external}
	case "", "s3bucket":
		dataStore = map[string]interface{}{"type": "s3bucket"}
		// When the control plane supplies the bucket name (CP-owned naming),
		// pin it on the CR so the composition provisions exactly that bucket
		// instead of deriving one. Empty ⇒ omit the field and let the
		// composition derive (legacy ducklings + deployments without
		// DUCKGRES_DUCKLING_BUCKET_SUFFIX).
		if opts.DataStoreBucket != "" {
			dataStore["bucketName"] = opts.DataStoreBucket
		}
	default:
		return fmt.Errorf("create duckling CR %q: unsupported data store type %q", name, opts.DataStoreType)
	}

	spec := map[string]interface{}{
		"metadataStore": metadataStore,
		"dataStore":     dataStore,
		// DuckLake is set explicitly (true or false) so the catalog choice is
		// unambiguous on the CR — the worker activator reads spec.ducklake.enabled
		// and only falls back to the legacy type-based default when the field is
		// absent (i.e. for ducklings created before decoupling).
		"ducklake": map[string]interface{}{"enabled": opts.DuckLakeEnabled},
	}
	if opts.IcebergEnabled {
		iceberg := map[string]interface{}{"enabled": true}
		if ns := opts.IcebergNamespace; ns != "" {
			iceberg["namespace"] = ns
		}
		spec["iceberg"] = iceberg
	}
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ducklingNamespace,
			},
			"spec": spec,
		},
	}

	_, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create duckling CR %q: %w", name, err)
	}
	return nil
}

// getCR fetches the org's Duckling CR, trying the current hyphen-preserving
// name first and falling back to the legacy de-hyphenated name for CRs created
// before ducklingName preserved hyphens. Returns the CR and the name it was
// found under (callers that mutate need the actual name). When neither exists,
// returns the current-name NotFound error so callers see the new scheme.
func (d *DucklingClient) getCR(ctx context.Context, orgID string) (*unstructured.Unstructured, string, error) {
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return cr, name, nil
	}
	if legacy := legacyDucklingName(orgID); legacy != name && apierrors.IsNotFound(err) {
		if lcr, lerr := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, legacy, metav1.GetOptions{}); lerr == nil {
			return lcr, legacy, nil
		}
	}
	return nil, name, err
}

// ListCRNames returns the names of every Duckling CR in the namespace. Used by
// the admin drift finder to detect orphan CRs (CRs with no warehouse row).
func (d *DucklingClient) ListCRNames(ctx context.Context) ([]string, error) {
	list, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list duckling CRs: %w", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}
	return names, nil
}

// CRMetadataStore is the metadata-store slice of one Duckling CR's status —
// the live (composition-assigned) backend, notably which cnpg shard a
// cnpg-shard tenant landed on. The config store does not hold this (the
// composition picks the active shard at provision time and only the CR status
// carries the endpoint), so the admin console reads it from here.
type CRMetadataStore struct {
	Type     string // "cnpg-shard" | "external"
	Endpoint string // Postgres host, e.g. "shard-001-pooler.cnpg-shards.svc.cluster.local"
}

// CRMetadataStores lists every Duckling CR and returns each one's
// status.metadataStore type + endpoint, keyed by CR name. CRs whose status is
// not yet populated (still provisioning) are skipped rather than failing the
// whole listing.
func (d *DucklingClient) CRMetadataStores(ctx context.Context) (map[string]CRMetadataStore, error) {
	list, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list duckling CRs: %w", err)
	}
	out := make(map[string]CRMetadataStore, len(list.Items))
	for i := range list.Items {
		status, perr := parseDucklingStatus(&list.Items[i])
		if perr != nil || status.MetadataStore.Endpoint == "" {
			continue
		}
		out[list.Items[i].GetName()] = CRMetadataStore{
			Type:     status.MetadataStore.Type,
			Endpoint: status.MetadataStore.Endpoint,
		}
	}
	return out, nil
}

// CRStatus reports whether the org's Duckling CR exists and, if so, whether it
// is Ready — without treating absence as an error. A NotFound resolves to
// (false, false, nil) so the drift finder can classify a missing CR rather than
// surfacing a 500. Any other error is returned as-is.
func (d *DucklingClient) CRStatus(ctx context.Context, orgID string) (present bool, ready bool, err error) {
	cr, _, gerr := d.getCR(ctx, orgID)
	if gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return false, false, nil
		}
		return false, false, gerr
	}
	status, perr := parseDucklingStatus(cr)
	if perr != nil {
		return true, false, perr
	}
	return true, status.ReadyCondition, nil
}

// Get fetches the Duckling CR and parses its status.
func (d *DucklingClient) Get(ctx context.Context, orgID string) (*DucklingStatus, error) {
	cr, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return nil, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	return parseDucklingStatus(cr)
}

// Delete removes the Duckling CR for the given org. Resolves the legacy name so
// pre-rename CRs are still deletable.
func (d *DucklingClient) Delete(ctx context.Context, orgID string) error {
	_, name, err := d.getCR(ctx, orgID)
	if apierrors.IsNotFound(err) {
		// Already gone — nothing to delete. (reconcileDeleting treats this as success.)
		return err
	}
	if derr := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Delete(ctx, name, metav1.DeleteOptions{}); derr != nil {
		return fmt.Errorf("delete duckling CR %q: %w", name, derr)
	}
	return nil
}

// GetPgBouncerEnabled reads spec.metadataStore.pgbouncer.enabled from the
// Duckling CR. Missing blocks (composition at an older schema, CR never
// carried a pgbouncer section) are reported as false — same as an explicit
// opt-out — so the caller just needs to compare against the desired value.
func (d *DucklingClient) GetPgBouncerEnabled(ctx context.Context, orgID string) (bool, error) {
	cr, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return false, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	ms, ok := spec["metadataStore"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	pgb, ok := ms["pgbouncer"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	enabled, _ := pgb["enabled"].(bool)
	return enabled, nil
}

// SetPgBouncerEnabled patches spec.metadataStore.pgbouncer.enabled on the
// Duckling CR for the given org. Uses a JSON merge patch (RFC 7396) so the
// call is idempotent and only touches the pgbouncer block — sibling fields
// under metadataStore (type, external) are left untouched.
func (d *DucklingClient) SetPgBouncerEnabled(ctx context.Context, orgID string, enabled bool) error {
	_, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return fmt.Errorf("resolve duckling CR for %q: %w", orgID, err)
	}
	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"pgbouncer": map[string]interface{}{
					"enabled": enabled,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal pgbouncer patch for %q: %w", name, err)
	}
	_, err = d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch duckling CR %q pgbouncer: %w", name, err)
	}
	return nil
}

// GetIcebergEnabled reads spec.iceberg.enabled from the Duckling CR. Missing
// blocks (composition at an older schema, CR predates iceberg support) are
// reported as false — same as an explicit opt-out — so the caller can just
// compare against the desired value.
func (d *DucklingClient) GetIcebergEnabled(ctx context.Context, orgID string) (bool, error) {
	cr, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return false, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	iceberg, ok := spec["iceberg"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	enabled, _ := iceberg["enabled"].(bool)
	return enabled, nil
}

// SetIcebergEnabled patches spec.iceberg.enabled on the Duckling CR for the
// given org. Uses a JSON merge patch (RFC 7396) so the call is idempotent and
// only touches the iceberg block — sibling fields under spec (metadataStore,
// dataStore) are left untouched.
//
// Note: iceberg.namespace is enforced immutable by the XRD's CEL rule, so
// this method intentionally only patches enabled — namespace changes have
// to go through warehouse re-creation.
func (d *DucklingClient) SetIcebergEnabled(ctx context.Context, orgID string, enabled bool) error {
	_, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return fmt.Errorf("resolve duckling CR for %q: %w", orgID, err)
	}
	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"iceberg": map[string]interface{}{
				"enabled": enabled,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal iceberg patch for %q: %w", name, err)
	}
	_, err = d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch duckling CR %q iceberg: %w", name, err)
	}
	return nil
}

// GetDataStoreBucketName reads spec.dataStore.bucketName from the Duckling CR.
// Empty (missing block / missing key) means the CR predates CP-owned naming
// and the composition is still deriving the name — the signal the backfill in
// reconcileReady uses to decide whether to patch.
func (d *DucklingClient) GetDataStoreBucketName(ctx context.Context, orgID string) (string, error) {
	cr, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return "", fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		return "", nil
	}
	ds, ok := spec["dataStore"].(map[string]interface{})
	if !ok {
		return "", nil
	}
	bucket, _ := ds["bucketName"].(string)
	return bucket, nil
}

// SetDataStoreBucketName patches spec.dataStore.bucketName on the Duckling CR.
// JSON merge patch (RFC 7396) so it's idempotent and only touches dataStore —
// the type field and any sibling under spec are left untouched. Used to backfill
// the CP-owned name onto ducklings created before this field existed (their
// spec.dataStore carries only {type: s3bucket}); the name supplied here is the
// same string the composition was already deriving into status, so the patch
// causes no bucket churn — it just moves the name from a derived output into a
// durable input so the composition stops deriving.
func (d *DucklingClient) SetDataStoreBucketName(ctx context.Context, orgID, bucket string) error {
	_, name, err := d.getCR(ctx, orgID)
	if err != nil {
		return fmt.Errorf("resolve duckling CR for %q: %w", orgID, err)
	}
	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"dataStore": map[string]interface{}{
				"bucketName": bucket,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal dataStore bucket patch for %q: %w", name, err)
	}
	_, err = d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch duckling CR %q dataStore bucketName: %w", name, err)
	}
	return nil
}

// readSpecDuckLakeEnabled returns spec.ducklake.enabled as *bool — nil when the
// ducklake block (or its enabled key) is absent, so callers can distinguish a
// legacy CR (apply the type-based default) from an explicit true/false.
func readSpecDuckLakeEnabled(cr *unstructured.Unstructured) *bool {
	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		return nil
	}
	dl, ok := spec["ducklake"].(map[string]interface{})
	if !ok {
		return nil
	}
	v, ok := dl["enabled"].(bool)
	if !ok {
		return nil
	}
	return &v
}

func parseDucklingStatus(cr *unstructured.Unstructured) (*DucklingStatus, error) {
	// spec.ducklake.enabled lives in .spec (not .status) — read it first so it's
	// captured even before the composition writes any status. Present/absent is
	// significant: absent (legacy CR) leaves DuckLakeEnabled nil.
	duckLakeEnabled := readSpecDuckLakeEnabled(cr)

	status, ok := cr.Object["status"].(map[string]interface{})
	if !ok {
		return &DucklingStatus{DuckLakeEnabled: duckLakeEnabled}, nil
	}

	ds := &DucklingStatus{
		IAMRoleARN:      getNestedString(status, "iamRoleArn"),
		DuckLakeEnabled: duckLakeEnabled,
	}

	// Parse status.metadataStore
	if md, ok := status["metadataStore"].(map[string]interface{}); ok {
		ds.MetadataStore.Type = getNestedString(md, "type")
		ds.MetadataStore.Endpoint = getNestedString(md, "endpoint")
		ds.MetadataStore.PgBouncerEndpoint = getNestedString(md, "pgbouncerEndpoint")
		ds.MetadataStore.Password = getNestedString(md, "password")
		ds.MetadataStore.User = getNestedString(md, "user")
		ds.MetadataStore.Database = getNestedString(md, "database")
	}

	// Parse status.dataStore
	if store, ok := status["dataStore"].(map[string]interface{}); ok {
		ds.DataStore.Type = getNestedString(store, "type")
		ds.DataStore.BucketName = getNestedString(store, "bucketName")
		ds.DataStore.S3Region = getNestedString(store, "s3Region")
	}

	// Parse status.iceberg (only populated when spec.iceberg.enabled=true)
	if ic, ok := status["iceberg"].(map[string]interface{}); ok {
		ds.Iceberg.NamespaceName = getNestedString(ic, "namespaceName")
		ds.Iceberg.Region = getNestedString(ic, "region")
	}

	// Parse conditions
	conditions, _ := status["conditions"].([]interface{})
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType := getNestedString(condMap, "type")
		condStatus := getNestedString(condMap, "status")

		switch condType {
		case "Ready":
			ds.ReadyCondition = condStatus == "True"
		case "Synced":
			if condStatus == "False" {
				ds.SyncedFalseMessage = getNestedString(condMap, "message")
			}
		}
	}

	return ds, nil
}

func getNestedString(obj map[string]interface{}, key string) string {
	v, _ := obj[key].(string)
	return v
}
