//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
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
// The Duckling composition provisions AWS infrastructure (Aurora, S3, IAM)
// but not K8s workloads — those are managed by the duckgres Helm chart.
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
	// Iceberg is populated when spec.iceberg.enabled=true and the
	// composition has reconciled the per-tenant S3 Tables bucket. Empty
	// when the tenant has not opted in. TableBucketArn arrives last
	// (after the bucket is reconciled); the controller uses its presence
	// as the trigger to flip iceberg_state to Ready in the configstore.
	Iceberg struct {
		TableBucketArn string
		NamespaceName  string
		Region         string
	}
	IAMRoleARN         string
	ReadyCondition     bool
	SyncedFalseMessage string
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

// ducklingName converts an org ID to a valid K8s/AWS resource name by stripping
// hyphens. This keeps names under the 63-char limit for RDS cluster identifiers
// when the org ID is a full UUID.
func ducklingName(orgID string) string {
	return strings.ReplaceAll(orgID, "-", "")
}

// CreateOptions carries per-org knobs that shape the generated Duckling CR.
type CreateOptions struct {
	// MetadataStoreType selects the Duckling's metadata-store backend. Empty
	// is treated as configstore.MetadataStoreKindAurora (the historical
	// default, retained only for reconciling pre-existing aurora ducklings).
	// The control plane creates cnpg-shard and external; any other value is
	// rejected by Create.
	MetadataStoreType string
	MinACU            float64
	MaxACU            float64
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
	// composition only provisions a per-tenant S3 Tables bucket when this
	// is true; flipping it post-create is handled by the controller's
	// Ready-state drift logic.
	IcebergEnabled bool
	// IcebergNamespace is the Iceberg namespace within the tenant's catalog.
	// Empty falls back to the XRD default ("main").
	IcebergNamespace string
}

// Create creates a Duckling CR for the given org.
func (d *DucklingClient) Create(ctx context.Context, orgID string, opts CreateOptions) error {
	name := ducklingName(orgID)

	var metadataStore map[string]interface{}
	switch opts.MetadataStoreType {
	case configstore.MetadataStoreKindCnpgShard:
		// The cnpg-shard metadata store backs the per-tenant Lakekeeper
		// Iceberg catalog on the shared CloudNativePG shard. It carries no
		// per-claim config — the composition reads the active shard from chart
		// values and provisions a lakekeeper_<org> role + database via
		// provider-sql. The XRD admission rule requires iceberg.enabled=true
		// for this type, so refuse to emit a CR that would be rejected (and
		// that would have no usable catalog).
		if !opts.IcebergEnabled {
			return fmt.Errorf("create duckling CR %q: metadata store type %q requires iceberg enabled", name, configstore.MetadataStoreKindCnpgShard)
		}
		// No aurora block and no pgbouncer block: cnpg-shard tenants reach
		// Postgres through the shard's own session-mode Pooler, not a
		// per-Duckling PgBouncer.
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
	case "", configstore.MetadataStoreKindAurora:
		metadataStore = map[string]interface{}{
			"type": configstore.MetadataStoreKindAurora,
			"aurora": map[string]interface{}{
				"minACU": opts.MinACU,
				"maxACU": opts.MaxACU,
			},
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
	default:
		return fmt.Errorf("create duckling CR %q: unsupported data store type %q", name, opts.DataStoreType)
	}

	spec := map[string]interface{}{
		"metadataStore": metadataStore,
		"dataStore":     dataStore,
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

// Get fetches the Duckling CR and parses its status.
func (d *DucklingClient) Get(ctx context.Context, orgID string) (*DucklingStatus, error) {
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	return parseDucklingStatus(cr)
}

// Delete removes the Duckling CR for the given org.
func (d *DucklingClient) Delete(ctx context.Context, orgID string) error {
	name := ducklingName(orgID)
	err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete duckling CR %q: %w", name, err)
	}
	return nil
}

// GetPgBouncerEnabled reads spec.metadataStore.pgbouncer.enabled from the
// Duckling CR. Missing blocks (composition at an older schema, CR never
// carried a pgbouncer section) are reported as false — same as an explicit
// opt-out — so the caller just needs to compare against the desired value.
func (d *DucklingClient) GetPgBouncerEnabled(ctx context.Context, orgID string) (bool, error) {
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
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
// under metadataStore (aurora, type) are left untouched.
func (d *DucklingClient) SetPgBouncerEnabled(ctx context.Context, orgID string, enabled bool) error {
	name := ducklingName(orgID)
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
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
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
	name := ducklingName(orgID)
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

func parseDucklingStatus(cr *unstructured.Unstructured) (*DucklingStatus, error) {
	status, ok := cr.Object["status"].(map[string]interface{})
	if !ok {
		return &DucklingStatus{}, nil
	}

	ds := &DucklingStatus{
		IAMRoleARN: getNestedString(status, "iamRoleArn"),
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
		ds.Iceberg.TableBucketArn = getNestedString(ic, "tableBucketArn")
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
