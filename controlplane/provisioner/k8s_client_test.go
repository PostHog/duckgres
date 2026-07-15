//go:build kubernetes

package provisioner

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func mrWithPolicies(policies []interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{}
	if policies != nil {
		spec["managementPolicies"] = policies
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{"spec": spec}}
}

// TestDescribeManagementPolicies pins the op-log rendering of a managed
// resource's managementPolicies (the orphan wait's periodic diagnostic), and
// that its reading of "absent means full lifecycle" matches the conservative
// managementPoliciesHaveDelete used for the orphan decision itself.
func TestDescribeManagementPolicies(t *testing.T) {
	cases := []struct {
		name       string
		policies   []interface{}
		want       string
		haveDelete bool
	}{
		{"absent defaults to full lifecycle", nil, `absent (defaults to ["*"], full lifecycle incl. Delete)`, true},
		{"wildcard", []interface{}{"*"}, `["*"]`, true},
		{"explicit delete", []interface{}{"Observe", "Create", "Update", "Delete"}, `["Observe","Create","Update","Delete"]`, true},
		{"orphan-ready no-delete", []interface{}{"Observe", "Create", "Update"}, `["Observe","Create","Update"]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := mrWithPolicies(tc.policies)
			if got := describeManagementPolicies(obj); got != tc.want {
				t.Fatalf("describeManagementPolicies = %q, want %q", got, tc.want)
			}
			if got := managementPoliciesHaveDelete(obj); got != tc.haveDelete {
				t.Fatalf("managementPoliciesHaveDelete = %t, want %t", got, tc.haveDelete)
			}
		})
	}
}

// seedDucklingForCompaction creates a Duckling CR with the given spec.ducklake
// block (nil = the legacy no-spec.ducklake shape) and status metadata-store
// type, for the compaction-patch tests.
func seedDucklingForCompaction(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, name string, ducklake map[string]interface{}, statusMetadataType string) {
	t.Helper()
	spec := map[string]interface{}{
		"metadataStore": map[string]interface{}{"type": statusMetadataType},
	}
	if ducklake != nil {
		spec["ducklake"] = ducklake
	}
	obj := map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": name, "namespace": ducklingNamespace},
		"spec":       spec,
		"status": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type":     statusMetadataType,
				"endpoint": "endpoint.example.internal",
			},
		},
	}
	cr := &unstructured.Unstructured{Object: obj}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
}

// applyXRDDucklakeDefault simulates the Duckling XRD's structural-schema
// defaulting: when spec.ducklake exists WITHOUT an `enabled` key, the API
// server stamps the schema default `enabled: false` onto it. The fake dynamic
// client performs no defaulting, so the tests apply it explicitly after each
// patch — this is what makes the regression real: without the pin in
// SetCompactionEnabled, a compaction patch that materializes spec.ducklake on
// a legacy CR gets `enabled: false` stamped and DuckLake is silently disabled
// for the org (the prod incident).
func applyXRDDucklakeDefault(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, name string) {
	t.Helper()
	ctx := context.Background()
	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR for defaulting: %v", err)
	}
	spec, _ := cr.Object["spec"].(map[string]interface{})
	dl, ok := spec["ducklake"].(map[string]interface{})
	if !ok {
		return
	}
	if _, has := dl["enabled"]; !has {
		dl["enabled"] = false
		if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Update(ctx, cr, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("apply XRD default: %v", err)
		}
	}
}

// specDucklakeOf re-fetches the CR's spec.ducklake block (nil when absent).
func specDucklakeOf(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, name string) map[string]interface{} {
	t.Helper()
	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec, _ := cr.Object["spec"].(map[string]interface{})
	dl, _ := spec["ducklake"].(map[string]interface{})
	return dl
}

// TestSetCompactionEnabledPinsDuckLakeEnablement pins the XRD-defaulting guard
// (prod incident): the reshard's compaction merge patch on a legacy CR with NO
// spec.ducklake object materializes that object, and the XRD's
// `ducklake.enabled: {default: false}` then stamps `enabled: false` onto it —
// silently disabling DuckLake for the org (every later activation fails with
// "tenant activation requires a ducklake metadata_store"). SetCompactionEnabled
// must pin the org's EFFECTIVE enablement (the legacy metadata-store type
// coupling: external → enabled, matching
// shared_worker_activator.buildDuckLakeConfigFromDuckling) into the same patch
// when — and only when — it materializes the object.
func TestSetCompactionEnabledPinsDuckLakeEnablement(t *testing.T) {
	ctx := context.Background()
	off := false

	// Legacy CR (no spec.ducklake) on an EXTERNAL metadata store: the coupling
	// says DuckLake is enabled, so the patch must pin enabled=true — XRD
	// defaulting must find the key already present.
	t.Run("legacy CR external status pins enabled=true", func(t *testing.T) {
		dc, fakeK8s := newFakeDucklingClient()
		seedDucklingForCompaction(t, fakeK8s, "org-a", nil, configstore.MetadataStoreKindExternal)
		if err := dc.SetCompactionEnabled(ctx, "org-a", &off); err != nil {
			t.Fatalf("SetCompactionEnabled: %v", err)
		}
		applyXRDDucklakeDefault(t, fakeK8s, "org-a")
		dl := specDucklakeOf(t, fakeK8s, "org-a")
		if enabled, ok := dl["enabled"].(bool); !ok || !enabled {
			t.Fatalf("spec.ducklake.enabled = %v (present %t), want pinned true — XRD defaulting would otherwise disable DuckLake", dl["enabled"], ok)
		}
		enabled, present, err := dc.GetCompactionSetting(ctx, "org-a")
		if err != nil || !present || enabled {
			t.Fatalf("compaction = enabled %t present %t err %v, want explicit false", enabled, present, err)
		}
	})

	// Legacy CR on a cnpg-shard metadata store: the coupling derives
	// enabled=false, so pinning false matches (and the XRD default agrees).
	t.Run("legacy CR cnpg status pins the coupling value", func(t *testing.T) {
		dc, fakeK8s := newFakeDucklingClient()
		seedDucklingForCompaction(t, fakeK8s, "org-b", nil, configstore.MetadataStoreKindCnpgShard)
		if err := dc.SetCompactionEnabled(ctx, "org-b", &off); err != nil {
			t.Fatalf("SetCompactionEnabled: %v", err)
		}
		applyXRDDucklakeDefault(t, fakeK8s, "org-b")
		dl := specDucklakeOf(t, fakeK8s, "org-b")
		if enabled, ok := dl["enabled"].(bool); !ok || enabled {
			t.Fatalf("spec.ducklake.enabled = %v (present %t), want pinned false (matches the type coupling)", dl["enabled"], ok)
		}
	})

	// CR that already HAS spec.ducklake: the patch must not touch `enabled`.
	// enabled=true with a cnpg status is the discriminating shape — if the
	// code wrongly pinned the coupling value (false), this would flip.
	t.Run("existing spec.ducklake is left untouched", func(t *testing.T) {
		dc, fakeK8s := newFakeDucklingClient()
		seedDucklingForCompaction(t, fakeK8s, "org-c", map[string]interface{}{"enabled": true}, configstore.MetadataStoreKindCnpgShard)
		if err := dc.SetCompactionEnabled(ctx, "org-c", &off); err != nil {
			t.Fatalf("SetCompactionEnabled: %v", err)
		}
		applyXRDDucklakeDefault(t, fakeK8s, "org-c")
		dl := specDucklakeOf(t, fakeK8s, "org-c")
		if enabled, ok := dl["enabled"].(bool); !ok || !enabled {
			t.Fatalf("spec.ducklake.enabled = %v (present %t), want the pre-existing true left untouched", dl["enabled"], ok)
		}
	})

	// The restore path (enabled=nil removes the compaction key) can materialize
	// the object just the same on a legacy CR — it must pin too.
	t.Run("restore remove-path pins on a legacy CR", func(t *testing.T) {
		dc, fakeK8s := newFakeDucklingClient()
		seedDucklingForCompaction(t, fakeK8s, "org-d", nil, configstore.MetadataStoreKindExternal)
		if err := dc.SetCompactionEnabled(ctx, "org-d", nil); err != nil {
			t.Fatalf("SetCompactionEnabled(nil): %v", err)
		}
		applyXRDDucklakeDefault(t, fakeK8s, "org-d")
		dl := specDucklakeOf(t, fakeK8s, "org-d")
		if enabled, ok := dl["enabled"].(bool); !ok || !enabled {
			t.Fatalf("spec.ducklake.enabled = %v (present %t), want pinned true on the remove-path too", dl["enabled"], ok)
		}
		if _, present, _ := dc.GetCompactionSetting(ctx, "org-d"); present {
			t.Fatal("compaction key present after the remove-path, want absent")
		}
	})
}
