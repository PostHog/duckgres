//go:build kubernetes

package provisioner

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newFakeESOClient builds a DucklingClient over a fake dynamic client that
// knows the ExternalSecret kind, optionally seeded with one ExternalSecret
// carrying the given status conditions.
func newFakeESOClient(t *testing.T, name string, conditions []interface{}) *DucklingClient {
	t.Helper()
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "external-secrets.io", Version: "v1", Kind: "ExternalSecret",
	}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "external-secrets.io", Version: "v1", Kind: "ExternalSecretList",
	}, &unstructured.UnstructuredList{})
	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme)
	if name != "" {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "external-secrets.io/v1",
			"kind":       "ExternalSecret",
			"metadata":   map[string]interface{}{"name": name, "namespace": ducklingNamespace},
		}}
		if conditions != nil {
			obj.Object["status"] = map[string]interface{}{"conditions": conditions}
		}
		if _, err := fakeClient.Resource(externalSecretGVR).Namespace(ducklingNamespace).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed ExternalSecret: %v", err)
		}
	}
	return NewDucklingClientWithDynamic(fakeClient)
}

// TestExternalSecretSyncError pins the diagnostic read: name derivation
// (duckling-<name>-password), Ready=False → "reason: message", healthy → "",
// and a missing object → error (the caller's degrade-quietly signal).
func TestExternalSecretSyncError(t *testing.T) {
	t.Run("sync error surfaces reason and message", func(t *testing.T) {
		dc := newFakeESOClient(t, "duckling-org-a-password", []interface{}{
			map[string]interface{}{
				"type": "Ready", "status": "False",
				"reason":  "SecretSyncedError",
				"message": "AccessDeniedException: not authorized to perform secretsmanager:GetSecretValue",
			},
		})
		msg, err := dc.ExternalSecretSyncError(context.Background(), "org-a")
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if !strings.HasPrefix(msg, "SecretSyncedError: AccessDeniedException") {
			t.Fatalf("msg = %q, want reason+message", msg)
		}
	})

	t.Run("healthy yields empty", func(t *testing.T) {
		dc := newFakeESOClient(t, "duckling-org-a-password", []interface{}{
			map[string]interface{}{"type": "Ready", "status": "True", "reason": "SecretSynced"},
		})
		msg, err := dc.ExternalSecretSyncError(context.Background(), "org-a")
		if err != nil || msg != "" {
			t.Fatalf("msg=%q err=%v, want empty and nil", msg, err)
		}
	})

	t.Run("no status yields empty", func(t *testing.T) {
		dc := newFakeESOClient(t, "duckling-org-a-password", nil)
		msg, err := dc.ExternalSecretSyncError(context.Background(), "org-a")
		if err != nil || msg != "" {
			t.Fatalf("msg=%q err=%v, want empty and nil", msg, err)
		}
	})

	t.Run("missing object errors (degrade signal)", func(t *testing.T) {
		dc := newFakeESOClient(t, "", nil)
		if _, err := dc.ExternalSecretSyncError(context.Background(), "org-a"); err == nil {
			t.Fatal("want an error for a missing ExternalSecret")
		}
	})
}
