//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server/iceberg"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildIcebergConfig_LakekeeperAllowall(t *testing.T) {
	cs := fake.NewSimpleClientset()
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: "duckgres"}

	src := &configstore.ManagedWarehouseIceberg{
		Enabled:                   true,
		Backend:                   iceberg.BackendLakekeeper,
		Namespace:                 "main",
		LakekeeperEndpoint:        "http://lakekeeper-acme.lakekeeper.svc:8181/catalog",
		LakekeeperWarehouse:       "org-acme",
		LakekeeperClientID:        "duckling-acme",
		LakekeeperOAuth2ServerURI: "", // allowall mode
		// LakekeeperClientCredentials is empty → no secret resolution
	}
	ic, err := a.buildIcebergConfig(context.Background(), "acme", src)
	if err != nil {
		t.Fatalf("buildIcebergConfig: %v", err)
	}
	if ic.LakekeeperEndpoint != src.LakekeeperEndpoint {
		t.Errorf("LakekeeperEndpoint = %q, want %q", ic.LakekeeperEndpoint, src.LakekeeperEndpoint)
	}
	if ic.LakekeeperWarehouse != "org-acme" {
		t.Errorf("LakekeeperWarehouse = %q, want org-acme", ic.LakekeeperWarehouse)
	}
	if ic.LakekeeperClientSecret != "" {
		t.Errorf("allowall mode should not call readSecretValue; got LakekeeperClientSecret=%q", ic.LakekeeperClientSecret)
	}
}

func TestBuildIcebergConfig_LakekeeperResolvesOAuth2Secret(t *testing.T) {
	const ns = "lakekeeper"
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "lakekeeper-acme", Namespace: ns},
		Data:       map[string][]byte{"oauth2-client-secret": []byte("super-secret-token")},
	})
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: ns}

	src := &configstore.ManagedWarehouseIceberg{
		Enabled:                   true,
		Backend:                   iceberg.BackendLakekeeper,
		LakekeeperEndpoint:        "http://lakekeeper-acme.lakekeeper.svc:8181/catalog",
		LakekeeperWarehouse:       "org-acme",
		LakekeeperClientID:        "duckling-acme",
		LakekeeperOAuth2ServerURI: "http://oidc/token",
		LakekeeperClientCredentials: configstore.SecretRef{
			Namespace: ns,
			Name:      "lakekeeper-acme",
			Key:       "oauth2-client-secret",
		},
	}
	ic, err := a.buildIcebergConfig(context.Background(), "acme", src)
	if err != nil {
		t.Fatalf("buildIcebergConfig: %v", err)
	}
	if ic.LakekeeperClientSecret != "super-secret-token" {
		t.Errorf("LakekeeperClientSecret = %q, want super-secret-token", ic.LakekeeperClientSecret)
	}
}

func TestBuildIcebergConfig_EmptyBackendDefaultsLakekeeper(t *testing.T) {
	cs := fake.NewSimpleClientset()
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: "duckgres"}

	src := &configstore.ManagedWarehouseIceberg{
		Enabled: true,
		// Backend left empty → ResolvedBackend returns lakekeeper
		LakekeeperEndpoint:  "http://x/catalog",
		LakekeeperWarehouse: "org-x",
	}
	ic, err := a.buildIcebergConfig(context.Background(), "x", src)
	if err != nil {
		t.Fatalf("buildIcebergConfig: %v", err)
	}
	if ic.ResolvedBackend() != iceberg.BackendLakekeeper {
		t.Errorf("empty Backend should resolve to lakekeeper, got %q", ic.ResolvedBackend())
	}
}

func TestBuildIcebergConfig_SecretFetchErrorSurfaces(t *testing.T) {
	const ns = "lakekeeper"
	cs := fake.NewSimpleClientset() // no secret pre-loaded
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: ns}

	src := &configstore.ManagedWarehouseIceberg{
		Enabled:                   true,
		Backend:                   iceberg.BackendLakekeeper,
		LakekeeperOAuth2ServerURI: "http://oidc/token",
		LakekeeperClientCredentials: configstore.SecretRef{
			Namespace: ns,
			Name:      "missing-secret",
			Key:       "oauth2-client-secret",
		},
	}
	_, err := a.buildIcebergConfig(context.Background(), "x", src)
	if err == nil {
		t.Fatal("expected error for missing secret, got nil")
	}
}
