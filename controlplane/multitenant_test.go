//go:build kubernetes

package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/provisioner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestHealthHandlerReturnsServiceUnavailableWhenUnhealthy(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.GET("/health", newHealthHandler(func() bool { return false }))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 while unhealthy, got %d", rec.Code)
	}
}

func TestHealthHandlerReturnsOKWhenHealthy(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.GET("/health", newHealthHandler(func() bool { return true }))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 while healthy, got %d", rec.Code)
	}
}

func TestCatalogCopierProberUsesShardProvisionerSecret(t *testing.T) {
	cluster := k8sfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cnpg-shard-004-provisioner", Namespace: "ducklings"},
		Data: map[string][]byte{
			"endpoint": []byte("shard-004-rw.cnpg-shards.svc.cluster.local"),
			"port":     []byte("5432"),
			"username": []byte("tenant_provisioner"),
			"password": []byte("not-logged"),
		},
	})
	var got provisioner.CatalogEndpoint
	prober := catalogCopierProber{
		cluster: cluster,
		probe: func(_ context.Context, endpoint provisioner.CatalogEndpoint) error {
			got = endpoint
			return nil
		},
	}

	if err := prober.ProbeCNPG(context.Background(), "shard-004"); err != nil {
		t.Fatalf("ProbeCNPG: %v", err)
	}
	if got.Host != "shard-004-rw.cnpg-shards.svc.cluster.local" || got.Port != 5432 ||
		got.User != "tenant_provisioner" || got.Database != "postgres" ||
		got.Password != "not-logged" || got.SSLMode != "require" {
		t.Fatalf("probe endpoint = %+v", got)
	}
}
