//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// TestE2E_LakekeeperProvisionerOnOrbstack drives EnsureForOrg against a real
// Kubernetes cluster (orbstack) with the lakekeeper-operator installed and a
// Postgres + MinIO running on the host (via tmp/lakekeeper-proto/docker-compose).
//
// Skipped unless LAKEKEEPER_E2E_KUBECONFIG is set. To run:
//
//	export LAKEKEEPER_E2E_KUBECONFIG=$HOME/.kube/config
//	export PG_ADMIN_DSN='postgres://lakekeeper:lakekeeper@localhost:5434/lakekeeper?sslmode=disable'
//	go test -tags kubernetes ./controlplane/provisioner/ -run TestE2E_LakekeeperProvisionerOnOrbstack -v
//
// What's exercised:
//   - Real K8s API server (not the fake dynamic client) so CR field
//     types are validated against the operator's CRD OpenAPI schema
//   - Real Postgres so EnsureDatabase + EnsureRole + the GRANT chain run
//     end-to-end (verifies the previously-missing CREATE ROLE bug stays
//     fixed)
//   - Real lakekeeper-operator reconciling the CR — status.bootstrappedAt
//     flips when the operator actually completes bootstrap
//   - Real Lakekeeper pod calling /management/v1/warehouse, which exercises
//     our HTTP client against a real Lakekeeper server (not httptest)
func TestE2E_LakekeeperProvisionerOnOrbstack(t *testing.T) {
	kubeconfig := os.Getenv("LAKEKEEPER_E2E_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("LAKEKEEPER_E2E_KUBECONFIG not set; skipping orbstack e2e test")
	}
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Fatal("PG_ADMIN_DSN is required for e2e test")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("load kubeconfig: %v", err)
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	orgID := fmt.Sprintf("e2e%d", time.Now().Unix()) // alnum-only so it passes the label-value rule
	t.Logf("orgID=%s", orgID)

	const namespace = "lakekeeper" // separate from lakekeeper-operator's own namespace
	if err := ensureNamespace(t, kc, namespace); err != nil {
		t.Fatalf("ensure namespace %s: %v", namespace, err)
	}

	k8sClient := NewLakekeeperK8sClientWithClients(dc, kc, namespace)

	store := newFakeStore()
	store.warehouses[orgID] = &configstore.ManagedWarehouse{
		OrgID: orgID,
		State: configstore.ManagedWarehouseStateProvisioning,
	}

	// The provisioner computes a baseURL like
	// http://lakekeeper-<orgid>.lakekeeper.svc:8181 — that hostname is only
	// resolvable inside the cluster. From the host, we go through the API
	// server's service-proxy subresource. WithClientFactory swaps the
	// baseURL for the proxy URL on every NewLakekeeperClient call.
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatalf("rest http client: %v", err)
	}
	apiBase := cfg.Host
	clientFor := func(baseURL string) *LakekeeperClient {
		// The provisioner passes a baseURL like
		//   http://lakekeeper-<orgid>.lakekeeper.svc:8181
		// (no /catalog suffix — management API lives at the root). From the
		// host we route via the API server's service-proxy subresource.
		proxyBase := fmt.Sprintf("%s/api/v1/namespaces/%s/services/http:%s:8181/proxy",
			apiBase, namespace, LakekeeperResourceName(orgID))
		c := NewLakekeeperClient(proxyBase)
		c.WithHTTPClient(httpClient)
		return c
	}
	p := NewLakekeeperProvisioner(store, k8sClient,
		WithImage("quay.io/lakekeeper/catalog:latest"),
		WithClientFactory(clientFor),
	)

	in := ProvisioningInputs{
		AdminDSN:  dsn,
		PGHost:    "host.docker.internal", // reachable from orbstack pods
		PGPort:    5434,
		PGSSLMode: "disable", // prototype PG has no TLS
		S3: S3StorageConfig{
			Bucket:    "warehouse",
			KeyPrefix: orgID,
			Endpoint:  "http://host.docker.internal:9100",
			Region:    "us-east-1",
			Flavor:    "s3-compat",
			// MinIO doesn't actually do STS-compat with these creds, but
			// the warehouse-create call only validates the credential is
			// well-formed; the actual S3 operations happen later when a
			// duckling tries to write.
			StaticAccessKeyID:     "minioadmin",
			StaticAccessKeySecret: "minioadmin",
		},
	}

	t.Cleanup(func() {
		// Cleanup: drop the per-org PG database + role, delete the CR + Secret.
		// Keep best-effort — failures here shouldn't break the test result.
		ctx := context.Background()
		_ = k8sClient.dynamic.Resource(lakekeeperGVR).Namespace(namespace).
			Delete(ctx, LakekeeperResourceName(orgID), metav1DeleteOpts())
		_ = k8sClient.kubernetes.CoreV1().Secrets(namespace).
			Delete(ctx, LakekeeperResourceName(orgID), metav1DeleteOpts())
		dropDatabaseAndRole(t, dsn, lakekeeperDBName(orgID))
	})

	// Retry EnsureForOrg until bootstrap completes (or timeout). The real
	// operator's bootstrap takes 10–30s including image pull on first run.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	startedAt := time.Now()
	var lastErr error
	for ctx.Err() == nil {
		lastErr = p.EnsureForOrg(ctx, store.warehouses[orgID], in)
		if lastErr == nil {
			t.Logf("EnsureForOrg succeeded after %s", time.Since(startedAt))
			break
		}
		if errors.Is(lastErr, ErrBootstrapPending) {
			t.Logf("[%s] bootstrap pending, requeueing...", time.Since(startedAt).Round(time.Second))
			time.Sleep(3 * time.Second)
			continue
		}
		t.Fatalf("EnsureForOrg returned fatal error: %v", lastErr)
	}
	if lastErr != nil {
		t.Fatalf("EnsureForOrg timed out: %v", lastErr)
	}

	w := store.warehouses[orgID]
	if !w.Iceberg.Enabled {
		t.Errorf("Iceberg.Enabled not flipped on")
	}
	if w.Iceberg.LakekeeperEndpoint == "" {
		t.Errorf("LakekeeperEndpoint not persisted")
	}
	if w.Iceberg.LakekeeperWarehouse != lakekeeperWarehouseName(orgID) {
		t.Errorf("LakekeeperWarehouse = %q, want %q", w.Iceberg.LakekeeperWarehouse, lakekeeperWarehouseName(orgID))
	}
	if w.Iceberg.LakekeeperClientID != oauthClientID(orgID) {
		t.Errorf("LakekeeperClientID = %q, want %q", w.Iceberg.LakekeeperClientID, oauthClientID(orgID))
	}
	t.Logf("warehouse row: endpoint=%s warehouse=%s client_id=%s",
		w.Iceberg.LakekeeperEndpoint, w.Iceberg.LakekeeperWarehouse, w.Iceberg.LakekeeperClientID)
}
