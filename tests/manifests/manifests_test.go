// Package manifests_test holds artifact-validation unit tests for the static
// k8s/ manifests — no cluster, no build tag, runs in the normal `go test ./...`
// lane. These four asserts were rescued from the retired kind suite
// (tests/k8s/setup_config_test.go) when its end-to-end coverage moved to the
// real-cluster harness in tests/e2e-mw-dev/. They guard real shipped config
// (k8s/rbac.yaml, k8s/networkpolicy.yaml), so they keep earning their place;
// the rest of that file tested the kind-harness loader and went with it.
package manifests_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestControlPlaneRBACIncludesLeaseAccess(t *testing.T) {
	content := readManifest(t, "k8s", "rbac.yaml")
	for _, want := range []string{
		`apiGroups: ["coordination.k8s.io"]`,
		`resources: ["leases"]`,
		`verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/rbac.yaml", want)
		}
	}
}

func TestControlPlaneRBACIncludesSharedWorkerConfigMapRead(t *testing.T) {
	content := readManifest(t, "k8s", "rbac.yaml")
	for _, want := range []string{
		`resources: ["configmaps"]`,
		`verbs: ["get"]`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/rbac.yaml", want)
		}
	}
}

func TestControlPlaneRBACDefinesSharedWorkerServiceAccount(t *testing.T) {
	content := readManifest(t, "k8s", "rbac.yaml")
	for _, want := range []string{
		"kind: ServiceAccount",
		"name: duckgres-worker",
		"automountServiceAccountToken: false",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/rbac.yaml", want)
		}
	}
	if strings.Contains(content, "subjects:\n  - kind: ServiceAccount\n    name: duckgres-worker") {
		t.Fatal("expected shared worker service account to have no RoleBinding in k8s/rbac.yaml")
	}
}

func TestNamespaceManifestIncludesDucklingsNamespace(t *testing.T) {
	content := readManifest(t, "k8s", "namespace.yaml")
	for _, want := range []string{
		"name: duckgres",
		"name: ducklings",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/namespace.yaml", want)
		}
	}
}

func TestNetworkPolicyAllowsControlPlaneToReachWorkerGRPC(t *testing.T) {
	manifest := readManifest(t, "k8s", "networkpolicy.yaml")

	var controlPlaneDoc string
	for _, doc := range strings.Split(manifest, "---") {
		if strings.Contains(doc, "name: duckgres-control-plane-boundaries") {
			controlPlaneDoc = doc
			break
		}
	}
	if controlPlaneDoc == "" {
		t.Fatal("could not find control-plane network policy in k8s/networkpolicy.yaml")
	}
	for _, want := range []string{
		"- port: 8816",
		"protocol: TCP",
	} {
		if !strings.Contains(controlPlaneDoc, want) {
			t.Fatalf("expected %q in control-plane network policy in k8s/networkpolicy.yaml", want)
		}
	}
	if strings.Contains(controlPlaneDoc, "namespaceSelector: {}") {
		t.Fatal("expected control-plane ingress to stay namespace-local in k8s/networkpolicy.yaml")
	}
}

func TestQueryLogWriterManifestDefinesDeployment(t *testing.T) {
	content := readManifest(t, "k8s", "query-log-writer.yaml")
	for _, want := range []string{
		"kind: Deployment",
		"name: duckgres-query-log-writer",
		"serviceAccountName: duckgres-query-log-writer",
		"- \"--mode\"",
		"- \"query-log-writer\"",
		"name: metrics",
		"containerPort: 9090",
		"name: DUCKGRES_CONFIG_STORE",
		"name: DUCKGRES_QUERY_LOG_KAFKA_BROKERS",
		"name: DUCKGRES_QUERY_LOG_KAFKA_TOPIC",
		"name: DUCKGRES_QUERY_LOG_KAFKA_GROUP_ID",
		"name: config",
		"mountPath: /etc/duckgres",
		"name: data",
		"mountPath: /data",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/query-log-writer.yaml", want)
		}
	}
}

func TestQueryLogWriterManifestRequiresRuntimeConfig(t *testing.T) {
	content := readManifest(t, "k8s", "query-log-writer.yaml")
	for _, name := range []string{
		"DUCKGRES_CONFIG_STORE",
		"DUCKGRES_QUERY_LOG_KAFKA_BROKERS",
		"DUCKGRES_QUERY_LOG_KAFKA_TOPIC",
	} {
		block := envBlock(t, content, name)
		if strings.Contains(block, "optional: true") {
			t.Fatalf("expected %s to be required in k8s/query-log-writer.yaml", name)
		}
	}
}

func TestControlPlaneManifestsExposeOptionalKafkaQueryLogEnv(t *testing.T) {
	for _, parts := range [][]string{
		{"k8s", "control-plane-multitenant-local.yaml"},
		{"k8s", "kind", "control-plane.yaml"},
	} {
		content := readManifest(t, parts...)
		for _, want := range []string{
			"name: DUCKGRES_QUERY_LOG_SINK",
			"name: DUCKGRES_QUERY_LOG_KAFKA_BROKERS",
			"name: DUCKGRES_QUERY_LOG_KAFKA_TOPIC",
			"name: DUCKGRES_QUERY_LOG_KAFKA_CLIENT_ID",
			"optional: true",
		} {
			if !strings.Contains(content, want) {
				t.Fatalf("expected %q in %s", want, filepath.Join(parts...))
			}
		}
	}
}

func TestRBACIncludesQueryLogWriterAccess(t *testing.T) {
	content := readManifest(t, "k8s", "rbac.yaml")
	for _, want := range []string{
		"name: duckgres-query-log-writer",
		"name: duckgres-query-log-writer-runtime",
		`resources: ["secrets"]`,
		`verbs: ["get"]`,
		"name: duckgres-query-log-writer-ducklings",
		"namespace: ducklings",
		`apiGroups: ["k8s.posthog.com"]`,
		`resources: ["ducklings"]`,
		`verbs: ["get"]`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/rbac.yaml", want)
		}
	}
	if strings.Contains(content, "kind: ClusterRole\nmetadata:\n  name: duckgres-query-log-writer-ducklings") ||
		strings.Contains(content, "kind: ClusterRoleBinding\nmetadata:\n  name: duckgres-query-log-writer-ducklings") {
		t.Fatal("expected query-log writer Duckling access to be namespace-scoped")
	}
}

func TestNetworkPolicyAllowsQueryLogKafkaTraffic(t *testing.T) {
	manifest := readManifest(t, "k8s", "networkpolicy.yaml")

	var controlPlaneDoc string
	var writerDoc string
	for _, doc := range strings.Split(manifest, "---") {
		switch {
		case strings.Contains(doc, "name: duckgres-control-plane-boundaries"):
			controlPlaneDoc = doc
		case strings.Contains(doc, "name: duckgres-query-log-writer-boundaries"):
			writerDoc = doc
		}
	}
	if controlPlaneDoc == "" {
		t.Fatal("could not find control-plane network policy in k8s/networkpolicy.yaml")
	}
	if writerDoc == "" {
		t.Fatal("could not find query-log writer network policy in k8s/networkpolicy.yaml")
	}
	for _, doc := range []struct {
		name string
		body string
	}{
		{name: "control-plane", body: controlPlaneDoc},
		{name: "query-log-writer", body: writerDoc},
	} {
		for _, want := range []string{
			"- port: 9092",
			"protocol: TCP",
		} {
			if !strings.Contains(doc.body, want) {
				t.Fatalf("expected %q in %s network policy", want, doc.name)
			}
		}
	}
	if !strings.Contains(writerDoc, "- port: 80") {
		t.Fatal("expected query-log writer network policy to allow HTTP extension bootstrap/object-store egress")
	}
}

func TestQueryLogWriterAlertsReferenceWriterMetrics(t *testing.T) {
	content := readManifest(t, "k8s", "query-log-writer-alerts.example.yaml")
	for _, want := range []string{
		"kind: PrometheusRule",
		"DuckgresQueryLogWriterFailures",
		"DuckgresQueryLogWriterDrops",
		"DuckgresQueryLogWriterRetriesHigh",
		`duckgres_query_log_kafka_writer_events_total{outcome="failed"}`,
		`duckgres_query_log_kafka_writer_events_total{outcome="dropped",reason=~"no_target|resolve_failed|write_failed"}`,
		`duckgres_query_log_kafka_writer_events_total{outcome="retried"}`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in k8s/query-log-writer-alerts.example.yaml", want)
		}
	}
}

func TestManifestTestsRunInUnitRecipe(t *testing.T) {
	content := readManifest(t, "justfile")
	if !strings.Contains(content, "./tests/manifests/...") {
		t.Fatal("expected just test-unit to include ./tests/manifests/...")
	}
}

// readManifest reads a file relative to the project root (located by walking up
// to the go.mod), matching how the manifests ship in the repo.
func readManifest(t *testing.T, parts ...string) string {
	t.Helper()
	root := projectRoot(t)
	b, err := os.ReadFile(filepath.Join(append([]string{root}, parts...)...))
	if err != nil {
		t.Fatalf("read %s: %v", filepath.Join(parts...), err)
	}
	return string(b)
}

func projectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root")
		}
		dir = parent
	}
}

func envBlock(t *testing.T, content, name string) string {
	t.Helper()
	start := strings.Index(content, "name: "+name)
	if start == -1 {
		t.Fatalf("could not find env var %s", name)
	}
	rest := content[start:]
	if next := strings.Index(rest[len("name: "+name):], "\n            - name: "); next != -1 {
		return rest[:len("name: "+name)+next]
	}
	return rest
}
