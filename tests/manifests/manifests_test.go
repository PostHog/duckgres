// Package manifests_test holds artifact-validation unit tests for the static
// k8s/ manifests — no cluster, no build tag, runs in the normal `go test ./...`
// lane. These four asserts were rescued from the retired kind suite
// (tests/k8s/setup_config_test.go) when its end-to-end coverage moved to the
// real-cluster harness in tests/mw-dev/. They guard real shipped config
// (k8s/rbac.yaml, k8s/networkpolicy.yaml), so they keep earning their place;
// the rest of that file tested the kind-harness loader and went with it.
package manifests_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

// TestNetworkPolicyCoversReshardRunnerPods pins the dedicated reshard runner
// pod policy: selected by app=duckgres-reshard, egress to Postgres (5432,
// config store + cnpg poolers + external RDS), S3/STS (443), the CP admin API
// (8080, the ephemeral-password pull) and DNS — and NO ingress rules (the
// runner only dials out).
func TestNetworkPolicyCoversReshardRunnerPods(t *testing.T) {
	manifest := readManifest(t, "k8s", "networkpolicy.yaml")

	var reshardDoc string
	for _, doc := range strings.Split(manifest, "---") {
		if strings.Contains(doc, "name: duckgres-reshard-runner-boundaries") {
			reshardDoc = doc
			break
		}
	}
	if reshardDoc == "" {
		t.Fatal("could not find reshard runner network policy in k8s/networkpolicy.yaml")
	}
	for _, want := range []string{
		"app: duckgres-reshard",
		"- port: 5432",
		"- port: 443",
		"- port: 8080",
		"- port: 53",
	} {
		if !strings.Contains(reshardDoc, want) {
			t.Fatalf("expected %q in reshard runner network policy in k8s/networkpolicy.yaml", want)
		}
	}
	if strings.Contains(reshardDoc, "ingress:") {
		t.Fatal("reshard runner policy must define no ingress rules (deny-all ingress)")
	}
}

func TestManifestTestsRunInUnitRecipe(t *testing.T) {
	content := readManifest(t, "justfile")
	if !strings.Contains(content, "./tests/manifests/...") {
		t.Fatal("expected just test-unit to include ./tests/manifests/...")
	}
}

func TestCurrentDuckLakeReleasePins(t *testing.T) {
	const (
		tag     = "v1.0-posthog.6"
		version = "49ec0dc8"
	)

	tests := []struct {
		path []string
		want []string
	}{
		{path: []string{"Dockerfile"}, want: []string{"ARG DUCKLAKE_EXTENSION_TAG=" + tag}},
		{path: []string{"Dockerfile.worker"}, want: []string{"ARG DUCKLAKE_EXTENSION_TAG=" + tag}},
		{path: []string{".github", "workflows", "scenario-dev.yml"}, want: []string{"DUCKLAKE_EXTENSION_TAG=" + tag}},
		{path: []string{".github", "workflows", "e2e-mw-dev.yml"}, want: []string{"DUCKLAKE_EXTENSION_TAG=" + tag}},
		{
			path: []string{"tests", "mw-dev", "e2e", "harness.sh"},
			want: []string{"DUCKLAKE_EXTENSION_TAG=" + tag, `EXPECT_DUCKLAKE_SHA="` + version + `"`},
		},
	}

	for _, tt := range tests {
		content := readManifest(t, tt.path...)
		for _, want := range tt.want {
			if !strings.Contains(content, want) {
				t.Errorf("expected %q in %s", want, filepath.Join(tt.path...))
			}
		}
	}

	var workflow struct {
		Jobs map[string]struct {
			Strategy struct {
				Matrix struct {
					DuckDB []struct {
						DuckLake string `yaml:"ducklake"`
						Default  bool   `yaml:"default"`
					} `yaml:"duckdb"`
				} `yaml:"matrix"`
			} `yaml:"strategy"`
		} `yaml:"jobs"`
	}
	workerWorkflow := readManifest(t, ".github", "workflows", "container-image-worker-cd.yml")
	if err := yaml.Unmarshal([]byte(workerWorkflow), &workflow); err != nil {
		t.Fatalf("parse worker image workflow: %v", err)
	}
	buildJob, ok := workflow.Jobs["build"]
	if !ok {
		t.Fatal("worker image workflow has no build job")
	}
	var defaults []string
	for _, row := range buildJob.Strategy.Matrix.DuckDB {
		if row.Default {
			defaults = append(defaults, row.DuckLake)
		}
	}
	if len(defaults) != 1 {
		t.Fatalf("expected exactly one default DuckDB worker matrix row, got %d", len(defaults))
	}
	if defaults[0] != tag {
		t.Errorf("default DuckDB worker matrix row pins DuckLake %q, want %q", defaults[0], tag)
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
