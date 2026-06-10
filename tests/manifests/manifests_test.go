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
