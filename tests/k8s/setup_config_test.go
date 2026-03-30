package k8s_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadK8sTestEnvironmentDefaultsToKind(t *testing.T) {
	cfg, err := loadK8sTestEnvironment(func(string) string { return "" })
	if err != nil {
		t.Fatalf("loadK8sTestEnvironment: %v", err)
	}

	if cfg.SetupMode != setupModeKind {
		t.Fatalf("expected kind setup mode, got %q", cfg.SetupMode)
	}
	if cfg.SetupRecipe != "run-multitenant-kind" {
		t.Fatalf("expected kind setup recipe, got %q", cfg.SetupRecipe)
	}
	if cfg.CleanupRecipe != "cleanup-multitenant-kind" {
		t.Fatalf("expected kind cleanup recipe, got %q", cfg.CleanupRecipe)
	}
	if cfg.SeedSQLPath != "k8s/kind/config-store.seed.sql" {
		t.Fatalf("expected kind seed sql path, got %q", cfg.SeedSQLPath)
	}
	if cfg.ControlPlanePath != "k8s/kind/control-plane.yaml" {
		t.Fatalf("expected kind control-plane path, got %q", cfg.ControlPlanePath)
	}
	if cfg.Namespace != "duckgres" {
		t.Fatalf("expected default namespace duckgres, got %q", cfg.Namespace)
	}
}

func TestLoadK8sTestEnvironmentSupportsLocal(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_K8S_TEST_SETUP":     "local",
		"DUCKGRES_K8S_TEST_NAMESPACE": "duckgres",
	}
	cfg, err := loadK8sTestEnvironment(func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("loadK8sTestEnvironment: %v", err)
	}

	if cfg.SetupMode != setupModeLocal {
		t.Fatalf("expected local setup mode, got %q", cfg.SetupMode)
	}
	if cfg.SetupRecipe != "run-multitenant-local" {
		t.Fatalf("expected local setup recipe, got %q", cfg.SetupRecipe)
	}
	if cfg.CleanupRecipe != "cleanup-multitenant-local" {
		t.Fatalf("expected local cleanup recipe, got %q", cfg.CleanupRecipe)
	}
	if cfg.SeedSQLPath != "k8s/local-config-store.seed.sql" {
		t.Fatalf("expected local seed sql path, got %q", cfg.SeedSQLPath)
	}
	if cfg.ControlPlanePath != "k8s/control-plane-multitenant-local.yaml" {
		t.Fatalf("expected local control-plane path, got %q", cfg.ControlPlanePath)
	}
}

func TestLoadK8sTestEnvironmentRejectsUnknownSetupMode(t *testing.T) {
	_, err := loadK8sTestEnvironment(func(key string) string {
		if key == "DUCKGRES_K8S_TEST_SETUP" {
			return "bogus"
		}
		return ""
	})
	if err == nil {
		t.Fatal("expected error for unknown setup mode")
	}
}

func TestKindSetupArtifactsEnableSharedWarmTarget(t *testing.T) {
	root := findProjectRootForUnitTest(t)

	manifestPath := filepath.Join(root, "k8s", "kind", "control-plane.yaml")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read kind control-plane manifest: %v", err)
	}

	content := string(manifest)
	for _, want := range []string{
		"postgres://duckgres:duckgres@duckgres-config-store:5432/duckgres_config?sslmode=disable",
		"--k8s-shared-warm-target",
		"1",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in %s", want, manifestPath)
		}
	}

	seedPath := filepath.Join(root, "k8s", "kind", "config-store.seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("read kind seed sql: %v", err)
	}

	for _, want := range []string{
		"'duckgres-local-ducklake-metadata'",
		"'duckgres-local-minio:9000'",
		"'duckgres-local-metadata'",
		"'duckgres-local-s3'",
	} {
		if !strings.Contains(string(seedSQL), want) {
			t.Fatalf("expected %q in %s", want, seedPath)
		}
	}
}

func TestLocalDependencyPortsStayFixedAndPreflighted(t *testing.T) {
	root := findProjectRootForUnitTest(t)

	composePath := filepath.Join(root, "k8s", "local-config-store.compose.yaml")
	compose, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read local compose file: %v", err)
	}

	for _, want := range []string{
		`"5434:5432"`,
	} {
		if !strings.Contains(string(compose), want) {
			t.Fatalf("expected fixed port mapping %q in %s", want, composePath)
		}
	}

	orbstackOverlayPath := filepath.Join(root, "k8s", "orbstack", "dependency-ports.overlay.yaml")
	orbstackOverlay, err := os.ReadFile(orbstackOverlayPath)
	if err != nil {
		t.Fatalf("read OrbStack overlay file: %v", err)
	}

	for _, want := range []string{
		`"35434:5432"`,
		`"35433:5432"`,
		`"39000:9000"`,
		`"39001:9001"`,
	} {
		if !strings.Contains(string(orbstackOverlay), want) {
			t.Fatalf("expected fixed port mapping %q in %s", want, orbstackOverlayPath)
		}
	}

	justfilePath := filepath.Join(root, "justfile")
	justfile, err := os.ReadFile(justfilePath)
	if err != nil {
		t.Fatalf("read justfile: %v", err)
	}

	for _, want := range []string{
		"check-multitenant-local-ports:",
		"multitenant-config-store-up: check-multitenant-local-ports",
		"check-multitenant-kind-ports:",
		"multitenant-config-store-up-kind: check-multitenant-kind-ports",
		"Required local dev port",
	} {
		if !strings.Contains(string(justfile), want) {
			t.Fatalf("expected %q in %s", want, justfilePath)
		}
	}
}

func TestControlPlaneRBACIncludesLeaseAccess(t *testing.T) {
	root := findProjectRootForUnitTest(t)

	rbacPath := filepath.Join(root, "k8s", "rbac.yaml")
	manifest, err := os.ReadFile(rbacPath)
	if err != nil {
		t.Fatalf("read rbac manifest: %v", err)
	}

	content := string(manifest)
	for _, want := range []string{
		`apiGroups: ["coordination.k8s.io"]`,
		`resources: ["leases"]`,
		`verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in %s", want, rbacPath)
		}
	}
}

func findProjectRootForUnitTest(t *testing.T) string {
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
