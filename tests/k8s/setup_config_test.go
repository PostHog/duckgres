package k8s_test

import "testing"

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
	if cfg.CleanupRecipe != "multitenant-config-store-down-kind" {
		t.Fatalf("expected kind cleanup recipe, got %q", cfg.CleanupRecipe)
	}
	if cfg.ExpectedMetadataStoreHost != "duckgres-local-ducklake-metadata" {
		t.Fatalf("expected kind metadata host, got %q", cfg.ExpectedMetadataStoreHost)
	}
	if cfg.ExpectedMetadataStorePort != 5432 {
		t.Fatalf("expected kind metadata port 5432, got %d", cfg.ExpectedMetadataStorePort)
	}
	if cfg.ExpectedS3Endpoint != "duckgres-local-minio:9000" {
		t.Fatalf("expected kind s3 endpoint, got %q", cfg.ExpectedS3Endpoint)
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
	if cfg.CleanupRecipe != "multitenant-config-store-down" {
		t.Fatalf("expected local cleanup recipe, got %q", cfg.CleanupRecipe)
	}
	if cfg.ExpectedMetadataStoreHost != "host.docker.internal" {
		t.Fatalf("expected local metadata host, got %q", cfg.ExpectedMetadataStoreHost)
	}
	if cfg.ExpectedMetadataStorePort != 5436 {
		t.Fatalf("expected local metadata port 5436, got %d", cfg.ExpectedMetadataStorePort)
	}
	if cfg.ExpectedS3Endpoint != "host.docker.internal:39000" {
		t.Fatalf("expected local s3 endpoint, got %q", cfg.ExpectedS3Endpoint)
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
