package datasets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveRuntimeContractNoVersionConfigured(t *testing.T) {
	t.Setenv(DatasetVersionEnv, "")
	t.Setenv(ManifestPathEnv, "")

	contract, err := ResolveRuntimeContract("")
	if err != nil {
		t.Fatalf("ResolveRuntimeContract returned error: %v", err)
	}
	if contract.Frozen {
		t.Fatalf("expected non-frozen runtime contract: %+v", contract)
	}
}

func TestResolveRuntimeContractFailsForMissingRequestedVersion(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(manifestPath, []byte("versions:\n  - dataset_version: v1\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	t.Setenv(DatasetVersionEnv, "v2")
	t.Setenv(ManifestPathEnv, manifestPath)
	_, err := ResolveRuntimeContract("")
	if err == nil {
		t.Fatalf("expected missing requested version to fail")
	}
	if !strings.Contains(err.Error(), "not found in manifest") {
		t.Fatalf("expected manifest version error, got %v", err)
	}
}

func TestResolveRuntimeContractWithDefaultManifestPath(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(manifestPath, []byte("versions:\n  - dataset_version: v3\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	t.Setenv(DatasetVersionEnv, "v3")
	t.Setenv(ManifestPathEnv, "")
	contract, err := ResolveRuntimeContract(manifestPath)
	if err != nil {
		t.Fatalf("ResolveRuntimeContract returned error: %v", err)
	}
	if !contract.Frozen || contract.DatasetVersion != "v3" {
		t.Fatalf("unexpected runtime contract: %+v", contract)
	}
	if contract.ManifestPath != manifestPath {
		t.Fatalf("expected manifest path %q, got %q", manifestPath, contract.ManifestPath)
	}
}
