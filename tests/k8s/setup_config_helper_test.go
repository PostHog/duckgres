package k8s_test

import "fmt"

type setupMode string

const (
	setupModeLocal setupMode = "local"
	setupModeKind  setupMode = "kind"
)

type k8sTestEnvironment struct {
	Namespace                 string
	SetupMode                 setupMode
	SetupRecipe               string
	CleanupRecipe             string
	ExpectedMetadataStoreHost string
	ExpectedMetadataStorePort int
	ExpectedS3Endpoint        string
}

func loadK8sTestEnvironment(getenv func(string) string) (k8sTestEnvironment, error) {
	mode := setupMode(getenv("DUCKGRES_K8S_TEST_SETUP"))
	if mode == "" {
		mode = setupModeKind
	}

	cfg := k8sTestEnvironment{
		Namespace: getenv("DUCKGRES_K8S_TEST_NAMESPACE"),
		SetupMode: mode,
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "duckgres"
	}

	switch mode {
	case setupModeLocal:
		cfg.SetupRecipe = "run-multitenant-local"
		cfg.CleanupRecipe = "multitenant-config-store-down"
		cfg.ExpectedMetadataStoreHost = "host.docker.internal"
		cfg.ExpectedMetadataStorePort = 5436
		cfg.ExpectedS3Endpoint = "host.docker.internal:39000"
	case setupModeKind:
		cfg.SetupRecipe = "run-multitenant-kind"
		cfg.CleanupRecipe = "multitenant-config-store-down-kind"
		cfg.ExpectedMetadataStoreHost = "duckgres-local-ducklake-metadata"
		cfg.ExpectedMetadataStorePort = 5432
		cfg.ExpectedS3Endpoint = "duckgres-local-minio:9000"
	default:
		return k8sTestEnvironment{}, fmt.Errorf("unknown DUCKGRES_K8S_TEST_SETUP %q", mode)
	}

	return cfg, nil
}
