package k8s_test

import "fmt"

type setupMode string

const (
	setupModeLocal setupMode = "local"
	setupModeKind  setupMode = "kind"
)

type k8sTestEnvironment struct {
	Namespace        string
	SetupMode        setupMode
	SetupRecipe      string
	CleanupRecipe    string
	SeedSQLPath      string
	ControlPlanePath string
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
		cfg.CleanupRecipe = "cleanup-multitenant-local"
		cfg.SeedSQLPath = "k8s/local-config-store.seed.sql"
		cfg.ControlPlanePath = "k8s/control-plane-multitenant-local.yaml"
	case setupModeKind:
		cfg.SetupRecipe = "run-multitenant-kind"
		cfg.CleanupRecipe = "cleanup-multitenant-kind"
		cfg.SeedSQLPath = "k8s/kind/config-store.seed.sql"
		cfg.ControlPlanePath = "k8s/kind/control-plane.yaml"
	default:
		return k8sTestEnvironment{}, fmt.Errorf("unknown DUCKGRES_K8S_TEST_SETUP %q", mode)
	}

	return cfg, nil
}
