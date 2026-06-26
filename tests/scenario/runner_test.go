package scenario

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/scenario/core"
	"github.com/posthog/duckgres/tests/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/scenario/sql"
)

var (
	scenarioRun        = flag.Bool("scenario-run", false, "run a real scenario file")
	scenarioFile       = flag.String("scenario-file", "", "scenario YAML file to run")
	scenarioOutputBase = flag.String("scenario-output-base", "artifacts/scenario", "base directory for scenario artifacts")
	scenarioRunID      = flag.String("scenario-run-id", "", "scenario run id")
	scenarioMaxRuntime = flag.Duration("scenario-max-runtime", 30*time.Minute, "maximum scenario runtime")
)

func TestScenarioRunner(t *testing.T) {
	if !*scenarioRun {
		t.Skip("set -scenario-run to execute a real scenario")
	}
	if *scenarioFile == "" {
		t.Fatal("-scenario-file is required")
	}

	loaded, _, err := loadScenarioForRun(*scenarioFile)
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	runID := *scenarioRunID
	if runID == "" {
		runID = defaultRunID(loaded)
	}
	if missing := missingRequiredEnv(loaded); len(missing) != 0 {
		t.Fatalf("missing required scenario environment: %s", strings.Join(missing, ", "))
	}
	loaded, err = resolveRunTemplates(loaded, runID)
	if err != nil {
		t.Fatalf("resolve scenario templates: %v", err)
	}

	provisionClient, err := provision.NewClient(provision.Config{
		BaseURL:        mustEnv(t, "DUCKGRES_SCENARIO_API_BASE"),
		InternalSecret: mustEnv(t, "DUCKGRES_SCENARIO_INTERNAL_SECRET"),
	})
	if err != nil {
		t.Fatalf("create provision client: %v", err)
	}
	provisionState := provision.NewState()
	provisionExecutor := provision.NewExecutor(provision.ExecutorConfig{
		Client: provisionClient,
		State:  provisionState,
		WaitOptions: provision.WaitOptions{
			PollInterval: 10 * time.Second,
			Timeout:      15 * time.Minute,
		},
	})

	sqlExecutor := scenariosql.NewExecutor(scenariosql.ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:        mustEnv(t, "DUCKGRES_SCENARIO_PG_HOST"),
			SNISuffix:       mustEnv(t, "DUCKGRES_SCENARIO_SNI_SUFFIX"),
			Port:            intEnv(t, "DUCKGRES_SCENARIO_PG_PORT", 5432),
			SSLMode:         "require",
			ConnectTimeout:  intEnv(t, "DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT", 10),
			ApplicationName: "duckgres-scenario-runner",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), *scenarioMaxRuntime)
	defer cancel()
	runner := core.NewRunner(core.RunnerConfig{
		RunID:          runID,
		Scenario:       loaded,
		Executor:       dispatchExecutor{provision: provisionExecutor, sql: sqlExecutor},
		OutputDir:      filepath.Join(*scenarioOutputBase, runID),
		WriteFiles:     true,
		CleanupTimeout: 15 * time.Minute,
	})
	if summary, err := runner.Run(ctx); err != nil {
		t.Fatalf("scenario failed: %+v: %v", summary, err)
	}
}

func TestProvisionSmokeScenarioUsesRunUniqueSupportedSteps(t *testing.T) {
	scenario, err := core.LoadScenario(filepath.Join("scenarios", "provision_smoke.yaml"))
	if err != nil {
		t.Fatalf("load provision smoke: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-smoke-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
	}
	provisionStep := resolved.Steps[0]
	orgID, _ := provisionStep.With["org_id"].(string)
	if orgID == "scenario-smoke" || !strings.Contains(orgID, "20260102t030405z") {
		t.Fatalf("org_id = %q, want run-unique templated org", orgID)
	}
	request, ok := provisionStep.With["request"].(map[string]any)
	if !ok {
		t.Fatalf("provision request = %#v, want map", provisionStep.With["request"])
	}
	databaseName, _ := request["database_name"].(string)
	if databaseName == "scenario_smoke" || !strings.Contains(databaseName, "20260102t030405z") {
		t.Fatalf("database_name = %q, want run-unique templated database", databaseName)
	}
}

func TestFrozenMetadataScenarioRequiresDatasetURI(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "")

	scenario, err := core.LoadScenario(filepath.Join("scenarios", "posthog_frozen_metadata.yaml"))
	if err != nil {
		t.Fatalf("load frozen metadata scenario: %v", err)
	}
	missing := missingRequiredEnv(scenario)
	if len(missing) != 1 || missing[0] != "DUCKGRES_SCENARIO_FROZEN_S3_URI" {
		t.Fatalf("missing required env = %#v, want frozen S3 URI", missing)
	}
}

func TestFrozenMetadataScenarioResolvesEnvTemplates(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")

	scenario, err := core.LoadScenario(filepath.Join("scenarios", "posthog_frozen_metadata.yaml"))
	if err != nil {
		t.Fatalf("load frozen metadata scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-frozen-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
	}
}

func TestLoadScenarioForRunResolvesScenarioRelativeFiles(t *testing.T) {
	scenario, scenarioPath, err := loadScenarioForRun(filepath.Join("scenarios", "posthog_frozen_metadata.yaml"))
	if err != nil {
		t.Fatalf("loadScenarioForRun returned error: %v", err)
	}
	if !filepath.IsAbs(scenarioPath) {
		t.Fatalf("scenarioPath = %q, want absolute path", scenarioPath)
	}

	foundSetupFile := false
	foundCatalogFile := false
	for _, step := range scenario.Steps {
		file, ok := step.With["file"].(string)
		if !ok {
			continue
		}
		if !filepath.IsAbs(file) {
			t.Fatalf("step %s file path = %q, want absolute path", step.ID, file)
		}
		if _, err := os.Stat(file); err != nil {
			t.Fatalf("step %s file path %q should exist: %v", step.ID, file, err)
		}
		switch step.ID {
		case "setup_frozen_views":
			foundSetupFile = strings.HasSuffix(file, filepath.Join("sql", "setup_frozen_views.sql"))
		case "metadata_exploration":
			foundCatalogFile = strings.HasSuffix(file, filepath.Join("sql", "metadata_catalog.yaml"))
		}
	}
	if !foundSetupFile {
		t.Fatal("expected setup_frozen_views file to resolve under sql/")
	}
	if !foundCatalogFile {
		t.Fatal("expected metadata_exploration file to resolve under sql/")
	}
}

func TestResolveRunTemplatesRejectsMissingEnvTemplate(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "")

	_, err := resolveRunTemplates(core.Scenario{
		Name: "env-template",
		Steps: []core.Step{{
			ID:   "setup",
			Type: scenariosql.StepTypeSQL,
			With: map[string]any{
				"sql": "SELECT '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}'",
			},
		}},
	}, "scenario-env-20260102t030405z")
	if err == nil {
		t.Fatal("expected missing env template to fail")
	}
	if !strings.Contains(err.Error(), "DUCKGRES_SCENARIO_FROZEN_S3_URI") {
		t.Fatalf("error = %v, want env var name", err)
	}
}

type dispatchExecutor struct {
	provision *provision.Executor
	sql       *scenariosql.Executor
}

func (e dispatchExecutor) ExecuteStep(ctx context.Context, step core.Step) error {
	switch step.Type {
	case provision.StepTypeProvisionWarehouse, provision.StepTypeWaitWarehouseReady, provision.StepTypeDeprovisionWarehouse:
		return e.provision.ExecuteStep(ctx, step)
	case scenariosql.StepTypeSQL, scenariosql.StepTypeSQLCatalog:
		return e.sql.ExecuteStep(ctx, step)
	default:
		return fmt.Errorf("unsupported scenario step type %q", step.Type)
	}
}

func dispatchSupports(stepType string) bool {
	switch stepType {
	case provision.StepTypeProvisionWarehouse, provision.StepTypeWaitWarehouseReady, provision.StepTypeDeprovisionWarehouse:
		return true
	case scenariosql.StepTypeSQL, scenariosql.StepTypeSQLCatalog:
		return true
	default:
		return false
	}
}

func mustEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required", key)
	}
	return value
}

func intEnv(t *testing.T, key string, fallback int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("%s must be an integer: %v", key, err)
	}
	return parsed
}

func defaultRunID(s core.Scenario) string {
	prefix := s.RunIDPrefix
	if prefix == "" {
		prefix = "scenario"
	}
	return fmt.Sprintf("%s-%s", prefix, time.Now().UTC().Format("20060102t150405z"))
}

func loadScenarioForRun(path string) (core.Scenario, string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return core.Scenario{}, "", fmt.Errorf("resolve scenario path %s: %w", path, err)
	}
	scenario, err := core.LoadScenario(absPath)
	if err != nil {
		return core.Scenario{}, "", err
	}
	return resolveScenarioFilePaths(scenario, filepath.Dir(absPath)), absPath, nil
}

func resolveScenarioFilePaths(s core.Scenario, baseDir string) core.Scenario {
	out := s
	out.Steps = make([]core.Step, len(s.Steps))
	for i, step := range s.Steps {
		if step.With == nil {
			out.Steps[i] = step
			continue
		}
		with := make(map[string]any, len(step.With))
		for k, v := range step.With {
			if k == "file" {
				if file, ok := v.(string); ok && file != "" && !filepath.IsAbs(file) {
					v = filepath.Clean(filepath.Join(baseDir, file))
				}
			}
			with[k] = v
		}
		step.With = with
		out.Steps[i] = step
	}
	return out
}

func missingRequiredEnv(s core.Scenario) []string {
	var missing []string
	for _, key := range s.RequiredEnv {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	return missing
}

func resolveRunTemplates(s core.Scenario, runID string) (core.Scenario, error) {
	vars := map[string]string{
		"run_id":         runID,
		"run_id_compact": compactRunID(runID),
	}
	out := s
	out.Steps = make([]core.Step, len(s.Steps))
	for i, step := range s.Steps {
		if step.With != nil {
			resolved, err := resolveTemplateValue(step.With, vars)
			if err != nil {
				return core.Scenario{}, fmt.Errorf("step %s: %w", step.ID, err)
			}
			step.With = resolved.(map[string]any)
		}
		out.Steps[i] = step
	}
	return out, nil
}

func compactRunID(runID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(runID) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "scenario"
	}
	return b.String()
}

func resolveTemplateValue(value any, vars map[string]string) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			resolved, err := resolveTemplateValue(v, vars)
			if err != nil {
				return nil, err
			}
			out[k] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, v := range typed {
			resolved, err := resolveTemplateValue(v, vars)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	case string:
		out := typed
		for k, v := range vars {
			out = strings.ReplaceAll(out, "${"+k+"}", v)
		}
		out, err := core.ResolveEnvTemplates(out)
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return typed, nil
	}
}

func containsTemplate(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for _, v := range typed {
			if containsTemplate(v) {
				return true
			}
		}
	case []any:
		for _, v := range typed {
			if containsTemplate(v) {
				return true
			}
		}
	case string:
		return strings.Contains(typed, "${")
	}
	return false
}
