package scenario

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
	scenariodbt "github.com/posthog/duckgres/tests/mw-dev/scenario/dbt"
	scenarioperf "github.com/posthog/duckgres/tests/mw-dev/scenario/perf"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/mw-dev/scenario/sql"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
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
			DialHost:        mustEnv(t, "DUCKGRES_SCENARIO_PG_HOST"),
			SNISuffix:       mustEnv(t, "DUCKGRES_SCENARIO_SNI_SUFFIX"),
			Port:            intEnv(t, "DUCKGRES_SCENARIO_PG_PORT", 5432),
			SSLMode:         "require",
			ConnectTimeout:  intEnv(t, "DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT", 10),
			ApplicationName: "duckgres-scenario-runner",
		},
	})
	scenarioOutputDir := filepath.Join(*scenarioOutputBase, runID)
	perfExecutor := scenarioperf.NewExecutor(scenarioperf.ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			DialHost:        mustEnv(t, "DUCKGRES_SCENARIO_PG_HOST"),
			SNISuffix:       mustEnv(t, "DUCKGRES_SCENARIO_SNI_SUFFIX"),
			Port:            intEnv(t, "DUCKGRES_SCENARIO_PG_PORT", 5432),
			SSLMode:         "require",
			ConnectTimeout:  intEnv(t, "DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT", 10),
			ApplicationName: "duckgres-scenario-runner",
		},
		OutputDir:                scenarioOutputDir,
		FlightAddr:               os.Getenv("DUCKGRES_SCENARIO_FLIGHT_ADDR"),
		FlightInsecureSkipVerify: boolEnv(t, "DUCKGRES_SCENARIO_FLIGHT_INSECURE_SKIP_VERIFY", true),
	})
	dbtExecutor := scenariodbt.NewExecutor(scenariodbt.ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			DialHost:        mustEnv(t, "DUCKGRES_SCENARIO_PG_HOST"),
			SNISuffix:       mustEnv(t, "DUCKGRES_SCENARIO_SNI_SUFFIX"),
			Port:            intEnv(t, "DUCKGRES_SCENARIO_PG_PORT", 5432),
			SSLMode:         "require",
			ConnectTimeout:  intEnv(t, "DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT", 10),
			ApplicationName: "duckgres-scenario-runner",
		},
		OutputDir: scenarioOutputDir,
		DBTBinary: envOrDefault("DUCKGRES_SCENARIO_DBT_BIN", "dbt"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), *scenarioMaxRuntime)
	defer cancel()
	runner := core.NewRunner(core.RunnerConfig{
		RunID:          runID,
		Scenario:       loaded,
		Executor:       dispatchExecutor{provision: provisionExecutor, sql: sqlExecutor, perf: perfExecutor, dbt: dbtExecutor},
		OutputDir:      scenarioOutputDir,
		WriteFiles:     true,
		CleanupTimeout: 15 * time.Minute,
	})
	if summary, err := runner.Run(ctx); err != nil {
		t.Fatalf("scenario failed: %+v: %v", summary, err)
	}
}

func TestProvisionSmokeScenarioUsesIsolatedStackWarehouseIdentityAndSupportedSteps(t *testing.T) {
	const scenarioOrgID = "ci-pr-123-cnpg"
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", scenarioOrgID)

	scenario, err := core.LoadScenario(filepath.Join("scenarios", "provision_smoke.yaml"))
	if err != nil {
		t.Fatalf("load provision smoke: %v", err)
	}
	requireScenarioEnv(t, scenario, "DUCKGRES_SCENARIO_ORG_ID")
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
		orgID, ok := step.With["org_id"].(string)
		if !ok {
			t.Fatalf("step %s is missing org_id", step.ID)
		}
		if orgID != scenarioOrgID {
			t.Fatalf("step %s org_id = %q, want isolated-stack identity %q", step.ID, orgID, scenarioOrgID)
		}
	}
	provisionStep := resolved.Steps[0]
	request, ok := provisionStep.With["request"].(map[string]any)
	if !ok {
		t.Fatalf("provision request = %#v, want map", provisionStep.With["request"])
	}
	databaseName, _ := request["database_name"].(string)
	if databaseName == "scenario_smoke" || !strings.Contains(databaseName, "scenario_smoke_") {
		t.Fatalf("database_name = %q, want run-unique templated database", databaseName)
	}
	requireScenarioDefaultTeamID(t, request)
}

func TestFrozenSuccessScenariosUseIsolatedStackWarehouseIdentity(t *testing.T) {
	const scenarioOrgID = "ci-pr-123-cnpg"

	for _, scenarioFile := range []string{
		"posthog_frozen_metadata.yaml",
		"posthog_frozen_perf.yaml",
		"events_rowgroup_perf.yaml",
		"posthog_frozen_dbt.yaml",
		"fast-suite.yaml",
		"full-suite.yaml",
	} {
		t.Run(scenarioFile, func(t *testing.T) {
			t.Setenv("DUCKGRES_SCENARIO_ORG_ID", scenarioOrgID)
			scenario, err := core.LoadScenario(filepath.Join("scenarios", scenarioFile))
			if err != nil {
				t.Fatalf("load scenario: %v", err)
			}
			requireScenarioEnv(t, scenario, "DUCKGRES_SCENARIO_ORG_ID")
			runID := scenario.RunIDPrefix + "-20260701t135927z"
			resolved, err := resolveRunTemplates(scenario, runID)
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
				orgID, hasOrgID := step.With["org_id"].(string)
				if !hasOrgID {
					t.Fatalf("step %s is missing org_id", step.ID)
				}
				if orgID != scenarioOrgID {
					t.Fatalf("step %s org_id = %q, want isolated-stack identity %q", step.ID, orgID, scenarioOrgID)
				}
				if step.Type == provision.StepTypeProvisionWarehouse {
					request, ok := step.With["request"].(map[string]any)
					if !ok {
						t.Fatalf("provision request = %#v, want map", step.With["request"])
					}
					requireScenarioDefaultTeamID(t, request)
				}
			}
		})
	}
}

func TestFastSuiteScenarioComposesWorkloadsWithoutDBT(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

	scenario, _, err := loadScenarioForRun(filepath.Join("scenarios", "fast-suite.yaml"))
	if err != nil {
		t.Fatalf("load fast suite scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-fast-suite-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}

	steps := make(map[string]core.Step, len(resolved.Steps))
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
		if step.Type == scenariodbt.StepTypeDBTRun {
			t.Fatalf("fast suite contains dbt step %s", step.ID)
		}
		steps[step.ID] = step
	}

	if _, ok := steps["dbt_models"]; ok {
		t.Fatal("fast suite should not contain dbt_models")
	}
	for _, stepID := range []string{"setup_frozen_views", "metadata_exploration", "perf_queries"} {
		step, ok := steps[stepID]
		if !ok {
			t.Fatalf("missing %s step", stepID)
		}
		for _, key := range []string{"file", "catalog_file"} {
			path, ok := step.With[key].(string)
			if !ok {
				continue
			}
			if !filepath.IsAbs(path) {
				t.Fatalf("step %s %s = %q, want absolute path", step.ID, key, path)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("step %s %s %q should exist: %v", step.ID, key, path, err)
			}
		}
	}
	for _, stepID := range []string{"metadata_exploration", "perf_queries"} {
		if got := steps[stepID].DependsOn; len(got) != 1 || got[0] != "setup_frozen_views" {
			t.Fatalf("step %s dependencies = %#v, want [setup_frozen_views]", stepID, got)
		}
	}
	assertPerfQueryErrorsFailStep(t, steps["perf_queries"])
	assertPerfTargetsOnlyPGWire(t, steps["perf_queries"])
	deprovision := steps["deprovision"]
	if !deprovision.AlwaysRun {
		t.Fatal("deprovision should always run")
	}
	if got := deprovision.DependsOn; len(got) != 2 || got[0] != "metadata_exploration" || got[1] != "perf_queries" {
		t.Fatalf("deprovision dependencies = %#v, want metadata and perf branches", got)
	}
}

func TestFullSuiteScenarioComposesWorkloadsAsDAG(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

	scenario, _, err := loadScenarioForRun(filepath.Join("scenarios", "full-suite.yaml"))
	if err != nil {
		t.Fatalf("load full suite scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-full-suite-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}

	steps := make(map[string]core.Step, len(resolved.Steps))
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
		steps[step.ID] = step
	}

	for _, stepID := range []string{"setup_frozen_views", "metadata_exploration", "perf_queries", "dbt_models"} {
		step, ok := steps[stepID]
		if !ok {
			t.Fatalf("missing %s step", stepID)
		}
		for _, key := range []string{"file", "catalog_file", "project_dir"} {
			path, ok := step.With[key].(string)
			if !ok {
				continue
			}
			if !filepath.IsAbs(path) {
				t.Fatalf("step %s %s = %q, want absolute path", step.ID, key, path)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("step %s %s %q should exist: %v", step.ID, key, path, err)
			}
		}
	}

	for _, stepID := range []string{"metadata_exploration", "perf_queries", "dbt_models"} {
		if got := steps[stepID].DependsOn; len(got) != 1 || got[0] != "setup_frozen_views" {
			t.Fatalf("step %s dependencies = %#v, want [setup_frozen_views]", stepID, got)
		}
	}
	assertPerfQueryErrorsFailStep(t, steps["perf_queries"])
	assertPerfTargetsOnlyPGWire(t, steps["perf_queries"])
	assertDBTWorkerSize(t, steps["dbt_models"])
	deprovision := steps["deprovision"]
	if !deprovision.AlwaysRun {
		t.Fatal("deprovision should always run")
	}
	if got := deprovision.DependsOn; len(got) != 3 || got[0] != "metadata_exploration" || got[1] != "perf_queries" || got[2] != "dbt_models" {
		t.Fatalf("deprovision dependencies = %#v, want all frozen workload branches", got)
	}
}

func TestProvisionRejectionScenarioUsesExpectedProvisionFailure(t *testing.T) {
	scenario, err := core.LoadScenario(filepath.Join("scenarios", "provision_rejection.yaml"))
	if err != nil {
		t.Fatalf("load provision rejection: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-smoke-manual-20260102t030405z")
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
	if len(orgID) <= 35 {
		t.Fatalf("org_id = %q, want deliberately too-long slug", orgID)
	}
	expected := provisionStep.ExpectError
	if expected == nil {
		t.Fatal("provision step should assert the expected rejection")
	}
	if expected.ErrorClass != provision.ErrorClassProvisionStepError {
		t.Fatalf("error_class = %q, want %s", expected.ErrorClass, provision.ErrorClassProvisionStepError)
	}
	if got := strings.Join(expected.Contains, "\n"); !strings.Contains(got, "HTTP 400") || !strings.Contains(got, "slug of at most 35 characters") {
		t.Fatalf("expected error contains = %#v, want HTTP 400 and slug limit", expected.Contains)
	}
	request, ok := provisionStep.With["request"].(map[string]any)
	if !ok {
		t.Fatalf("provision request = %#v, want map", provisionStep.With["request"])
	}
	requireScenarioDefaultTeamID(t, request)
}

func TestFrozenMetadataScenarioRequiresDatasetURI(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

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
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

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

func TestFrozenPerfScenarioUsesSupportedStepsAndRelativeCatalog(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

	scenario, _, err := loadScenarioForRun(filepath.Join("scenarios", "posthog_frozen_perf.yaml"))
	if err != nil {
		t.Fatalf("load frozen perf scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-frozen-perf-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}

	foundPerf := false
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
		if step.Type != scenarioperf.StepTypePerfQueries {
			continue
		}
		foundPerf = true
		catalogFile, ok := step.With["catalog_file"].(string)
		if !ok || !filepath.IsAbs(catalogFile) {
			t.Fatalf("perf catalog_file = %#v, want absolute path", step.With["catalog_file"])
		}
		if _, err := os.Stat(catalogFile); err != nil {
			t.Fatalf("perf catalog file %q should exist: %v", catalogFile, err)
		}
		if runID, _ := step.With["run_id"].(string); runID != "scenario-frozen-perf-20260102t030405z" {
			t.Fatalf("perf run_id = %q, want scenario run id", runID)
		}
		if _, ok := step.With["flight_addr"]; ok {
			t.Fatal("frozen perf scenario should not configure the deprecated Flight endpoint")
		}
		assertPerfQueryErrorsFailStep(t, step)
		assertPerfTargetsOnlyPGWire(t, step)
		if _, ok := step.With["flight_insecure_skip_verify"]; ok {
			t.Fatal("perf scenario should use DUCKGRES_SCENARIO_FLIGHT_INSECURE_SKIP_VERIFY default instead of hardcoding TLS behavior")
		}
	}
	if !foundPerf {
		t.Fatal("expected frozen perf scenario to include a perf_queries step")
	}
}

func TestEventsRowGroupPerfScenarioBuildsControlledAB(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

	scenario, _, err := loadScenarioForRun(filepath.Join("scenarios", "events_rowgroup_perf.yaml"))
	if err != nil {
		t.Fatalf("load events row-group perf scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-events-rowgroup-perf-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}

	steps := make(map[string]core.Step, len(resolved.Steps))
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
		steps[step.ID] = step
	}

	rewrite, ok := steps["rewrite_events_rowgroups"]
	if !ok {
		t.Fatal("missing rewrite_events_rowgroups step")
	}
	assertScenarioFileExists(t, rewrite, "file")
	if got, _ := rewrite.With["worker_cpu"].(string); got != "4" {
		t.Fatalf("rewrite worker_cpu = %q, want 4", got)
	}
	if got, _ := rewrite.With["worker_memory"].(string); got != "16Gi" {
		t.Fatalf("rewrite worker_memory = %q, want 16Gi", got)
	}
	rewriteSQL := readScenarioStepFile(t, rewrite, "file")
	for _, required := range []string{
		"SET threads = 1;",
		"SET preserve_insertion_order = false;",
		"events/*year=2024__month=10__day=01__*.parquet",
		"LIMIT 524288",
		"'parquet_row_group_size_bytes', '128MiB'",
		"'parquet_row_group_size_bytes', '1GiB'",
		"'target_file_size', '10GB'",
		"INSERT INTO rowgroup_ab.events_rg_1gib\nSELECT * FROM rowgroup_ab.events_rg_128mib",
		"RESET threads;",
		"RESET preserve_insertion_order;",
	} {
		if !strings.Contains(rewriteSQL, required) {
			t.Fatalf("row-group rewrite SQL missing %q", required)
		}
	}
	if got := strings.Count(rewriteSQL, "'parquet_row_group_size', 250000"); got != 2 {
		t.Fatalf("row-group rewrite SQL sets the shared 250000-row cap %d times, want 2", got)
	}

	perfStep, ok := steps["compare_rowgroup_queries"]
	if !ok {
		t.Fatal("missing compare_rowgroup_queries step")
	}
	assertScenarioFileExists(t, perfStep, "catalog_file")
	if got, _ := perfStep.With["worker_cpu"].(string); got != "4" {
		t.Fatalf("perf worker_cpu = %q, want 4", got)
	}
	if got, _ := perfStep.With["worker_memory"].(string); got != "16Gi" {
		t.Fatalf("perf worker_memory = %q, want 16Gi", got)
	}
	if disabled, _ := perfStep.With["disable_external_file_cache"].(bool); !disabled {
		t.Fatal("row-group perf must disable the external file cache before warmup")
	}
	if got := perfStep.DependsOn; len(got) != 1 || got[0] != "validate_rowgroup_rewrite" {
		t.Fatalf("perf dependencies = %#v, want validated rewrite first", got)
	}
	catalogPath, _ := perfStep.With["catalog_file"].(string)
	catalog, err := perfcore.LoadCatalog(catalogPath)
	if err != nil {
		t.Fatalf("load row-group perf catalog: %v", err)
	}
	if len(catalog.Targets) != 1 || catalog.Targets[0] != perfcore.ProtocolPGWire {
		t.Fatalf("row-group perf targets = %#v, want PGWire only", catalog.Targets)
	}
	if !catalog.AlternateQueryOrder || catalog.MeasureIterations%2 != 0 {
		t.Fatalf("row-group perf order must alternate across an even measurement count: %+v", catalog)
	}
	intentCounts := make(map[string]int)
	for _, query := range catalog.Queries {
		intentCounts[query.IntentID]++
		uses128MiB := strings.Contains(query.PGWireSQL, "rowgroup_ab.events_rg_128mib")
		uses1GiB := strings.Contains(query.PGWireSQL, "rowgroup_ab.events_rg_1gib")
		if uses128MiB == uses1GiB {
			t.Fatalf("query %s should select exactly one A/B table: %s", query.QueryID, query.PGWireSQL)
		}
	}
	if len(intentCounts) < 4 {
		t.Fatalf("row-group perf intents = %#v, want at least four workloads", intentCounts)
	}
	for intent, count := range intentCounts {
		if count != 2 {
			t.Fatalf("intent %s has %d queries, want one 128MiB/1GiB pair", intent, count)
		}
	}

	validate, ok := steps["validate_rowgroup_rewrite"]
	if !ok {
		t.Fatal("missing validate_rowgroup_rewrite step")
	}
	assertScenarioFileExists(t, validate, "file")
	validationSQL := readScenarioStepFile(t, validate, "file")
	for _, required := range []string{
		"SET VARIABLE events_rg_128mib_files",
		"parquet_file_metadata(getvariable('events_rg_128mib_files'))",
		"row_groups_1gib < row_groups_128mib",
		"('rowgroup_ab.events_rg_128mib', 'parquet_row_group_size_bytes', '134217728')",
		"('rowgroup_ab.events_rg_1gib', 'parquet_row_group_size_bytes', '1073741824')",
		"bit_xor(hash(",
	} {
		if !strings.Contains(validationSQL, required) {
			t.Fatalf("row-group validation SQL missing %q", required)
		}
	}
	if got := validate.DependsOn; len(got) != 1 || got[0] != "rewrite_events_rowgroups" {
		t.Fatalf("validation dependencies = %#v, want rewrite preflight", got)
	}
	deprovision := steps["deprovision"]
	if !deprovision.AlwaysRun {
		t.Fatal("deprovision should always run")
	}
	if got := deprovision.DependsOn; len(got) != 1 || got[0] != "compare_rowgroup_queries" {
		t.Fatalf("deprovision dependencies = %#v, want benchmark completion", got)
	}
}

func assertScenarioFileExists(t *testing.T, step core.Step, key string) {
	t.Helper()
	path, ok := step.With[key].(string)
	if !ok || !filepath.IsAbs(path) {
		t.Fatalf("step %s %s = %#v, want absolute path", step.ID, key, step.With[key])
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("step %s %s %q should exist: %v", step.ID, key, path, err)
	}
}

func readScenarioStepFile(t *testing.T, step core.Step, key string) string {
	t.Helper()
	path, _ := step.With[key].(string)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read step %s %s %q: %v", step.ID, key, path, err)
	}
	return string(raw)
}

func TestFrozenDBTScenarioUsesSupportedStepsAndRelativeProject(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	t.Setenv("DUCKGRES_SCENARIO_ORG_ID", "ci-pr-123-cnpg")

	scenario, _, err := loadScenarioForRun(filepath.Join("scenarios", "posthog_frozen_dbt.yaml"))
	if err != nil {
		t.Fatalf("load frozen dbt scenario: %v", err)
	}
	resolved, err := resolveRunTemplates(scenario, "scenario-frozen-dbt-20260102t030405z")
	if err != nil {
		t.Fatalf("resolve templates: %v", err)
	}

	foundDBT := false
	for _, step := range resolved.Steps {
		if !dispatchSupports(step.Type) {
			t.Fatalf("step %s has unsupported type %q", step.ID, step.Type)
		}
		if containsTemplate(step.With) {
			t.Fatalf("step %s still contains unresolved template values: %#v", step.ID, step.With)
		}
		if step.Type != scenariodbt.StepTypeDBTRun {
			continue
		}
		foundDBT = true
		projectDir, ok := step.With["project_dir"].(string)
		if !ok || !filepath.IsAbs(projectDir) {
			t.Fatalf("dbt project_dir = %#v, want absolute path", step.With["project_dir"])
		}
		if _, err := os.Stat(filepath.Join(projectDir, "dbt_project.yml")); err != nil {
			t.Fatalf("dbt project should exist at %q: %v", projectDir, err)
		}
		retry, ok := step.With["retry"].(map[string]any)
		if !ok {
			t.Fatal("frozen dbt scenario should enable dbt retry metadata")
		}
		if enabled, _ := retry["enabled"].(bool); !enabled {
			t.Fatalf("frozen dbt retry enabled = %#v, want true", retry["enabled"])
		}
		if maxAttempts := fmt.Sprint(retry["max_attempts"]); maxAttempts != "2" {
			t.Fatalf("frozen dbt retry max_attempts = %#v, want 2", retry["max_attempts"])
		}
		assertDBTWorkerSize(t, step)
	}
	if !foundDBT {
		t.Fatal("expected frozen dbt scenario to include a dbt_run step")
	}
}

func assertDBTWorkerSize(t *testing.T, step core.Step) {
	t.Helper()
	if got, _ := step.With["worker_cpu"].(string); got != "2" {
		t.Fatalf("dbt worker_cpu = %q, want 2", got)
	}
	if got, _ := step.With["worker_memory"].(string); got != "4Gi" {
		t.Fatalf("dbt worker_memory = %q, want 4Gi", got)
	}
	if got := fmt.Sprint(step.With["connect_timeout"]); got != "360" {
		t.Fatalf("dbt connect_timeout = %q, want 360 seconds", got)
	}
}

func assertPerfQueryErrorsFailStep(t *testing.T, step core.Step) {
	t.Helper()
	if failOnQueryErrors, _ := step.With["fail_on_query_errors"].(bool); !failOnQueryErrors {
		t.Fatalf("perf step %s should record query errors as a failed DAG step", step.ID)
	}
}

func assertPerfTargetsOnlyPGWire(t *testing.T, step core.Step) {
	t.Helper()
	targets, ok := step.With["targets"].([]any)
	if !ok || len(targets) != 1 || targets[0] != "pgwire" {
		t.Fatalf("perf step %s targets = %#v, want [pgwire]", step.ID, step.With["targets"])
	}
}

func TestFrozenDBTProjectModelsRealisticProductAnalyticsWorkload(t *testing.T) {
	projectDir := filepath.Join("dbt", "posthog_frozen_project")
	requiredModels := map[string][]string{
		"models/staging/stg_events.sql": {
			"event_timestamp",
			"person_id",
			"event_category",
		},
		"models/staging/stg_persons.sql": {
			"person_created_at",
			"person_day",
		},
		"models/intermediate/int_person_first_seen.sql": {
			"first_event_timestamp",
			"first_person_timestamp",
			"first_seen_timestamp",
		},
		"models/intermediate/int_sessionized_events.sql": {
			"lag(event_timestamp)",
			"session_number",
			"session_id",
		},
		"models/intermediate/int_event_days.sql": {
			"event_day",
			"events",
			"pageview_events",
		},
		"models/intermediate/int_person_activity_daily.sql": {
			"person_id",
			"event_day",
			"feature_events",
		},
		"models/intermediate/int_person_feature_usage_daily.sql": {
			"person_id",
			"feature_area",
			"events",
		},
		"models/facts/fct_sessions.sql": {
			"session_start",
			"session_duration_seconds",
			"pageview_events",
		},
		"models/facts/fct_user_activity_daily.sql": {
			"int_person_activity_daily",
			"active_persons",
			"pageview_events",
			"feature_events",
		},
		"models/facts/fct_activation_funnel.sql": {
			"saw_pageview",
			"used_feature",
			"activated",
		},
		"models/facts/fct_retention_daily.sql": {
			"cohort_day",
			"days_since_first_seen",
			"retained_persons",
		},
		"models/facts/fct_feature_usage_daily.sql": {
			"int_person_feature_usage_daily",
			"feature_area",
			"users",
			"events",
		},
		"models/marts/mart_product_kpis_daily.sql": {
			"daily_active_users",
			"activated_users",
			"retained_users",
		},
	}

	for modelPath, requiredSnippets := range requiredModels {
		body, err := os.ReadFile(filepath.Join(projectDir, modelPath))
		if err != nil {
			t.Fatalf("expected realistic dbt model %s: %v", modelPath, err)
		}
		sql := strings.ToLower(string(body))
		for _, snippet := range requiredSnippets {
			if !strings.Contains(sql, strings.ToLower(snippet)) {
				t.Fatalf("model %s missing realistic analytics snippet %q", modelPath, snippet)
			}
		}
	}

	schema, err := os.ReadFile(filepath.Join(projectDir, "models", "schema.yml"))
	if err != nil {
		t.Fatalf("read dbt schema.yml: %v", err)
	}
	for _, modelName := range []string{
		"stg_events",
		"stg_persons",
		"int_person_first_seen",
		"int_sessionized_events",
		"int_event_days",
		"int_person_activity_daily",
		"int_person_feature_usage_daily",
		"fct_sessions",
		"fct_user_activity_daily",
		"fct_activation_funnel",
		"fct_retention_daily",
		"fct_feature_usage_daily",
		"mart_product_kpis_daily",
	} {
		if !strings.Contains(string(schema), "name: "+modelName) {
			t.Fatalf("schema.yml should document and test model %s", modelName)
		}
	}

	project, err := os.ReadFile(filepath.Join(projectDir, "dbt_project.yml"))
	if err != nil {
		t.Fatalf("read dbt_project.yml: %v", err)
	}
	projectYAML := string(project)
	if !strings.Contains(projectYAML, "staging:\n      +materialized: table") {
		t.Fatal("frozen dbt staging models should be materialized as tables so downstream facts do not repeatedly scan raw parquet")
	}
	if !strings.Contains(projectYAML, "intermediate:\n      +materialized: table") {
		t.Fatal("frozen dbt intermediate models should be materialized as tables to reuse reduced-grain aggregates")
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
	perf      *scenarioperf.Executor
	dbt       *scenariodbt.Executor
}

func (e dispatchExecutor) ExecuteStep(ctx context.Context, step core.Step) error {
	switch step.Type {
	case provision.StepTypeProvisionWarehouse, provision.StepTypeWaitWarehouseReady, provision.StepTypeDeprovisionWarehouse:
		return e.provision.ExecuteStep(ctx, step)
	case scenariosql.StepTypeSQL, scenariosql.StepTypeSQLCatalog:
		return e.sql.ExecuteStep(ctx, step)
	case scenarioperf.StepTypePerfQueries:
		return e.perf.ExecuteStep(ctx, step)
	case scenariodbt.StepTypeDBTRun:
		return e.dbt.ExecuteStep(ctx, step)
	default:
		return fmt.Errorf("unsupported scenario step type %q", step.Type)
	}
}

func (e dispatchExecutor) StepResultMetadata(stepID string) (core.StepResultMetadata, bool) {
	if e.dbt == nil {
		return core.StepResultMetadata{}, false
	}
	return e.dbt.StepResultMetadata(stepID)
}

func dispatchSupports(stepType string) bool {
	switch stepType {
	case provision.StepTypeProvisionWarehouse, provision.StepTypeWaitWarehouseReady, provision.StepTypeDeprovisionWarehouse:
		return true
	case scenariosql.StepTypeSQL, scenariosql.StepTypeSQLCatalog:
		return true
	case scenarioperf.StepTypePerfQueries:
		return true
	case scenariodbt.StepTypeDBTRun:
		return true
	default:
		return false
	}
}

func requireScenarioDefaultTeamID(t *testing.T, request map[string]any) {
	t.Helper()
	defaultTeamID, ok := request["default_team_id"]
	if !ok {
		t.Fatal("provision request should include default_team_id")
	}
	if fmt.Sprint(defaultTeamID) != "1" {
		t.Fatalf("default_team_id = %#v, want numeric 1", defaultTeamID)
	}
}

func requireScenarioEnv(t *testing.T, scenario core.Scenario, want string) {
	t.Helper()
	for _, name := range scenario.RequiredEnv {
		if name == want {
			return
		}
	}
	t.Fatalf("scenario %s required_env = %#v, want %s", scenario.Name, scenario.RequiredEnv, want)
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

func boolEnv(t *testing.T, key string, fallback bool) bool {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		t.Fatalf("%s must be a boolean: %v", key, err)
	}
	return parsed
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
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
			if k == "file" || k == "catalog_file" || k == "project_dir" || k == "profiles_dir" {
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
		"run_id_token":   shortRunIDToken(runID),
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

func shortRunIDToken(runID string) string {
	sum := sha1.Sum([]byte(runID))
	return fmt.Sprintf("%x", sum)[:12]
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
