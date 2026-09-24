package scenario

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
	scenarioperf "github.com/posthog/duckgres/tests/mw-dev/scenario/perf"
	scenariosql "github.com/posthog/duckgres/tests/mw-dev/scenario/sql"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
	"github.com/posthog/duckgres/tests/perf/properties"
	"gopkg.in/yaml.v3"
)

const stepTypePropertiesComparison = "properties_comparison"

// This phase runs only after the original benchmarks have written their
// artifacts. Fixture preparation errors cannot prevent those results publishing.
func (e dispatchExecutor) runPropertiesComparison(ctx context.Context, step core.Step) error {
	// Properties results publish under the nightly's dataset so one dashboard
	// selection covers every suite; the fixture hash is kept as fixture_version.
	datasetVersion, _ := step.With["dataset_version"].(string)
	if datasetVersion == "" {
		return fmt.Errorf("properties comparison requires the nightly dataset_version")
	}
	source := os.Getenv("DUCKGRES_SCENARIO_PROPERTIES_S3_URI")
	prepared, err := preparePropertiesComparison(ctx, source, step)
	if err != nil {
		return fmt.Errorf("prepare properties comparison: %w", err)
	}
	defer func() { _ = os.RemoveAll(prepared.directory) }()
	source = prepared.dataset.Prefix
	orgID := step.With["org_id"]
	if err := e.sql.ExecuteStep(ctx, core.Step{ID: step.ID + "_setup", Type: scenariosql.StepTypeSQL, With: map[string]any{
		"org_id": orgID, "catalog": "ducklake", "file": filepath.Join(prepared.directory, "setup.sql"),
		"max_attempts": 1, "exec_only": true,
	}}); err != nil {
		return err
	}
	hoglakeWith := map[string]any{
		"org_id": orgID, "uri": step.With["hoglake_uri"], "file": step.With["hoglake_file"],
		"properties_source": source, "representation": "json", "hoglake_catalog": step.With["hoglake_catalog"],
	}
	if timeout, ok := step.With["hydration_timeout"]; ok {
		hoglakeWith["hydration_timeout"] = timeout
	}
	if err := e.perf.ExecuteStep(ctx, core.Step{ID: step.ID + "_hoglake", Type: scenarioperf.StepTypeSetupHoglake, With: hoglakeWith}); err != nil {
		return err
	}
	with := make(map[string]any, len(step.With)+6)
	for key, value := range step.With {
		with[key] = value
	}
	with["catalog_file"] = filepath.Join(prepared.directory, "catalog.yaml")
	with["dataset_version"] = datasetVersion
	with["suite"] = perfcore.SuiteProperties
	with["fixture_version"] = "properties-sha256-" + prepared.dataset.SHA256
	with["output_subdir"] = "perf-properties"
	// Distinct run ID so the publisher never overwrites the table-suite run;
	// consumers pair the two through nightly_run_id, not this suffix.
	with["nightly_run_id"] = fmt.Sprint(step.With["run_id"])
	with["run_id"] = fmt.Sprint(step.With["run_id"]) + "-properties"
	return e.perf.ExecuteStep(ctx, core.Step{ID: step.ID, Type: scenarioperf.StepTypePerfQueries, With: with})
}

type preparedPropertiesComparison struct {
	directory string
	dataset   *properties.Dataset
}

func preparePropertiesComparison(ctx context.Context, source string, step core.Step) (*preparedPropertiesComparison, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	if region, _ := step.With["athena_region"].(string); region != "" {
		cfg.Region = region
	}
	client := glue.NewFromConfig(cfg)
	database, _ := step.With["athena_database"].(string)
	const table = "properties_events_supported"
	if source == "" {
		out, err := client.GetTable(ctx, &glue.GetTableInput{DatabaseName: aws.String(database), Name: aws.String(table)})
		if err != nil {
			return nil, fmt.Errorf("discover default properties fixture from Athena table: %w", err)
		}
		if out.Table == nil || out.Table.StorageDescriptor == nil || aws.ToString(out.Table.StorageDescriptor.Location) == "" {
			return nil, fmt.Errorf("Athena properties table has no default fixture location")
		}
		source = aws.ToString(out.Table.StorageDescriptor.Location)
	}
	dataset, err := properties.Discover(ctx, s3.NewFromConfig(cfg), source)
	if err != nil {
		return nil, err
	}
	if err := dataset.VerifyAthenaTable(ctx, client, database, table); err != nil {
		return nil, fmt.Errorf("selected fixture must match the preprovisioned Athena properties table; update its mapping when generating the replacement fixture: %w", err)
	}
	catalog, err := yaml.Marshal(properties.Catalog())
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "properties-perf-")
	if err != nil {
		return nil, err
	}
	for name, data := range map[string][]byte{"setup.sql": []byte(dataset.SetupSQL()), "catalog.yaml": catalog} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			_ = os.RemoveAll(dir)
			return nil, err
		}
	}
	fmt.Printf("Selected %d generated properties Parquet objects for the properties comparison.\n", len(dataset.Files))
	return &preparedPropertiesComparison{directory: dir, dataset: dataset}, nil
}
