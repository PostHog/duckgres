package perf

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/posthog/duckgres/tests/perf/datasets"
	"github.com/posthog/duckgres/tests/perf/publisher"
)

const defaultDatasetManifestTable = "ducklake.main.dataset_manifest"

type datasetManifestArtifact struct {
	DatasetVersion string `json:"dataset_version"`
	ManifestTable  string `json:"manifest_table"`
	Catalog        string `json:"catalog"`
	CheckedAtUTC   string `json:"checked_at_utc"`
}

var writeFrozenDatasetMetadata = writeFrozenDatasetMetadataArtifact

func finalizeRunArtifacts(
	ctx context.Context,
	publishCfg publisher.Config,
	runtimeContract datasets.RuntimeContract,
	outputDir string,
	catalogPath string,
	manifestTable string,
	now func() time.Time,
) error {
	if runtimeContract.Frozen {
		if err := writeFrozenDatasetMetadata(outputDir, runtimeContract.DatasetVersion, manifestTable, catalogPath, now); err != nil {
			return err
		}
	}
	return publishArtifactsIfConfigured(ctx, publishCfg, outputDir)
}

func writeFrozenDatasetMetadataArtifact(
	outputDir string,
	datasetVersion string,
	manifestTable string,
	catalogPath string,
	now func() time.Time,
) error {
	if datasetVersion == "" {
		return fmt.Errorf("dataset manifest requires dataset_version")
	}
	if manifestTable == "" {
		manifestTable = defaultDatasetManifestTable
	}
	if now == nil {
		now = time.Now
	}

	artifact := datasetManifestArtifact{
		DatasetVersion: datasetVersion,
		ManifestTable:  manifestTable,
		Catalog:        catalogPath,
		CheckedAtUTC:   now().UTC().Format(time.RFC3339),
	}

	path := filepath.Join(outputDir, "dataset_manifest.json")
	data, err := json.Marshal(artifact)
	if err != nil {
		return fmt.Errorf("marshal dataset_manifest.json: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write dataset_manifest.json: %w", err)
	}
	if err := validateFrozenDatasetMetadata(path, datasetVersion); err != nil {
		return err
	}
	return nil
}

func validateFrozenDatasetMetadata(path, expectedDatasetVersion string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read dataset_manifest.json: %w", err)
	}
	var artifact datasetManifestArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return fmt.Errorf("decode dataset_manifest.json: %w", err)
	}
	if artifact.DatasetVersion == "" {
		return fmt.Errorf("dataset_manifest.json is missing dataset_version")
	}
	if artifact.DatasetVersion != expectedDatasetVersion {
		return fmt.Errorf("dataset_manifest.json dataset_version %q does not match expected %q", artifact.DatasetVersion, expectedDatasetVersion)
	}
	return nil
}
