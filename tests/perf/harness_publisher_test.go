package perf

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/datasets"
	"github.com/posthog/duckgres/tests/perf/publisher"
)

func TestPublishArtifactsIfConfiguredSkipsWhenDisabled(t *testing.T) {
	called := false
	orig := publishRunDir
	publishRunDir = func(context.Context, publisher.Config, string) error {
		called = true
		return nil
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{}, "/tmp/run")
	if err != nil {
		t.Fatalf("publishArtifactsIfConfigured returned error: %v", err)
	}
	if called {
		t.Fatalf("expected publisher not to be called")
	}
}

func TestPublishArtifactsIfConfiguredInvokesPublisher(t *testing.T) {
	called := false
	gotRunDir := ""
	orig := publishRunDir
	publishRunDir = func(_ context.Context, cfg publisher.Config, runDir string) error {
		called = true
		gotRunDir = runDir
		if cfg.Schema != "duckgres_perf" {
			t.Fatalf("schema = %q", cfg.Schema)
		}
		return nil
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{
		DSN:    "host=127.0.0.1 user=duckgres dbname=perf sslmode=require",
		Schema: "duckgres_perf",
	}, "/tmp/run")
	if err != nil {
		t.Fatalf("publishArtifactsIfConfigured returned error: %v", err)
	}
	if !called {
		t.Fatalf("expected publisher to be called")
	}
	if gotRunDir != "/tmp/run" {
		t.Fatalf("runDir = %q", gotRunDir)
	}
}

func TestPublishArtifactsIfConfiguredReturnsClearPublisherError(t *testing.T) {
	orig := publishRunDir
	publishRunDir = func(context.Context, publisher.Config, string) error {
		return errors.New("boom")
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{
		DSN:    "host=127.0.0.1 user=duckgres dbname=perf sslmode=require",
		Schema: "duckgres_perf",
	}, "/tmp/run")
	if err == nil {
		t.Fatal("expected publishArtifactsIfConfigured to return an error")
	}
	if !strings.Contains(err.Error(), "publish perf artifacts") {
		t.Fatalf("expected clear publish error, got %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected wrapped publisher error, got %v", err)
	}
}

func TestFinalizeRunArtifactsWritesFrozenMetadataBeforePublish(t *testing.T) {
	outputDir := t.TempDir()
	origPublish := publishRunDir
	origWrite := writeFrozenDatasetMetadata
	var steps []string
	writeFrozenDatasetMetadata = func(outputDir, datasetVersion, manifestTable, catalogPath string, now func() time.Time) error {
		steps = append(steps, "write")
		return writeFrozenDatasetMetadataArtifact(outputDir, datasetVersion, manifestTable, catalogPath, now)
	}
	publishRunDir = func(context.Context, publisher.Config, string) error {
		steps = append(steps, "publish")
		return nil
	}
	t.Cleanup(func() {
		writeFrozenDatasetMetadata = origWrite
		publishRunDir = origPublish
	})

	err := finalizeRunArtifacts(
		context.Background(),
		publisher.Config{DSN: "host=127.0.0.1 user=duckgres dbname=perf sslmode=require", Schema: "duckgres_perf"},
		datasets.RuntimeContract{Frozen: true, DatasetVersion: "v1"},
		outputDir,
		"queries/ducklake_frozen.yaml",
		defaultDatasetManifestTable,
		func() time.Time { return time.Date(2026, time.March, 11, 12, 0, 0, 0, time.UTC) },
	)
	if err != nil {
		t.Fatalf("finalizeRunArtifacts returned error: %v", err)
	}
	if !slices.Equal(steps, []string{"write", "publish"}) {
		t.Fatalf("steps = %v", steps)
	}

	data, err := os.ReadFile(filepath.Join(outputDir, "dataset_manifest.json"))
	if err != nil {
		t.Fatalf("read dataset_manifest.json: %v", err)
	}
	var artifact datasetManifestArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("decode dataset_manifest.json: %v", err)
	}
	if artifact.DatasetVersion != "v1" {
		t.Fatalf("dataset_version = %q", artifact.DatasetVersion)
	}
	if artifact.ManifestTable != defaultDatasetManifestTable {
		t.Fatalf("manifest_table = %q", artifact.ManifestTable)
	}
}

func TestFinalizeRunArtifactsDoesNotPublishWhenFrozenMetadataWriteFails(t *testing.T) {
	origPublish := publishRunDir
	origWrite := writeFrozenDatasetMetadata
	calledPublish := false
	writeFrozenDatasetMetadata = func(string, string, string, string, func() time.Time) error {
		return errors.New("metadata write failed")
	}
	publishRunDir = func(context.Context, publisher.Config, string) error {
		calledPublish = true
		return nil
	}
	t.Cleanup(func() {
		writeFrozenDatasetMetadata = origWrite
		publishRunDir = origPublish
	})

	err := finalizeRunArtifacts(
		context.Background(),
		publisher.Config{DSN: "host=127.0.0.1 user=duckgres dbname=perf sslmode=require", Schema: "duckgres_perf"},
		datasets.RuntimeContract{Frozen: true, DatasetVersion: "v1"},
		t.TempDir(),
		"queries/ducklake_frozen.yaml",
		defaultDatasetManifestTable,
		time.Now,
	)
	if err == nil {
		t.Fatal("expected finalizeRunArtifacts to return an error")
	}
	if !strings.Contains(err.Error(), "metadata write failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	if calledPublish {
		t.Fatal("expected publisher not to be called")
	}
}

func TestValidateFrozenDatasetMetadataRejectsMismatchedVersion(t *testing.T) {
	outputDir := t.TempDir()
	path := filepath.Join(outputDir, "dataset_manifest.json")
	if err := os.WriteFile(path, []byte(`{"dataset_version":"v2"}`), 0o644); err != nil {
		t.Fatalf("write dataset_manifest.json: %v", err)
	}

	err := validateFrozenDatasetMetadata(path, "v1")
	if err == nil {
		t.Fatal("expected validateFrozenDatasetMetadata to return an error")
	}
	if !strings.Contains(err.Error(), "does not match expected") {
		t.Fatalf("unexpected error: %v", err)
	}
}
