package perf

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPerfNightlyScript_PropagatesVarsThroughBashC(t *testing.T) {
	root, err := findProjectRoot()
	if err != nil {
		t.Fatalf("find project root: %v", err)
	}

	tmpDir := t.TempDir()
	tmpScriptsDir := filepath.Join(tmpDir, "scripts")
	if err := os.MkdirAll(tmpScriptsDir, 0o755); err != nil {
		t.Fatalf("mkdir scripts dir: %v", err)
	}

	nightlySource := filepath.Join(root, "scripts", "perf_nightly.sh")
	nightlyBody, err := os.ReadFile(nightlySource)
	if err != nil {
		t.Fatalf("read %s: %v", nightlySource, err)
	}
	nightlyScript := filepath.Join(tmpScriptsDir, "perf_nightly.sh")
	if err := os.WriteFile(nightlyScript, nightlyBody, 0o755); err != nil {
		t.Fatalf("write nightly script: %v", err)
	}

	perfSmoke := filepath.Join(tmpScriptsDir, "perf_smoke.sh")
	if err := os.WriteFile(perfSmoke, []byte(`#!/bin/bash
set -euo pipefail
: "${DUCKGRES_PERF_OUTPUT_BASE:?missing output base}"
: "${DUCKGRES_PERF_RUN_ID:?missing run id}"
: "${DUCKGRES_PERF_DATASET_VERSION:?missing dataset version}"
: "${DUCKGRES_PERF_DATASET_MANIFEST_TABLE:?missing manifest table}"
: "${DUCKGRES_PERF_CATALOG:?missing catalog path}"
run_dir="$DUCKGRES_PERF_OUTPUT_BASE/$DUCKGRES_PERF_RUN_ID"
mkdir -p "$run_dir"
printf '{"dataset_version":"%s"}\n' "$DUCKGRES_PERF_DATASET_VERSION" > "$run_dir/dataset_manifest.json"
`), 0o755); err != nil {
		t.Fatalf("write perf_smoke.sh: %v", err)
	}

	fakeBin := filepath.Join(tmpDir, "fakebin")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatalf("mkdir fakebin: %v", err)
	}
	fakeFlock := filepath.Join(fakeBin, "flock")
	if err := os.WriteFile(fakeFlock, []byte(`#!/bin/bash
set -euo pipefail
if [[ "$1" == "-n" ]]; then
  shift 2
fi
exec "$@"
`), 0o755); err != nil {
		t.Fatalf("write fake flock: %v", err)
	}
	fakeTimeout := filepath.Join(fakeBin, "timeout")
	if err := os.WriteFile(fakeTimeout, []byte(`#!/bin/bash
set -euo pipefail
shift
exec "$@"
`), 0o755); err != nil {
		t.Fatalf("write fake timeout: %v", err)
	}

	cmd := exec.Command("bash", nightlyScript)
	cmd.Dir = tmpDir
	cmd.Env = append(os.Environ(),
		"DUCKGRES_PERF_DATASET_VERSION=v1",
		"DUCKGRES_PERF_OUTPUT_BASE="+filepath.Join(tmpDir, "artifacts"),
		"DUCKGRES_PERF_RUN_ID=nightly-v1-test",
		"DUCKGRES_PERF_CATALOG="+filepath.Join(tmpDir, "catalog.yaml"),
		"DUCKGRES_PERF_LOCK_FILE="+filepath.Join(tmpDir, "nightly.lock"),
		"PATH="+fakeBin+":"+os.Getenv("PATH"),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("perf_nightly.sh failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Nightly perf run complete:") {
		t.Fatalf("expected success output, got: %s", output)
	}
}
