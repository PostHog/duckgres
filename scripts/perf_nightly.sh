#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOCK_FILE="${DUCKGRES_PERF_LOCK_FILE:-/tmp/duckgres-perf-nightly.lock}"
MAX_RUNTIME_SECONDS="${DUCKGRES_PERF_MAX_RUNTIME_SECONDS:-3600}"
DATASET_VERSION="${DUCKGRES_PERF_DATASET_VERSION:-}"
if [[ -z "$DATASET_VERSION" ]]; then
  echo "DUCKGRES_PERF_DATASET_VERSION is required for nightly frozen dataset runs"
  exit 1
fi
CATALOG_PATH="${DUCKGRES_PERF_CATALOG:-$ROOT_DIR/tests/perf/queries/ducklake_frozen.yaml}"
OUTPUT_BASE="${DUCKGRES_PERF_OUTPUT_BASE:-$ROOT_DIR/artifacts/perf}"
RUN_ID="${DUCKGRES_PERF_RUN_ID:-nightly-${DATASET_VERSION}-$(date -u +%Y%m%dT%H%M%SZ)}"
MANIFEST_TABLE="${DUCKGRES_PERF_DATASET_MANIFEST_TABLE:-ducklake.main.dataset_manifest}"

# Child shells launched via timeout/flock only inherit exported variables.
export MAX_RUNTIME_SECONDS
export DATASET_VERSION
export CATALOG_PATH
export OUTPUT_BASE
export RUN_ID
export MANIFEST_TABLE

run_smoke() {
  export DUCKGRES_PERF_DATASET_VERSION="$DATASET_VERSION"
  export DUCKGRES_PERF_DATASET_MANIFEST_TABLE="$MANIFEST_TABLE"
  DUCKGRES_PERF_CATALOG="$CATALOG_PATH" \
  DUCKGRES_PERF_OUTPUT_BASE="$OUTPUT_BASE" \
  DUCKGRES_PERF_RUN_ID="$RUN_ID" \
  ./scripts/perf_smoke.sh
}

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "${MAX_RUNTIME_SECONDS}s" bash -c "$(declare -f run_smoke); run_smoke"
    return
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    gtimeout "${MAX_RUNTIME_SECONDS}s" bash -c "$(declare -f run_smoke); run_smoke"
    return
  fi
  run_smoke
}

if command -v flock >/dev/null 2>&1; then
  flock -n "$LOCK_FILE" bash -c "set -euo pipefail; $(declare -f run_smoke); $(declare -f run_with_timeout); run_with_timeout"
else
  LOCK_DIR="${LOCK_FILE}.d"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "Another nightly perf run is active (lock: $LOCK_DIR)"
    exit 1
  fi
  trap 'rm -rf "$LOCK_DIR"' EXIT
  run_with_timeout
fi

RUN_DIR="$OUTPUT_BASE/$RUN_ID"
MANIFEST_ARTIFACT="$RUN_DIR/dataset_manifest.json"
if [[ ! -f "$MANIFEST_ARTIFACT" ]]; then
  echo "Expected dataset manifest artifact missing: $MANIFEST_ARTIFACT"
  exit 1
fi
if ! grep -Fq "\"dataset_version\":\"$DATASET_VERSION\"" "$MANIFEST_ARTIFACT"; then
  echo "Dataset manifest artifact does not match DUCKGRES_PERF_DATASET_VERSION=$DATASET_VERSION"
  exit 1
fi

if [[ -n "${DUCKGRES_PERF_PUBLISH_DSN:-}" ]]; then
  if [[ -n "${DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD:-}" ]]; then
    echo "DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD is deprecated and ignored when DUCKGRES_PERF_PUBLISH_DSN is set"
  fi
elif [[ -n "${DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD:-}" ]]; then
  echo "DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD is deprecated; prefer DUCKGRES_PERF_PUBLISH_DSN"
  DUCKGRES_PERF_RUN_DIR="$RUN_DIR" bash -lc "$DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD"
fi

echo "Nightly perf run complete: $RUN_DIR"
