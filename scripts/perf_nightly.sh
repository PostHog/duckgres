#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOCK_FILE="${DUCKGRES_PERF_LOCK_FILE:-/tmp/duckgres-perf-nightly.lock}"
MAX_RUNTIME_SECONDS="${DUCKGRES_PERF_MAX_RUNTIME_SECONDS:-3600}"
CATALOG_PATH="${DUCKGRES_PERF_CATALOG:-tests/perf/queries/smoke.yaml}"
OUTPUT_BASE="${DUCKGRES_PERF_OUTPUT_BASE:-artifacts/perf}"
RUN_ID="${DUCKGRES_PERF_RUN_ID:-nightly-$(date -u +%Y%m%dT%H%M%SZ)}"

run_smoke() {
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
  echo "WARN: flock not found; continuing without overlap protection."
  run_with_timeout
fi

RUN_DIR="$OUTPUT_BASE/$RUN_ID"
if [[ -n "${DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD:-}" ]]; then
  DUCKGRES_PERF_RUN_DIR="$RUN_DIR" bash -lc "$DUCKGRES_PERF_ARTIFACT_UPLOAD_CMD"
fi

echo "Nightly perf run complete: $RUN_DIR"
