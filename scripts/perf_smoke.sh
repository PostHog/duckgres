#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CATALOG_PATH="${DUCKGRES_PERF_CATALOG:-$ROOT_DIR/tests/perf/queries/smoke.yaml}"
OUTPUT_BASE="${DUCKGRES_PERF_OUTPUT_BASE:-$ROOT_DIR/artifacts/perf}"
RUN_ID="${DUCKGRES_PERF_RUN_ID:-smoke-$(date -u +%Y%m%dT%H%M%SZ)}"

echo "Running perf smoke harness"
echo "  catalog: $CATALOG_PATH"
echo "  output:  $OUTPUT_BASE/$RUN_ID"

mkdir -p "$OUTPUT_BASE"

go test ./tests/perf \
  -run TestGoldenQueryPerformanceHarness \
  -perf-run \
  -perf-catalog "$CATALOG_PATH" \
  -perf-output-base "$OUTPUT_BASE" \
  -perf-run-id "$RUN_ID"

echo "Perf smoke artifacts written to $OUTPUT_BASE/$RUN_ID"
