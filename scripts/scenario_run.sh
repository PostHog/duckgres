#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/scenario_run.sh [--check-env] [--scenario-file PATH] [--output-base DIR] [--run-id ID] [PATH]

Runs a Duckgres scenario through the Go test entry point.

Required environment:
  DUCKGRES_SCENARIO_API_BASE
  DUCKGRES_SCENARIO_INTERNAL_SECRET
  DUCKGRES_SCENARIO_PG_HOST        (libpq hostaddr / direct TCP address)
  DUCKGRES_SCENARIO_SNI_SUFFIX

Optional environment:
  DUCKGRES_SCENARIO_OUTPUT_BASE
  DUCKGRES_SCENARIO_RUN_ID
  DUCKGRES_SCENARIO_PG_PORT
  DUCKGRES_SCENARIO_PG_CONNECT_TIMEOUT
  DUCKGRES_SCENARIO_MAX_RUNTIME
  DUCKGRES_SCENARIO_GO_TEST_TIMEOUT
  DUCKGRES_SCENARIO_FROZEN_S3_URI (required by frozen dataset scenarios)
USAGE
}

scenario_file="${DUCKGRES_SCENARIO_FILE:-tests/scenario/scenarios/provision_smoke.yaml}"
output_base="${DUCKGRES_SCENARIO_OUTPUT_BASE:-artifacts/scenario}"
run_id="${DUCKGRES_SCENARIO_RUN_ID:-}"
max_runtime="${DUCKGRES_SCENARIO_MAX_RUNTIME:-30m}"
go_test_timeout="${DUCKGRES_SCENARIO_GO_TEST_TIMEOUT:-60m}"
check_env_only=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check-env)
      check_env_only=1
      shift
      ;;
    --scenario-file)
      scenario_file="${2:?--scenario-file requires a path}"
      shift 2
      ;;
    --output-base)
      output_base="${2:?--output-base requires a directory}"
      shift 2
      ;;
    --run-id)
      run_id="${2:?--run-id requires an id}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      scenario_file="$1"
      shift
      ;;
  esac
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

root_relative_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$repo_root/$1" ;;
  esac
}

required=(
  DUCKGRES_SCENARIO_API_BASE
  DUCKGRES_SCENARIO_INTERNAL_SECRET
  DUCKGRES_SCENARIO_PG_HOST
  DUCKGRES_SCENARIO_SNI_SUFFIX
)
missing=()
for name in "${required[@]}"; do
  if [ -z "${!name:-}" ]; then
    missing+=("$name")
  fi
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "Missing required Duckgres scenario environment:" >&2
  for name in "${missing[@]}"; do
    echo "  - $name" >&2
  done
  exit 2
fi

if [ "$check_env_only" -eq 1 ]; then
  echo "Duckgres scenario environment is configured."
  exit 0
fi

scenario_file="$(root_relative_path "$scenario_file")"
output_base="$(root_relative_path "$output_base")"

args=(
  go test -count=1 ./tests/scenario
  -timeout "$go_test_timeout"
  -run TestScenarioRunner
  -scenario-run
  -scenario-file "$scenario_file"
  -scenario-output-base "$output_base"
  -scenario-max-runtime "$max_runtime"
)
if [ -n "$run_id" ]; then
  args+=(-scenario-run-id "$run_id")
fi

"${args[@]}"
