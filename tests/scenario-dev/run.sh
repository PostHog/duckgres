#!/usr/bin/env bash
# Run scenario-dev against either a per-run isolated Duckgres stack or the
# shared dev stack. The isolated stack is delegated to tests/e2e-mw-dev/run.sh;
# this script owns only the scenario-runner Kubernetes Job.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTX="${KUBE_CONTEXT:?KUBE_CONTEXT is required}"
KUBECTL=(kubectl --context "$CTX")
USE_SHARED_DEV="${USE_SHARED_DEV:-false}"
SCENARIO_NAMESPACE="${SCENARIO_NAMESPACE:-${NAMESPACE:-}}"
SCENARIO_SHARED_NAMESPACE="${SCENARIO_SHARED_NAMESPACE:-duckgres}"
SCENARIO_CONFIG_SECRET_NAME="${SCENARIO_CONFIG_SECRET_NAME:-duckgres-scenario-config}"
SCENARIO_CONFIG_SECRET_NAMESPACE="${SCENARIO_CONFIG_SECRET_NAMESPACE:-$SCENARIO_SHARED_NAMESPACE}"
SCENARIO_INTERNAL_SECRET_NAME="${SCENARIO_INTERNAL_SECRET_NAME:-duckgres-tokens}"
SCENARIO_INTERNAL_SECRET_KEY="${SCENARIO_INTERNAL_SECRET_KEY:-internal-secret}"
SCENARIO_SNI_SUFFIX="${SCENARIO_SNI_SUFFIX:-.ci.duckgres.local}"
SCENARIO_JOB_WATCH_TIMEOUT_SECONDS="${SCENARIO_JOB_WATCH_TIMEOUT_SECONDS:-16200}"

target_namespace() {
  if [ "$USE_SHARED_DEV" = "true" ]; then
    printf '%s\n' "$SCENARIO_SHARED_NAMESPACE"
  else
    : "${SCENARIO_NAMESPACE:?SCENARIO_NAMESPACE or NAMESPACE is required}"
    printf '%s\n' "$SCENARIO_NAMESPACE"
  fi
}

control_plane_api_base() {
  local ns
  ns="$(target_namespace)"
  if [ "$USE_SHARED_DEV" = "true" ]; then
    printf '%s\n' "${SCENARIO_SHARED_API_BASE:?SCENARIO_SHARED_API_BASE is required when USE_SHARED_DEV=true}"
  else
    printf 'http://duckgres-control-plane.%s.svc:8080\n' "$ns"
  fi
}

pg_host() {
  local ns
  ns="$(target_namespace)"
  if [ "$USE_SHARED_DEV" = "true" ]; then
    printf '%s\n' "${SCENARIO_SHARED_PG_HOST:?SCENARIO_SHARED_PG_HOST is required when USE_SHARED_DEV=true}"
  else
    printf 'duckgres-control-plane.%s.svc\n' "$ns"
  fi
}

flight_addr() {
  local ns
  ns="$(target_namespace)"
  if [ "$USE_SHARED_DEV" = "true" ]; then
    printf '%s\n' "${SCENARIO_SHARED_FLIGHT_ADDR:?SCENARIO_SHARED_FLIGHT_ADDR is required when USE_SHARED_DEV=true}"
  else
    printf 'duckgres-control-plane.%s.svc:8815\n' "$ns"
  fi
}

ensure_config_secret() {
  local target_ns source_ns name
  target_ns="$(target_namespace)"
  source_ns="$SCENARIO_CONFIG_SECRET_NAMESPACE"
  name="$SCENARIO_CONFIG_SECRET_NAME"
  if [ "$target_ns" = "$source_ns" ]; then
    return 0
  fi
  "${KUBECTL[@]}" -n "$source_ns" get secret "$name" -o json \
    | jq --arg ns "$target_ns" 'del(
        .metadata.annotations,
        .metadata.creationTimestamp,
        .metadata.managedFields,
        .metadata.resourceVersion,
        .metadata.uid
      ) | .metadata.namespace = $ns' \
    | "${KUBECTL[@]}" apply -f -
}

scenario_requires_frozen_config() {
  local scenario_path
  case "$SCENARIO_FILE" in
    /*) scenario_path="$SCENARIO_FILE" ;;
    *) scenario_path="$ROOT/$SCENARIO_FILE" ;;
  esac
  [ -f "$scenario_path" ] && grep -q 'DUCKGRES_SCENARIO_FROZEN_S3_URI' "$scenario_path"
}

job_name() {
  local name
  name="${SCENARIO_NAME:?SCENARIO_NAME is required}"
  printf 'duckgres-scenario-%s\n' "$name" | tr '_' '-'
}

cmd_deploy() {
  if [ "$USE_SHARED_DEV" = "true" ]; then
    echo "Skipping isolated Duckgres deploy; using shared dev stack."
    return 0
  fi
  bash "$ROOT/tests/e2e-mw-dev/run.sh" deploy
}

cmd_test() {
  local ns job api_base pg flight rc
  : "${SCENARIO_RUNNER_IMAGE:?SCENARIO_RUNNER_IMAGE is required}"
  : "${SCENARIO_FILE:?SCENARIO_FILE is required}"
  : "${DUCKGRES_SCENARIO_RUN_ID:?DUCKGRES_SCENARIO_RUN_ID is required}"
  : "${DUCKGRES_SCENARIO_MAX_RUNTIME:?DUCKGRES_SCENARIO_MAX_RUNTIME is required}"
  : "${DUCKGRES_SCENARIO_GO_TEST_TIMEOUT:?DUCKGRES_SCENARIO_GO_TEST_TIMEOUT is required}"

  ns="$(target_namespace)"
  job="$(job_name)"
  api_base="$(control_plane_api_base)"
  pg="$(pg_host)"
  flight="$(flight_addr)"
  if scenario_requires_frozen_config; then
    ensure_config_secret
  fi

  "${KUBECTL[@]}" -n "$ns" delete job "$job" --ignore-not-found
  cat <<YAML | "${KUBECTL[@]}" -n "$ns" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $SCENARIO_JOB_WATCH_TIMEOUT_SECONDS
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: scenario
          image: $SCENARIO_RUNNER_IMAGE
          imagePullPolicy: IfNotPresent
          args: ["$SCENARIO_FILE"]
          env:
            - { name: DUCKGRES_SCENARIO_API_BASE, value: "$api_base" }
            - name: DUCKGRES_SCENARIO_INTERNAL_SECRET
              valueFrom:
                secretKeyRef:
                  name: $SCENARIO_INTERNAL_SECRET_NAME
                  key: $SCENARIO_INTERNAL_SECRET_KEY
            - { name: DUCKGRES_SCENARIO_PG_HOST, value: "$pg" }
            - { name: DUCKGRES_SCENARIO_SNI_SUFFIX, value: "$SCENARIO_SNI_SUFFIX" }
            - { name: DUCKGRES_SCENARIO_FROZEN_S3_URI, valueFrom: { secretKeyRef: { name: $SCENARIO_CONFIG_SECRET_NAME, key: frozen-s3-uri, optional: true } } }
            - { name: DUCKGRES_SCENARIO_FLIGHT_ADDR, value: "$flight" }
            - { name: DUCKGRES_SCENARIO_FLIGHT_INSECURE_SKIP_VERIFY, value: "true" }
            - { name: DUCKGRES_SCENARIO_DBT_BIN, value: "dbt" }
            - { name: DUCKGRES_SCENARIO_OUTPUT_BASE, value: "/artifacts/scenario-dev" }
            - { name: DUCKGRES_SCENARIO_RUN_ID, value: "$DUCKGRES_SCENARIO_RUN_ID" }
            - { name: DUCKGRES_SCENARIO_MAX_RUNTIME, value: "$DUCKGRES_SCENARIO_MAX_RUNTIME" }
            - { name: DUCKGRES_SCENARIO_GO_TEST_TIMEOUT, value: "$DUCKGRES_SCENARIO_GO_TEST_TIMEOUT" }
            - { name: GOCACHE, value: "/tmp/go-cache" }
          resources:
            requests: { cpu: "1", memory: "2Gi" }
            limits: { memory: "6Gi" }
          volumeMounts:
            - { name: artifacts, mountPath: /artifacts }
      volumes:
        - { name: artifacts, emptyDir: {} }
YAML

  echo "Streaming scenario logs for $job..."
  "${KUBECTL[@]}" -n "$ns" wait --for=condition=ready pod -l "job-name=$job" --timeout=180s || true
  "${KUBECTL[@]}" -n "$ns" logs -f "job/$job" || true
  rc=0
  wait_for_job "$ns" "$job" || rc=$?
  copy_artifacts "$ns" "$job"
  return "$rc"
}

wait_for_job() {
  local ns="$1" job="$2" deadline
  deadline=$(( $(date +%s) + SCENARIO_JOB_WATCH_TIMEOUT_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$("${KUBECTL[@]}" -n "$ns" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null)" = "True" ]; then
      echo "scenario Job complete."
      return 0
    fi
    if [ "$("${KUBECTL[@]}" -n "$ns" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null)" = "True" ]; then
      echo "scenario Job failed." >&2
      return 1
    fi
    sleep 10
  done
  echo "scenario Job did not reach a terminal state in time." >&2
  return 1
}

copy_artifacts() {
  local ns="$1" job="$2" pod dest
  pod="$("${KUBECTL[@]}" -n "$ns" get pod -l "job-name=$job" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "$pod" ] || return 0
  dest="$ROOT/artifacts/scenario-dev/${SCENARIO_NAME}"
  mkdir -p "$dest"
  "${KUBECTL[@]}" -n "$ns" cp "$pod:/artifacts/scenario-dev" "$dest" >/dev/null 2>&1 || true
}

cmd_diagnostics() {
  local ns job
  ns="$(target_namespace)"
  job="$(job_name)"
  echo "::group::scenario job state"
  "${KUBECTL[@]}" -n "$ns" get job,pod -l "job-name=$job" -o wide || true
  "${KUBECTL[@]}" -n "$ns" describe job "$job" || true
  "${KUBECTL[@]}" -n "$ns" describe pod -l "job-name=$job" || true
  echo "::endgroup::"
  if [ "$USE_SHARED_DEV" != "true" ]; then
    bash "$ROOT/tests/e2e-mw-dev/run.sh" diagnostics || true
  fi
}

cmd_teardown() {
  local ns job
  ns="$(target_namespace)"
  job="$(job_name)"
  "${KUBECTL[@]}" -n "$ns" delete job "$job" --ignore-not-found --wait=false || true
  if [ "$USE_SHARED_DEV" = "true" ]; then
    return 0
  fi
  bash "$ROOT/tests/e2e-mw-dev/run.sh" teardown
}

case "${1:?usage: run.sh deploy|test|diagnostics|teardown}" in
  deploy) cmd_deploy ;;
  test) cmd_test ;;
  diagnostics) cmd_diagnostics ;;
  teardown) cmd_teardown ;;
  *) echo "unknown: $1" >&2; exit 2 ;;
esac
