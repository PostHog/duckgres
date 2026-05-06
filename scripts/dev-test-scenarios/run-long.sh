#!/usr/bin/env bash
set -u

usage() {
  cat <<USAGE
Usage: bash scripts/dev-test-scenarios/run-long.sh <suite>

Suites:
  long-boundaries  Run long-query boundary, cancel, and killed-client scenarios.
  hour-boundary    Run the one-hour long-query boundary scenario.
  all              Run long-boundaries, then hour-boundary in separate processes.
USAGE
}
run_long_boundaries() {

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SCHEMA="${SCHEMA:-duckgres_long_boundary_${RUN_ID}}"
DSN="${DSN:-host=perfdev.dw.dev.postwh.com port=5432 dbname=perfdev user=bill sslmode=require}"
export PGPASSWORD="${PGPASSWORD:-dev-bill055}"

ROOT_DIR="$(pwd)"
ARTIFACT_DIR="${ROOT_DIR}/artifacts/dev-deploy-scenarios/${RUN_ID}-long-boundaries"
REPORT="${ARTIFACT_DIR}/long-boundary-report.md"
SUMMARY="${ARTIFACT_DIR}/summary.tsv"
mkdir -p "${ARTIFACT_DIR}"/{sql,outputs,activity,loki}

LOKI_PORT="${LOKI_PORT:-3103}"
LOKI_URL="http://127.0.0.1:${LOKI_PORT}"
LOKI_PID=""
psql_cmd=(psql "${DSN}" -v ON_ERROR_STOP=1)

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_ms() { printf '%s000' "$(date +%s)"; }
now_ns() { printf '%s000000000' "$(date +%s)"; }

elapsed_ms() {
  local start="$1"
  printf '%s' "$(( $(now_ms) - start ))"
}

record() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" "$7" >>"${SUMMARY}"
}

write_sql() {
  printf '%s\n' "$2" >"${ARTIFACT_DIR}/sql/$1.sql"
}

run_sql() {
  local scenario="$1" name="$2" sql="$3" expected="${4:-pass}"
  local start_iso start_ms rc elapsed
  write_sql "${name}" "${sql}"
  start_iso="$(now_iso)"
  start_ms="$(now_ms)"
  "${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/${name}.sql" >"${ARTIFACT_DIR}/outputs/${name}.out" 2>"${ARTIFACT_DIR}/outputs/${name}.err"
  rc=$?
  elapsed="$(elapsed_ms "${start_ms}")"
  record "${scenario}" "${name}" "${start_iso}" "$(now_iso)" "${elapsed}" "${rc}" "${expected}"
  return "${rc}"
}

activity_snapshot() {
  local name="$1"
  "${psql_cmd[@]}" -At -F $'\t' -c "SELECT pid, datname, usename, state, worker_id, query FROM pg_stat_activity ORDER BY pid;" >"${ARTIFACT_DIR}/activity/${name}.tsv" 2>"${ARTIFACT_DIR}/activity/${name}.err" &
  local pid="$!"
  local waited=0
  while kill -0 "${pid}" >/dev/null 2>&1; do
    if [[ "${waited}" -ge 15 ]]; then
      kill -TERM "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" >/dev/null 2>&1 || true
      printf 'snapshot timed out after 15s\n' >>"${ARTIFACT_DIR}/activity/${name}.err"
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "${pid}"
}

loki_query() {
  local name="$1" query="$2"
  curl -sS -G "${LOKI_URL}/loki/api/v1/query_range" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${RUN_START_NS}" \
    --data-urlencode "end=$(now_ns)" \
    --data-urlencode "limit=1000" \
    --data-urlencode "direction=backward" >"${ARTIFACT_DIR}/loki/${name}.json" 2>"${ARTIFACT_DIR}/loki/${name}.err" || true
  jq -r '[.data.result[].values[]?] | length' "${ARTIFACT_DIR}/loki/${name}.json" >"${ARTIFACT_DIR}/loki/${name}.count" 2>/dev/null || true
}

cleanup() {
  if [[ -n "${LOKI_PID}" ]]; then
    kill "${LOKI_PID}" >/dev/null 2>&1 || true
    wait "${LOKI_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

printf 'scenario\tname\tstart_utc\tend_utc\telapsed_ms\trc\texpected\n' >"${SUMMARY}"
RUN_START_ISO="$(now_iso)"
RUN_START_NS="$(now_ns)"

kubectl --context posthog-mw-dev -n monitoring port-forward svc/loki-logs-read "${LOKI_PORT}:3100" >"${ARTIFACT_DIR}/loki/port-forward.out" 2>"${ARTIFACT_DIR}/loki/port-forward.err" &
LOKI_PID=$!
sleep 2
curl -sS "${LOKI_URL}/ready" >"${ARTIFACT_DIR}/loki/ready.out" 2>"${ARTIFACT_DIR}/loki/ready.err" || true

cat >"${REPORT}" <<EOF
# Long Query Boundary Run

- Run ID: \`${RUN_ID}\`
- Schema: \`${SCHEMA}\`
- DSN: \`${DSN}\`
- Started UTC: \`${RUN_START_ISO}\`
- Artifact directory: \`${ARTIFACT_DIR}\`

## Boundary Targets

- Complete a medium-long analytical query crossing ~30s.
- Complete a longer analytical query crossing ~2 minutes when feasible.
- Cancel one active long query after 5 minutes.
- Kill one active client after 2 minutes.
- Capture pg_stat_activity and Loki evidence around each boundary.

EOF

run_sql preflight connect "SELECT '${RUN_ID}:long_boundary:preflight' AS marker, current_database(), current_user;"
run_sql setup create_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};"
run_sql setup create_events_3m "CREATE TABLE ${SCHEMA}.events AS
SELECT
  i AS event_id,
  i % 1000 AS user_id,
  TIMESTAMP '2026-01-01' + i * INTERVAL '1 second' AS ts,
  (i % 10000)::DOUBLE / 100.0 AS value
FROM generate_series(1, 3000000) AS t(i);
SELECT '${RUN_ID}:long_boundary:setup_3m' AS marker, COUNT(*) AS rows FROM ${SCHEMA}.events;"

long_join_sql() {
  local marker="$1" cutoff="$2"
  cat <<SQL
SELECT '${RUN_ID}:long_boundary:${marker}' AS marker, a.user_id, SUM(a.value * b.value) AS score
FROM ${SCHEMA}.events a
JOIN ${SCHEMA}.events b ON a.user_id = b.user_id
WHERE a.event_id <= ${cutoff} AND b.event_id <= ${cutoff}
GROUP BY a.user_id
ORDER BY score DESC
LIMIT 20;
SQL
}

run_sql boundary_complete complete_1m "$(long_join_sql complete_1m 1000000)"
run_sql boundary_complete complete_15m "$(long_join_sql complete_15m 1500000)"
run_sql boundary_complete complete_2m "$(long_join_sql complete_2m 2000000)"

write_sql cancel_5min "$(long_join_sql cancel_5min 3000000)"
CANCEL_START="$(now_ms)"
CANCEL_START_ISO="$(now_iso)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/cancel_5min.sql" >"${ARTIFACT_DIR}/outputs/cancel_5min.out" 2>"${ARTIFACT_DIR}/outputs/cancel_5min.err" &
CANCEL_PID=$!
sleep 30
activity_snapshot cancel_5min_after_30s
sleep 120
activity_snapshot cancel_5min_after_150s
sleep 150
activity_snapshot cancel_5min_before_cancel
kill -INT "${CANCEL_PID}" >/dev/null 2>&1 || true
wait "${CANCEL_PID}"
CANCEL_RC=$?
record boundary_cancel cancel_5min "${CANCEL_START_ISO}" "$(now_iso)" "$(elapsed_ms "${CANCEL_START}")" "${CANCEL_RC}" expected-cancel
activity_snapshot cancel_5min_after_cancel
run_sql boundary_cancel cancel_after "SELECT '${RUN_ID}:long_boundary:cancel_after' AS marker, 1 AS ok;"

write_sql kill_2min "$(long_join_sql kill_2min 3000000)"
KILL_START="$(now_ms)"
KILL_START_ISO="$(now_iso)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/kill_2min.sql" >"${ARTIFACT_DIR}/outputs/kill_2min.out" 2>"${ARTIFACT_DIR}/outputs/kill_2min.err" &
KILL_PID=$!
sleep 60
activity_snapshot kill_2min_after_60s
sleep 60
activity_snapshot kill_2min_before_kill
kill -TERM "${KILL_PID}" >/dev/null 2>&1 || true
wait "${KILL_PID}"
KILL_RC=$?
record boundary_kill kill_2min "${KILL_START_ISO}" "$(now_iso)" "$(elapsed_ms "${KILL_START}")" "${KILL_RC}" expected-kill
activity_snapshot kill_2min_after_kill
run_sql boundary_kill kill_after "SELECT '${RUN_ID}:long_boundary:kill_after' AS marker, 1 AS ok;"

run_sql cleanup drop_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;"

loki_query run_id '{namespace="duckgres"} |= "'"${RUN_ID}"'"'
loki_query pressure '{namespace="duckgres"} |~ "ERROR|Canceled|cancel|unresponsive|timeout|worker|Failed to create session"'

{
  printf '\n## Completed\n\n'
  printf -- '- Finished UTC: `%s`\n' "$(now_iso)"
  printf -- '- Summary TSV: `%s`\n' "${SUMMARY}"
  printf -- '- SQL files: `%s/sql/`\n' "${ARTIFACT_DIR}"
  printf -- '- Outputs: `%s/outputs/`\n' "${ARTIFACT_DIR}"
  printf -- '- Activity snapshots: `%s/activity/`\n' "${ARTIFACT_DIR}"
  printf -- '- Loki output: `%s/loki/`\n\n' "${ARTIFACT_DIR}"
  printf '## Scenario Summary\n\n'
  printf '| Scenario | Name | Start UTC | End UTC | Elapsed ms | RC | Expected |\n'
  printf '|---|---|---:|---:|---:|---:|---|\n'
  tail -n +2 "${SUMMARY}" | while IFS=$'\t' read -r scenario name start end elapsed rc expected; do
    printf '| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` | `%s` |\n' "${scenario}" "${name}" "${start}" "${end}" "${elapsed}" "${rc}" "${expected}"
  done
  printf '\n## Loki Counts\n\n'
  printf -- '- RUN_ID matches: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/run_id.count" 2>/dev/null || printf unknown)"
  printf -- '- Pressure/error matches: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/pressure.count" 2>/dev/null || printf unknown)"
} >>"${REPORT}"

printf '%s\n' "${REPORT}"
}

run_hour_boundary() {

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SCHEMA="${SCHEMA:-duckgres_hour_boundary_${RUN_ID}}"
DSN="${DSN:-host=perfdev.dw.dev.postwh.com port=5432 dbname=perfdev user=bill sslmode=require}"
export PGPASSWORD="${PGPASSWORD:-dev-bill055}"

ROOT_DIR="$(pwd)"
ARTIFACT_DIR="${ROOT_DIR}/artifacts/dev-deploy-scenarios/${RUN_ID}-hour-boundary"
REPORT="${ARTIFACT_DIR}/hour-boundary-report.md"
SUMMARY="${ARTIFACT_DIR}/summary.tsv"
mkdir -p "${ARTIFACT_DIR}"/{sql,outputs,activity,loki}

LOKI_PORT="${LOKI_PORT:-3105}"
LOKI_URL="http://127.0.0.1:${LOKI_PORT}"
LOKI_PID=""
QUERY_PID=""
psql_cmd=(psql "${DSN}" -v ON_ERROR_STOP=1)

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_s() { date +%s; }
now_ns() { printf '%s000000000' "$(date +%s)"; }

record() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" "$7" >>"${SUMMARY}"
}

run_sql() {
  local scenario="$1" name="$2" sql="$3" expected="${4:-pass}"
  local start_iso start_s rc elapsed_ms
  printf '%s\n' "${sql}" >"${ARTIFACT_DIR}/sql/${name}.sql"
  start_iso="$(now_iso)"
  start_s="$(now_s)"
  "${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/${name}.sql" >"${ARTIFACT_DIR}/outputs/${name}.out" 2>"${ARTIFACT_DIR}/outputs/${name}.err"
  rc=$?
  elapsed_ms="$(( ($(now_s) - start_s) * 1000 ))"
  record "${scenario}" "${name}" "${start_iso}" "$(now_iso)" "${elapsed_ms}" "${rc}" "${expected}"
  return "${rc}"
}

snapshot_activity() {
  local name="$1"
  "${psql_cmd[@]}" -At -F $'\t' -c "SELECT pid, datname, usename, state, worker_id, query FROM pg_stat_activity ORDER BY pid;" >"${ARTIFACT_DIR}/activity/${name}.tsv" 2>"${ARTIFACT_DIR}/activity/${name}.err" &
  local snap_pid="$!"
  local waited=0
  while kill -0 "${snap_pid}" >/dev/null 2>&1; do
    if [[ "${waited}" -ge 20 ]]; then
      kill -TERM "${snap_pid}" >/dev/null 2>&1 || true
      wait "${snap_pid}" >/dev/null 2>&1 || true
      printf 'snapshot timed out after 20s\n' >>"${ARTIFACT_DIR}/activity/${name}.err"
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "${snap_pid}"
}

loki_query() {
  local name="$1" query="$2"
  curl -sS -G "${LOKI_URL}/loki/api/v1/query_range" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${RUN_START_NS}" \
    --data-urlencode "end=$(now_ns)" \
    --data-urlencode "limit=1000" \
    --data-urlencode "direction=backward" >"${ARTIFACT_DIR}/loki/${name}.json" 2>"${ARTIFACT_DIR}/loki/${name}.err" || true
  jq -r '[.data.result[].values[]?] | length' "${ARTIFACT_DIR}/loki/${name}.json" >"${ARTIFACT_DIR}/loki/${name}.count" 2>/dev/null || true
}

cleanup() {
  if [[ -n "${QUERY_PID}" ]] && kill -0 "${QUERY_PID}" >/dev/null 2>&1; then
    kill -INT "${QUERY_PID}" >/dev/null 2>&1 || true
    wait "${QUERY_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${LOKI_PID}" ]]; then
    kill "${LOKI_PID}" >/dev/null 2>&1 || true
    wait "${LOKI_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

printf 'scenario\tname\tstart_utc\tend_utc\telapsed_ms\trc\texpected\n' >"${SUMMARY}"
RUN_START_ISO="$(now_iso)"
RUN_START_NS="$(now_ns)"

kubectl --context posthog-mw-dev -n monitoring port-forward svc/loki-logs-read "${LOKI_PORT}:3100" >"${ARTIFACT_DIR}/loki/port-forward.out" 2>"${ARTIFACT_DIR}/loki/port-forward.err" &
LOKI_PID=$!
sleep 2
curl -sS "${LOKI_URL}/ready" >"${ARTIFACT_DIR}/loki/ready.out" 2>"${ARTIFACT_DIR}/loki/ready.err" || true

cat >"${REPORT}" <<EOF
# One-Hour Long Query Boundary Run

- Run ID: \`${RUN_ID}\`
- Schema: \`${SCHEMA}\`
- DSN: \`${DSN}\`
- Started UTC: \`${RUN_START_ISO}\`
- Artifact directory: \`${ARTIFACT_DIR}\`

## Target

Run one long analytical self-join intended to stay active across 10m, 30m, and 60m boundaries, then cancel at 60m and verify a fresh query succeeds.

EOF

run_sql preflight connect "SELECT '${RUN_ID}:hour_boundary:preflight' AS marker, current_database(), current_user;"
run_sql setup create_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};"
run_sql setup create_events_14m "CREATE TABLE ${SCHEMA}.events AS
SELECT
  i AS event_id,
  i % 1000 AS user_id,
  TIMESTAMP '2026-01-01' + i * INTERVAL '1 second' AS ts,
  (i % 10000)::DOUBLE / 100.0 AS value
FROM generate_series(1, 14000000) AS t(i);
SELECT '${RUN_ID}:hour_boundary:setup_14m' AS marker, COUNT(*) AS rows FROM ${SCHEMA}.events;"

cat >"${ARTIFACT_DIR}/sql/hour_query_cancel_at_60m.sql" <<SQL
SELECT '${RUN_ID}:hour_boundary:cancel_at_60m' AS marker, a.user_id, SUM(a.value * b.value) AS score
FROM ${SCHEMA}.events a
JOIN ${SCHEMA}.events b ON a.user_id = b.user_id
WHERE a.event_id <= 14000000 AND b.event_id <= 14000000
GROUP BY a.user_id
ORDER BY score DESC
LIMIT 20;
SQL

QUERY_START_ISO="$(now_iso)"
QUERY_START_S="$(now_s)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/hour_query_cancel_at_60m.sql" >"${ARTIFACT_DIR}/outputs/hour_query_cancel_at_60m.out" 2>"${ARTIFACT_DIR}/outputs/hour_query_cancel_at_60m.err" &
QUERY_PID=$!

sleep 600
snapshot_activity after_10m

if kill -0 "${QUERY_PID}" >/dev/null 2>&1; then
  sleep 1200
  snapshot_activity after_30m
fi

if kill -0 "${QUERY_PID}" >/dev/null 2>&1; then
  sleep 1740
  snapshot_activity before_60m_cancel
fi

if kill -0 "${QUERY_PID}" >/dev/null 2>&1; then
  sleep 60
  kill -INT "${QUERY_PID}" >/dev/null 2>&1 || true
  wait "${QUERY_PID}"
  QUERY_RC=$?
  EXPECTED="expected-cancel"
else
  wait "${QUERY_PID}"
  QUERY_RC=$?
  EXPECTED="completed-before-60m"
fi

QUERY_END_ISO="$(now_iso)"
QUERY_ELAPSED_MS="$(( ($(now_s) - QUERY_START_S) * 1000 ))"
record boundary_hour hour_query_cancel_at_60m "${QUERY_START_ISO}" "${QUERY_END_ISO}" "${QUERY_ELAPSED_MS}" "${QUERY_RC}" "${EXPECTED}"
QUERY_PID=""

snapshot_activity after_query_end
run_sql boundary_hour post_boundary_select "SELECT '${RUN_ID}:hour_boundary:post_boundary_select' AS marker, 1 AS ok;"
run_sql cleanup drop_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;"

loki_query run_id '{namespace="duckgres"} |= "'"${RUN_ID}"'"'
loki_query hour_marker '{namespace="duckgres"} |= "'"${RUN_ID}:hour_boundary:cancel_at_60m"'"'
loki_query pressure '{namespace="duckgres"} |~ "ERROR|Canceled|cancel|unresponsive|timeout|worker|Failed to create session"'

{
  printf '\n## Completed\n\n'
  printf -- '- Finished UTC: `%s`\n' "$(now_iso)"
  printf -- '- Summary TSV: `%s`\n' "${SUMMARY}"
  printf -- '- SQL files: `%s/sql/`\n' "${ARTIFACT_DIR}"
  printf -- '- Outputs: `%s/outputs/`\n' "${ARTIFACT_DIR}"
  printf -- '- Activity snapshots: `%s/activity/`\n' "${ARTIFACT_DIR}"
  printf -- '- Loki output: `%s/loki/`\n\n' "${ARTIFACT_DIR}"
  printf '## Scenario Summary\n\n'
  printf '| Scenario | Name | Start UTC | End UTC | Elapsed ms | RC | Expected |\n'
  printf '|---|---|---:|---:|---:|---:|---|\n'
  tail -n +2 "${SUMMARY}" | while IFS=$'\t' read -r scenario name start end elapsed rc expected; do
    printf '| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` | `%s` |\n' "${scenario}" "${name}" "${start}" "${end}" "${elapsed}" "${rc}" "${expected}"
  done
  printf '\n## Loki Counts\n\n'
  printf -- '- RUN_ID matches: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/run_id.count" 2>/dev/null || printf unknown)"
  printf -- '- Hour marker matches: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/hour_marker.count" 2>/dev/null || printf unknown)"
  printf -- '- Pressure/error matches: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/pressure.count" 2>/dev/null || printf unknown)"
} >>"${REPORT}"

printf '%s\n' "${REPORT}"
}

suite="${1:-}"
case "${suite}" in
  long-boundaries)
    run_long_boundaries
    ;;
  hour-boundary)
    run_hour_boundary
    ;;
  all)
    "${BASH_SOURCE[0]}" long-boundaries
    "${BASH_SOURCE[0]}" hour-boundary
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
