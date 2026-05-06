#!/usr/bin/env bash
set -u

usage() {
  cat <<USAGE
Usage: bash scripts/dev-test-scenarios/run.sh <suite>

Suites:
  full             Run the full dev deploy PGWire scenario suite.
  long-boundaries  Run long-query boundary, cancel, and killed-client scenarios.
  hour-boundary    Run the one-hour long-query boundary scenario.
  all              Run full, long-boundaries, then hour-boundary in separate processes.
USAGE
}

run_full_suite() {

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SCHEMA="${SCHEMA:-duckgres_dev_scenarios_${RUN_ID}}"
DSN="${DSN:-host=perfdev.dw.dev.postwh.com port=5432 dbname=perfdev user=bill sslmode=require}"
export AWS_PROFILE="${AWS_PROFILE:-mw-dev}"
export PGPASSWORD="${PGPASSWORD:-dev-bill055}"

ROOT_DIR="$(pwd)"
ARTIFACT_DIR="${ROOT_DIR}/artifacts/dev-deploy-scenarios/${RUN_ID}"
REPORT="${ARTIFACT_DIR}/execution-report.md"
SUMMARY_TSV="${ARTIFACT_DIR}/summary.tsv"
mkdir -p "${ARTIFACT_DIR}"/{sql,outputs,loki,activity}

LOKI_PORT="${LOKI_PORT:-3102}"
LOKI_URL="http://127.0.0.1:${LOKI_PORT}"
LOKI_PID=""

psql_cmd=(psql "${DSN}" -v ON_ERROR_STOP=1)

json_escape() {
  jq -Rs . <"$1"
}

now_iso() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

now_ns() {
  printf '%s000000000' "$(date +%s)"
}

now_ms() {
  printf '%s000' "$(date +%s)"
}

elapsed_ms() {
  local start="$1"
  local end
  end="$(now_ms)"
  printf '%s' "$((end - start))"
}

record_summary() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"${SUMMARY_TSV}"
}

write_sql() {
  local name="$1"
  local sql="$2"
  printf '%s\n' "${sql}" >"${ARTIFACT_DIR}/sql/${name}.sql"
}

run_sql() {
  local scenario="$1"
  local name="$2"
  local sql="$3"
  local expected="${4:-pass}"
  local sql_file="${ARTIFACT_DIR}/sql/${name}.sql"
  local out_file="${ARTIFACT_DIR}/outputs/${name}.out"
  local err_file="${ARTIFACT_DIR}/outputs/${name}.err"
  local start_iso start_ms end_iso rc elapsed
  write_sql "${name}" "${sql}"
  start_iso="$(now_iso)"
  start_ms="$(now_ms)"
  "${psql_cmd[@]}" -f "${sql_file}" >"${out_file}" 2>"${err_file}"
  rc=$?
  elapsed="$(elapsed_ms "${start_ms}")"
  end_iso="$(now_iso)"
  record_summary "${scenario}" "${name}" "${start_iso}" "${end_iso}" "${elapsed}" "${rc}/${expected}"
  return "${rc}"
}

run_sql_allow_fail() {
  local scenario="$1"
  local name="$2"
  local sql="$3"
  run_sql "${scenario}" "${name}" "${sql}" "expected-failure"
  return 0
}

run_command() {
  local scenario="$1"
  local name="$2"
  shift 2
  local out_file="${ARTIFACT_DIR}/outputs/${name}.out"
  local err_file="${ARTIFACT_DIR}/outputs/${name}.err"
  local start_iso start_ms end_iso rc elapsed
  start_iso="$(now_iso)"
  start_ms="$(now_ms)"
  "$@" >"${out_file}" 2>"${err_file}"
  rc=$?
  elapsed="$(elapsed_ms "${start_ms}")"
  end_iso="$(now_iso)"
  record_summary "${scenario}" "${name}" "${start_iso}" "${end_iso}" "${elapsed}" "${rc}/pass"
  return "${rc}"
}

activity_snapshot() {
  local name="$1"
  "${psql_cmd[@]}" -At -F $'\t' -c "SELECT pid, datname, usename, state, worker_id, query FROM pg_stat_activity ORDER BY pid;" >"${ARTIFACT_DIR}/activity/${name}.tsv" 2>"${ARTIFACT_DIR}/activity/${name}.err"
}

activity_count() {
  "${psql_cmd[@]}" -At -c "SELECT pid FROM pg_stat_activity;" 2>/dev/null | sed '/^$/d' | wc -l | tr -d ' '
}

loki_query() {
  local name="$1"
  local query="$2"
  local start_ns="${3:-${RUN_START_NS}}"
  local end_ns="${4:-$(now_ns)}"
  curl -sS -G "${LOKI_URL}/loki/api/v1/query_range" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "limit=500" \
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

printf 'scenario\tname\tstart_utc\tend_utc\telapsed_ms\trc_expected\n' >"${SUMMARY_TSV}"

RUN_START_ISO="$(now_iso)"
RUN_START_NS="$(now_ns)"

cat >"${REPORT}" <<EOF
# Dev Deploy PGWire Scenario Execution

- Run ID: \`${RUN_ID}\`
- Schema: \`${SCHEMA}\`
- DSN: \`${DSN}\`
- AWS profile: \`${AWS_PROFILE}\`
- Started UTC: \`${RUN_START_ISO}\`
- Artifact directory: \`${ARTIFACT_DIR}\`

EOF

run_command preflight aws_identity aws sts get-caller-identity || true
run_command preflight network_check nc -vz perfdev.dw.dev.postwh.com 5432 || true
run_sql preflight psql_connect "SELECT '${RUN_ID}:preflight:connect' AS marker, 1 AS ok; SELECT current_database(), current_user;"

kubectl --context posthog-mw-dev -n monitoring port-forward "svc/loki-logs-read" "${LOKI_PORT}:3100" >"${ARTIFACT_DIR}/loki/port-forward.out" 2>"${ARTIFACT_DIR}/loki/port-forward.err" &
LOKI_PID=$!
sleep 2
run_command preflight loki_ready curl -sS "${LOKI_URL}/ready" || true

run_sql setup create_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};"
run_sql setup create_events "CREATE TABLE ${SCHEMA}.events AS
SELECT
  i AS event_id,
  i % 1000 AS user_id,
  CASE i % 5
    WHEN 0 THEN 'signup'
    WHEN 1 THEN 'pageview'
    WHEN 2 THEN 'click'
    WHEN 3 THEN 'purchase'
    ELSE 'logout'
  END AS event_type,
  TIMESTAMP '2026-01-01' + i * INTERVAL '1 second' AS ts,
  (i % 10000)::DOUBLE / 100.0 AS value
FROM generate_series(1, 1000000) AS t(i);
SELECT '${RUN_ID}:setup:events' AS marker, COUNT(*) AS rows FROM ${SCHEMA}.events;"
run_sql setup create_users "CREATE TABLE ${SCHEMA}.users AS
SELECT i AS user_id, 'user_' || i::VARCHAR AS name, i % 10 AS cohort
FROM generate_series(0, 999) AS t(i);
SELECT '${RUN_ID}:setup:users' AS marker, COUNT(*) AS rows FROM ${SCHEMA}.users;"

activity_snapshot baseline_before_scenarios

run_sql analytics analytics_aggregation "SELECT '${RUN_ID}:analytics:aggregation' AS marker, event_type, COUNT(*) AS events, SUM(value) AS total_value FROM ${SCHEMA}.events GROUP BY event_type ORDER BY event_type;"
run_sql analytics analytics_hourly "SELECT '${RUN_ID}:analytics:hourly' AS marker, date_trunc('hour', ts) AS hour, COUNT(*) AS events FROM ${SCHEMA}.events GROUP BY 2 ORDER BY 2 LIMIT 24;"
run_sql analytics analytics_high_cardinality "SELECT '${RUN_ID}:analytics:high_cardinality' AS marker, user_id, COUNT(*) AS events FROM ${SCHEMA}.events GROUP BY user_id ORDER BY events DESC, user_id LIMIT 20;"
run_sql analytics analytics_window "SELECT '${RUN_ID}:analytics:window' AS marker, user_id, total_value, RANK() OVER (ORDER BY total_value DESC) AS value_rank FROM (SELECT user_id, SUM(value) AS total_value FROM ${SCHEMA}.events GROUP BY user_id) ranked ORDER BY value_rank LIMIT 20;"
run_sql analytics analytics_join "SELECT '${RUN_ID}:analytics:join' AS marker, u.cohort, e.event_type, COUNT(*) AS events, SUM(e.value) AS total_value FROM ${SCHEMA}.events e JOIN ${SCHEMA}.users u USING (user_id) GROUP BY u.cohort, e.event_type ORDER BY u.cohort, e.event_type LIMIT 50;"

long_sql="SELECT '${RUN_ID}:long_query:aggregate_join' AS marker, a.user_id, SUM(a.value * b.value) AS score FROM ${SCHEMA}.events a JOIN ${SCHEMA}.events b ON a.user_id = b.user_id WHERE a.event_id <= 500000 AND b.event_id <= 500000 GROUP BY a.user_id ORDER BY score DESC LIMIT 20;"
write_sql long_query_active "${long_sql}"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/long_query_active.sql" >"${ARTIFACT_DIR}/outputs/long_query_active.out" 2>"${ARTIFACT_DIR}/outputs/long_query_active.err" &
LONG_PID=$!
LONG_START_ISO="$(now_iso)"
LONG_START_MS="$(now_ms)"
sleep 1
activity_snapshot long_query_during
run_sql long_query long_query_observer_select "SELECT '${RUN_ID}:long_query:observer' AS marker, 1 AS ok;"
wait "${LONG_PID}"
LONG_RC=$?
LONG_ELAPSED="$(elapsed_ms "${LONG_START_MS}")"
record_summary long_query long_query_active "${LONG_START_ISO}" "$(now_iso)" "${LONG_ELAPSED}" "${LONG_RC}/pass"
activity_snapshot long_query_after

run_concurrent_batch() {
  local n="$1"
  local scenario="concurrent_${n}"
  local start_iso start_ms rc elapsed
  local pids=()
  start_iso="$(now_iso)"
  start_ms="$(now_ms)"
  for i in $(seq 1 "${n}"); do
    cat >"${ARTIFACT_DIR}/sql/${scenario}_${i}.sql" <<SQL
SELECT '${RUN_ID}:concurrent:${n}:${i}' AS marker, user_id, COUNT(*) AS events, SUM(value) AS total_value
FROM ${SCHEMA}.events
WHERE user_id % ${n} = ${i} % ${n}
GROUP BY user_id
ORDER BY total_value DESC
LIMIT 10;
SQL
    "${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/${scenario}_${i}.sql" >"${ARTIFACT_DIR}/outputs/${scenario}_${i}.out" 2>"${ARTIFACT_DIR}/outputs/${scenario}_${i}.err" &
    pids+=("$!")
  done
  sleep 1
  activity_snapshot "${scenario}_during"
  rc=0
  for pid in "${pids[@]}"; do
    wait "${pid}" || rc=1
  done
  elapsed="$(elapsed_ms "${start_ms}")"
  record_summary concurrent "${scenario}" "${start_iso}" "$(now_iso)" "${elapsed}" "${rc}/pass"
}
run_concurrent_batch 5
run_concurrent_batch 10
run_concurrent_batch 25
activity_snapshot concurrent_after

QUICK_START="$(now_ms)"
QUICK_START_ISO="$(now_iso)"
QUICK_FAILURES=0
for i in $(seq 1 100); do
  marker="${RUN_ID}:quick_session:${i}"
  "${psql_cmd[@]}" -At -c "SELECT '${marker}' AS marker, ${i} AS iteration, current_database(), current_user;" >"${ARTIFACT_DIR}/outputs/quick_session_${i}.out" 2>"${ARTIFACT_DIR}/outputs/quick_session_${i}.err" || QUICK_FAILURES=$((QUICK_FAILURES + 1))
done
record_summary quick_sessions quick_sessions_100 "${QUICK_START_ISO}" "$(now_iso)" "$(elapsed_ms "${QUICK_START}")" "${QUICK_FAILURES}_failures/pass"

WORKER_REUSE_FILE="${ARTIFACT_DIR}/outputs/worker_reuse.tsv"
printf 'iteration\tactivity_snapshot\n' >"${WORKER_REUSE_FILE}"
for i in $(seq 1 10); do
  run_sql worker_reuse "worker_reuse_${i}" "SELECT '${RUN_ID}:worker_reuse:${i}' AS marker, ${i} AS iteration, COUNT(*) FROM ${SCHEMA}.events WHERE user_id = ${i};" || true
  "${psql_cmd[@]}" -At -F $'\t' -c "SELECT '${i}', pid, worker_id, state, query FROM pg_stat_activity ORDER BY pid;" >>"${WORKER_REUSE_FILE}" 2>>"${ARTIFACT_DIR}/outputs/worker_reuse.err" || true
  sleep 2
done

BASELINE_COUNT="$(activity_count)"
printf '%s\n' "${BASELINE_COUNT}" >"${ARTIFACT_DIR}/activity/session_cleanup_baseline.count"
SESSION_CLEANUP_START_ISO="$(now_iso)"
SESSION_CLEANUP_START="$(now_ms)"
SESSION_CLEANUP_PIDS=()
for i in $(seq 1 10); do
  { printf "SELECT '%s:session_cleanup:%s' AS marker, %s AS iteration;\n" "${RUN_ID}" "${i}" "${i}"; sleep 8; } | "${psql_cmd[@]}" >"${ARTIFACT_DIR}/outputs/session_cleanup_${i}.out" 2>"${ARTIFACT_DIR}/outputs/session_cleanup_${i}.err" &
  SESSION_CLEANUP_PIDS+=("$!")
done
sleep 2
activity_snapshot session_cleanup_during
for pid in "${SESSION_CLEANUP_PIDS[@]}"; do
  wait "${pid}" || true
done
for poll in $(seq 1 30); do
  count="$(activity_count)"
  printf '%s\t%s\t%s\n' "$(now_iso)" "${poll}" "${count}" >>"${ARTIFACT_DIR}/activity/session_cleanup_poll.tsv"
  [[ "${count}" -le "$((BASELINE_COUNT + 1))" ]] && break
  sleep 2
done
record_summary session_cleanup session_cleanup_10 "${SESSION_CLEANUP_START_ISO}" "$(now_iso)" "$(elapsed_ms "${SESSION_CLEANUP_START}")" "0/pass"

run_sql prepared prepared_statements "PREPARE event_count(int) AS SELECT COUNT(*) FROM ${SCHEMA}.events WHERE user_id = \$1;
EXECUTE event_count(42);
EXECUTE event_count(999);
DEALLOCATE event_count;
SELECT '${RUN_ID}:prepared:after_deallocate' AS marker, 1 AS ok;"

run_sql tx_error tx_rollback "BEGIN;
CREATE TABLE ${SCHEMA}.tx_tmp AS SELECT * FROM ${SCHEMA}.events LIMIT 10;
ROLLBACK;
SELECT '${RUN_ID}:tx:rollback_check' AS marker, COUNT(*) FROM information_schema.tables WHERE table_schema = '${SCHEMA}' AND table_name = 'tx_tmp';"
run_sql_allow_fail tx_error tx_missing_table "SELECT '${RUN_ID}:tx:missing_table' AS marker; SELECT * FROM definitely_missing_table;"
run_sql tx_error tx_after_error "SELECT '${RUN_ID}:tx:after_error' AS marker, 1 AS ok;"

run_sql temp_isolation temp_session_a_create "CREATE TEMP TABLE temp_events AS SELECT * FROM ${SCHEMA}.events LIMIT 10; SELECT '${RUN_ID}:temp:A' AS marker, COUNT(*) FROM temp_events;"
run_sql_allow_fail temp_isolation temp_session_b_cannot_see "SELECT '${RUN_ID}:temp:B' AS marker; SELECT COUNT(*) FROM temp_events;"
activity_snapshot temp_after

run_sql catalog catalog_info_schema_tables "SELECT '${RUN_ID}:catalog:tables' AS marker, COUNT(*) FROM information_schema.tables;"
run_sql catalog catalog_info_schema_columns "SELECT '${RUN_ID}:catalog:columns' AS marker, COUNT(*) FROM information_schema.columns WHERE table_schema = '${SCHEMA}';"
run_sql catalog catalog_pg_class "SELECT '${RUN_ID}:catalog:pg_class' AS marker, COUNT(*) FROM pg_catalog.pg_class;"
run_sql catalog catalog_pg_stat_activity "SELECT '${RUN_ID}:catalog:activity' AS marker, COUNT(*) FROM pg_stat_activity;"
run_sql catalog catalog_current "SELECT '${RUN_ID}:catalog:current' AS marker, current_schema(), current_database(), current_user;"
run_sql_allow_fail catalog catalog_has_table_privilege "SELECT '${RUN_ID}:catalog:privilege' AS marker, has_table_privilege('${SCHEMA}.events', 'SELECT');"
run_sql catalog catalog_backend_pid "SELECT '${RUN_ID}:catalog:pid' AS marker, pg_backend_pid();"

run_sql large_result large_sort "SELECT '${RUN_ID}:large:sort' AS marker, * FROM ${SCHEMA}.events ORDER BY value DESC, ts DESC LIMIT 1000;"
run_sql large_result large_stream "SELECT '${RUN_ID}:large:stream' AS marker, * FROM ${SCHEMA}.events LIMIT 100000;"
run_sql large_result large_after "SELECT '${RUN_ID}:large:after' AS marker, 1 AS ok;"

churn_long_sql="SELECT '${RUN_ID}:long_churn:heavy' AS marker, a.user_id, SUM(a.value * b.value) AS score FROM ${SCHEMA}.events a JOIN ${SCHEMA}.events b ON a.user_id = b.user_id WHERE a.event_id <= 500000 AND b.event_id <= 500000 GROUP BY a.user_id ORDER BY score DESC LIMIT 20;"
write_sql long_churn_heavy "${churn_long_sql}"
CHURN_START_ISO="$(now_iso)"
CHURN_START="$(now_ms)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/long_churn_heavy.sql" >"${ARTIFACT_DIR}/outputs/long_churn_heavy.out" 2>"${ARTIFACT_DIR}/outputs/long_churn_heavy.err" &
CHURN_LONG_PID=$!
CHURN_PIDS=("${CHURN_LONG_PID}")
for i in $(seq 1 100); do
  "${psql_cmd[@]}" -At -c "SELECT '${RUN_ID}:churn_session:${i}' AS marker, ${i} AS iteration;" >"${ARTIFACT_DIR}/outputs/churn_session_${i}.out" 2>"${ARTIFACT_DIR}/outputs/churn_session_${i}.err" &
  CHURN_PIDS+=("$!")
  sleep 0.1
done
sleep 1
activity_snapshot long_churn_during
CHURN_RC=0
for pid in "${CHURN_PIDS[@]}"; do
  wait "${pid}" || CHURN_RC=1
done
record_summary long_churn long_query_plus_100_churn "${CHURN_START_ISO}" "$(now_iso)" "$(elapsed_ms "${CHURN_START}")" "${CHURN_RC}/pass"
activity_snapshot long_churn_after

write_sql cancel_interrupt "SELECT '${RUN_ID}:cancel:ctrl_c' AS marker, COUNT(*) FROM ${SCHEMA}.events a, ${SCHEMA}.events b;"
CANCEL_START_ISO="$(now_iso)"
CANCEL_START="$(now_ms)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/cancel_interrupt.sql" >"${ARTIFACT_DIR}/outputs/cancel_interrupt.out" 2>"${ARTIFACT_DIR}/outputs/cancel_interrupt.err" &
CANCEL_PID=$!
sleep 2
activity_snapshot cancel_during
kill -INT "${CANCEL_PID}" >/dev/null 2>&1 || true
wait "${CANCEL_PID}"
CANCEL_RC=$?
record_summary cancel cancel_ctrl_c_simulated "${CANCEL_START_ISO}" "$(now_iso)" "$(elapsed_ms "${CANCEL_START}")" "${CANCEL_RC}/expected-cancel"
activity_snapshot cancel_after

write_sql killed_client "SELECT '${RUN_ID}:cancel:killed_client' AS marker, COUNT(*) FROM ${SCHEMA}.events a, ${SCHEMA}.events b;"
KILL_START_ISO="$(now_iso)"
KILL_START="$(now_ms)"
"${psql_cmd[@]}" -f "${ARTIFACT_DIR}/sql/killed_client.sql" >"${ARTIFACT_DIR}/outputs/killed_client.out" 2>"${ARTIFACT_DIR}/outputs/killed_client.err" &
KILLED_PID=$!
sleep 2
activity_snapshot killed_client_during
kill -TERM "${KILLED_PID}" >/dev/null 2>&1 || true
wait "${KILLED_PID}"
KILLED_RC=$?
record_summary cancel killed_client "${KILL_START_ISO}" "$(now_iso)" "$(elapsed_ms "${KILL_START}")" "${KILLED_RC}/expected-kill"
activity_snapshot killed_client_after
run_sql cancel cancel_after "SELECT '${RUN_ID}:cancel:after' AS marker, 1 AS ok;"

IDLE_START_ISO="$(now_iso)"
IDLE_START="$(now_ms)"
IDLE_PIDS=()
for i in $(seq 1 20); do
  { printf "SELECT '%s:idle_pressure:%s' AS marker, %s AS iteration;\n" "${RUN_ID}" "${i}" "${i}"; sleep 180; } | "${psql_cmd[@]}" >"${ARTIFACT_DIR}/outputs/idle_pressure_${i}.out" 2>"${ARTIFACT_DIR}/outputs/idle_pressure_${i}.err" &
  IDLE_PIDS+=("$!")
  echo "$!" >>"${ARTIFACT_DIR}/outputs/idle_pressure_pids.txt"
done
sleep 5
activity_snapshot idle_pressure_during
run_sql idle_pressure idle_pressure_new_session "SELECT '${RUN_ID}:idle_pressure:new_session' AS marker, 1 AS ok;"
sleep 10
while read -r pid; do
  kill -TERM "${pid}" >/dev/null 2>&1 || true
done <"${ARTIFACT_DIR}/outputs/idle_pressure_pids.txt"
for pid in "${IDLE_PIDS[@]}"; do
  wait "${pid}" || true
done
for poll in $(seq 1 30); do
  count="$(activity_count)"
  printf '%s\t%s\t%s\n' "$(now_iso)" "${poll}" "${count}" >>"${ARTIFACT_DIR}/activity/idle_pressure_poll.tsv"
  [[ "${count}" -le "$((BASELINE_COUNT + 1))" ]] && break
  sleep 2
done
record_summary idle_pressure idle_pressure_20 "$(printf '%s' "${IDLE_START_ISO}")" "$(now_iso)" "$(elapsed_ms "${IDLE_START}")" "0/pass"

NEG_START_ISO="$(now_iso)"
NEG_START="$(now_ms)"
PGPASSWORD='wrong-password' psql "${DSN}" -v ON_ERROR_STOP=1 -c "SELECT '${RUN_ID}:negative_auth:wrong_password' AS marker, 1;" >"${ARTIFACT_DIR}/outputs/negative_wrong_password.out" 2>"${ARTIFACT_DIR}/outputs/negative_wrong_password.err"
NEG_WRONG_RC=$?
psql "host=perfdev.dw.dev.postwh.com port=5432 dbname=missing_db user=bill sslmode=require" -v ON_ERROR_STOP=1 -c "SELECT '${RUN_ID}:negative_auth:missing_db' AS marker, 1;" >"${ARTIFACT_DIR}/outputs/negative_missing_db.out" 2>"${ARTIFACT_DIR}/outputs/negative_missing_db.err"
NEG_DB_RC=$?
run_sql negative_auth negative_valid_after "SELECT '${RUN_ID}:negative_auth:valid_after' AS marker, 1 AS ok;"
record_summary negative_auth wrong_password_and_missing_db "${NEG_START_ISO}" "$(now_iso)" "$(elapsed_ms "${NEG_START}")" "wrong=${NEG_WRONG_RC},db=${NEG_DB_RC}/expected-failure"

activity_snapshot before_cleanup
run_sql cleanup drop_schema "DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;"
activity_snapshot after_cleanup

RUN_END_NS="$(now_ns)"
RUN_END_ISO="$(now_iso)"
loki_query run_id '{namespace="duckgres"} |= "'"${RUN_ID}"'"' "${RUN_START_NS}" "${RUN_END_NS}"
loki_query user_bill '{namespace="duckgres"} |= "user=bill"' "${RUN_START_NS}" "${RUN_END_NS}"
loki_query pressure '{namespace="duckgres"} |~ "queue|timeout|capacity|spawn|pre-warm|ERROR|panic"' "${RUN_START_NS}" "${RUN_END_NS}"

{
  printf '\n## Completed\n\n'
  printf -- '- Finished UTC: `%s`\n' "${RUN_END_ISO}"
  printf -- '- Summary TSV: `%s`\n' "${SUMMARY_TSV}"
  printf -- '- Raw psql outputs: `%s/outputs/`\n' "${ARTIFACT_DIR}"
  printf -- '- SQL files: `%s/sql/`\n' "${ARTIFACT_DIR}"
  printf -- '- Activity snapshots: `%s/activity/`\n' "${ARTIFACT_DIR}"
  printf -- '- Loki outputs: `%s/loki/`\n\n' "${ARTIFACT_DIR}"
  printf '## Scenario Summary\n\n'
  printf '| Scenario | Name | Start UTC | End UTC | Elapsed ms | Result |\n'
  printf '|---|---|---:|---:|---:|---|\n'
  tail -n +2 "${SUMMARY_TSV}" | while IFS=$'\t' read -r scenario name start end elapsed result; do
    printf '| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` |\n' "${scenario}" "${name}" "${start}" "${end}" "${elapsed}" "${result}"
  done
  printf '\n## Key Evidence\n\n'
  printf -- '- Baseline pg_stat_activity count: `%s`\n' "${BASELINE_COUNT}"
  printf -- '- Loki RUN_ID match count: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/run_id.count" 2>/dev/null || printf unknown)"
  printf -- '- Loki user=bill match count: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/user_bill.count" 2>/dev/null || printf unknown)"
  printf -- '- Loki pressure/error match count: `%s`\n' "$(cat "${ARTIFACT_DIR}/loki/pressure.count" 2>/dev/null || printf unknown)"
  printf '\n## Notes\n\n'
  printf -- '- Expected-failure scenarios include missing table, temp table isolation from a different session, unsupported catalog privilege if not implemented, cancel, killed client, wrong password, and missing database.\n'
  printf -- '- Inspect individual `.err` files for exact error strings where `rc_expected` is not `0/pass`.\n'
} >>"${REPORT}"

printf '%s\n' "${REPORT}"
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
  full)
    run_full_suite
    ;;
  long-boundaries)
    run_long_boundaries
    ;;
  hour-boundary)
    run_hour_boundary
    ;;
  all)
    "${BASH_SOURCE[0]}" full
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
