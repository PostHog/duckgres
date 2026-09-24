#!/bin/sh
# Engine-specific Trino e2e lane. The coordinator, worker, OPA bundle, catalog
# store, credentials, and cell id all belong to this PR namespace.
set -eu
trap 'rc=$?; [ "$rc" = 0 ] || echo "TRINO HARNESS EXIT rc=$rc" >&2' EXIT

API="${CP_API:?}"
SECRET="${INTERNAL_SECRET:?}"
PR="${PR_NUMBER:?}"
NS="${NAMESPACE:?}"
H="X-Duckgres-Internal-Secret: $SECRET"
TRINO=""
CA=/trino-ca/ca.crt
ORG_A="ci-pr-${PR}-trinoa"
ORG_B="ci-pr-${PR}-trinob"
DB_A="trino-a-${PR}"
DB_B="trino-b-${PR}"
CAT_A="org_$(printf %s "$DB_A" | tr '-' '_')"
CAT_B="org_$(printf %s "$DB_B" | tr '-' '_')"
TEAM_A=93001
TEAM_B=93002
HOGLAKE="${HOGLAKE_URI:?}"
# Password rotation crosses the 10s provisioner reconcile, kubelet's
# eventually-consistent Secret-volume projection, and Trino's 5s file reload.
# Keep the total window above the expected projection delay while avoiding a
# tight authentication loop against the coordinator.
TRINO_AUTH_ROTATION_ATTEMPTS=36
TRINO_AUTH_ROTATION_RETRY_SECONDS=5

fail() { echo "FAIL: $*" >&2; exit 1; }
log() { echo ">>> $*" >&2; }
apk add --no-cache curl jq >/dev/null 2>&1
[ -s "$CA" ] || fail "per-run Trino CA is not mounted"
KUBECTL=/tmp/kubectl
KUBECTL_VERSION=v1.33.1
curl -fsSLo "$KUBECTL" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/arm64/kubectl"
chmod +x "$KUBECTL"
"$KUBECTL" version --client >/dev/null || fail "pinned kubectl bootstrap failed"

api() { curl --connect-timeout 5 --max-time 60 -fsS -H "$H" "$@"; }


provision() { # org db team
  api -X POST -H 'Content-Type: application/json' \
    -d '{"database_name":"'"$2"'","team_id":'"$3"',"metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true},"trino":{"enabled":true,"tier":"free"}}' \
    "$API/api/v1/orgs/$1/provision"
}

wait_warehouse() { # org
  i=0
  while [ "$i" -lt 240 ]; do
    state="$(api "$API/api/v1/orgs/$1/warehouse/status" | jq -r '.state // empty' 2>/dev/null || true)"
    [ "$state" = ready ] && return 0
    [ "$state" = failed ] && fail "$1 warehouse failed: $(api "$API/api/v1/orgs/$1/warehouse/status")"
    sleep 5; i=$((i + 1))
  done
  fail "$1 warehouse did not become ready"
}

wait_trino() { # org expected-principal expected-catalog
  i=0
  while [ "$i" -lt 180 ]; do
    body="$(api "$API/api/v1/orgs/$1/trino" 2>/dev/null || true)"
    state="$(printf %s "$body" | jq -r '.status.state // empty' 2>/dev/null || true)"
    if [ "$state" = ready ]; then
      printf %s "$body" | jq -e --arg p "$2" --arg c "$3" --arg cell legacy --arg host "duckgres-trino.$NS.svc" \
        '.enabled == true and .available == true and .status.principal == $p and .status.catalog == $c and .status.cell == $cell and .status.tier == "free" and .status.connection.host == $host and .status.connection.port == 8443 and .status.connection.username == $p and (.status.connection | has("password") | not)' >/dev/null \
        || fail "$1 Trino status identity mismatch: $body"
      api "$API/api/v1/orgs/$1" | jq -e --arg stored_cell "ci-pr-$PR" \
        '.trino.trino_cell_id == $stored_cell and .trino.backend == "hoglake"' >/dev/null \
        || fail "$1 legacy API identity changed persisted Trino ownership"
      TRINO="https://$(printf %s "$body" | jq -r '.status.connection.host'):$(printf %s "$body" | jq -r '.status.connection.port')"
      return 0
    fi
    [ "$state" = failed ] && fail "$1 Trino provisioning failed: $body"
    sleep 5; i=$((i + 1))
  done
  fail "$1 Trino catalog did not become ready"
}

# Print all result rows as compact JSON. Trino's statement protocol pages via
# nextUri; every follow-up keeps both Basic auth and the tenant identity.
# TRINO_HOST, when set, is sent as the Host header: the tenant host name a
# host-qualified login is resolved against (TLS still verifies the coordinator).
trino_query() { # principal password sql
  principal="$1" password="$2" sql="$3"
  set -- -H "X-Trino-User: $principal" -H 'X-Trino-Time-Zone: UTC'
  [ -z "${TRINO_HOST:-}" ] || set -- "$@" -H "Host: $TRINO_HOST"
  response="$(curl --connect-timeout 5 --max-time 60 --cacert "$CA" -fsS --user "$principal:$password" \
    "$@" --data-binary "$sql" "$TRINO/v1/statement")" || return 1
  rows='[]'
  while :; do
    err="$(printf %s "$response" | jq -r '.error.message // empty')"
    if [ -n "$err" ]; then
      echo "$err" >&2
      # Show bounded exception classes, not nested messages, SQL, URLs, or credentials.
      printf %s "$response" | jq -c '
        def matched($pattern): if type == "string" and test($pattern) then . else null end;
        {
          queryId: (.id | matched("^[0-9]{8}_[0-9]{6}_[0-9]+_[a-z0-9]+$")),
          errorName: (.error.errorName | matched("^[A-Z][A-Z0-9_]{0,127}$")),
          errorType: (.error.errorType | matched("^[A-Z][A-Z0-9_]{0,63}$")),
          errorCode: (.error.errorCode | if type == "number" then . else null end),
          causeTypes: [limit(12; .error.failureInfo | recurse(.cause // empty) |
            .type | matched("^[A-Za-z_$][A-Za-z0-9_.$]{0,255}$") | select(. != null))]
        }' >&2 2>/dev/null || true
      return 1
    fi
    rows="$(printf %s "$response" | jq -c --argjson rows "$rows" '$rows + (.data // [])')"
    next="$(printf %s "$response" | jq -r '.nextUri // empty')"
    [ -n "$next" ] || break
    response="$(curl --connect-timeout 5 --max-time 60 --cacert "$CA" -fsS --user "$principal:$password" \
      "$@" "$next")" || return 1
  done
  printf '%s\n' "$rows"
}

scalar() { trino_query "$1" "$2" "$3" | jq -r '.[0][0]'; }
must_fail() { # principal password sql pattern
  out="$(trino_query "$1" "$2" "$3" 2>&1)" && fail "query unexpectedly succeeded: $3"
  printf %s "$out" | grep -Eqi "$4" || fail "query failed for wrong reason: $out"
}

# BEGIN concurrent bootstrap helpers
bootstrap_scope() {
  case "$PR" in ''|*[!0-9]*) fail "bootstrap requires a numeric fixture identity" ;; esac
  [ "$NS" = "duckgres-ci-pr-$PR" ] || fail "bootstrap requires the exact PR namespace"
}

bootstrap_pods_healthy() {
  jq -e --argjson count "$1" --argjson old "$2" '
    (.items | length) == $count and all(.items[];
      .metadata.deletionTimestamp == null and
      (.metadata.uid as $uid | ($old | index($uid)) == null) and
      any(.status.conditions[]?; .type == "Ready" and .status == "True") and
      ([.status.containerStatuses[]? | select(.name == "controlplane")] | length) == 1 and
      all(.status.containerStatuses[]?; .ready == true and .restartCount == 0))' >/dev/null
}

bootstrap_remove_pairs_patch() {
  jq -ce '
    ["admin-password", "admin-password-hash", "observer-password", "observer-password-hash"] as $keys |
    . as $secret |
    if (.metadata.resourceVersion | type) != "string" or .metadata.resourceVersion == "" or
       any($keys[]; . as $key | ($secret.data[$key] | type) != "string" or $secret.data[$key] == "") then error("invalid fixture secret") else
      [{op:"test",path:"/metadata/resourceVersion",value:.metadata.resourceVersion}] +
      [$keys[] | {op:"remove",path:("/data/" + .)}]
    end'
}

bootstrap_pair_fingerprint() (
  bootstrap_secret="$("$KUBECTL" -n "$NS" get secret trino-auth -o json)" || return 1
  bootstrap_pairs="$(printf %s "$bootstrap_secret" | jq -ceS '
    .data | {"admin-password":.["admin-password"], "admin-password-hash":.["admin-password-hash"],
      "observer-password":.["observer-password"], "observer-password-hash":.["observer-password-hash"]} |
    if all(.[]; type == "string" and length > 0) then . else error("missing credential pair") end')" || return 1
  printf %s "$bootstrap_pairs" | sha256sum | awk '{print $1}'
)

bootstrap_fixed_secrets_fingerprint() (
  bootstrap_secrets="$("$KUBECTL" -n "$NS" get secret trino-internal-communication trino-opa-bundle-token -o json)" || return 1
  bootstrap_fixed="$(printf %s "$bootstrap_secrets" | jq -ceS '
    [.items[] | {name:.metadata.name,data:.data}] | sort_by(.name) |
    if length == 2 and all(.[]; (.data | type) == "object" and (.data | length) > 0)
    then . else error("missing fixed cluster credentials") end')" || return 1
  printf %s "$bootstrap_fixed" | sha256sum | awk '{print $1}'
)

bootstrap_wait_pods() {
  bootstrap_wait_attempt=0
  while [ "$bootstrap_wait_attempt" -lt 60 ]; do
    if "$KUBECTL" -n "$NS" get pods -l app=duckgres-control-plane -o json |
        bootstrap_pods_healthy "$1" "$bootstrap_old_uids"; then
      return 0
    fi
    sleep 3
    bootstrap_wait_attempt=$((bootstrap_wait_attempt + 1))
  done
  fail "concurrent bootstrap did not produce the required ready, restart-free replicas"
}
# END concurrent bootstrap helpers

log "concurrent credential bootstrap in the isolated fixture"
bootstrap_scope
"$KUBECTL" -n "$NS" get deployment duckgres-control-plane -o json |
  jq -e --arg ns "$NS" '.metadata.namespace == $ns and .metadata.name == "duckgres-control-plane" and
    .spec.replicas == 1 and .spec.selector.matchLabels.app == "duckgres-control-plane" and
    .spec.template.metadata.labels.app == "duckgres-control-plane"' >/dev/null || fail "unexpected bootstrap fixture deployment"
bootstrap_old_pods_json="$("$KUBECTL" -n "$NS" get pods -l app=duckgres-control-plane -o json)"
bootstrap_old_uids="$(printf %s "$bootstrap_old_pods_json" | jq -ce '[.items[].metadata.uid] | select(length > 0)')"
bootstrap_old_names="$(printf %s "$bootstrap_old_pods_json" | jq -r '.items[] | "pod/" + .metadata.name')"
"$KUBECTL" -n "$NS" patch deployment duckgres-control-plane --type=merge -p '{"spec":{"replicas":0}}' >/dev/null
# Expand only the names returned by the fixture's pod list.
# shellcheck disable=SC2086
"$KUBECTL" -n "$NS" wait --for=delete $bootstrap_old_names --timeout=180s >/dev/null
"$KUBECTL" -n "$NS" get pods -l app=duckgres-control-plane -o json |
  jq -e '.items | length == 0' >/dev/null || fail "old fixture controllers still exist"
bootstrap_fixed_fingerprint="$(bootstrap_fixed_secrets_fingerprint)"
bootstrap_auth_before="$("$KUBECTL" -n "$NS" get secret trino-auth -o json)"
bootstrap_patch="$(printf %s "$bootstrap_auth_before" | bootstrap_remove_pairs_patch)"
"$KUBECTL" -n "$NS" patch secret trino-auth --type=json -p "$bootstrap_patch" >/dev/null
bootstrap_other_data="$(printf %s "$bootstrap_auth_before" | jq -cS '.data | del(.["admin-password"],.["admin-password-hash"],.["observer-password"],.["observer-password-hash"])')"
[ "$("$KUBECTL" -n "$NS" get secret trino-auth -o json | jq -cS '.data')" = "$bootstrap_other_data" ] || fail "bootstrap modified unrelated Secret keys"
unset bootstrap_auth_before bootstrap_other_data bootstrap_patch
"$KUBECTL" -n "$NS" patch deployment duckgres-control-plane --type=merge -p '{"spec":{"replicas":3}}' >/dev/null
bootstrap_wait_pods 3
bootstrap_initial_fingerprint="$(bootstrap_pair_fingerprint)"
bootstrap_auth="$("$KUBECTL" -n "$NS" get secret trino-auth -o json)"
bootstrap_admin="$(printf %s "$bootstrap_auth" | jq -r '.data["admin-password"] | @base64d')"
bootstrap_observer="$(printf %s "$bootstrap_auth" | jq -r '.data["observer-password"] | @base64d')"
unset bootstrap_auth
TRINO="https://duckgres-trino.$NS.svc:8443"
bootstrap_auth_attempt=0
bootstrap_authenticated=false
while [ "$bootstrap_auth_attempt" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  if trino_query __admin_provisioner "$bootstrap_admin" 'SHOW CATALOGS' >/dev/null 2>&1 &&
      trino_query __duckgres_observer "$bootstrap_observer" 'SELECT count(*) FROM system.runtime.nodes' >/dev/null 2>&1; then
    bootstrap_authenticated=true
    break
  fi
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"
  bootstrap_auth_attempt=$((bootstrap_auth_attempt + 1))
done
unset bootstrap_admin bootstrap_observer
[ "$bootstrap_authenticated" = true ] || fail "concurrent bootstrap credentials did not authenticate against Trino"
bootstrap_wait_pods 3
[ "$(bootstrap_pair_fingerprint)" = "$bootstrap_initial_fingerprint" ] || fail "credential pairs changed after concurrent startup"
[ "$(bootstrap_fixed_secrets_fingerprint)" = "$bootstrap_fixed_fingerprint" ] || fail "bootstrap rotated fixed cluster credentials"
"$KUBECTL" -n "$NS" patch deployment duckgres-control-plane --type=merge -p '{"spec":{"replicas":1}}' >/dev/null
bootstrap_wait_pods 1
[ "$(bootstrap_pair_fingerprint)" = "$bootstrap_initial_fingerprint" ] || fail "credential pairs changed after scale-down"

log "provisioning first Trino tenant"
pw_a="$(provision "$ORG_A" "$DB_A" "$TEAM_A" | jq -r .password)"
[ -n "$pw_a" ] && [ "$pw_a" != null ] || fail "tenant A provision returned no password"
wait_warehouse "$ORG_A"
wait_trino "$ORG_A" "$DB_A" "$CAT_A"

log "TLS/password auth, discovery, and DDL/DML"
[ "$(scalar "$DB_A" "$pw_a" 'SELECT 1')" = 1 ] || fail "Trino SELECT 1 failed"
[ "$(bootstrap_pair_fingerprint)" = "$bootstrap_initial_fingerprint" ] || fail "tenant provisioning replaced bootstrap credential pairs"
must_fail "$DB_A" definitely-wrong-password 'SELECT 1' '401|Unauthorized|Authentication|credentials'
must_fail "$ORG_A" "$pw_a" 'SELECT 1' '401|Unauthorized|Authentication|credentials'
catalogs="$(trino_query "$DB_A" "$pw_a" 'SHOW CATALOGS')"
printf %s "$catalogs" | jq -e --arg c "$CAT_A" 'any(.[]; .[0] == $c)' >/dev/null || fail "own catalog absent: $catalogs"
printf %s "$catalogs" | jq -e --arg c "$CAT_B" 'all(.[]; .[0] != $c)' >/dev/null || fail "foreign catalog visible before tenant B exists"
table="e2e_trino_${PR}"
schema=main
writes="${table}_writes"
scratch="${table}_ctas"
schemas="$(trino_query "$DB_A" "$pw_a" "SHOW SCHEMAS FROM $CAT_A")"
printf %s "$schemas" | jq -e --arg s "$schema" 'any(.[]; .[0] == $s)' >/dev/null || fail "managed namespace absent"
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$table (id BIGINT, flag BOOLEAN, amount DECIMAL(38,2), label VARCHAR, event_date DATE)" >/dev/null
trino_query "$DB_A" "$pw_a" "INSERT INTO $CAT_A.$schema.$table VALUES (2, false, DECIMAL '123456789012345678901234.56', 'two', DATE '2026-09-01')" >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT concat(cast(flag AS varchar), '|', cast(amount AS varchar), '|', label, '|', cast(event_date AS varchar)) FROM $CAT_A.$schema.$table")" = "false|123456789012345678901234.56|two|2026-09-01" ] || fail "Hoglake typed write/read mismatch"
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$scratch AS SELECT * FROM $CAT_A.$schema.$table" >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT count(*) FROM $CAT_A.$schema.$scratch")" = 1 ] || fail "Hoglake CTAS lost rows"
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$writes (id INTEGER, amount DECIMAL(38,2))" >/dev/null
pids=""
for id in 1 2 3 4 5 6 7 8; do
  trino_query "$DB_A" "$pw_a" "INSERT INTO $CAT_A.$schema.$writes VALUES ($id, DECIMAL '123456789012345678901234.56')" >/dev/null & pids="$pids $!"
done
rc=0; for pid in $pids; do wait "$pid" || rc=1; done
[ "$rc" = 0 ] || fail "a concurrent Hoglake write failed"
[ "$(scalar "$DB_A" "$pw_a" "SELECT count(*) FROM $CAT_A.$schema.$writes")" = 8 ] || fail "concurrent Hoglake writes lost rows"
before_rows="$(trino_query "$DB_A" "$pw_a" "SELECT id, CAST(amount AS VARCHAR) FROM $CAT_A.$schema.$writes ORDER BY id")"
files_uri="$HOGLAKE/v1/catalogs/$ORG_A/namespaces/$schema/tables/$writes/scan"
file_count_before="$(curl -fsS "$files_uri" | jq length)"
[ "$file_count_before" -ge 8 ] || fail "compaction fixture did not create separate files"
curl --connect-timeout 5 --max-time 120 -fsS -X POST "$HOGLAKE/v1/catalogs/$ORG_A/maintenance/compact?batch=100" >/dev/null
file_count_after="$(curl -fsS "$files_uri" | jq length)"
[ "$file_count_after" -lt "$file_count_before" ] || fail "compaction did not reduce file_count"
[ "$(trino_query "$DB_A" "$pw_a" "SELECT id, CAST(amount AS VARCHAR) FROM $CAT_A.$schema.$writes ORDER BY id")" = "$before_rows" ] || fail "compaction changed wide decimal rows"

log "hot-add second tenant without restarting coordinator"
coord_uid_before="$("$KUBECTL" -n "$NS" get pod -l 'app=duckgres-trino,component=coordinator' -o jsonpath='{.items[0].metadata.uid}')"
pw_b="$(provision "$ORG_B" "$DB_B" "$TEAM_B" | jq -r .password)"
[ -n "$pw_b" ] && [ "$pw_b" != null ] || fail "tenant B provision returned no password"
wait_warehouse "$ORG_B"
wait_trino "$ORG_B" "$DB_B" "$CAT_B"
if [ "${TRINO_SERVICE_CREDENTIALS_ENABLED:-false}" = true ]; then
  . /harness/trino-service-credentials.sh
fi
[ "$("$KUBECTL" -n "$NS" get pod -l 'app=duckgres-trino,component=coordinator' -o jsonpath='{.items[0].metadata.uid}')" = "$coord_uid_before" ] \
  || fail "adding tenant B restarted the Trino coordinator"
[ "$(scalar "$DB_B" "$pw_b" 'SELECT 1')" = 1 ] || fail "hot-added tenant cannot authenticate"
admin_pw="$("$KUBECTL" -n "$NS" get secret trino-auth -o go-template='{{index .data "admin-password"}}' | base64 -d)"
[ -n "$admin_pw" ] || fail "trino-auth has no admin-password"
admin_catalogs="$(trino_query __admin_provisioner "$admin_pw" 'SHOW CATALOGS')"
printf %s "$admin_catalogs" | jq -e --arg a "$CAT_A" --arg b "$CAT_B" \
  'any(.[]; .[0] == $a) and any(.[]; .[0] == $b)' >/dev/null \
  || fail "admin cannot see both hot-added managed catalogs: $admin_catalogs"

log "OPA tenant isolation and batched metadata filtering"
catalogs_b="$(trino_query "$DB_B" "$pw_b" 'SHOW CATALOGS')"
printf %s "$catalogs_b" | jq -e --arg own "$CAT_B" --arg foreign "$CAT_A" \
  'any(.[]; .[0] == $own) and all(.[]; .[0] != $foreign)' >/dev/null || fail "tenant B catalog filter mismatch: $catalogs_b"
# information_schema exercises the OPA batched filter path, not just SHOW CATALOGS.
trino_query "$DB_A" "$pw_a" "SELECT table_name FROM $CAT_A.information_schema.tables" >/dev/null
foreign_table="${table}_tenant_b"
trino_query "$DB_B" "$pw_b" "CREATE TABLE $CAT_B.main.$foreign_table (id INTEGER)" >/dev/null
trino_query "$DB_B" "$pw_b" "INSERT INTO $CAT_B.main.$foreign_table VALUES 7" >/dev/null
must_fail "$DB_A" "$pw_a" "SELECT * FROM $CAT_B.main.$foreign_table" 'denied|access|catalog|not found|does not exist'
must_fail "$DB_A" "$pw_a" "INSERT INTO $CAT_B.main.$foreign_table VALUES 8" 'denied|access|catalog|not found|does not exist'
must_fail "$DB_A" "$pw_a" "ALTER TABLE $CAT_B.main.$foreign_table RENAME TO ${foreign_table}_renamed" 'denied|access|catalog|not found|does not exist'
must_fail "$DB_A" "$pw_a" "DROP TABLE $CAT_B.main.$foreign_table" 'denied|access|catalog|not found|does not exist'
[ "$(scalar "$DB_B" "$pw_b" "SELECT count(*) FROM $CAT_B.main.$foreign_table")" = 1 ] || fail "cross-tenant attempts changed tenant B data"
must_fail "$DB_B" "$pw_b" "SELECT * FROM $CAT_A.$schema.$table" 'denied|access|catalog|not found|does not exist'
must_fail "$DB_B" "$pw_b" "DROP TABLE $CAT_A.$schema.$table" 'denied|access|catalog|not found|does not exist'
[ "$(scalar "$DB_A" "$pw_a" "SELECT count(*) FROM $CAT_A.$schema.$table")" = 1 ] || fail "cross-tenant attempts changed tenant A data"

log "admin Trino fleet/org/query surfaces"
api "$API/api/v1/trino/status" | jq -e --arg cell legacy '.available == true and .cell.id == $cell' >/dev/null
api "$API/api/v1/trino/nodes" | jq -e '.available == true and (.nodes | length) >= 2' >/dev/null
api "$API/api/v1/trino/orgs" | jq -e --arg a "$ORG_A" --arg b "$ORG_B" \
  'any(.orgs[]; .org == $a and .state == "ready" and .cell == "legacy") and any(.orgs[]; .org == $b and .state == "ready" and .cell == "legacy")' >/dev/null
queries="$(api "$API/api/v1/trino/queries?org=$ORG_A")"
printf %s "$queries" | jq -e --arg a "$ORG_A" --arg table "$table" \
  'all(.queries[]; .org == $a) and any(.queries[]; .query | contains($table))' >/dev/null \
  || fail "admin query list is unscoped or missing tenant A SQL: $queries"

log "tenant query visibility and audited admin kill"
kill_out=/tmp/trino-kill-query.out
( trino_query "$DB_A" "$pw_a" \
    'SELECT count(*) FROM UNNEST(sequence(1, 10000)) a(x) CROSS JOIN UNNEST(sequence(1, 10000)) b(y) CROSS JOIN UNNEST(sequence(1, 100)) c(z) WHERE random() >= 0' \
    >"$kill_out" 2>&1 ) & kill_pid=$!
query_id=""; i=0
while [ "$i" -lt 30 ]; do
  query_id="$(api "$API/api/v1/trino/queries?org=$ORG_A&active=1" \
    | jq -r '.queries[0].query_id // empty')"
  [ -n "$query_id" ] && break
  kill -0 "$kill_pid" 2>/dev/null || break
  sleep 1; i=$((i + 1))
done
[ -n "$query_id" ] || { wait "$kill_pid" 2>/dev/null || true; fail "long Trino query never appeared in admin live queries: $(cat "$kill_out")"; }
api "$API/api/v1/trino/queries/$query_id" | jq -e --arg q "$query_id" --arg org "$ORG_A" \
  '.query_id == $q and .org == $org' >/dev/null \
  || fail "admin Trino query detail did not identify tenant A query $query_id"
code="$(curl --cacert "$CA" -sS -o /tmp/trino-cross-query -w '%{http_code}' \
  --user "$DB_B:$pw_b" -H "X-Trino-User: $DB_B" "$TRINO/v1/query/$query_id")"
[ "$code" = 403 ] || fail "tenant B query detail for tenant A returned HTTP $code, want 403: $(cat /tmp/trino-cross-query)"
api -X POST -H 'Content-Type: application/json' -d '{"reason":"e2e operator cancellation"}' \
  "$API/api/v1/trino/queries/$query_id/kill" \
  | jq -e --arg org "$ORG_A" '.killed == true and .org == $org' >/dev/null
wait "$kill_pid" 2>/dev/null && fail "admin kill did not fail the tenant query"
api "$API/api/v1/audit?org=$ORG_A" | jq -e --arg q "$query_id" \
  'any(.entries[]?; .action == "trino.query.kill" and .target_user == $q and .status == 200)' >/dev/null \
  || fail "Trino query kill audit row missing"

log "per-user duckgres logins authenticate to Trino as <database_name>.<username>"
# Every org login is projected into the cell's password file under its
# qualified principal, with the same bcrypt hash pgwire verifies, so the same
# password works on both engines. Its queries belong to its org, it cannot
# reach another tenant, and disabling the login removes it from Trino.
analyst=analyst
analyst_principal="$DB_A.$analyst"
analyst_pw="$(head -c 18 /dev/urandom | od -An -tx1 | tr -d ' \n')"
api -X POST -H 'Content-Type: application/json' \
  -d "{\"org_id\":\"$ORG_A\",\"username\":\"$analyst\",\"password\":\"$analyst_pw\"}" \
  "$API/api/v1/users" >/dev/null
i=0
while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  # User creation is asynchronous. Password and group files refresh
  # independently, so successful authentication alone cannot acknowledge access.
  [ "$(scalar "$analyst_principal" "$analyst_pw" "SELECT count(*) FROM $CAT_A.$schema.$table" 2>/dev/null)" = 1 ] && break
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"; i=$((i + 1))
done
[ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ] || fail "per-user login $analyst_principal never gained access to its own catalog"
[ "$(scalar "$analyst_principal" "$analyst_pw" "SELECT count(*) FROM $CAT_A.$schema.$table")" = 1 ] \
  || fail "per-user login cannot read its own org's catalog"
trino_query "$analyst_principal" "$pw_a" 'SELECT 1' >/dev/null 2>&1 \
  && fail "per-user login authenticated with the root password"
must_fail "$analyst_principal" "$analyst_pw" "SELECT * FROM $CAT_B.main.$foreign_table" 'denied|access|catalog|not found|does not exist'
marker="per_user_attribution_$PR"
trino_query "$analyst_principal" "$analyst_pw" "SELECT '$marker'" >/dev/null
api "$API/api/v1/trino/queries?org=$ORG_A" | jq -e --arg m "$marker" --arg p "$analyst_principal" --arg org "$ORG_A" \
  'any(.queries[]; .principal == $p and .org == $org and (.query | contains($m)))' >/dev/null \
  || fail "admin query list did not attribute the per-user login's query to its org"

if [ -n "${TRINO_HOST_QUALIFIED_DOMAIN:-}" ]; then
  log "host-qualified login: $analyst on $DB_A.$TRINO_HOST_QUALIFIED_DOMAIN authenticates as $analyst_principal"
  identity="$(TRINO_HOST="$DB_A.$TRINO_HOST_QUALIFIED_DOMAIN" trino_query "$analyst" "$analyst_pw" 'SELECT current_user')" \
    || fail "host-qualified login failed for $analyst on tenant A's host"
  printf %s "$identity" | jq -e --arg p "$analyst_principal" '.[0][0] == $p' >/dev/null \
    || fail "host-qualified login ran as $identity, want $analyst_principal"
  TRINO_HOST="$DB_B.$TRINO_HOST_QUALIFIED_DOMAIN" trino_query "$analyst" "$analyst_pw" 'SELECT 1' >/dev/null 2>&1 \
    && fail "tenant A's login authenticated on tenant B's host"
else
  # Requires a Trino image with http-server.authentication.password.host-qualified-user
  # (PostHog/trino) and that property set on the lane's coordinator; see
  # tests/mw-dev/README.md "Isolated Trino lane".
  log "SKIP host-qualified login: TRINO_HOST_QUALIFIED_DOMAIN is unset for this Trino image"
fi

api -X POST "$API/api/v1/orgs/$ORG_A/users/$analyst/disable" >/dev/null
i=0
while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  trino_query "$analyst_principal" "$analyst_pw" 'SELECT 1' >/dev/null 2>&1 || break
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"; i=$((i + 1))
done
[ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ] || fail "disabled per-user login still authenticates to Trino"

log "password rotation"
new_pw="$(api -X POST "$API/api/v1/orgs/$ORG_A/reset-password" | jq -r .password)"
[ -n "$new_pw" ] && [ "$new_pw" != null ] || fail "password reset returned no password"
i=0
while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  trino_query "$DB_A" "$new_pw" 'SELECT 1' >/dev/null 2>&1 && break
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"; i=$((i + 1))
done
[ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ] || fail "rotated Trino password never became active"
trino_query "$DB_A" "$pw_a" 'SELECT 1' >/dev/null 2>&1 && fail "old Trino password still authenticates"
pw_a="$new_pw"

# The bare <database_name> principal authenticates with root's hash, so the
# per-user kill switch on root must revoke it too (it used to survive a root
# disable). The org stays enabled; re-enabling root restores the login.
log "disabling root revokes the bare org principal"
api -X POST "$API/api/v1/orgs/$ORG_A/users/root/disable" >/dev/null
i=0
while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  trino_query "$DB_A" "$pw_a" 'SELECT 1' >/dev/null 2>&1 || break
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"; i=$((i + 1))
done
[ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ] || fail "bare org principal still authenticates after root was disabled"
api -X POST "$API/api/v1/orgs/$ORG_A/users/root/enable" >/dev/null
i=0
while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do
  trino_query "$DB_A" "$pw_a" 'SELECT 1' >/dev/null 2>&1 && break
  sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"; i=$((i + 1))
done
[ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ] || fail "bare org principal did not come back after root was re-enabled"

log "worker restart preserves Hoglake data"
"$KUBECTL" -n "$NS" delete pod -l 'app=duckgres-trino,component=worker' --wait=true >/dev/null
"$KUBECTL" -n "$NS" rollout status deploy/duckgres-trino-worker --timeout=240s >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT label FROM $CAT_A.$schema.$table")" = two ] || fail "data missing after Trino worker restart"

log "coordinator restart restores catalog store, auth, and OPA bundle"
"$KUBECTL" -n "$NS" delete pod -l 'app=duckgres-trino,component=coordinator' --wait=true >/dev/null
"$KUBECTL" -n "$NS" rollout status deploy/duckgres-trino-coordinator --timeout=240s >/dev/null
i=0
while [ "$i" -lt 30 ]; do
  value="$(scalar "$DB_A" "$pw_a" "SELECT label FROM $CAT_A.$schema.$table" 2>/dev/null || true)"
  [ "$value" = two ] && break
  sleep 2; i=$((i + 1))
done
[ "$i" -lt 30 ] || fail "catalog/auth/OPA did not recover after coordinator restart"

deprovision_must_conflict() {
  status="$(curl --connect-timeout 5 --max-time 60 -sS -o /dev/null -w '%{http_code}' -H "$H" -X POST "$API/api/v1/orgs/$ORG_B/deprovision")"
  [ "$status" = 409 ] || fail "Hoglake deprovision must require explicit recovery, got $status"
}
deprovision_must_conflict
log "disable removes tenant B auth, catalog, and projection"
api -X DELETE "$API/api/v1/orgs/$ORG_B/trino" >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  body="$(api "$API/api/v1/orgs/$ORG_B/trino")"
  enabled="$(printf %s "$body" | jq -r .enabled)"
  secret_present="$("$KUBECTL" -n "$NS" get secret trino-tenant-secrets -o json | jq -r --arg k "$ORG_B" '.data | has($k)')"
  catalog_absent=false
  if admin_catalogs="$(trino_query __admin_provisioner "$admin_pw" 'SHOW CATALOGS' 2>/dev/null)"; then
    printf %s "$admin_catalogs" | jq -e --arg c "$CAT_B" 'all(.[]; .[0] != $c)' >/dev/null \
      && catalog_absent=true
  fi
  if [ "$enabled" = false ] && [ "$secret_present" = false ] && [ "$catalog_absent" = true ] \
      && ! trino_query "$DB_B" "$pw_b" 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 2; i=$((i + 1))
done
[ "$i" -lt 60 ] || fail "disabled tenant B retained Trino auth or tenant-secret projection"
catalogs_a="$(trino_query "$DB_A" "$pw_a" 'SHOW CATALOGS')"
printf %s "$catalogs_a" | jq -e --arg c "$CAT_B" 'all(.[]; .[0] != $c)' >/dev/null || fail "disabled tenant B catalog remains visible"
printf %s "$admin_catalogs" | jq -e --arg c "$CAT_B" 'all(.[]; .[0] != $c)' >/dev/null \
  || fail "disabled tenant B catalog remains in the managed catalog store: $admin_catalogs"

deprovision_must_conflict
log "reenable preserves the Hoglake catalog and data"
api -X POST -H 'Content-Type: application/json' -d '{"enabled":true,"tier":"free"}' "$API/api/v1/orgs/$ORG_B/trino" >/dev/null
wait_trino "$ORG_B" "$DB_B" "$CAT_B"
[ "$(scalar "$DB_B" "$pw_b" "SELECT count(*) FROM $CAT_B.main.$foreign_table")" = 1 ] || fail "reenabled tenant lost data"
# Namespace teardown removes fixture metadata; the runner removes only its S3 prefixes.
if [ "${TRINO_MULTICELL_ENABLED:-false}" = true ]; then
  . /harness/trino-multicell.sh
  if [ "${TRINO_SHARED_CATALOGS_ENABLED:-false}" = true ]; then
    . /harness/trino-shared-catalogs.sh
  fi
fi
log "PASS: isolated Trino provisioning + verified auth + per-user logins + DDL/DML + OPA isolation/batching + hot-add + admin + rotation + restart + disable"
