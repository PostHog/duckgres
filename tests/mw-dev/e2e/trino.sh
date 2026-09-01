#!/bin/sh
# Engine-specific Trino e2e lane. The coordinator, worker, OPA bundle, catalog
# store, credentials, and cell id all belong to this PR namespace.
set -eu
trap 'rc=$?; [ "$rc" = 0 ] || echo "TRINO HARNESS EXIT rc=$rc" >&2' EXIT

API="${CP_API:?}"
PGHOST="${CP_PG_HOST:?}"
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
SNI_SUFFIX=".ci.duckgres.local"
CP_IP=""
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
CP_IP="$(getent hosts "$PGHOST" | awk '{print $1}' | head -1)"
[ -n "$CP_IP" ] || fail "could not resolve $PGHOST"
KUBECTL=/tmp/kubectl
KUBECTL_VERSION=v1.33.1
curl -fsSLo "$KUBECTL" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/arm64/kubectl"
chmod +x "$KUBECTL"
"$KUBECTL" version --client >/dev/null || fail "pinned kubectl bootstrap failed"

api() { curl -fsS -H "$H" "$@"; }

# A fresh managed warehouse has an empty metadata database. DuckLake's Trino
# connector consumes an existing DuckLake catalog; it does not create the
# metadata tables itself. Initialize them once through Duckgres's normal
# DuckLake activation path before asking Trino to use the catalog. This is
# setup, not duplicated DuckDB coverage: all behavioral assertions below still
# execute through Trino.
bootstrap_ducklake() { # org password
  attempt=0
  while [ "$attempt" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
        -v ON_ERROR_STOP=1 -tAc 'SELECT 1' 2>&1)" && [ "$out" = 1 ]; then
      return 0
    fi
    case "$out" in
      *"capacity exhausted"*|*"no Duckgres worker"*|\
      *"still provisioning"*|*"failed to initialize session"*|\
      *"timed out waiting for an available worker"*|*"failed to start"*|\
      *"spawn sized worker"*|*"failed to detect attached catalogs"*) ;;
      *) fail "DuckLake bootstrap failed for $1: $out" ;;
    esac
    log "DuckLake bootstrap worker not ready for $1; retrying"
    sleep 15
    attempt=$((attempt + 1))
  done
  fail "DuckLake bootstrap worker did not become ready for $1"
}

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
      printf %s "$body" | jq -e --arg p "$2" --arg c "$3" --arg cell "ci-pr-$PR" --arg host "duckgres-trino.$NS.svc" \
        '.enabled == true and .available == true and .status.principal == $p and .status.catalog == $c and .status.cell == $cell and .status.tier == "free" and .status.connection.host == $host and .status.connection.port == 8443 and .status.connection.username == $p and (.status.connection | has("password") | not)' >/dev/null \
        || fail "$1 Trino status identity mismatch: $body"
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
trino_query() { # principal password sql
  principal="$1" password="$2" sql="$3"
  response="$(curl --cacert "$CA" -fsS --user "$principal:$password" \
    -H "X-Trino-User: $principal" -H 'X-Trino-Time-Zone: UTC' \
    --data-binary "$sql" "$TRINO/v1/statement")" || return 1
  rows='[]'
  while :; do
    err="$(printf %s "$response" | jq -r '.error.message // empty')"
    [ -z "$err" ] || { echo "$err" >&2; return 1; }
    rows="$(printf %s "$response" | jq -c --argjson rows "$rows" '$rows + (.data // [])')"
    next="$(printf %s "$response" | jq -r '.nextUri // empty')"
    [ -n "$next" ] || break
    response="$(curl --cacert "$CA" -fsS --user "$principal:$password" \
      -H "X-Trino-User: $principal" -H 'X-Trino-Time-Zone: UTC' "$next")" || return 1
  done
  printf '%s\n' "$rows"
}

scalar() { trino_query "$1" "$2" "$3" | jq -r '.[0][0]'; }
must_fail() { # principal password sql pattern
  out="$(trino_query "$1" "$2" "$3" 2>&1)" && fail "query unexpectedly succeeded: $3"
  printf %s "$out" | grep -Eqi "$4" || fail "query failed for wrong reason: $out"
}

log "provisioning first Trino tenant"
pw_a="$(provision "$ORG_A" "$DB_A" "$TEAM_A" | jq -r .password)"
[ -n "$pw_a" ] && [ "$pw_a" != null ] || fail "tenant A provision returned no password"
wait_warehouse "$ORG_A"
bootstrap_ducklake "$ORG_A" "$pw_a"
wait_trino "$ORG_A" "$DB_A" "$CAT_A"

log "TLS/password auth, discovery, and DDL/DML"
[ "$(scalar "$DB_A" "$pw_a" 'SELECT 1')" = 1 ] || fail "Trino SELECT 1 failed"
must_fail "$DB_A" definitely-wrong-password 'SELECT 1' '401|Unauthorized|Authentication|credentials'
must_fail "$ORG_A" "$pw_a" 'SELECT 1' '401|Unauthorized|Authentication|credentials'
catalogs="$(trino_query "$DB_A" "$pw_a" 'SHOW CATALOGS')"
printf %s "$catalogs" | jq -e --arg c "$CAT_A" 'any(.[]; .[0] == $c)' >/dev/null || fail "own catalog absent: $catalogs"
printf %s "$catalogs" | jq -e --arg c "$CAT_B" 'all(.[]; .[0] != $c)' >/dev/null || fail "foreign catalog visible before tenant B exists"
table="e2e_trino_${PR}"
view="${table}_view"
schema="e2e_${PR}"
writes="${table}_writes"
scratch="${table}_scratch"
trino_query "$DB_A" "$pw_a" "CREATE SCHEMA $CAT_A.$schema" >/dev/null
schemas="$(trino_query "$DB_A" "$pw_a" "SHOW SCHEMAS FROM $CAT_A")"
printf %s "$schemas" | jq -e --arg s "$schema" 'any(.[]; .[0] == $s)' >/dev/null || fail "created schema absent: $schemas"
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$table (id BIGINT, flag BOOLEAN, amount DECIMAL(10,2), label VARCHAR, event_date DATE)" >/dev/null
trino_query "$DB_A" "$pw_a" "INSERT INTO $CAT_A.$schema.$table VALUES (1, true, DECIMAL '1.25', 'one', DATE '2026-08-31'), (2, false, DECIMAL '2.50', 'two', DATE '2026-09-01')" >/dev/null
trino_query "$DB_A" "$pw_a" "UPDATE $CAT_A.$schema.$table SET label='TWO' WHERE id=2" >/dev/null
trino_query "$DB_A" "$pw_a" "DELETE FROM $CAT_A.$schema.$table WHERE id=1" >/dev/null
trino_query "$DB_A" "$pw_a" "CREATE VIEW $CAT_A.$schema.$view AS SELECT * FROM $CAT_A.$schema.$table" >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT concat(cast(flag AS varchar), '|', cast(amount AS varchar), '|', label, '|', cast(event_date AS varchar)) FROM $CAT_A.$schema.$view")" = "false|2.50|TWO|2026-09-01" ] \
  || fail "Trino representative type/DML/view data mismatch"
tables="$(trino_query "$DB_A" "$pw_a" "SHOW TABLES FROM $CAT_A.$schema")"
printf %s "$tables" | jq -e --arg t "$table" --arg v "$view" \
  'any(.[]; .[0] == $t) and any(.[]; .[0] == $v)' >/dev/null || fail "SHOW TABLES missed table/view: $tables"
trino_query "$DB_A" "$pw_a" "EXPLAIN SELECT * FROM $CAT_A.$schema.$table" >/dev/null
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$scratch (id INTEGER)" >/dev/null
trino_query "$DB_A" "$pw_a" "INSERT INTO $CAT_A.$schema.$scratch VALUES 1, 2" >/dev/null
trino_query "$DB_A" "$pw_a" "TRUNCATE TABLE $CAT_A.$schema.$scratch" >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT count(*) FROM $CAT_A.$schema.$scratch")" = 0 ] || fail "Trino TRUNCATE did not remove rows"
trino_query "$DB_A" "$pw_a" "CREATE TABLE $CAT_A.$schema.$writes (id INTEGER)" >/dev/null
pids=""
for id in 1 2 3 4; do
  trino_query "$DB_A" "$pw_a" "INSERT INTO $CAT_A.$schema.$writes VALUES ($id)" >/dev/null & pids="$pids $!"
done
rc=0; for pid in $pids; do wait "$pid" || rc=1; done
[ "$rc" = 0 ] || fail "a concurrent Trino write failed"
[ "$(scalar "$DB_A" "$pw_a" "SELECT count(*) FROM $CAT_A.$schema.$writes")" = 4 ] || fail "concurrent Trino writes lost rows"

log "hot-add second tenant without restarting coordinator"
coord_uid_before="$("$KUBECTL" -n "$NS" get pod -l 'app=duckgres-trino,component=coordinator' -o jsonpath='{.items[0].metadata.uid}')"
pw_b="$(provision "$ORG_B" "$DB_B" "$TEAM_B" | jq -r .password)"
[ -n "$pw_b" ] && [ "$pw_b" != null ] || fail "tenant B provision returned no password"
wait_warehouse "$ORG_B"
bootstrap_ducklake "$ORG_B" "$pw_b"
wait_trino "$ORG_B" "$DB_B" "$CAT_B"
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
api "$API/api/v1/trino/status" | jq -e --arg cell "ci-pr-$PR" '.available == true and .cell.id == $cell' >/dev/null
api "$API/api/v1/trino/nodes" | jq -e '.available == true and (.nodes | length) >= 2' >/dev/null
api "$API/api/v1/trino/orgs" | jq -e --arg a "$ORG_A" --arg b "$ORG_B" \
  'any(.orgs[]; .org == $a and .state == "ready") and any(.orgs[]; .org == $b and .state == "ready")' >/dev/null
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

log "worker restart preserves DuckLake data"
"$KUBECTL" -n "$NS" delete pod -l 'app=duckgres-trino,component=worker' --wait=true >/dev/null
"$KUBECTL" -n "$NS" rollout status deploy/duckgres-trino-worker --timeout=240s >/dev/null
[ "$(scalar "$DB_A" "$pw_a" "SELECT label FROM $CAT_A.$schema.$table")" = TWO ] || fail "data missing after Trino worker restart"

log "coordinator restart restores catalog store, auth, and OPA bundle"
"$KUBECTL" -n "$NS" delete pod -l 'app=duckgres-trino,component=coordinator' --wait=true >/dev/null
"$KUBECTL" -n "$NS" rollout status deploy/duckgres-trino-coordinator --timeout=240s >/dev/null
i=0
while [ "$i" -lt 30 ]; do
  value="$(scalar "$DB_A" "$pw_a" "SELECT label FROM $CAT_A.$schema.$table" 2>/dev/null || true)"
  [ "$value" = TWO ] && break
  sleep 2; i=$((i + 1))
done
[ "$i" -lt 30 ] || fail "catalog/auth/OPA did not recover after coordinator restart"

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

trino_query "$DB_A" "$pw_a" "DROP VIEW $CAT_A.$schema.$view" >/dev/null
trino_query "$DB_A" "$pw_a" "DROP TABLE $CAT_A.$schema.$table" >/dev/null
trino_query "$DB_A" "$pw_a" "DROP TABLE $CAT_A.$schema.$scratch" >/dev/null
trino_query "$DB_A" "$pw_a" "DROP TABLE $CAT_A.$schema.$writes" >/dev/null
trino_query "$DB_A" "$pw_a" "DROP SCHEMA $CAT_A.$schema" >/dev/null
log "PASS: isolated Trino provisioning + verified auth + DDL/DML + OPA isolation/batching + hot-add + admin + rotation + restart + disable"
