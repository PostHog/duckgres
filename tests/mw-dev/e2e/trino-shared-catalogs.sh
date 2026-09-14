#!/bin/sh
# Run after the static multicell lane in the same disposable fixture namespaces.
# This tests the real control-plane and Gateway APIs, not Kargo's Git publisher.

GATEWAY="https://duckgres-trino-gateway.$NS.svc:8443"
ORG_D="ci-pr-${PR}-trinod"
DB_D="trino-d-${PR}"
CAT_D="org_$(printf %s "$DB_D" | tr '-' '_')"
shared_operation=''
shared_plan_hash="$(printf 'isolated-shared-catalog-%s' "$PR" | sha256sum | cut -d ' ' -f 1)"
shared_operation_id="fixture-shared-$PR"
shared_store_password="$("$KUBECTL" -n "$NS" get secret duckgres-trino-catalog-store -o json | jq -r '.data.password' | base64 -d)"

shared_sql() {
  PGPASSWORD="$shared_store_password" psql -X -h "duckgres-config-store.$NS.svc" -p 5432 -U duckgres -d duckgres \
    -v ON_ERROR_STOP=1 -tAc "$1"
}

shared_gateway() {
  if [ -n "$shared_operation" ]; then
    set -- "$@" -H "X-Gateway-Operation-Id: $shared_operation_id" \
      -H "X-Gateway-Operation-Version: $(printf %s "$shared_operation" | jq -r .version)"
  fi
  curl --connect-timeout 5 --max-time 20 --cacert "$CA" -fsS \
    -u "cell-rollout:$rollout_token" -H "X-Gateway-Transaction-Admin-Token: $rollout_token" \
    -H 'Content-Type: application/json' "$@"
}

shared_cp() {
  curl --connect-timeout 5 --max-time 15 -fsS -H "X-Gateway-Transaction-Admin-Token: $rollout_token" \
    -H 'Content-Type: application/json' "$@"
}

shared_checkpoint() {
  shared_evidence='{}'
  if [ "$#" -gt 1 ]; then shared_evidence="$2"; fi
  shared_checkpoint_body="$(printf %s "$shared_operation" | jq -c --arg phase "$1" --argjson evidence "$shared_evidence" \
    '{operationId,expectedVersion:.version,phase:$phase,evidence:$evidence}')"
  shared_operation="$(shared_gateway -X PUT -d "$shared_checkpoint_body" "$GATEWAY/gateway/transactions/rollouts/cell-test/checkpoint")" \
    || fail "Gateway checkpoint failed"
}

shared_mode() {
  shared_registry="$("$KUBECTL" -n "$NS" get configmap trino-cell-registry -o json | jq -r '.data["cells.json"]' \
    | jq -c --arg mode "$1" '.cells[0].catalog_management=$mode')"
  shared_patch="$(printf %s "$shared_registry" | jq -Rs '{data:{"cells.json":.}}')"
  snapshot_control_plane_pods
  "$KUBECTL" -n "$NS" patch configmap trino-cell-registry --type=merge -p "$shared_patch" >/dev/null
  shared_patch="$(jq -cn --arg mode "$1" --arg gateway "$GATEWAY" '{spec:{template:{metadata:{annotations:{"test.duckgres/shared-mode":$mode}},spec:{containers:[{name:"controlplane",env:[{name:"DUCKGRES_TRINO_MANAGED_GATEWAY_URL",value:$gateway},{name:"DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME",value:"cell-rollout"}]}]}}}}')"
  "$KUBECTL" -n "$NS" patch deployment duckgres-control-plane --type=strategic -p "$shared_patch" >/dev/null
  wait_control_plane_rollout
}

shared_scale() {
  for shared_role in coordinator worker; do
    "$KUBECTL" -n "$CELL_NS" patch deployment "duckgres-trino-$1-$shared_role" --type=merge \
      -p '{"spec":{"replicas":'"$2"'}}' >/dev/null
  done
  if [ "$2" = 0 ]; then
    shared_attempt=0
    while [ "$shared_attempt" -lt 90 ]; do
      shared_count="$("$KUBECTL" -n "$CELL_NS" get pods -l "app=duckgres-trino-$1" -o json | jq '.items | length')"
      [ "$shared_count" = 0 ] && return 0
      sleep 2; shared_attempt=$((shared_attempt + 1))
    done
    fail "stopped color retained pods"
  fi
  for shared_role in coordinator worker; do
    "$KUBECTL" -n "$CELL_NS" rollout status "deployment/duckgres-trino-$1-$shared_role" --timeout=300s >/dev/null
  done
}

shared_catalog_fingerprint() {
  shared_sql "SELECT md5(row(connector_name,catalog_version,properties,updated_at)::text) FROM trino_catalogs WHERE cell_id='ci-pr-$PR-blue' AND catalog_name='$CAT_C'"
}

shared_gateway_query() {
  shared_query_response="$(curl --connect-timeout 5 --max-time 20 --cacert "$CA" -fsS \
    -u "$1:$2" -H "X-Trino-User: $1" --data-binary "$3" "$GATEWAY/v1/statement")" || return 1
  shared_query_rows='[]'
  shared_query_pages=0
  while [ "$shared_query_pages" -lt 100 ]; do
    printf %s "$shared_query_response" | jq -e 'has("error") | not' >/dev/null || return 1
    shared_query_rows="$(printf %s "$shared_query_response" | jq -c --argjson rows "$shared_query_rows" '$rows + (.data // [])')"
    shared_query_next="$(printf %s "$shared_query_response" | jq -r '.nextUri // empty')"
    if [ -z "$shared_query_next" ]; then printf '%s' "$shared_query_rows"; return 0; fi
    case "$shared_query_next" in
      "$GATEWAY/"*) ;;
      *) fail "Gateway advertised a continuation outside its verified origin" ;;
    esac
    shared_query_response="$(curl --connect-timeout 5 --max-time 20 --cacert "$CA" -fsS \
      -u "$1:$2" -H "X-Trino-User: $1" "$shared_query_next")" || return 1
    shared_query_pages=$((shared_query_pages + 1))
  done
  fail "Gateway query exceeded its page bound"
}

log "shared-store transition pauses catalog writes before changing stopped green"
shared_mode paused
shared_sql 'CREATE SCHEMA gateway_rollout_test' >/dev/null
"$KUBECTL" -n "$NS" patch deployment duckgres-trino-gateway --type=merge -p '{"spec":{"replicas":1}}' >/dev/null
"$KUBECTL" -n "$NS" rollout status deployment/duckgres-trino-gateway --timeout=300s >/dev/null
for shared_color in blue green; do
  shared_endpoint="https://duckgres-trino-$shared_color.$CELL_NS.svc:8443"
  shared_body="$(jq -cn --arg name "cell-test-$shared_color" --arg url "$shared_endpoint" \
    '{name:$name,proxyTo:$url,externalUrl:$url,active:true,routingGroup:"cell-test"}')"
  curl --connect-timeout 5 --max-time 20 --cacert "$CA" -fsS -u "fixture-admin:$rollout_token" \
    -H 'Content-Type: application/json' -d "$shared_body" "$GATEWAY/entity?entityType=GATEWAY_BACKEND" >/dev/null
  shared_status="$(shared_gateway -X POST "$GATEWAY/gateway/transactions/backends/cell-test-$shared_color/drain")"
  shared_body="$(printf %s "$shared_status" | jq -c '{generation}')"
  shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-$shared_color/resume" >/dev/null
done
shared_blue="$(shared_gateway "$GATEWAY/gateway/transactions/backends/cell-test-blue/drain")"
shared_body="$(printf %s "$shared_blue" | jq -c '{expectedGeneration:0,expectedBackendName:null,backendName:"cell-test-blue",backendIncarnation:.incarnation}')"
shared_route="$(shared_gateway -X PUT -d "$shared_body" "$GATEWAY/gateway/transactions/routes/cell-test")"
shared_green="$(shared_gateway "$GATEWAY/gateway/transactions/backends/cell-test-green/drain")"
shared_body="$(printf %s "$shared_green" | jq -c '{expectedIncarnation:.incarnation,expectedGeneration:.generation}')"
shared_green="$(shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-green/drain")"
shared_body="$(printf %s "$shared_green" | jq -c '{generation}')"
shared_green="$(shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-green/seal")"
shared_scale green 0
shared_green_config="$("$KUBECTL" -n "$CELL_NS" get configmap duckgres-trino-green-coordinator -o json)"
shared_patch="$(printf %s "$shared_green_config" | jq -c --arg old "cell-id=ci-pr-$PR-green" --arg new "cell-id=ci-pr-$PR-blue" \
  'if (.data["catalog-store.properties"] | contains($old)) then {data:{"catalog-store.properties":(.data["catalog-store.properties"] | split($old) | join($new))}} else error("unexpected initial catalog identity") end')"
"$KUBECTL" -n "$CELL_NS" patch configmap duckgres-trino-green-coordinator --type=merge -p "$shared_patch" >/dev/null
shared_mode gateway-shared
wait_cell_ready
shared_admin="$("$KUBECTL" -n "$CELL_NS" get secret trino-auth -o json | jq -r '.data["admin-password"]' | base64 -d)"
[ "$(shared_gateway_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")" = '[[2,18]]' ] \
  || fail "Gateway could not route the warehouse to blue before cutover"
shared_gateway_query __admin_provisioner "$shared_admin" 'SELECT node_id FROM system.runtime.nodes WHERE coordinator' \
  | jq -e --arg node "$(printf %s "$shared_blue" | jq -r .nodeId)" '.==[[$node]]' >/dev/null \
  || fail "Gateway query did not reach the active blue coordinator"

log "freeze admissions before target startup; existing catalogs must load without replay"
shared_initial_epoch="$(shared_cp "$API/internal/trino/rollout-provisioning/cell-test" | jq -r .admissionEpoch)"
shared_plan="$(jq -cn --arg operationId "$shared_operation_id" --arg planHash "$shared_plan_hash" \
  --argjson route "$shared_route" --argjson blue "$shared_blue" --argjson green "$shared_green" \
  '{operationId:$operationId,planHash:$planHash,expectedRouteGeneration:$route.generation,sourceBackend:"cell-test-blue",sourceIncarnation:$blue.incarnation,targetBackend:"cell-test-green",targetIncarnation:$green.incarnation}')"
shared_operation="$(shared_gateway -X POST -d "$shared_plan" "$GATEWAY/gateway/transactions/rollouts/cell-test/acquire")"
shared_freeze_body="$(jq -cn --arg op "$shared_operation_id" --arg hash "$shared_plan_hash" --argjson epoch "$shared_initial_epoch" \
  '{operationId:$op,planHash:$hash,expectedAdmissionEpoch:$epoch}')"
shared_attempt=0
while [ "$shared_attempt" -lt 90 ]; do
  shared_freeze="$(shared_cp -X POST -d "$shared_freeze_body" "$API/internal/trino/rollout-provisioning/cell-test/freeze")"
  if printf %s "$shared_freeze" | jq -e --arg op "$shared_operation_id" --argjson epoch "$shared_initial_epoch" \
    '.operationId==$op and .admissionEpoch==($epoch+1) and .frozen and .stable' >/dev/null; then break; fi
  sleep 2; shared_attempt=$((shared_attempt + 1))
done
[ "$shared_attempt" -lt 90 ] || fail "admission freeze never stabilized"
shared_frozen_epoch="$(printf %s "$shared_freeze" | jq -r .admissionEpoch)"
shared_before="$(shared_catalog_fingerprint)"
[ -n "$shared_before" ] || fail "admitted catalog has no persisted definition"

pw_d="$(api -X POST -H 'Content-Type: application/json' \
  -d '{"database_name":"'"$DB_D"'","team_id":93004,"metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true},"trino":{"enabled":false}}' \
  "$API/api/v1/orgs/$ORG_D/provision" | jq -r .password)"
[ -n "$pw_d" ] && [ "$pw_d" != null ] || fail "new isolated warehouse returned no password"
wait_warehouse "$ORG_D"
bootstrap_ducklake "$ORG_D" "$pw_d"
api -X PUT -H 'Content-Type: application/json' -d '{"cell":"cell-test"}' "$API/api/v1/orgs/$ORG_D/trino/cell" >/dev/null
api -X POST -H 'Content-Type: application/json' -d '{"enabled":true,"tier":"free"}' "$API/api/v1/orgs/$ORG_D/trino" >/dev/null
sleep 12
api "$API/api/v1/orgs/$ORG_D/trino" | jq -e '.status.state != "ready"' >/dev/null || fail "frozen cell admitted a new warehouse"
[ "$(shared_sql "SELECT count(*) FROM trino_catalogs WHERE cell_id='ci-pr-$PR-blue' AND catalog_name='$CAT_D'")" = 0 ] \
  || fail "frozen cell created a new catalog"

shared_claim="$(printf %s "$shared_operation" | jq -c --arg hash "$shared_plan_hash" '{operationId,expectedVersion:.version,planHash:$hash}')"
shared_operation="$(shared_gateway -X POST -d "$shared_claim" "$GATEWAY/gateway/transactions/rollouts/cell-test/publications/warm/claim")"
# Synthetic evidence exercises API phases; this fixture does not create a Git PR.
shared_checkpoint CLAIMED '{"warmPublication":{"branch":"fixture-warm","baseSha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","headSha":"cccccccccccccccccccccccccccccccccccccccc","pullRequest":1}}'
shared_scale green 1
wait_rollout_warm green
shared_body="$(printf %s "$shared_green" | jq -c '{incarnation,generation}')"
shared_green="$(shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-green/reincarnate")"
shared_body="$(printf %s "$shared_green" | jq -c '{generation}')"
shared_green="$(shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-green/resume")"
shared_checkpoint WARMED
shared_attempt=0
while [ "$shared_attempt" -lt 90 ]; do
  shared_certificate="$(shared_cp "$API/internal/trino/rollout-provisioning/cell-test")"
  if printf %s "$shared_certificate" | jq -e --arg op "$shared_operation_id" --argjson epoch "$shared_frozen_epoch" --argjson target "$shared_green" \
    '.operationId==$op and .admissionEpoch==$epoch and .frozen and .stable and .prepared and .targetBackend=="cell-test-green" and .nodeId==$target.nodeId and .coordinatorId==$target.coordinatorId and .admittedCount==1' >/dev/null; then break; fi
  sleep 2; shared_attempt=$((shared_attempt + 1))
done
[ "$shared_attempt" -lt 90 ] || fail "target certificate did not bind the admitted roster to the new process"
[ "$(shared_catalog_fingerprint)" = "$shared_before" ] || fail "target startup replayed the existing catalog write"
TRINO="$GREEN_TRINO"
[ "$(trino_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")" = '[[2,18]]' ] \
  || fail "shared target cannot read existing DuckLake data"
shared_checkpoint VERIFIED

log "cutover releases provisioning only onto the new active coordinator"
shared_body="$(jq -cn --argjson route "$shared_route" --argjson target "$shared_green" \
  '{expectedGeneration:$route.generation,expectedBackendName:"cell-test-blue",backendName:"cell-test-green",backendIncarnation:$target.incarnation}')"
shared_gateway -X PUT -d "$shared_body" "$GATEWAY/gateway/transactions/routes/cell-test" >/dev/null
shared_checkpoint CUTOVER
[ "$(shared_gateway_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")" = '[[2,18]]' ] \
  || fail "Gateway could not route the warehouse to green after cutover"
shared_gateway_query __admin_provisioner "$shared_admin" 'SELECT node_id FROM system.runtime.nodes WHERE coordinator' \
  | jq -e --arg node "$(printf %s "$shared_green" | jq -r .nodeId)" '.==[[$node]]' >/dev/null \
  || fail "Gateway query did not reach the active green coordinator"
shared_release_body="$(jq -cn --arg op "$shared_operation_id" --arg hash "$shared_plan_hash" --argjson epoch "$shared_frozen_epoch" \
  '{operationId:$op,planHash:$hash,admissionEpoch:$epoch}')"
shared_cp -X POST -d "$shared_release_body" "$API/internal/trino/rollout-provisioning/cell-test/release" \
  | jq -e --arg op "$shared_operation_id" --argjson epoch "$shared_frozen_epoch" '.operationId==$op and .admissionEpoch==($epoch+1) and (.frozen|not) and (.prepared|not)' >/dev/null \
  || fail "release did not return its exact durable receipt"
shared_attempt=0
while [ "$shared_attempt" -lt 90 ]; do
  if api "$API/api/v1/orgs/$ORG_D/trino" | jq -e '.status.state=="ready"' >/dev/null; then break; fi
  sleep 2; shared_attempt=$((shared_attempt + 1))
done
[ "$shared_attempt" -lt 90 ] || fail "post-cutover warehouse was not admitted"
TRINO="$GREEN_TRINO"
trino_query __admin_provisioner "$shared_admin" 'SHOW CATALOGS' | jq -e --arg catalog "$CAT_D" 'any(.[]; .[0]==$catalog)' >/dev/null \
  || fail "active green missed the new catalog"
TRINO="$BLUE_TRINO"
trino_query __admin_provisioner "$shared_admin" 'SHOW CATALOGS' | jq -e --arg catalog "$CAT_D" 'all(.[]; .[0]!=$catalog)' >/dev/null \
  || fail "draining blue received a post-cutover catalog write"
[ "$(trino_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")" = '[[2,18]]' ] \
  || fail "unchanged source catalog stopped serving during overlap"

shared_blue="$(shared_gateway "$GATEWAY/gateway/transactions/backends/cell-test-blue/drain")"
shared_body="$(printf %s "$shared_blue" | jq -c '{expectedIncarnation:.incarnation,expectedGeneration:.generation}')"
shared_blue="$(shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-blue/drain")"
shared_checkpoint DRAINING
shared_attempt=0
while [ "$shared_attempt" -lt 60 ]; do
  shared_blue="$(shared_gateway "$GATEWAY/gateway/transactions/backends/cell-test-blue/drain")"
  if printf %s "$shared_blue" | jq -e '.readyToSeal' >/dev/null; then break; fi
  sleep 2; shared_attempt=$((shared_attempt + 1))
done
[ "$shared_attempt" -lt 60 ] || fail "fixture source did not drain within its retention bound"
shared_body="$(printf %s "$shared_blue" | jq -c '{generation}')"
shared_gateway -X POST -d "$shared_body" "$GATEWAY/gateway/transactions/backends/cell-test-blue/seal" | jq -e '.drained' >/dev/null
shared_checkpoint SEALED
shared_claim="$(printf %s "$shared_operation" | jq -c --arg hash "$shared_plan_hash" '{operationId,expectedVersion:.version,planHash:$hash}')"
shared_operation="$(shared_gateway -X POST -d "$shared_claim" "$GATEWAY/gateway/transactions/rollouts/cell-test/publications/stop/claim")"
shared_checkpoint SEALED '{"stopPublication":{"branch":"fixture-stop","baseSha":"cccccccccccccccccccccccccccccccccccccccc","headSha":"dddddddddddddddddddddddddddddddddddddddd","pullRequest":2}}'
shared_scale blue 0
shared_checkpoint STOPPED
shared_checkpoint COMPLETE
TRINO="$LEGACY_TRINO"
unset shared_store_password shared_admin pw_d
log "PASS: real shared-store freeze, startup without replay, process certificate, cutover, active-only admission and sealed source stop"
