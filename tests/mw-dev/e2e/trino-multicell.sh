#!/bin/sh
# The main Trino harness supplies authenticated API and SQL helpers.
CELL_NS="${TRINO_CELL_NAMESPACE:?}"
ORG_C="ci-pr-${PR}-trinoc"
DB_C="trino-c-${PR}"
CAT_C="org_$(printf %s "$DB_C" | tr '-' '_')"
LEGACY_TRINO="$TRINO"
BLUE_TRINO="https://duckgres-trino-blue.$CELL_NS.svc:8443"
GREEN_TRINO="https://duckgres-trino-green.$CELL_NS.svc:8443"

log "multicell initial placement with green stopped"
api "$API/api/v1/trino/cells" | jq -e '.cells | map(.id) | sort == ["cell-test","legacy"]' >/dev/null \
  || fail "both cells must be registered"
for target in coordinator worker; do
  "$KUBECTL" -n "$CELL_NS" get deployment "duckgres-trino-green-$target" -o json \
    | jq -e '.spec.replicas == 0 and (.status.replicas // 0) == 0' >/dev/null \
    || fail "green must start stopped"
done
blue_internal="$("$KUBECTL" -n "$CELL_NS" get secret trino-blue-internal -o json | jq -r '.data["shared-secret"]')"
green_internal="$("$KUBECTL" -n "$CELL_NS" get secret trino-green-internal -o json | jq -r '.data["shared-secret"]')"
[ -n "$blue_internal" ] && [ "$blue_internal" != "$green_internal" ] || fail "backend internal secrets must differ"

pw_c="$(api -X POST -H 'Content-Type: application/json' \
  -d '{"database_name":"'"$DB_C"'","team_id":93003,"metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true},"trino":{"enabled":false}}' \
  "$API/api/v1/orgs/$ORG_C/provision" | jq -r .password)"
[ -n "$pw_c" ] && [ "$pw_c" != null ] || fail "new warehouse returned no password"
wait_warehouse "$ORG_C"
bootstrap_ducklake "$ORG_C" "$pw_c"
api -X PUT -H 'Content-Type: application/json' -d '{"cell":"cell-test"}' \
  "$API/api/v1/orgs/$ORG_C/trino/cell" | jq -e '.assigned == true and .cell.id == "cell-test"' >/dev/null \
  || fail "initial cell selection failed"
api "$API/api/v1/orgs/$ORG_C/trino" | jq -e '.enabled == false and .assigned == true and .cell.id == "cell-test"' >/dev/null \
  || fail "selection must not enable Trino"
api -X POST -H 'Content-Type: application/json' -d '{"enabled":true,"tier":"free"}' "$API/api/v1/orgs/$ORG_C/trino" >/dev/null

wait_cell_ready() {
  attempt=0
  while [ "$attempt" -lt 120 ]; do
    result="$(api "$API/api/v1/orgs/$ORG_C/trino" 2>/dev/null || true)"
    if printf %s "$result" | jq -e --arg principal "$DB_C" --arg catalog "$CAT_C" \
      '.enabled == true and .available == true and .cell.id == "cell-test" and .status.cell == "cell-test" and .status.state == "ready" and .status.principal == $principal and .status.catalog == $catalog' >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
    attempt=$((attempt + 1))
  done
  fail "registered cell did not reconcile tenant readiness"
}
wait_cell_ready
wait_cell_auth() {
  attempt=0
  while [ "$attempt" -lt 36 ]; do
    if catalogs="$(trino_query "$DB_C" "$pw_c" 'SHOW CATALOGS' 2>/dev/null)" && \
       printf %s "$catalogs" | jq -e --arg catalog "$CAT_C" 'any(.[]; .[0] == $catalog)' >/dev/null; then
      return 0
    fi
    sleep 5
    attempt=$((attempt + 1))
  done
  fail "tenant auth and OPA catalog visibility did not converge"
}
api "$API/api/v1/orgs/$ORG_C" | jq -e '.trino.trino_cell_id == "registered:cell-test"' >/dev/null \
  || fail "new logical cell did not preserve its distinct stored ownership"
code="$(curl --connect-timeout 5 --max-time 30 -sS -o /tmp/trino-cell-selection-error -w '%{http_code}' -H "$H" -H 'Content-Type: application/json' \
  -X PUT -d '{"cell":"cell-test"}' "$API/api/v1/orgs/$ORG_A/trino/cell")"
[ "$code" = 409 ] || fail "existing legacy ownership must reject a cell move"

TRINO="$BLUE_TRINO"
wait_cell_auth
trino_query "$DB_C" "$pw_c" "CREATE SCHEMA $CAT_C.cell_test" >/dev/null
trino_query "$DB_C" "$pw_c" "CREATE TABLE $CAT_C.cell_test.values_test (value BIGINT)" >/dev/null
trino_query "$DB_C" "$pw_c" "INSERT INTO $CAT_C.cell_test.values_test VALUES (7),(11)" >/dev/null
result="$(trino_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")"
[ "$result" = '[[2,18]]' ] || fail "new cell did not query real DuckLake data"
must_fail "$DB_A" "$pw_a" 'SELECT 1' '401|Unauthorized|Authentication|credentials'
TRINO="$LEGACY_TRINO"
log "legacy remains queryable"
[ "$(trino_query "$DB_A" "$pw_a" 'SELECT 1')" = '[[1]]' ] || fail "legacy query regressed"
must_fail "$DB_C" "$pw_c" 'SELECT 1' '401|Unauthorized|Authentication|credentials'

legacy_token="$("$KUBECTL" -n "$NS" get secret trino-opa-bundle-token -o json | jq -r '.data.token' | base64 -d)"
cell_token="$("$KUBECTL" -n "$CELL_NS" get secret trino-opa-bundle-token -o json | jq -r '.data.token' | base64 -d)"
for pair in "legacy-to-cell" "cell-to-legacy"; do
  token="$legacy_token" path=/bundles/trino/cell-test
  if [ "$pair" = cell-to-legacy ]; then token="$cell_token"; path=/bundles/trino; fi
  code="$(curl --connect-timeout 5 --max-time 30 -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $token" "$API$path")"
  [ "$code" = 401 ] || [ "$code" = 403 ] || fail "OPA token crossed cell boundary"
done

log "green catalog hydration"
registry="$("$KUBECTL" -n "$NS" get configmap trino-cell-registry -o json \
  | jq -r '.data["cells.json"]' | jq '.cells[0].backends |= map(if .id == "green" then .running=true else . end)')"
patch="$(printf %s "$registry" | jq -Rs '{data:{"cells.json":.}}')"
"$KUBECTL" -n "$NS" patch configmap trino-cell-registry --type=merge -p "$patch" >/dev/null
"$KUBECTL" -n "$NS" rollout restart deployment/duckgres-control-plane >/dev/null
"$KUBECTL" -n "$NS" rollout status deployment/duckgres-control-plane --timeout=180s >/dev/null
for target in coordinator worker; do
  "$KUBECTL" -n "$CELL_NS" patch deployment "duckgres-trino-green-$target" --type=merge -p '{"spec":{"replicas":1}}' >/dev/null
done
# Named reads avoid namespace-wide Deployment list permissions.
attempt=0
while [ "$attempt" -lt 90 ]; do
  ready=1
  for target in coordinator worker; do
    "$KUBECTL" -n "$CELL_NS" get deployment "duckgres-trino-green-$target" -o json \
      | jq -e '.status.observedGeneration == .metadata.generation and .status.readyReplicas == 1 and .status.updatedReplicas == 1' >/dev/null \
      || ready=0
  done
  [ "$ready" = 1 ] && break
  sleep 5
  attempt=$((attempt + 1))
done
[ "$ready" = 1 ] || fail "green did not become ready"
wait_cell_ready
TRINO="$GREEN_TRINO"
wait_cell_auth
result="$(trino_query "$DB_C" "$pw_c" "SELECT COUNT(*), SUM(value) FROM $CAT_C.cell_test.values_test")"
[ "$result" = '[[2,18]]' ] || fail "green failed to hydrate its independent catalog from the same DuckLake warehouse"
must_fail "$DB_A" "$pw_a" 'SELECT 1' '401|Unauthorized|Authentication|credentials'
TRINO="$BLUE_TRINO"
[ "$(trino_query "$DB_C" "$pw_c" "SELECT COUNT(*) FROM $CAT_C.cell_test.values_test")" = '[[2]]' ] || fail "blue stopped serving during green hydration"
TRINO="$LEGACY_TRINO"
[ "$(trino_query "$DB_A" "$pw_a" 'SELECT 1')" = '[[1]]' ] || fail "legacy failed during green hydration"
log "PASS: initial placement + stopped green + isolated credentials/OPA + real DuckLake queries + green hydration"
