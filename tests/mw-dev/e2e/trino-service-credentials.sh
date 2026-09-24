#!/bin/sh
# Sourced only by the disposable two-tenant Trino fixture lane.
[ "$NS" = "duckgres-ci-pr-$PR" ] || fail "service credential checks require an isolated PR namespace"
[ "$ORG_A" = "ci-pr-${PR}-trinoa" ] || fail "service credential checks require the fixture organization"
log "checking Trino service credentials and non-rotating renewal"
svc_mint="$(api -X POST -H 'Content-Type: application/json' \
  -d '{"principal":"harness:trino-service-credentials","ttl_seconds":60}' \
  "$API/api/v1/orgs/$ORG_A/service-credentials")"
printf %s "$svc_mint" | jq -e '.trino_connect.http_scheme == "https" and .secret_rotated == true and (.credential_secret | length > 0)' >/dev/null \
  || fail "service credential support is not enabled on the candidate control plane"
svc_id="$(printf %s "$svc_mint" | jq -r .credential_id)"
svc_user="$(printf %s "$svc_mint" | jq -r .trino_connect.username)"
svc_password="$(printf %s "$svc_mint" | jq -r .credential_secret)"
svc_origin="https://$(printf %s "$svc_mint" | jq -r .trino_connect.host):$(printf %s "$svc_mint" | jq -r .trino_connect.port)"
[ "$svc_origin" = "$TRINO" ] || fail "service target differs from the isolated fixture endpoint"
svc_response="$(curl --connect-timeout 5 --max-time 30 --cacert "$CA" -fsS --user "$svc_user:$svc_password" \
  -H "X-Trino-User: $svc_user" --data-binary 'SELECT 1' "$svc_origin/v1/statement")" \
  || fail "service credential statement authentication failed"
svc_renew="$(api -X POST -H 'Content-Type: application/json' \
  -d '{"credential_id":"'"$svc_id"'","ttl_seconds":120,"rotate_secret":false}' \
  "$API/api/v1/orgs/$ORG_A/service-credentials/refresh")"
printf %s "$svc_renew" | jq -e --arg id "$svc_id" '.credential_id == $id and .secret_rotated == false and (has("credential_secret") | not)' >/dev/null \
  || fail "renewal did not explicitly preserve the existing secret"
svc_pages=0
while [ "$svc_pages" -lt 100 ]; do
  printf %s "$svc_response" | jq -e 'has("error") | not' >/dev/null || fail "service query failed"
  svc_next="$(printf %s "$svc_response" | jq -r '.nextUri // empty')"
  [ -n "$svc_next" ] || break
  case "$svc_next" in "$svc_origin/"*) ;; *) fail "unexpected service query continuation origin" ;; esac
  svc_response="$(curl --connect-timeout 5 --max-time 30 --cacert "$CA" -fsS --user "$svc_user:$svc_password" \
    -H "X-Trino-User: $svc_user" "$svc_next")" || fail "renewed service query polling failed"
  svc_pages=$((svc_pages + 1))
done
[ "$svc_pages" -lt 100 ] || fail "service query did not finish within its page bound"
[ "$(scalar "$svc_user" "$svc_password" "SELECT count(*) FROM $CAT_A.information_schema.schemata")" -gt 0 ] \
  || fail "service grant cannot read its own catalog metadata"
if trino_query "$svc_user" "$svc_password" "SHOW SCHEMAS FROM $CAT_B" >/dev/null 2>&1; then
  fail "service grant crossed the organization catalog boundary"
fi
svc_wrong_status="$(curl --connect-timeout 5 --max-time 30 --cacert "$CA" -sS -o /dev/null -w '%{http_code}' \
  --user "$DB_B.$svc_id:$svc_password" -H "X-Trino-User: $DB_B.$svc_id" --data-binary 'SELECT 1' "$svc_origin/v1/statement")"
[ "$svc_wrong_status" = 401 ] || fail "service secret authenticated for another tenant"
api -X DELETE "$API/api/v1/orgs/$ORG_A/service-grants/$svc_id" >/dev/null
svc_revoked_status="$(curl --connect-timeout 5 --max-time 30 --cacert "$CA" -sS -o /dev/null -w '%{http_code}' \
  --user "$svc_user:$svc_password" -H "X-Trino-User: $svc_user" --data-binary 'SELECT 1' "$svc_origin/v1/statement")"
[ "$svc_revoked_status" = 401 ] || fail "revoked service grant still authenticates"
unset svc_mint svc_renew svc_password svc_response
log "PASS: service authentication, stable renewal, polling, tenant isolation and revocation"
