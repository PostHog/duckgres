#!/bin/sh
# In-cluster e2e harness. Runs as a Job in the per-PR namespace, talking to the
# control-plane ClusterIP service. Provisions cnpg + ext ducklings (aurora is
# being retired — out of scope), then exercises DuckLake + Iceberg read/write,
# the create→delete→recreate lifecycle, and (TODO) durability/concurrency/netpol.
#
# Exit non-zero on any failure → the Job fails → the workflow step fails.
#
# Env (from run.sh): NAMESPACE, PR_NUMBER, INTERNAL_SECRET, CP_API, CP_PG_HOST
set -eu

apk add --no-cache curl jq postgresql-client >/dev/null 2>&1 || true

API="${CP_API:?}"
PGHOST="${CP_PG_HOST:?}"
SECRET="${INTERNAL_SECRET:?}"
H="X-Duckgres-Internal-Secret: $SECRET"
CNPG="ci-pr-${PR_NUMBER}-cnpg"
EXT="ci-pr-${PR_NUMBER}-ext"

# duckling-example RDS — the shared external metadata store (same one the
# manual validation used). Endpoint is stable in mw-dev.
EXT_RDS_ENDPOINT="duckling-example-managed-warehouse-dev-us-east-1.c8jy2c68kipq.us-east-1.rds.amazonaws.com"
EXT_RDS_SECRET="duckling-example-managed-warehouse-dev-us-east-1-rds-password"

fail() { echo "FAIL: $*" >&2; exit 1; }
# stderr, NOT stdout: log() is called inside functions whose stdout is captured
# in $(...) and piped to jq (e.g. provision | jq -r .password). A log line on
# stdout would land in that capture and make jq choke ("Invalid numeric literal").
log()  { echo ">>> $*" >&2; }

api_post() { curl -fsS -X POST -H "$H" "$API/api/v1/orgs/$1/$2"; }
api_get()  { curl -fsS -H "$H" "$API/api/v1/orgs/$1/$2"; }
state_of() { api_get "$1" warehouse/status 2>/dev/null | jq -r '.state // "missing"'; }

provision() { # org metadata_json
  log "provision $1"
  curl -fsS -X POST -H "$H" -H 'Content-Type: application/json' -d "$2" \
    "$API/api/v1/orgs/$1/provision"
}

wait_state() { # org target timeout_s
  org="$1"; want="$2"; deadline=$(( $(date +%s) + ${3:-600} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    s="$(state_of "$org")"
    [ "$s" = "$want" ] && { log "$org -> $s"; return 0; }
    [ "$s" = "failed" ] && fail "$org entered failed state"
    sleep 10
  done
  fail "$org did not reach $want within ${3}s (last=$(state_of "$org"))"
}

# Org routing is by TLS SNI hostname <org>.<managed-suffix>; the dbname selects
# the CATALOG (`ducklake` or `iceberg`), not the org (PR #651). The control
# plane rejects connections without a managed SNI host. We use libpq's
# host (→ SNI + TLS name) / hostaddr (→ TCP target) split: SNI carries the org
# hostname while the TCP connection still lands on the CP ClusterIP. The suffix
# (.ci.duckgres.local) is CI-internal and never resolves in DNS — hostaddr is
# what actually connects.
SNI_SUFFIX=".ci.duckgres.local"
CP_IP=""
resolve_cp_ip() {
  # ClusterIP of the CP service, for hostaddr. getent works from the alpine Job.
  CP_IP="$(getent hosts "$PGHOST" | awk '{print $1}' | head -1)"
  [ -n "$CP_IP" ] || fail "could not resolve $PGHOST"
}

pg() { # org password dbname(catalog) sql
  PGPASSWORD="$2" psql \
    "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=$3" \
    -v ON_ERROR_STOP=1 -tAc "$4"
}

# Connect preflight: a warm worker isn't always available the instant a
# warehouse goes ready (shared_warm_target=0, so the first connection for an
# org cold-spawns a worker; a second org can find the pool momentarily
# exhausted). The CP returns a transient "no warm Duckgres worker is currently
# available; retry in ~45s" / "still provisioning" / "failed to initialize
# session". Retry `SELECT 1` through those, bounded, BEFORE the R/W. Auth
# failures are NOT in this set, so this never feeds the rate limiter.
wait_worker() { # org password catalog
  attempt=0
  while [ "$attempt" -lt 10 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc 'SELECT 1' 2>&1)" && [ "$out" = "1" ]; then
      return 0
    fi
    log "worker not ready for $1/$3 ($(printf %s "$out" | tr -d '\n' | tail -c 80)); retry…"
    sleep 15
    attempt=$((attempt + 1))
  done
  fail "no warm worker for $1/$3 after retries"
}

rw_ducklake() { # org password
  log "DuckLake R/W on $1"
  t="e2e_dl_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "DROP TABLE IF EXISTS $t; CREATE TABLE $t(id INT, label VARCHAR);
            INSERT INTO $t VALUES (1,'one'),(2,'two'),(3,'three');"
  n="$(pg "$1" "$2" ducklake "SELECT COUNT(*) FROM $t;")"
  [ "$n" = "3" ] || fail "$1 DuckLake rowcount=$n want 3"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

rw_iceberg() { # org password
  log "Iceberg R/W on $1"
  t="e2e_ice_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" iceberg "DROP TABLE IF EXISTS iceberg.public.$t;
            CREATE TABLE iceberg.public.$t(id INT, label VARCHAR);
            INSERT INTO iceberg.public.$t VALUES (10,'ten'),(20,'twenty');"
  n="$(pg "$1" "$2" iceberg "SELECT COUNT(*) FROM iceberg.public.$t;")"
  [ "$n" = "2" ] || fail "$1 Iceberg rowcount=$n want 2"
  pg "$1" "$2" iceberg "DROP TABLE iceberg.public.$t;"
}

# ---- cnpg duckling: cnpg-shard metadata + DuckLake + Iceberg --------------
CNPG_BODY='{"database_name":"'"$CNPG"'","metadata_store":{"type":"cnpg-shard"},
  "data_store":{"type":"s3bucket"},"ducklake":{"enabled":true},
  "iceberg":{"enabled":true,"namespace":"main"}}'

# ---- ext duckling: external RDS metadata + DuckLake + Iceberg -------------
EXT_BODY='{"database_name":"'"$EXT"'",
  "metadata_store":{"type":"external","external":{
    "endpoint":"'"$EXT_RDS_ENDPOINT"'","password_aws_secret":"'"$EXT_RDS_SECRET"'",
    "user":"ducklingexample","database":"ducklingexample"}},
  "data_store":{"type":"external","bucket_name":"posthog-duckling-example-managed-warehouse-dev","region":"us-east-1"},
  "ducklake":{"enabled":true},"iceberg":{"enabled":true,"namespace":"main"}}'

main() {
  resolve_cp_ip
  # Use the password returned at provision time — do NOT call reset-password and
  # then retry-connect in a tight loop: the CP rate-limiter bans the source IP
  # after a handful of failed auths, and a fresh password isn't live until the
  # next config-store poll. Provision returns the live password directly.
  cnpg_pw="$(provision "$CNPG" "$CNPG_BODY" | jq -r .password)"
  ext_pw="$(provision "$EXT" "$EXT_BODY" | jq -r .password)"
  wait_state "$CNPG" ready 600
  wait_state "$EXT" ready 600

  # Settle: let the CP's config-store poll pick up the provisioned org/users
  # before the first connection, so we don't burn failed-auth attempts against
  # the rate limiter while the auth cache catches up.
  log "settling ${CONFIG_POLL_SETTLE:-40}s for CP auth cache…"
  sleep "${CONFIG_POLL_SETTLE:-40}"

  # 1. DuckLake + Iceberg read/write, both metadata backends. wait_worker
  #    preflights each org so a transient empty worker pool doesn't fail the run.
  wait_worker "$CNPG" "$cnpg_pw" ducklake
  rw_ducklake "$CNPG" "$cnpg_pw"; rw_iceberg "$CNPG" "$cnpg_pw"
  wait_worker "$EXT" "$ext_pw" ducklake
  rw_ducklake "$EXT"  "$ext_pw";  rw_iceberg "$EXT"  "$ext_pw"

  # 2. Lifecycle: deprovision cnpg → confirm deleted. Proves the teardown path
  #    (warehouse delete + Duckling CR delete) works end to end.
  #
  #    NOTE: same-orgID *recreate* (the regression net for the stranded-state
  #    bugs in #649/#650/#11518/#11522) is intentionally NOT done here. A
  #    deprovision marks the warehouse deleted as soon as the Duckling CR delete
  #    is issued, but the CR's finalizers + the downstream async drop of the
  #    cnpg role/db keep running afterwards. Re-provisioning the same orgID
  #    immediately races a still-terminating Duckling CR and stalls in
  #    "no usable Duckling CR". Until teardown is synchronous (the documented
  #    async-teardown follow-up) the recreate-same-orgID check belongs in a
  #    slower, dedicated test, not the per-PR smoke.
  log "lifecycle: deprovision $CNPG"
  api_post "$CNPG" deprovision >/dev/null
  wait_state "$CNPG" deleted 600

  # 3. TODO durability: kill the active worker pod mid-session, reconnect, read
  #    back DuckLake data from object storage. Needs `kubectl delete pod
  #    -l app=duckgres-worker` (the Job SA has pod-delete in-ns) + a reconnect
  #    loop. Stubbed until the connect/routing model below is confirmed green.
  log "SKIP durability (TODO)"

  # 4. TODO concurrency: N writers INSERT under DuckLake conflict-retry. Port
  #    from tests/k8s/ducklake_test.go::TestK8sDuckLakeConcurrentWriters.
  log "SKIP concurrency (TODO)"

  # 5. TODO network policy: assert worker egress reaches the cnpg pooler +
  #    lakekeeper:8181 and cross-tenant is denied. The Cilium policies are
  #    cluster-wide and snap on via the duckgres/active-org label — verify the
  #    label is stamped and a denied destination times out.
  log "SKIP netpol (TODO)"

  log "PASS: cnpg + ext DuckLake/Iceberg R/W + cnpg deprovision"
}

main "$@"
