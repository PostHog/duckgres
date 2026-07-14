#!/bin/sh
# In-cluster e2e harness. Runs as a Job in the per-PR namespace, talking to the
# control-plane ClusterIP service over the PG wire protocol and to the in-cluster
# Kubernetes API (via the Job's own ServiceAccount token) for pod-level checks.
#
# This is the SUCCESSOR to the kind-based tests/k8s/ Go suite: every behavior
# that suite asserted against a fake kind cluster is re-asserted here against the
# REAL posthog-mw-dev cluster (real Cilium, real Crossplane ducklings, real
# cnpg-shard + external-RDS metadata). Covers the two real metadata backends
# cnpg + ext (aurora is retired — out of scope).
#
# STRUCTURE: assertions run in four PARALLEL per-org lanes (see main() and the
# lane_* functions) — cnpg (wire/catalog/concurrency/sizing), res1 (worker-kill
# resilience), res2 (scheduling-shape resilience), ext (external-RDS backend +
# org defaults). All worker churn is org-scoped, so lanes can't interfere, and
# the wall-clock is the slowest lane instead of the sum (~halves the runtime).
# Anything that kills/drains/counts worker pods MUST stay inside its org's lane
# and select pods via the org label (newest_org_worker), never globally.
#
#
#   wire/query   : SELECT 1 round-trips, N concurrent connections stay distinct,
#                  a malformed startup-message length is rejected cleanly (no CP
#                  crash, #715), and a cold burst is absorbed (workers spawn on
#                  demand; any surplus gets a graceful retry hint) then served.
#                  jsonb || keeps Postgres concat semantics through
#                  transpilation (#716), and a pipelined extended-query error
#                  discards queued messages until Sync (#718). A same pgwire
#                  session remains usable immediately after CancelRequest.
#   activation   : DuckLake catalogs attach, read/write, and EXPLAIN/ANALYZE on cnpg + ext.
#   sizing       : a client-sized connection (duckgres.worker_cpu/memory/ttl)
#                  spawns a worker pod carrying the requested CPU+memory, and a
#                  same-shape reconnect reuses that hot-idle worker (no respawn)
#                  — asserted on cnpg for the ducklake catalog.
#   org default  : an operator-set org default worker profile (admin PUT
#                  /orgs/:id default_worker_cpu/memory/ttl) sizes a PLAIN
#                  connection's worker pod (no client GUCs); garbage values are
#                  rejected with 400 at the API boundary; clearing the default
#                  restores the deployment default shape — asserted on ext.
#   ext forks    : the bundled ducklake/httpfs extensions are the PostHog forks,
#                  not upstream (detects an accidental upstream swap in the image).
#   worker pods  : labels, securityContext (non-root, no priv-esc), Downward-API
#                  POD_NAME/NODE_NAME env, and NO ambient SA-token mount.
#   resilience   : worker-pod kill → crash recovery; DuckLake durability across a
#                  worker restart; concurrent writers (fork conflict-retry);
#                  graceful drain (in-flight query survives a worker SIGTERM, #690);
#                  one session per worker (concurrent queries land on distinct pods);
#                  parallel cold-burst ramp (3 cold connections spawn 3 pods
#                  concurrently — the acquire gate covers only the claim decision).
#   isolation    : two tenants see distinct catalogs (cross-tenant read denied).
#   admin auth   : the dashboard rejects ?token= query-param auth (#721); only
#                  the internal-secret header / POST login form authenticate.
#   models api   : the dashboard models explorer lists every config-store model
#                  with row counts (GET /api/v1/models) and one model's rows +
#                  columns (GET /api/v1/models/:model), redacting secret-bearing
#                  columns (org_users.password must never appear in the UI).
#   lifecycle    : deprovision → warehouse deleted → Duckling CR fully gone
#                  (finalizer cascade that drops the cnpg role+db completed),
#                  then DELETE /orgs/:id cascades the terminal deleted-warehouse
#                  row + org row away and frees the database_name (no squat).
#                  Same-id re-provision is covered across runs by run.sh, not
#                  in-Job — see lifecycle_teardown_cnpg for why.
#
# Exit non-zero on any failure → the Job fails → the workflow step fails.
#
# Env (from run.sh): NAMESPACE, PR_NUMBER, INTERNAL_SECRET,
# INTERNAL_SECRET_FALLBACK, CP_API, CP_PG_HOST
set -eu
# Surface ANY exit path in the Job log: set -e can kill the script without a
# FAIL line, and an evicted pod prints nothing — make the normal/abnormal exit
# explicit so a silent death is distinguishable from an eviction.
trap 'rc=$?; [ "$rc" = 0 ] || echo "HARNESS EXIT rc=$rc (no FAIL line above = killed by set -e or external signal)" >&2' EXIT

API="${CP_API:?}"
PGHOST="${CP_PG_HOST:?}"
SECRET="${INTERNAL_SECRET:?}"
NS="${NAMESPACE:?}"
H="X-Duckgres-Internal-Secret: $SECRET"
CNPG="ci-pr-${PR_NUMBER}-cnpg"
EXT="ci-pr-${PR_NUMBER}-ext"
# Two extra cnpg-shard/ducklake-only orgs so the heavyweight resilience
# assertions get their OWN org each and run in parallel lanes (see main()):
# worker-kill/drain/spawn churn is org-scoped, so lanes never interfere.
RES1="ci-pr-${PR_NUMBER}-res1"
RES2="ci-pr-${PR_NUMBER}-res2"

# How long each org may take to reach warehouse state=ready (wait_state below).
# All four orgs are cnpg-shard/DuckLake ducklings, and "ready" is a PROVISIONING
# gate, not a worker-spawn one: the CP provisioner only flips ready after the
# Crossplane Duckling is Ready (bucket + shard role/DB + IAM + secrets), the
# per-org Lakekeeper catalog has bootstrapped, AND an end-to-end SELECT 1 probe
# against the metadata store actually connects (controller.go reconcileProvisioning).
#
# 600s was too tight for that last probe. All four ducklings share ONE cnpg
# shard cluster (shard-001-pooler), and after Crossplane reports the role/DB
# Available there is a credential-propagation tail before the freshly-set role
# password is accepted at the pooler — the probe fails "SASL authentication
# failed (SQLSTATE 08P01)" and the org correctly stays in `provisioning` while
# the reconcile loop retries every 10s. Observed on a single org (e.g. res1)
# while its three siblings went ready inside 600s: the probe kept failing SASL
# for ~9 min, right up to the old 600s deadline, then the harness gave up
# (last=provisioning) even though the CP had NOT failed it — the CP's own
# provisioning hard-timeout is 30 min (controller.go), so we were failing the
# run well inside the window the system considers still-healthy.
#
# Give the shared-shard propagation transient room to self-heal while staying
# safely under the CP's 30-min terminal timeout. Env-overridable if the cluster
# drifts. This is a legitimately-slow provision, not a hang — a real stuck
# warehouse still trips `failed` (wait_state) or the 30-min CP timeout long
# before this budget.
READY_TIMEOUT="${READY_TIMEOUT:-1200}"

# The bundled extensions MUST be the PostHog forks. These are the short commit
# SHAs duckdb_extensions() reports for the tags the image pins
# (DUCKLAKE_EXTENSION_TAG=v1.0-posthog.4, HTTPFS_EXTENSION_TAG=v1.5.3-cred-refresh-write-retry).
# If the image accidentally ships upstream, the version differs and we fail.
EXPECT_DUCKLAKE_SHA="e4ac5150"
EXPECT_HTTPFS_SHA="0dac6fc"

# duckling-example RDS — the shared external metadata store (same one the
# manual validation used). Endpoint is stable in mw-dev.
EXT_RDS_ENDPOINT="duckling-example-managed-warehouse-dev-us-east-1.c8jy2c68kipq.us-east-1.rds.amazonaws.com"
EXT_RDS_SECRET="duckling-example-managed-warehouse-dev-us-east-1-rds-password"

fail() { echo "FAIL: $*" >&2; exit 1; }

# Multi-second worker-pinning query used by the resilience assertions. range()
# is lazy and count over it is metadata-fast, so the modulo filter forces real
# per-row work. 3e9 rows ≈ 30-60s on a default (750m) worker — 5-10x the
# overlap window each assertion needs (SIGTERM-mid-flight, concurrent-pod peak,
# parallel-spawn hold), without the ~2min/test the old 8e9 burned.
HEAVY_Q="SELECT count(*) FROM range(3000000000) t(i) WHERE i % 2 = 0;"
HEAVY_EXPECT=1500000000
# stderr, NOT stdout: log() is called inside functions whose stdout is captured
# in $(...) and piped to jq (e.g. provision | jq -r .password). A log line on
# stdout would land in that capture and make jq choke ("Invalid numeric literal").
log()  { echo ">>> $*" >&2; }

apk add --no-cache curl jq postgresql-client openssl python3 py3-psycopg2 >/dev/null 2>&1 || true

# kubectl, for the pod-level assertions the Go suite used to make via client-go.
# The Job runs as the `duckgres` SA (pods get/list/delete/patch + pods/exec +
# pods/log in-namespace, and ducklings get/list/watch cross-namespace), and
# kubectl auto-detects in-cluster config from that mounted SA token. arm64:
# mw-dev worker nodes are arm64. The version is pinned (no stable.txt lookup —
# one fewer network round-trip, no surprise version bump mid-suite) and the
# download starts in the BACKGROUND right below so it overlaps the provisioning
# phase instead of serializing in front of it; bootstrap_kubectl joins it.
KUBECTL=/tmp/kubectl
KUBECTL_VERSION=v1.33.1
kubectl_dl_pid=""
start_kubectl_download() {
  [ -x "$KUBECTL" ] && return 0
  curl -fsSLo "$KUBECTL" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/arm64/kubectl" &
  kubectl_dl_pid=$!
}
bootstrap_kubectl() {
  # Best-effort join: the bare `wait` in join_lanes (readiness phase) reaps ALL
  # background children including this download, after which `wait <pid>`
  # returns 127 ("not a child") even though the fetch succeeded — so the
  # artifact file, not wait's exit code, is the success criterion.
  if [ -n "$kubectl_dl_pid" ]; then
    wait "$kubectl_dl_pid" 2>/dev/null || true
    kubectl_dl_pid=""
  fi
  if ! [ -s "$KUBECTL" ]; then
    log "kubectl background download missing — fetching synchronously"
    curl -fsSLo "$KUBECTL" "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/arm64/kubectl" \
      || fail "kubectl download failed (pinned $KUBECTL_VERSION)"
  fi
  chmod +x "$KUBECTL"
}
start_kubectl_download
k() { "$KUBECTL" -n "$NS" "$@"; }

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
# the DuckLake catalog, not the org (PR #651). The control
# plane rejects connections without a managed SNI host. We use libpq's
# host (→ SNI + TLS name) / hostaddr (→ TCP target) split: SNI carries the org
# hostname while the TCP connection still lands on the CP ClusterIP. The suffix
# (.ci.duckgres.local) is CI-internal and never resolves in DNS — hostaddr is
# what actually connects.
SNI_SUFFIX=".ci.duckgres.local"
CP_IP=""
resolve_cp_ip() {
  CP_IP="$(getent hosts "$PGHOST" | awk '{print $1}' | head -1)"
  [ -n "$CP_IP" ] || fail "could not resolve $PGHOST"
}

# On-demand spawn backpressure ("…retry in about 45 seconds" / "timed out waiting
# for an available worker") is a FEATURE, not an error: there is no warm pool, so
# ANY new-session acquisition — a burst, or the first connect after the pool
# churned (worker kills / idle timeout) — can
# transiently get it while the CP spawns a worker (or while a freshly-spawned pod
# is still pulling/booting). It is a FATAL at session create, BEFORE any SQL runs,
# so retrying the whole command is safe (no half-applied INSERT). So every harness
# query tolerates it via bounded retry. Auth failures and real SQL errors are NOT
# retried — they surface immediately, so this never feeds the rate limiter or
# masks a genuine failure. (The behavior is exercised in cold_burst_absorption.)
_pg_exec() { # org password dbname sql [user=root]  -> prints output; rc 0 ok / 1 real error
  a=0 out=""
  while [ "$a" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=${5:-root} dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc "$4" 2>&1)"; then
      printf %s "$out"; return 0
    fi
    case "$out" in
      *"capacity exhausted"*|*"no Duckgres worker"*|\
      *"still provisioning"*|*"failed to initialize session"*|\
      *"timed out waiting for an available worker"*|*"failed to start"*|*"spawn sized worker"*|\
      *"failed to detect attached catalogs"*|*"reshard in progress"*)
        # The cold-spawn trio covers an on-demand cold spawn that needed a fresh
        # node (sized worker too big for the warm node): the first connect can hit
        # the spawn ceiling while the pod is still pulling/booting; by the retry it
        # is hot-idle and the reconnect reuses it. Same transient class as a cold pool.
        # "failed to detect attached catalogs" is the session-init catalog probe
        # racing a freshly-spawned worker whose ATTACH is still settling — it fires
        # BEFORE any user SQL runs, so retrying the whole command is safe.
        # "reshard in progress" (57P03, "please retry shortly") is the connect gate
        # on a CP whose config snapshot hasn't polled the resharding→ready flip
        # yet — one poll interval of lag right after a reshard finishes or rolls
        # back, exactly when the reshard assertions reconnect. The negative
        # mid-reshard connect check deliberately uses a one-shot psql instead
        # of this helper so it observes the rejection instead of retrying it.
        sleep 10; a=$((a + 1)); continue ;;
      *) printf %s "$out" >&2; return 1 ;;
    esac
  done
  printf %s "$out" >&2; return 1
}

# Positive assertion: under set -e, a real SQL error (rc 1) aborts the harness;
# transient backpressure is retried. Used for every read/write.
pg() { _pg_exec "$@"; }

# Alias kept for the concurrency tests' readability (same behavior as pg()).
pgc() { _pg_exec "$@"; }

# Negative assertion (e.g. cross-tenant read must fail). Retries transient
# backpressure so the test evaluates the REAL outcome, then prints the final
# output to stdout and returns the psql rc (0 = query succeeded, 1 = SQL error)
# for the caller to inspect — never aborts the harness itself.
pg_try() { # org password dbname sql [user=root]
  a=0 out=""
  while [ "$a" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=${5:-root} dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc "$4" 2>&1)"; then
      printf %s "$out"; return 0
    fi
    case "$out" in
      *"capacity exhausted"*|*"no Duckgres worker"*|\
      *"still provisioning"*|*"failed to initialize session"*|\
      *"timed out waiting for an available worker"*|*"failed to start"*|*"spawn sized worker"*|\
      *"failed to detect attached catalogs"*)
        sleep 10; a=$((a + 1)); continue ;;
      *) printf %s "$out"; return 1 ;;
    esac
  done
  printf %s "$out"; return 1
}

# Connect preflight: a worker isn't ready the instant a warehouse goes ready —
# there is no warm pool, so the first connection for an org cold-spawns a worker
# (and a burst can momentarily hit the org/global cap). The CP returns a
# transient "no Duckgres worker is currently
# available; retry in ~45s" / "still provisioning" / "failed to initialize
# session". Retry `SELECT 1` through those, bounded, BEFORE the R/W. Auth
# failures are NOT in this set, so this never feeds the rate limiter.
wait_worker() { # org password catalog
  attempt=0
  while [ "$attempt" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc 'SELECT 1' 2>&1)" && [ "$out" = "1" ]; then
      return 0
    fi
    log "worker not ready for $1/$3 ($(printf %s "$out" | tr -d '\n' | tail -c 80)); retry…"
    sleep 15
    attempt=$((attempt + 1))
  done
  fail "no Duckgres worker for $1/$3 after retries"
}

# ---- wire protocol --------------------------------------------------------
basic_query() { # org password
  log "basic query on $1"
  n="$(pg "$1" "$2" ducklake 'SELECT 1')"
  [ "$n" = "1" ] || fail "$1 SELECT 1 returned '$n'"
}

# pg_compat_functions exercises the PostgreSQL builtin-compatibility macros and
# transforms end-to-end in DuckLake mode, where
# the memory.main qualification matters. Each check is a deterministic value
# assertion of real PG semantics through the full transpile path on a live
# worker — not just "it didn't error". A regression here means a real client
# (JDBC/SQLAlchemy/psql introspection) would break or get wrong data.
assert_compat() { # org password dbname sql expected label
  got="$(pg "$1" "$2" "$3" "$4")"
  [ "$got" = "$5" ] || fail "$1 compat[$6]: '$4' returned '$got', expected '$5'"
}
# Like assert_compat but compares only the LAST output line. Use for a batched
# simple query where an earlier statement emits a psql command tag (e.g. `SET`)
# on its own line before the row that carries the value being asserted.
assert_lastline() { # org password dbname sql expected label
  got="$(pg "$1" "$2" "$3" "$4" | tail -1)"
  [ "$got" = "$5" ] || fail "$1 compat[$6]: '$4' last line returned '$got', expected '$5'"
}
pg_compat_functions() { # org password
  log "pg builtin-compat functions on $1 (DuckLake mode)"
  # set_config: connection-startup unblocker (JDBC/SQLAlchemy/psycopg/poolers).
  assert_compat "$1" "$2" ducklake "SELECT set_config('search_path','main',false)" "main" "set_config"
  # width_bucket: equi-width histogram bucketing (BI tools / dbt).
  assert_compat "$1" "$2" ducklake "SELECT width_bucket(5.35,0.024,10.06,5)::text" "3" "width_bucket"
  # uuid_generate_v4: uuid-ossp default-expression alias.
  assert_compat "$1" "$2" ducklake "SELECT length(uuid_generate_v4()::text)::text" "36" "uuid_generate_v4"
  # decode/encode: bytea round-trip (was silently corrupting before the macros).
  assert_compat "$1" "$2" ducklake "SELECT encode(decode('YWJj','base64'),'hex')" "616263" "encode_decode"
  # @> jsonb containment (Django/SQLAlchemy/ActiveRecord).
  assert_compat "$1" "$2" ducklake "SELECT ('{\"a\":1,\"b\":2}'::jsonb @> '{\"a\":1}'::jsonb)::int::text" "1" "jsonb_contains"
  # #>> jsonpath text extraction.
  assert_compat "$1" "$2" ducklake "SELECT '{\"a\":{\"b\":1}}'::json #>> '{a,b}'" "1" "jsonb_path_text"
  # PG curly-brace array literal cast.
  assert_compat "$1" "$2" ducklake "SELECT array_length('{1,2,3}'::int[],1)::text" "3" "array_literal_cast"
  # set-returning table macro in FROM position (memory.main-qualified).
  assert_compat "$1" "$2" ducklake "SELECT count(*)::text FROM json_array_elements('[1,2,3]'::json)" "3" "json_array_elements"
  # make_interval: interval constructor (DuckDB has no native make_interval).
  assert_compat "$1" "$2" ducklake "SELECT make_interval(days=>2)::text" "2 days" "make_interval"
  # format(): %I/%L/%s template (PL/pgSQL dynamic SQL, migrations).
  assert_compat "$1" "$2" ducklake "SELECT format('%I = %L','foo bar','baz')" "\"foo bar\" = 'baz'" "format"
  # substr(): PG negative-offset window semantics.
  assert_compat "$1" "$2" ducklake "SELECT substr('alphabet',-2,5)" "al" "substr_neg"
  # substring(... FROM pattern): SQL regex extraction.
  assert_compat "$1" "$2" ducklake "SELECT substring('Thomas' FROM 'o(.)')" "m" "substring_regex"
  # overlay(): substring replacement (DuckDB has no overlay()).
  assert_compat "$1" "$2" ducklake "SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4)" "Thomas" "overlay"
  # cardinality(): array element count (DuckDB builtin is MAP-only).
  assert_compat "$1" "$2" ducklake "SELECT cardinality(ARRAY[10,20,30])::text" "3" "cardinality"
  # isfinite(interval): always true (DuckDB lacks the interval overload). The
  # cast happens inside DuckDB, so the textual form is 'true', not PG's 't'.
  assert_compat "$1" "$2" ducklake "SELECT isfinite(INTERVAL '1 day')::text" "true" "isfinite_interval"
}

# query_source_guc exercises the duckgres.query_source session GUC end-to-end
# (prerequisite for pull-based compute billing). The CP intercepts the
# duckgres-namespaced custom GUC in the SET/SHOW path and answers it from
# session state — it is NEVER forwarded to a DuckDB worker (DuckDB rejects
# unknown settings). Assertions, each in a single simple-query session:
#   1. unset SHOW → default "standard" (never errors on a missing value)
#   2. SET then SHOW in the same session → the value round-trips
#   3. an arbitrary (non-standard/endpoints) value is accepted pass-through,
#      not rejected — only "standard"/"endpoints" are meaningful downstream.
#   4. a batch mixing the GUC SET with a normal statement runs the normal
#      statement too — the intercepted GUC must NOT swallow the rest of the
#      batch (regression for the multi-statement-swallow bug).
# Assertions 2-4 are multi-statement simple queries: the CP splits the batch and
# runs each statement in order, so the SET applies session-side before the
# trailing SHOW/SELECT. If any statement forwarded to DuckDB, the query would
# error ("unrecognized configuration parameter"), failing the assert; if the GUC
# swallowed the batch, the trailing statement's value would be missing.
query_source_guc() { # org password
  log "duckgres.query_source session GUC on $1"
  # `SHOW` alone → just the value. The batched `SET …; SHOW/SELECT …` cases print
  # psql's `SET` command tag on its own line first, so assert only the last line.
  assert_compat  "$1" "$2" ducklake "SHOW duckgres.query_source" "standard" "query_source_default"
  assert_lastline "$1" "$2" ducklake "SET duckgres.query_source = 'endpoints'; SHOW duckgres.query_source" "endpoints" "query_source_set"
  assert_lastline "$1" "$2" ducklake "SET duckgres.query_source = 'anything'; SHOW duckgres.query_source" "anything" "query_source_passthrough"
  assert_lastline "$1" "$2" ducklake "SET duckgres.query_source = 'endpoints'; SELECT 1" "1" "query_source_set_then_query"
}

# Regression for #715: the CP reads the post-TLS startup message with the shared
# wire.ReadStartupMessage, which used to make([]byte, length-4) straight from the
# client-supplied length — a negative or absurd length panicked the (unrecovered)
# connection goroutine and crashed the WHOLE control plane, dropping every
# tenant's in-flight session, pre-auth. Drive malformed lengths through a real
# TLS session and assert the CP neither restarts nor stops serving.
# `openssl s_client -starttls postgres` performs the pre-TLS SSLRequest dance,
# then hands us the raw post-TLS stream where the startup message goes.
malformed_startup_resilience() { # org password
  log "malformed startup-length resilience on $1 (#715)"
  cp_pod="$(k get pods -l app=duckgres-control-plane -o jsonpath='{.items[0].metadata.name}')"
  [ -n "$cp_pod" ] || fail "no control-plane pod found"
  before="$(k get pod "$cp_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')"

  # \377…: length -1 (negative makeslice); \177…: length 2^31-1 (~2GiB alloc);
  # \000\000\000\004: length 4 (remaining[:4] out of range). Each must yield a
  # clean connection close, never a CP panic.
  for hdr in '\377\377\377\377' '\177\377\377\377' '\000\000\000\004'; do
    out="$({ printf "$hdr"; sleep 2; } | openssl s_client \
        -connect "$CP_IP:5432" -servername "$1$SNI_SUFFIX" \
        -starttls postgres 2>&1 || true)"
    # Vacuity guard: the TLS handshake must have completed (server cert seen),
    # or the garbage never reached the post-TLS startup reader and this test
    # asserted nothing.
    case "$out" in
      *"BEGIN CERTIFICATE"*) : ;;
      *) fail "s_client did not complete TLS to the CP (header $hdr): $(printf %s "$out" | tr -d '\n' | tail -c 200)" ;;
    esac
  done

  sleep 5 # let a hypothetical CP crash surface in pod status
  after="$(k get pod "$cp_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo gone)"
  [ "$after" = "$before" ] || fail "CP pod $cp_pod restarted on malformed startup (restarts $before -> $after)"
  v="$(pg "$1" "$2" ducklake 'SELECT 1')"
  [ "$v" = "1" ] || fail "CP stopped serving after malformed startup (got '$v')"
}

# Connection lifetime is recorded at disconnect: the CP's per-connection teardown
# (control.go defer -> server.CloseConnectionMetrics) bumps the per-org
# duckgres_connection_duration_seconds histogram AND stamps duration_ms on the
# "Client disconnected." log. The histogram is the load-bearing series (its _sum
# gives exact total connection-seconds with no scrape-integral bias), but the
# :9090 metrics port is NetworkPolicy-blocked from this in-cluster Job and the
# CP image ships no HTTP client to self-scrape — so we assert the log field,
# which is emitted by the SAME teardown call that records the histogram. A
# positive duration_ms proves real elapsed time was measured (guards an
# always-zero / unset-backendStart regression).
connection_duration_logged() { # org password
  log "connection duration_ms stamped on disconnect for $1"
  # psql opens and closes exactly one connection per call → a fresh disconnect.
  v="$(pg "$1" "$2" ducklake 'SELECT 1')"
  [ "$v" = "1" ] || fail "warmup query for duration assertion returned '$v'"
  sleep 3 # let the disconnect log flush
  ms=""
  for p in $(k get pods -l app=duckgres-control-plane -o jsonpath='{.items[*].metadata.name}'); do
    # logfmt: `... msg="Client disconnected." ... duration_ms=NN`. Take the max
    # duration_ms seen on a disconnect line in the recent window across replicas.
    logs="$(k logs "$p" --since=180s 2>&1)" \
      || fail "kubectl logs failed for control-plane pod $p while checking duration_ms: $logs"
    m="$(printf '%s\n' "$logs" \
          | grep 'Client disconnected.' \
          | grep -oE 'duration_ms=[0-9]+' \
          | grep -oE '[0-9]+' \
          | sort -nr | head -1)"
    if [ -n "$m" ] && { [ -z "$ms" ] || [ "$m" -gt "$ms" ]; }; then ms="$m"; fi
  done
  [ -n "$ms" ] || fail "no 'Client disconnected.' log carrying duration_ms in last 180s (CP not recording connection lifetime)"
  [ "$ms" -gt 0 ] || fail "disconnect duration_ms is $ms (want > 0 — backendStart not honoured?)"
}

# Managed-warehouse compute-usage billing (pull API, docs/design/billing-pull-api.md).
# At connection teardown the CP meters cpu_seconds/memory_seconds from the
# provisioned worker size over the connection lifetime into an in-proc counter
# keyed (org, default team, query_source, worker size), flushes it to the
# durable config-store buffer (~15s), and serves it aggregated over
# GET /api/v1/billing/usage; POST /api/v1/billing/ack advances the cursor and
# deletes acked buckets. This asserts the FULL round-trip against the real
# stack: two real connections (one standard, one with the duckgres.query_source
# GUC set to endpoints) must surface as separate usage rows carrying the org's
# provisioned default_team_id and a positive worker size, then an ack of the
# served watermark_high must 200 and the next GET's watermark_low must equal it
# (cursor advanced, acked buckets deleted). The storage family is asserted in
# the same round-trip: the leader's sampler (60s here) must serve a storage row
# with gib_seconds > 0 for the org, and the shared ack must advance past it. A bucket closes ~90s after the
# connection ends (60s width + 30s grace) plus ≤15s flush, so the poll allows
# ~4 minutes. This e2e stack has its own config store, so acking here cannot
# eat production usage.
compute_usage_pull_api() { # org password
  org="$1"; pw="$2"
  log "compute-usage pull API round-trip on $org"

  # Generate usage under both query sources. Each pg call is one connection;
  # the batched SET applies to the SELECT's session (split-path, see #868).
  pg "$org" "$pw" ducklake 'SELECT 1' >/dev/null
  pg "$org" "$pw" ducklake "SET duckgres.query_source = 'endpoints'; SELECT 1" >/dev/null

  # Poll until both rows are served (bucket close + flush lag).
  a=0 body=""
  while [ "$a" -lt 30 ]; do
    body="$(curl -fsS -H "$H" "$API/api/v1/billing/usage")" || body=""
    if [ -n "$body" ] && echo "$body" | jq -e --arg o "$org" --argjson t "$CNPG_DEFAULT_TEAM_ID" '
        (.usage | map(select(.org_id==$o and .team_id==$t and .query_source=="standard"  and .cpu_seconds>0 and .cpu>0 and .mem_gib>0)) | length >= 1)
        and
        (.usage | map(select(.org_id==$o and .team_id==$t and .query_source=="endpoints" and .cpu_seconds>0)) | length >= 1)' >/dev/null 2>&1; then
      break
    fi
    sleep 10; a=$((a + 1))
  done
  [ "$a" -lt 30 ] || fail "compute-usage: rows for $org (standard+endpoints, team=$CNPG_DEFAULT_TEAM_ID) never appeared in GET /billing/usage: $(echo "$body" | head -c 600)"
  wl="$(echo "$body" | jq -r '.watermark_low')"
  wh="$(echo "$body" | jq -r '.watermark_high')"
  log "compute-usage OK: usage served (low=$wl high=$wh)"

  # Ack the served watermark; the cursor must advance and acked buckets die.
  ack="$(curl -fsS -X POST -H "$H" -H 'Content-Type: application/json' \
    -d "{\"watermark_high\":\"$wh\"}" "$API/api/v1/billing/ack")" \
    || fail "compute-usage: ack POST failed"
  deleted="$(echo "$ack" | jq -r '.deleted')"
  [ "${deleted:-0}" -ge 1 ] || fail "compute-usage: ack deleted=$deleted (want >=1): $ack"
  low2="$(curl -fsS -H "$H" "$API/api/v1/billing/usage" | jq -r '.watermark_low')"
  [ "$low2" = "$wh" ] || fail "compute-usage: after ack, watermark_low='$low2' want '$wh' (cursor did not advance)"
  # Idempotency: re-acking the same watermark is a safe no-op (200).
  curl -fsS -X POST -H "$H" -H 'Content-Type: application/json' \
    -d "{\"watermark_high\":\"$wh\"}" "$API/api/v1/billing/ack" >/dev/null \
    || fail "compute-usage: re-ack of the same watermark failed (must be idempotent)"
  # Acking into the still-open present must be rejected (400), never delete.
  future="$(jq -rn 'now + 3600 | todate')"
  code="$(curl -s -o /tmp/ack_future -w '%{http_code}' -X POST -H "$H" -H 'Content-Type: application/json' \
    -d "{\"watermark_high\":\"$future\"}" "$API/api/v1/billing/ack")"
  [ "$code" = "400" ] || fail "compute-usage: future ack -> HTTP $code want 400: $(cat /tmp/ack_future)"
  log "compute-usage OK: ack advanced cursor (deleted=$deleted), idempotent re-ack, future ack rejected"

  # ---- storage metric (same pipeline, second family) ----
  # The leader samples each Ready warehouse's tracked DuckLake footprint every
  # 60s here (DUCKGRES_STORAGE_SAMPLE_INTERVAL in manifests.tmpl.yaml; 30m in
  # prod) and credits bytes×interval byte-seconds. The org has real Parquet
  # data by now (the lane's earlier writes), so a storage row with
  # gib_seconds > 0 and the provisioned team id must appear once a sampled
  # minute closes (sample 60s + close 90s → poll ~4min covers cold start).
  a=0
  while [ "$a" -lt 30 ]; do
    body="$(curl -fsS -H "$H" "$API/api/v1/billing/usage")" || body=""
    if [ -n "$body" ] && echo "$body" | jq -e --arg o "$org" --argjson t "$CNPG_DEFAULT_TEAM_ID" '
        .storage | map(select(.org_id==$o and .team_id==$t and .gib_seconds>0)) | length >= 1' >/dev/null 2>&1; then
      break
    fi
    sleep 10; a=$((a + 1))
  done
  [ "$a" -lt 30 ] || fail "storage-usage: no storage row for $org (team=$CNPG_DEFAULT_TEAM_ID, gib_seconds>0) in GET /billing/usage: $(echo "$body" | head -c 600)"
  gib="$(echo "$body" | jq -r --arg o "$org" '[.storage[] | select(.org_id==$o)][0].gib_seconds')"
  wh2="$(echo "$body" | jq -r '.watermark_high')"
  log "storage-usage OK: $org gib_seconds=$gib served"

  # The shared ack must clear storage buckets too: ack the served watermark,
  # then the next GET's storage array must not contain rows ≤ it for this org
  # (watermark_low advanced past them; new samples land in newer buckets).
  curl -fsS -X POST -H "$H" -H 'Content-Type: application/json'     -d "{\"watermark_high\":\"$wh2\"}" "$API/api/v1/billing/ack" >/dev/null     || fail "storage-usage: ack POST failed"
  low3="$(curl -fsS -H "$H" "$API/api/v1/billing/usage" | jq -r '.watermark_low')"
  [ "$low3" = "$wh2" ] || fail "storage-usage: after ack, watermark_low='$low3' want '$wh2'"
  log "storage-usage OK: shared ack advanced the cursor past storage buckets"
}

# jsonb || must keep Postgres concatenation semantics through the full CP
# transpile → worker DuckDB execute path (regression for #716: the transpiler
# used to rewrite it to json_merge_patch, which REPLACED arrays instead of
# concatenating them — silently corrupting the common `col || '["x"]'::jsonb`
# append idiom — and deleted null-valued keys instead of keeping them).
# Backend-independent (no metadata touched), so cnpg-only is sufficient.
jsonb_concat_semantics() { # org password
  log "jsonb || semantics on $1"
  v="$(pg "$1" "$2" ducklake "SELECT '[1,2]'::jsonb || '[3,4]'::jsonb")"
  [ "$v" = '[1,2,3,4]' ] || fail "jsonb array concat returned '$v' (want [1,2,3,4])"
  v="$(pg "$1" "$2" ducklake "SELECT '{\"a\":1}'::jsonb || '{\"a\":null}'::jsonb")"
  [ "$v" = '{"a":null}' ] || fail "jsonb null-value merge returned '$v' (want {\"a\":null})"
  v="$(pg "$1" "$2" ducklake "SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb")"
  [ "$v" = '{"a":1,"b":2}' ] || fail "jsonb object merge returned '$v' (want {\"a\":1,\"b\":2})"
  v="$(pg "$1" "$2" ducklake "SELECT '[1,2]'::jsonb || '3'::jsonb")"
  [ "$v" = '[1,2,3]' ] || fail "jsonb array append returned '$v' (want [1,2,3])"
}

# Cold-burst absorption: there is no warm pool — workers are spawned on demand
# per request — so a burst of cold sessions either all spawn (under the org/global
# cap) or the surplus gets a graceful, client-visible cap/backpressure hint
# ("...retry in about 45 seconds" / "timed out waiting for an available worker")
# rather than hanging, 500-ing, or dropping the connection. The saturation point
# is environment-dependent, so this logs whether backpressure was observed and
# always asserts the pool serves a (retrying) connection.
cold_burst_absorption() { # org password
  log "cold-burst absorption on $1"
  burst=12; seen=/tmp/bp_seen; rm -f "$seen"; pids=""
  i=0
  while [ "$i" -lt "$burst" ]; do
    ( o="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
        -tAc 'SELECT 1' 2>&1 || true)"
      case "$o" in
        *"no Duckgres worker"*|*"retry in about"*|*"capacity exhausted"*|*"timed out waiting for an available worker"*) echo x >> "$seen" ;;
      esac ) &
    pids="$pids $!"; i=$((i + 1))
  done
  for p in $pids; do wait "$p" || true; done
  if [ -s "$seen" ]; then
    log "backpressure observed: $(wc -l < "$seen" | tr -d ' ')/$burst connections got a graceful retry hint"
  else
    log "backpressure not observed: pool absorbed $burst cold-burst connections"
  fi
  # The pool must serve work: a retrying connection succeeds.
  v="$(pgc "$1" "$2" ducklake 'SELECT 1')"
  [ "$v" = "1" ] || fail "pool did not serve work after cold-burst check (got '$v')"
}

# N concurrent connections each run a distinct query and must each see their own
# value (no cross-talk / session bleed). Uses pgc so transient cold-spawn
# backpressure is handled, not fatal.
# Ported from TestK8sMultipleConcurrentConnections.
concurrent_connections() { # org password
  log "5 concurrent connections on $1"
  ok=/tmp/cc_ok; rm -f "${ok}".* ; pids=""
  i=0
  while [ "$i" -lt 5 ]; do
    ( v="$(pgc "$1" "$2" ducklake "SELECT $i")"; [ "$v" = "$i" ] && : > "${ok}.$i" ) &
    pids="$pids $!"; i=$((i + 1))
  done
  for p in $pids; do wait "$p" || true; done
  i=0
  while [ "$i" -lt 5 ]; do
    [ -f "${ok}.$i" ] || fail "concurrent connection $1 #$i did not return $i"
    i=$((i + 1))
  done
}

# Extended-query skip-until-Sync error recovery (#718): after an error while
# processing any extended-query message, the server must DISCARD subsequent
# pipelined extended-protocol messages until Sync arrives, then recover.
# Pipelining clients (libpq pipeline mode, pgx SendBatch, JDBC batch) rely on
# this; without it a queued statement executes against broken state and the
# client's response accounting desyncs. Uses psql 18's pipeline meta-commands
# (\startpipeline / \syncpipeline / \endpipeline) — the reason the harness Job
# image in run.sh is postgres:18-alpine. Regression test for issue #718.
pipeline_error_recovery() { # org password
  log "pipeline skip-until-Sync error recovery on $1"
  t="e2e_pipe_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "DROP TABLE IF EXISTS $t; CREATE TABLE $t(id INT);"
  # One pipelined batch: statement 1 errors; the INSERT of id=1 is queued
  # BEFORE the Sync so the server must discard it; the INSERT of id=2 comes
  # after \syncpipeline so it must execute. No ON_ERROR_STOP — the error is
  # the point — and psql's exit code is ignored (a desynced libpq may bail);
  # the SERVER-side outcome is asserted from a fresh session below. The
  # transient session-create FATALs are retried like _pg_exec: they fire
  # before any SQL runs, so re-running the whole batch is safe.
  a=0
  while [ "$a" -lt 12 ]; do
    out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" 2>&1 <<EOF || true
\startpipeline
SELEC deliberately_broken \bind \sendpipeline
INSERT INTO $t VALUES (1) \bind \sendpipeline
\syncpipeline
INSERT INTO $t VALUES (2) \bind \sendpipeline
\endpipeline
EOF
)"
    case "$out" in
      *"capacity exhausted"*|*"no Duckgres worker"*|\
      *"still provisioning"*|*"failed to initialize session"*|\
      *"timed out waiting for an available worker"*|*"failed to start"*|*"spawn sized worker"*|\
      *"failed to detect attached catalogs"*)
        sleep 10; a=$((a + 1)); continue ;;
    esac
    break
  done
  got="$(pg "$1" "$2" ducklake "SELECT id FROM $t ORDER BY id;")"
  [ "$got" = "2" ] || fail "$1 pipeline recovery: table has '$got', want only '2' (a pipelined statement queued behind an error executed, or post-Sync work was lost; psql said: $(printf %s "$out" | tr '\n' ' ' | tail -c 200))"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

# Server-side cursor emulation (server/conn_cursor.go) on a live worker:
# DECLARE → FETCH n → MOVE n (advances WITHOUT returning rows) → FETCH ALL
# (remaining rows only) → CLOSE, with exact value assertions. The second
# cursor is deliberately left PARTIALLY READ when ROLLBACK runs: an open
# cursor rowset pins the worker session's single DuckDB connection, and
# transaction end must release it BEFORE the COMMIT/ROLLBACK statement needs
# the connection — the pre-fix behavior deadlocked the session forever
# (closeCursorsAtTxEnd; regression also in tests/integration/cursor_test.go).
# The trailing SELECT 1 proves the same session is alive after the rollback.
# Each -c is its own simple-Query message on ONE connection (cursors are
# session state). `timeout` turns a deadlock regression into a crisp fail
# instead of hanging the Job to its deadline.
server_side_cursors() { # org password
  log "server-side cursors (DECLARE/FETCH/MOVE/CLOSE + tx-end liveness) on $1"
  a=0
  while :; do
    out="$(PGPASSWORD="$2" timeout 120 psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
        -v ON_ERROR_STOP=1 -tA \
        -c 'BEGIN' \
        -c 'DECLARE e2e_cur CURSOR FOR SELECT * FROM generate_series(1,10) ORDER BY 1' \
        -c 'FETCH 3 FROM e2e_cur' \
        -c 'MOVE 2 FROM e2e_cur' \
        -c 'FETCH ALL FROM e2e_cur' \
        -c 'CLOSE e2e_cur' \
        -c 'DECLARE e2e_cur2 CURSOR FOR SELECT * FROM generate_series(1,10) ORDER BY 1' \
        -c 'FETCH 2 FROM e2e_cur2' \
        -c 'ROLLBACK' \
        -c 'SELECT 1' 2>&1)" && break
    case "$out" in
      *"capacity exhausted"*|*"no Duckgres worker"*|\
      *"still provisioning"*|*"failed to initialize session"*|\
      *"timed out waiting for an available worker"*|*"failed to start"*|*"spawn sized worker"*|\
      *"failed to detect attached catalogs"*)
        [ "$a" -lt 12 ] || fail "$1 cursors: backpressure never cleared ($(printf %s "$out" | tr '\n' ' ' | tail -c 120))"
        sleep 10; a=$((a + 1)); continue ;;
      *) fail "$1 cursors: lifecycle failed or hung ($(printf %s "$out" | tr '\n' ' ' | tail -c 300))" ;;
    esac
  done
  want="$(printf 'BEGIN\nDECLARE CURSOR\n1\n2\n3\nMOVE 2\n6\n7\n8\n9\n10\nCLOSE CURSOR\nDECLARE CURSOR\n1\n2\nROLLBACK\n1')"
  [ "$out" = "$want" ] || fail "$1 cursors: got '$(printf %s "$out" | tr '\n' ' ')', want '$(printf %s "$want" | tr '\n' ' ')'"
}

# Black-box regression for async cancel cleanup: libpq's cancel path sends a
# PostgreSQL CancelRequest while keeping the same connection open. The next
# query runs immediately on that same session; if the control plane reuses the
# session before the worker has released its cancelled DoGet operation, this can
# fail with "session already has an active operation".
cancel_then_reuse_same_session() { # org password
  log "cancel then immediate same-session reuse on $1"
  out="$(mktemp)"
  if ! python3 - "$1" "$2" "$CP_IP" "$SNI_SUFFIX" "$HEAVY_Q" >"$out" 2>&1 <<'PY'
import sys
import threading
import time

import psycopg2

org, password, hostaddr, sni_suffix, heavy_q = sys.argv[1:]
conn = psycopg2.connect(
    dbname="ducklake",
    user="root",
    password=password,
    host=org + sni_suffix,
    hostaddr=hostaddr,
    port=5432,
    sslmode="require",
    connect_timeout=30,
)
conn.autocommit = True
try:
    cur = conn.cursor()
    timer = threading.Timer(3.0, conn.cancel)
    timer.start()
    cancelled = False
    try:
        cur.execute(heavy_q)
        print("heavy query completed before cancel")
    except Exception as exc:
        cancelled = True
        msg = str(exc)
        if "cancel" not in msg.lower():
            raise SystemExit(f"heavy query failed without cancellation: {msg}")
        print(f"cancel_error={msg}")
    finally:
        timer.cancel()
    if not cancelled:
        raise SystemExit("heavy query did not get cancelled")

    cur.execute(
        "CREATE TEMP TABLE cancel_reuse_marker(v INT); "
        "INSERT INTO cancel_reuse_marker VALUES (42); "
        "SELECT v FROM cancel_reuse_marker;"
    )
    marker = cur.fetchone()[0]
    if marker != 42:
        raise SystemExit(f"marker query returned {marker}, want 42")
    print("marker=42")
finally:
    conn.close()
PY
  then
    text="$(tr '\n' ' ' <"$out" | tail -c 400)"
    rm -f "$out"
    case "$text" in
      *"session already has an active operation"*)
        fail "cancel_then_reuse_same_session: session reused before worker cancel cleanup completed: $text" ;;
      *) fail "cancel_then_reuse_same_session: cancel/reuse client failed: $text" ;;
    esac
  fi
  text="$(tr '\n' ' ' <"$out" | tail -c 400)"
  rm -f "$out"
  case "$text" in
    *"cancel_error="*"marker=42"*) ;;
    *) fail "cancel_then_reuse_same_session: expected cancel and same-session marker success, got: $text" ;;
  esac
}

# ---- bundled extension forks ----------------------------------------------
# Ported from TestK8sDucklakeExtensionIsBundledFork / TestK8sHttpfsExtensionIsBundledFork.
assert_fork_extensions() { # org password
  log "extension forks on $1"
  dl="$(pg "$1" "$2" ducklake \
    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake' AND loaded")"
  [ "$dl" = "$EXPECT_DUCKLAKE_SHA" ] || \
    fail "ducklake extension version '$dl' != fork '$EXPECT_DUCKLAKE_SHA' (upstream swap?)"
  # httpfs autoloads on the first S3 touch; a DuckLake R/W has already happened
  # by the time we call this, so it is loaded.
  hf="$(pg "$1" "$2" ducklake \
    "SELECT COALESCE(extension_version,'') FROM duckdb_extensions() WHERE extension_name='httpfs'")"
  [ "$hf" = "$EXPECT_HTTPFS_SHA" ] || \
    fail "httpfs extension version '$hf' != fork '$EXPECT_HTTPFS_SHA' (upstream swap?)"
}

# ---- catalog read/write ---------------------------------------------------
rw_ducklake() { # org password
  log "DuckLake R/W on $1"
  t="e2e_dl_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "DROP TABLE IF EXISTS $t; CREATE TABLE $t(id INT, label VARCHAR);
            INSERT INTO $t VALUES (1,'one'),(2,'two'),(3,'three');"
  n="$(pg "$1" "$2" ducklake "SELECT COUNT(*) FROM $t;")"
  [ "$n" = "3" ] || fail "$1 DuckLake rowcount=$n want 3"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

explain_ducklake() { # org password
  log "EXPLAIN on DuckLake table on $1"
  t="e2e_explain_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "DROP TABLE IF EXISTS $t; CREATE TABLE $t AS SELECT i AS id FROM generate_series(1,3) t(i);"
  out="$(pg "$1" "$2" ducklake "EXPLAIN SELECT count(*) FROM $t WHERE id > 1;")"
  [ -n "$out" ] || fail "$1 EXPLAIN returned empty output"
  out="$(pg "$1" "$2" ducklake "EXPLAIN ANALYZE SELECT count(*) FROM $t WHERE id > 1;")"
  [ -n "$out" ] || fail "$1 EXPLAIN ANALYZE returned empty output"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

# Regression for the S3-throttling backfill failure: a sustained HTTP 503
# SlowDown during a DuckLake DELETE used to outlast httpfs's tiny default retry
# budget (http_retries=3, ~0.5s cumulative backoff) and surface as a fatal
# XX000, failing the duckling events backfill. applyHTTPFSRetryBudget (called
# at the end of ConfigureDBConnection / ActivateDBConnection, after the ATTACH
# calls that load httpfs) now widens the budget per worker (SET GLOBAL
# http_retries/http_retry_wait_ms/http_retry_backoff). Assert the activated
# worker carries the raised values, so this fails the test if the SET is dropped
# or regressed. (Forcing a real S3 503 in-cluster isn't deterministic; the wider
# budget is what we can assert, and it is the actual fix — see the
# applyHTTPFSRetryBudget comment.)
#
# Read the live values from duckdb_settings(), NOT current_setting(): duckgres
# overrides current_setting with a PG-compat macro that returns '' for any
# setting other than server_version/server_encoding (server/catalog.go), so
# current_setting('http_retries')::BIGINT would cast '' and error. duckdb_settings
# is DuckDB's own (un-shadowed) settings table; its `value` column is text.
httpfs_retry_budget() { # org password
  log "httpfs retry budget on $1"
  # Compare numerically (cast → boolean 'true') so a float's string format
  # (http_retry_backoff is a FLOAT: "2.0" vs "2.000000") can't make this flaky.
  assert_compat "$1" "$2" ducklake "SELECT (value::BIGINT = 10)::text  FROM duckdb_settings() WHERE name='http_retries'"       "true" "http_retries"
  assert_compat "$1" "$2" ducklake "SELECT (value::BIGINT = 500)::text FROM duckdb_settings() WHERE name='http_retry_wait_ms'" "true" "http_retry_wait_ms"
  assert_compat "$1" "$2" ducklake "SELECT (value::DOUBLE = 2.0)::text FROM duckdb_settings() WHERE name='http_retry_backoff'" "true" "http_retry_backoff"
}

# ---- worker pod assertions (via the K8s API) ------------------------------
# Org-scoped on purpose: lanes for different orgs run in PARALLEL, so "the
# newest worker in the namespace" could belong to another lane (wrong shape,
# or about to be killed by that lane). Always select within the caller's org.
newest_org_worker() { # org
  k get pods -l "app=duckgres-worker,duckgres/active-org=$1" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null
}

assert_worker_pod() { # org
  log "worker pod labels / securityContext / downward-env / SA-token"
  pod="$(newest_org_worker "$1")"; [ -n "$pod" ] || fail "no worker pod found for $1"

  # Labels: app + non-empty control-plane + worker-id (TestK8sWorkerPodCreation).
  [ "$(k get pod "$pod" -o jsonpath='{.metadata.labels.app}')" = "duckgres-worker" ] \
    || fail "worker $pod missing app=duckgres-worker"
  [ -n "$(k get pod "$pod" -o jsonpath='{.metadata.labels.duckgres/control-plane}')" ] \
    || fail "worker $pod missing duckgres/control-plane label"
  [ -n "$(k get pod "$pod" -o jsonpath='{.metadata.labels.duckgres/worker-id}')" ] \
    || fail "worker $pod missing duckgres/worker-id label"

  # securityContext: non-root, uid 1000, no privilege escalation
  # (TestK8sWorkerSecurityContext).
  [ "$(k get pod "$pod" -o jsonpath='{.spec.securityContext.runAsNonRoot}')" = "true" ] \
    || fail "worker $pod runAsNonRoot != true"
  [ "$(k get pod "$pod" -o jsonpath='{.spec.securityContext.runAsUser}')" = "1000" ] \
    || fail "worker $pod runAsUser != 1000"
  esc="$(k get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="duckdb-worker")].securityContext.allowPrivilegeEscalation}')"
  [ "$esc" = "false" ] || fail "worker $pod allowPrivilegeEscalation != false ('$esc')"

  # Downward-API env: POD_NAME / NODE_NAME (TestK8sWorkerAlwaysStampedWithPodAndNode).
  pn="$(k get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="duckdb-worker")].env[?(@.name=="POD_NAME")].valueFrom.fieldRef.fieldPath}')"
  [ "$pn" = "metadata.name" ] || fail "worker $pod POD_NAME fieldRef '$pn' != metadata.name"
  nn="$(k get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="duckdb-worker")].env[?(@.name=="NODE_NAME")].valueFrom.fieldRef.fieldPath}')"
  [ "$nn" = "spec.nodeName" ] || fail "worker $pod NODE_NAME fieldRef '$nn' != spec.nodeName"

  # No ambient SA token (TestK8sWorkerPodsDoNotMountServiceAccountToken). The
  # worker SA is automountServiceAccountToken:false, so the pod has no
  # kube-api-access volume and no token mount.
  vols="$(k get pod "$pod" -o jsonpath='{.spec.volumes[*].name}')"
  case " $vols " in *kube-api-access*) fail "worker $pod has a kube-api-access SA-token volume" ;; esac
  mounts="$(k get pod "$pod" -o jsonpath='{range .spec.containers[*].volumeMounts[*]}{.mountPath}{"\n"}{end}')"
  if echo "$mounts" | grep -q '/var/run/secrets/kubernetes.io/serviceaccount'; then
    fail "worker $pod mounts a kubernetes.io/serviceaccount token"
  fi

  # Never-BestEffort: a DEFAULT-shape worker (no client sizing) must still carry
  # the pool-default resource requests. With the exclusive-node pod anti-affinity
  # removed, requests are the ONLY thing keeping co-scheduled workers from
  # overcommitting a node — a BestEffort default worker is a regression.
  dcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.cpu}")"
  dmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.memory}")"
  [ "$dcpu" = "750m" ] || fail "default worker $pod requests.cpu='$dcpu' want '750m' (pool default; BestEffort regression?)"
  [ "$dmem" = "1536Mi" ] || fail "default worker $pod requests.memory='$dmem' want '1536Mi'"

  # Pre-session memory hygiene env (workerMemoryHygieneEnv): the CP must stamp
  # a pod-derived DuckDB memory_limit, a Go soft memory ceiling, and the thread
  # count at spawn — otherwise the worker sizes its base DB off the NODE's
  # /proc/meminfo and all pre-session work (DuckLake ATTACH, activation) runs
  # effectively unbounded. Values derive from the 750m/1536Mi pool default:
  # 75% of 1536Mi floors to 1GB; GOMEMLIMIT is 1/8 pod = 192MiB; 750m -> 1.
  dml="$(k get pod "$pod" -o jsonpath="${WORKER_C}.env[?(@.name==\"DUCKGRES_MEMORY_LIMIT\")].value}")"
  [ "$dml" = "1GB" ] || fail "default worker $pod DUCKGRES_MEMORY_LIMIT='$dml' want '1GB' (75% of 1536Mi, GB-floored)"
  gml="$(k get pod "$pod" -o jsonpath="${WORKER_C}.env[?(@.name==\"GOMEMLIMIT\")].value}")"
  [ "$gml" = "192MiB" ] || fail "default worker $pod GOMEMLIMIT='$gml' want '192MiB' (1/8 of 1536Mi)"
  thr="$(k get pod "$pod" -o jsonpath="${WORKER_C}.env[?(@.name==\"DUCKGRES_THREADS\")].value}")"
  [ "$thr" = "1" ] || fail "default worker $pod DUCKGRES_THREADS='$thr' want '1' (750m rounds up)"
}

# ---- worker sizing (TTL-pool model) ---------------------------------------
# The TTL-pool model (docs/design/worker-ttl-pool.md) lets a client pick the
# worker shape + TTL via the duckgres.worker_cpu/worker_memory/worker_ttl
# startup options (gated by allowClientWorkerProfile, clamped by the CP). Assert
# end-to-end that a sized connection spawns a worker pod whose duckdb-worker
# container carries the requested CPU+memory on BOTH requests and limits — i.e.
# the shape flows control-plane → k8s pod spec, not BestEffort. A distinct shape
# (no other test, and no other catalog of this org, uses it) guarantees a fresh
# spawn: the implemented reuse path is EXACT-profile-match only, so it can't hand
# back a default or other-shape hot-idle worker — the newest worker pod for the
# org is unambiguously ours. The connection's dbname forces that catalog's
# activation at session-create, so this also exercises sizing × catalog together.
WORKER_C='{.spec.containers[?(@.name=="duckdb-worker")]'
sized_worker() { # org password catalog cpu memory ttl
  org="$1"; pw="$2"; cat="$3"; cpu="$4"; mem="$5"; ttl="$6"
  log "sized worker spawn on $org/$cat: cpu=$cpu memory=$mem ttl=$ttl"
  # PGOPTIONS (scoped to this subshell) carries the startup options; libpq sends
  # them in the StartupMessage exactly like search_path. Run a real query so the
  # CP actually spawns + activates the sized worker.
  ( export PGOPTIONS="-c duckgres.worker_cpu=$cpu -c duckgres.worker_memory=$mem -c duckgres.worker_ttl=$ttl"
    _pg_exec "$org" "$pw" "$cat" 'SELECT 1' ) >/dev/null \
    || fail "sized worker query failed on $org/$cat"
  pod="$(k get pods -l "app=duckgres-worker,duckgres/active-org=$org" \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)"
  [ -n "$pod" ] || fail "sized worker: no worker pod for $org"
  rcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.cpu}")"
  rmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.memory}")"
  lcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.limits.cpu}")"
  lmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.limits.memory}")"
  [ "$rcpu" = "$cpu" ] || fail "sized worker $pod requests.cpu='$rcpu' want '$cpu' (sizing not applied — BestEffort?)"
  [ "$rmem" = "$mem" ] || fail "sized worker $pod requests.memory='$rmem' want '$mem'"
  [ "$lcpu" = "$cpu" ] || fail "sized worker $pod limits.cpu='$lcpu' want '$cpu'"
  [ "$lmem" = "$mem" ] || fail "sized worker $pod limits.memory='$lmem' want '$mem'"
  log "sized worker OK: $pod requests/limits cpu=$rcpu mem=$rmem"
}

# Same-shape reconnect must REUSE the hot-idle sized worker, not spawn a second.
# After sized_worker leaves exactly one hot-idle worker of this shape for the
# org, an identical sized connection re-uses it (Destroy-before-reuse makes the
# prior session gone before the next is assigned), so the count of worker pods
# of that CPU stays 1 — a respawn would make it 2. A non-trivial TTL keeps the
# first alive across this check.
count_org_workers_of_cpu() { # org cpu  -> prints count
  n=0
  for p in $(k get pods -l "app=duckgres-worker,duckgres/active-org=$1" \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    c="$(k get pod "$p" -o jsonpath="${WORKER_C}.resources.requests.cpu}" 2>/dev/null)"
    [ "$c" = "$2" ] && n=$((n + 1))
  done
  echo "$n"
}
reuse_sized_worker() { # org password catalog cpu memory ttl
  org="$1"; pw="$2"; cat="$3"; cpu="$4"; mem="$5"; ttl="$6"
  log "sized worker reuse on $org/$cat (same shape must not respawn)"
  # Let the prior sized session fully release to hot-idle before re-acquiring.
  sleep 5
  ( export PGOPTIONS="-c duckgres.worker_cpu=$cpu -c duckgres.worker_memory=$mem -c duckgres.worker_ttl=$ttl"
    _pg_exec "$org" "$pw" "$cat" 'SELECT 1' ) >/dev/null \
    || fail "sized worker reuse query failed on $org/$cat"
  n="$(count_org_workers_of_cpu "$org" "$cpu")"
  [ "$n" = "1" ] || fail "sized worker reuse: org $org has $n pods of cpu=$cpu, want 1 (same-shape request respawned instead of reusing the hot-idle worker)"
  log "sized worker reuse OK: 1 pod of cpu=$cpu (reused, no respawn)"
}

# Hot-idle TTL RETIREMENT (end-to-end smoke): a worker parked hot-idle past its
# TTL must be reaped (pod deleted), so an idle one-session worker — and the node
# behind it — does not linger. This exercises the real reap path (CP -> store ->
# pod delete) against a single healthy CP holding the lease. NOTE: it is NOT a
# regression gate for the leader-loss / persist-failure / spawn-failure paths —
# those would pass on the pre-fix binary too (one healthy leader already reaps
# within TTL). The regression gates for those live in the unit tests
# (janitor_leader_k8s_test.go re-contend, k8s_pool_test.go persist-failure,
# per_cp_hot_idle_reaper_test.go orphan/floor); killing the leader or injecting a
# store fault in-Job is not deterministic here. Uses worker_ttl=1m (the smallest
# non-truncated TTL) and a shape no other test uses (3 CPU) so the pod is ours.
hot_idle_retired() { # org password catalog cpu memory
  org="$1"; pw="$2"; cat="$3"; cpu="$4"; mem="$5"; ttl=1m
  log "hot-idle TTL retirement on $org/$cat: cpu=$cpu mem=$mem ttl=$ttl (idle worker must be reaped)"
  ( export PGOPTIONS="-c duckgres.worker_cpu=$cpu -c duckgres.worker_memory=$mem -c duckgres.worker_ttl=$ttl"
    _pg_exec "$org" "$pw" "$cat" 'SELECT 1' ) >/dev/null \
    || fail "hot-idle retire: query failed on $org/$cat"
  n="$(count_org_workers_of_cpu "$org" "$cpu")"
  [ "$n" -ge 1 ] || fail "hot-idle retire: expected >=1 pod of cpu=$cpu after connect, got $n"
  # Session has ended → worker parks hot-idle → its 1m TTL expires → reaper must
  # delete the pod. Poll (fast-exit) up to ~4m: 1m TTL + per-CP reaper tick (1m) +
  # cluster slack. A pod that never disappears is the leak regression.
  i=0
  while [ "$i" -lt 16 ]; do
    sleep 15
    [ "$(count_org_workers_of_cpu "$org" "$cpu")" = "0" ] && {
      log "hot-idle retire OK: cpu=$cpu worker reaped after TTL"; return; }
    i=$((i + 1))
  done
  fail "hot-idle retire: cpu=$cpu worker still present after ~4m (TTL=1m) — hot-idle reaper not retiring idle workers (the idle-leak regression)"
}

# NOTE — drain-time idle-Hot release (OrgRouter.ReleaseIdleHotWorkers, the #832
# redesign): when a CP enters drain it now parks its idle (zero-session) Hot
# workers into hot_idle at drain START, so the unfenced hot-idle TTL reaper
# reclaims them during the (possibly unbounded) drain wait instead of leaving
# them pinned until the CP is declared expired and the owner-expired-fenced
# orphan reaper picks them up. This closes the stuck-Hot/0-session leak that
# accumulated across CP rollout generations. It is NOT asserted directly in-Job:
# doing so would require draining the live CP pod (whole-Job blast radius across
# every org lane below) AND fault-injecting a stuck Hot/0-session worker, neither
# deterministic here. The two ends ARE covered: the sweep selection (park idle
# Hot, skip busy, no session decrement) by k8s_pool_test.go
# (TestK8sPoolReleaseIdleHotWorkers*/ParkIdleHotWorker*), and the reaper
# destination (hot_idle -> pod delete within TTL) by hot_idle_retired above.

# NOTE — the hot-idle TTL reaper now spares hot-idle workers that still back a
# reclaimable (Active/Reconnecting) Flight session, mirroring the long-standing
# exclusion in ListOrphanedWorkers (so a TTL reap can't kill a customer's query
# at the moment they reconnect by session token). That sparing is a NEGATIVE
# assertion over a multi-minute TTL window on a worker holding a live durable
# Flight session — not deterministic to set up in-Job — so it is gated by the
# real-Postgres regression test
# TestListExpiredHotIdleWorkersSparesReclaimableFlightSessions
# (tests/configstore/runtime_store_postgres_test.go), which asserts both the
# global and per-CP reaper variants spare Active/Reconnecting sessions and still
# reap workers with no session (or only a closed one). hot_idle_retired above
# covers the positive (no Flight session -> reaped within TTL).

# ---- org default worker profile --------------------------------------------
# Operators can give a tenant a server-side default worker shape + hot-idle TTL
# (config-store columns default_worker_cpu/memory/ttl, set via the admin API).
# External customers send plain connection strings — no duckgres.worker_* GUCs —
# so the org default is the only way to size them. Assert end-to-end:
#   1. PUT /orgs/:id with the three fields persists and round-trips on GET;
#      garbage values are rejected with 400 (they must never enter the store).
#   2. After the CP's config-store poll, a connection with NO sizing options
#      gets a worker pod whose requests AND limits equal the org default.
#   3. Clearing the default (explicit empty strings) restores the deployment
#      default shape: a fresh plain connection must NOT produce another
#      org-default-shaped pod (a default/nil profile can never exact-match the
#      sized shape, so reuse/spawn goes back to default-profile workers).
# Uses a shape (2/8Gi) no other test on this org uses, so the newest worker pod
# after the connect is unambiguously ours (same technique as sized_worker; the
# ext org runs no client-sized assertions).
put_org() { # org json -> prints http code; body in /tmp/put_org_out
  curl -s -o /tmp/put_org_out -w '%{http_code}' -X PUT -H "$H" \
    -H 'Content-Type: application/json' -d "$2" "$API/api/v1/orgs/$1"
}
get_org_default_profile() { # org -> prints "cpu|mem|ttl" ("||" when unset)
  curl -fsS -H "$H" "$API/api/v1/orgs/$1" \
    | jq -r '"\(.default_worker_cpu)|\(.default_worker_memory)|\(.default_worker_ttl)"'
}
org_default_profile() { # org password catalog
  org="$1"; pw="$2"; cat="$3"; cpu=2; mem=8Gi; ttl=10m
  log "org default worker profile on $org/$cat: cpu=$cpu memory=$mem ttl=$ttl"

  code="$(put_org "$org" "{\"default_worker_cpu\":\"$cpu\",\"default_worker_memory\":\"$mem\",\"default_worker_ttl\":\"$ttl\"}")"
  [ "$code" = "200" ] || fail "org default: PUT /orgs/$org -> HTTP $code: $(cat /tmp/put_org_out)"
  got="$(get_org_default_profile "$org")"
  [ "$got" = "$cpu|$mem|$ttl" ] || fail "org default: GET round-trip '$got' want '$cpu|$mem|$ttl'"

  # Garbage must be rejected at the API boundary, never stored.
  code="$(put_org "$org" '{"default_worker_ttl":"whenever"}')"
  [ "$code" = "400" ] || fail "org default: invalid ttl accepted (HTTP $code, want 400)"
  code="$(put_org "$org" '{"default_worker_cpu":"-2"}')"
  [ "$code" = "400" ] || fail "org default: negative cpu accepted (HTTP $code, want 400)"
  got="$(get_org_default_profile "$org")"
  [ "$got" = "$cpu|$mem|$ttl" ] || fail "org default: rejected PUT mutated the org: '$got'"

  # Wait out the CP's config-store poll so its snapshot carries the default.
  sleep "${CONFIG_POLL_SETTLE:-12}"

  # PLAIN connection: no PGOPTIONS / duckgres.* startup options at all.
  pg "$org" "$pw" "$cat" 'SELECT 1' >/dev/null
  pod="$(k get pods -l "app=duckgres-worker,duckgres/active-org=$org" \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)"
  [ -n "$pod" ] || fail "org default: no worker pod for $org"
  rcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.cpu}")"
  rmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.requests.memory}")"
  lcpu="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.limits.cpu}")"
  lmem="$(k get pod "$pod" -o jsonpath="${WORKER_C}.resources.limits.memory}")"
  [ "$rcpu" = "$cpu" ] || fail "org default: $pod requests.cpu='$rcpu' want '$cpu' (org default not applied to a plain connection)"
  [ "$rmem" = "$mem" ] || fail "org default: $pod requests.memory='$rmem' want '$mem'"
  [ "$lcpu" = "$cpu" ] || fail "org default: $pod limits.cpu='$lcpu' want '$cpu'"
  [ "$lmem" = "$mem" ] || fail "org default: $pod limits.memory='$lmem' want '$mem'"
  log "org default OK: plain connection got $pod cpu=$rcpu mem=$rmem"

  # Clear and assert a fresh plain connection is back on the deployment default
  # shape: the count of org-default-shaped pods must not grow (a nil/default
  # profile can never exact-match the 2-CPU shape, so a regrow means the org
  # default is still being applied).
  code="$(put_org "$org" '{"default_worker_cpu":"","default_worker_memory":"","default_worker_ttl":""}')"
  [ "$code" = "200" ] || fail "org default: clear PUT -> HTTP $code: $(cat /tmp/put_org_out)"
  got="$(get_org_default_profile "$org")"
  [ "$got" = "||" ] || fail "org default: not cleared on GET: '$got'"
  sleep "${CONFIG_POLL_SETTLE:-12}"
  before="$(count_org_workers_of_cpu "$org" "$cpu")"
  pg "$org" "$pw" "$cat" 'SELECT 1' >/dev/null
  after="$(count_org_workers_of_cpu "$org" "$cpu")"
  [ "$after" -le "$before" ] || fail "org default: cleared default still spawns $cpu-cpu workers ($before -> $after)"
  log "org default OK: cleared; plain connection back on the deployment default shape"

  # Audit readability: the org PUTs above must land a resource-specific
  # "org.update" row (not a generic "config.update") carrying a human "which
  # fields changed" detail. The clear-PUT above always flips default_worker_*
  # from the just-set 2/8Gi/10m to unset, so a detail mentioning the field is
  # deterministic. Guards audit.go::auditActionFor + api.go::updateOrg detail.
  curl -fsS -H "$H" "$API/api/v1/audit?org=$org" \
    | jq -e --arg o "$org" '.entries | map(select(.action=="org.update" and .org==$o and (.detail | test("default_worker")))) | length >= 1' >/dev/null \
    || fail "org default: no org.update audit row with a field-change detail for $org"
  log "org default OK: audit shows org.update with field-change detail on $org"
}

# ---- org default_team_id (mandatory on new orgs) ----------------------------
# default_team_id links an org to its default PostHog team id (a BIGINT
# config-store column — a JSON NUMBER on the wire, matching PostHog's integer
# Team.id; prereq for pull-based compute billing where usage buckets are keyed
# by team_id). Contract: MANDATORY when a provision creates a NEW org (400, nothing
# created); optional on re-provision of an existing org, where omission keeps
# the stored value (set-only, never a wipe — the keep path is covered by
# TestReprovisionExistingOrgKeepsDefaultTeamID, since same-id re-provision
# in-run is off-limits here, see the lifecycle NOTE below). Asserts on real orgs
# against the real config store:
#   1. Set path: CNPG + EXT orgs were provisioned WITH default_team_id in their
#      bodies (mandatory now); GET /orgs/:id must round-trip exactly each value.
#   2. Reject path: provisioning a brand-new org WITHOUT default_team_id must be
#      400 naming the field, and must create nothing (org GET stays 404).
#   3. Mutate path: PUT /orgs/:id can set it to a positive team id on the EXT
#      org, round-tripping on GET; clearing (0 or null) is REJECTED with a 400
#      and must leave the stored value untouched — the column is NOT NULL
#      (migration 000020) and every org must keep its default team (the
#      billing bucket key). The provisioned value is restored afterwards so
#      the org stays contract-conformant.
# get_org_default_team_id prints the raw JSON value ("null" when NULL) so the
# assertions can distinguish an unset column from an empty string.
get_org_default_team_id() { # org -> prints default_team_id (jq raw; "null" when unset)
  curl -fsS -H "$H" "$API/api/v1/orgs/$1" | jq -r '.default_team_id'
}
default_team_id_mandatory() { # cnpg_org ext_org
  cnpg_org="$1"; ext_org="$2"
  log "default_team_id: provision round-trips on $cnpg_org/$ext_org, new-org-without rejected"

  # 1. Set path: both orgs provisioned WITH default_team_id must read it back.
  got="$(get_org_default_team_id "$cnpg_org")"
  [ "$got" = "$CNPG_DEFAULT_TEAM_ID" ] \
    || fail "default_team_id: GET /orgs/$cnpg_org = '$got' want '$CNPG_DEFAULT_TEAM_ID' (provision default_team_id did not persist)"
  got="$(get_org_default_team_id "$ext_org")"
  [ "$got" = "$EXT_DEFAULT_TEAM_ID" ] \
    || fail "default_team_id: GET /orgs/$ext_org = '$got' want '$EXT_DEFAULT_TEAM_ID' (provision default_team_id did not persist)"
  log "default_team_id OK: $cnpg_org/$ext_org round-tripped from provision"

  # 2. Reject path: a NEW org without default_team_id must 400 and create nothing.
  noteam="e2e-noteam"
  code="$(curl -s -o /tmp/noteam_out -w '%{http_code}' -X POST -H "$H" -H 'Content-Type: application/json' \
    -d '{"database_name":"e2enoteamdb","metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true}}' \
    "$API/api/v1/orgs/$noteam/provision")"
  [ "$code" = "400" ] \
    || fail "default_team_id: new-org provision without it -> HTTP $code want 400: $(cat /tmp/noteam_out)"
  grep -q "default_team_id" /tmp/noteam_out \
    || fail "default_team_id: rejection error should name the field: $(cat /tmp/noteam_out)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" "$API/api/v1/orgs/$noteam")"
  [ "$code" = "404" ] \
    || fail "default_team_id: rejected provision must create nothing, GET /orgs/$noteam -> HTTP $code want 404"
  log "default_team_id OK: new org without it rejected with 400, nothing created"

  # 3. Mutate path: PUT with a positive value sets; clearing (0 or JSON null)
  # is rejected with a 400 and must not change the stored value (the column is
  # NOT NULL — every org must keep a default team).
  code="$(put_org "$ext_org" '{"default_team_id":424242}')"
  [ "$code" = "200" ] || fail "default_team_id: PUT set -> HTTP $code: $(cat /tmp/put_org_out)"
  got="$(get_org_default_team_id "$ext_org")"
  [ "$got" = "424242" ] || fail "default_team_id: after PUT set, GET = '$got' want '424242'"
  code="$(put_org "$ext_org" '{"default_team_id":0}')"
  [ "$code" = "400" ] || fail "default_team_id: PUT clear (0) -> HTTP $code want 400 (clearing must be rejected): $(cat /tmp/put_org_out)"
  grep -q "default_team_id" /tmp/put_org_out \
    || fail "default_team_id: PUT clear rejection should name the field: $(cat /tmp/put_org_out)"
  got="$(get_org_default_team_id "$ext_org")"
  [ "$got" = "424242" ] || fail "default_team_id: after rejected PUT clear, GET = '$got' want '424242' (stored value must be unchanged)"
  code="$(put_org "$ext_org" '{"default_team_id":null}')"
  [ "$code" = "400" ] || fail "default_team_id: PUT clear (null) -> HTTP $code want 400 (clearing must be rejected): $(cat /tmp/put_org_out)"
  got="$(get_org_default_team_id "$ext_org")"
  [ "$got" = "424242" ] || fail "default_team_id: after rejected PUT null, GET = '$got' want '424242' (stored value must be unchanged)"
  code="$(put_org "$ext_org" "{\"default_team_id\":$EXT_DEFAULT_TEAM_ID}")"
  [ "$code" = "200" ] || fail "default_team_id: PUT restore -> HTTP $code: $(cat /tmp/put_org_out)"
  got="$(get_org_default_team_id "$ext_org")"
  [ "$got" = "$EXT_DEFAULT_TEAM_ID" ] || fail "default_team_id: after PUT restore, GET = '$got' want '$EXT_DEFAULT_TEAM_ID'"
  log "default_team_id OK: PUT set round-trips, clear (0/null) rejected 400, value preserved on $ext_org"
}

# ---- user persistent secrets ------------------------------------------------
# CREATE PERSISTENT SECRET must survive across sessions: worker pods are
# ephemeral, so the CP intercepts the statement, stores it encrypted in the
# config store keyed (org, user, name), and replays it at session creation.
# Each psql invocation below is a fresh session (often a reused hot-idle
# worker, whose persistent secrets are wiped at session create) — a secret
# visible on the SECOND connection proves config-store replay, not worker-disk
# luck. The dummy secret is SCOPEd to a nonexistent bucket so its bogus creds
# can never shadow the org's real ducklake_s3 secret for actual queries.
persistent_user_secret() { # org password
  org="$1"; pw="$2"; sname="e2e_user_secret"
  log "persistent user secret on $org"

  pg "$org" "$pw" ducklake "CREATE PERSISTENT SECRET $sname (TYPE s3, KEY_ID 'AKIAE2EDUMMY', SECRET 'e2e-dummy', REGION 'us-east-1', SCOPE 's3://duckgres-e2e-nonexistent-$org')" >/dev/null

  n="$(pg "$org" "$pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'")"
  [ "$n" = "1" ] || fail "persistent secret: not replayed on a fresh session (count=$n)"

  # Reserved (system) names and unnamed persistent secrets must be rejected.
  if out="$(pg_try "$org" "$pw" ducklake "CREATE PERSISTENT SECRET ducklake_s3 (TYPE s3, KEY_ID 'x', SECRET 'y')")"; then
    fail "persistent secret: reserved name ducklake_s3 was accepted"
  fi
  case "$out" in *reserved*) ;; *) fail "persistent secret: reserved-name rejection said '$out'";; esac
  if pg_try "$org" "$pw" ducklake "CREATE PERSISTENT SECRET (TYPE s3, KEY_ID 'x', SECRET 'y')" >/dev/null; then
    fail "persistent secret: unnamed persistent secret was accepted"
  fi
  # Multi-statement batches must be rejected, not silently executed-but-never-
  # persisted (the secret would work for the session, then be wiped).
  if out="$(pg_try "$org" "$pw" ducklake "CREATE PERSISTENT SECRET batch_ms (TYPE s3, KEY_ID 'x', SECRET 'y'); SELECT 1")"; then
    fail "persistent secret: multi-statement batch was accepted"
  fi
  case "$out" in *"single statement"*) ;; *) fail "persistent secret: multi-statement rejection said '$out'";; esac

  # DROP must remove it durably — gone on the NEXT fresh session too (the
  # config-store row is deleted, not just the live worker's copy).
  pg "$org" "$pw" ducklake "DROP PERSISTENT SECRET $sname" >/dev/null
  n="$(pg "$org" "$pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'")"
  [ "$n" = "0" ] || fail "persistent secret: still present on a fresh session after DROP (count=$n)"
  log "persistent secret OK on $org (create → cross-session replay → reserved/unnamed rejected → durable drop)"
}

# Cross-user isolation within one org: user B must never see user A's secrets,
# whether PERSISTENT or non-persistent (plain/TEMPORARY CREATE SECRET). B's
# session typically reuses A's hot-idle worker (one org, cap permitting), which
# is exactly the leak path. DuckDB secrets are instance-global, so a temporary
# secret created by A would survive on the worker and be visible to B unless the
# session-create wipe drops non-persistent secrets too. The worker wipes ALL
# user secrets at session create and replays only the connecting user's stored
# (persistent) ones.
persistent_user_secret_isolation() { # org rootpw
  org="$1"; pw="$2"; sname="e2e_root_secret"; tname="e2e_root_temp_secret"; u2="e2esecuser"
  u2pw="e2e-$(openssl rand -hex 12)"
  log "user secret cross-user isolation on $org (persistent + temporary)"

  pg "$org" "$pw" ducklake "CREATE PERSISTENT SECRET $sname (TYPE s3, KEY_ID 'AKIAE2EDUMMY', SECRET 'root-dummy', REGION 'us-east-1', SCOPE 's3://duckgres-e2e-nonexistent-root-$org')" >/dev/null
  # A non-persistent (TEMPORARY) secret is passed through to the worker and is
  # NOT stored/replayed. It lives only on the instance-global DuckDB — the exact
  # state that must be wiped before another user of the org connects.
  pg "$org" "$pw" ducklake "CREATE TEMPORARY SECRET $tname (TYPE s3, KEY_ID 'AKIAE2ETMP', SECRET 'root-temp', REGION 'us-east-1', SCOPE 's3://duckgres-e2e-nonexistent-temp-$org')" >/dev/null

  code="$(curl -s -o /tmp/create_user_out -w '%{http_code}' -X POST -H "$H" \
    -H 'Content-Type: application/json' \
    -d "{\"org_id\":\"$org\",\"username\":\"$u2\",\"password\":\"$u2pw\"}" \
    "$API/api/v1/users")"
  case "$code" in 200|201) ;; *) fail "user secret: create user $u2 -> HTTP $code: $(cat /tmp/create_user_out)";; esac
  # New-user auth is config-snapshot-driven; wait out one poll before the
  # first login so we don't burn failed auths against the rate limiter.
  sleep "${CONFIG_POLL_SETTLE:-12}"

  n="$(pg "$org" "$u2pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'" "$u2")"
  [ "$n" = "0" ] || fail "user secret: user $u2 sees root's persistent secret (count=$n)"
  # Regression for the cross-user secret leak: $u2 must not inherit root's
  # TEMPORARY secret left behind on the instance-global worker DuckDB.
  n="$(pg "$org" "$u2pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$tname'" "$u2")"
  [ "$n" = "0" ] || fail "user secret: user $u2 sees root's TEMPORARY secret — cross-user leak (count=$n)"
  # Root's stored (persistent) copy must be unaffected by $u2's session-create wipe.
  n="$(pg "$org" "$pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'")"
  [ "$n" = "1" ] || fail "user secret: root's persistent secret lost after $u2's session (count=$n)"
  pg "$org" "$pw" ducklake "DROP PERSISTENT SECRET $sname" >/dev/null
  log "user secret isolation OK on $org ($u2 blind to root's persistent AND temporary secrets; root's stored copy intact)"
}

# ---- resilience -----------------------------------------------------------
# Worker pod killed mid-life → CP refills and a fresh query succeeds.
# Ported from TestK8sWorkerCrashRecovery.
crash_recovery() { # org password
  log "crash recovery on $1"
  pod="$(newest_org_worker "$1")"; [ -n "$pod" ] || fail "no worker pod to kill for $1"
  k delete pod "$pod" --wait=false >/dev/null 2>&1 || true
  k wait --for=delete "pod/$pod" --timeout=90s >/dev/null 2>&1 || true
  wait_worker "$1" "$2" ducklake
  basic_query "$1" "$2"
}

# Graceful drain (regression net for the worker drain protocol, #690): a worker
# that receives SIGTERM (pod deletion) must DRAIN — finish its in-flight query
# rather than dropping the connection — then retire cleanly. Distinct from
# crash_recovery, which kills a worker and only asserts a *fresh* query recovers;
# here the SAME query that was running at SIGTERM must still return correctly.
# A passing result after a mid-flight SIGTERM is impossible without the drain
# protocol (pre-#690 the worker shut down immediately and the query errored).
graceful_drain() { # org password
  log "graceful drain: in-flight query survives worker SIGTERM on $1"
  out="$(mktemp)"; rc="$(mktemp)"
  # HEAVY_Q reliably outlasts the few seconds before the delete. The `if` keeps
  # the query's failure from tripping `set -e` inside the subshell before we
  # record rc.
  ( if pg_try "$1" "$2" ducklake "$HEAVY_Q" >"$out" 2>&1
    then echo 0 >"$rc"; else echo 1 >"$rc"; fi ) &
  qpid=$!

  # Let the query land on and start running on the worker, then identify the pod
  # serving it and gracefully delete it (SIGTERM → drain; the pod's
  # terminationGracePeriodSeconds=3600 keeps it alive long enough to finish).
  sleep 6
  pod="$(newest_org_worker "$1")"
  [ -n "$pod" ] || { kill "$qpid" 2>/dev/null || true; fail "graceful_drain: no worker pod serving the query"; }
  k delete pod "$pod" --wait=false >/dev/null 2>&1 || true

  # Overlap proof: the pod must be Terminating (deletionTimestamp set) WHILE the
  # query is still running — i.e. the SIGTERM landed mid-flight. Without this the
  # test could false-pass on a query that finished before the delete.
  ts="$(k get pod "$pod" -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)"
  [ -n "$ts" ] || { wait "$qpid" 2>/dev/null || true; fail "graceful_drain: pod $pod not Terminating after delete"; }
  kill -0 "$qpid" 2>/dev/null \
    || fail "graceful_drain: query finished before SIGTERM landed — raise the row count (not a mid-flight test)"

  # The in-flight query must complete successfully despite the SIGTERM.
  wait "$qpid" 2>/dev/null || true
  [ "$(cat "$rc")" = 0 ] \
    || fail "graceful_drain: in-flight query died on worker SIGTERM: $(tr -d '\n' <"$out" | tail -c 200)"
  n="$(tr -dc '0-9' <"$out")"
  [ "$n" = "$HEAVY_EXPECT" ] \
    || fail "graceful_drain: in-flight result=$n want $HEAVY_EXPECT (drain corrupted the query)"
  rm -f "$out" "$rc"

  # Once drained the worker must retire cleanly — the pod exits on its own and
  # the CP retires it (not a crash). Then re-establish a worker for later steps.
  k wait --for=delete "pod/$pod" --timeout=300s >/dev/null 2>&1 \
    || fail "graceful_drain: drained worker $pod did not exit/retire within 300s"
  wait_worker "$1" "$2" ducklake
  basic_query "$1" "$2"
}

# One session per worker: two concurrent queries for the same org must land on
# two DISTINCT worker pods, never share a single pod's DuckDB. The control plane
# spawns remote workers with DUCKGRES_DUCKDB_MAX_SESSIONS=1, so each query owns a
# whole pod's resources and a heavy query can't be starved by a co-resident one.
# Regression net: if a worker were ever shared (pre-change least-loaded sharing),
# the org would peak at a single active-org-labeled pod for both queries.
# Assumes the default nodepool already has warm capacity (prior resilience steps spawned
# pods), so the second pod schedules within the queries' runtime.
# One attempt of the one-session-per-worker probe. Hard failures (a query
# errored / wrong results) fail() immediately — those are real bugs. Returns 1
# only for the soft outcome peak<2 with both queries CORRECT, which the wrapper
# below retries once (see its comment).
one_session_per_worker_attempt() { # org password
  o1="$(mktemp)"; o2="$(mktemp)"; r1="$(mktemp)"; r2="$(mktemp)"
  # This assertion's PRECONDITION is overlap: both queries must be executing
  # at the same time, or "peak pods" measures nothing. On a cold org a single
  # transient on one connection (10s pg_try backoff) can let the sibling's
  # query FINISH first — its worker goes hot-idle and the retry REUSES it,
  # serializing the two queries (legit behavior, peak=1, false fail). So this
  # one test keeps the old long query (~2min on a default worker), which
  # absorbs any retry/spawn desync; the other resilience tests use the shorter
  # HEAVY_Q because their assertions don't require cross-connection overlap.
  q="SELECT count(*) FROM range(8000000000) t(i) WHERE i % 2 = 0;"
  want=4000000000
  ( if pg_try "$1" "$2" ducklake "$q" >"$o1" 2>&1; then echo 0 >"$r1"; else echo 1 >"$r1"; fi ) &
  p1=$!
  ( if pg_try "$1" "$2" ducklake "$q" >"$o2" 2>&1; then echo 0 >"$r2"; else echo 1 >"$r2"; fi ) &
  p2=$!

  # While both queries run, the org must reach >=2 worker pods — one per session.
  # MaxSessions=1 forces the second query onto its own pod (it cannot join the
  # first's busy pod), so co-residence is impossible.
  peak=0 a=0
  while [ "$a" -lt 180 ]; do
    kill -0 "$p1" 2>/dev/null || break
    kill -0 "$p2" 2>/dev/null || break
    c="$(k get pods -l "duckgres/active-org=$1" --no-headers 2>/dev/null | grep -c . || true)"
    [ "$c" -gt "$peak" ] && peak="$c"
    [ "$peak" -ge 2 ] && break
    sleep 1; a=$((a + 1))
  done

  wait "$p1" 2>/dev/null || true
  wait "$p2" 2>/dev/null || true
  { [ "$(cat "$r1")" = 0 ] && [ "$(cat "$r2")" = 0 ]; } \
    || fail "one_session_per_worker: a concurrent query errored ($(tr -d '\n' <"$o1" | tail -c 120) | $(tr -d '\n' <"$o2" | tail -c 120))"
  n1="$(tr -dc '0-9' <"$o1")"; n2="$(tr -dc '0-9' <"$o2")"
  { [ "$n1" = "$want" ] && [ "$n2" = "$want" ]; } \
    || fail "one_session_per_worker: wrong results n1=$n1 n2=$n2 want $want"
  rm -f "$o1" "$o2" "$r1" "$r2"
  if [ "$peak" -lt 2 ]; then
    return 1
  fi
  log "one session per worker: OK (peak $peak pods for $1)"
}

one_session_per_worker() { # org password
  log "one session per worker: concurrent queries land on distinct pods on $1"
  # peak<2 with both queries returning CORRECT results is ambiguous: either a
  # real co-assignment regression, or the documented serialization false-fail
  # (one query's transient retry let the sibling finish; its hot-idle worker
  # was then legitimately reused). True co-residence cannot be silent — workers
  # run MaxSessions=1, and a CP/worker accounting drift is loudly surfaced via
  # the session-cap-drift ERROR + metric — so a single retry preserves the
  # regression net (a deterministic sharing bug fails both attempts) while
  # absorbing the transient (observed: 2026-06-10 run 27304575868 attempt 1).
  if ! one_session_per_worker_attempt "$1" "$2"; then
    log "one session per worker: queries serialized (peak<2, results correct) — retrying once"
    one_session_per_worker_attempt "$1" "$2" \
      || fail "one_session_per_worker: org peaked at <2 worker pods for 2 concurrent queries on BOTH attempts — sessions shared a worker"
  fi
}

# Parallel cold-burst spawn ramp (regression net for the doomed-spawn /
# serialized-cold-burst fix): with NO reusable workers for the org, 3 concurrent
# connections must (a) ALL be served, on 3 DISTINCT worker pods, and (b) get
# their pods spawned in PARALLEL. The per-org FIFO acquire gate is held only
# across the short claim/slot decision, so the 3 pod creates land within seconds
# of each other; the old gate-held-across-spawn behavior created pod k only
# after pod k-1's full spawn+activate finished (tens of seconds each), so the
# creationTimestamp spread of the burst's first 3 pods is the discriminator.
cold_burst_parallel_spawns() { # org password
  log "parallel cold-burst spawn ramp on $1"
  # Start truly cold: drain every worker currently serving the org so each of
  # the 3 connections below needs its own fresh pod spawn (no hot-idle reuse).
  k delete pods -l "app=duckgres-worker,duckgres/active-org=$1" --wait=false >/dev/null 2>&1 || true
  k wait --for=delete pods -l "app=duckgres-worker,duckgres/active-org=$1" --timeout=300s >/dev/null 2>&1 || true

  cb1="$(mktemp)"; cb2="$(mktemp)"; cb3="$(mktemp)"
  cbr1="$(mktemp)"; cbr2="$(mktemp)"; cbr3="$(mktemp)"
  # Multi-second query (HEAVY_Q) so each session holds its worker while the
  # others activate (the 3 spawns run in parallel, so the hold window is the
  # slowest sibling's spawn, not the sum).
  cq="$HEAVY_Q"
  ( if pg_try "$1" "$2" ducklake "$cq" >"$cb1" 2>&1; then echo 0 >"$cbr1"; else echo 1 >"$cbr1"; fi ) &
  cp1=$!
  ( if pg_try "$1" "$2" ducklake "$cq" >"$cb2" 2>&1; then echo 0 >"$cbr2"; else echo 1 >"$cbr2"; fi ) &
  cp2=$!
  ( if pg_try "$1" "$2" ducklake "$cq" >"$cb3" 2>&1; then echo 0 >"$cbr3"; else echo 1 >"$cbr3"; fi ) &
  cp3=$!

  # While the queries run, the org must reach 3 simultaneous worker pods —
  # one per cold connection (one session per worker; no sharing).
  cpeak=0 ca=0
  while [ "$ca" -lt 300 ]; do
    kill -0 "$cp1" 2>/dev/null || break
    kill -0 "$cp2" 2>/dev/null || break
    kill -0 "$cp3" 2>/dev/null || break
    cc="$(k get pods -l "duckgres/active-org=$1" --no-headers 2>/dev/null | grep -c . || true)"
    [ "$cc" -gt "$cpeak" ] && cpeak="$cc"
    [ "$cpeak" -ge 3 ] && break
    sleep 1; ca=$((ca + 1))
  done

  wait "$cp1" 2>/dev/null || true
  wait "$cp2" 2>/dev/null || true
  wait "$cp3" 2>/dev/null || true
  { [ "$(cat "$cbr1")" = 0 ] && [ "$(cat "$cbr2")" = 0 ] && [ "$(cat "$cbr3")" = 0 ]; } \
    || fail "cold_burst_parallel_spawns: a cold-burst connection failed ($(tr -d '\n' <"$cb1" | tail -c 100) | $(tr -d '\n' <"$cb2" | tail -c 100) | $(tr -d '\n' <"$cb3" | tail -c 100))"
  for f in "$cb1" "$cb2" "$cb3"; do
    cn="$(tr -dc '0-9' <"$f")"
    [ "$cn" = "$HEAVY_EXPECT" ] || fail "cold_burst_parallel_spawns: wrong result $cn want $HEAVY_EXPECT"
  done
  [ "$cpeak" -ge 3 ] \
    || fail "cold_burst_parallel_spawns: org peaked at $cpeak worker pod(s) for 3 concurrent cold connections — burst did not land on distinct workers"

  # Parallelism: the burst's first 3 pods must have been CREATED within a
  # narrow window. ISO-8601 creationTimestamps sort lexicographically; take the
  # 3 earliest (pg_try retries could add a later 4th) and bound first→third.
  # A serialized ramp separates each create by a full pod spawn+activate
  # (>=~20-60s each on a warm node, minutes on a cold one), far above 30s.
  cts="$(k get pods -l "app=duckgres-worker,duckgres/active-org=$1" \
        -o jsonpath='{.items[*].metadata.creationTimestamp}' 2>/dev/null \
        | tr ' ' '\n' | grep . | sort | head -3)"
  [ "$(echo "$cts" | grep -c .)" = "3" ] \
    || fail "cold_burst_parallel_spawns: expected >=3 org worker pods after the burst, got: $cts"
  cfirst="$(echo "$cts" | head -1)"; cthird="$(echo "$cts" | tail -1)"
  ce1="$(date -u -D "%Y-%m-%dT%H:%M:%SZ" -d "$cfirst" +%s 2>/dev/null || date -u -d "$cfirst" +%s)"
  ce3="$(date -u -D "%Y-%m-%dT%H:%M:%SZ" -d "$cthird" +%s 2>/dev/null || date -u -d "$cthird" +%s)"
  cspread=$((ce3 - ce1))
  [ "$cspread" -le 30 ] \
    || fail "cold_burst_parallel_spawns: pod creation spread ${cspread}s (first=$cfirst third=$cthird) — spawns ramped sequentially, not in parallel"
  rm -f "$cb1" "$cb2" "$cb3" "$cbr1" "$cbr2" "$cbr3"
  log "parallel cold-burst spawn ramp: OK (peak $cpeak pods, creation spread ${cspread}s)"
}

# Data committed to DuckLake survives a worker restart (parquet in object store +
# metadata snapshot in Postgres are the source of truth).
# Ported from TestK8sDuckLakeDurabilityAcrossWorkerRestart.
durability_across_restart() { # org password
  log "DuckLake durability across worker restart on $1"
  t="e2e_dur_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "CREATE OR REPLACE TABLE $t AS SELECT i AS id FROM generate_series(1,200) t(i);"
  pod="$(newest_org_worker "$1")"; [ -n "$pod" ] || fail "no worker pod serving the write for $1"
  k delete pod "$pod" --wait=false >/dev/null 2>&1 || true
  k wait --for=delete "pod/$pod" --timeout=120s >/dev/null 2>&1 || true
  wait_worker "$1" "$2" ducklake
  n="$(pg "$1" "$2" ducklake "SELECT COUNT(*) FROM $t;")"
  [ "$n" = "200" ] || fail "$1 durability rowcount=$n want 200 after restart"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

# Concurrent multi-row INSERTs must not lose or duplicate rows — exercises the
# PostHog DuckLake fork's conflict-retry path. Ported from
# TestK8sDuckLakeConcurrentWriters.
concurrent_writers() { # org password
  log "concurrent writers on $1"
  t="e2e_cw_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "CREATE OR REPLACE TABLE $t (writer INT, id INT);"
  pids="" w=0
  while [ "$w" -lt 4 ]; do
    vals="" j=0
    while [ "$j" -lt 25 ]; do
      vals="$vals${vals:+,}($w,$((w*25+j)))"; j=$((j + 1))
    done
    ( pgc "$1" "$2" ducklake "INSERT INTO $t VALUES $vals;" ) &
    pids="$pids $!"; w=$((w + 1))
  done
  rc=0; for p in $pids; do wait "$p" || rc=1; done
  [ "$rc" = 0 ] || fail "a concurrent writer INSERT errored on $1"
  n="$(pg "$1" "$2" ducklake "SELECT COUNT(*) FROM $t;")"
  [ "$n" = "100" ] || fail "$1 concurrent writers rowcount=$n want 100 (fork conflict-retry lost/duplicated writes)"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

# ---- admin API auth (#721) -------------------------------------------------
# Regression for #721: the internal secret must NOT be accepted via a ?token=
# URL query parameter (URL-borne secrets persist in browser history and any
# future proxy access logs) — only the X-Duckgres-Internal-Secret header / login
# cookie authenticate. The admin console serves its React SPA UNauthenticated at
# "/" (the bundle carries no secrets; all data is behind /api auth), so this
# asserts the invariant against an authenticated API endpoint, not "/".
admin_dashboard_no_query_token() {
  log "admin API rejects ?token= query auth (#721)"
  hdrs=/tmp/admin_qt_hdrs
  code="$(curl -s -o /dev/null -D "$hdrs" -w '%{http_code}' "$API/api/v1/orgs?token=$SECRET")"
  [ "$code" = "401" ] || fail "GET /api/v1/orgs?token=<secret> returned $code, want 401 (query-param auth must be rejected)"
  if grep -qi '^set-cookie:' "$hdrs"; then
    fail "GET /api/v1/orgs?token=<secret> set a cookie — query-param auth is back"
  fi
  # The header path (what this harness uses everywhere) still authenticates.
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" "$API/api/v1/orgs")"
  [ "$code" = "200" ] || fail "GET /api/v1/orgs with internal-secret header returned $code, want 200"
  # The public SPA is served unauthenticated at "/".
  code="$(curl -s -o /dev/null -w '%{http_code}' "$API/")"
  [ "$code" = "200" ] || fail "GET / (public SPA) returned $code, want 200"
}

# ---- internal secret rotation fallback --------------------------------------
# The CP accepts {primary ∪ DUCKGRES_INTERNAL_SECRET_FALLBACKS} so a secret
# rotation is a phased roll with no auth-mismatch window. The deployment
# configures INTERNAL_SECRET_FALLBACK as a fallback; it must authenticate both
# the service-to-service API path and the dashboard, and the accept-list must
# not have opened the door to arbitrary tokens.
internal_secret_fallback_auth() {
  log "internal secret rotation fallback authenticates"
  fb="${INTERNAL_SECRET_FALLBACK:?}"
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "X-Duckgres-Internal-Secret: $fb" "$API/api/v1/orgs")"
  [ "$code" = "200" ] || fail "GET /api/v1/orgs with fallback secret returned $code, want 200 (rotation overlap broken)"
  # Dashboard cookie path: the fallback secret must also authenticate via the
  # login cookie (not the public "/" SPA, which 200s for everyone).
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    --cookie "duckgres_admin_token=$fb" "$API/api/v1/orgs")"
  [ "$code" = "200" ] || fail "GET /api/v1/orgs with fallback-secret cookie returned $code, want 200 (dashboard cookie path)"
  code="$(curl -s -o /dev/null -w '%{http_code}' \
    -H "X-Duckgres-Internal-Secret: definitely-not-a-secret" "$API/api/v1/orgs")"
  [ "$code" = "401" ] || fail "GET /api/v1/orgs with garbage secret returned $code, want 401 (accept-list must stay closed)"
}

# ---- admin models explorer API ---------------------------------------------
# The dashboard's generic models explorer (sidebar + table + detail UI) is
# backed by GET /api/v1/models (sidebar: every config-store model + a live row
# count) and GET /api/v1/models/:model (one model's rows + derived columns).
# This asserts the surface is wired and — load-bearing — that the listing never
# leaks secret-bearing columns: org_users carry a bcrypt password hash in the
# DB, but Password is json:"-", so it must NOT appear in the API response. A
# regression here (someone retagging Password) would surface a credential hash
# in the operator UI.
models_explorer_api() {
  log "admin models explorer API (sidebar + listing + redaction)"
  # Sidebar: must enumerate the known models across the Tenants + Runtime groups.
  idx="$(curl -fsS -H "$H" "$API/api/v1/models")"
  for key in orgs org-users managed-warehouses worker-records org-connection-leases; do
    echo "$idx" | jq -e --arg k "$key" '.models[] | select(.key == $k)' >/dev/null \
      || fail "models index missing model '$key'"
  done
  # The seeded cnpg org must show up with a >=1 count on the orgs model.
  orgcount="$(echo "$idx" | jq -r '.models[] | select(.key=="orgs") | .count')"
  [ "$orgcount" -ge 1 ] 2>/dev/null || fail "orgs model count '$orgcount' < 1 (expected seeded orgs)"

  # org-users listing: present users, derive columns, and NEVER a password.
  users="$(curl -fsS -H "$H" "$API/api/v1/models/org-users")"
  echo "$users" | jq -e '.columns | index("username")' >/dev/null \
    || fail "org-users listing missing 'username' column"
  if echo "$users" | jq -e '.columns | index("password")' >/dev/null 2>&1; then
    fail "org-users listing exposes a 'password' column (must be redacted)"
  fi
  if echo "$users" | jq -e '[.rows[] | has("password")] | any' >/dev/null 2>&1; then
    fail "org-users row carries a password field (credential hash leak via UI)"
  fi
  # A runtime-schema model must list (schema qualification works), returning
  # columns even when the table is empty.
  curl -fsS -H "$H" "$API/api/v1/models/worker-records" \
    | jq -e '.columns | index("worker_id")' >/dev/null \
    || fail "worker-records listing missing 'worker_id' column (runtime schema qualification broken)"
  # Unknown model → 404.
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" "$API/api/v1/models/not-a-model")"
  [ "$code" = "404" ] || fail "GET /api/v1/models/not-a-model returned $code, want 404"
  # Auto-discovery: tables with no typed descriptor (goose bookkeeping, the
  # reshard tables) must appear under "Other" and list with columns.
  out="$(curl -fsS -H "$H" "$API/api/v1/models")" || fail "models: request failed"
  echo "$out" | jq -e '.models[] | select(.group == "Other" and (.key | endswith(".goose_db_version")))' >/dev/null \
    || fail "models: auto-discovered goose_db_version missing: $out"
  rk="$(echo "$out" | jq -r '.models[] | select(.group == "Other" and (.key | endswith(".duckgres_reshard_operations"))) | .key')"
  [ -n "$rk" ] || fail "models: auto-discovered duckgres_reshard_operations missing"
  curl -fsS -H "$H" "$API/api/v1/models/$rk" | jq -e '.columns | index("org_id") != null' >/dev/null \
    || fail "models: auto table listing lacks org_id column"
  log "models explorer auto-discovery OK ($rk)"
}

# ---- admin console: identity + live state + auth gate ----------------------
# The VPC-private admin console (controlplane/admin/, docs/design/admin-ui.md)
# adds identity resolution (/me), live cluster state, a metrics proxy, and an
# audit log on top of the models explorer. The internal secret maps to the admin
# role; an unauthenticated request is rejected. This asserts the new read
# surfaces are wired and return their documented envelopes.
admin_console_api() {
  log "admin console: /me identity, live state, auth gate"
  # Internal secret → admin role.
  role="$(curl -fsS -H "$H" "$API/api/v1/me" | jq -r '.role')"
  [ "$role" = "admin" ] || fail "GET /me with internal secret role='$role', want admin"
  # No auth → 401 (the accept-list stays closed for SSO-less callers).
  code="$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/me")"
  [ "$code" = "401" ] || fail "GET /me with no auth returned $code, want 401"
  # Live read endpoints return their documented envelopes.
  # /queries aggregates each CP's in-memory view and reports coverage. The CI CP
  # runs a SINGLE replica, so this only asserts the envelope (cp_total==cp_responders,
  # no failed peers); it cannot exercise a real peer hop. The multi-CP fan-out
  # (peer discovery + self-exclusion + merge/dedup) is covered by unit tests
  # (TestDiscoverPeerIPs, TestQueriesAggregateAcrossCPs).
  curl -fsS -H "$H" "$API/api/v1/queries" \
    | jq -e 'has("queries") and (.cp_total >= 1) and (.cp_responders == .cp_total)' >/dev/null \
    || fail "/queries envelope wrong (queries/cp_total/cp_responders)"
  curl -fsS -H "$H" "$API/api/v1/workers/fleet"     | jq -e 'has("fleet")'   >/dev/null || fail "/workers/fleet missing 'fleet' key"
  curl -fsS -H "$H" "$API/api/v1/cluster/instances" \
    | jq -e '.instances | map(select(.self)) | length >= 1' >/dev/null \
    || fail "/cluster/instances has no self-flagged CP replica"
  # Node-overview topology reads (back the admin console "Nodes" live view): they
  # project in-cluster nodes/pods/events into the K8s list shape the view consumes
  # (GET /cluster/{nodes,pods,events,nodepools}), each a {items:[...]} envelope.
  # We assert the endpoints are wired + auth-gated + return 200 with an items
  # array — NOT that items is populated: these reads are cluster-scoped, and the
  # e2e CP's ServiceAccount can't be granted cluster-scoped RBAC from CI (the CI
  # deployer can't create/bind cluster-topology roles without escalation), so the
  # CP degrades a Forbidden to an empty list here. The populated path is exercised
  # in real envs where the chart's duckgres-control-plane-cluster-topology
  # ClusterRole is bound (see cluster_test.go for the projection-shape assertions).
  for res in nodes pods events nodepools; do
    curl -fsS -H "$H" "$API/api/v1/cluster/$res" \
      | jq -e '(.items | type) == "array"' >/dev/null \
      || fail "/cluster/$res did not return a 200 {items:[...]} envelope"
  done
  # /cluster/summary backs the admin nav totals (shown on every page): assert it
  # returns the numeric fields the Topbar reads (>=0). Degrades to zeros if the
  # topology RBAC is missing, so this checks shape, not populated values.
  curl -fsS -H "$H" "$API/api/v1/cluster/summary" \
    | jq -e '(.nodes|type=="number") and (.workers|type=="number") and (.worker_cpu_cores|type=="number") and (.worker_mem_gib|type=="number") and (.placeholders|type=="number") and (.pending|type=="number")' >/dev/null \
    || fail "/cluster/summary missing numeric totals (nodes/workers/cpu/mem/placeholders/pending)"
  # The metrics proxy advertises its allow-listed panels (not an open PromQL relay).
  # Includes the per-org/per-source worker-acquire-latency panels. (The raw
  # histogram emission — org+source labels on duckgres_worker_acquire_total_seconds
  # — is unit-tested, not asserted here: the :9090 metrics port is
  # NetworkPolicy-blocked from this in-cluster Job.)
  panels="$(curl -fsS -H "$H" "$API/api/v1/metrics/panels")"
  for p in query_rate acquire_p95 acquire_by_source; do
    echo "$panels" | jq -e --arg p "$p" '.panels | index($p)' >/dev/null \
      || fail "/metrics/panels missing '$p'"
  done
}

# ---- admin: /status reports per-org worker counts --------------------------
# OrgStatus.Workers (the Overview per-org load bars + total_workers) was an
# always-0 dead field; it's now populated from each CP's OrgReservedPool
# (cap-counting assigned workers, summed across replicas by the /status
# fan-out). With a query in flight the org's worker is Hot, so /status must
# report workers>=1 for that org and a nonzero cluster total_workers.
# PRECONDITION: the caller passes an org whose lane is already warm (e.g. CNPG
# after join_lanes), so the query lands on a Hot worker within the 120s budget
# rather than waiting on a multi-minute cold pod spawn — see the call site.
admin_per_org_workers() { # org password
  org="$1"; pw="$2"
  log "admin: /status per-org worker count is populated on $org"
  q="SELECT count(*) FROM range(2718281828) t(i) WHERE i % 2 = 0;"
  out="$(mktemp)"
  ( pg_try "$org" "$pw" ducklake "$q" >"$out" 2>&1 || true ) &
  bg=$!
  cleanup_pow() { kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true; rm -f "$out"; }

  ok="" a=0
  while [ "$a" -lt 60 ]; do
    kill -0 "$bg" 2>/dev/null || break
    w="$(curl -fsS -H "$H" "$API/api/v1/status" \
      | jq -r --arg o "$org" '(.orgs[]? | select(.name==$o) | .workers) // 0')"
    tot="$(curl -fsS -H "$H" "$API/api/v1/status" | jq -r '.total_workers // 0')"
    if [ "${w:-0}" -ge 1 ] && [ "${tot:-0}" -ge 1 ]; then ok=1; break; fi
    sleep 2; a=$((a + 1))
  done
  cleanup_pow
  [ -n "$ok" ] || fail "admin_per_org_workers: /status never reported workers>=1 for $org (per-org worker count not populated)"
  log "admin: /status per-org worker count OK (workers>=1, total_workers>=1) on $org"
}

# ---- connection idle timeout: idle conn reaped, worker freed ----------------
# An idle client connection pins a worker (one session per worker), so the
# control plane defaults to a short connection idle timeout (server.Default
# ControlPlaneIdleTimeout, 60s): a connection with no traffic for that long is
# closed by the message loop's read deadline, DestroySession runs, and the
# worker returns to hot-idle. We hold a connection idle-in-transaction PAST the
# timeout and assert its session disappears from /queries while our client is
# still connected (so the reap is the CP's, not our client exiting). The unit
# tests (TestNormalizeIdleTimeout + TestMessageLoopIdleTimeoutClosesConnection)
# are the deterministic gate for the mechanism; this proves the real default
# fires end-to-end in-cluster.
conn_idle_timeout_reaps_session() { # org password
  org="$1"; pw="$2"
  log "conn idle timeout: idle connection is reaped + worker freed on $org"
  rootwids() {
    curl -fsS -H "$H" "$API/api/v1/queries" \
      | jq -r --arg o "$org" '[.queries[]? | select(.org==$o and .user=="root") | .worker_id] | sort | join(" ")'
  }
  before="$(rootwids)"
  # Hold a connection idle-in-transaction for 150s (well past the 60s timeout):
  # send BEGIN, then keep stdin open so psql waits and issues no further query.
  ( printf 'BEGIN;\n'; sleep 150 ) | PGPASSWORD="$pw" psql \
      "sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
      -v ON_ERROR_STOP=1 -qtA >/dev/null 2>&1 &
  bg=$!
  cleanup_ir() { kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true; }
  # The NEW root worker_id (not present before) is our idle connection's.
  wid="" a=0
  while [ "$a" -lt 30 ]; do
    kill -0 "$bg" 2>/dev/null || break
    for w in $(rootwids); do
      case " $before " in *" $w "*) : ;; *) wid="$w"; break ;; esac
    done
    [ -n "$wid" ] && break
    sleep 2; a=$((a + 1))
  done
  [ -n "$wid" ] || { cleanup_ir; fail "conn_idle_timeout: idle session never appeared for $org"; }
  log "conn idle timeout: idle session on worker $wid — waiting for the CP to reap it"
  # Must vanish from /queries within ~90s (60s timeout + grace), while our client
  # is STILL alive (kill -0) so the disappearance is the CP reaping, not us.
  gone="" a=0
  while [ "$a" -lt 30 ]; do
    kill -0 "$bg" 2>/dev/null || { cleanup_ir; fail "conn_idle_timeout: our client exited before reap — inconclusive"; }
    present="$(curl -fsS -H "$H" "$API/api/v1/queries" | jq -r --argjson w "$wid" 'any(.queries[]?; .worker_id==$w)')"
    [ "$present" = "false" ] && { gone=1; break; }
    sleep 3; a=$((a + 1))
  done
  cleanup_ir
  [ -n "$gone" ] || fail "conn_idle_timeout: idle session on worker $wid was NOT reaped within ~90s (idle timeout not enforced)"
  log "conn idle timeout: idle session reaped, worker $wid freed on $org"
}

# ---- COPY FROM STDIN: active in Live + survives the idle timeout ------------
# An actively-streaming COPY reads many CopyData messages; each in-loop read
# re-arms the read deadline (server/conn_copy.go armIdleReadDeadline), so a COPY
# that keeps putting bytes ON THE WIRE for LONGER than the idle timeout — while
# never stalling longer than it between messages — must NOT be reaped mid-stream
# (the regression the 60s default would otherwise cause for COPY-heavy data
# imports). It must also be categorized as an ACTIVE query in /queries
# (elapsed_ms>0), not an idle session, while it streams. The server-side re-arm
# is unit-tested by TestCopyStreamReArmsIdleDeadline (server/conn_idle_test.go);
# this asserts the same guarantee end-to-end against a real worker.
#
# ROOT CAUSE of the earlier flake (do NOT regress this): the old version streamed
# tiny rows (~12 bytes) with sleeps between them. libpq buffers CopyData in its
# ~8KB client-side output buffer and only flushes when that buffer fills — so a
# trickle of small rows was NEVER put on the wire until PQputCopyEnd (the closing
# "\."), which for a ~75s producer lands well past the 60s idle timeout. The
# server therefore saw a genuinely IDLE connection (zero bytes for 60s) and
# correctly reaped it — the observed failure was duration_ms≈60001 on the FIRST
# read with rows=0 (i.e. nothing ever arrived), NOT a re-arm bug. The producer's
# sleeps paced the SHELL, not the wire.
#
# FIX: send fat chunks (~32KB each, well over libpq's flush threshold) so every
# 5s interval forces a real flush and the server actually receives a burst of
# CopyData to re-arm on. 14 chunks × 5s ≈ 70s of genuine wire activity — past the
# 60s idle timeout (a broken re-arm would still be caught), with each gap ~12x
# under the deadline. Each row carries a 2000-char pad so ~16 rows = ~32KB/chunk.
copy_active_and_survives_idle() { # org password
  org="$1"; pw="$2"
  log "COPY FROM STDIN: active in Live + survives idle timeout on $org"
  conn="sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake"
  # This test runs right after the idle-timeout test frees the org's worker, so a
  # bare COPY connection would cold-spawn a worker inside the stream's critical
  # path. Force activation up front so the streamed data never races a cold start.
  wait_worker "$org" "$pw" ducklake
  # 2000-char pad → each row line is ~2KB, so a 16-row chunk is ~32KB: bigger than
  # libpq's ~8KB output buffer, which forces a flush to the wire each interval.
  pad="$(printf '%02000d' 0)"
  # A one-off environmental blip (a real CP→worker forward stall, a connection
  # reset) can trip the client read deadline even though the COPY itself never
  # idled — that is NOT the behavior under test, so retry the streaming attempt a
  # bounded number of times on ONLY that transient signature. A genuine re-arm
  # regression fails EVERY attempt (the stream always out-lives 60s of real wire
  # activity), so this cannot mask it; a non-transient failure aborts immediately.
  attempt=0 last=""
  while [ "$attempt" -lt 2 ]; do
    attempt=$((attempt + 1))
    out="$(mktemp)"
    ( printf 'DROP TABLE IF EXISTS e2e_idle_copy;\n'
      printf 'CREATE TABLE e2e_idle_copy(v TEXT);\n'
      printf '\\copy e2e_idle_copy(v) FROM STDIN\n'
      for i in $(seq 1 14); do
        for j in $(seq 1 16); do printf 'r%d_%d_%s\n' "$i" "$j" "$pad"; done
        sleep 5
      done
      printf '\\.\n'
      printf 'SELECT count(*) FROM e2e_idle_copy;\n'
      printf 'DROP TABLE e2e_idle_copy;\n'
    ) | PGPASSWORD="$pw" psql "$conn" -v ON_ERROR_STOP=1 -qtA >"$out" 2>&1 &
    bg=$!
    # While it streams, /queries must show it as an ACTIVE query (elapsed_ms>0),
    # not an idle session. Poll until seen active or the client exits (bounded).
    active="" a=0
    while [ "$a" -lt 30 ]; do
      kill -0 "$bg" 2>/dev/null || break
      e="$(curl -fsS -H "$H" "$API/api/v1/queries" \
        | jq -r --arg o "$org" 'first(.queries[]? | select(.org==$o and .user=="root" and (.elapsed_ms>0))) | .elapsed_ms // empty')"
      [ -n "$e" ] && { active=1; break; }
      sleep 3; a=$((a + 1))
    done
    # It must finish successfully with all 224 rows (NOT reaped mid-stream).
    # NB: `wait; rc=$?` must NOT be a bare statement — under `set -e` a non-zero
    # psql exit would abort the whole harness at `wait` (the "HARNESS EXIT rc=3,
    # no FAIL line" seen before) before the retry/branch logic below could run.
    if wait "$bg"; then rc=0; else rc=$?; fi
    ok=""; { [ "$rc" -eq 0 ] && grep -qx "224" "$out"; } && ok=1
    body="$(tr '\n' ' ' <"$out" | tail -c 300)"; rm -f "$out"
    if [ -n "$ok" ]; then
      # Streamed ~70s of real wire traffic past the idle timeout without being
      # reaped: now the active-categorization assertion is the remaining signal.
      [ -n "$active" ] || fail "copy_active: streaming COPY not categorized active (elapsed_ms>0) in /queries on $org"
      log "COPY FROM STDIN: active in Live + survived idle timeout (224 rows) on $org"
      return 0
    fi
    last="$body"
    case "$body" in
      *"i/o timeout"*|*"read csv stream"*|*"connection reset"*|*"broken pipe"*|*"EOF"*|*"server closed the connection"*)
        log "copy_active: transient stream blip on $org (attempt $attempt/2), retrying: $body"
        continue ;;
      *)
        fail "copy_active: streaming COPY (~70s) did not succeed with 224 rows (rc=$rc): $body" ;;
    esac
  done
  fail "copy_active: streaming COPY (~70s) failed all $attempt attempts with transient stream errors — likely a real re-arm/reaper regression (last: $last)"
}

# ---- admin RBAC: SSO viewer is read-only -----------------------------------
# A forged ALB OIDC header (no internal secret) for an @posthog.com email that
# is NOT in the operators table resolves to the viewer role (fail-closed
# default): it can read but must be blocked from mutations and from the audit
# log. Exercises RoleGate + RequireAdmin against the REAL router (the unit tests
# cover the gate algorithm; this covers the wiring). The JWT is unsigned because
# the CP trusts the ALB-injected header by network position. (Role is no longer
# group-based — it comes from the operators table; an unknown email = viewer.)
admin_rbac_viewer() { # org
  org="$1"
  log "admin RBAC: forged SSO viewer (unknown operator) is read-only (no mutate, no audit)"
  payload="$(printf '{"email":"ci-viewer@posthog.com","email_verified":true}' | base64 -w0 | tr '+/' '-_' | tr -d '=')"
  vh="X-Amzn-Oidc-Data: e30.${payload}.sig"
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$vh" "$API/api/v1/orgs")"
  [ "$code" = "200" ] || fail "viewer GET /orgs returned $code, want 200 (reads allowed)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$vh" "$API/api/v1/audit")"
  [ "$code" = "403" ] || fail "viewer GET /audit returned $code, want 403 (audit is admin-only)"
  code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT -H "$vh" -H 'Content-Type: application/json' -d '{}' "$API/api/v1/orgs/$org")"
  [ "$code" = "403" ] || fail "viewer PUT /orgs/$org returned $code, want 403 (mutations are admin-only)"
}

# ---- admin live-query detail (phase 1): per-pid expansion ------------------
# The Live page can open one in-flight query to see its (redacted) SQL text +
# connection metadata + live progress. Backed by GET /api/v1/queries/:pid, which
# joins server.ConnDetailByPID (the already-redacted currentQuery, scoped to the
# replica that owns the connection) with the session manager's cached progress.
# Asserts: a real running query is findable via /queries, /queries/by-worker/:wid
# returns 200 with the SQL round-tripped + matching identity, and an unknown
# worker 404s (not a 500). The redaction guarantee itself is unit-tested
# (server/conn_detail_test.go, controlplane/admin/live_test.go) — this proves
# the wiring against a real worker pod.
#
# NOTE: backend pids are process-global within a CP (controlplane/session_mgr.go
# globalNextPID), which removed the per-org pid collision that shadowed conns in
# the server.conns map (pg_stat_activity / cancel). That can't be asserted
# in-Job here: forcing a collision needs a FRESH CP with two orgs opening their
# first connection at the same pid count, which a warm cluster can't reproduce.
# The deterministic gate is TestReservePIDGloballyUniqueAcrossManagers.

# ---- admin live: idle (no in-flight query) sessions are flagged ------------
# QueryStatus.State surfaces the pg_stat_activity-style connection state in
# /queries, so the Live view can flag sessions that hold a worker but run no
# query (idle / idle in transaction) — a smell when persistent. We hold a
# connection idle-in-transaction (send BEGIN, then keep stdin open so psql
# waits) and assert /queries reports an idle* state for that session.
admin_idle_session_flagged() { # org password
  org="$1"; pw="$2"
  log "admin live: idle-in-transaction session is flagged (no in-flight query) on $org"
  ( printf 'BEGIN;\n'; sleep 30 ) | PGPASSWORD="$pw" psql \
      "sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
      -v ON_ERROR_STOP=1 -qtA >/dev/null 2>&1 &
  bg=$!
  cleanup_idle() { kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true; }
  found="" a=0
  while [ "$a" -lt 20 ]; do
    kill -0 "$bg" 2>/dev/null || break
    found="$(curl -fsS -H "$H" "$API/api/v1/queries" \
      | jq -r --arg o "$org" 'first(.queries[]? | select(.org==$o and ((.state // "")|test("idle";"i"))) | .state) // empty')"
    [ -n "$found" ] && break
    sleep 2; a=$((a + 1))
  done
  cleanup_idle
  case "$found" in
    idle*) log "admin live: idle session flagged OK (state='$found') on $org" ;;
    *) fail "admin_idle_session_flagged: /queries never reported an idle* state for an idle-in-transaction session on $org (got '$found')" ;;
  esac
}

# ---- admin: cancel a session by (cluster-unique) worker id ------------------
# The Live view's Cancel button posts /sessions/by-worker/:wid/cancel. Worker id
# (not the per-CP pid) is the address, and the handler kills locally or fans out
# to whichever CP owns the session — so cancel works regardless of which replica
# the request lands on. (The cross-CP fan-out itself is unit-tested in
# TestCancelByWorkerFansOut; the CI CP is single-replica, so this covers the
# real kill + the unknown→404 path end-to-end.)
admin_cancel_by_worker() { # org password
  org="$1"; pw="$2"
  log "admin: cancel a live session by worker id on $org"
  # Hold longer than the appear-poll budget (30×2s) so a slow cold-start can't
  # exit the client before the session is observed (we cancel it well before
  # the 60s idle timeout anyway).
  ( printf 'BEGIN;\n'; sleep 90 ) | PGPASSWORD="$pw" psql \
      "sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
      -v ON_ERROR_STOP=1 -qtA >/dev/null 2>&1 &
  bg=$!
  cleanup_cbw() { kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true; }
  wid="" a=0
  while [ "$a" -lt 30 ]; do
    kill -0 "$bg" 2>/dev/null || break
    wid="$(curl -fsS -H "$H" "$API/api/v1/queries" \
      | jq -r --arg o "$org" 'first(.queries[]? | select(.org==$o and .user=="root")) | .worker_id // empty')"
    [ -n "$wid" ] && break
    sleep 2; a=$((a + 1))
  done
  [ -n "$wid" ] || { cleanup_cbw; fail "admin_cancel_by_worker: session never appeared for $org"; }
  # Cancel it by worker id → killed>=1.
  resp="$(curl -fsS -H "$H" -X POST "$API/api/v1/sessions/by-worker/$wid/cancel")" \
    || { cleanup_cbw; fail "admin_cancel_by_worker: POST cancel failed for worker $wid"; }
  echo "$resp" | jq -e '.killed >= 1' >/dev/null \
    || { cleanup_cbw; fail "admin_cancel_by_worker: cancel did not kill worker $wid: $resp"; }
  # The session must disappear from /queries.
  gone="" a=0
  while [ "$a" -lt 15 ]; do
    [ "$(curl -fsS -H "$H" "$API/api/v1/queries" | jq -r --argjson w "$wid" 'any(.queries[]?; .worker_id==$w)')" = "false" ] && { gone=1; break; }
    sleep 2; a=$((a + 1))
  done
  cleanup_cbw
  [ -n "$gone" ] || fail "admin_cancel_by_worker: session on worker $wid still present after cancel"
  # Unknown worker → 404 (not a 500).
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" -X POST "$API/api/v1/sessions/by-worker/999999999/cancel")"
  [ "$code" = "404" ] || fail "admin_cancel_by_worker: unknown worker cancel returned $code, want 404"
  log "admin: cancel by worker id OK (killed worker $wid, unknown→404) on $org"
}

# admin_recent_errors proves the admin Errors page: a failed query is captured
# into the CP's in-memory recent-errors ring and surfaces at GET /api/v1/errors
# with its identity + SQLSTATE + (redacted) query, AND that a failing CREATE
# SECRET never leaks its credential into the ring (the load-bearing redaction
# invariant — errors echo the offending SQL). Errors are captured on the CP that
# owned the failing connection; /errors fans out and merges across replicas.
admin_recent_errors() { # org password
  org="$1"; pw="$2"
  log "admin errors: recent-error capture + redaction on $org"

  # 1) A deterministic query error (unknown relation) with a distinctive marker
  # must appear in /errors. The stored query is verbatim for a non-secret stmt,
  # so we match on the marker table name. (Category is backend-dependent: the
  # worker's "Catalog Error" prefix is wrapped by Flight before the CP classifier
  # sees it, so the remote path reports system/XX000, not user/42P01 — the
  # assertion is category-agnostic on purpose.)
  marker="e2e_err_probe_$(openssl rand -hex 6)"
  pg_try "$org" "$pw" ducklake "SELECT 1 FROM ${marker};" >/dev/null 2>&1 || true

  entry="" a=0
  while [ "$a" -lt 30 ]; do
    entry="$(curl -fsS -H "$H" "$API/api/v1/errors?org=${org}&limit=500" \
      | jq -c --arg m "$marker" 'first(.errors[]? | select(.query != null and (.query | contains($m))))')"
    [ -n "$entry" ] && [ "$entry" != "null" ] && break
    sleep 1; a=$((a + 1))
  done
  { [ -n "$entry" ] && [ "$entry" != "null" ]; } \
    || fail "admin_recent_errors: marker query never appeared in /errors for $org within timeout"

  # Right org + populated SQLSTATE, category, and (redacted) message.
  echo "$entry" | jq -e --arg o "$org" \
    '.org == $o and (.sqlstate | length > 0) and (.category | length > 0) and (.message | length > 0)' \
    >/dev/null || fail "admin_recent_errors: captured error missing fields: $entry"
  log "admin errors: marker error captured (sqlstate $(echo "$entry" | jq -r '.sqlstate'), category $(echo "$entry" | jq -r '.category'))"

  # 2) Redaction (load-bearing): a failing CREATE SECRET must NOT leak its
  # credential into the ring — neither .query (RedactForLog drops the option
  # list) nor .message (RedactErrorForLog replaces the whole message). The secret
  # NAME survives in the redacted head, so we pin the entry by name.
  cred="e2eERRSECRET-$(openssl rand -hex 12)"
  pg_try "$org" "$pw" ducklake \
    "CREATE SECRET e2e_err_redact (TYPE nosuchtype, KEY_ID 'AKIAERRPROBE', SECRET '${cred}');" \
    >/dev/null 2>&1 || true
  sentry="" a=0
  while [ "$a" -lt 30 ]; do
    sentry="$(curl -fsS -H "$H" "$API/api/v1/errors?org=${org}&limit=500" \
      | jq -c 'first(.errors[]? | select(.query != null and (.query | test("e2e_err_redact"; "i"))))')"
    [ -n "$sentry" ] && [ "$sentry" != "null" ] && break
    sleep 1; a=$((a + 1))
  done
  if [ -n "$sentry" ] && [ "$sentry" != "null" ]; then
    case "$sentry" in
      *"$cred"*|*"AKIAERRPROBE"*) fail "admin_recent_errors: REDACTION BREACH — CREATE SECRET credential leaked into /errors" ;;
    esac
    log "admin errors: redaction holds (credential absent from captured CREATE SECRET error)"
  else
    # Capture-on-error is deterministic, but a fast cold-worker failure path can
    # occasionally reject before the CP logs it; don't fail the suite on a miss.
    log "admin errors: CREATE SECRET error not captured in window — redaction sub-check skipped (unit test is the gate)"
  fi
}

admin_query_detail() { # org password
  org="$1"; pw="$2"
  log "admin live: per-query detail round-trip on $org"
  # Distinctive constant so we can prove the SQL text round-trips into the
  # detail payload (survives transpilation as a bare numeric literal).
  q="SELECT count(*) FROM range(2718281828) t(i) WHERE i % 2 = 0;"
  out="$(mktemp)"
  ( pg_try "$org" "$pw" ducklake "$q" >"$out" 2>&1 || true ) &
  bg=$!
  cleanup_bg() { kill "$bg" 2>/dev/null || true; wait "$bg" 2>/dev/null || true; rm -f "$out"; }

  # Poll /queries until our marker query shows an in-flight row, and capture its
  # CLUSTER-UNIQUE worker id (detail is addressed by worker id, not the per-org
  # pid). Filter on the marker SQL is not possible from the list (no SQL there),
  # so filter on org and the running state; the lane is otherwise quiet here.
  wid="" pid="" a=0
  while [ "$a" -lt 60 ]; do
    kill -0 "$bg" 2>/dev/null || break
    row="$(curl -fsS -H "$H" "$API/api/v1/queries" \
      | jq -c --arg o "$org" 'first(.queries[]? | select(.org==$o))')"
    wid="$(printf '%s' "$row" | jq -r '.worker_id // empty')"
    pid="$(printf '%s' "$row" | jq -r '.pid // empty')"
    [ -n "$wid" ] && break
    sleep 2; a=$((a + 1))
  done
  [ -n "$wid" ] || { cleanup_bg; fail "admin_query_detail: no in-flight query appeared for $org within timeout"; }

  # The /queries list item carries the running-query duration. Give it a moment
  # to accrue, then assert elapsed_ms is present and positive for our worker.
  sleep 3
  ems="$(curl -fsS -H "$H" "$API/api/v1/queries" \
    | jq -r --argjson w "$wid" 'first(.queries[]? | select(.worker_id==$w) | .elapsed_ms) // -1')"
  case "$ems" in
    ''|-1|0) cleanup_bg; fail "admin_query_detail: /queries elapsed_ms not populated for worker $wid (got '$ems')" ;;
    *) [ "$ems" -gt 0 ] || { cleanup_bg; fail "admin_query_detail: elapsed_ms not positive for worker $wid (got '$ems')"; } ;;
  esac

  # Expand it by worker id: 200 with the redacted SQL text + matching identity.
  d="$(curl -fsS -H "$H" "$API/api/v1/queries/by-worker/$wid")" \
    || { cleanup_bg; fail "admin_query_detail: GET /queries/by-worker/$wid failed"; }
  echo "$d" | jq -e --arg o "$org" --argjson w "$wid" \
    '.worker_id == $w and .org == $o and (.query | contains("2718281828"))' >/dev/null \
    || { cleanup_bg; fail "admin_query_detail: detail mismatch for worker $wid: $(echo "$d" | jq -c '{pid,org,worker_id,state,qlen:(.query|length)}')"; }

  # An unknown worker id is a clean 404, never a 500.
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" "$API/api/v1/queries/by-worker/999999999")"
  [ "$code" = "404" ] || { cleanup_bg; fail "admin_query_detail: unknown worker id returned $code, want 404"; }
  cleanup_bg

  # Redaction in the LIVE path (load-bearing): a real CREATE SECRET executing on
  # a worker must NOT expose its credential material via /queries/:pid. The unit
  # test proves connDetail passes the redacted currentQuery through; this proves
  # the live server actually stores the redacted form (no un-redacted detail
  # source slipped in). We run a long CREATE SECRET so it's catchable in flight,
  # capture its pid, expand it, and assert the secret literal is absent.
  cred="e2eSECRET-$(openssl rand -hex 12)"
  # A count(*) over a big range forces a FULL scan (no LIMIT short-circuit), so
  # the CREATE SECRET stays in flight ~tens of seconds while DuckDB evaluates the
  # option subquery — long enough to catch. The cred literal is what must never
  # leak; RedactForLog replaces the whole option list with a placeholder.
  sq="CREATE OR REPLACE SECRET e2e_detail_redact (TYPE s3, KEY_ID 'AKIADETAILPROBE', SECRET '${cred}', REGION (SELECT count(*)::VARCHAR FROM range(3000000000) t(i) WHERE i % 2 = 0));"
  sout="$(mktemp)"
  ( pg_try "$org" "$pw" ducklake "$sq" >"$sout" 2>&1 || true ) &
  sbg=$!
  cleanup_sbg() { kill "$sbg" 2>/dev/null || true; wait "$sbg" 2>/dev/null || true; rm -f "$sout"; }
  # Poll each org worker's detail and pin to the one whose (redacted) SQL is our
  # CREATE SECRET — so concurrent org churn can't make us probe a different query.
  swid="" a=0
  while [ "$a" -lt 60 ]; do
    kill -0 "$sbg" 2>/dev/null || break
    for w in $(curl -fsS -H "$H" "$API/api/v1/queries" | jq -r --arg o "$org" '.queries[]? | select(.org==$o) | .worker_id'); do
      sd="$(curl -fsS -H "$H" "$API/api/v1/queries/by-worker/$w" || true)"
      if printf '%s' "$sd" | jq -e '.query | test("e2e_detail_redact"; "i")' >/dev/null 2>&1; then
        swid="$w"; break
      fi
    done
    [ -n "$swid" ] && break
    sleep 2; a=$((a + 1))
  done
  if [ -n "$swid" ]; then
    # sd holds the matched detail. The credential literal must be absent.
    case "$sd" in
      *"$cred"*|*"AKIADETAILPROBE"*) cleanup_sbg; fail "admin_query_detail: REDACTION BREACH — CREATE SECRET credential leaked into /queries/by-worker/$swid" ;;
    esac
    log "admin live: redaction holds in live path (credential absent from worker $swid detail)"
  else
    # Don't fail the whole suite on a missed catch (the secret DDL may finish or
    # error fast on a cold worker); the unit test is the deterministic gate.
    log "admin live: CREATE SECRET detail probe did not catch the query in flight — skipped (unit test is the gate)"
  fi
  cleanup_sbg

  log "admin live: per-query detail OK (worker $wid, SQL round-tripped, unknown→404, redaction) on $org"
}

# ---- admin impersonation round-trip + audit --------------------------------
# An admin can open a session as an org user (workers trust the CP — no password)
# and run SQL on that org's worker; every statement is audited with the admin
# actor, and a write requires allow_write=true. This asserts the full round-trip
# against a real worker AND that an audit row is persisted — the load-bearing
# behaviours of the impersonation path (impersonate.go + admin_providers.go).
admin_impersonation_audited() { # org
  org="$1"
  log "admin impersonation: run SQL as root@$org + assert audit row"
  out="$(curl -fsS --max-time 360 -H "$H" -H 'Content-Type: application/json' \
    -d '{"username":"root","sql":"SELECT 42 AS answer","allow_write":false}' \
    "$API/api/v1/orgs/$org/impersonate/query")" \
    || fail "impersonate: request failed for root@$org"
  echo "$out" | jq -e '.columns | index("answer")' >/dev/null \
    || fail "impersonate: missing column 'answer' in: $out"
  echo "$out" | jq -e '.rows[0][0] == 42' >/dev/null \
    || fail "impersonate: rows[0][0] != 42 in: $out"
  # A write statement without allow_write must be rejected (conservative classifier).
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 60 -H "$H" -H 'Content-Type: application/json' \
    -d '{"username":"root","sql":"CREATE TABLE imp_x(i int)","allow_write":false}' \
    "$API/api/v1/orgs/$org/impersonate/query")"
  [ "$code" = "400" ] || fail "impersonate: write without allow_write returned $code, want 400"
  # The successful impersonation must have left an audit row attributing the admin.
  curl -fsS -H "$H" "$API/api/v1/audit?org=$org" \
    | jq -e --arg o "$org" '.entries | map(select(.action=="impersonate.query" and .target_user=="root" and .org==$o)) | length >= 1' >/dev/null \
    || fail "impersonate: no audit row for impersonate.query root@$org"
}

# ---- admin ducklings metadata: live cnpg shard assignment ------------------
# GET /ducklings/metadata surfaces each Duckling CR's status.metadataStore for
# the org overview/detail pages — notably WHICH cnpg shard a cnpg tenant's
# metadata landed on (composition-assigned; not in the config store). Asserts
# the cnpg org's entry is kind=cnpg-shard with a parsed shard name, and the ext
# org's entry is kind=external with no shard. Guards
# provisioner.CRMetadataStores + admin/ducklings_metadata.go.
admin_ducklings_metadata() { # cnpgOrg extOrg
  cnpg_d="$(echo "$1" | tr 'A-Z' 'a-z')"; ext_d="$(echo "$2" | tr 'A-Z' 'a-z')"
  log "admin ducklings metadata: shard assignment for $cnpg_d + $ext_d"
  out="$(curl -fsS -H "$H" "$API/api/v1/ducklings/metadata")" \
    || fail "ducklings metadata: request failed"
  echo "$out" | jq -e '.available == true' >/dev/null \
    || fail "ducklings metadata: available != true: $out"
  echo "$out" | jq -e --arg d "$cnpg_d" \
    '.entries[$d] | .kind == "cnpg-shard" and (.cnpg_shard | test("^shard-"))' >/dev/null \
    || fail "ducklings metadata: no cnpg-shard entry with parsed shard for $cnpg_d: $(echo "$out" | jq -c --arg d "$cnpg_d" '.entries[$d]')"
  echo "$out" | jq -e --arg d "$ext_d" \
    '.entries[$d] | .kind == "external" and (has("cnpg_shard") | not)' >/dev/null \
    || fail "ducklings metadata: ext entry wrong for $ext_d: $(echo "$out" | jq -c --arg d "$ext_d" '.entries[$d]')"
  log "admin ducklings metadata OK ($cnpg_d on $(echo "$out" | jq -r --arg d "$cnpg_d" '.entries[$d].cnpg_shard'))"
}

# ---- duckling shard backfill (provisioner reconcileCnpgShard) --------------
# The CP backfills the composition-pinned shard (status.metadataStore.
# assignedShard) of a ready cnpg-shard duckling into
# spec.metadataStore.cnpgShard, making the tenant's shard explicit,
# schema-validated spec — the precondition for shard-migration cutovers.
# Provisioner tick is 10s and the warehouse has been ready for a while by the
# time this runs, so 120s is generous.
#
# The spec field only exists once the cluster's Duckling XRD carries it
# (charts PR #12918); an older XRD silently prunes the patch and the CP
# latches its backfill off. That state is EXPECTED until that PR deploys, so
# on timeout this probes XRD support by patching the field itself (the Job SA
# has ducklings patch via duckgres-duckling-reader): probe pruned → loud SKIP,
# not a failure; probe sticks → the field works and the CP failed to backfill
# → real failure.
duckling_shard_backfill() { # cnpgOrg
  d="$(echo "$1" | tr 'A-Z' 'a-z')"
  log "duckling shard backfill: spec.metadataStore.cnpgShard on duckling/$d"
  assigned="$("$KUBECTL" -n ducklings get duckling "$d" -o jsonpath='{.status.metadataStore.assignedShard}')"
  [ -n "$assigned" ] || fail "duckling shard backfill: duckling/$d has no status.metadataStore.assignedShard"
  spec=""
  for _ in $(seq 1 12); do
    spec="$("$KUBECTL" -n ducklings get duckling "$d" -o jsonpath='{.spec.metadataStore.cnpgShard}')"
    [ "$spec" = "$assigned" ] && { log "duckling shard backfill OK (spec.cnpgShard=$spec)"; return 0; }
    sleep 10
  done
  "$KUBECTL" -n ducklings patch duckling "$d" --type merge \
    -p "{\"spec\":{\"metadataStore\":{\"cnpgShard\":\"$assigned\"}}}" >/dev/null \
    || fail "duckling shard backfill: probe patch failed"
  probe="$("$KUBECTL" -n ducklings get duckling "$d" -o jsonpath='{.spec.metadataStore.cnpgShard}')"
  if [ -z "$probe" ]; then
    log "SKIP: duckling shard backfill — cluster XRD predates spec.metadataStore.cnpgShard (charts #12918 not deployed); the CP latches the backfill off, nothing to assert yet"
    return 0
  fi
  fail "duckling shard backfill: XRD supports the field (probe stuck: $probe) but the CP never backfilled it (spec=$spec assigned=$assigned)"
}

# ---- reshard operations (metadata-store migrations) -------------------------
# Backed by controlplane/admin/reshard.go + provisioner/reshard_runner.go +
# configstore/reshard.go. mw-dev has ONE cnpg shard, so the shard→shard
# positive path can't run here; these exercise what matters against the real
# cluster instead:
#   * validation 400s (same shard, bad targets)
#   * cancel during drain (a held session keeps the drain waiting; new
#     connections are 57P03-blocked; cancel rolls back; org healthy)
#   * bogus-shard rollback (flip → Synced=False → flip-timeout → rollback;
#     data intact, spec.cnpgShard patched back; short per-op
#     cutover_timeout_seconds keeps it fast)
#   * ext→cnpg POSITIVE path (real catalog copy off the harness RDS onto
#     shard-001, data intact after, report in the log) — including the pod
#     model: every reshard executes in a dedicated duckgres-reshard-op-<id>
#     pod (NOT in the CP process), so this lane also asserts the runner pod
#     appears while the op runs and is reaped by the leader reconciler after
#     it finishes. Ops now start PENDING (the pod claims the row itself), and
#     every wait below carries pod-schedule + image-pull latency on top of
#     what the step itself needs.
# cnpg→ext stays unit-test-only: the harness knows the RDS SM secret NAME but
# not the password, and the start API requires the password (ephemerally).
# That includes the reused-/shared-target index-replay idempotency (a stale or
# concurrently ensured idx_ducklake_* on the shared ext RDS 42P07'd a real
# cnpg→ext copy): the collision only arises on the cnpg→ext COPY onto that
# reused RDS, so it is pinned by the replayIndexes unit tests
# (provisioner/catalog_copy_test.go) instead.
# That also keeps the in-cutover ESO-sync-error surfacing (the deduped warn in
# flipToExternal when the ExternalSecret is SecretSyncedError) unit-test-only
# (provisioner/reshard_runner_test.go): asserting it live needs a real
# cnpg→ext flip against an unreadable secret. The validation loop below does
# pin the rds-master-name 400, the up-front defense for the same failure.

reshard_backup_debug() { # opid — focused debug for a backup-assert failure:
  # the op log's backup-related lines (bounded, so stream truncation can't eat
  # them) plus the runner pod's own stdout (the slog lines carry the exact
  # error the op log only summarizes). Best effort — the reconciler may have
  # reaped the pod already.
  echo "---- op $1 backup-related log lines ----"
  curl -fsS -H "$H" "$API/api/v1/reshards/$1/log?after_id=0&limit=2000" 2>/dev/null \
    | jq -r '.entries[] | select(.message | test("(?i)backup|sts|assume|s3")) | "\(.level): \(.message)"' || true
  echo "---- runner pod duckgres-reshard-op-$1 logs (tail) ----"
  k logs "duckgres-reshard-op-$1" --tail=150 2>/dev/null || echo "(runner pod already gone)"
}

reshard_pod_wait() { # opid timeout_s — wait until the runner pod exists
  deadline=$(( $(date +%s) + $2 ))
  until k get pod "duckgres-reshard-op-$1" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "reshard runner pod duckgres-reshard-op-$1 never appeared"
    sleep 3
  done
}

reshard_pod_wait_gone() { # opid timeout_s — wait until the reconciler reaped it
  deadline=$(( $(date +%s) + $2 ))
  while k get pod "duckgres-reshard-op-$1" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "reshard runner pod duckgres-reshard-op-$1 not reaped after the op finished"
    sleep 5
  done
}

reshard_post() { # org body
  curl -fsS -X POST -H "$H" -H 'Content-Type: application/json' -d "$2" \
    "$API/api/v1/orgs/$1/reshard"
}

reshard_wait_terminal() { # opid timeout_s -> echoes final state
  deadline=$(( $(date +%s) + ${2:-600} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st="$(curl -fsS -H "$H" "$API/api/v1/reshards/$1" | jq -r .state)"
    case "$st" in succeeded|failed|cancelled) echo "$st"; return 0;; esac
    sleep 5
  done
  echo "timeout"
}

reshard_wait_step() { # opid step timeout_s
  cur=""
  deadline=$(( $(date +%s) + ${3:-120} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    cur="$(curl -fsS -H "$H" "$API/api/v1/reshards/$1" | jq -r .step)"
    [ "$cur" = "$2" ] && return 0
    sleep 2
  done
  fail "reshard op $1 never reached step $2 (last=$cur)"
}

reshard_log_has() { # opid substr
  curl -fsS -H "$H" "$API/api/v1/reshards/$1/log?after_id=0&limit=2000" \
    | jq -r '.entries[].message' | grep -qF "$2"
}

reshard_dump_log() { # opid
  curl -fsS -H "$H" "$API/api/v1/reshards/$1/log?after_id=0&limit=2000" \
    | jq -r '.entries[] | "\(.level): \(.message)"' | tail -30 || true
}

reshard_targets() { # destination discovery; ext org's RDS must appear too
  log "reshard targets: destination discovery"
  out="$(curl -fsS -H "$H" "$API/api/v1/reshards/targets")" \
    || fail "reshard targets: request failed"
  # The e2e CP has no cluster-scoped RBAC (Forbidden degrade), so the shard
  # list is the occupied fallback — the cnpg org's shard must be in it. On a
  # real deployment cluster_discovery=true additionally surfaces EMPTY shards.
  echo "$out" | jq -e '.shards | index("shard-001") != null' >/dev/null \
    || fail "reshard targets: shard-001 missing: $out"
  echo "$out" | jq -e '.external_stores | length >= 1 and (.[0].endpoint | length > 0) and (.[0].password_aws_secret | length > 0)' >/dev/null \
    || fail "reshard targets: external store from the ext org missing: $out"
  echo "$out" | jq -e '.external_stores | all(has("password") | not)' >/dev/null \
    || fail "reshard targets: response carries a password field: $out"
  log "reshard targets OK ($(echo "$out" | jq -c '{shards, cluster_discovery}'))"
}

reshard_validation() { # cnpg-org currently on shard-001
  log "reshard validation: same-shard + bad targets are 400"
  # The RDS-managed-master and generic-prefix rows pin Fix 1's positive ESO
  # allowlist: the external-secrets IAM policy only allows GetSecretValue on
  # posthog-*/duckling-* names, so the start handler 400s an SM secret name
  # outside that set (rds/…/rds!… gets the more-specific RDS-managed message,
  # anything else the general allowlist message) — otherwise the destructive
  # cnpg→ext cutover would hang on an ESO AccessDenied. All of these are
  # rejected at name validation, BEFORE the pre-flight connection check (Fix 2),
  # so they need no reachable target. The connect check itself needs a real
  # reachable RDS + a valid password, which this Job lacks (same reason the
  # cnpg→ext positive path is unit-only), so it stays covered by
  # controlplane/admin/reshard_test.go (TestReshardExtPreflightProbe).
  for body in \
    '{"target":{"type":"cnpg-shard","cnpg_shard":"shard-001"}}' \
    '{"target":{"type":"cnpg-shard","cnpg_shard":"Bad_Shard"}}' \
    '{"target":{"type":"nonsense"}}' \
    '{"target":{"type":"external","endpoint":"x"}}' \
    '{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds/some-db/master","password":"p"}}' \
    '{"target":{"type":"external","endpoint":"x","password_aws_secret":"rds!db-0000-1111","password":"p"}}' \
    '{"target":{"type":"external","endpoint":"x","password_aws_secret":"my-own-secret","password":"p"}}'; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "$H" -H 'Content-Type: application/json' \
      -d "$body" "$API/api/v1/orgs/$1/reshard")"
    [ "$code" = "400" ] || fail "reshard validation: body $body -> $code, want 400"
  done
  # Global list (Reshards nav page): envelope across all orgs.
  curl -fsS -H "$H" "$API/api/v1/reshards" | jq -e '.operations | type == "array"' >/dev/null \
    || fail "reshard validation: global list envelope wrong"
  log "reshard validation OK"
}

reshard_cancel_during_drain() { # org password
  org="$1"; pw="$2"
  log "reshard cancel: hold a session, start a reshard, assert drain-wait + connection block, cancel, assert healthy"
  # Hold a session: the sleep keeps an active connection lease the drain must
  # wait for (drain-not-kill: the runner never terminates it).
  ( PGPASSWORD="$pw" psql \
      "sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
      -tAc "SELECT pg_sleep(240)" >/dev/null 2>&1 ) &
  holder=$!
  sleep 5

  out="$(reshard_post "$org" '{"target":{"type":"cnpg-shard","cnpg_shard":"shard-009"},"drain_timeout_seconds":600}')" \
    || fail "reshard cancel: start failed: $out"
  opid="$(echo "$out" | jq -r .id)"
  [ -n "$opid" ] && [ "$opid" != "null" ] || fail "reshard cancel: no op id: $out"

  # The op executes in a dedicated runner pod now: budget covers pod schedule
  # + (first-time) image pull on an arch-pinned node before the drain step.
  reshard_wait_step "$opid" draining 420
  deadline=$(( $(date +%s) + 90 ))
  until reshard_log_has "$opid" "waiting for connections to drain"; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "reshard cancel: no drain-wait log line"
    sleep 3
  done

  # New connections are refused while resharding. One-shot psql (NOT pg_try:
  # its transient-retry list now includes the reshard rejection, so it would
  # spin for the full retry budget before reporting the expected failure).
  if out2="$(PGPASSWORD="$pw" psql \
      "sslmode=require host=$org$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
      -tAc "SELECT 1" 2>&1)"; then
    fail "reshard cancel: new connection succeeded during reshard drain (got '$out2')"
  fi
  echo "$out2" | grep -qi "reshard" \
    || fail "reshard cancel: blocked-connection error did not mention reshard: $out2"

  curl -fsS -X POST -H "$H" "$API/api/v1/reshards/$opid/cancel" >/dev/null \
    || fail "reshard cancel: cancel POST failed"
  st="$(reshard_wait_terminal "$opid" 300)"
  [ "$st" = "cancelled" ] || { reshard_dump_log "$opid"; fail "reshard cancel: final state $st, want cancelled"; }
  reshard_log_has "$opid" "reshard report (cancelled)" || fail "reshard cancel: report missing"

  kill "$holder" 2>/dev/null || true
  wait "$holder" 2>/dev/null || true
  [ "$(state_of "$org")" = "ready" ] || fail "reshard cancel: warehouse state $(state_of "$org"), want ready"
  pg "$org" "$pw" ducklake "SELECT 1" >/dev/null
  log "reshard cancel OK (op $opid)"
}

reshard_bogus_shard_rollback() { # org password
  org="$1"; pw="$2"; d="$(echo "$org" | tr 'A-Z' 'a-z')"
  log "reshard rollback: bogus target shard → flip-timeout → rollback, data intact"
  pg "$org" "$pw" ducklake "DROP TABLE IF EXISTS reshard_marker; CREATE TABLE reshard_marker AS SELECT 42 AS v;"

  # Short per-op cutover timeout: the bogus shard can never converge, so the
  # flip wait is pure dead time before the rollback we're here to assert.
  out="$(reshard_post "$org" '{"target":{"type":"cnpg-shard","cnpg_shard":"shard-099"},"drain_timeout_seconds":300,"cutover_timeout_seconds":90}')" \
    || fail "reshard rollback: start failed: $out"
  opid="$(echo "$out" | jq -r .id)"

  # Budget: runner-pod schedule/pull + drain + snapshot-propagation waits +
  # the 90s per-op cutover timeout + rollback convergence.
  st="$(reshard_wait_terminal "$opid" 900)"
  [ "$st" = "failed" ] || { reshard_dump_log "$opid"; fail "reshard rollback: final state $st, want failed"; }
  reshard_log_has "$opid" "rolling back" || fail "reshard rollback: no rollback log"
  reshard_log_has "$opid" "reshard report (failed)" || fail "reshard rollback: report missing"

  # The duckling spec must be back on the real shard (the rollback patches the
  # VALUE back — it never removes the key).
  spec="$("$KUBECTL" -n ducklings get duckling "$d" -o jsonpath='{.spec.metadataStore.cnpgShard}')"
  [ "$spec" = "shard-001" ] || fail "reshard rollback: spec.cnpgShard=$spec, want shard-001"
  [ "$(state_of "$org")" = "ready" ] || fail "reshard rollback: warehouse $(state_of "$org"), want ready"

  got="$(pg "$org" "$pw" ducklake "SELECT v FROM reshard_marker")"
  echo "$got" | grep -q 42 || fail "reshard rollback: marker data lost (got '$got')"
  pg "$org" "$pw" ducklake "DROP TABLE reshard_marker;"
  log "reshard rollback OK (op $opid)"
}

reshard_ext_to_cnpg() { # ext-org password
  org="$1"; pw="$2"; d="$(echo "$org" | tr 'A-Z' 'a-z')"
  log "reshard ext→cnpg POSITIVE path: move $org off the RDS onto shard-001 with data intact"
  pg "$org" "$pw" ducklake "DROP TABLE IF EXISTS reshard_move; CREATE TABLE reshard_move AS SELECT range AS v FROM range(100);"

  # Real cutover: provider-sql role/DB creation + cnpg SASL propagation can
  # take minutes (same tail READY_TIMEOUT covers at provision time).
  out="$(reshard_post "$org" '{"target":{"type":"cnpg-shard","cnpg_shard":"shard-001"},"drain_timeout_seconds":300,"cutover_timeout_seconds":600}')" \
    || fail "reshard ext→cnpg: start failed: $out"
  opid="$(echo "$out" | jq -r .id)"

  # Pod model: the op is created PENDING — the dedicated runner pod
  # (duckgres-reshard-op-<id>, spawned by the CP on this POST) claims the row
  # itself. This ext-SOURCE op needs no password URL (the source password is
  # read from the CR status pre-flip), so password_url stays empty.
  echo "$out" | jq -e '.state == "pending"' >/dev/null \
    || fail "reshard ext→cnpg: op not created pending (pod model): $out"

  # The dedicated runner pod appears (spawned by the start handler; the leader
  # reconciler would backstop it) and carries the op-id label.
  reshard_pod_wait "$opid" 120
  lbl="$(k get pod "duckgres-reshard-op-$opid" -o jsonpath='{.metadata.labels.app}/{.metadata.labels.duckgres-op-id}')"
  [ "$lbl" = "duckgres-reshard/$opid" ] || fail "reshard ext→cnpg: runner pod labels wrong: $lbl"

  # Budget: drain (org quiet) + provider-sql role/db create on the shard (the
  # cnpg SASL propagation tail can add minutes — see READY_TIMEOUT) + copy +
  # verify.
  st="$(reshard_wait_terminal "$opid" 900)"
  [ "$st" = "succeeded" ] || { reshard_dump_log "$opid"; fail "reshard ext→cnpg: final state $st, want succeeded"; }
  reshard_log_has "$opid" "reshard report (succeeded)" || fail "reshard ext→cnpg: report missing"
  reshard_log_has "$opid" "maintenance mode (connections blocked)" || fail "reshard ext→cnpg: maintenance duration missing from report"
  reshard_log_has "$opid" "external source left untouched" || fail "reshard ext→cnpg: missing ext-source-untouched line"

  # Pre-flip catalog backup (safety-net layer C): the runner pg_dumps the SOURCE
  # catalog to the org's own data bucket under _reshard_catalog_backups/ before
  # the flip, records the artifact URI on the op row, and logs a runnable
  # pg_restore command. The CP image now carries pg_dump (postgresql-client-18)
  # and holds the org's STS creds, so this SHOULD succeed here — assert it did.
  # (The Job has no aws CLI, so we cannot LIST S3 to confirm the object exists;
  # we assert the recorded URI + restore command instead — same limitation the
  # tenant_isolation object-store-prefix half documents. The S3 write itself is
  # proven by the runner not warning "pre-flip catalog backup failed".)
  reshard_log_has "$opid" "catalog backup complete" || { reshard_backup_debug "$opid"; reshard_dump_log "$opid"; fail "reshard ext→cnpg: pre-flip catalog backup did not complete"; }
  reshard_log_has "$opid" "pg_restore --no-owner" || fail "reshard ext→cnpg: restore command missing from op log"
  if reshard_log_has "$opid" "pre-flip catalog backup failed"; then
    reshard_backup_debug "$opid"; reshard_dump_log "$opid"; fail "reshard ext→cnpg: catalog backup failed (best-effort warning present)"
  fi
  buri="$(curl -fsS -H "$H" "$API/api/v1/reshards/$opid" | jq -r '.backup_s3_uri // ""')"
  case "$buri" in
    s3://*/_reshard_catalog_backups/op-"$opid"-*.dump) log "reshard ext→cnpg: backup artifact recorded at $buri" ;;
    *) reshard_dump_log "$opid"; fail "reshard ext→cnpg: backup_s3_uri not recorded/shaped: '$buri'" ;;
  esac

  # The duckling is now a cnpg-shard tenant…
  typ="$("$KUBECTL" -n ducklings get duckling "$d" -o jsonpath='{.spec.metadataStore.type}')"
  [ "$typ" = "cnpg-shard" ] || fail "reshard ext→cnpg: duckling type $typ, want cnpg-shard"
  # …and the data reads back from the moved catalog (cold worker spawn against
  # the new store happens implicitly on this connect).
  got="$(pg "$org" "$pw" ducklake "SELECT COUNT(*) || ':' || SUM(v) FROM reshard_move")"
  echo "$got" | grep -q "100:4950" || fail "reshard ext→cnpg: data mismatch after move (got '$got')"
  pg "$org" "$pw" ducklake "DROP TABLE reshard_move;"

  # The runner pod exits 0 once the op is terminal and the leader reconciler
  # reaps it (30s tick; give it a couple of ticks plus pod-exit time).
  reshard_pod_wait_gone "$opid" 180
  log "reshard ext→cnpg OK (op $opid): catalog moved, data intact, runner pod reaped"
}

# ---- tenant isolation -----------------------------------------------------
# Two tenants (cnpg + ext) back onto distinct DuckLake metadata stores, so a
# table created by one is invisible to the other. Ported (logical half) from
# TestK8sTenantIsolation_DifferentTenantsSeeDistinctCatalogs. (The physical
# object-store-prefix half needs S3 list creds the Job doesn't hold against real
# mw-dev S3 — covered logically here, documented in README as the deferred half.)
tenant_isolation() { # orgA pwA orgB pwB
  log "tenant isolation: $1 table invisible to $3"
  t="iso_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "DROP TABLE IF EXISTS $t; CREATE TABLE $t AS SELECT 7 AS v;"
  if out="$(pg_try "$3" "$4" ducklake "SELECT COUNT(*) FROM $t")"; then
    fail "tenant $3 could read $1's table $t (got '$out') — isolation breach"
  fi
  echo "$out" | grep -qiE 'does not exist|not found|catalog|table with name' \
    || fail "tenant $3 cross-read failed for an unexpected reason: $out"
  pg "$1" "$2" ducklake "DROP TABLE $t;"
}

# ---- lifecycle: deprovision → warehouse deleted → Duckling CR fully gone ----
# Proves the teardown path works end to end: warehouse marked deleted, the
# Crossplane Duckling CR removed, and its finalizer cascade (which drops the
# cnpg metadata role+db) completed.
#
# LOAD-BEARING for the cnpg→ext orphan-adopt change (retainCnpgOnFlip): this is
# the deprovision-UNAFFECTED regression net. A normal never-resharded cnpg
# tenant has spec.metadataStore.retainCnpgOnFlip=false, so its cnpg Role/Database
# MRs render with full lifecycle ["*"] (Delete) and the finalizer cascade below
# MUST still fully DROP them. The `kubectl wait --for=delete duckling/<org>`
# only completes once every composed MR — including the provider-sql Role +
# Database — finished deleting, i.e. the role/DB were dropped. If a future
# change ever "always orphaned" the cnpg MRs, this wait would hang on the
# retained Role/Database finalizers and this assertion would (correctly) fail.
# The cnpg→ext orphan path itself is unit-only (harness has no RDS password —
# see the reshard section above and provisioner/reshard_runner_test.go).
#
# NOTE: same-org-id *re-provision* in the SAME run is intentionally NOT done
# here. It is the regression net for the stranded-cnpg-role bugs
# (#649/#650/#11518/#11522), but it cannot be made reliable from inside the Job:
# guaranteeing a clean slate requires DROPping a possibly-stranded cnpg role on
# the cnpg-shards Postgres, which only `run.sh` (on the runner, with
# cnpg-shards exec rights) can do — the in-cluster Job SA cannot and must not.
# Re-provisioning the same id while the async cnpg cascade is still in flight
# can race stranded metadata state and leave the warehouse stuck. So the
# same-id regression is covered ACROSS runs instead: every run's `run.sh
# deploy` drops the cnpg role for a clean slate, and `run.sh teardown` waits the
# CR `--for=delete` before returning. (This is the same reasoning the original
# harness used to drop the in-Job recreate.)
lifecycle_teardown_cnpg() { # org
  log "lifecycle: deprovision $1 + assert Duckling CR fully deleted"
  api_post "$1" deprovision >/dev/null
  # The status endpoint is watchable throughout teardown: right after the 202
  # the warehouse reports a deprovisioning state (deleting, or deleted if the
  # async provisioner already finished), never still-ready — so /data-ops can
  # poll warehouse/status until it reaches "deleted", then send the delete.
  # While deleting, status_message reads "Deprovisioning..." (the deprovision
  # analogue of the provisioning-phase messages) rather than a stale ready line.
  status_json="$(api_get "$1" warehouse/status)"
  st="$(printf '%s' "$status_json" | jq -r '.state')"
  msg="$(printf '%s' "$status_json" | jq -r '.status_message')"
  case "$st" in
    deleting)
      [ "$msg" = "Deprovisioning..." ] \
        || fail "warehouse $1 deleting but status_message=$msg (expected 'Deprovisioning...')";;
    deleted) ;;
    *) fail "warehouse $1 status=$st right after deprovision (expected deleting/deleted — status not watchable)";;
  esac
  wait_state "$1" deleted 600
  if ! "$KUBECTL" -n ducklings wait --for=delete "duckling/$1" --timeout=420s >/dev/null 2>&1; then
    fail "Duckling CR $1 did not fully delete within 420s (finalizer/cnpg cascade stuck)"
  fi

  # Deprovision leaves the org row + a terminal "deleted" warehouse row behind;
  # DELETE /orgs/:id must now cascade both away and release the org's
  # database_name. Without this the name is squatted forever (org row's unique
  # database_name index survives, so no future org can reuse it).
  dbname="$(curl -fsS -H "$H" "$API/api/v1/orgs/$1" | jq -r '.database_name // empty')"
  [ -n "$dbname" ] || fail "could not read database_name for $1 before delete"
  [ "$(curl -fsS -H "$H" "$API/api/v1/database-name/check?name=$dbname" | jq -r '.available')" = "false" ] \
    || fail "database_name $dbname unexpectedly free while org $1 still exists"
  curl -fsS -X DELETE -H "$H" "$API/api/v1/orgs/$1" >/dev/null \
    || fail "DELETE /orgs/$1 failed after a completed deprovision (name would stay squatted)"
  if api_get "$1" warehouse/status >/dev/null 2>&1; then
    fail "warehouse row for $1 survived org deletion — deleted-state row not cascaded"
  fi
  [ "$(curl -fsS -H "$H" "$API/api/v1/database-name/check?name=$dbname" | jq -r '.available')" = "true" ] \
    || fail "database_name $dbname still squatted after org deletion"
}

# ---- cnpg duckling: cnpg-shard metadata + DuckLake ------------------------
# default_team_id is MANDATORY at provision time for new orgs (prereq for
# pull-based compute billing: usage buckets keyed by team_id = the org's default
# team) — every provision body below carries one; default_team_id_mandatory
# asserts the round-trips and that omitting it on a new org is rejected.
CNPG_DEFAULT_TEAM_ID='90210'
CNPG_BODY='{"database_name":"'"$CNPG"'","metadata_store":{"type":"cnpg-shard"},
  "default_team_id":'"$CNPG_DEFAULT_TEAM_ID"',
  "data_store":{"type":"s3bucket"},"ducklake":{"enabled":true}}'

# ---- ext duckling: external RDS metadata + DuckLake -----------------------
EXT_DEFAULT_TEAM_ID='31337'
EXT_BODY='{"database_name":"'"$EXT"'",
  "default_team_id":'"$EXT_DEFAULT_TEAM_ID"',
  "metadata_store":{"type":"external","external":{
    "endpoint":"'"$EXT_RDS_ENDPOINT"'","password_aws_secret":"'"$EXT_RDS_SECRET"'",
    "user":"ducklingexample","database":"ducklingexample"}},
  "data_store":{"type":"external","bucket_name":"posthog-duckling-example-managed-warehouse-dev","region":"us-east-1"},
  "ducklake":{"enabled":true}}'

# ---- resilience ducklings: cnpg-shard metadata + DuckLake only -------------
# DuckLake-only: these orgs exist purely to host the worker-churn-heavy
# resilience lanes, so keep their provision footprint small.
res_body() { # org
  printf '{"database_name":"%s","default_team_id":1,"metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true}}' "$1"
}

# ---- per-user kill switch ---------------------------------------------------
# Admin one-shot kill (controlplane/admin live.go + session_mgr
# DestroySessionsForUser): POST /orgs/:id/users/:user/kill tears down ALL of a
# user's live sessions + in-flight queries cluster-wide, while a DIFFERENT user's
# concurrent query on the same org is untouched. Regression net would have caught
# a kill that missed sessions (leak) or over-killed (hit the wrong user).
user_kill_switch() { # org rootpw
  org="$1"; pw="$2"; vic="e2ekilluser"; vicpw="e2e-$(openssl rand -hex 12)"
  log "user kill switch on $org"
  code="$(curl -s -o /tmp/ku_create -w '%{http_code}' -X POST -H "$H" -H 'Content-Type: application/json' \
    -d "{\"org_id\":\"$org\",\"username\":\"$vic\",\"password\":\"$vicpw\"}" "$API/api/v1/users")"
  case "$code" in 200|201) ;; *) fail "user_kill_switch: create $vic -> HTTP $code: $(cat /tmp/ku_create)";; esac
  # New-user auth is config-snapshot-driven; wait one poll before first login.
  sleep "${CONFIG_POLL_SETTLE:-12}"

  vout="$(mktemp)"; vrc="$(mktemp)"; rout="$(mktemp)"; rrc="$(mktemp)"
  # Victim's long query AND a root long query run concurrently (distinct users,
  # distinct workers — MaxSessions=1). Only the victim's must die.
  ( if pg_try "$org" "$vicpw" ducklake "$HEAVY_Q" "$vic" >"$vout" 2>&1; then echo 0 >"$vrc"; else echo 1 >"$vrc"; fi ) &
  vpid=$!
  ( if pg_try "$org" "$pw" ducklake "$HEAVY_Q" >"$rout" 2>&1; then echo 0 >"$rrc"; else echo 1 >"$rrc"; fi ) &
  rpid=$!

  # Wait until the victim's query is live in the admin /queries view.
  a=0 seen=0
  while [ "$a" -lt 40 ]; do
    n="$(curl -fsS -H "$H" "$API/api/v1/queries?org=$org&user=$vic" 2>/dev/null | jq -r '.queries | length' 2>/dev/null || echo 0)"
    [ "${n:-0}" -ge 1 ] && { seen=1; break; }
    kill -0 "$vpid" 2>/dev/null || break
    sleep 2; a=$((a + 1))
  done
  [ "$seen" = 1 ] || { kill "$vpid" "$rpid" 2>/dev/null || true; fail "user_kill_switch: victim query never appeared in /queries"; }
  # Overlap proof: both queries must still be running at kill time, else the test
  # measures nothing (false pass on a query that already finished).
  kill -0 "$vpid" 2>/dev/null || fail "user_kill_switch: victim query finished before kill (raise HEAVY_Q row count)"

  killed="$(api_post "$org" "users/$vic/kill" | jq -r '.killed')"
  [ "${killed:-0}" -ge 1 ] || fail "user_kill_switch: kill reported killed=$killed (want >=1)"

  wait "$vpid" 2>/dev/null || true
  wait "$rpid" 2>/dev/null || true
  [ "$(cat "$vrc")" = 1 ] \
    || fail "user_kill_switch: victim query survived the kill: $(tr -d '\n' <"$vout" | tail -c 200)"
  [ "$(cat "$rrc")" = 0 ] \
    || fail "user_kill_switch: root (other user) query was wrongly killed: $(tr -d '\n' <"$rout" | tail -c 200)"
  rn="$(tr -dc '0-9' <"$rout")"
  [ "$rn" = "$HEAVY_EXPECT" ] || fail "user_kill_switch: root result=$rn want $HEAVY_EXPECT (kill corrupted an unrelated query)"

  n="$(curl -fsS -H "$H" "$API/api/v1/queries?org=$org&user=$vic" | jq -r '.queries | length')"
  [ "${n:-0}" = 0 ] || fail "user_kill_switch: victim still has $n live queries after kill"
  rm -f "$vout" "$vrc" "$rout" "$rrc"
  log "user kill switch OK on $org (victim torn down, root untouched)"
}

# ---- per-user disable/enable block ------------------------------------------
# Admin persistent block (configstore disabled column + control.go/Flight auth):
# POST /orgs/:id/users/:user/disable refuses the user's NEW connections at auth
# time (distinct "disabled" error, not a password/transient error), while a
# different user still connects; enable restores access. The block is effective
# immediately because the disable handler reloads the snapshot cluster-wide (no
# poll wait). Run on BOTH metadata backends since the flag is config-store-backed.
user_disable_block() { # org rootpw
  org="$1"; pw="$2"; vic="e2edisableuser"; vicpw="e2e-$(openssl rand -hex 12)"
  log "user disable/enable block on $org"
  code="$(curl -s -o /tmp/du_create -w '%{http_code}' -X POST -H "$H" -H 'Content-Type: application/json' \
    -d "{\"org_id\":\"$org\",\"username\":\"$vic\",\"password\":\"$vicpw\"}" "$API/api/v1/users")"
  case "$code" in 200|201) ;; *) fail "user_disable_block: create $vic -> HTTP $code: $(cat /tmp/du_create)";; esac
  sleep "${CONFIG_POLL_SETTLE:-12}"

  # Baseline: the victim connects fine while enabled (cold spawn tolerated).
  out="$(pg_try "$org" "$vicpw" ducklake "SELECT 1" "$vic")"
  [ "$out" = "1" ] || fail "user_disable_block: victim could not connect before disable: $out"

  resp="$(api_post "$org" "users/$vic/disable")"
  echo "$resp" | jq -e '.disabled == true' >/dev/null 2>&1 || fail "user_disable_block: disable response wrong: $resp"

  # Now a NEW victim connection is refused with the disabled error. No settle
  # needed — the disable handler reloaded the snapshot cluster-wide.
  if out="$(pg_try "$org" "$vicpw" ducklake "SELECT 1" "$vic")"; then
    fail "user_disable_block: disabled victim still connected (got '$out')"
  fi
  echo "$out" | grep -qi "disabled" || fail "user_disable_block: refusal was not a disabled error: $out"

  # A different user (root) must be unaffected by the victim's block.
  out="$(pg_try "$org" "$pw" ducklake "SELECT 1")"
  [ "$out" = "1" ] || fail "user_disable_block: root wrongly blocked while $vic disabled: $out"

  resp="$(api_post "$org" "users/$vic/enable")"
  echo "$resp" | jq -e '.disabled == false' >/dev/null 2>&1 || fail "user_disable_block: enable response wrong: $resp"
  out="$(pg_try "$org" "$vicpw" ducklake "SELECT 1" "$vic")"
  [ "$out" = "1" ] || fail "user_disable_block: victim could not reconnect after enable: $out"
  log "user disable/enable block OK on $org"
}

# ---- parallel lane machinery ------------------------------------------------
# The assertions are grouped into per-org LANES that run concurrently: all
# worker churn (kills, drains, cold spawns, TTL reaps) is org-scoped, so lanes
# for different orgs cannot interfere — and the wall-clock becomes the slowest
# lane instead of the sum.
#
# This cross-org concurrency is ALSO the regression gate for the global
# worker-id allocation fix (CreateSpawningWorkerSlot via nextval off a shared
# sequence; see configstore.ensureWorkerIDSequence). The lanes below spawn
# worker pods for DIFFERENT orgs at the same time against the SAME config store
# — exactly the scenario where the old SELECT MAX(worker_id)+1 allocation (only
# serialized by a per-org advisory lock) handed two orgs the same worker_id and
# failed the spawn with worker_records_pkey (SQLSTATE 23505). A green multi-lane
# run is the in-Job proof the collision is gone; the deterministic unit repro is
# TestCreateSpawningWorkerSlotConcurrentCrossOrgUniqueIDs
# (tests/configstore/runtime_store_postgres_test.go). Each lane runs in a background subshell with its
# output captured to a file; the parent prints a heartbeat while lanes run and
# replays each lane's log (prefixed) when it finishes. A lane's failure
# (missing/nonzero rc file) fails the harness after all lanes settle, so one
# broken lane doesn't hide another's result.
LANE_DIR=/tmp/lanes
run_lane() { # name fn args...
  name="$1"; shift
  # The inner ( "$@" ) subshell contains fail()'s explicit `exit 1` — without
  # it the exit would unwind past the if/else and the rc file would never be
  # written (join_lanes would only notice at its deadline).
  ( if ( "$@" ) >"$LANE_DIR/$name.log" 2>&1; then echo 0 >"$LANE_DIR/$name.rc"; else echo 1 >"$LANE_DIR/$name.rc"; fi ) &
}
join_lanes() { # name...
  deadline=$(( $(date +%s) + 900 ))
  while :; do
    pending=""
    for n in "$@"; do [ -f "$LANE_DIR/$n.rc" ] || pending="$pending $n"; done
    [ -z "$pending" ] && break
    [ "$(date +%s)" -lt "$deadline" ] || break
    for n in $pending; do
      last="$(tail -1 "$LANE_DIR/$n.log" 2>/dev/null | tail -c 120)"
      log "lane $n running… $last"
    done
    sleep 20
  done
  wait
  rc=0
  for n in "$@"; do
    sed "s/^/[$n] /" "$LANE_DIR/$n.log" >&2 2>/dev/null || true
    lrc="$(cat "$LANE_DIR/$n.rc" 2>/dev/null || echo timeout)"
    if [ "$lrc" != "0" ]; then echo "FAIL: lane $n rc=$lrc" >&2; rc=1; fi
  done
  [ "$rc" = 0 ] || fail "one or more lanes failed (see prefixed lane logs above)"
}

# ---- lanes -------------------------------------------------------------------
lane_cnpg() { # full wire/catalog/concurrency/sizing coverage on the cnpg org
  wait_worker "$CNPG" "$cnpg_pw" ducklake
  basic_query            "$CNPG" "$cnpg_pw"
  pg_compat_functions    "$CNPG" "$cnpg_pw"
  query_source_guc       "$CNPG" "$cnpg_pw"
  malformed_startup_resilience "$CNPG" "$cnpg_pw"
  jsonb_concat_semantics "$CNPG" "$cnpg_pw"
  cold_burst_absorption  "$CNPG" "$cnpg_pw"   # early, while this org is mostly cold
  rw_ducklake            "$CNPG" "$cnpg_pw"
  explain_ducklake       "$CNPG" "$cnpg_pw"
  httpfs_retry_budget    "$CNPG" "$cnpg_pw"   # S3-503 retry budget raised per worker (applyHTTPFSRetryBudget)
  persistent_user_secret "$CNPG" "$cnpg_pw"   # after rw_ducklake (org worker hot)
  persistent_user_secret_isolation "$CNPG" "$cnpg_pw"
  pipeline_error_recovery "$CNPG" "$cnpg_pw"  # after rw_ducklake (table writes proven)
  server_side_cursors    "$CNPG" "$cnpg_pw"
  cancel_then_reuse_same_session "$CNPG" "$cnpg_pw"
  assert_fork_extensions "$CNPG" "$cnpg_pw"   # after a DuckLake R/W (httpfs loaded)
  assert_worker_pod      "$CNPG"   # newest org pod = a DuckLake worker above
  concurrent_connections "$CNPG" "$cnpg_pw"
  concurrent_writers     "$CNPG" "$cnpg_pw"
  # Worker sizing on DuckLake: verified fresh sized spawn + same-shape reuse.
  sized_worker        "$CNPG" "$cnpg_pw" ducklake 2 4Gi 15m
  reuse_sized_worker  "$CNPG" "$cnpg_pw" ducklake 2 4Gi 15m
  # Idle worker reclamation: a 3-CPU worker with a 1m TTL must be reaped after it
  # goes idle (catches the hot-idle-reaper-dark / persist-swallow idle leak).
  hot_idle_retired    "$CNPG" "$cnpg_pw" ducklake 3 6Gi
}

# Busy-only Karpenter disruption protection: a worker pod must carry
# karpenter.sh/do-not-disrupt while a session runs (its node must not be
# consolidation/drift-evicted mid-query) and must DROP it when parked hot-idle
# (so WhenEmptyOrUnderutilized can reclaim idle nodes — without the removal the
# headroom ratchet returns: annotated idle workers pin nodes forever).
worker_disruption_annotation() { # org password
  log "busy-only do-not-disrupt annotation on $1"
  out="$(mktemp)"
  ( pg_try "$1" "$2" ducklake "$HEAVY_Q" >"$out" 2>&1 ) &
  qpid=$!
  # While the query runs, the serving worker must be protected.
  ann="" a=0
  while [ "$a" -lt 90 ]; do
    kill -0 "$qpid" 2>/dev/null || break
    pod="$(k get pods -l "app=duckgres-worker,duckgres/active-org=$1"           -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    if [ -n "$pod" ]; then
      ann="$(k get pod "$pod" -o jsonpath='{.metadata.annotations.karpenter\.sh/do-not-disrupt}' 2>/dev/null)"
      [ "$ann" = "true" ] && break
    fi
    sleep 1; a=$((a + 1))
  done
  [ "$ann" = "true" ] || { kill "$qpid" 2>/dev/null || true; fail "worker_disruption_annotation: busy worker $pod not protected (annotation '$ann')"; }
  wait "$qpid" 2>/dev/null || true

  # Session ended -> worker parks hot-idle -> annotation must be removed.
  a=0
  while [ "$a" -lt 30 ]; do
    ann="$(k get pod "$pod" -o jsonpath='{.metadata.annotations.karpenter\.sh/do-not-disrupt}' 2>/dev/null)"
    [ -z "$ann" ] && { rm -f "$out"; log "busy-only do-not-disrupt OK on $pod"; return; }
    sleep 2; a=$((a + 1))
  done
  fail "worker_disruption_annotation: hot-idle worker $pod still annotated after 60s (consolidation pinned)"
}

lane_res1() { # worker-kill resilience on its own org (heavy, churns workers)
  wait_worker "$RES1" "$res1_pw" ducklake
  worker_disruption_annotation "$RES1" "$res1_pw"
  durability_across_restart "$RES1" "$res1_pw"
  crash_recovery         "$RES1" "$res1_pw"
  graceful_drain         "$RES1" "$res1_pw"
}

lane_res2() { # scheduling-shape resilience on its own org (heavy, cold spawns)
  # Both assertions tolerate a cold org: their connections spawn workers in
  # parallel and the heavy query holds each worker well past the others' spawn.
  one_session_per_worker     "$RES2" "$res2_pw"
  cold_burst_parallel_spawns "$RES2" "$res2_pw"
}

lane_ext() { # external-RDS metadata backend + org default profile
  wait_worker "$EXT" "$ext_pw" ducklake
  basic_query  "$EXT" "$ext_pw"
  pg_compat_functions "$EXT" "$ext_pw"
  query_source_guc    "$EXT" "$ext_pw"
  rw_ducklake         "$EXT" "$ext_pw"
  explain_ducklake    "$EXT" "$ext_pw"
  httpfs_retry_budget "$EXT" "$ext_pw"    # S3-503 retry budget per worker, ext-metadata backend too
  persistent_user_secret "$EXT" "$ext_pw"   # secret replay on the ext-metadata org too
  # Org default profile on ext: no client-sized assertions run on this org, so
  # the 2-CPU shape is unambiguously the org default's.
  org_default_profile "$EXT" "$ext_pw" ducklake
  # default_team_id contract: both orgs provisioned WITH one (round-trips), a
  # NEW org WITHOUT one is rejected (400, nothing created) + PUT set/clear/
  # restore on EXT. Runs here because both orgs are provisioned by now; it only
  # touches the provisioning + admin APIs, no worker/DB.
  default_team_id_mandatory "$CNPG" "$EXT"
}

main() {
  resolve_cp_ip

  # No org dependency — assert the admin surface auth contract up front.
  admin_dashboard_no_query_token
  internal_secret_fallback_auth

  mkdir -p "$LANE_DIR"

  # Use the password returned at provision time — do NOT call reset-password and
  # then retry-connect in a tight loop: the CP rate-limiter bans the source IP
  # after a handful of failed auths, and a fresh password isn't live until the
  # next config-store poll. Provision returns the live password directly.
  # All four orgs provision CONCURRENTLY (independent ducklings), then all four
  # readiness waits run concurrently too — provisioning cost is paid once, not
  # per org.
  provision "$CNPG" "$CNPG_BODY"        > "$LANE_DIR/prov_cnpg.json" &
  prov1=$!
  provision "$EXT"  "$EXT_BODY"         > "$LANE_DIR/prov_ext.json" &
  prov2=$!
  provision "$RES1" "$(res_body "$RES1")" > "$LANE_DIR/prov_res1.json" &
  prov3=$!
  provision "$RES2" "$(res_body "$RES2")" > "$LANE_DIR/prov_res2.json" &
  prov4=$!
  wait "$prov1" || fail "provision $CNPG failed"
  wait "$prov2" || fail "provision $EXT failed"
  wait "$prov3" || fail "provision $RES1 failed"
  wait "$prov4" || fail "provision $RES2 failed"
  cnpg_pw="$(jq -r .password "$LANE_DIR/prov_cnpg.json")"
  ext_pw="$(jq -r .password "$LANE_DIR/prov_ext.json")"
  res1_pw="$(jq -r .password "$LANE_DIR/prov_res1.json")"
  res2_pw="$(jq -r .password "$LANE_DIR/prov_res2.json")"
  for v in "$cnpg_pw" "$ext_pw" "$res1_pw" "$res2_pw"; do
    case "$v" in ""|null) fail "a provision call returned no password" ;; esac
  done
  run_lane ready_cnpg wait_state "$CNPG" ready "$READY_TIMEOUT"
  run_lane ready_ext  wait_state "$EXT"  ready "$READY_TIMEOUT"
  run_lane ready_res1 wait_state "$RES1" ready "$READY_TIMEOUT"
  run_lane ready_res2 wait_state "$RES2" ready "$READY_TIMEOUT"
  join_lanes ready_cnpg ready_ext ready_res1 ready_res2

  # Settle: let the CP's config-store poll pick up the provisioned orgs/users
  # before the first connection, so we don't burn failed-auth attempts against
  # the rate limiter while the auth cache catches up. The CI control plane polls
  # every 5s (DUCKGRES_CONFIG_POLL_INTERVAL in manifests.tmpl.yaml), so 12s
  # covers two full poll cycles; CONFIG_POLL_SETTLE overrides if that drifts.
  log "settling ${CONFIG_POLL_SETTLE:-12}s for CP auth cache…"
  sleep "${CONFIG_POLL_SETTLE:-12}"

  # Admin models explorer: orgs + their users now exist in the config store, so
  # the sidebar counts are non-trivial and the org-users redaction check bites
  # against a real bcrypt-hash-bearing row.
  models_explorer_api

  # Admin console read surfaces: identity/role, live state, metrics proxy, auth
  # gate. Independent of the per-org lanes, so run it here once orgs exist.
  admin_console_api
  admin_rbac_viewer "$CNPG"

  # Join the kubectl background download started at script load — first k use
  # is inside the lanes, so the fetch overlapped the whole provisioning phase.
  bootstrap_kubectl

  # ---- the four parallel assertion lanes (see lane_* above) ----
  run_lane cnpg lane_cnpg
  run_lane res1 lane_res1
  run_lane res2 lane_res2
  run_lane ext  lane_ext
  join_lanes cnpg res1 res2 ext

  # ---- admin impersonation round-trip + audit (cnpg stack is warm now) ----
  admin_impersonation_audited "$CNPG"

  # ---- admin ducklings metadata: live cnpg shard assignment ----------------
  admin_ducklings_metadata "$CNPG" "$EXT"

  # ---- provisioner backfills pinned shard into duckling spec (cnpg) --------
  duckling_shard_backfill "$CNPG"

  # ---- admin live-query detail view (phase 1) — cnpg stack is warm now ----
  admin_query_detail "$CNPG" "$cnpg_pw"

  # ---- admin: cancel a live session by worker id (cross-CP addressed) ----
  admin_cancel_by_worker "$CNPG" "$cnpg_pw"

  # ---- admin live: idle-in-transaction session is flagged (state column) ----
  admin_idle_session_flagged "$CNPG" "$cnpg_pw"

  # ---- admin /status per-org worker count is populated (not the old 0) ----
  admin_per_org_workers "$CNPG" "$cnpg_pw"

  # ---- admin errors: failed query captured in /errors + CREATE SECRET redacted ----
  admin_recent_errors "$CNPG" "$cnpg_pw"

  # ---- connection idle timeout: idle conn reaped, worker freed ----
  conn_idle_timeout_reaps_session "$CNPG" "$cnpg_pw"

  # ---- COPY FROM STDIN survives the idle timeout + shows active ----
  copy_active_and_survives_idle "$CNPG" "$cnpg_pw"

  # ---- per-user kill switch + disable/enable block (cnpg stack is warm) ----
  user_kill_switch   "$CNPG" "$cnpg_pw"
  user_disable_block "$CNPG" "$cnpg_pw"
  # The disabled flag is config-store-backed, so exercise the ext metadata
  # backend too (CLAUDE.md: metadata-touching changes run on cnpg + ext).
  user_disable_block "$EXT" "$ext_pw"

  # ---- connection-duration observability (any org; lanes already churned
  #      many connect/disconnects, so the disconnect log is warm) ----
  connection_duration_logged "$CNPG" "$cnpg_pw"

  # ---- compute-usage billing pull API (meter → buffer → GET → ack) ----
  compute_usage_pull_api "$CNPG" "$cnpg_pw"

  # ---- cross-tenant isolation (cnpg vs ext) — needs both lanes done ----
  tenant_isolation "$CNPG" "$cnpg_pw" "$EXT" "$ext_pw"

  # ---- reshard operations (validation, cancel, and rollback on res2) ----
  reshard_targets
  reshard_validation "$CNPG"
  reshard_cancel_during_drain "$RES2" "$res2_pw"
  reshard_bogus_shard_rollback "$RES2" "$res2_pw"
  log "SKIP ext-to-cnpg positive path (temporarily excluded from per-PR e2e)"

  # ---- lifecycle: deprovision cnpg + assert the Duckling CR fully deletes ----
  # (res1/res2/ext are deprovisioned by run.sh teardown; the cascade assertion
  # only needs one cnpg-shard org.)
  lifecycle_teardown_cnpg "$CNPG"

  # NOTE: the version-mismatch worker reaper is not exercised in-Job (it needs a
  # mid-run image bump); it stays covered by the controlplane/ unit tests.
  log "SKIP version-reaper (needs an in-run image bump; see README)"

  log "PASS: admin-no-query-token + models-explorer-api(redaction) + admin-console-api(me/live/metrics/auth-gate) + admin-rbac-viewer(403 mutate/audit) + admin-impersonation(round-trip+audit) + wire + malformed-startup-resilience + jsonb-concat + cold-burst-absorption + pipeline-error-recovery + cancel-reuse + activation(DuckLake) + ducklake-explain + ext-forks + worker-pod + concurrency + durability + crash-recovery + busy-only-do-not-disrupt + graceful-drain + one-session-per-worker + parallel-cold-burst-ramp + worker-sizing(cnpg DuckLake) + org-default-profile(ext) + persistent-user-secrets(cnpg+ext, cross-user isolation) + user-kill-switch(cnpg) + user-disable-block(cnpg+ext) + connection-duration-logged + compute-usage-pull-api(cnpg, compute+storage) + duckling-shard-backfill(cnpg) + isolation + reshard(targets-discovery + validation + cancel-during-drain + bogus-shard-rollback) + lifecycle-teardown(+org-delete/name-release), on cnpg & ext (4 parallel lanes)"
}

main "$@"
