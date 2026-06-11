#!/bin/sh
# In-cluster e2e harness. Runs as a Job in the per-PR namespace, talking to the
# control-plane ClusterIP service over the PG wire protocol and to the in-cluster
# Kubernetes API (via the Job's own ServiceAccount token) for pod-level checks.
#
# This is the SUCCESSOR to the kind-based tests/k8s/ Go suite: every behavior
# that suite asserted against a fake kind cluster is re-asserted here against the
# REAL posthog-mw-dev cluster (real Cilium, real Crossplane ducklings, real
# cnpg-shard + external-RDS metadata, real per-org Lakekeeper). Covers the two
# real metadata backends cnpg + ext (aurora is retired — out of scope).
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
#   activation   : DuckLake + Iceberg catalogs attach and read/write. The FIRST
#                  session on a cold Iceberg worker must not fail the pg_catalog
#                  compat-view bind (the CP primes the REST catalog's schema list
#                  first); asserted on cnpg + ext.
#   sizing       : a client-sized connection (duckgres.worker_cpu/memory/ttl)
#                  spawns a worker pod carrying the requested CPU+memory, and a
#                  same-shape reconnect reuses that hot-idle worker (no respawn)
#                  — asserted on cnpg for BOTH the ducklake and iceberg catalogs.
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
#   lifecycle    : deprovision → warehouse deleted → Duckling CR fully gone
#                  (finalizer cascade that drops the cnpg role+db completed).
#                  Same-id re-provision is covered across runs by run.sh, not
#                  in-Job — see lifecycle_teardown_cnpg for why.
#
# Exit non-zero on any failure → the Job fails → the workflow step fails.
#
# Env (from run.sh): NAMESPACE, PR_NUMBER, INTERNAL_SECRET, CP_API, CP_PG_HOST
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

# The bundled extensions MUST be the PostHog forks. These are the short commit
# SHAs duckdb_extensions() reports for the tags the image pins
# (DUCKLAKE_EXTENSION_TAG=v1.0-posthog.4, HTTPFS_EXTENSION_TAG=v1.5.3-stoi-fix).
# If the image accidentally ships upstream, the version differs and we fail.
EXPECT_DUCKLAKE_SHA="e4ac5150"
EXPECT_HTTPFS_SHA="c727795"

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
# the CATALOG (`ducklake` or `iceberg`), not the org (PR #651). The control
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
# ANY new-session acquisition — a fresh catalog (ducklake→iceberg), a burst, or
# the first connect after the pool churned (worker kills / idle timeout) — can
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
      *"failed to detect attached catalogs"*)
        # The last three cover an on-demand cold spawn that needed a fresh node
        # (sized worker too big for the warm node): the first connect can hit the
        # spawn ceiling while the pod is still pulling/booting; by the retry it is
        # hot-idle and the reconnect reuses it. Same transient class as a cold pool.
        # "failed to detect attached catalogs" is the session-init catalog probe
        # racing a freshly-spawned worker whose ATTACH is still settling — it fires
        # BEFORE any user SQL runs, so retrying the whole command is safe.
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

rw_iceberg() { # org password
  log "Iceberg R/W on $1"
  c="$(pg "$1" "$2" iceberg "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name='iceberg'")"
  [ "$c" = "1" ] || fail "$1 iceberg catalog not attached (duckdb_databases count=$c)"
  t="e2e_ice_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" iceberg "DROP TABLE IF EXISTS iceberg.public.$t;
            CREATE TABLE iceberg.public.$t(id INT, label VARCHAR);
            INSERT INTO iceberg.public.$t VALUES (10,'ten'),(20,'twenty');"
  n="$(pg "$1" "$2" iceberg "SELECT COUNT(*) FROM iceberg.public.$t;")"
  [ "$n" = "2" ] || fail "$1 Iceberg rowcount=$n want 2"
  pg "$1" "$2" iceberg "DROP TABLE iceberg.public.$t;"
}

# Regression for the Iceberg cold-start session-init bug: the FIRST session on a
# freshly-spawned (cold) worker must not fail the pg_catalog compat-view bind.
# Before the fix, InitSessionDatabaseMetadata enumerated every attached catalog
# before anything had materialized the Iceberg REST catalog's schema list on the
# new session connection, so the bind hit `Catalog Error: Schema with name ""
# not found` and the connection was rejected with `failed to initialize session
# database metadata`; only a reconnect (instance since settled) masked it. With
# the warm pool gone, every org's first connect lands the literal first session
# on its worker, so this fired in practice. The CP now primes the catalog with a
# retried schema-enumeration probe before the bind, and the bind itself retries
# once on the settle signature. This forces a cold worker and asserts
# the first connect succeeds: cold-spawn backpressure is still tolerated, but the
# metadata-init bind error fails the test immediately instead of being retried
# away (which is exactly how a live reconnect would mask it).
iceberg_cold_first_connect() { # org password
  log "iceberg cold first-connect (metadata-init regression) on $1"
  # Force cold: delete every worker for the org and wait until none remain, so
  # the next connect cold-spawns a brand-new worker whose first session is the
  # client's — the precise condition that triggered the bug.
  for p in $(k get pods -l "app=duckgres-worker,duckgres/active-org=$1" -o name 2>/dev/null); do
    k delete "$p" --grace-period=0 --force >/dev/null 2>&1 || true
  done
  i=0
  while [ "$i" -lt 45 ]; do
    n="$(k get pods -l "app=duckgres-worker,duckgres/active-org=$1" --no-headers 2>/dev/null | grep -c . || true)"
    [ "$n" = "0" ] && break
    sleep 2; i=$((i + 1))
  done
  # First connect. Tolerate cold-spawn backpressure (worker still booting), but
  # treat the session-metadata-init bind failure as a hard regression — do NOT
  # retry it, or the bug would be masked exactly as a live reconnect masks it.
  # The metadata-init clause is listed first so it wins over the broader
  # "failed to initialize session" transient it is a prefix-superset of.
  a=0 out=""
  while [ "$a" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=iceberg" \
        -v ON_ERROR_STOP=1 -tAc "SELECT 1" 2>&1)"; then
      [ "$out" = "1" ] || fail "iceberg_cold_first_connect: $1 returned '$out' want 1"
      return
    fi
    case "$out" in
      *"failed to initialize session database metadata"*|*'Schema with name "" not found'*)
        fail "iceberg_cold_first_connect: cold first session hit the metadata-init bind bug on $1 (prime regressed): $out" ;;
      *"capacity exhausted"*|*"no Duckgres worker"*|*"still provisioning"*|\
      *"failed to initialize session"*|*"timed out waiting for an available worker"*|\
      *"failed to start"*|*"spawn sized worker"*|*"failed to detect attached catalogs"*)
        sleep 10; a=$((a + 1)); continue ;;
      *) fail "iceberg_cold_first_connect: unexpected error on $1: $out" ;;
    esac
  done
  fail "iceberg_cold_first_connect: exhausted retries waiting for a cold worker on $1"
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

# Cross-user isolation within one org: user B must never see user A's
# persistent secret. B's session typically reuses A's hot-idle worker (one org,
# cap permitting), which is exactly the leak path: the worker wipes persistent
# secrets at session create and replays only the connecting user's.
persistent_user_secret_isolation() { # org rootpw
  org="$1"; pw="$2"; sname="e2e_root_secret"; u2="e2esecuser"
  u2pw="e2e-$(openssl rand -hex 12)"
  log "persistent secret cross-user isolation on $org"

  pg "$org" "$pw" ducklake "CREATE PERSISTENT SECRET $sname (TYPE s3, KEY_ID 'AKIAE2EDUMMY', SECRET 'root-dummy', REGION 'us-east-1', SCOPE 's3://duckgres-e2e-nonexistent-root-$org')" >/dev/null

  code="$(curl -s -o /tmp/create_user_out -w '%{http_code}' -X POST -H "$H" \
    -H 'Content-Type: application/json' \
    -d "{\"org_id\":\"$org\",\"username\":\"$u2\",\"password\":\"$u2pw\"}" \
    "$API/api/v1/users")"
  case "$code" in 200|201) ;; *) fail "persistent secret: create user $u2 -> HTTP $code: $(cat /tmp/create_user_out)";; esac
  # New-user auth is config-snapshot-driven; wait out one poll before the
  # first login so we don't burn failed auths against the rate limiter.
  sleep "${CONFIG_POLL_SETTLE:-12}"

  n="$(pg "$org" "$u2pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'" "$u2")"
  [ "$n" = "0" ] || fail "persistent secret: user $u2 sees root's secret (count=$n)"
  # Root's stored copy must be unaffected by $u2's session-create wipe.
  n="$(pg "$org" "$pw" ducklake "SELECT count(*) FROM duckdb_secrets() WHERE name = '$sname'")"
  [ "$n" = "1" ] || fail "persistent secret: root's secret lost after $u2's session (count=$n)"
  pg "$org" "$pw" ducklake "DROP PERSISTENT SECRET $sname" >/dev/null
  log "persistent secret isolation OK on $org ($u2 blind to root's secret; root's copy intact)"
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

# ---- admin dashboard auth (#721) -------------------------------------------
# Regression for #721: the admin dashboard must NOT accept the internal secret
# via a ?token= URL query parameter (URL-borne secrets persist in browser
# history and any future proxy access logs). A request carrying the correct
# token in the query string gets the login page (401) with no auth cookie and
# no redirect; the X-Duckgres-Internal-Secret header path still authenticates.
admin_dashboard_no_query_token() {
  log "admin dashboard rejects ?token= query auth"
  hdrs=/tmp/admin_qt_hdrs
  code="$(curl -s -o /dev/null -D "$hdrs" -w '%{http_code}' "$API/?token=$SECRET")"
  [ "$code" = "401" ] || fail "GET /?token=<secret> returned $code, want 401 (query-param auth must be rejected)"
  if grep -qi '^set-cookie:' "$hdrs"; then
    fail "GET /?token=<secret> set a cookie — query-param auth is back"
  fi
  # Sanity: the header path (what this harness uses everywhere) still works.
  code="$(curl -s -o /dev/null -w '%{http_code}' -H "$H" "$API/")"
  [ "$code" = "200" ] || fail "GET / with internal-secret header returned $code, want 200"
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
# cnpg lakekeeper_<org> role+db) completed.
#
# NOTE: same-org-id *re-provision* in the SAME run is intentionally NOT done
# here. It is the regression net for the stranded-cnpg-role bugs
# (#649/#650/#11518/#11522), but it cannot be made reliable from inside the Job:
# guaranteeing a clean slate requires DROPping a possibly-stranded cnpg role on
# the cnpg-shards Postgres, which only `run.sh` (on the runner, with
# cnpg-shards exec rights) can do — the in-cluster Job SA cannot and must not.
# Re-provisioning the same id while the async cnpg cascade is still in flight
# races a drifted-password role → Lakekeeper SASL failure → the warehouse never
# goes ready. So the same-id regression is covered ACROSS runs instead: every
# run's `run.sh deploy` drops the cnpg role for a clean slate, and `run.sh
# teardown` waits the CR `--for=delete` before returning. (This is the same
# reasoning the original harness used to drop the in-Job recreate.)
lifecycle_teardown_cnpg() { # org
  log "lifecycle: deprovision $1 + assert Duckling CR fully deleted"
  api_post "$1" deprovision >/dev/null
  wait_state "$1" deleted 600
  if ! "$KUBECTL" -n ducklings wait --for=delete "duckling/$1" --timeout=420s >/dev/null 2>&1; then
    fail "Duckling CR $1 did not fully delete within 420s (finalizer/cnpg cascade stuck)"
  fi
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

# ---- resilience ducklings: cnpg-shard metadata + DuckLake only -------------
# No iceberg (no per-org Lakekeeper) — these orgs exist purely to host the
# worker-churn-heavy resilience lanes, so keep their provision footprint small.
res_body() { # org
  printf '{"database_name":"%s","metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true}}' "$1"
}

# ---- parallel lane machinery ------------------------------------------------
# The assertions are grouped into per-org LANES that run concurrently: all
# worker churn (kills, drains, cold spawns, TTL reaps) is org-scoped, so lanes
# for different orgs cannot interfere — and the wall-clock becomes the slowest
# lane instead of the sum. Each lane runs in a background subshell with its
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
  malformed_startup_resilience "$CNPG" "$cnpg_pw"
  jsonb_concat_semantics "$CNPG" "$cnpg_pw"
  cold_burst_absorption  "$CNPG" "$cnpg_pw"   # early, while this org is mostly cold
  rw_ducklake            "$CNPG" "$cnpg_pw"
  persistent_user_secret "$CNPG" "$cnpg_pw"   # after rw_ducklake (org worker hot)
  persistent_user_secret_isolation "$CNPG" "$cnpg_pw"
  pipeline_error_recovery "$CNPG" "$cnpg_pw"  # after rw_ducklake (table writes proven)
  cancel_then_reuse_same_session "$CNPG" "$cnpg_pw"
  assert_fork_extensions "$CNPG" "$cnpg_pw"   # after a DuckLake R/W (httpfs loaded)
  iceberg_cold_first_connect "$CNPG" "$cnpg_pw"  # cold first session must not fail the metadata-init bind
  rw_iceberg             "$CNPG" "$cnpg_pw"
  assert_worker_pod      "$CNPG"   # newest org pod = the default-shape iceberg worker above
  concurrent_connections "$CNPG" "$cnpg_pw"
  concurrent_writers     "$CNPG" "$cnpg_pw"
  # Worker sizing, both catalogs. Distinct shapes per catalog (2/4Gi vs 1/2Gi)
  # so the iceberg sized request can't reuse the ducklake hot-idle 2-CPU worker
  # — each catalog gets a verified fresh sized spawn + a same-shape reuse.
  sized_worker        "$CNPG" "$cnpg_pw" ducklake 2 4Gi 15m
  reuse_sized_worker  "$CNPG" "$cnpg_pw" ducklake 2 4Gi 15m
  sized_worker        "$CNPG" "$cnpg_pw" iceberg  1 2Gi 15m
  reuse_sized_worker  "$CNPG" "$cnpg_pw" iceberg  1 2Gi 15m
}

lane_res1() { # worker-kill resilience on its own org (heavy, churns workers)
  wait_worker "$RES1" "$res1_pw" ducklake
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
  rw_ducklake  "$EXT" "$ext_pw"
  persistent_user_secret "$EXT" "$ext_pw"   # secret replay on the ext-metadata org too
  iceberg_cold_first_connect "$EXT" "$ext_pw"  # cold first session must not fail the metadata-init bind (ext backend)
  rw_iceberg   "$EXT" "$ext_pw"
  # Org default profile on ext: no client-sized assertions run on this org, so
  # the 2-CPU shape is unambiguously the org default's.
  org_default_profile "$EXT" "$ext_pw" ducklake
}

main() {
  resolve_cp_ip

  # No org dependency — assert the admin surface auth contract up front.
  admin_dashboard_no_query_token

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
  run_lane ready_cnpg wait_state "$CNPG" ready 600
  run_lane ready_ext  wait_state "$EXT"  ready 600
  run_lane ready_res1 wait_state "$RES1" ready 600
  run_lane ready_res2 wait_state "$RES2" ready 600
  join_lanes ready_cnpg ready_ext ready_res1 ready_res2

  # Settle: let the CP's config-store poll pick up the provisioned orgs/users
  # before the first connection, so we don't burn failed-auth attempts against
  # the rate limiter while the auth cache catches up. The CI control plane polls
  # every 5s (DUCKGRES_CONFIG_POLL_INTERVAL in manifests.tmpl.yaml), so 12s
  # covers two full poll cycles; CONFIG_POLL_SETTLE overrides if that drifts.
  log "settling ${CONFIG_POLL_SETTLE:-12}s for CP auth cache…"
  sleep "${CONFIG_POLL_SETTLE:-12}"

  # Join the kubectl background download started at script load — first k use
  # is inside the lanes, so the fetch overlapped the whole provisioning phase.
  bootstrap_kubectl

  # ---- the four parallel assertion lanes (see lane_* above) ----
  run_lane cnpg lane_cnpg
  run_lane res1 lane_res1
  run_lane res2 lane_res2
  run_lane ext  lane_ext
  join_lanes cnpg res1 res2 ext

  # ---- cross-tenant isolation (cnpg vs ext) — needs both lanes done ----
  tenant_isolation "$CNPG" "$cnpg_pw" "$EXT" "$ext_pw"

  # ---- lifecycle: deprovision cnpg + assert the Duckling CR fully deletes ----
  # (res1/res2/ext are deprovisioned by run.sh teardown; the cascade assertion
  # only needs one cnpg-shard org.)
  lifecycle_teardown_cnpg "$CNPG"

  # NOTE: the version-mismatch worker reaper is not exercised in-Job (it needs a
  # mid-run image bump); it stays covered by the controlplane/ unit tests.
  log "SKIP version-reaper (needs an in-run image bump; see README)"

  log "PASS: admin-no-query-token + wire + malformed-startup-resilience + jsonb-concat + cold-burst-absorption + pipeline-error-recovery + cancel-reuse + activation(DuckLake/Iceberg) + iceberg-cold-first-connect + ext-forks + worker-pod + concurrency + durability + crash-recovery + graceful-drain + one-session-per-worker + parallel-cold-burst-ramp + worker-sizing(cnpg DuckLake+Iceberg) + org-default-profile(ext) + persistent-user-secrets(cnpg+ext, cross-user isolation) + isolation + lifecycle-teardown, on cnpg & ext (4 parallel lanes)"
}

main "$@"
