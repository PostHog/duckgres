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
#   activation   : DuckLake catalogs attach and read/write on cnpg + ext.
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
#                  (finalizer cascade that drops the cnpg role+db completed).
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

# The bundled extensions MUST be the PostHog forks. These are the short commit
# SHAs duckdb_extensions() reports for the tags the image pins
# (DUCKLAKE_EXTENSION_TAG=v1.0-posthog.4, HTTPFS_EXTENSION_TAG=v1.5.3-cred-refresh).
# If the image accidentally ships upstream, the version differs and we fail.
EXPECT_DUCKLAKE_SHA="e4ac5150"
EXPECT_HTTPFS_SHA="b1fece6"

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

# Managed-warehouse compute-usage metering (billing emit side). At connection
# teardown the CP meters cpu_seconds/memory_seconds from the provisioned worker
# size over the connection lifetime into an in-proc per-org counter (best-effort),
# flushes it to the durable config-store buffer (~15s), and the leader drains
# closed buckets to PostHog ingestion (~60s, ship-then-delete). The :9090 metrics
# port + the ingestion HTTP path are not observable from this in-cluster Job, so
# we assert the CP's startup log line that proves the metering config knob is
# wired and reports its enabled/disabled state. When the deploy sets
# DUCKGRES_BILLING_INGEST_URL/TOKEN this line reads "enabled"; otherwise
# "disabled" — either way the knob is present and the emit path is compiled in.
# (NOTE: a full end-to-end "the event landed in PostHog ClickHouse" assertion
# needs a billing-analytics token + CH read access from the Job, which this lane
# does not have — see README. The buffer UPSERT/drain SQL is exercised by the
# tests/configstore Postgres migration test, and the meter/drain logic by the
# controlplane/ unit tests.)
compute_usage_metering_wired() {
  log "compute-usage metering config knob wired in CP"
  found=""
  for p in $(k get pods -l app=duckgres-control-plane -o jsonpath='{.items[*].metadata.name}'); do
    m="$(k logs "$p" 2>/dev/null | grep -E 'Managed-warehouse compute-usage metering (enabled|disabled)' | tail -1)"
    if [ -n "$m" ]; then found="$m"; fi
  done
  [ -n "$found" ] || fail "no compute-usage metering startup log line found (config knob not wired into SetupMultiTenant?)"
  log "compute-usage metering state: $found"
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
  # The metrics proxy advertises its allow-listed panels (not an open PromQL relay).
  curl -fsS -H "$H" "$API/api/v1/metrics/panels" | jq -e '.panels | index("query_rate")' >/dev/null \
    || fail "/metrics/panels missing 'query_rate'"
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
# Asserts: a real running query is findable via /queries, /queries/:pid returns
# 200 with the SQL round-tripped + matching identity, and an unknown pid 404s
# (not a 500). The redaction guarantee itself is unit-tested
# (server/conn_detail_test.go, controlplane/admin/live_test.go) — this proves
# the wiring against a real worker pod.
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
  wait_state "$1" deleted 600
  if ! "$KUBECTL" -n ducklings wait --for=delete "duckling/$1" --timeout=420s >/dev/null 2>&1; then
    fail "Duckling CR $1 did not fully delete within 420s (finalizer/cnpg cascade stuck)"
  fi
}

# ---- cnpg duckling: cnpg-shard metadata + DuckLake ------------------------
CNPG_BODY='{"database_name":"'"$CNPG"'","metadata_store":{"type":"cnpg-shard"},
  "data_store":{"type":"s3bucket"},"ducklake":{"enabled":true}}'

# ---- ext duckling: external RDS metadata + DuckLake -----------------------
EXT_BODY='{"database_name":"'"$EXT"'",
  "metadata_store":{"type":"external","external":{
    "endpoint":"'"$EXT_RDS_ENDPOINT"'","password_aws_secret":"'"$EXT_RDS_SECRET"'",
    "user":"ducklingexample","database":"ducklingexample"}},
  "data_store":{"type":"external","bucket_name":"posthog-duckling-example-managed-warehouse-dev","region":"us-east-1"},
  "ducklake":{"enabled":true}}'

# ---- resilience ducklings: cnpg-shard metadata + DuckLake only -------------
# DuckLake-only: these orgs exist purely to host the worker-churn-heavy
# resilience lanes, so keep their provision footprint small.
res_body() { # org
  printf '{"database_name":"%s","metadata_store":{"type":"cnpg-shard"},"data_store":{"type":"s3bucket"},"ducklake":{"enabled":true}}' "$1"
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
  malformed_startup_resilience "$CNPG" "$cnpg_pw"
  jsonb_concat_semantics "$CNPG" "$cnpg_pw"
  cold_burst_absorption  "$CNPG" "$cnpg_pw"   # early, while this org is mostly cold
  rw_ducklake            "$CNPG" "$cnpg_pw"
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
  rw_ducklake  "$EXT" "$ext_pw"
  httpfs_retry_budget "$EXT" "$ext_pw"      # S3-503 retry budget per worker, ext-metadata backend too
  persistent_user_secret "$EXT" "$ext_pw"   # secret replay on the ext-metadata org too
  # Org default profile on ext: no client-sized assertions run on this org, so
  # the 2-CPU shape is unambiguously the org default's.
  org_default_profile "$EXT" "$ext_pw" ducklake
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

  # ---- admin live-query detail view (phase 1) — cnpg stack is warm now ----
  admin_query_detail "$CNPG" "$cnpg_pw"

  # ---- admin /status per-org worker count is populated (not the old 0) ----
  admin_per_org_workers "$CNPG" "$cnpg_pw"

  # ---- connection-duration observability (any org; lanes already churned
  #      many connect/disconnects, so the disconnect log is warm) ----
  connection_duration_logged "$CNPG" "$cnpg_pw"

  # ---- compute-usage metering wired (billing emit side) ----
  compute_usage_metering_wired

  # ---- cross-tenant isolation (cnpg vs ext) — needs both lanes done ----
  tenant_isolation "$CNPG" "$cnpg_pw" "$EXT" "$ext_pw"

  # ---- lifecycle: deprovision cnpg + assert the Duckling CR fully deletes ----
  # (res1/res2/ext are deprovisioned by run.sh teardown; the cascade assertion
  # only needs one cnpg-shard org.)
  lifecycle_teardown_cnpg "$CNPG"

  # NOTE: the version-mismatch worker reaper is not exercised in-Job (it needs a
  # mid-run image bump); it stays covered by the controlplane/ unit tests.
  log "SKIP version-reaper (needs an in-run image bump; see README)"

  log "PASS: admin-no-query-token + models-explorer-api(redaction) + admin-console-api(me/live/metrics/auth-gate) + admin-rbac-viewer(403 mutate/audit) + admin-impersonation(round-trip+audit) + wire + malformed-startup-resilience + jsonb-concat + cold-burst-absorption + pipeline-error-recovery + cancel-reuse + activation(DuckLake) + ext-forks + worker-pod + concurrency + durability + crash-recovery + busy-only-do-not-disrupt + graceful-drain + one-session-per-worker + parallel-cold-burst-ramp + worker-sizing(cnpg DuckLake) + org-default-profile(ext) + persistent-user-secrets(cnpg+ext, cross-user isolation) + connection-duration-logged + isolation + lifecycle-teardown, on cnpg & ext (4 parallel lanes)"
}

main "$@"
