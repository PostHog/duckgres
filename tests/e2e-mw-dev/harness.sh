#!/bin/sh
# In-cluster e2e harness. Runs as a Job in the per-PR namespace, talking to the
# control-plane ClusterIP service over the PG wire protocol and to the in-cluster
# Kubernetes API (via the Job's own ServiceAccount token) for pod-level checks.
#
# This is the SUCCESSOR to the kind-based tests/k8s/ Go suite: every behavior
# that suite asserted against a fake kind cluster is re-asserted here against the
# REAL posthog-mw-dev cluster (real Cilium, real Crossplane ducklings, real
# cnpg-shard + external-RDS metadata, real per-org Lakekeeper). Covers, per the
# two real metadata backends cnpg + ext (aurora is retired — out of scope):
#
#   wire/query   : SELECT 1 round-trips, N concurrent connections stay distinct,
#                  and a cold-pool burst gets the graceful warm-pool backpressure
#                  hint ("retry in ~45s") then recovers.
#   activation   : DuckLake + Iceberg catalogs attach and read/write.
#   ext forks    : the bundled ducklake/httpfs extensions are the PostHog forks,
#                  not upstream (detects an accidental upstream swap in the image).
#   worker pods  : labels, securityContext (non-root, no priv-esc), Downward-API
#                  POD_NAME/NODE_NAME env, and NO ambient SA-token mount.
#   resilience   : worker-pod kill → crash recovery; DuckLake durability across a
#                  worker restart; concurrent writers (fork conflict-retry);
#                  graceful drain (in-flight query survives a worker SIGTERM, #690);
#                  one session per worker (concurrent queries land on distinct pods).
#   isolation    : two tenants see distinct catalogs (cross-tenant read denied).
#   lifecycle    : deprovision → warehouse deleted → Duckling CR fully gone
#                  (finalizer cascade that drops the cnpg role+db completed).
#                  Same-id re-provision is covered across runs by run.sh, not
#                  in-Job — see lifecycle_teardown_cnpg for why.
#
# Exit non-zero on any failure → the Job fails → the workflow step fails.
#
# Env (from run.sh): NAMESPACE, PR_NUMBER, INTERNAL_SECRET, CP_API, CP_PG_HOST
set -eu

API="${CP_API:?}"
PGHOST="${CP_PG_HOST:?}"
SECRET="${INTERNAL_SECRET:?}"
NS="${NAMESPACE:?}"
H="X-Duckgres-Internal-Secret: $SECRET"
CNPG="ci-pr-${PR_NUMBER}-cnpg"
EXT="ci-pr-${PR_NUMBER}-ext"

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
# stderr, NOT stdout: log() is called inside functions whose stdout is captured
# in $(...) and piped to jq (e.g. provision | jq -r .password). A log line on
# stdout would land in that capture and make jq choke ("Invalid numeric literal").
log()  { echo ">>> $*" >&2; }

apk add --no-cache curl jq postgresql-client >/dev/null 2>&1 || true

# kubectl, for the pod-level assertions the Go suite used to make via client-go.
# The Job runs as the `duckgres` SA (pods get/list/delete/patch + pods/exec +
# pods/log in-namespace, and ducklings get/list/watch cross-namespace), and
# kubectl auto-detects in-cluster config from that mounted SA token. arm64:
# mw-dev worker nodes are arm64.
KUBECTL=/tmp/kubectl
bootstrap_kubectl() {
  [ -x "$KUBECTL" ] && return 0
  kver="$(curl -fsSL https://dl.k8s.io/release/stable.txt 2>/dev/null || echo v1.30.5)"
  curl -fsSLo "$KUBECTL" "https://dl.k8s.io/release/${kver}/bin/linux/arm64/kubectl"
  chmod +x "$KUBECTL"
}
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

# Warm-pool backpressure ("no warm Duckgres worker … retry in about 45 seconds")
# is a FEATURE, not an error: with shared_warm_target=0 the per-org pool is cold,
# so ANY new-session acquisition — a fresh catalog (ducklake→iceberg), a burst,
# or the first connect after the pool churned (worker kills / idle timeout) — can
# transiently get it while the CP spawns a worker. It is a FATAL at session
# create, BEFORE any SQL runs, so retrying the whole command is safe (no
# half-applied INSERT). So every harness query tolerates it via bounded retry.
# Auth failures and real SQL errors are NOT retried — they surface immediately,
# so this never feeds the rate limiter or masks a genuine failure. (The
# backpressure *contract itself* is asserted separately in
# warm_capacity_backpressure, which uses raw psql to observe the hint.)
_pg_exec() { # org password dbname sql  -> prints output; rc 0 ok / 1 real error
  a=0 out=""
  while [ "$a" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc "$4" 2>&1)"; then
      printf %s "$out"; return 0
    fi
    case "$out" in
      *"capacity exhausted"*|*"no warm Duckgres worker"*|*"no warm worker"*|\
      *"still provisioning"*|*"failed to initialize session"*)
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
pg_try() { # org password dbname sql
  a=0 out=""
  while [ "$a" -lt 12 ]; do
    if out="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=$3" \
        -v ON_ERROR_STOP=1 -tAc "$4" 2>&1)"; then
      printf %s "$out"; return 0
    fi
    case "$out" in
      *"capacity exhausted"*|*"no warm Duckgres worker"*|*"no warm worker"*|\
      *"still provisioning"*|*"failed to initialize session"*)
        sleep 10; a=$((a + 1)); continue ;;
      *) printf %s "$out"; return 1 ;;
    esac
  done
  printf %s "$out"; return 1
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
  fail "no warm worker for $1/$3 after retries"
}

# ---- wire protocol --------------------------------------------------------
basic_query() { # org password
  log "basic query on $1"
  n="$(pg "$1" "$2" ducklake 'SELECT 1')"
  [ "$n" = "1" ] || fail "$1 SELECT 1 returned '$n'"
}

# Warm-pool backpressure is a FEATURE: when a burst of sessions outruns the
# cold worker pool (shared_warm_target=0), the CP rejects the surplus with a
# graceful, client-visible "no warm Duckgres worker is currently available;
# retry in about 45 seconds" rather than hanging, 500-ing, or dropping the
# connection. Assert both halves of the contract: (1) under a cold-pool burst at
# least one connection receives that exact graceful hint, and (2) the pool then
# drains so a (retrying) connection succeeds. Run this BEFORE the heavier
# concurrency tests, while only one worker is warm, so the burst reliably
# exceeds instantaneous spawn capacity.
warm_capacity_backpressure() { # org password
  log "warm-pool backpressure contract on $1"
  burst=12; seen=/tmp/bp_seen; rm -f "$seen"; pids=""
  i=0
  while [ "$i" -lt "$burst" ]; do
    ( o="$(PGPASSWORD="$2" psql \
        "sslmode=require host=$1$SNI_SUFFIX hostaddr=$CP_IP port=5432 user=root dbname=ducklake" \
        -tAc 'SELECT 1' 2>&1 || true)"
      case "$o" in
        *"no warm Duckgres worker"*|*"retry in about"*|*"capacity exhausted"*) echo x >> "$seen" ;;
      esac ) &
    pids="$pids $!"; i=$((i + 1))
  done
  for p in $pids; do wait "$p" || true; done
  [ -s "$seen" ] || fail "expected graceful 'retry in ~45s' backpressure under a cold-pool burst of $burst, but no connection saw it"
  log "backpressure observed: $(wc -l < "$seen" | tr -d ' ')/$burst connections got the graceful retry hint"
  # The pool must recover: a retrying connection succeeds.
  v="$(pgc "$1" "$2" ducklake 'SELECT 1')"
  [ "$v" = "1" ] || fail "pool did not recover after backpressure (got '$v')"
}

# N concurrent connections each run a distinct query and must each see their own
# value (no cross-talk / session bleed). Uses pgc so the cold-pool backpressure
# (asserted separately in warm_capacity_backpressure) is handled, not fatal.
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

# ---- worker pod assertions (via the K8s API) ------------------------------
newest_worker() {
  k get pods -l app=duckgres-worker \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null
}

assert_worker_pod() {
  log "worker pod labels / securityContext / downward-env / SA-token"
  pod="$(newest_worker)"; [ -n "$pod" ] || fail "no worker pod found"

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
}

# ---- resilience -----------------------------------------------------------
# Worker pod killed mid-life → CP refills and a fresh query succeeds.
# Ported from TestK8sWorkerCrashRecovery.
crash_recovery() { # org password
  log "crash recovery on $1"
  pod="$(newest_worker)"; [ -n "$pod" ] || fail "no worker pod to kill"
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
  # range() is lazy and count over it is metadata-fast, so the modulo filter
  # forces real per-row work; 8e9 rows reliably outlast the few seconds before
  # the delete. Expected = count of even i in [0, 8e9) = 4e9. The `if` keeps the
  # query's failure from tripping `set -e` inside the subshell before we record rc.
  ( if pg_try "$1" "$2" ducklake \
        "SELECT count(*) FROM range(8000000000) t(i) WHERE i % 2 = 0;" >"$out" 2>&1
    then echo 0 >"$rc"; else echo 1 >"$rc"; fi ) &
  qpid=$!

  # Let the query land on and start running on the worker, then identify the pod
  # serving it and gracefully delete it (SIGTERM → drain; the pod's
  # terminationGracePeriodSeconds=3600 keeps it alive long enough to finish).
  sleep 6
  pod="$(newest_worker)"
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
  [ "$n" = "4000000000" ] \
    || fail "graceful_drain: in-flight result=$n want 4000000000 (drain corrupted the query)"
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
# Assumes the worker nodepool is already warm (prior resilience steps spawned
# pods), so the second pod schedules within the queries' runtime.
one_session_per_worker() { # org password
  log "one session per worker: concurrent queries land on distinct pods on $1"
  o1="$(mktemp)"; o2="$(mktemp)"; r1="$(mktemp)"; r2="$(mktemp)"
  # Deterministic, multi-second query (range() is lazy; the modulo filter forces
  # real per-row work). Expected = count of even i in [0, 8e9) = 4e9.
  q="SELECT count(*) FROM range(8000000000) t(i) WHERE i % 2 = 0;"
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
  { [ "$n1" = "4000000000" ] && [ "$n2" = "4000000000" ]; } \
    || fail "one_session_per_worker: wrong results n1=$n1 n2=$n2 want 4000000000"
  [ "$peak" -ge 2 ] \
    || fail "one_session_per_worker: org peaked at $peak worker pod(s) for 2 concurrent queries — sessions shared a worker"
  rm -f "$o1" "$o2" "$r1" "$r2"
  log "one session per worker: OK (peak $peak pods for $1)"
}

# Data committed to DuckLake survives a worker restart (parquet in object store +
# metadata snapshot in Postgres are the source of truth).
# Ported from TestK8sDuckLakeDurabilityAcrossWorkerRestart.
durability_across_restart() { # org password
  log "DuckLake durability across worker restart on $1"
  t="e2e_dur_$(echo "$1" | tr -c 'a-z0-9' _)"
  pg "$1" "$2" ducklake "CREATE OR REPLACE TABLE $t AS SELECT i AS id FROM generate_series(1,200) t(i);"
  pod="$(newest_worker)"; [ -n "$pod" ] || fail "no worker pod serving the write"
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

main() {
  bootstrap_kubectl
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

  # ---- cnpg backend (full coverage incl. pod-level + resilience) ----
  wait_worker "$CNPG" "$cnpg_pw" ducklake
  basic_query            "$CNPG" "$cnpg_pw"
  warm_capacity_backpressure "$CNPG" "$cnpg_pw"   # while only one worker is warm
  rw_ducklake            "$CNPG" "$cnpg_pw"
  assert_fork_extensions "$CNPG" "$cnpg_pw"   # after a DuckLake R/W (httpfs loaded)
  rw_iceberg             "$CNPG" "$cnpg_pw"
  assert_worker_pod
  concurrent_connections "$CNPG" "$cnpg_pw"
  concurrent_writers     "$CNPG" "$cnpg_pw"
  durability_across_restart "$CNPG" "$cnpg_pw"
  crash_recovery         "$CNPG" "$cnpg_pw"
  graceful_drain         "$CNPG" "$cnpg_pw"
  one_session_per_worker "$CNPG" "$cnpg_pw"

  # ---- ext backend (activation + R/W on the external-RDS metadata path) ----
  wait_worker "$EXT" "$ext_pw" ducklake
  basic_query  "$EXT" "$ext_pw"
  rw_ducklake  "$EXT" "$ext_pw"
  rw_iceberg   "$EXT" "$ext_pw"

  # ---- cross-tenant isolation (cnpg vs ext) ----
  tenant_isolation "$CNPG" "$cnpg_pw" "$EXT" "$ext_pw"

  # ---- lifecycle: deprovision cnpg + assert the Duckling CR fully deletes ----
  lifecycle_teardown_cnpg "$CNPG"

  # NOTE: warm-pool-specific behaviors (shared-warm-worker activation, and the
  # version-mismatch idle-worker reaper) are NOT exercised here: the per-PR CP
  # runs DUCKGRES_K8S_SHARED_WARM_TARGET=0, so there are no idle warm workers to
  # assert on. They stay covered by the controlplane/ unit tests; running them
  # end-to-end would need a warm target >0 in the per-PR CP (see README).
  log "SKIP shared-warm-activation + version-reaper (CP runs warm-target=0; see README)"

  log "PASS: wire + warm-pool-backpressure + activation(DuckLake/Iceberg) + ext-forks + worker-pod + concurrency + durability + crash-recovery + graceful-drain + one-session-per-worker + isolation + lifecycle-teardown, on cnpg & ext"
}

main "$@"
