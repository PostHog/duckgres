#!/usr/bin/env bash
# Probe a worker pod's network egress and report which destinations are
# reachable. Intended to be run before and after a Cilium NetworkPolicy is
# applied — the table output is identical-shape so a diff highlights exactly
# which targets the policy newly blocks.
#
# Mechanism: each probe runs as its own `kubectl debug` ephemeral container
# attached to the worker pod's network namespace. Cilium endpoints cover all
# containers in a pod, so the ephemeral container inherits whatever policy
# applies to the worker. One probe per debug invocation pays ~3 s startup
# each (image cached after first call) but keeps the bash quoting trivial —
# a previous one-shot inline approach silently dropped output past ~3 probes
# under ephemeral-container streaming buffers.
#
# Usage:
#   WORKER_POD=duckgres-…-worker-7664 \
#   TENANT_RDS_HOST=tenant.cluster-….rds.amazonaws.com \
#   TENANT_BUCKET=posthog-tenant-bucket \
#   OTHER_RDS_HOST=posthog-duckgres-config-store-mw-dev.cluster-….rds.amazonaws.com \
#   OTHER_WORKER_POD=duckgres-…-worker-7665 \
#   ./scripts/probe_worker_egress.sh
set -euo pipefail

WORKER_POD="${WORKER_POD:?WORKER_POD env var required}"
NAMESPACE="${NAMESPACE:-duckgres}"
TENANT_RDS_HOST="${TENANT_RDS_HOST:-}"
TENANT_BUCKET="${TENANT_BUCKET:-}"
OTHER_RDS_HOST="${OTHER_RDS_HOST:-}"
OTHER_WORKER_POD="${OTHER_WORKER_POD:-}"

NODE_IP="$(kubectl -n "$NAMESPACE" get pod "$WORKER_POD" -o jsonpath='{.status.hostIP}')"
WORKER_NODE="$(kubectl -n "$NAMESPACE" get pod "$WORKER_POD" -o jsonpath='{.spec.nodeName}')"
# The cache proxy is a per-node DaemonSet pod (not hostNetwork) — find the
# instance running on the same node as the worker so we test the actual
# path the worker would take.
CACHE_PROXY_IP="$(kubectl -n "$NAMESPACE" get pod \
  -l app.kubernetes.io/name=duckgres-cache-proxy \
  --field-selector="spec.nodeName=$WORKER_NODE" \
  -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)"
CACHE_PROXY_PORT=8080  # S3 forward proxy port; 8081 is peer↔peer, 8082 is health
APISERVER_IP="$(kubectl get svc kubernetes -n default -o jsonpath='{.spec.clusterIP}')"
KUBE_DNS_IP="$(kubectl get svc kube-dns -n kube-system -o jsonpath='{.spec.clusterIP}')"
S3_REGION="${S3_REGION:-us-east-1}"
S3_HOST="s3.${S3_REGION}.amazonaws.com"

OTHER_WORKER_IP=""
if [[ -n "$OTHER_WORKER_POD" ]]; then
  OTHER_WORKER_IP="$(kubectl -n "$NAMESPACE" get pod "$OTHER_WORKER_POD" -o jsonpath='{.status.podIP}')"
fi

echo "Probing worker: $WORKER_POD (node $NODE_IP)"
echo "Cache proxy on node: ${CACHE_PROXY_IP:-MISSING}:$CACHE_PROXY_PORT"
echo

# probe runs one command inside an ephemeral container sharing $WORKER_POD's
# netns. Echoes one tab-separated row: KIND TARGET EXPECTED RESULT VERDICT DETAIL.
probe() {
  local kind="$1" target="$2" expected="$3" cmd="$4"
  local out ec result verdict detail
  # kubectl debug --attach=true does NOT propagate the inner shell's exit
  # code (always exits 0 once the ephemeral container is attached). Embed
  # the inner exit code in the output as a sentinel and parse it back.
  raw=$(kubectl -n "$NAMESPACE" debug "$WORKER_POD" \
    --image=nicolaka/netshoot \
    --target=duckdb-worker \
    --image-pull-policy=IfNotPresent \
    --profile=general \
    -q --attach=true \
    -- sh -c "$cmd; echo __PROBE_EXIT=\$?" 2>&1) || true
  ec=$(printf '%s' "$raw" | grep -oE '__PROBE_EXIT=[0-9]+' | tail -1 | cut -d= -f2)
  ec="${ec:-1}"
  out=$(printf '%s' "$raw" | grep -v "consider using" | grep -v "deprecated and will be removed" | grep -v "__PROBE_EXIT=" || true)
  detail=$(printf '%s' "$out" | tr '\t\n' '  ' | tail -c 100)
  # Tools we use (nc, curl with --max-time, dig +tries=1) all exit non-zero
  # when the network path is denied/unreachable, so the exit code alone is
  # the reachable/blocked signal.
  if [[ "$ec" == "0" ]]; then
    result=reachable
  else
    result=blocked
  fi
  if [[ "$expected" == "allow" && "$result" == "reachable" ]] || \
     [[ "$expected" == "block" && "$result" == "blocked"  ]]; then
    verdict=PASS
  else
    verdict=FAIL
  fi
  printf "%-7s %-32s %-8s %-9s %-8s %s\n" \
    "$kind" "$target" "$expected" "$result" "$verdict" "$detail"
}

printf "%-7s %-32s %-8s %-9s %-8s %s\n" KIND TARGET EXPECTED RESULT VERDICT DETAIL
printf "%-7s %-32s %-8s %-9s %-8s %s\n" ------- -------------------------------- -------- --------- -------- ------------------------------

if [[ -n "$CACHE_PROXY_IP" ]]; then
  probe TCP "cache-proxy (node-local)"     allow "nc -zv -w 3 $CACHE_PROXY_IP $CACHE_PROXY_PORT"
fi
probe DNS   "kube-dns resolution"          allow "dig +time=2 +tries=1 +short @${KUBE_DNS_IP} kubernetes.default.svc.cluster.local"
probe HTTPS "S3 region endpoint"           allow "curl -sS -o /dev/null -w %{http_code} --max-time 5 https://$S3_HOST/"
probe HTTPS "public internet (example.com)" allow "curl -sS -o /dev/null -w %{http_code} --max-time 5 https://example.com/"
# Port-scope regression checks: world egress is allowlisted to TCP 443 +
# 5432 only, so any other port to a public host must stay blocked. If
# either of these flips to reachable in a future probe run, somebody
# widened the world rule and we want to catch it. Targets are chosen so
# the destination port is genuinely listening pre-policy (otherwise the
# "block" outcome would be a false positive caused by the host refusing
# the connection rather than Cilium): example.com:80 is served by
# Cloudflare's HTTP redirector, github.com:22 is GitHub's SSH endpoint.
probe TCP   "public HTTP example.com:80"   block "nc -zv -w 3 example.com 80"
probe TCP   "public SSH github.com:22"     block "nc -zv -w 3 github.com 22"
probe TCP   "EC2 IMDS (169.254.169.254)"   block "nc -zv -w 3 169.254.169.254 80"
probe HTTP  "EC2 IMDS"                     block "curl -sS -o /dev/null -w %{http_code} --max-time 3 http://169.254.169.254/latest/meta-data/"
probe TCP   "kube-apiserver"               block "nc -zv -w 3 $APISERVER_IP 443"

if [[ -n "$TENANT_RDS_HOST" ]]; then
  probe TCP "tenant RDS"                   allow "nc -zv -w 3 $TENANT_RDS_HOST 5432"
fi
if [[ -n "$TENANT_BUCKET" ]]; then
  probe HTTPS "tenant bucket"              allow "curl -sS -o /dev/null -w %{http_code} --max-time 5 https://${TENANT_BUCKET}.s3.${S3_REGION}.amazonaws.com/"
fi
if [[ -n "$OTHER_RDS_HOST" ]]; then
  # Documented trade-off: this policy does not scope RDS hostnames per
  # tenant, so any RDS in the VPC remains reachable at the network layer
  # (AWS-credential layers gate actual data access). Expected `allow`.
  probe TCP "other tenant RDS (world)"     allow "nc -zv -w 3 $OTHER_RDS_HOST 5432"
fi
if [[ -n "$OTHER_WORKER_IP" ]]; then
  probe TCP "other worker (Flight)"        block "nc -zv -w 3 $OTHER_WORKER_IP 8816"
fi
