#!/usr/bin/env bash
# Orchestrate the per-PR mw-dev e2e stack from the CI runner (kubectl reaches
# the private API via Tailscale). Subcommands: deploy | test | diagnostics |
# teardown. All kubectl calls pin --context explicitly — never rely on the
# current-context default.
#
# Required env (set by .github/workflows/e2e-mw-dev.yml):
#   NAMESPACE, PR_NUMBER, WORKER_IMAGE, CONTROLPLANE_IMAGE, KUBE_CONTEXT,
#   CP_POD_IDENTITY_ROLE (the duckgres-control-plane-dev role ARN — the per-PR
#   CP assumes the SAME EKS Pod Identity as the real mw-dev control plane, so
#   the real STS-brokered S3 activation path works without any cred injection),
#   EKS_CLUSTER_NAME, AWS_REGION.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="${KUBE_CONTEXT:?}"
# NAMESPACE is required by deploy|test|diagnostics|teardown but NOT by
# e2e-cleanup (which discovers stale namespaces itself). Don't require it here.
NS="${NAMESPACE:-}"
KUBECTL=(kubectl --context "$CTX")
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-posthog-mw-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
SA_NAME="duckgres"

# Internal secret for the per-PR control plane. Random per run; never reused.
# Stamped into the rendered manifests and handed to the in-cluster harness.
internal_secret_file="/tmp/duckgres-ci-internal-secret"
# Rotation fallback secret (DUCKGRES_INTERNAL_SECRET_FALLBACKS): a second
# random value the CP must also accept, so the harness can assert the
# rotation-overlap path (internal_secret_fallback_auth).
internal_secret_fallback_file="/tmp/duckgres-ci-internal-secret-fallback"
# AES key for user persistent secrets (DUCKGRES_USER_SECRET_KEY). Random per
# run: stored user secrets only need to outlive the run's sessions.
user_secret_key_file="/tmp/duckgres-ci-user-secret-key"

render() {
  : "${WORKER_IMAGE:?}" "${CONTROLPLANE_IMAGE:?}" "${PR_NUMBER:?}"
  [ -f "$internal_secret_file" ] || openssl rand -hex 16 > "$internal_secret_file"
  [ -f "$internal_secret_fallback_file" ] || openssl rand -hex 16 > "$internal_secret_fallback_file"
  [ -f "$user_secret_key_file" ] || openssl rand -base64 32 > "$user_secret_key_file"
  INTERNAL_SECRET="$(cat "$internal_secret_file")" \
  INTERNAL_SECRET_FALLBACK="$(cat "$internal_secret_fallback_file")" \
  USER_SECRET_KEY="$(cat "$user_secret_key_file")" \
  NAMESPACE="$NS" PR_NUMBER="$PR_NUMBER" \
  WORKER_IMAGE="$WORKER_IMAGE" CONTROLPLANE_IMAGE="$CONTROLPLANE_IMAGE" \
    envsubst '$NAMESPACE $PR_NUMBER $WORKER_IMAGE $CONTROLPLANE_IMAGE $INTERNAL_SECRET $INTERNAL_SECRET_FALLBACK $USER_SECRET_KEY' \
    < "$HERE/manifests.tmpl.yaml"
}

# Bind this namespace's `duckgres` SA to the same IAM role the real mw-dev
# control plane uses (EKS Pod Identity). With it, the CP brokers per-duckling
# S3 STS creds exactly like prod and activation completes — without it, DuckLake/
# Iceberg activation fails at AssumeRole. Idempotent: if an association for this
# (ns, sa) already exists, leave it. Requires the runner's AWS role to hold
# eks:{Create,List,Delete}PodIdentityAssociation + iam:PassRole on the CP role.
ensure_pod_identity() {
  : "${CP_POD_IDENTITY_ROLE:?CP_POD_IDENTITY_ROLE (CP IAM role ARN) is required}"
  # Always (re)create a FRESH association: a stale one left by an
  # interrupted/cancelled prior run can be present but not honored by the
  # pod-identity agent for newly-admitted pods, leaving the CP without creds.
  # Delete any existing, then create.
  delete_pod_identity
  aws eks create-pod-identity-association --region "$AWS_REGION" \
    --cluster-name "$EKS_CLUSTER_NAME" --namespace "$NS" \
    --service-account "$SA_NAME" --role-arn "$CP_POD_IDENTITY_ROLE" >/dev/null
  echo "Created Pod Identity association $NS/$SA_NAME -> $CP_POD_IDENTITY_ROLE"
}

delete_pod_identity() {
  local ids id
  ids="$(aws eks list-pod-identity-associations --region "$AWS_REGION" \
    --cluster-name "$EKS_CLUSTER_NAME" --namespace "$NS" \
    --query "associations[].associationId" --output text 2>/dev/null || true)"
  for id in $ids; do
    [ "$id" = "None" ] && continue
    aws eks delete-pod-identity-association --region "$AWS_REGION" \
      --cluster-name "$EKS_CLUSTER_NAME" --association-id "$id" >/dev/null 2>&1 || true
    echo "Deleted Pod Identity association $id"
  done
}

# The EKS Pod Identity agent injects credentials at pod ADMISSION and never
# retries for the life of the pod. A pod admitted before the agent has cached
# the freshly-created association gets no creds, and STS AssumeRole then fails
# with "no EC2 IMDS role found". So restart the CP and VERIFY the new pod
# actually carries AWS_CONTAINER_CREDENTIALS_FULL_URI; retry the restart until
# it does (the association propagates within a few seconds-to-a-minute).
restart_cp_with_identity() {
  local attempt uri pod
  # Give the just-created association a head start before the first restart so
  # the pod-identity agent has likely cached it by admission time.
  sleep 15
  for attempt in $(seq 1 10); do
    "${KUBECTL[@]}" -n "$NS" rollout restart deploy/duckgres-control-plane >/dev/null
    "${KUBECTL[@]}" -n "$NS" rollout status deploy/duckgres-control-plane --timeout=180s
    pod="$("${KUBECTL[@]}" -n "$NS" get pod -l app=duckgres-control-plane -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    uri="$("${KUBECTL[@]}" -n "$NS" exec "$pod" -- sh -c 'printf %s "$AWS_CONTAINER_CREDENTIALS_FULL_URI"' 2>/dev/null || true)"
    if [ -n "$uri" ]; then
      echo "CP pod $pod has Pod Identity creds (attempt $attempt)."
      return 0
    fi
    echo "CP pod $pod has no Pod Identity creds yet (attempt $attempt); waiting for association to propagate…"
    sleep 20
  done
  echo "CP never received Pod Identity credentials — STS activation will fail." >&2
  return 1
}

# The cnpg lakekeeper_<org> role+db are owned by the Crossplane composition and
# dropped when the Duckling CR deletes — but that cascade is async and can lag,
# leaving a stranded role from a prior run (incl. a cancelled one with no
# teardown). On the next run the same org id re-provisions against the stranded
# role whose password has drifted, the Lakekeeper migrate Job hits SASL auth
# failure, and the warehouse never goes ready. So drop it directly on the
# active shard, idempotently, BOTH before provisioning (clean slate, so a rerun
# never inherits stranded state) and at teardown (deterministic clean exit).
# Scoped to this PR's unique org ids, so it can't touch another PR's tenant.
drop_cnpg_role() { # org-id
  local ident
  # Mirror the composition's PG identifier: lakekeeper_<lower, [^a-z0-9_]→_>.
  ident="lakekeeper_$(printf %s "$1" | tr 'A-Z-' 'a-z_' | tr -cd 'a-z0-9_')"
  "${KUBECTL[@]}" -n cnpg-shards exec shard-001-1 -c postgres -- \
    psql -U postgres -c "DROP DATABASE IF EXISTS ${ident} WITH (FORCE);" >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n cnpg-shards exec shard-001-1 -c postgres -- \
    psql -U postgres -c "DROP ROLE IF EXISTS ${ident};" >/dev/null 2>&1 || true
}

# Every duckling org a harness run provisions for a PR (harness.sh main()):
# cnpg + ext (full coverage) and res1 + res2 (cnpg-shard/ducklake-only orgs
# hosting the parallel resilience lanes). Keep in sync with harness.sh.
ci_orgs() { # pr-number
  local pr="$1"
  echo "ci-pr-${pr}-cnpg ci-pr-${pr}-ext ci-pr-${pr}-res1 ci-pr-${pr}-res2"
}
# The cnpg-shard-backed orgs (everything except ext) — these own a
# lakekeeper_<org> role+db on the shard that drop_cnpg_role must clean.
ci_cnpg_orgs() { # pr-number
  local pr="$1"
  echo "ci-pr-${pr}-cnpg ci-pr-${pr}-res1 ci-pr-${pr}-res2"
}

delete_ci_ducklings() { # pr-number
  local pr="$1" org
  for org in $(ci_orgs "$pr"); do
    "${KUBECTL[@]}" -n ducklings delete "duckling/$org" --ignore-not-found --wait=false 2>/dev/null || true
  done
}

wait_ci_ducklings_deleted() { # pr-number timeout
  local pr="$1" timeout="$2" org
  for org in $(ci_orgs "$pr"); do
    "${KUBECTL[@]}" -n ducklings wait --for=delete "duckling/$org" --timeout="$timeout" 2>/dev/null || true
  done
}

delete_ci_bindings() { # pr-number
  local pr="$1"
  "${KUBECTL[@]}" delete clusterrolebinding -l "duckgres.posthog.com/ci-pr=${pr}" --ignore-not-found
  "${KUBECTL[@]}" -n lakekeeper delete rolebinding -l "duckgres.posthog.com/ci-pr=${pr}" --ignore-not-found
}

reset_pr_stack() {
  # A cancelled or failed run can leave a namespace, config-store rows, and
  # shared Duckling/Lakekeeper resources for this PR. Start from a clean slate
  # so apply never reuses stale network policies, services, or tenant state.
  delete_ci_ducklings "$PR_NUMBER"
  wait_ci_ducklings_deleted "$PR_NUMBER" 300s
  for org in $(ci_cnpg_orgs "$PR_NUMBER"); do drop_cnpg_role "$org"; done
  delete_pod_identity
  delete_ci_bindings "$PR_NUMBER"
  "${KUBECTL[@]}" delete namespace "$NS" --ignore-not-found --wait=true --timeout=300s
}

cmd_deploy() {
  reset_pr_stack

  echo "::group::Apply manifests ($NS)"
  render | "${KUBECTL[@]}" apply -f -
  echo "::endgroup::"

  "${KUBECTL[@]}" -n "$NS" rollout status deploy/duckgres-config-store --timeout=120s

  # Create the Pod Identity association first, then restart the CP until its
  # pod actually carries the injected credentials.
  ensure_pod_identity
  restart_cp_with_identity
}

cmd_test() {
  # Ship harness.sh into the namespace as a ConfigMap and run it as a Job that
  # talks to the control-plane ClusterIP service. The Job SA is `duckgres`,
  # which can delete worker pods in-namespace (durability test).
  "${KUBECTL[@]}" -n "$NS" create configmap duckgres-harness \
    --from-file=harness.sh="$HERE/harness.sh" \
    --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -

  INTERNAL_SECRET="$(cat "$internal_secret_file")"
  INTERNAL_SECRET_FALLBACK="$(cat "$internal_secret_fallback_file")"
  "${KUBECTL[@]}" -n "$NS" delete job duckgres-harness --ignore-not-found
  cat <<YAML | "${KUBECTL[@]}" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: duckgres-harness
  namespace: $NS
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: duckgres
      restartPolicy: Never
      containers:
        - name: harness
          # postgres:18-alpine: psql 18 ships the pipeline meta-commands
          # (\startpipeline/\syncpipeline/\endpipeline) that the harness's
          # pipeline_error_recovery check (#718) drives the extended-query
          # protocol with. Earlier psql cannot pipeline.
          image: public.ecr.aws/docker/library/postgres:18-alpine
          command: ["/bin/sh", "/harness/harness.sh"]
          env:
            - { name: NAMESPACE, value: "$NS" }
            - { name: PR_NUMBER, value: "$PR_NUMBER" }
            - { name: INTERNAL_SECRET, value: "$INTERNAL_SECRET" }
            - { name: INTERNAL_SECRET_FALLBACK, value: "$INTERNAL_SECRET_FALLBACK" }
            - { name: CP_API, value: "http://duckgres-control-plane.$NS.svc:8080" }
            - { name: CP_PG_HOST, value: "duckgres-control-plane.$NS.svc" }
          # Real requests/limits: the harness shares the default nodepool with
          # the bursty per-PR worker pods. A BestEffort pod is the first thing
          # the kubelet evicts under node memory pressure — observed killing
          # the harness mid-run with no output.
          resources:
            requests: { cpu: 200m, memory: 256Mi }
            limits: { memory: 512Mi }
          volumeMounts: [{ name: h, mountPath: /harness }]
      volumes: [{ name: h, configMap: { name: duckgres-harness } }]
YAML

  echo "Streaming harness logs…"
  # Wait for the pod then follow logs; surface the Job's final status as our exit.
  "${KUBECTL[@]}" -n "$NS" wait --for=condition=ready pod -l job-name=duckgres-harness --timeout=120s || true
  "${KUBECTL[@]}" -n "$NS" logs -f job/duckgres-harness || true

  # Decide pass/fail by polling BOTH terminal conditions. `wait
  # --for=condition=complete` alone hangs the full timeout when the Job
  # FAILED (the condition it waits for never becomes true), which is what
  # made a one-second harness failure stall the step for 20 minutes.
  deadline=$(( $(date +%s) + 1200 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$("${KUBECTL[@]}" -n "$NS" get job duckgres-harness -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null)" = "True" ]; then
      echo "harness Job complete."; return 0
    fi
    if [ "$("${KUBECTL[@]}" -n "$NS" get job duckgres-harness -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null)" = "True" ]; then
      echo "harness Job failed." >&2; return 1
    fi
    sleep 5
  done
  echo "harness Job did not reach a terminal state in time." >&2
  return 1
}

cmd_diagnostics() {
  echo "::group::namespace state"
  "${KUBECTL[@]}" -n "$NS" get pods,svc,job -o wide || true
  echo "::endgroup::"
  echo "::group::harness pod status (eviction / OOM / exit code)"
  "${KUBECTL[@]}" -n "$NS" get pods -l job-name=duckgres-harness     -o jsonpath='{range .items[*]}{.metadata.name} phase={.status.phase} reason={.status.reason} msg={.status.message} exit={.status.containerStatuses[0].state.terminated.exitCode} term-reason={.status.containerStatuses[0].state.terminated.reason}{"
"}{end}' || true
  "${KUBECTL[@]}" -n "$NS" describe pods -l job-name=duckgres-harness 2>/dev/null | tail -30 || true
  echo "::endgroup::"
  echo "::group::control-plane logs (tail)"
  "${KUBECTL[@]}" -n "$NS" logs deploy/duckgres-control-plane --tail=200 || true
  echo "::endgroup::"
  echo "::group::worker pods"
  "${KUBECTL[@]}" -n "$NS" get pods -l app=duckgres-worker -o wide || true
  echo "::endgroup::"
}

cmd_teardown() {
  # Deprovision the ci-pr ducklings FIRST so shared-infra resources (S3 bucket,
  # cnpg role+db, lakekeeper CR/secret/SA) are cleaned up by the control plane
  # before we delete it. Best-effort — the namespace delete + e2e-cleanup are the
  # backstop. Uses the CP admin API via a short-lived port-forward.
  if "${KUBECTL[@]}" -n "$NS" get deploy/duckgres-control-plane >/dev/null 2>&1; then
    # Teardown may run on a different runner than deploy (it's a separate
    # always() job so the gating e2e check finishes sooner), so the secret file
    # written at deploy time may not exist — recover it from the in-cluster
    # Secret the deploy templated it into.
    secret="$(cat "$internal_secret_file" 2>/dev/null || true)"
    if [ -z "$secret" ]; then
      secret="$("${KUBECTL[@]}" -n "$NS" get secret duckgres-tokens \
        -o jsonpath='{.data.internal-secret}' 2>/dev/null | base64 -d || true)"
    fi
    if [ -n "$secret" ]; then
      "${KUBECTL[@]}" -n "$NS" port-forward svc/duckgres-control-plane 18080:8080 >/dev/null 2>&1 &
      pf=$!; sleep 4
      for org in $(ci_orgs "$PR_NUMBER"); do
        curl -fsS -X POST -H "X-Duckgres-Internal-Secret: $secret" \
          "http://localhost:18080/api/v1/orgs/$org/deprovision" >/dev/null 2>&1 || true
      done
      # Give the provisioner a moment to drive the duckling deletes.
      for _ in $(seq 1 30); do
        gone=1
        for org in $(ci_orgs "$PR_NUMBER"); do
          st="$(curl -fsS -H "X-Duckgres-Internal-Secret: $secret" \
            "http://localhost:18080/api/v1/orgs/$org/warehouse/status" 2>/dev/null \
            | sed -n 's/.*"state":"\([^"]*\)".*/\1/p')"
          [ "$st" = "deleted" ] || [ -z "$st" ] || gone=0
        done
        [ "$gone" = 1 ] && break
        sleep 10
      done
      kill "$pf" 2>/dev/null || true
    fi
  fi

  # Wait for the Duckling CRs to FULLY delete — not just the warehouse row.
  # A warehouse flips to "deleted" the moment the CR delete is issued, but the
  # CR's finalizers keep running (incl the downstream provider-sql DROP of the
  # cnpg lakekeeper_<org> role+db). If we return before that finishes, a later
  # run that reuses the same org id (rerun, or a force-pushed PR) provisions
  # against a stranded cnpg role whose password has drifted -> the Lakekeeper
  # migrate Job hits SASL auth failure and the warehouse never goes ready.
  # `wait --for=delete` blocks until the CR (and its cascade) is gone.
  delete_ci_ducklings "$PR_NUMBER"
  wait_ci_ducklings_deleted "$PR_NUMBER" 300s

  # Deterministically drop the cnpg role+db in case the composition's async
  # cascade lagged the CR delete above (see drop_cnpg_role). Idempotent.
  for org in $(ci_cnpg_orgs "$PR_NUMBER"); do drop_cnpg_role "$org"; done

  # Drop the Pod Identity association (it's an EKS resource, not in the ns).
  delete_pod_identity

  # Cross-namespace bindings carry the ci-pr label — sweep them, then the ns.
  delete_ci_bindings "$PR_NUMBER"
  "${KUBECTL[@]}" delete namespace "$NS" --ignore-not-found --wait=false
}

# Sweep stale per-PR namespaces left behind by runs that died hard (cancelled
# mid-flight, runner OOM, etc.) before their always() teardown could fire.
# Backstop, not the primary cleanup. Discovers namespaces by the
# managed-by=e2e-mw-dev label and deletes any older than E2E_CLEANUP_MAX_AGE_HOURS
# (default 6h — a real run finishes in <40m, so anything older is orphaned).
# For each: delete its ducklings directly (drives the Crossplane teardown
# without needing the now-gone per-run CP), drop the cnpg role+db, drop the Pod
# Identity association, and sweep the ci-pr-labelled cross-ns bindings + the ns.
# Named e2e-cleanup (not "janitor") to avoid colliding with duckgres's own
# control-plane janitor. NAMESPACE is not required for this path.
cmd_e2e_cleanup() {
  local max_age_h now ns created age pr
  max_age_h="${E2E_CLEANUP_MAX_AGE_HOURS:-6}"
  now="$(date +%s)"
  "${KUBECTL[@]}" get ns -l app.kubernetes.io/managed-by=e2e-mw-dev \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.creationTimestamp}{"\n"}{end}' 2>/dev/null \
  | while read -r ns created; do
      [ -n "$ns" ] || continue
      age=$(( (now - $(date -d "$created" +%s)) / 3600 ))
      if [ "$age" -lt "$max_age_h" ]; then
        echo "e2e-cleanup: keep $ns (age ${age}h < ${max_age_h}h)"; continue
      fi
      pr="${ns#duckgres-ci-pr-}"
      echo "e2e-cleanup: reaping $ns (age ${age}h, PR $pr)"
      delete_ci_ducklings "$pr"
      wait_ci_ducklings_deleted "$pr" 300s
      for org in $(ci_cnpg_orgs "$pr"); do drop_cnpg_role "$org"; done
      NS="$ns" delete_pod_identity
      delete_ci_bindings "$pr"
      "${KUBECTL[@]}" delete namespace "$ns" --ignore-not-found --wait=false
    done
}

case "${1:?usage: run.sh deploy|test|diagnostics|teardown|e2e-cleanup}" in
  deploy) : "${NAMESPACE:?}"; cmd_deploy ;;
  test) : "${NAMESPACE:?}"; cmd_test ;;
  diagnostics) : "${NAMESPACE:?}"; cmd_diagnostics ;;
  teardown) : "${NAMESPACE:?}"; cmd_teardown ;;
  e2e-cleanup) cmd_e2e_cleanup ;;
  *) echo "unknown: $1" >&2; exit 2 ;;
esac
