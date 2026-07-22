#!/usr/bin/env bash
# Orchestrate isolated mw-dev test stacks from CI (kubectl reaches the private
# API via Tailscale). Payloads are selected by subcommand; deploy/teardown and
# diagnostics are shared by e2e and scenario runs.
#
# Required env (set by .github/workflows/*mw-dev.yml):
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
FROZEN_S3_URI="${DUCKGRES_SCENARIO_FROZEN_S3_URI:-s3://posthog-duckgres-scenario-frozen-data-mw-dev/frozen_v1/}"
SCENARIO_JOB_WATCH_TIMEOUT_SECONDS="${SCENARIO_JOB_WATCH_TIMEOUT_SECONDS:-16200}"
SCENARIO_JOB_CLEANUP_TIMEOUT_SECONDS="${SCENARIO_JOB_CLEANUP_TIMEOUT_SECONDS:-180}"
SCENARIO_POD_START_TIMEOUT_SECONDS="${SCENARIO_POD_START_TIMEOUT_SECONDS:-180}"
SCENARIO_NAME="${SCENARIO_NAME:-full-suite}"
SCENARIO_ARTIFACTS_DIR="${SCENARIO_ARTIFACTS_DIR:-$HERE/../../artifacts/scenario-dev}"
DUCKGRES_K8S_WORKER_CPU_REQUEST="${DUCKGRES_K8S_WORKER_CPU_REQUEST:-750m}"
DUCKGRES_K8S_WORKER_MEMORY_REQUEST="${DUCKGRES_K8S_WORKER_MEMORY_REQUEST:-1536Mi}"

# Internal secret for the per-PR control plane. Random per run; never reused.
# Stamped into the rendered manifests and handed to the in-cluster harness.
secret_dir="${DUCKGRES_CI_SECRET_DIR:-/tmp}"
internal_secret_file="$secret_dir/duckgres-ci-internal-secret"
# Rotation fallback secret (DUCKGRES_INTERNAL_SECRET_FALLBACKS): a second
# random value the CP must also accept, so the harness can assert the
# rotation-overlap path (internal_secret_fallback_auth).
internal_secret_fallback_file="$secret_dir/duckgres-ci-internal-secret-fallback"
# AES key for user persistent secrets (DUCKGRES_USER_SECRET_KEY). Random per
# run: stored user secrets only need to outlive the run's sessions.
user_secret_key_file="$secret_dir/duckgres-ci-user-secret-key"

require_pr_identity() {
  : "${PR_NUMBER:?PR_NUMBER is required}"
  case "$PR_NUMBER" in
    *[!0-9]*)
      echo "PR_NUMBER must be numeric, got '$PR_NUMBER'." >&2
      return 2
      ;;
  esac
  if [ -z "$NS" ]; then
    echo "NAMESPACE is required." >&2
    return 2
  fi
  if [ "$NS" != "duckgres-ci-pr-$PR_NUMBER" ]; then
    echo "NAMESPACE '$NS' does not match PR_NUMBER '$PR_NUMBER'." >&2
    return 2
  fi
}

render() {
  : "${WORKER_IMAGE:?}" "${CONTROLPLANE_IMAGE:?}" "${PR_NUMBER:?}"
  ensure_secret_dir
  [ -f "$internal_secret_file" ] || (umask 077; openssl rand -hex 16 > "$internal_secret_file")
  [ -f "$internal_secret_fallback_file" ] || (umask 077; openssl rand -hex 16 > "$internal_secret_fallback_file")
  [ -f "$user_secret_key_file" ] || (umask 077; openssl rand -base64 32 > "$user_secret_key_file")
  INTERNAL_SECRET="$(cat "$internal_secret_file")" \
  INTERNAL_SECRET_FALLBACK="$(cat "$internal_secret_fallback_file")" \
  USER_SECRET_KEY="$(cat "$user_secret_key_file")" \
  NAMESPACE="$NS" PR_NUMBER="$PR_NUMBER" \
  WORKER_IMAGE="$WORKER_IMAGE" CONTROLPLANE_IMAGE="$CONTROLPLANE_IMAGE" \
  DUCKGRES_K8S_WORKER_CPU_REQUEST="$DUCKGRES_K8S_WORKER_CPU_REQUEST" \
  DUCKGRES_K8S_WORKER_MEMORY_REQUEST="$DUCKGRES_K8S_WORKER_MEMORY_REQUEST" \
    envsubst '$NAMESPACE $PR_NUMBER $WORKER_IMAGE $CONTROLPLANE_IMAGE $INTERNAL_SECRET $INTERNAL_SECRET_FALLBACK $USER_SECRET_KEY $DUCKGRES_K8S_WORKER_CPU_REQUEST $DUCKGRES_K8S_WORKER_MEMORY_REQUEST' \
    < "$HERE/manifests.tmpl.yaml"
}

ensure_secret_dir() {
  if [ -e "$secret_dir" ] && [ ! -d "$secret_dir" ]; then
    echo "DUCKGRES_CI_SECRET_DIR is not a directory: $secret_dir" >&2
    return 1
  fi
  if [ ! -d "$secret_dir" ]; then
    (umask 077; mkdir -p -- "$secret_dir") || {
      echo "Failed to create DUCKGRES_CI_SECRET_DIR: $secret_dir" >&2
      return 1
    }
  fi
}

# Bind this namespace's `duckgres` SA to the same IAM role the real mw-dev
# control plane uses (EKS Pod Identity). With it, the CP brokers per-duckling
# S3 STS creds exactly like prod and activation completes — without it,
# DuckLake activation fails at AssumeRole. Idempotent: if an association for
# this (ns, sa) already exists, leave it. Requires the runner's AWS role to hold
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

# The cnpg-shard metadata role+db are owned by the Crossplane composition and
# dropped when the Duckling CR deletes — but that cascade is async and can lag,
# leaving stranded state from a prior run (incl. a cancelled one with no
# teardown). Ducklings use mdstore_<org>. Drop the role+db name directly on
# the active shard, idempotently, BOTH before provisioning (clean slate, so a
# rerun never inherits stranded state) and at teardown (deterministic clean
# exit). Scoped to this PR's unique org ids, so it can't touch another PR's
# tenant.
drop_cnpg_role() { # org-id
  local suffix ident
  # Mirror the composition's PG identifier suffix: lower, [^a-z0-9_] -> _.
  suffix="$(printf %s "$1" | tr 'A-Z-' 'a-z_' | tr -cd 'a-z0-9_')"
  ident="mdstore_${suffix}"
  "${KUBECTL[@]}" -n cnpg-shards exec shard-001-1 -c postgres -- \
    psql -U postgres -c "DROP DATABASE IF EXISTS ${ident} WITH (FORCE);" >/dev/null 2>&1 || true
  "${KUBECTL[@]}" -n cnpg-shards exec shard-001-1 -c postgres -- \
    psql -U postgres -c "DROP ROLE IF EXISTS ${ident};" >/dev/null 2>&1 || true
}

# Every CNPG-backed duckling org a harness run provisions for a PR
# (harness.sh main()). Keep in sync with harness.sh.
ci_orgs() { # pr-number
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
  local pr="$1" timeout="$2" org get_out deletion_timestamp finalizers conditions rest rc=0
  for org in $(ci_orgs "$pr"); do
    if ! "${KUBECTL[@]}" -n ducklings wait --for=delete "duckling/$org" --timeout="$timeout"; then
      if get_out="$("${KUBECTL[@]}" -n ducklings get "duckling/$org" \
          -o jsonpath='{.metadata.deletionTimestamp}{"|"}{.metadata.finalizers}{"|"}{range .status.conditions[*]}{.type}={.status}:{.reason}{";"}{end}' 2>&1)"; then
        deletion_timestamp="${get_out%%|*}"
        rest="${get_out#*|}"
        finalizers="${rest%%|*}"
        conditions="${rest#*|}"
        [ -n "$deletion_timestamp" ] || deletion_timestamp="<none>"
        [ -n "$finalizers" ] || finalizers="<none>"
        [ -n "$conditions" ] || conditions="<none>"
        echo "Duckling $org did not delete within $timeout; refusing to reuse same-PR resource names." >&2
        echo "Duckling $org summary: deletionTimestamp=$deletion_timestamp finalizers=$finalizers conditions=$conditions" >&2
        rc=1
      elif ! printf '%s\n' "$get_out" | grep -qi 'not found'; then
        echo "Could not verify Duckling $org was deleted after wait failed; refusing to reuse same-PR resource names." >&2
        printf '%s\n' "$get_out" >&2
        rc=1
      fi
    fi
  done
  return "$rc"
}

delete_ci_bindings() { # pr-number
  local pr="$1"
  "${KUBECTL[@]}" delete clusterrolebinding -l "duckgres.posthog.com/ci-pr=${pr}" --ignore-not-found
}

reset_pr_stack() {
  # A cancelled or failed run can leave a namespace, config-store rows, and
  # shared Duckling resources for this PR. Start from a clean slate
  # so apply never reuses stale network policies, services, or tenant state.
  delete_ci_ducklings "$PR_NUMBER"
  wait_ci_ducklings_deleted "$PR_NUMBER" 300s
  for org in $(ci_orgs "$PR_NUMBER"); do drop_cnpg_role "$org"; done
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

cmd_test_e2e() {
  # Ship harness.sh into the namespace as a ConfigMap and run it as a Job that
  # talks to the control-plane ClusterIP service. The Job SA is `duckgres`,
  # which can delete worker pods in-namespace (durability test).
  "${KUBECTL[@]}" -n "$NS" create configmap duckgres-harness \
    --from-file=harness.sh="$HERE/e2e/harness.sh" \
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
    metadata:
      annotations:
        # Same consolidation pinning as the CP/config-store: the harness runs
        # ~15 minutes of ordered assertions with zero retry budget
        # (backoffLimit 0), and the worker-churn lanes empty nodes mid-run —
        # a Karpenter reclaim of the harness pod's node kills the Job at
        # whatever lane is next (observed: rc=130 mid COPY lane, no FAIL line).
        karpenter.sh/do-not-disrupt: "true"
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
  #
  # This watch window must exceed the harness's total wall time: the ready-wait
  # (harness.sh READY_TIMEOUT, up to 1200s while a cnpg-shard org's metadata
  # probe waits out the shared-shard credential-propagation transient) PLUS the
  # assertion lanes that run after. 1200s used to be the whole budget, which
  # meant a slow provision left no room for the lanes and this loop returned a
  # false "did not reach a terminal state" even though the harness would pass.
  # The Job has no activeDeadlineSeconds, so this loop is the only cap; size it
  # generously above READY_TIMEOUT and stay under the CP's 30-min provisioning
  # hard-timeout. Env-overridable to keep it in lockstep with READY_TIMEOUT.
  # 2100: the reshard lanes now run in dedicated runner pods, adding pod
  # schedule + image pull (~1-2 min worst case per lane) on top of the old
  # in-process execution.
  deadline=$(( $(date +%s) + ${HARNESS_WATCH_TIMEOUT:-2100} ))
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

scenario_name_for_file() {
  basename "$1" .yaml | tr '_' '-'
}

scenario_job_name() {
  local name="$1" run_hash
  run_hash="$(printf '%s' "${DUCKGRES_SCENARIO_RUN_ID:?DUCKGRES_SCENARIO_RUN_ID is required}" | cksum | awk '{print $1}')"
  printf 'duckgres-scenario-%s-%s\n' "$name" "$run_hash" | tr '_' '-'
}

cmd_test_scenario() {
  local scenario_file scenario_name
  : "${SCENARIO_RUNNER_IMAGE:?SCENARIO_RUNNER_IMAGE is required}"

  case "$SCENARIO_NAME" in
    ""|*[!a-z0-9_-]*)
      echo "SCENARIO_NAME must contain only lowercase letters, numbers, hyphens, and underscores, got '$SCENARIO_NAME'." >&2
      return 2
      ;;
  esac

  scenario_file="tests/mw-dev/scenario/scenarios/${SCENARIO_NAME}.yaml"
  if [ ! -f "$HERE/scenario/scenarios/${SCENARIO_NAME}.yaml" ]; then
    echo "Scenario '$SCENARIO_NAME' does not exist at $scenario_file." >&2
    return 2
  fi

  scenario_name="$(scenario_name_for_file "$scenario_file")"
  DUCKGRES_SCENARIO_RUN_ID="scenario-dev-${scenario_name}-${PR_NUMBER}" \
    run_scenario "$scenario_name" "$scenario_file"
}

run_scenario() {
  local scenario_name="$1" scenario_file="$2" job pod api_base pg flight suffix internal_secret artifact_rc=0 container_rc=0 container_exit_code scenario_rc=0
  api_base="http://duckgres-control-plane.$NS.svc:8080"
  pg="$("${KUBECTL[@]}" -n "$NS" get svc duckgres-control-plane -o jsonpath='{.spec.clusterIP}')"
  flight="duckgres-control-plane.$NS.svc:8815"
  suffix=".ci.duckgres.local"
  internal_secret="$(cat "$internal_secret_file")"
  job="$(scenario_job_name "$scenario_name")"

  delete_scenario_job "$job"
  cat <<YAML | "${KUBECTL[@]}" -n "$NS" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
spec:
  backoffLimit: 0
  activeDeadlineSeconds: $SCENARIO_JOB_WATCH_TIMEOUT_SECONDS
  template:
    metadata:
      annotations:
        # The scenario is a long-running, zero-retry Job whose emptyDir also
        # holds the only artifact copy. Voluntary disruption would lose both.
        karpenter.sh/do-not-disrupt: "true"
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/arch: arm64
      containers:
        - name: scenario
          image: $SCENARIO_RUNNER_IMAGE
          imagePullPolicy: IfNotPresent
          args: ["$scenario_file"]
          env:
            - { name: DUCKGRES_SCENARIO_API_BASE, value: "$api_base" }
            - { name: DUCKGRES_SCENARIO_INTERNAL_SECRET, value: "$internal_secret" }
            - { name: DUCKGRES_SCENARIO_PG_HOST, value: "$pg" }
            - { name: DUCKGRES_SCENARIO_SNI_SUFFIX, value: "$suffix" }
            - { name: DUCKGRES_SCENARIO_FROZEN_S3_URI, value: "$FROZEN_S3_URI" }
            - { name: DUCKGRES_SCENARIO_FLIGHT_ADDR, value: "$flight" }
            - { name: DUCKGRES_SCENARIO_FLIGHT_INSECURE_SKIP_VERIFY, value: "true" }
            - { name: DUCKGRES_SCENARIO_DBT_BIN, value: "dbt" }
            # The Crossplane composition grants this isolated service account
            # exact-name access to only the matching CNPG credential Secret.
            - { name: DUCKGRES_SCENARIO_ORG_ID, value: "ci-pr-${PR_NUMBER}-cnpg" }
            - { name: DUCKGRES_SCENARIO_OUTPUT_BASE, value: "/artifacts/scenario-dev" }
            - { name: DUCKGRES_SCENARIO_RUN_ID, value: "$DUCKGRES_SCENARIO_RUN_ID" }
            - { name: DUCKGRES_SCENARIO_MAX_RUNTIME, value: "${DUCKGRES_SCENARIO_MAX_RUNTIME:-4h}" }
            - { name: DUCKGRES_SCENARIO_GO_TEST_TIMEOUT, value: "${DUCKGRES_SCENARIO_GO_TEST_TIMEOUT:-4h15m}" }
            - { name: GOCACHE, value: "/tmp/go-cache" }
          resources:
            requests: { cpu: "1", memory: "2Gi" }
            limits: { memory: "6Gi" }
          volumeMounts:
            - { name: artifacts, mountPath: /artifacts }
        # Keep the shared artifact volume attached to a running container after
        # the scenario exits. kubectl cp uses exec/tar and cannot copy from a
        # terminated scenario container.
        - name: artifact-keeper
          image: $SCENARIO_RUNNER_IMAGE
          imagePullPolicy: IfNotPresent
          command: ["bash", "-c"]
          args:
            - |
              until [ -f /artifacts/artifacts-collected ]; do sleep 1; done
          resources:
            requests: { cpu: "10m", memory: "16Mi" }
            limits: { memory: "64Mi" }
          volumeMounts:
            - { name: artifacts, mountPath: /artifacts }
      volumes:
        - { name: artifacts, emptyDir: {} }
YAML

  echo "Streaming scenario logs for $job..."
  if ! pod="$(discover_scenario_pod "$job")"; then
    echo "Scenario pod for $job could not be discovered." >&2
    delete_scenario_job "$job" || true
    return 1
  fi
  "${KUBECTL[@]}" -n "$NS" logs -f "pod/$pod" -c scenario \
    --pod-running-timeout="${SCENARIO_POD_START_TIMEOUT_SECONDS}s" || true
  wait_for_scenario_container "$pod" || container_rc=$?
  if [ "$container_rc" -ne 0 ]; then
    echo "Scenario container for $job did not reach a terminated state; refusing to collect a potentially partial snapshot." >&2
    delete_scenario_job "$job" || echo "Failed to stop scenario Job $job after an unconfirmed container termination." >&2
    return "$container_rc"
  fi
  copy_scenario_artifacts "$scenario_name" "$pod" || artifact_rc=$?
  if [ "$artifact_rc" -eq 2 ]; then
    container_exit_code="$(scenario_container_exit_code "$pod" || true)"
    echo "Scenario container exit code before forced Job cleanup: ${container_exit_code:-unknown}." >&2
    delete_scenario_job "$job" || true
    case "$container_exit_code" in
      ""|*[!0-9]*|0) return "$artifact_rc" ;;
      *) return "$container_exit_code" ;;
    esac
  fi
  wait_for_scenario_job "$job" || scenario_rc=$?
  [ "$scenario_rc" -eq 0 ] || return "$scenario_rc"
  return "$artifact_rc"
}

delete_scenario_job() {
  local job="$1"
  "${KUBECTL[@]}" -n "$NS" delete job "$job" --ignore-not-found \
    --cascade=foreground --wait=true --timeout="${SCENARIO_JOB_CLEANUP_TIMEOUT_SECONDS}s"
}

discover_scenario_pod() {
  local job="$1" attempt pod
  for attempt in 1 2 3 4 5 6; do
    pod="$("${KUBECTL[@]}" -n "$NS" get pod -l "job-name=$job" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -n "$pod" ]; then
      printf '%s\n' "$pod"
      return 0
    fi
    [ "$attempt" -eq 6 ] || sleep 5
  done
  return 1
}

wait_for_scenario_container() {
  local pod="$1"
  "${KUBECTL[@]}" -n "$NS" wait \
    --for='jsonpath={.status.containerStatuses[?(@.name=="scenario")].state.terminated.reason}' \
    "pod/$pod" --timeout="${SCENARIO_JOB_WATCH_TIMEOUT_SECONDS}s"
}

scenario_container_exit_code() {
  local pod="$1"
  "${KUBECTL[@]}" -n "$NS" get "pod/$pod" \
    -o jsonpath='{.status.containerStatuses[?(@.name=="scenario")].state.terminated.exitCode}' 2>/dev/null
}

wait_for_scenario_job() {
  local job="$1" deadline
  deadline=$(( $(date +%s) + SCENARIO_JOB_WATCH_TIMEOUT_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$("${KUBECTL[@]}" -n "$NS" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null)" = "True" ]; then
      echo "scenario Job $job complete."
      return 0
    fi
    if [ "$("${KUBECTL[@]}" -n "$NS" get job "$job" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null)" = "True" ]; then
      echo "scenario Job $job failed." >&2
      return 1
    fi
    sleep 10
  done
  echo "scenario Job $job did not reach a terminal state in time." >&2
  return 1
}

copy_scenario_artifacts() {
  local scenario_name="$1" pod="$2" artifact attempt dest partial staging="" token
  local copy_failed=0 failure_reason=""

  if ! mkdir -p "$SCENARIO_ARTIFACTS_DIR"; then
    echo "Failed to create scenario artifact root $SCENARIO_ARTIFACTS_DIR." >&2
    copy_failed=1
    failure_reason="failed to create local scenario artifact root"
  else
    for attempt in 1 2 3 4 5 6 7 8 9 10; do
      if ! staging="$(mktemp -d "$SCENARIO_ARTIFACTS_DIR/.${scenario_name}.XXXXXX")"; then
        echo "Failed to create scenario artifact staging directory." >&2
        copy_failed=1
        failure_reason="failed to create local scenario artifact staging directory"
        staging=""
        break
      fi
      token="${staging##*.}"
      dest="$SCENARIO_ARTIFACTS_DIR/${scenario_name}-${token}"
      partial="${dest}.partial"
      if [ ! -e "$dest" ] && [ ! -e "$partial" ]; then
        break
      fi
      if ! rmdir "$staging"; then
        echo "Failed to discard colliding scenario artifact staging directory $staging." >&2
        copy_failed=1
        failure_reason="failed to discard colliding scenario artifact staging directory"
        staging=""
        break
      fi
      staging=""
    done
    if [ "$copy_failed" -eq 0 ] && [ -z "$staging" ]; then
      echo "Failed to allocate a unique scenario artifact destination." >&2
      copy_failed=1
      failure_reason="failed to allocate a unique scenario artifact destination"
    fi
  fi

  if [ "$copy_failed" -eq 0 ] && "${KUBECTL[@]}" -n "$NS" cp -c artifact-keeper \
      "$pod:/artifacts/scenario-dev/$DUCKGRES_SCENARIO_RUN_ID/." "$staging"; then
    for artifact in scenario_summary.json scenario_summary.md step_results.csv events.jsonl; do
      if [ -z "$(find "$staging" -type f -name "$artifact" -print -quit)" ]; then
        echo "Failed to copy scenario artifacts: missing required scenario artifact $artifact." >&2
        copy_failed=1
        failure_reason="${failure_reason}${failure_reason:+; }missing required scenario artifact $artifact"
      fi
    done
    if [ "$copy_failed" -eq 0 ]; then
      if mv "$staging" "$dest"; then
        echo "Copied scenario artifacts to $dest."
      else
        echo "Failed to promote scenario artifacts to $dest." >&2
        copy_failed=1
        failure_reason="failed to promote validated scenario artifacts"
      fi
    fi
  elif [ "$copy_failed" -eq 0 ]; then
    echo "Failed to copy scenario artifacts from $pod." >&2
    copy_failed=1
    failure_reason="kubectl cp failed for scenario artifacts from $pod"
  fi

  # Failed and partial copies remain visible under the workflow's upload root.
  # upload-artifact excludes dot-prefixed staging directories by default.
  if [ "$copy_failed" -ne 0 ] && [ -n "${staging:-}" ] && [ -d "$staging" ]; then
    if ! printf '%s\n' "${failure_reason:-scenario artifact collection failed}" > "$staging/artifact_collection_error.txt"; then
      echo "Failed to write scenario artifact collection error marker." >&2
    fi
    if mv "$staging" "$partial"; then
      echo "Preserved partial scenario artifacts at $partial." >&2
    else
      echo "Failed to publish partial scenario artifacts at $partial." >&2
    fi
  fi

  # Release the keeper even when copying fails so the Job can reach a terminal
  # state and preserve the scenario container's real exit status.
  if ! release_artifact_keeper "$pod"; then
    echo "Failed to release scenario artifact keeper for $pod." >&2
    return 2
  fi
  return "$copy_failed"
}

release_artifact_keeper() {
  local pod="$1" attempt
  for attempt in 1 2 3; do
    if "${KUBECTL[@]}" -n "$NS" exec -c artifact-keeper "$pod" -- touch /artifacts/artifacts-collected; then
      return 0
    fi
    echo "Artifact keeper release attempt $attempt failed for $pod." >&2
    [ "$attempt" -eq 3 ] || sleep 2
  done
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
  echo "::group::reshard runner pods (spec + logs)"
  # Reshard ops execute in dedicated duckgres-reshard-op-<id> pods; their
  # stdout carries the step machine's slog lines (incl. the exact error of a
  # best-effort backup failure that the op log only summarizes). Describe
  # shows whether pod-identity env/volume injection happened.
  "${KUBECTL[@]}" -n "$NS" get pods -l app=duckgres-reshard -o wide || true
  for p in $("${KUBECTL[@]}" -n "$NS" get pods -l app=duckgres-reshard -o name 2>/dev/null); do
    echo "---- $p describe (tail) ----"
    "${KUBECTL[@]}" -n "$NS" describe "$p" 2>/dev/null | tail -40 || true
    echo "---- $p logs ----"
    "${KUBECTL[@]}" -n "$NS" logs "$p" --tail=300 2>/dev/null || true
  done
  echo "::endgroup::"
  echo "::group::scenario jobs"
  "${KUBECTL[@]}" -n "$NS" get job,pod -l job-name -o wide || true
  echo "::endgroup::"
}

cmd_teardown() {
  local duckling_delete_rc=0

  # Deprovision the ci-pr ducklings FIRST so shared-infra resources (S3 bucket,
  # cnpg role+db) are cleaned up by the control plane
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
          # A 404 here means the org was fully deleted (row cascaded away, e.g.
          # the lifecycle test's DELETE /orgs/:id) — that counts as gone. Under
          # `set -euo pipefail` an unguarded curl -f 404 (exit 22) would abort
          # teardown, so tolerate it and let the empty-state check below treat
          # it as gone.
          st="$(curl -fsS -H "X-Duckgres-Internal-Secret: $secret" \
            "http://localhost:18080/api/v1/orgs/$org/warehouse/status" 2>/dev/null \
            | sed -n 's/.*"state":"\([^"]*\)".*/\1/p' || true)"
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
  # cnpg metadata role+db). If we return before that finishes, a later run that
  # reuses the same org id (rerun, or a force-pushed PR) can provision against
  # stranded cnpg state and never reach ready.
  # `wait --for=delete` blocks until the CR (and its cascade) is gone.
  delete_ci_ducklings "$PR_NUMBER"
  wait_ci_ducklings_deleted "$PR_NUMBER" 300s || duckling_delete_rc=$?

  # Deterministically drop the cnpg role+db in case the composition's async
  # cascade lagged the CR delete above (see drop_cnpg_role). Idempotent.
  for org in $(ci_orgs "$PR_NUMBER"); do drop_cnpg_role "$org"; done

  # Drop the Pod Identity association (it's an EKS resource, not in the ns).
  delete_pod_identity

  # Cross-namespace bindings carry the ci-pr label — sweep them, then the ns.
  delete_ci_bindings "$PR_NUMBER"
  "${KUBECTL[@]}" delete namespace "$NS" --ignore-not-found --wait=false
  return "$duckling_delete_rc"
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
      wait_ci_ducklings_deleted "$pr" 300s || true
      for org in $(ci_orgs "$pr"); do drop_cnpg_role "$org"; done
      NS="$ns" delete_pod_identity
      delete_ci_bindings "$pr"
      "${KUBECTL[@]}" delete namespace "$ns" --ignore-not-found --wait=false
    done
}

case "${1:?usage: run.sh deploy|test-e2e|test-scenario|diagnostics|teardown|e2e-cleanup}" in
  deploy) : "${NAMESPACE:?}"; require_pr_identity; cmd_deploy ;;
  test-e2e|test) : "${NAMESPACE:?}"; require_pr_identity; cmd_test_e2e ;;
  test-scenario) : "${NAMESPACE:?}"; require_pr_identity; cmd_test_scenario ;;
  diagnostics) : "${NAMESPACE:?}"; require_pr_identity; cmd_diagnostics ;;
  teardown) : "${NAMESPACE:?}"; require_pr_identity; cmd_teardown ;;
  e2e-cleanup) cmd_e2e_cleanup ;;
  *) echo "unknown: $1" >&2; exit 2 ;;
esac
