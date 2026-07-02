# mw-dev per-PR e2e harness

Replaces what `tests/k8s/` (kind) cannot exercise — real Cilium network
policies, real Crossplane Duckling provisioning, and real cnpg-shard +
external RDS metadata stores. Those are the layers where this quarter's
production bugs lived.

## Flow (`.github/workflows/e2e-mw-dev.yml`)

1. **Build** the arm64-only all-in-one `duckgres` image (`_image-build.yml`),
   tagged `pr-<N>-<sha>-arm64`, pushed to ECR. mw-dev runs ONE image for both
   the control plane (`--mode control-plane`) and the workers
   (`DUCKGRES_K8S_WORKER_IMAGE` = same image), so the PR builds just that.
   One arch suffices (mw-dev is arm64); merge-to-main keeps the full
   multi-arch + worker/CP-split matrices in the existing CD workflows.
2. **Tailscale** join via OIDC/WIF → reach the private mw-dev EKS API.
3. **Deploy** an isolated `duckgres-ci-pr-<N>` namespace: throwaway
   config-store Postgres + a control-plane Deployment on the PR image, spawning
   worker pods in the same namespace.
4. **Test** via an in-cluster Job hitting the CP ClusterIP service. Covers
   the **cnpg + ext** metadata backends. Assertions run in four **parallel
   per-org lanes** (cnpg core suite, two resilience orgs, ext) — worker churn
   is org-scoped, so lanes can't interfere and the wall-clock is the slowest
   lane, not the sum.
5. **Teardown** always: deprovision the ci-pr ducklings (clean shared-infra
   footprint) then delete the namespace.

A scheduled (`cron`) **e2e-cleanup** job (`run.sh e2e-cleanup`) runs every 6h and
reaps any `duckgres-ci-pr-*` namespace older than 6h — a backstop for runs that
died hard before their `always()` teardown could fire. (Named e2e-cleanup, not
"janitor", to avoid colliding with duckgres's own control-plane janitor.)

## What the harness asserts (`harness.sh`)

This suite is the **successor to the retired kind suite** (`tests/k8s/`): every
behavior that suite asserted against a fake kind cluster is re-asserted here
against real mw-dev, on both the cnpg and external-RDS metadata backends. The
in-cluster Job runs as the `duckgres` SA and uses `kubectl` (in-cluster config
from its mounted SA token) for the pod-level checks the Go suite made via
client-go:

- **wire/query** — `SELECT 1` round-trips; 5 concurrent connections stay
  distinct (ported from `TestK8sMultipleConcurrentConnections`); a malformed
  post-TLS startup-message length (negative / ~2GiB / truncated, injected via
  `openssl s_client -starttls postgres`) gets a clean connection close — the CP
  pod must not restart and must keep serving (regression for #715).
- **pipeline error recovery** (#718) — a pipelined extended-query batch (psql
  18 `\startpipeline`) whose first statement errors must have its queued
  statements **discarded until Sync** (the queued INSERT must not execute);
  the statement after `\syncpipeline` must execute normally. This is why the
  harness Job image is `postgres:18-alpine` (pipeline meta-commands are
  psql 18+). The same wire lane also asserts that a pgwire CancelRequest leaves
  the same session immediately reusable.
- **server-side cursors** — DECLARE → `FETCH n` → `MOVE n` (advances without
  returning rows) → `FETCH ALL` (remaining rows only) → CLOSE, with exact value
  assertions on a live worker; then ROLLBACK while a second cursor is still
  partially read must return promptly and leave the session usable (an open
  cursor rowset pins the worker session's single DuckDB connection — the
  pre-fix behavior deadlocked the session at transaction end).
- **cold-burst absorption** — there is no warm pool, so a burst of cold sessions
  spawns workers on demand; if it outruns the org/global cap the surplus gets a
  graceful client-visible hint (`no Duckgres worker … retry in about 45 seconds`
  / `timed out waiting for an available worker`) rather than a hang/500/drop, and
  the pool must then serve a retrying connection. The harness logs whether
  backpressure was observed **and** handles it (queries retry through it).
- **activation** — DuckLake catalogs attach and read/write on cnpg and ext.
- **worker sizing** (TTL-pool model, `docs/design/worker-ttl-pool.md`) — a
  client-sized connection (`duckgres.worker_cpu`/`worker_memory`/`worker_ttl`
  startup options, sent via `PGOPTIONS`; CP runs `allowClientWorkerProfile=true`
  with clamps) spawns a worker pod whose `duckdb-worker` container carries the
  requested CPU+memory on **both** requests and limits — proving the shape flows
  control-plane → k8s pod spec, not BestEffort. A same-shape reconnect **reuses**
  that hot-idle worker (no respawn — the count of that-shape pods stays 1).
  Asserted on cnpg for the ducklake catalog. There is no warm pool, so the only
  workers are the ones a request sizes + spawns on demand.
  Clamp enforcement itself is unit-covered (`controlplane/worker_profile_test.go`).
- **org default worker profile** — an operator-set per-org default worker shape
  + hot-idle TTL (config-store columns `default_worker_cpu`/`memory`/`ttl`, set
  via the admin API `PUT /orgs/:id`) must size a **plain** connection — one that
  sends no `duckgres.worker_*` startup options at all (the external-customer
  case). The harness sets `2/8Gi/10m` on the ext org, asserts the admin API
  round-trips the fields and 400s garbage values, then connects without
  `PGOPTIONS` and asserts the worker pod carries the org default on requests
  **and** limits; finally it clears the default (explicit empty strings) and
  asserts a fresh plain connection no longer produces org-default-shaped pods.
  Per-field client-GUC-over-org-default precedence and the
  AllowClientWorkerProfile-independence of org defaults are unit-covered
  (`controlplane/worker_profile_test.go`).
- **extension forks** — the bundled `ducklake`/`httpfs` extensions are the
  PostHog forks, not upstream (ported from the `*IsBundledFork` tests).
- **worker pods** — labels (`app`, `duckgres/control-plane`,
  `duckgres/worker-id`), securityContext (`runAsNonRoot`, uid 1000, no
  priv-esc), Downward-API `POD_NAME`/`NODE_NAME` env, and **no** ambient
  SA-token mount.
- **resilience** — worker-pod kill → crash recovery; DuckLake durability across
  a worker restart; concurrent writers (fork conflict-retry, the test that was
  flaking on main); graceful drain (a worker SIGTERM'd mid-query drains — the
  in-flight query completes correctly while the pod is Terminating — then retires
  cleanly; regression net for the worker drain protocol, #690); one session per
  worker (two concurrent queries for one org land on two distinct worker pods,
  never sharing a pod's DuckDB — workers run DUCKGRES_DUCKDB_MAX_SESSIONS=1 so a
  query owns the whole pod's resources). The org-at-max-workers clear error and
  the under-cap hold-for-spawn / FIFO anti-snatch paths are covered by unit tests
  (controlplane/org_reserved_pool_test.go, org_acquire_gate_test.go) — exercising
  them in-Job would need a dedicated max_workers=1 org and deterministic cold-spawn
  timing the shared cluster can't guarantee.
- **persistent user secrets** — `CREATE PERSISTENT SECRET` survives across
  sessions on both metadata-backend orgs (the CP stores the statement encrypted
  in the config store and replays it at session creation — worker pods are
  ephemeral); reserved system names (`ducklake_s3`, …) and unnamed persistent
  secrets are rejected; `DROP PERSISTENT SECRET` removes it durably; and a
  second user of the same org never sees the first user's secret, even on a
  reused hot-idle worker (cnpg lane).
- **isolation** — two tenants (cnpg vs ext) see distinct catalogs; a
  cross-tenant read is denied.
- **lifecycle** — deprovision → `warehouse=deleted` → the Crossplane Duckling
  CR **fully** deletes (`kubectl wait --for=delete`, asserting the finalizer
  cascade that drops the cnpg role+db completed). Same-id **re-provision** is
  *not* done in-Job: a clean slate needs DROPping a possibly-stranded cnpg role,
  which only `run.sh` (on the runner, with cnpg-shards exec) can do — so the
  stranded-cnpg-role regression (#649/#650/#11518/#11522) is covered **across
  runs** (`run.sh deploy` drops the role for a clean slate; `run.sh teardown`
  waits the CR `--for=delete`), not within one Job.

**Static-manifest asserts** (`k8s/rbac.yaml`, `k8s/networkpolicy.yaml`) that the
kind suite carried as unit tests now live in `tests/manifests/` and run in the
normal `go test ./...` lane.

### Deliberately not covered here

- **Mid-statement STS credential recovery (httpfs `v1.5.3-cred-refresh-write-retry`)** —
  the worker image bundles the PostHog httpfs fork patch that re-resolves the
  latest committed `ducklake_s3` secret and retries on ExpiredToken read/write
  auth failures, letting a statement outlive the STS token it started with.
  Proving real in-statement
  expiry in-Job needs a statement longer than the shortest AssumeRole token
  (900s AWS floor), which blows the Job time budget. The behavior is
  deterministically covered by the MinIO rotation tests in
  `tests/integration/credential_rotation_pin_test.go` (stock httpfs dies /
  patched httpfs survives rotation + revocation mid-scan); the harness's
  `assert_fork_extensions` pins that workers actually run the patched build
  (`EXPECT_HTTPFS_SHA`), and every DuckLake assertion here exercises the patched
  request paths against real S3.
- **Version-mismatch worker reaper** — needs a mid-run image bump, so it stays
  covered by `controlplane/` unit tests rather than in-Job.
- **Physical object-store-prefix isolation** — the Go suite listed the MinIO
  prefix to prove writes land only in a tenant's own path. Against real mw-dev
  S3 the Job holds no list creds, so isolation is asserted **logically** (the
  cross-tenant read is denied) rather than by enumerating S3 objects.
- **Cilium egress allow/deny probing** — asserting a worker reaches the cnpg
  pooler but not a denied destination needs a stable exec-into-worker probe;
  deferred (high flake risk). The policies themselves are asserted statically
  in `tests/manifests/`.
- **Oversized Bind-parameter rejection (#717)** — rejecting a Bind message whose
  declared parameter length exceeds the remaining message body requires crafting
  a malformed wire-protocol packet on a raw socket (through TLS + auth); libpq
  clients like psql always emit well-formed lengths, and the Job image carries no
  raw-packet tooling. Covered by the unit regression test in
  `server/conn_bind_test.go` instead.
- **Concurrent worker operations on one session** — the worker rejects
  overlapping same-session Flight operations with `FailedPrecondition`, but the
  harness enters through pgwire, where one client connection maps to one worker
  session and operations are serialized by the control plane. Driving this
  specific defense-in-depth path end-to-end would need a bespoke concurrent
  Flight-ingress client, so the rejection contract, required
  GetFlightInfo-to-DoGet handoffs, and abandoned-continuation cleanup are covered
  by `duckdbservice` unit tests. The harness still asserts the cluster invariant
  this protects: one active session owns one worker.
- **Worker DoGet close acknowledgement internals** — the harness covers the
  black-box pgwire behavior (CancelRequest then immediate same-session reuse),
  but not the exact internal wait point. Pausing the worker exactly after it
  observes gRPC cancellation but before it releases the session operation token
  would need a bespoke worker/Flight fault-injection client. Covered by
  `server/flightclient` and `duckdbservice` unit tests instead.
- **Malformed Bind message validation (#720)** — negative count/length fields
  in a Bind message must return a clean `08P01` instead of panicking. Every
  real client (psql, lib/pq, ...) only emits well-formed Bind messages, so
  triggering this needs a raw pgwire client that completes TLS + SCRAM auth
  and then sends crafted bytes — tooling the alpine Job image (psql/curl/jq)
  doesn't have. Covered by `server/conn_bind_test.go` unit tests
  feeding malformed payloads directly to `handleBind`.
- **STS credential freshness floor** (`stsCacheSafetyMargin` /
  `credentialRefreshLookahead`, `controlplane/sts_broker.go`) — the guarantee
  is temporal: every statement starts with ≥35min of STS token validity (at
  the default 1h session), and the refresh scheduler re-pushes worker secrets
  30min ahead of expiry. Asserting it in-Job would mean holding a query open
  across a real token-expiry boundary: even time-compressed via the env-only
  `DUCKGRES_STS_SESSION_DURATION` knob, AWS's 900s AssumeRole floor puts the
  shortest meaningful wait far past the Job budget, and the shared cluster
  gives no deterministic control over when the scheduler tick lands. Covered
  by `controlplane/sts_broker_test.go` +
  `shared_worker_activator_credentials_test.go` (cache margin, lookahead
  invariant, expiry surfacing on both activation paths) and the integration
  pin `tests/integration/credential_rotation_pin_test.go`, which proves
  against a real MinIO that on STOCK httpfs an in-flight scan does NOT
  survive credential rotation (DuckDB resolves secrets through the
  statement's MVCC snapshot, and scan-workload file opens skip the HEAD that
  could trigger httpfs' refresh-on-403). With the bundled
  `v1.5.3-cred-refresh-write-retry` fork build the floor is defense-in-depth rather than
  the only protection — see the mid-statement recovery bullet above.
- **PostHog product-analytics events** (`internal/analytics`,
  `warehouse_provision_begin`/`_success`/`_failed`,
  `warehouse_deprovision_begin`/`_success`/`_failed`, `warehouse_password_reset`,
  `query_initiated`/`query_failed`) — the harness already drives every path that
  fires these (it provisions, deprovisions, resets, and runs queries on both
  metadata backends), but the events are sent asynchronously to PostHog's
  external capture API and the in-cluster Job holds no PostHog query-API creds
  to read them back, so ingestion cannot be asserted in-Job. The emission logic
  (event name, org group-analytics attribution, properties, failure-category
  classification, and "no event on handler failure") is covered by
  `internal/analytics/analytics_test.go`,
  `controlplane/provisioning/analytics_events_test.go` (the `_begin` admin-API
  events), `controlplane/provisioner/controller_analytics_test.go` (the
  terminal `_success`/`_failed` events the provisioner controller emits on the
  Ready/Failed/Deleted transitions), and `server/conn_analytics_test.go`.

## Isolation model

Dedicated CP + throwaway config-store **per PR**, provisioning **real**
ducklings (org IDs `ci-pr-<N>-cnpg`, `ci-pr-<N>-ext`, plus the
ducklake-only resilience-lane orgs `ci-pr-<N>-res1`/`-res2`) through the **shared**
Crossplane / cnpg-shards / external RDS infra. The config-store
uses a namespace-scoped PVC so a pod recreation during the harness does not
erase provisioned org rows. Everything PR-specific lives in the namespace and
is deleted; the shared-infra footprint is removed by deprovisioning the
ducklings first.

Each deploy first reaps any existing resources for the same PR number (namespace,
Duckling CRs, pod identity association, cross-namespace bindings, and cnpg role).
It fails before applying manifests if the same-PR Duckling CRs do not fully
delete, so a rerun never reuses PR-scoped bucket/org names while Crossplane
finalizers are still running.

## One-time repo configuration

| kind | name | purpose |
|---|---|---|
| var | `TS_WIF_CLIENT_ID_MW_DEV` | Tailscale OAuth WIF client (mirror of hogland's) |
| var | `TS_WIF_AUDIENCE_MW_DEV` | Tailscale WIF audience |
| secret | `MW_DEV_ACCOUNT_ID` | mw-dev AWS account id (kept out of committed code; ARNs are built from it) |
| secret | `AWS_ECR_PUBLISH_IAM_ROLE` | ECR push (already exists; used by CD) |
| (role) | `github-duckgres-e2e` | dedicated stripped role in the mw-dev account (posthog-cloud-infra) — `eks:DescribeCluster` + Pod Identity association calls + `iam:PassRole`/`iam:GetRole` on the CP role + an EKS access entry for kubectl. The workflow assumes `arn:aws:iam::<MW_DEV_ACCOUNT_ID>:role/github-duckgres-e2e`. |
| repo setting | "Require approval for all outside collaborators" | the access gate (see below) |

The Tailscale tailnet ACL must allow `tag:github-runner` to reach the mw-dev
VPC subnet router (same pattern hogland set up for its dev cluster).

## Access control — "external people cannot run this"

Same model as the AWS/OIDC job in `ci.yml`: the gate is the repo setting
**"Require approval for all outside collaborators"**. Members' PRs run
automatically; fork PRs from outside collaborators get no secrets and don't run
until a maintainer clicks *approve-and-run*, so they can't reach the cluster or
assume the IAM role unapproved.

No per-workflow guard job or required-reviewer Environment: a guard on
`author_association` would block external PRs *even after a maintainer approves*
(the opposite of the intent), and a required-reviewer Environment would force an
approval click on every maintainer push. The repo setting gives exactly
"members auto, outsiders need approval".

## Validated locally against mw-dev (dry-run with the shipped `duckgres` image)

Running `run.sh` from a laptop on the VPN (kubectl `--context posthog-mw-dev`)
got through, in order — each was a real fix:
1. ✅ deploy — namespace, throwaway config-store, CP, cross-ns RBAC all apply.
2. ✅ worker boot — needed `data_dir: /data` in the worker ConfigMap (the CP
   factory mounts an emptyDir at `/data`; default `./data` → `mkdir
   data/extensions: permission denied` and the worker exits 1).
3. ✅ CP secret reconciler — needed `list`/`watch` on secrets in the Role.
4. ✅ SNI routing — the CP rejects non-SNI connections
   (`this server requires connecting via <org-id>.<managed-suffix>`). Fixed by
   `DUCKGRES_MANAGED_HOSTNAME_SUFFIXES=.ci.duckgres.local` +
   `DUCKGRES_SNI_ROUTING_MODE=passthrough`, and connecting with libpq
   `host=<org>.<suffix>` (SNI) + `hostaddr=<CP ClusterIP>` (TCP).
5. ✅ catalog selection — `dbname` must be `ducklake`, not the org
   (PR #651: *database = catalog selection*). harness.sh now does this.

6. ✅ **activation (cnpg DuckLake)** — the blocker was NOT a metadata
   SecretRef; the duckling-status path reads the metadata password straight
   from the CR. It failed at **S3 STS brokering** because the isolated CP had no
   AWS identity. Fixed by binding the per-PR `duckgres` SA to the **same EKS Pod
   Identity role the real mw-dev control plane uses**
   (`duckgres-control-plane-dev`), via `aws eks create-pod-identity-association`
   in `run.sh` deploy (deleted in teardown). With it, the CP brokers per-duckling
   S3 creds exactly like prod: validated the warehouse reaches `ready`, the pod
   gets the EKS creds endpoint, and a psql session authenticates + attaches the
   DuckLake catalog. (Full CREATE/INSERT/SELECT reconfirm was interrupted by a
   VPN drop; the activation path itself is proven.)

The CP pod-identity role is `role/duckgres-control-plane-dev` in the mw-dev
account; the workflow builds the ARN from `secrets.MW_DEV_ACCOUNT_ID` (no account
id committed).

## Other open items

- **`github-duckgres-e2e` role (posthog-cloud-infra).** AWS policy:
  `eks:DescribeCluster`, `eks:{Create,List,Delete,Describe}PodIdentityAssociation`
  on the cluster + its associations, and `iam:PassRole` on
  `duckgres-control-plane-dev`. Trust: `repo:PostHog/duckgres:*`. Plus an EKS
  **access entry** binding the role to k8s RBAC that can create namespaces, the
  cross-namespace bindings, and the in-namespace resources (the kubectl the
  harness runs). Scope as tightly as the cluster admins allow — deliberately
  NOT the account-admin `github-terraform-infra-role`.
- **Don't hammer auth.** The CP rate-limiter bans the source IP after a few
  failed auths (~15 min). The harness uses the provision-time password and
  settles one config-poll interval before connecting — keep it that way; a
  reset-password + tight retry loop will trip the ban.
- **Teardown / recreate are now CR-synchronous.** `run.sh deploy`, `run.sh
  teardown`, and the in-harness same-org recreate all `kubectl wait --for=delete`
  on the Duckling CR, whose finalizers run the Crossplane DROP of the cnpg
  role+db, before returning / re-provisioning. Deploy and teardown fail if the
  same-PR CRs do not delete within the timeout; the scheduled `e2e-cleanup` sweep
  logs a narrow stuck-CR summary but keeps going so one old namespace does not
  block the janitor. `drop_cnpg_role` is still called at deploy + at teardown as a
  belt-and-suspenders idempotent backstop; it sweeps the `mdstore_<org>`
  identifiers. (Composition `managementPolicies:
  ["*"]` from charts#11522 does the drop; the `--for=delete` wait is what makes
  it synchronous from our side.)
- **Shared-infra contention.** Concurrent PRs provision real ducklings against
  the same cnpg-shards / RDS infra. Org-ID prefix keeps them
  distinct; watch quay.io / cnpg pooler / RDS limits under parallelism.
- **e2e-cleanup** is wired: the `e2e-mw-dev.yml` `schedule` trigger runs
  `run.sh e2e-cleanup` every 6h, reaping `duckgres-ci-pr-*` namespaces older than
  6h (`E2E_CLEANUP_MAX_AGE_HOURS`) along with their ducklings, cnpg role+db, Pod
  Identity association, and ci-pr-labelled cross-ns bindings.
- **Remaining deferrals** are listed under "Deliberately not covered here"
  above (warm-pool activation + version-reaper, physical S3-prefix isolation,
  Cilium egress allow/deny probing).

## Local dry-run

Needs VPN to the private mw-dev API and `AWS_PROFILE=mw-dev` for the `aws eks`
pod-identity calls. Both images point at the same all-in-one ref.

```sh
IMG=<ecr>/duckgres:<tag>
AWS_PROFILE=mw-dev \
NAMESPACE=duckgres-ci-pr-0 PR_NUMBER=0 KUBE_CONTEXT=posthog-mw-dev \
  WORKER_IMAGE=$IMG CONTROLPLANE_IMAGE=$IMG \
  CP_POD_IDENTITY_ROLE=arn:aws:iam::<mw-dev-account-id>:role/duckgres-control-plane-dev \
  bash tests/e2e-mw-dev/run.sh deploy
```
