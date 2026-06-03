# mw-dev per-PR e2e harness

Replaces what `tests/k8s/` (kind) cannot exercise — real Cilium network
policies, real Crossplane Duckling provisioning, real cnpg-shard + external
RDS metadata stores, and the real per-org Lakekeeper operator. Those are the
layers where this quarter's production bugs lived.

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
   **cnpg + ext** (aurora out of scope).
5. **Teardown** always: deprovision the ci-pr ducklings (clean shared-infra
   footprint) then delete the namespace.

## Isolation model

Dedicated CP + throwaway config-store **per PR**, provisioning **real**
ducklings (org IDs `ci-pr-<N>-cnpg`, `ci-pr-<N>-ext`) through the **shared**
Crossplane / cnpg-shards / external RDS / Lakekeeper operator. Everything
PR-specific lives in the namespace and is deleted; the shared-infra footprint
is removed by deprovisioning the ducklings first.

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
5. ✅ catalog selection — `dbname` must be `ducklake` or `iceberg`, not the org
   (PR #651: *database = catalog selection*). harness.sh now does this.

6. ✅ **activation (cnpg DuckLake + Iceberg)** — the blocker was NOT a metadata
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
- **Teardown is async-incomplete.** `run.sh teardown` deprovisions and waits on
  warehouse `state=deleted` (= Duckling CR deleted), but the downstream
  Crossplane DROP of the cnpg role+db lags behind that — observed a stranded
  `lakekeeper_ci_pr_<N>_cnpg` role after teardown returned. Either wait on the
  cnpg role/db actually being gone, or rely on the janitor below. (The
  composition `managementPolicies: ["*"]` from charts#11522 does drop them;
  it's just not synchronous with the CR delete.)
- **Shared-infra contention.** Concurrent PRs provision real ducklings against
  the same cnpg-shards / RDS / lakekeeper-operator. Org-ID prefix keeps them
  distinct; watch quay.io / cnpg pooler / RDS limits under parallelism.
- **Deep coverage stubbed.** Durability (worker-pod kill), concurrency, and
  network-policy assertions are `SKIP (TODO)` — activation (#6) is cleared, so
  these are the next layer to flesh out.
- **Janitor.** Periodic sweep of stale `duckgres-ci-pr-*` namespaces + their
  ci-pr-labelled cross-ns bindings + orphaned ducklings/cnpg roles, to back up
  the always() teardown for runs that die hard. Not yet added.

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
