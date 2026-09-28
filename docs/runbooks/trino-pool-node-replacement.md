# Shared-pool node replacement

This optional controller replaces a shared-pool Trino instance before its original
compute is removed for a voluntary infrastructure change. It does not change legacy
Trino, intercept SIGTERM, expire transactions, or convert failure into successful drain.

## Enable safely

1. Deploy the Duckgres image and migration 000043 with
   `DUCKGRES_TRINO_POOL_NODE_DISRUPTION_ENABLED=false` (the default).
   Wait for every control-plane replica to run that image before enabling the
   chart flag. An older leader cannot honor the new replacement evidence.
2. Review the required permissions: namespace-scoped pod `get/list/patch`,
   ReplicaSet `get`, existing Deployment reads, cluster-scoped Node `get/patch`,
   and NodeClaim `get/list`. Kubernetes RBAC cannot restrict dynamic Node patch
   permission by label. Code checks exact workload ownership and Node/NodeClaim
   UIDs before every cordon. Grant this only to the pool operator.
3. Add `karpenter.sh/do-not-disrupt: "true"` to both blueprint pod templates and
   enable the environment variable alongside the existing pool/operator flags.
   The source binary alone does not inject this annotation with the feature off.
4. Confirm every live coordinator and worker has the annotation before testing
   disruption. Existing immutable Deployments are not rolled to add it: the
   controller patches only the metadata of their exact owned pods.
5. In development, run the shared-pool E2E harness with
   `E2E_TRINO_POOL_NODE_PROTECTION=1`. This checks live pod protection, not an
   actual EC2/Karpenter replacement. A controlled isolated replacement test is
   still required before claiming end-to-end zero-loss infrastructure rotation.

Do not enable a finite NodeClaim `terminationGracePeriod`. Karpenter can override
pod protection after that deadline. The controller rejects these claims rather
than pretending their running work is protected indefinitely.

## What happens

The leader protects all live instances before advancing the lifecycle. Pods must
belong to the recorded Deployment through an exact ReplicaSet UID chain; labels
alone do not establish ownership. A protection pass has a 30-second budget and
rotates its starting instance. Individual Kubernetes calls use a 10-second budget.
One paginated NodeClaim inventory is shared within the pass, capped at 10,000
claims; errors and incomplete inventories never prove drift.

The first exact Node/NodeClaim identity with `Drifted=True`, or a deletion timestamp,
is recorded once in `node_replacement_evidence`. Later observations cannot rewrite
that request. The node is then cordoned with UID and resource-version preconditions
and the `posthog.com/trino-node-retirement` annotation. This excludes replacement
pods from the old node without removing its running pods. Cross-system fencing is
not atomic: the database authorizes the intent and Kubernetes rejects a changed
node identity or resource version. A delayed cordon is an idempotent scheduling
exclusion, never permission to delete an instance.

The existing planner creates and admits a replacement using the normal surge slot,
then drains the original. It never uses repair capacity merely because a node
drifted. With `max_surge=0`, replacement waits safely. The Gateway keeps queries,
transactions, and result obligations on the original instance until they finish.
Only normal retirement authorization permits UID-scoped deletion of its resources.

New candidates on drifted, deleting, unschedulable, or unverifiable nodes cannot
be admitted. Admission whose response was lost still replays the same Gateway
request; a concurrent successful admission cannot be deleted as a failed candidate.

API errors pause voluntary replacement. Missing or pending worker inventory is
normal convergence for a new candidate, but blocks voluntary replacement of a
serving instance. A candidate with definitively invalid placement fails through
the guarded candidate-retirement path, freeing its slot after cleanup. A
candidate waiting for scheduling or an unavailable API retains its slot and retries.
Health detection, explicit administrative recovery, and irreversible retirement
continue through their existing guards. A node lookup failure is neither drift
evidence nor proof that the admitted process died.

## Observe and recover

The first accepted request emits `Trino pool requested voluntary node replacement`
with the instance, Node UID, NodeClaim UID, and reason. The authenticated instance
inventory exposes a typed, read-only `node_replacement` object. Existing blocked
plan messages explain an exhausted surge slot or serving-floor constraint.

Recorded replacement is deliberate: it completes even if drift later clears.
Disabling the feature stops new observation, protection, and cordons, but does not
cancel an already-recorded replacement. Do not clear its database evidence to
reuse the old instance.

The controller never uncordons nodes. Normally Karpenter removes the emptied node.
If drift clears and unrelated workloads keep it occupied, an owned cordon may
remain. Before manually uncordoning, inspect the exact Node UID, retirement
annotation, matching NodeClaim UID, and all managed instances on that node. Confirm
their recorded replacements have retired. Do not undo an unrelated administrator's
cordon or assume that one retired instance proves all co-located work has left.

## Guarantee boundary

Karpenter 1.9.0's [eviction eligibility](https://github.com/kubernetes-sigs/karpenter/blob/v1.9.0/pkg/utils/pod/scheduling.go)
excludes protected pods but still waits for them in its drain accounting.
Its [termination controller](https://github.com/kubernetes-sigs/karpenter/blob/v1.9.0/pkg/controllers/node/termination/terminator/terminator.go)
only applies forced pod expiry when a node termination deadline exists. Therefore
ordinary expiration with no NodeClaim termination grace period can use the same
protected deletion-observation path; no NodePool expiry change is required.

Hardware failure, interruption deadlines, force deletion, kubelet failure, direct
pod deletion, and finite node termination deadlines remain outside the zero-loss
claim. Protection cannot recover an already-dead coordinator or worker. Do not
trigger drift on shared nodes to validate this feature; use an isolated approved
test pool and retain client results and transaction evidence throughout the test.
