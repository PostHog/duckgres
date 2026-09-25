# Trino pool administrative recovery

Administrative recovery retires one draining instance as a **failure**, then lets the pool operator replace it.
It can invalidate query continuations and retained results, including results from queries that already finished.
It is not a clean drain and does not silently clear query accounting.
Use it only with explicit authorization for the exact instance and this loss of results.

## Prerequisites

- Deploy the recovery-capable control plane and verify the active pool leader uses that version before submitting a request.
  An older leader does not process recovery intents.
  Complete the rollout before use so leadership cannot move back to an older replica.
- Verify Gateway supports destructive loss evidence and failed retirement claims.
- Use the authenticated admin API with an admin role.
  Viewer access cannot preview or request recovery.
- Independently check current coordinator workload, pending requests, open transactions, retained-result impact, and the readiness and capacity of the other serving instances.
  The preview reads stored lifecycle state only; it does **not** establish that there is no live work or that the remaining instances are healthy.
  Obligation checks are point-in-time observations, not an atomic guarantee against a later client continuation.
  The explicit override authorizes invalidating all retained work on the exact instance, including work that races with these checks.
- Confirm the selected pool and instance, including the exact coordinator process identity.
  Do not use a logical pool name in place of an individual instance ID.

## Use the admin console

Open **Trino cell**, select the logical cell, and find **Instance recovery**.
This section requires an admin role. Select an individual instance to load its
stored-state preview. The list excludes terminal instances and does not report
live workload or health.
Automatic inventory and preview polling stops when the API returns `404` or
`503`. Use the corresponding refresh button after recovery becomes available.
Authentication and permission failures also stop polling; restore access first.

After selecting an instance, the recovery action remains visible when it cannot
be used. The preview lists each failed prerequisite: a phase other than
`DRAINING`, a frozen pool, missing or invalid admitted identity fields, or a
stored serving count below the minimum. The capacity message shows both counts.
A draining phase alone does not make an instance eligible. Do not bypass the
minimum by editing lifecycle records. Restore sufficient serving capacity before
requesting recovery. Existing requests and retired instances show their status
with the new-request action disabled.

### Restore capacity when several instances are draining

Deploy Gateway support for capacity-deficit repairs of departing members before
deploying the corresponding Duckgres planner. The operator creates one candidate
at a time, validates it normally, and records a distinct replacement target for
each live repair. Draining instances retain their work and are not deleted to
make room. A pending candidate retains its original repair target if that target
advances through draining or retirement while registration is retried.

Size the configured repair budget explicitly for the simultaneous failures you
intend to cover. For example, four pinned draining instances and three required
serving instances need seven live slots: desired three, surge one, and repair
three. A repair budget of one cannot restore that capacity without first
retiring some pinned instances. Increase the budget through reviewed desired
configuration, not by changing lifecycle rows or lowering the minimum. The
planner stops at the configured live and repair limits.

Repair instances remain charged to the repair budget while serving. Retiring
their targets does not currently convert them into ordinary instances. Budget
normalization requires coordinated Gateway and control-plane work and remains
a separate TODO. Do not clear repair flags manually.

After the required serving count is restored, use the existing recovery process
below for each remaining stuck drain. Pending requests, open transactions,
identity checks and explicit destructive authorization still apply.

Before submitting, complete the independent checks above, enter a short reason,
type the exact instance ID, and acknowledge both those checks and the potential
loss of results. The confirmation applies to the displayed process identity and
generation, not merely to a reusable coordinator name. A changed snapshot requires
new confirmation; the UI explains when it resets the confirmation fields.
Background preview polling does not prevent editing those fields. Submission
requires a fresh preview, and an expired preview requires an explicit refresh.

The UI uses the same recovery API documented below; it does not bypass its guards.
An accepted request is immutable. The progress view follows the original request
even after the instance disappears from the active list. Terminal retirement is
not evidence that its replacement is already serving.
After acceptance, the UI shows progress without a retry button.

If the response is lost, use the status check before retrying. The UI preserves
the original operation and payload for an identical retry. Do not create a new
operation to bypass a conflict. Consult the API procedure below if browser state
is unavailable.

## Preview without changes

`GET /api/v1/trino/instances?cell=<configured-cell-id>` lists nonterminal
instances for selection. It returns only each instance ID, local phase, Gateway
state, and phase timestamp. Legacy cells have no shared-pool inventory.

Read `GET /api/v1/trino/instances/<instance-id>/recovery?cell=<configured-cell-id>` through the existing authenticated admin connection.
The `cell` parameter is the configured public pool ID shown by the admin API.
The response contains a sanitized instance snapshot, stored serving counts and floor, and any existing recovery request.
It excludes blueprint contents, credentials, and coordinator endpoints.
`live_work_verified` is always false.

Record these fields from `instance`:

- `expected_generation`
- `incarnation`
- `pod_uid`
- `boot_id`
- `node_id`
- `coordinator_id`

The preview is read-only and does not reserve the instance or authorize an operation.
Repeat it immediately before submitting a request.

## Submit an authorized recovery

POST to the same URL with an independently generated, unique `operation_id`, the exact snapshot fields, a short reason, and `destructive_authorization: true`.
Use at most 128 characters from `A-Z`, `a-z`, `0-9`, `_`, `.`, `:`, and `-` in the operation ID:

```json
{
  "operation_id": "recovery-example-unique-id",
  "expected_generation": 7,
  "incarnation": "incarnation-from-preview",
  "pod_uid": "pod-from-preview",
  "boot_id": "boot-from-preview",
  "node_id": "node-from-preview",
  "coordinator_id": "coordinator-from-preview",
  "reason": "Approved retirement after independent workload and capacity checks",
  "destructive_authorization": true
}
```

Do not send `requested_by`; the server binds the request to the authenticated administrator.
Do not include passwords, SQL, query results, or customer details in the reason.
A `202 Accepted` response means durable intent was recorded, not that resources were deleted or the replacement is ready.
No Gateway or Kubernetes mutation runs in the HTTP handler.

The store accepts a new request only for a draining instance with a matching identity and generation and sufficient stored serving capacity.
The active pool leader executes the request under its normal fencing and journaling, coordinates Gateway and local lifecycle state, obtains a failed-retirement claim, and only then deletes the owned resources.
Before the first Gateway transition, the operator checks that the live coordinator matches the approved process identity.
After the operation has started, it resumes the same recorded transitions even if that process exits; deletion remains guarded by the original resource identities.
A changed identity before the operation starts fails closed.
There is no cancellation or amendment endpoint for an accepted request.
The failure classification and durable authorization remain recorded.
A clean drain can win concurrently before the destructive transition.
In that case the operator preserves the clean drain and completes its existing retirement instead; the local terminal phase is `RETIRED` and the Gateway retirement kind is `DRAINED`.

## Follow progress and handle interruption

Repeat GET on the same URL to inspect the stored phase, Gateway state and generation, and original request.
`instance.last_error` contains a fixed, non-sensitive message when recovery is blocked; inspect the control-plane logs for details.
Verify the instance reaches `FAILURE_RETIRED` (or `RETIRED` if a clean drain won), its owned resources are absent, and a replacement reaches `SERVING` on the intended release.
Also verify the surviving instances stay healthy and the blocked rollout resumes.

If submission times out, repeat GET first.
If the original request exists, repeat the identical POST only when needed; keep its operation ID and full original snapshot unchanged.
A retry of an accepted intent remains the same operation even after lifecycle progress changes the current snapshot.
A changed operation ID, actor, reason, or identity is not an idempotent retry.

A `409 Conflict` means the intent or current state cannot be accepted safely.
Read a fresh preview and investigate; do not automatically replace the snapshot and resubmit authorization.
A `503 Service Unavailable` means this API deployment has no recovery-store capability.
Unexpected server errors require checking the control-plane logs and durable operation state before retrying.

Leader changes or a crash between Gateway and local writes must resume the recorded operation.
Do not hand-edit Gateway or Duckgres rows, borrow another leader's epoch, cancel queries as a substitute for recovery, or delete pods independently.
If progress stops, diagnose the recorded operation and ownership fences; do not bypass them.

### Resolve an ambiguous conflict

If a submission has an unknown outcome and an identical retry returns `409`,
stop retrying and preserve the original operation ID and payload. A conflict
does not cancel an earlier request or prove that no request can still commit.
For example, a retry can reach the server before the original submission while
the pool has insufficient capacity; the original can arrive after capacity recovers.

Use **Refresh preview** to check the recorded request and current identity.
If a request exists, follow that recorded operation. If none appears, inspect
the control-plane logs and durable operation state using the original operation
ID, cell, and instance. Confirm the original submission's outcome before deciding
whether another authorization is appropriate. Use the API procedure above only
after that investigation; it is not a bypass for an unresolved submission.
Do not clear browser storage or create a new operation merely to remove the
conflict warning. The UI deliberately preserves uncertainty across reloads.

## Rollback

Finish accepted recoveries before rolling the control-plane binary back.
An older leader does not understand pending intents and can leave a partially completed recovery waiting.
If recovery was interrupted, roll forward and resume the same immutable request.
Do not downgrade the recovery migration after it has been used; retain its authorization and audit records.

## Local validation

Run `just test-trino 'TestTrinoRecovery|TestTrinoPoolRecovery'` for the handler and operator behavior.
Run the config-store integration tests with the local test database configured to verify atomic acceptance and conflicting retries.
Production verification and recovery authorization are separate from these local tests.
