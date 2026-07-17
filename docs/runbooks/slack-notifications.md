# Slack notifications

Use this runbook to enable and troubleshoot Duckgres operational notifications
to Slack. Notifications are best-effort and must never block signup,
provisioning, deprovisioning, or admin requests.

## Configuration

Set these environment variables on the control-plane process:

```bash
DUCKGRES_SLACK_WEBHOOK_URL=<slack-incoming-webhook-url>
DUCKGRES_NOTIFICATION_HASH_KEY=<random-secret-for-org-correlation>
DUCKGRES_NOTIFICATION_QUEUE_SIZE=100
DUCKGRES_NOTIFICATION_TIMEOUT=2s
```

`DUCKGRES_SLACK_WEBHOOK_URL` is required to enable Slack delivery. The queue size
and timeout are optional; the values above are the defaults. The hash key is
optional; set it only if Slack messages need a stable org correlation token.

Shutdown does not drain the in-memory notification queue. This is intentional:
notifications remain best-effort during process exit just as they are on request
paths.

Do not put the Slack webhook URL in `duckgres.yaml`, committed examples, PR
descriptions, issue comments, or test fixtures. Treat it as a secret.

## Payload policy

Slack messages intentionally omit raw customer and internal identifiers. They
include:

- Event name, such as `org_created` or `warehouse_provision_failed`
- `org_ref=hmac-sha256:<prefix>`, a short keyed HMAC of the org ID, only when
  `DUCKGRES_NOTIFICATION_HASH_KEY` is set
- Low-risk allowlisted properties: `source`, `metadata_store`,
  `ducklake_enabled`, and stable failure `reason`

They do not include raw org IDs, customer names, database names, endpoints, SQL
text, credentials, secret names, bucket names, or hostnames.

## Local smoke test

1. Create a temporary Slack incoming webhook in a non-production Slack channel.
2. Run the control plane with `DUCKGRES_SLACK_WEBHOOK_URL` set.
3. Trigger `POST /api/v1/orgs/<test-org>/provision` with a throwaway org ID and
   placeholder metadata.
4. Confirm Slack receives `org_created` and `warehouse_provision_begin`.
5. Confirm the message does not contain the raw org ID or database name. If
   `DUCKGRES_NOTIFICATION_HASH_KEY` is set, confirm it contains only the HMAC
   org reference.

## Failure recovery

- No Slack messages: confirm `DUCKGRES_SLACK_WEBHOOK_URL` is set on the running
  control-plane pod or process, then restart/roll the process if the env var was
  added after boot.
- Webhook revoked or wrong channel: rotate the Slack webhook URL in the secret
  manager or deployment secret, then roll the control plane.
- Slow or failing Slack delivery: Duckgres logs delivery failures at debug level
  and increments `duckgres_notification_delivery_failures_total` while keeping
  requests flowing. Check the webhook URL, Slack app status, and outbound egress
  policy.
- Dropped notifications: increase `DUCKGRES_NOTIFICATION_QUEUE_SIZE` if bursts
  exceed the default queue. Watch `duckgres_notifications_dropped_total`.
  Dropping is preferred over blocking signup or admin paths.

Do not paste production org IDs, customer names, internal endpoints, or webhook
URLs into public issues, PRs, docs, or committed artifacts while debugging.
