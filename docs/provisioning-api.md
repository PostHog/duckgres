# Provisioning API

## Quick start

### 1. Set up credentials

```bash
# Get the internal secret
SECRET=$(kubectl --context arn:aws:eks:us-east-1:645773004826:cluster/posthog-mw-prod-us \
  -n duckgres get secret duckgres-tokens \
  -o jsonpath='{.data.internal-secret}' | base64 -d)

# API host (external)
HOST="http://k8s-duckgres-duckgres-e9776fa212-8471473077b8baa1.elb.us-east-1.amazonaws.com"

# NLB host (for psql connections)
NLB="k8s-duckgres-duckgres-ef6c9dc84f-74e43f553f1ea047.elb.us-east-1.amazonaws.com"
```

### 2. Check database name availability

```bash
curl -s "$HOST/api/v1/database-name/check?name=acme-analytics" \
  -H "X-Duckgres-Internal-Secret: $SECRET"
# {"name":"acme-analytics","available":true}
```

### 3. Provision a warehouse

```bash
curl -X POST "$HOST/api/v1/orgs/550e8400-e29b-41d4-a716-446655440000/provision" \
  -H "X-Duckgres-Internal-Secret: $SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "database_name": "acme-analytics",
    "metadata_store": {
      "type": "aurora",
      "aurora": {"min_acu": 0, "max_acu": 2}
    }
  }'
```

### 4. Poll until ready (~5-15 min)

```bash
curl -s "$HOST/api/v1/orgs/550e8400-e29b-41d4-a716-446655440000/warehouse/status" \
  -H "X-Duckgres-Internal-Secret: $SECRET" | python3 -m json.tool
```

### 5. Create a user

```bash
curl -X POST "$HOST/api/v1/users" \
  -H "X-Duckgres-Internal-Secret: $SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "mypassword",
    "org_id": "550e8400-e29b-41d4-a716-446655440000"
  }'
```

### 6. Connect

```bash
psql "host=$NLB port=5432 dbname=acme-analytics user=testuser sslmode=require"
```

### 7. Deprovision

```bash
curl -X POST "$HOST/api/v1/orgs/550e8400-e29b-41d4-a716-446655440000/deprovision" \
  -H "X-Duckgres-Internal-Secret: $SECRET"
```

---

## API reference

All endpoints require the `X-Duckgres-Internal-Secret` header.

### `GET /api/v1/database-name/check?name=<name>`

Check if a database name is available.

**Response:** `{"name": "acme-analytics", "available": true}`

### `POST /api/v1/orgs/:id/provision`

Provision a warehouse for an org. The org is auto-created if it doesn't exist.

**Body:**

| Field | Required | Description |
|-------|----------|-------------|
| `database_name` | yes | Globally unique name users connect with via `dbname=` |
| `metadata_store.type` | yes | `"aurora"` |
| `metadata_store.aurora.min_acu` | no | Aurora Serverless min capacity (default 0 = scale to zero) |
| `metadata_store.aurora.max_acu` | yes | Aurora Serverless max capacity (must be > 0) |

**Responses:**

| Status | Meaning |
|--------|---------|
| 202 | Provisioning started |
| 400 | Missing `database_name` or invalid `max_acu` |
| 409 | Warehouse already exists in non-terminal state |

### `GET /api/v1/orgs/:id/warehouse/status`

Get current provisioning status.

**Response:**

```json
{
  "org_id": "550e8400-...",
  "state": "ready",
  "status_message": "Infrastructure ready",
  "s3_state": "ready",
  "metadata_store_state": "ready",
  "identity_state": "ready",
  "secrets_state": "ready",
  "ready_at": "2026-03-25T16:02:01Z"
}
```

**State machine:**

```
pending → provisioning → ready
                       → failed → (retry) → pending
ready → deleting → deleted
failed → deleting → deleted
```

| Status | Meaning |
|--------|---------|
| 200 | Warehouse found |
| 404 | No warehouse for this org |

### `POST /api/v1/orgs/:id/deprovision`

Tear down all AWS infrastructure. Warehouse must be in `ready`, `failed`, or `provisioning` state.

| Status | Meaning |
|--------|---------|
| 202 | Deprovisioning started |
| 404 | No warehouse for this org |
| 409 | Warehouse not in a deprovisionable state |

### `POST /api/v1/users`

Create a user scoped to an org. Usernames are unique within an org but can be reused across orgs.

**Body:**

| Field | Required | Description |
|-------|----------|-------------|
| `username` | yes | Username for psql connections |
| `password` | yes | Password (stored as bcrypt hash) |
| `org_id` | yes | Org identifier |

| Status | Meaning |
|--------|---------|
| 201 | User created |
| 400 | Missing required fields |
| 409 | Username already exists in this org |
