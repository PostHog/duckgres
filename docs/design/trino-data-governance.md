# Trino Data Governance — Design

Status: **proposed**. How fine-grained data governance works for the multi-tenant Trino
cells, and why the control plane governs by **authoring policy** rather than by sitting in
the query path.

Today a cell enforces tenant isolation: an org can see and query its own catalog and
nothing else, proven end-to-end with two live tenants. That is a floor, not a governance
story. This describes how to get from "one org, one catalog" to "this user sees these
columns of these tables, and only these rows" — without a proxy.

## Decision: the control plane is NOT in the query path

Clients connect straight to a coordinator. The control plane creates the catalog, projects
`password.db` / `group.db`, serves the OPA bundle and writes resource groups — then steps
out. Switch it off and existing tenants keep querying.

That is the opposite of duckgres pgwire, where the control plane terminates the connection
and routes by SNI. The asymmetry is deliberate: duckgres multiplexes every org onto one
endpoint and *needs* to read the SNI to know where to send bytes. Trino identifies the
tenant from the login credential, so a proxy would learn nothing the engine does not
already know.

**A proxy would also make governance worse, not better.** A proxy sees SQL text. It does
not see what the text resolves to — what `SELECT *` expands to, which base tables a view
reads underneath, what a chain of CTEs actually touches. Enforcing column- or row-level
rules there means re-implementing Trino's analyzer, and every gap between the two
implementations is a bypass.

Trino's access-control SPI is called by the engine **after analysis**, with resolved
objects. That is the only place these decisions can be made correctly.

## What the engine already asks OPA

The OPA plugin is wired for far more than the catalog-level checks the current policy
answers:

| SPI call | Governs |
|---|---|
| `checkCanSelectFromColumns(table, columns)` | column-level authorization, post-expansion |
| `getRowFilters(context, table)` | row-level security |
| `getColumnMasks(context, table, columns)` | per-column redaction / hashing |
| `filterCatalogs` / `filterSchemas` / `filterTables` / `filterColumns` | what is *visible*, distinct from what is readable |
| `ViewQueryOwnedBy` / `FilterViewQueryOwnedBy` / `KillQueryOwnedBy` | who sees and controls whose queries |

Row filters and column masks are returned as SQL expressions
(`OpaViewExpression{expression, identity}`) which Trino injects into the query plan. A
filter of `team_id = 42` becomes part of the plan — there is no query shape that evades
it.

Both are opt-in per coordinator and currently unset:

```
opa.policy.row-filters-uri        # unset -> no row filtering applied
opa.policy.column-masking-uri     # unset -> no masking applied
opa.policy.batch-column-masking-uri
```

## The actual gap: identity, not architecture

Policy can only be as fine-grained as the identity it receives. Today every org is a
**single** Trino principal — its `database_name` — authenticated against a bcrypt hash in
`password.db` that is shared by everyone at that org. So "which user sees what" cannot be
expressed at all; there is only one user.

Nothing about the request path prevents finer rules. The missing piece is per-human
identity.

### Phase 1 — per-user identity

Move client authentication from the shared `password.db` entry to OIDC/JWT, so each human
arrives as a distinct principal carrying group claims. The org principal remains for
service-to-service use.

This is also what fixes a live operational wart: today the only Trino credential is the
org's **pgwire root password**, so handing someone Trino access means either sharing a
production credential or rotating it out from under whatever else uses it. Minting a
scoped, expiring Trino credential should be a control-plane call that does not disturb
pgwire — the existing `POST /orgs/:id/service-credentials` mint is the natural home, since
it already returns `{credential_id, credential_secret, expires_at, connect}`.

### Phase 2 — grants in the config store

The control plane already owns the OPA bundle; extend what it puts there. A grant model
roughly:

```
principal (user or group)
  → catalog / schema / table
  → allowed columns          (or denied columns)
  → row predicate            SQL, e.g. "team_id IN (42, 43)"
  → column masks             per column, e.g. "'***'" or "sha256(email)"
```

The provisioner renders these into `data.json` alongside today's `group_catalogs`, the
bundle ships on its existing poll, and no coordinator restarts. Same distribution
mechanism that already carries tenant isolation.

### Phase 3 — policy rules

`allow` gains table- and column-scoped rules; new `row_filters` and `column_masks`
entrypoints answer the two new URIs. The existing `batch` rule keeps working unchanged
because it is defined by re-evaluating `allow`, so anything added there is honoured in
batched filtering automatically.

## Audit

Governance needs a record, and Trino emits one natively — the event-listener SPI reports
every query with its resolved tables and columns. The chart already supports an
`eventListener` block, so query events can be shipped to the control plane **without** it
being in the request path. That closes the main thing a proxy would otherwise have
provided.

## What this design gives up

Worth stating plainly rather than discovering later:

- **Revocation is not instant.** Removing a grant takes effect on the next bundle poll
  (10–30s) or auth-file refresh (60s). A proxy could drop the connection mid-query.
  `KillQueryOwnedBy` covers the in-flight case; if sub-second revocation is ever a
  requirement, that is the one argument for reconsidering the path.
- **Network reach is a real boundary.** Access is gated by the internal-scheme NLB plus
  credentials. Anyone with VPC/tailnet reach and a valid credential connects without the
  control plane's involvement.

The flip side is that a control-plane outage does not take querying down.

## Why this is more control, not less

The control plane decides **policy**; Trino enforces it at the point where it knows exactly
which table, column and row are in play. That is strictly more governance than controlling
bytes on a socket, because the decision is made where the meaning is known.
