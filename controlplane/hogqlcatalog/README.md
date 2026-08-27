# HogQL semantic catalog transport

This package serves the HogQL-only semantic overlay consumed by Trino. DuckLake
remains authoritative for physical schemas, tables, columns, and their types.
Schema version 2 manifests add the declarative metadata needed to lower HogQL:

- logical tables, physical fields, properties, and relationship join keys;
- optional generic property lookup recipes with typed source and key arguments;
- optional relationship predicates with explicit source and target field scopes;
- expression fields built from typed field, literal, function, operator, and
  cast recipes;
- lazy tables with bounded relationship paths and typed projection recipes;
- virtual tables that project another declared relation;
- saved-query aliases with opaque provenance IDs, typed executable relation
  targets, and result fields, plus physical materialized-view references;
- actions and cohorts represented by a declarative predicate or a typed
  relation-membership recipe;
- function signatures and the capabilities supported by their Trino lowering;
- modifier defaults and, where applicable, their Trino session-property name.

The contract never carries SQL fragments or code. Physical names use structured
identifiers, expression recipes use a closed discriminated union, and literal
payloads have an explicit type signature and encoding. Saved-query IDs are
opaque provenance; their required logical, virtual, or materialized target is
the complete locally executable version 2 form. Query text is not part of this
transport, so reads never require a hot-path metadata RPC.

## Physical metadata assembly

`PhysicalMetadataProvider` is the boundary for an authoritative DuckLake
metadata reader. `BuildPhysicalSnapshot` translates one complete provider result
into a normalized version 2 snapshot: tables and columns are ordered
deterministically, exact structured identifiers are preserved, and each column
carries its provider-supplied Trino type signature, nullability, and star
visibility. HogQL logical types are derived from the Trino type family. Types
that do not have a HogQL family remain `UNKNOWN` without changing the exact
Trino signature. Nullability and star visibility use explicit dispositions, so
an omitted provider value cannot silently become `NOT NULL` or hidden.

The provider result is all-or-nothing. Missing inventories, a mismatched
catalog, noncanonical identifiers, duplicate tables, columns, or ordinals, and
empty or noncanonical type-signature text fail before a snapshot can be
published. Full Trino type syntax is parsed by the Trino consumer; this boundary
preserves the provider's exact signature rather than approximating it. This
package does not synthesize tables when DuckLake metadata is unavailable. The
control plane still needs a production provider backed by the tenant metadata
store; the current HTTP publisher accepts already assembled snapshots.

## API

The control plane exposes one internal-token-authenticated compatibility resource:

- `PUT /v1/hogql/compatibility/semantic-catalog` publishes one immutable generation.
- `GET /v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=:version&catalog=:catalog&catalogDelimited=:boolean`
  reads the highest published generation.
- Adding `&generation=:generation` to that `GET` reads an exact generation.

Every manifest must include `protocolVersion: 1`, `schemaVersion: 2`, a supported language
version, a positive monotonically increasing generation, and all fields in the
typed JSON contract. Unknown fields, unknown references, executable SQL, missing
generations, and catalog or version mismatches fail closed.

The original six semantic-metadata arrays are required, including when empty.
The additive `lazyTables`, `actions`, and `cohorts` arrays may be omitted by an
older version 2 publisher. Optional
recipe payloads are omitted unless selected by `kind`; exactly one payload must
match that discriminator. Definition names are case-insensitively unique in
their namespace, and logical, virtual, saved-query, and materialized-view names
share one relation namespace.

A property may add `keyTypeSignature`, `valueTypeSignature`, and a
`lookupRecipe`. The recipe must reference both `PROPERTY_SOURCE` and
`PROPERTY_KEY`. These placeholders let the compiler insert a resolved physical
storage field and the query's typed property key without interpolating SQL.
Lookup recipes use the same closed literal, function, operator, and cast forms
as expression fields. `SUBSCRIPT` is the stock two-argument representation for
map and JSON-object access. A `PROPERTY_LOOKUP` expression names a declared
property and carries one typed key expression.

Relationship join keys remain required and describe equijoins. An optional
`joinPredicate` adds a typed condition when the equijoin does not capture the
complete relationship. Join predicates use `SCOPED_FIELD_REFERENCE` with an
explicit `SOURCE` or `TARGET` side, so a consumer never infers which relation a
field belongs to.

A lazy table belongs to one logical table, follows a non-empty relationship
path, and declares its visible output through typed projection recipes. This
metadata gives the compiler enough information to select requested projections
before it emits the joins. An action or cohort has exactly one representation:
a predicate on its declared logical table, or a relation membership with
explicit source and target fields. Relation membership may target a logical,
virtual, saved-query, or materialized relation already present in the snapshot.

Validation resolves expression-field and virtual-table references before a
snapshot is published. Dependency cycles fail closed. All expression, property,
join, lazy-projection, action, and cohort recipes share a limit of 64 levels and
4,096 nodes per snapshot. Virtual relation chains and lazy relationship paths
are limited to 64 levels, and a snapshot may declare at most 10,000 semantic
definitions. Relationship cycles remain valid because bidirectional logical
relationships are normal data-model structure rather than expansion recipes.
Version 2 expression fields may reference only fields on their owning logical
table. A future cross-table recipe must carry an explicit relationship path;
consumers must never infer a join.

Function declarations state whether lowering uses a stock Trino function, a
registered UDF, or a compiler rewrite. Stock and UDF declarations omit
`rewrite` and provide a non-empty structured `trinoName`. Rewrite declarations
provide an empty `trinoName` and one closed `rewrite` identifier: `IS_NULL` or
`IS_NOT_NULL`. Rewrites are deterministic scalar functions; every signature is
unary, non-variadic, and returns `boolean` using case-insensitive canonical type
text. Distinct, ordering, filter, and window traits are disabled. Missing,
unknown, or implementation-incompatible rewrite values, signatures, and traits
fail validation. Modifier behavior is one of compiler handling, a named Trino
session property, a safe no-op, or explicitly unsupported. Consumers must still
fail closed when a requested capability is not declared.

Published generations are persisted in the config-store Postgres database.
Rows are append-only and the latest snapshot is selected by generation, so all
control-plane replicas and restarted processes observe the same history. An
identical retry of the latest generation is idempotent; changing an existing
generation or publishing a lower generation is rejected.

Schema version 2 is intentionally incompatible with schema version 1. Publishers
must switch only after every reader accepts version 2; mixed-version operation
requires a separate compatibility endpoint or dual publication outside this
contract.

To roll back semantic behavior, publish the prior semantic content under a new,
higher generation. Do not update or delete a published generation in place.
