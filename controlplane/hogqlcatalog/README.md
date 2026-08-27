# HogQL semantic catalog transport

This package serves the HogQL-only semantic overlay consumed by Trino. DuckLake
remains authoritative for physical schemas, tables, columns, and their types.
Schema version 2 manifests add the declarative metadata needed to lower HogQL:

- logical tables, physical fields, properties, and relationship join keys;
- expression fields built from typed field, literal, function, operator, and
  cast recipes;
- virtual tables that project another declared relation;
- saved-query aliases with opaque provenance IDs, typed executable relation
  targets, and result fields, plus physical materialized-view references;
- function signatures and the capabilities supported by their Trino lowering;
- modifier defaults and, where applicable, their Trino session-property name.

The contract never carries SQL fragments or code. Physical names use structured
identifiers, expression recipes use a closed discriminated union, and literal
payloads have an explicit type signature and encoding. Saved-query IDs are
opaque provenance; their required logical, virtual, or materialized target is
the complete locally executable version 2 form. Query text is not part of this
transport, so reads never require a hot-path metadata RPC.

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

All six semantic-metadata arrays are required, including when empty. Optional
recipe payloads are omitted unless selected by `kind`; exactly one payload must
match that discriminator. Definition names are case-insensitively unique in
their namespace, and logical, virtual, saved-query, and materialized-view names
share one relation namespace.

Validation resolves expression-field and virtual-table references before a
snapshot is published. Dependency cycles fail closed. Expression recipes are
limited to 64 levels and 4,096 nodes per snapshot, virtual relation chains are
limited to 64 levels, and a snapshot may declare at most 10,000 semantic
definitions. Relationship cycles remain valid because bidirectional logical
relationships are normal data-model structure rather than expansion recipes.
Version 2 expression fields may reference only fields on their owning logical
table. A future cross-table recipe must carry an explicit relationship path;
consumers must never infer a join.

Function declarations state whether lowering uses a stock Trino function, a
registered UDF, or a compiler rewrite. Modifier behavior is one of compiler
handling, a named Trino session property, a safe no-op, or explicitly
unsupported. Consumers must still fail closed when a requested capability is
not declared.

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
