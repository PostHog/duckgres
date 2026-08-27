# HogQL semantic catalog transport

This package serves the HogQL-only semantic overlay consumed by Trino. DuckLake
remains authoritative for physical schemas, tables, columns, and their types;
these manifests contain only logical table, field, property, and relationship
definitions plus references to physical qualified names.

## API

The control plane exposes one internal-token-authenticated compatibility resource:

- `PUT /v1/hogql/compatibility/semantic-catalog` publishes one immutable generation.
- `GET /v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=:version&catalog=:catalog&catalogDelimited=:boolean`
  reads the highest published generation.
- Adding `&generation=:generation` to that `GET` reads an exact generation.

Every manifest must include `protocolVersion: 1`, a supported schema and language
version, a positive monotonically increasing generation, and all fields in the
typed JSON contract. Unknown fields, unknown references, executable SQL, missing
generations, and catalog or version mismatches fail closed.

Published generations are persisted in the config-store Postgres database.
Rows are append-only and the latest snapshot is selected by generation, so all
control-plane replicas and restarted processes observe the same history. An
identical retry of the latest generation is idempotent; changing an existing
generation or publishing a lower generation is rejected.

To roll back semantic behavior, publish the prior semantic content under a new,
higher generation. Do not update or delete a published generation in place.
