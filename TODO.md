# Backlog / Ideas

This file is a lightweight, non-canonical backlog for project ideas that do not
yet have a better home. It is not the PostgreSQL compatibility source of truth;
use [docs/postgres-compatibility.md](docs/postgres-compatibility.md) for
feature-by-feature compatibility status, test citations, and known gaps.

Prefer GitHub issues for committed work. Keep this file short: remove items once
they move to issues, docs, or implementation.

## Authentication & Users

- Support PostgreSQL MD5 password authentication.
- Support SCRAM-SHA-256 authentication.
- Add dynamic user management without restart.

## Configuration & Operations

- Support hot-reloading config without restart.
- Add admin commands such as `\duckgres status` and `\duckgres users`.
- Add a health check endpoint for load balancers.

## Observability

- Add slow query logging for queries exceeding a configurable threshold.

## Architecture & Storage

- Review whether each connection should own a separate DuckDB instance.
- Support read-only replicas using DuckDB `ATTACH`.
- Support multiple named databases per user.
- Define remaining schema support beyond the compatibility surfaces documented in
  [docs/postgres-compatibility.md](docs/postgres-compatibility.md).

## Research

- Federation: query multiple DuckDB files in a single query.
- Caching layer: Redis/memcached for query results.
- Query rewriting: translate PostgreSQL-specific syntax to DuckDB.
- MotherDuck integration.
- Parquet direct query: `SELECT * FROM 's3://bucket/file.parquet'`.

## Contributing

Issues labeled [`good first issue`](https://github.com/PostHog/duckgres/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
are a good place to start.
