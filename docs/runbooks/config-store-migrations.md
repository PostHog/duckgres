# Config Store Migrations

The control plane applies config-store schema changes from embedded SQL files in
`controlplane/configstore/migrations/` during `NewConfigStore` startup.

## How It Works

1. The control plane opens the config-store Postgres database.
2. Goose takes a Postgres session advisory lock named
   `duckgres:configstore:migrations`, so concurrent replicas do not apply the
   same migration at the same time.
3. Goose creates or updates its `goose_db_version` tracking table.
4. Goose applies pending migration files in version order.
5. After each file succeeds, Goose records the applied version.

Treat shipped migrations as immutable: add a new migration file instead of
editing one that may already have run in an environment.

## Adding a Migration

1. Add a new file under `controlplane/configstore/migrations/` with the next
   zero-padded prefix, for example `000004_add_example_column.sql`.
2. Keep the SQL idempotent where practical, using `IF EXISTS` or
   `IF NOT EXISTS` for additive DDL and guarded data backfills.
3. Add or update a Postgres-backed test in `tests/configstore/`.
4. Run the red-green loop:

```bash
just test-configstore-integration
```

5. Run lint before committing:

```bash
just lint
```

## Failure Recovery

If startup fails while applying a migration, that migration is not recorded in
`goose_db_version`. Fix the SQL or the existing schema state, then restart the
control plane.

Do not manually edit `goose_db_version` unless you have verified the database is
already at the exact same schema and data state as the migration would produce.
