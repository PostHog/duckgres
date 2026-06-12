# Config Store Migrations

The control plane applies config-store schema changes from embedded SQL files in
`controlplane/configstore/migrations/` during `NewConfigStore` startup.

## How It Works

1. The control plane opens the config-store Postgres database.
2. It takes a transaction-scoped advisory lock named
   `duckgres:configstore:migrations`, so concurrent replicas do not apply the
   same migration at the same time.
3. It creates or upgrades `duckgres_schema_migrations`.
4. It applies migration files in filename order.
5. After each file succeeds, it records the migration name, SHA-256 checksum,
   and application timestamp.

The startup fails if a previously recorded migration has a different checksum
from the embedded file. Treat that as a changed historical migration: add a new
migration file instead of editing one that may already have shipped.

## Adding a Migration

1. Add a new file under `controlplane/configstore/migrations/` with the next
   zero-padded prefix, for example `000005_add_example_column.sql`.
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

If startup fails while applying a migration, the transaction rolls back and the
migration is not recorded. Fix the SQL or the existing schema state, then
restart the control plane.

If startup fails with a checksum mismatch, do not update
`duckgres_schema_migrations` manually unless you have verified the database was
already migrated to the exact same schema and data state. The normal fix is to
restore the original migration file and add a new migration for the follow-up
change.
