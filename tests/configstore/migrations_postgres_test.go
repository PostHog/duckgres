//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestConfigStoreRunsVersionedSQLMigrations(t *testing.T) {
	store := newIsolatedConfigStore(t)
	db := storeDB(t, store)

	requireGooseMigrationRecorded(t, db, 1)
	requireGooseMigrationRecorded(t, db, 3)
	requireGooseMigrationRecorded(t, db, 4)
	requireGooseMigrationRecorded(t, db, 5)
	requireGooseMigrationRecorded(t, db, 6)
	requireGooseMigrationRecorded(t, db, 7)
	requireGooseMigrationRecorded(t, db, 8)
	requireGooseMigrationRecorded(t, db, 9)
	requireGooseMigrationRecorded(t, db, 10)
	requireGooseMigrationRecorded(t, db, 11)
	requireGooseMigrationRecorded(t, db, 12)
	requireGooseMigrationRecorded(t, db, 13)
	requireGooseMigrationRecorded(t, db, 14)
	requireGooseMigrationRecorded(t, db, 15)
	requireGooseMigrationRecorded(t, db, 16)
	requireGooseMigrationRecorded(t, db, 17)
	requireGooseMigrationRecorded(t, db, 18)
	requireGooseMigrationRecorded(t, db, 19)
	requireGooseMigrationRecorded(t, db, 20)
	requireGooseMigrationRecorded(t, db, 21)
	requireGooseMigrationRecorded(t, db, 22)
	requireGooseMigrationRecorded(t, db, 23)
	requireGooseMigrationRecorded(t, db, 24)
	requireGooseMigrationRecorded(t, db, 25)
	requireGooseMigrationRecorded(t, db, 26)
	requireGooseMigrationRecorded(t, db, 27)
	requireGooseMigrationRecorded(t, db, 28)
	requireGooseMigrationRecorded(t, db, 29)
	requireGooseLatestVersion(t, db, 29)
	requireTableAbsent(t, db, "duckgres_schema_migrations")

	// Migration 000018 added the reshard operation + verbose log tables.
	requireTablePresent(t, db, "duckgres_reshard_operations")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "source_kind")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "to_shard")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "runner_epoch")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "cancel_requested")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "cutover_timeout_seconds")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "blocked_at")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "compaction_was_present")
	// Migration 000021 added the pre-flip catalog backup artifact URI.
	requireColumnPresent(t, db, "duckgres_reshard_operations", "backup_s3_uri")
	// Migration 000022 added the runner pod's ephemeral-password pull URL
	// (URL only — the password itself is never persisted).
	requireColumnPresent(t, db, "duckgres_reshard_operations", "password_url")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "respawn_attempts")
	requireColumnPresent(t, db, "duckgres_reshard_operations", "runner_image")
	requireTablePresent(t, db, "duckgres_reshard_operation_log")
	requireColumnPresent(t, db, "duckgres_reshard_operation_log", "operation_id")

	// Migration 000016 added the worker spawn log that feeds dynamic headroom
	// slot count/size.
	requireTablePresent(t, db, "duckgres_worker_spawn_log")
	requireColumnPresent(t, db, "duckgres_worker_spawn_log", "cpu_millis")
	requireColumnPresent(t, db, "duckgres_worker_spawn_log", "mem_bytes")
	requireColumnPresent(t, db, "duckgres_worker_spawn_log", "spawned_at")

	// Migration 000013 added the per-org default_team_id column (nullable at
	// the time); 000017 converted it (and the usage-bucket team_id below) to
	// BIGINT; 000024 replaced it with the per-org teams table — many PostHog
	// teams per org, exactly one marked as the billing team (the billing
	// bucket key) — and dropped the column.
	requireColumnAbsent(t, db, "duckgres_orgs", "default_team_id")
	requireTablePresent(t, db, "duckgres_org_teams")
	requireColumnType(t, db, "duckgres_org_teams", "team_id", "bigint")
	requireColumnNotNull(t, db, "duckgres_org_teams", "schema_name")
	requireColumnDefault(t, db, "duckgres_org_teams", "enabled", "true")
	requireColumnType(t, db, "duckgres_org_compute_usage", "team_id", "bigint")

	// Migration 000025 added the legacy table-name overrides (nullable — NULL
	// means "derive from schema_name") and the per-org schema uniqueness.
	requireColumnNullable(t, db, "duckgres_org_teams", "events_table_name")
	requireColumnNullable(t, db, "duckgres_org_teams", "persons_table_name")
	requireColumnNullable(t, db, "duckgres_org_teams", "schema_data_imports_name")
	requireUniqueIndex(t, db, "duckgres_org_teams", "org_id,schema_name")

	// Migration 000026 added PostHog's cached earliest-event date (nullable
	// DATE — NULL until the PostHog sensor resolves it).
	requireColumnNullable(t, db, "duckgres_org_teams", "earliest_event_date")
	requireColumnType(t, db, "duckgres_org_teams", "earliest_event_date", "date")

	// Migration 000027 pinned backfill_enabled to NOT NULL DEFAULT TRUE,
	// mirroring the PostHog-side Django BooleanField(default=True).
	requireColumnNotNull(t, db, "duckgres_org_teams", "backfill_enabled")
	requireColumnDefault(t, db, "duckgres_org_teams", "backfill_enabled", "true")

	// Migration 000029 dropped the billing-team marker: duckgres no longer
	// owns team-level billing attribution (the external billing service does).
	requireColumnAbsent(t, db, "duckgres_org_teams", "is_billing_team")

	// Migration 000007 added the compute-usage billing buffer; 000015 widened
	// its key for pull-based billing (team_id, query_source, worker size),
	// added the single ack cursor and dropped the push-drain state table.
	requireTablePresent(t, db, "duckgres_org_compute_usage")
	requireColumnPresent(t, db, "duckgres_org_compute_usage", "team_id")
	requireColumnPresent(t, db, "duckgres_org_compute_usage", "query_source")
	requireColumnPresent(t, db, "duckgres_org_compute_usage", "cpu")
	requireColumnPresent(t, db, "duckgres_org_compute_usage", "mem_gib")
	requireTablePresent(t, db, "duckgres_compute_billing_cursor")
	requireTableAbsent(t, db, "duckgres_org_compute_drain_state")

	// Migration 000019 added the storage-usage buffer (byte-seconds per
	// (org, team, bucket); NUMERIC — byte-seconds overflow BIGINT).
	requireTablePresent(t, db, "duckgres_org_storage_usage")
	requireColumnType(t, db, "duckgres_org_storage_usage", "team_id", "bigint")
	requireColumnType(t, db, "duckgres_org_storage_usage", "byte_seconds", "numeric")

	// Migration 000008 added the explicit Duckling CR name column on
	// managed warehouses, backfilled from lower(org_id).
	requireColumnPresent(t, db, "duckgres_managed_warehouses", "duckling_name")

	// Migration 000004 dropped the dead cluster-wide singleton config tables.
	requireTableAbsent(t, db, "duckgres_global_config")
	requireTableAbsent(t, db, "duckgres_ducklake_config")
	requireTableAbsent(t, db, "duckgres_rate_limit_config")
	requireTableAbsent(t, db, "duckgres_query_log_config")

	var columnCount int
	if err := store.DB().Raw(`
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'duckgres_orgs'
		  AND column_name = 'default_worker_min_hot_idle'
	`).Scan(&columnCount).Error; err != nil {
		t.Fatalf("query default_worker_min_hot_idle column: %v", err)
	}
	if columnCount != 1 {
		t.Fatalf("default_worker_min_hot_idle column count = %d, want 1", columnCount)
	}
	requireColumnDefault(t, db, "duckgres_orgs", "max_vcpus", "0")
	requireColumnDefault(t, db, "duckgres_org_users", "max_vcpus", "0")
	// Migration 000011 added the per-user kill-switch column.
	requireColumnDefault(t, db, "duckgres_org_users", "disabled", "false")
	requireColumnAbsent(t, db, "duckgres_orgs", "max_connections")
	// Migration 000014 dropped the Iceberg/Lakekeeper columns and the per-user
	// default_catalog selector.
	requireColumnAbsent(t, db, "duckgres_managed_warehouses", "iceberg_enabled")
	requireColumnAbsent(t, db, "duckgres_managed_warehouses", "iceberg_lakekeeper_endpoint")
	requireColumnAbsent(t, db, "duckgres_managed_warehouses", "iceberg_state")
	requireColumnAbsent(t, db, "duckgres_org_users", "default_catalog")
}

func TestConfigStoreSQLMigrationsUpgradeVersion8Schema(t *testing.T) {
	_, connStr := newIsolatedConfigStoreSchema(t)
	store, err := cpconfigStoreNew(connStr)
	if err != nil {
		t.Fatalf("create baseline config store: %v", err)
	}
	baselineDB := storeDB(t, store)
	t.Cleanup(func() {
		_ = baselineDB.Close()
	})

	if err := store.DB().Exec(`
			ALTER TABLE duckgres_orgs DROP COLUMN max_vcpus;
			ALTER TABLE duckgres_org_users DROP COLUMN max_vcpus;
			ALTER TABLE duckgres_org_users DROP COLUMN disabled;
			ALTER TABLE duckgres_orgs ADD COLUMN IF NOT EXISTS max_connections BIGINT DEFAULT 0;
			ALTER TABLE duckgres_managed_warehouses ALTER COLUMN duckling_name DROP NOT NULL;
			ALTER TABLE duckgres_org_users DROP CONSTRAINT IF EXISTS duckgres_org_users_team_fk;
			ALTER TABLE duckgres_org_users DROP COLUMN IF EXISTS team_id, DROP COLUMN IF EXISTS access_mode;
			DROP TABLE IF EXISTS duckgres_org_teams;
			ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS iceberg_enabled BOOLEAN DEFAULT false;
			ALTER TABLE duckgres_managed_warehouses ADD COLUMN IF NOT EXISTS iceberg_state VARCHAR(32);
			ALTER TABLE duckgres_org_users ADD COLUMN IF NOT EXISTS default_catalog VARCHAR(255);
			DROP TABLE duckgres_worker_spawn_log;
			-- Reverse 000015 + 000017 on the compute-usage buffer (widened
			-- pull-billing key → the original v7 (org_id, bucket_start) shape;
			-- dropping the PK columns drops the composite PK with them), and
			-- restore the push-drain state table 000015 removed. Without this
			-- the replay of 000015's backfill hits a BIGINT team_id with a ''
			-- comparison and fails.
			ALTER TABLE duckgres_org_compute_usage
				DROP COLUMN team_id, DROP COLUMN query_source, DROP COLUMN cpu, DROP COLUMN mem_gib;
			ALTER TABLE duckgres_org_compute_usage ADD PRIMARY KEY (org_id, bucket_start);
			DROP TABLE IF EXISTS duckgres_compute_billing_cursor;
			CREATE TABLE IF NOT EXISTS duckgres_org_compute_drain_state (
				org_id TEXT PRIMARY KEY,
				last_drained_bucket TIMESTAMPTZ NOT NULL
			);
			DROP TABLE IF EXISTS duckgres_reshard_operation_log;
			DROP TABLE IF EXISTS duckgres_reshard_operations;
			DELETE FROM goose_db_version WHERE version_id IN (9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
		`).Error; err != nil {
		t.Fatalf("downgrade baseline schema to pre-v9 shape: %v", err)
	}
	requireColumnAbsent(t, baselineDB, "duckgres_orgs", "max_vcpus")
	requireColumnAbsent(t, baselineDB, "duckgres_org_users", "max_vcpus")
	requireColumnAbsent(t, baselineDB, "duckgres_org_users", "disabled")
	requireColumnAbsent(t, baselineDB, "duckgres_orgs", "default_team_id")
	requireColumnPresent(t, baselineDB, "duckgres_orgs", "max_connections")
	requireColumnPresent(t, baselineDB, "duckgres_managed_warehouses", "iceberg_enabled")
	requireColumnPresent(t, baselineDB, "duckgres_org_users", "default_catalog")
	requireGooseLatestVersion(t, baselineDB, 8)

	upgradedStore, err := cpconfigStoreNew(connStr)
	if err != nil {
		t.Fatalf("upgrade pre-v8 schema: %v", err)
	}
	upgradedDB := storeDB(t, upgradedStore)
	t.Cleanup(func() {
		_ = upgradedDB.Close()
	})

	requireGooseMigrationRecorded(t, upgradedDB, 9)
	requireGooseMigrationRecorded(t, upgradedDB, 10)
	requireGooseMigrationRecorded(t, upgradedDB, 11)
	requireGooseMigrationRecorded(t, upgradedDB, 12)
	requireGooseMigrationRecorded(t, upgradedDB, 13)
	requireGooseMigrationRecorded(t, upgradedDB, 14)
	requireGooseMigrationRecorded(t, upgradedDB, 15)
	requireGooseMigrationRecorded(t, upgradedDB, 16)
	requireGooseMigrationRecorded(t, upgradedDB, 17)
	requireGooseMigrationRecorded(t, upgradedDB, 18)
	requireGooseMigrationRecorded(t, upgradedDB, 19)
	requireGooseMigrationRecorded(t, upgradedDB, 20)
	requireGooseMigrationRecorded(t, upgradedDB, 21)
	requireGooseMigrationRecorded(t, upgradedDB, 22)
	requireGooseMigrationRecorded(t, upgradedDB, 23)
	requireGooseMigrationRecorded(t, upgradedDB, 24)
	requireGooseMigrationRecorded(t, upgradedDB, 25)
	requireGooseMigrationRecorded(t, upgradedDB, 26)
	requireGooseMigrationRecorded(t, upgradedDB, 27)
	requireGooseMigrationRecorded(t, upgradedDB, 28)
	requireGooseMigrationRecorded(t, upgradedDB, 29)
	requireGooseLatestVersion(t, upgradedDB, 29)
	requireColumnPresent(t, upgradedDB, "duckgres_reshard_operations", "password_url")
	requireTablePresent(t, upgradedDB, "duckgres_worker_spawn_log")
	requireColumnDefault(t, upgradedDB, "duckgres_orgs", "max_vcpus", "0")
	requireColumnDefault(t, upgradedDB, "duckgres_org_users", "max_vcpus", "0")
	requireColumnDefault(t, upgradedDB, "duckgres_org_users", "disabled", "false")
	requireColumnAbsent(t, upgradedDB, "duckgres_orgs", "default_team_id")
	requireTablePresent(t, upgradedDB, "duckgres_org_teams")
	requireColumnNullable(t, upgradedDB, "duckgres_org_teams", "events_table_name")
	requireColumnNullable(t, upgradedDB, "duckgres_org_teams", "earliest_event_date")
	requireColumnNotNull(t, upgradedDB, "duckgres_org_teams", "backfill_enabled")
	requireUniqueIndex(t, upgradedDB, "duckgres_org_teams", "org_id,schema_name")
	requireColumnAbsent(t, upgradedDB, "duckgres_orgs", "max_connections")
	requireColumnAbsent(t, upgradedDB, "duckgres_managed_warehouses", "iceberg_enabled")
	requireColumnAbsent(t, upgradedDB, "duckgres_org_users", "default_catalog")
}

func TestConfigStoreSQLMigrationsUpgradeOldOrgSchema(t *testing.T) {
	_, connStr := newIsolatedConfigStoreSchema(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open schema db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	// default_team_id is seeded pre-populated: migration 000013 adds the
	// column with IF NOT EXISTS (so this no-ops there) and 000020 makes it
	// NOT NULL, failing loudly on any NULL row. Real envs were backfilled
	// before 000020 shipped, so a legacy-with-rows fixture must carry a value
	// too (the loud-fail path has its own regression test below).
	if _, err := db.Exec(`
		CREATE TABLE duckgres_orgs (
			name VARCHAR(255) PRIMARY KEY,
			database_name VARCHAR(255),
			hostname_alias VARCHAR(255),
			max_workers BIGINT DEFAULT 0,
			max_connections BIGINT DEFAULT 0,
			memory_budget VARCHAR(32),
			idle_timeout_s BIGINT DEFAULT 0,
			worker_cpu_request VARCHAR(32),
			worker_memory_request VARCHAR(32),
			default_worker_cpu VARCHAR(32),
			default_worker_memory VARCHAR(32),
			default_worker_ttl VARCHAR(32),
			default_team_id VARCHAR(255),
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		);
		CREATE UNIQUE INDEX idx_duckgres_orgs_database_name ON duckgres_orgs(database_name);
		CREATE UNIQUE INDEX idx_duckgres_orgs_hostname_alias ON duckgres_orgs(hostname_alias);
		INSERT INTO duckgres_orgs (name, database_name, default_team_id, created_at, updated_at)
		VALUES ('old-org', 'old-org', '1', now(), now());
	`); err != nil {
		t.Fatalf("create old org schema: %v", err)
	}

	store, err := cpconfigStoreNew(connStr)
	if err != nil {
		t.Fatalf("new config store against old schema: %v", err)
	}
	sqlDB := storeDB(t, store)
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	var floor int
	if err := store.DB().Raw(`SELECT default_worker_min_hot_idle FROM duckgres_orgs WHERE name = 'old-org'`).Scan(&floor).Error; err != nil {
		t.Fatalf("read migrated default_worker_min_hot_idle: %v", err)
	}
	if floor != 0 {
		t.Fatalf("default_worker_min_hot_idle after migration = %d, want 0", floor)
	}
	var maxVCPUs int
	if err := store.DB().Raw(`SELECT max_vcpus FROM duckgres_orgs WHERE name = 'old-org'`).Scan(&maxVCPUs).Error; err != nil {
		t.Fatalf("read migrated org max_vcpus: %v", err)
	}
	if maxVCPUs != 0 {
		t.Fatalf("org max_vcpus after migration = %d, want 0", maxVCPUs)
	}
	requireColumnAbsent(t, sqlDB, "duckgres_orgs", "max_connections")
	requireGooseMigrationRecorded(t, sqlDB, 3)

	// Migration 000024 backfilled the legacy default_team_id value into the
	// org's team row and dropped the column.
	requireColumnAbsent(t, sqlDB, "duckgres_orgs", "default_team_id")
	var team cpconfigstore.OrgTeam
	if err := store.DB().First(&team, "org_id = ?", "old-org").Error; err != nil {
		t.Fatalf("read backfilled org team row: %v", err)
	}
	if team.TeamID != 1 || team.SchemaName != "team_1" || !team.Enabled {
		t.Fatalf("backfilled team row = %+v, want team 1, schema team_1, enabled", team)
	}
	// 000024 seeded the row with backfill_enabled NULL; 000027 pins it TRUE.
	if team.BackfillEnabled == nil || !*team.BackfillEnabled {
		t.Fatalf("backfilled team row backfill_enabled = %v, want TRUE after migration 000028", team.BackfillEnabled)
	}
	// Migration 000029 dropped the billing-team marker entirely — duckgres no
	// longer owns team-level billing attribution.
	requireColumnAbsent(t, sqlDB, "duckgres_org_teams", "is_billing_team")
}

// TestConfigStoreMigrationFailsLoudlyOnNullDefaultTeamID pins the deploy-time
// stance of migration 000020: a NULL default_team_id row (an org that somehow
// escaped the backfill) makes SET NOT NULL fail loudly instead of being
// silently papered over. The correct outcome is to fix the row, not the
// constraint.
func TestConfigStoreMigrationFailsLoudlyOnNullDefaultTeamID(t *testing.T) {
	_, connStr := newIsolatedConfigStoreSchema(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open schema db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	// Minimal pre-000013 org schema with a live row and no default_team_id:
	// 000013 adds the column as NULL for the row, 000020 must then refuse.
	if _, err := db.Exec(`
		CREATE TABLE duckgres_orgs (
			name VARCHAR(255) PRIMARY KEY,
			database_name VARCHAR(255),
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		);
		INSERT INTO duckgres_orgs (name, database_name, created_at, updated_at)
		VALUES ('unbackfilled-org', 'unbackfilled-org', now(), now());
	`); err != nil {
		t.Fatalf("create unbackfilled org schema: %v", err)
	}

	store, err := cpconfigStoreNew(connStr)
	if err == nil {
		sqlDB := storeDB(t, store)
		_ = sqlDB.Close()
		t.Fatal("config store migration succeeded, want loud failure on NULL default_team_id")
	}
	if !strings.Contains(err.Error(), "default_team_id") || !strings.Contains(err.Error(), "null") {
		t.Fatalf("migration error should name the NULL default_team_id violation, got: %v", err)
	}
}

func TestConfigStoreSQLMigrationsUpgradeLegacyOrgUsersUsernamePK(t *testing.T) {
	_, connStr := newIsolatedConfigStoreSchema(t)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open schema db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	passwordHash, err := cpconfigstore.HashPassword("secret")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	// default_team_id pre-populated for the same reason as the old-org-schema
	// test above: 000020 makes it NOT NULL and real envs are backfilled.
	if _, err := db.Exec(`
		CREATE TABLE duckgres_orgs (
			name VARCHAR(255) PRIMARY KEY,
			database_name VARCHAR(255),
			default_team_id VARCHAR(255),
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		);
		CREATE TABLE duckgres_org_users (
			username VARCHAR(255) PRIMARY KEY,
			password VARCHAR(255) NOT NULL,
			org_id VARCHAR(255) NOT NULL,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		);
		INSERT INTO duckgres_orgs (name, database_name, default_team_id, created_at, updated_at)
		VALUES ('old-org', 'old-org', '1', now(), now());
	`); err != nil {
		t.Fatalf("create legacy org user schema: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO duckgres_org_users (username, password, org_id, created_at, updated_at)
		VALUES ('old-user', $1, 'old-org', now(), now());
	`, passwordHash); err != nil {
		t.Fatalf("seed legacy org user: %v", err)
	}

	store, err := cpconfigStoreNew(connStr)
	if err != nil {
		t.Fatalf("new config store against legacy org users schema: %v", err)
	}
	sqlDB := storeDB(t, store)
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	primaryKeys := loadConfigStorePrimaryKeys(t, sqlDB)
	wantPK := primaryKeyMetadata{TableName: "duckgres_org_users", ColumnNames: "org_id,username"}
	if got := primaryKeys["duckgres_org_users"]; got != wantPK {
		t.Fatalf("duckgres_org_users primary key = %#v, want %#v", got, wantPK)
	}

	foreignKeys := loadConfigStoreForeignKeys(t, sqlDB)
	wantFK := foreignKeyMetadata{
		TableName:          "duckgres_org_users",
		ColumnNames:        "org_id",
		ForeignTableName:   "duckgres_orgs",
		ForeignColumnNames: "name",
		DeleteRule:         "NO ACTION",
	}
	if got := foreignKeys["duckgres_org_users.org_id->duckgres_orgs.name"]; got != wantFK {
		t.Fatalf("duckgres_org_users org FK = %#v, want %#v", got, wantFK)
	}

	var maxVCPUs int
	if err := store.DB().Raw(`SELECT max_vcpus FROM duckgres_org_users WHERE org_id = 'old-org' AND username = 'old-user'`).Scan(&maxVCPUs).Error; err != nil {
		t.Fatalf("read migrated user max_vcpus: %v", err)
	}
	if maxVCPUs != 0 {
		t.Fatalf("user max_vcpus after migration = %d, want 0", maxVCPUs)
	}

	resolution := store.ResolvePostgresConnection("ducklake", "old-org", true, "old-user", "secret")
	if !resolution.Valid || resolution.OrgID != "old-org" {
		t.Fatalf("legacy migrated user resolution = %#v, want valid old-org", resolution)
	}
}

func TestConfigStoreSQLMigrationsMatchGORMModelMetadata(t *testing.T) {
	migratedStore := newIsolatedConfigStore(t)
	migratedDB := storeDB(t, migratedStore)

	_, gormConnStr := newIsolatedConfigStoreSchema(t)
	gormDB, err := gorm.Open(postgres.Open(gormConnStr), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open gorm schema db: %v", err)
	}
	if err := gormDB.AutoMigrate(
		&cpconfigstore.Org{},
		&cpconfigstore.ManagedWarehouse{},
		&cpconfigstore.OrgUser{},
		&cpconfigstore.OrgTeam{},
		&cpconfigstore.OrgUserSecret{},
		&cpconfigstore.Operator{},
	); err != nil {
		t.Fatalf("auto-migrate gorm comparison schema: %v", err)
	}
	gormSQLDB, err := gormDB.DB()
	if err != nil {
		t.Fatalf("gorm sql db: %v", err)
	}
	t.Cleanup(func() {
		_ = gormSQLDB.Close()
	})

	migratedColumns := loadConfigStoreColumnMetadata(t, migratedDB)
	gormColumns := loadConfigStoreColumnMetadata(t, gormSQLDB)
	if !reflect.DeepEqual(migratedColumns, gormColumns) {
		t.Fatalf("migration column metadata differs from GORM model metadata:\n%s", metadataDiff(migratedColumns, gormColumns))
	}

	migratedPrimaryKeys := loadConfigStorePrimaryKeys(t, migratedDB)
	gormPrimaryKeys := loadConfigStorePrimaryKeys(t, gormSQLDB)
	if !reflect.DeepEqual(migratedPrimaryKeys, gormPrimaryKeys) {
		t.Fatalf("migration primary keys differ from GORM model metadata:\n%s", metadataDiff(migratedPrimaryKeys, gormPrimaryKeys))
	}

	migratedIndexes := loadConfigStoreIndexes(t, migratedDB)
	gormIndexes := loadConfigStoreIndexes(t, gormSQLDB)
	if !reflect.DeepEqual(migratedIndexes, gormIndexes) {
		t.Fatalf("migration indexes differ from GORM model metadata:\n%s", metadataDiff(migratedIndexes, gormIndexes))
	}

	migratedFKs := loadConfigStoreForeignKeys(t, migratedDB)
	gormFKs := loadConfigStoreForeignKeys(t, gormSQLDB)
	if !reflect.DeepEqual(migratedFKs, gormFKs) {
		t.Fatalf("migration foreign keys differ from GORM model metadata:\n%s", metadataDiff(migratedFKs, gormFKs))
	}
}

func requireGooseMigrationRecorded(t *testing.T, db *sql.DB, version int64) {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM goose_db_version
		WHERE version_id = $1
		  AND is_applied
	`, version).Scan(&count); err != nil {
		t.Fatalf("count goose migration version %d: %v", version, err)
	}
	if count != 1 {
		t.Fatalf("goose migration version %d row count = %d, want 1", version, count)
	}
}

func requireGooseLatestVersion(t *testing.T, db *sql.DB, version int64) {
	t.Helper()

	var latest sql.NullInt64
	if err := db.QueryRow(`
		SELECT MAX(version_id)
		FROM goose_db_version
		WHERE is_applied
		  AND version_id > 0
	`).Scan(&latest); err != nil {
		t.Fatalf("read latest goose migration version: %v", err)
	}
	if !latest.Valid || latest.Int64 != version {
		t.Fatalf("latest goose migration version = %v, want %d", latest, version)
	}
}

func requireColumnPresent(t *testing.T, db *sql.DB, tableName, columnName string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists); err != nil {
		t.Fatalf("check column %q.%q presence: %v", tableName, columnName, err)
	}
	if !exists {
		t.Fatalf("column %q.%q missing, want present", tableName, columnName)
	}
}

func requireTablePresent(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = $1
		)
	`, tableName).Scan(&exists); err != nil {
		t.Fatalf("check table %q presence: %v", tableName, err)
	}
	if !exists {
		t.Fatalf("table %q missing, want present", tableName)
	}
}

func requireTableAbsent(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = $1
		)
	`, tableName).Scan(&exists); err != nil {
		t.Fatalf("check table %q absence: %v", tableName, err)
	}
	if exists {
		t.Fatalf("table %q exists, want absent", tableName)
	}
}

func requireColumnAbsent(t *testing.T, db *sql.DB, tableName, columnName string) {
	t.Helper()

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = $1
			  AND column_name = $2
		)
	`, tableName, columnName).Scan(&exists); err != nil {
		t.Fatalf("check column %s.%s absence: %v", tableName, columnName, err)
	}
	if exists {
		t.Fatalf("column %s.%s exists, want absent", tableName, columnName)
	}
}

func storeDB(t *testing.T, store *cpconfigstore.ConfigStore) *sql.DB {
	t.Helper()

	db, err := store.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	return db
}

type columnMetadata struct {
	TableName              string
	ColumnName             string
	IsNullable             string
	DataType               string
	CharacterMaximumLength sql.NullInt64
	ColumnDefault          sql.NullString
}

type primaryKeyMetadata struct {
	TableName   string
	ColumnNames string
}

type indexMetadata struct {
	TableName   string
	ColumnNames string
	Unique      bool
}

type foreignKeyMetadata struct {
	TableName          string
	ColumnNames        string
	ForeignTableName   string
	ForeignColumnNames string
	DeleteRule         string
}

func requireColumnType(t *testing.T, db *sql.DB, tableName, columnName, wantType string) {
	t.Helper()

	var got string
	err := db.QueryRow(`
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = $1
		  AND column_name = $2
	`, tableName, columnName).Scan(&got)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("%s.%s column missing", tableName, columnName)
		}
		t.Fatalf("query %s.%s column type: %v", tableName, columnName, err)
	}
	if got != wantType {
		t.Fatalf("%s.%s type = %q, want %q", tableName, columnName, got, wantType)
	}
}

func requireColumnNotNull(t *testing.T, db *sql.DB, tableName, columnName string) {
	t.Helper()

	var isNullable string
	err := db.QueryRow(`
		SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = $1
		  AND column_name = $2
	`, tableName, columnName).Scan(&isNullable)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("%s.%s column missing", tableName, columnName)
		}
		t.Fatalf("query %s.%s nullability: %v", tableName, columnName, err)
	}
	if isNullable != "NO" {
		t.Fatalf("%s.%s is_nullable = %q, want %q (column must be NOT NULL)", tableName, columnName, isNullable, "NO")
	}
}

func requireColumnNullable(t *testing.T, db *sql.DB, tableName, columnName string) {
	t.Helper()

	var isNullable string
	err := db.QueryRow(`
		SELECT is_nullable
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = $1
		  AND column_name = $2
	`, tableName, columnName).Scan(&isNullable)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("%s.%s column missing", tableName, columnName)
		}
		t.Fatalf("query %s.%s nullability: %v", tableName, columnName, err)
	}
	if isNullable != "YES" {
		t.Fatalf("%s.%s is_nullable = %q, want %q (column must be nullable)", tableName, columnName, isNullable, "YES")
	}
}

func requireUniqueIndex(t *testing.T, db *sql.DB, tableName, columnNames string) {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT idx.indexrelid,
				string_agg(src_col.attname, ',' ORDER BY cols.ord) AS column_names
			FROM pg_index idx
			JOIN pg_class src ON src.oid = idx.indrelid
			JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
			JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS cols(attnum, ord) ON true
			JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = cols.attnum
			WHERE idx.indisunique
			  AND NOT idx.indisprimary
			  AND src_ns.nspname = current_schema()
			  AND src.relname = $1
			GROUP BY idx.indexrelid
		) i
		WHERE i.column_names = $2
	`, tableName, columnNames).Scan(&count); err != nil {
		t.Fatalf("query unique index on %s(%s): %v", tableName, columnNames, err)
	}
	if count != 1 {
		t.Fatalf("unique index on %s(%s): count = %d, want 1", tableName, columnNames, count)
	}
}

func requireColumnDefault(t *testing.T, db *sql.DB, tableName, columnName, wantDefault string) {
	t.Helper()

	var got sql.NullString
	err := db.QueryRow(`
		SELECT column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = $1
		  AND column_name = $2
	`, tableName, columnName).Scan(&got)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatalf("%s.%s column missing", tableName, columnName)
		}
		t.Fatalf("query %s.%s column default: %v", tableName, columnName, err)
	}
	if !got.Valid || got.String != wantDefault {
		t.Fatalf("%s.%s default = %q (valid=%v), want %q", tableName, columnName, got.String, got.Valid, wantDefault)
	}
}

func loadConfigStoreColumnMetadata(t *testing.T, db *sql.DB) map[string]columnMetadata {
	t.Helper()

	rows, err := db.Query(`
		SELECT table_name, column_name, is_nullable, data_type, character_maximum_length, column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name IN (
			'duckgres_orgs',
			'duckgres_org_users',
			'duckgres_org_teams',
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
			'duckgres_operators',
			'duckgres_global_config',
			'duckgres_ducklake_config',
			'duckgres_rate_limit_config',
			'duckgres_query_log_config'
		  )
		ORDER BY table_name, ordinal_position
	`)
	if err != nil {
		t.Fatalf("query column metadata: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close column metadata rows: %v", err)
		}
	}()

	out := make(map[string]columnMetadata)
	for rows.Next() {
		var meta columnMetadata
		if err := rows.Scan(
			&meta.TableName,
			&meta.ColumnName,
			&meta.IsNullable,
			&meta.DataType,
			&meta.CharacterMaximumLength,
			&meta.ColumnDefault,
		); err != nil {
			t.Fatalf("scan column metadata: %v", err)
		}
		out[meta.TableName+"."+meta.ColumnName] = meta
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate column metadata: %v", err)
	}
	return out
}

func loadConfigStorePrimaryKeys(t *testing.T, db *sql.DB) map[string]primaryKeyMetadata {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			src.relname AS table_name,
			string_agg(src_col.attname, ',' ORDER BY cols.ord) AS column_names
		FROM pg_constraint c
		JOIN pg_class src ON src.oid = c.conrelid
		JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
		JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = cols.attnum
		WHERE c.contype = 'p'
		  AND src_ns.nspname = current_schema()
		  AND src.relname IN (
			'duckgres_orgs',
			'duckgres_org_users',
			'duckgres_org_teams',
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
			'duckgres_operators',
			'duckgres_global_config',
			'duckgres_ducklake_config',
			'duckgres_rate_limit_config',
			'duckgres_query_log_config'
		  )
		GROUP BY c.oid, src.relname
		ORDER BY src.relname
	`)
	if err != nil {
		t.Fatalf("query primary key metadata: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close primary key metadata rows: %v", err)
		}
	}()

	out := make(map[string]primaryKeyMetadata)
	for rows.Next() {
		var meta primaryKeyMetadata
		if err := rows.Scan(&meta.TableName, &meta.ColumnNames); err != nil {
			t.Fatalf("scan primary key metadata: %v", err)
		}
		out[meta.TableName] = meta
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate primary key metadata: %v", err)
	}
	return out
}

func loadConfigStoreIndexes(t *testing.T, db *sql.DB) map[string]indexMetadata {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			src.relname AS table_name,
			string_agg(src_col.attname, ',' ORDER BY cols.ord) AS column_names,
			idx.indisunique AS unique
		FROM pg_index idx
		JOIN pg_class src ON src.oid = idx.indrelid
		JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
		JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS cols(attnum, ord) ON true
		JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = cols.attnum
		WHERE NOT idx.indisprimary
		  AND src_ns.nspname = current_schema()
		  AND src.relname IN (
			'duckgres_orgs',
			'duckgres_org_users',
			'duckgres_org_teams',
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
			'duckgres_operators',
			'duckgres_global_config',
			'duckgres_ducklake_config',
			'duckgres_rate_limit_config',
			'duckgres_query_log_config'
		  )
		GROUP BY idx.indexrelid, src.relname, idx.indisunique
		ORDER BY src.relname, column_names
	`)
	if err != nil {
		t.Fatalf("query index metadata: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close index metadata rows: %v", err)
		}
	}()

	out := make(map[string]indexMetadata)
	for rows.Next() {
		var meta indexMetadata
		if err := rows.Scan(&meta.TableName, &meta.ColumnNames, &meta.Unique); err != nil {
			t.Fatalf("scan index metadata: %v", err)
		}
		out[fmt.Sprintf("%s.%s.unique=%t", meta.TableName, meta.ColumnNames, meta.Unique)] = meta
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index metadata: %v", err)
	}
	return out
}

func loadConfigStoreForeignKeys(t *testing.T, db *sql.DB) map[string]foreignKeyMetadata {
	t.Helper()

	rows, err := db.Query(`
		SELECT
			src.relname AS table_name,
			string_agg(src_col.attname, ',' ORDER BY src_cols.ord) AS column_names,
			dst.relname AS foreign_table_name,
			string_agg(dst_col.attname, ',' ORDER BY src_cols.ord) AS foreign_column_names,
			CASE c.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE c.confdeltype::text
			END AS delete_rule
		FROM pg_constraint c
		JOIN pg_class src ON src.oid = c.conrelid
		JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
		JOIN pg_class dst ON dst.oid = c.confrelid
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON true
		JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS dst_cols(attnum, ord) ON dst_cols.ord = src_cols.ord
		JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = src_cols.attnum
		JOIN pg_attribute dst_col ON dst_col.attrelid = dst.oid AND dst_col.attnum = dst_cols.attnum
		WHERE c.contype = 'f'
		  AND src_ns.nspname = current_schema()
		  AND src.relname IN (
			'duckgres_orgs',
			'duckgres_org_users',
			'duckgres_org_teams',
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
			'duckgres_operators',
			'duckgres_global_config',
			'duckgres_ducklake_config',
			'duckgres_rate_limit_config',
			'duckgres_query_log_config'
		  )
		GROUP BY c.oid, src.relname, dst.relname, c.confdeltype
		ORDER BY src.relname, column_names, dst.relname
	`)
	if err != nil {
		t.Fatalf("query foreign key metadata: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close foreign key metadata rows: %v", err)
		}
	}()

	out := make(map[string]foreignKeyMetadata)
	for rows.Next() {
		var meta foreignKeyMetadata
		if err := rows.Scan(
			&meta.TableName,
			&meta.ColumnNames,
			&meta.ForeignTableName,
			&meta.ForeignColumnNames,
			&meta.DeleteRule,
		); err != nil {
			t.Fatalf("scan foreign key metadata: %v", err)
		}
		out[meta.TableName+"."+meta.ColumnNames+"->"+meta.ForeignTableName+"."+meta.ForeignColumnNames] = meta
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate foreign key metadata: %v", err)
	}
	return out
}

func metadataDiff[T any](migrated, gorm map[string]T) string {
	keySet := make(map[string]struct{}, len(migrated)+len(gorm))
	for key := range migrated {
		keySet[key] = struct{}{}
	}
	for key := range gorm {
		keySet[key] = struct{}{}
	}

	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var out string
	for _, key := range keys {
		migratedValue, migratedOK := migrated[key]
		gormValue, gormOK := gorm[key]
		if migratedOK && gormOK && reflect.DeepEqual(migratedValue, gormValue) {
			continue
		}
		out += fmt.Sprintf("%s\n  migrated: %#v\n  gorm:     %#v\n", key, migratedValue, gormValue)
	}
	return out
}
