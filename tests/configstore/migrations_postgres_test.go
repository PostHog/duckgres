//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
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
	requireGooseLatestVersion(t, db, 4)
	requireTableAbsent(t, db, "duckgres_schema_migrations")

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

	for _, columnName := range []string{"retention_period_s", "retention_interval_s"} {
		var queryLogColumnCount int
		if err := store.DB().Raw(`
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'duckgres_query_log_config'
			  AND column_name = ?
		`, columnName).Scan(&queryLogColumnCount).Error; err != nil {
			t.Fatalf("query %s column: %v", columnName, err)
		}
		if queryLogColumnCount != 1 {
			t.Fatalf("%s column count = %d, want 1", columnName, queryLogColumnCount)
		}
	}
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
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		);
		CREATE UNIQUE INDEX idx_duckgres_orgs_database_name ON duckgres_orgs(database_name);
		CREATE UNIQUE INDEX idx_duckgres_orgs_hostname_alias ON duckgres_orgs(hostname_alias);
		INSERT INTO duckgres_orgs (name, database_name, created_at, updated_at)
		VALUES ('old-org', 'old-org', now(), now());
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
	requireGooseMigrationRecorded(t, sqlDB, 3)
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

	if _, err := db.Exec(`
		CREATE TABLE duckgres_orgs (
			name VARCHAR(255) PRIMARY KEY,
			database_name VARCHAR(255),
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
		INSERT INTO duckgres_orgs (name, database_name, created_at, updated_at)
		VALUES ('old-org', 'old-org', now(), now());
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
		&cpconfigstore.OrgUserSecret{},
		&cpconfigstore.GlobalConfig{},
		&cpconfigstore.DuckLakeConfig{},
		&cpconfigstore.RateLimitConfig{},
		&cpconfigstore.QueryLogConfig{},
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

func loadConfigStoreColumnMetadata(t *testing.T, db *sql.DB) map[string]columnMetadata {
	t.Helper()

	rows, err := db.Query(`
		SELECT table_name, column_name, is_nullable, data_type, character_maximum_length, column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name IN (
			'duckgres_orgs',
			'duckgres_org_users',
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
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
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
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
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
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
			'duckgres_org_user_secrets',
			'duckgres_managed_warehouses',
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
