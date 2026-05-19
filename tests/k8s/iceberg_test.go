//go:build k8s_integration

package k8s_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestK8sIcebergRoundTrip exercises the full per-tenant Iceberg-on-S3-Tables
// path against REAL AWS — kind cluster, real control plane, real worker
// pod, real ATTACH 'arn:aws:s3tables:...' (TYPE iceberg, ENDPOINT_TYPE
// 's3_tables') against actual S3 Tables. This is the only way to gain
// high confidence in the iceberg mode: every alternative (LocalStack
// community, moto, REST-catalog substitutes) exercises a DIFFERENT code
// path in the DuckDB iceberg extension. The 's3_tables' endpoint is
// derived from the ARN's region and goes straight to AWS; no environment
// flag overrides it.
//
// CI mechanics: the test is hard-gated on a persistent sandbox S3 Tables
// bucket (the recommended setup — see below). Skips locally and in any
// CI job that doesn't set DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN, so PR CI
// stays fast and free; a dedicated job (nightly or "iceberg" lane) sets
// the env vars and gets the real signal.
//
// Required env vars (test skips with a clear message when any is empty):
//
//	DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN   — arn:aws:s3tables:<region>:<acct>:bucket/<name>
//	DUCKGRES_K8S_ICEBERG_REGION             — must match the ARN's region
//	DUCKGRES_K8S_ICEBERG_DATA_BUCKET        — real S3 bucket name for DuckLake parquet
//	                                          (DuckLake is attached alongside iceberg;
//	                                          empty ObjectStore would skip the attach
//	                                          but we want both code paths exercised)
//	AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY — credentials with s3tables:* on the table
//	                                          bucket and s3:* on the data bucket
//	AWS_SESSION_TOKEN                       — optional; required for STS-vended creds
//
// Optional:
//
//	DUCKGRES_K8S_ICEBERG_NAMESPACE          — defaults to "main"
//
// CI sandbox bucket guidance:
//   - Use a SINGLE persistent table bucket per CI environment; tests
//     create tables with a unique suffix and DROP in cleanup. Avoids the
//     10-bucket-per-region service quota and saves ~30s/run vs.
//     create-bucket-per-run.
//   - The IAM principal needs s3tables:CreateTable, GetTable, DeleteTable,
//     GetTableMetadataLocation, UpdateTableMetadataLocation on the table
//     bucket, plus s3:GetObject/PutObject on the data bucket.
//   - DO NOT reuse a production bucket. The test creates and drops tables;
//     a leaked DROP would target whatever bucket the env var pointed at.
func TestK8sIcebergRoundTrip(t *testing.T) {
	cfg, ok := loadIcebergTestConfigOrSkip(t)
	if !ok {
		return
	}

	if err := seedIcebergTenantFixture(cfg); err != nil {
		t.Fatalf("seed iceberg tenant fixture: %v", err)
	}

	// Wait for the new tenant DB to be reachable — the control plane's
	// configstore poll picks up the new org on its next tick.
	if err := waitForTenantDBReady(icebergTenantName, icebergTenantPassword, initialDBReadyTimeout); err != nil {
		t.Fatalf("iceberg tenant login not ready: %v", err)
	}

	// Confirm the iceberg catalog actually attached on the worker. If the
	// ATTACH failed (e.g. wrong region, missing s3tables permission), this
	// is where we'd see it — the activation path logs+returns rather than
	// silently skipping, so the catalog is either present or the session
	// never came up.
	attached, err := queryIntWithReconnectAs(icebergTenantName, icebergTenantPassword,
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'iceberg'", 60*time.Second)
	if err != nil {
		t.Fatalf("query iceberg attach state: %v", err)
	}
	if attached != 1 {
		t.Fatalf("iceberg catalog not attached (count=%d); ATTACH against %s probably failed — check control-plane logs", attached, cfg.tableBucketARN)
	}

	// Real S3 Tables round trip. Single-row table is intentional: we are
	// testing the wiring, not throughput.
	tableSuffix := time.Now().UnixNano()
	fqTable := fmt.Sprintf("iceberg.%s.t_%d", cfg.namespace, tableSuffix)

	if err := retryDBOperationWithReconnectAs(icebergTenantName, icebergTenantPassword, 60*time.Second, "create iceberg table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "CREATE TABLE "+fqTable+" (id INTEGER, label VARCHAR)")
		return err
	}); err != nil {
		t.Fatalf("CREATE TABLE against real S3 Tables: %v", err)
	}
	t.Cleanup(func() {
		_ = retryDBOperationWithReconnectAs(icebergTenantName, icebergTenantPassword, 30*time.Second, "drop iceberg table", func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+fqTable)
			return err
		})
	})

	if err := retryDBOperationWithReconnectAs(icebergTenantName, icebergTenantPassword, 60*time.Second, "insert iceberg rows", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "INSERT INTO "+fqTable+" VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
		return err
	}); err != nil {
		t.Fatalf("INSERT into iceberg table: %v", err)
	}

	count, err := queryIntWithReconnectAs(icebergTenantName, icebergTenantPassword,
		"SELECT COUNT(*) FROM "+fqTable, 60*time.Second)
	if err != nil {
		t.Fatalf("SELECT from iceberg table: %v", err)
	}
	if count != 3 {
		t.Fatalf("iceberg roundtrip row count = %d, want 3", count)
	}
}

const (
	icebergTenantName     = "iceberg-test"
	icebergTenantPassword = "postgres"
	// bcrypt hash of "postgres", matching the existing tenant fixtures so
	// the auth wiring is identical to analytics/billing.
	icebergTenantPasswordHash = "$2a$10$TQyt73Vw91Q1d7YcE86EVuhms/0u4qBydMDyVvZYlqDwc3/VtQAbm"
)

type icebergTestConfig struct {
	tableBucketARN string
	region         string
	namespace      string
	dataBucket     string
	accessKeyID    string
	secretKey      string
	sessionToken   string
}

func loadIcebergTestConfigOrSkip(t *testing.T) (icebergTestConfig, bool) {
	t.Helper()
	required := map[string]string{
		"DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN": os.Getenv("DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN"),
		"DUCKGRES_K8S_ICEBERG_REGION":           os.Getenv("DUCKGRES_K8S_ICEBERG_REGION"),
		"DUCKGRES_K8S_ICEBERG_DATA_BUCKET":      os.Getenv("DUCKGRES_K8S_ICEBERG_DATA_BUCKET"),
		"AWS_ACCESS_KEY_ID":                     os.Getenv("AWS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY":                 os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
	var missing []string
	for k, v := range required {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		t.Skipf("real-AWS iceberg test skipped — missing env vars: %s. Set them in the iceberg CI lane against a sandbox S3 Tables bucket; see TestK8sIcebergRoundTrip godoc for setup.", strings.Join(missing, ", "))
		return icebergTestConfig{}, false
	}
	ns := os.Getenv("DUCKGRES_K8S_ICEBERG_NAMESPACE")
	if ns == "" {
		ns = "main"
	}
	return icebergTestConfig{
		tableBucketARN: required["DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN"],
		region:         required["DUCKGRES_K8S_ICEBERG_REGION"],
		namespace:      ns,
		dataBucket:     required["DUCKGRES_K8S_ICEBERG_DATA_BUCKET"],
		accessKeyID:    required["AWS_ACCESS_KEY_ID"],
		secretKey:      required["AWS_SECRET_ACCESS_KEY"],
		sessionToken:   os.Getenv("AWS_SESSION_TOKEN"),
	}, true
}

// seedIcebergTenantFixture installs everything the iceberg tenant needs:
//   - k8s secrets in the duckgres namespace (warehouse DB DSN, ducklake
//     metadata DSN, S3 creds payload, runtime config). The S3 creds carry
//     the REAL AWS keys — they're consumed both by DuckLake (against the
//     real data bucket) and by the iceberg extension (against S3 Tables)
//     because AttachIcebergCatalog reuses DuckLake.S3* per the comment in
//     server/iceberg/migration.go.
//   - A dedicated DuckLake metadata DB on the local Postgres so this
//     tenant doesn't share metadata with the default 'local' fixture.
//   - A row in duckgres_managed_warehouses with iceberg_* fields populated
//     and state='ready', so the activator picks up the config without
//     waiting on the provisioner controller (kind has no Duckling CR).
//   - An org + an org-user.
func seedIcebergTenantFixture(cfg icebergTestConfig) error {
	if err := ensurePostgresDatabase(duckLakeMetadataContainer, "ducklake", "ducklake_metadata_iceberg"); err != nil {
		return fmt.Errorf("create iceberg ducklake metadata DB: %w", err)
	}

	s3CredsPayload := map[string]string{
		"access_key_id":     cfg.accessKeyID,
		"secret_access_key": cfg.secretKey,
	}
	if cfg.sessionToken != "" {
		s3CredsPayload["session_token"] = cfg.sessionToken
	}
	s3CredsJSON, err := json.Marshal(s3CredsPayload)
	if err != nil {
		return fmt.Errorf("marshal s3 creds payload: %w", err)
	}

	secrets := map[string]map[string]string{
		"iceberg-test-warehouse-db": {"dsn": "duckgres"},
		"iceberg-test-metadata":     {"dsn": "ducklake"},
		"iceberg-test-s3":           {"credentials": string(s3CredsJSON)},
		"iceberg-test-runtime":      {"duckgres.yaml": baseTenantRuntimeConfig()},
	}
	for name, data := range secrets {
		if err := upsertTenantIsolationSecret(name, data); err != nil {
			return fmt.Errorf("upsert secret %s: %w", name, err)
		}
	}

	seed := buildIcebergConfigStoreSeed(cfg)
	if err := applyConfigStoreSeedInline(seed); err != nil {
		return fmt.Errorf("apply iceberg seed: %w", err)
	}
	return nil
}

// buildIcebergConfigStoreSeed constructs the SQL that registers the
// iceberg-test org + warehouse + user. Mirrors the column set used by
// k8s/kind/config-store.seed.sql and tenant-isolation.seed.sql; diverges
// only where iceberg matters (iceberg_enabled, table bucket ARN/region,
// state='ready' so the activator doesn't wait on the provisioner) and in
// the S3 endpoint (real AWS regional endpoint instead of MinIO).
func buildIcebergConfigStoreSeed(cfg icebergTestConfig) string {
	return fmt.Sprintf(`
INSERT INTO duckgres_orgs (name, database_name, max_workers, memory_budget, idle_timeout_s, created_at, updated_at)
VALUES ('%s', '%s', 0, '', 0, NOW(), NOW())
ON CONFLICT (name) DO UPDATE SET updated_at = NOW();

INSERT INTO duckgres_managed_warehouses (
    org_id, image, aurora_min_acu, aurora_max_acu,
    warehouse_database_region, warehouse_database_endpoint, warehouse_database_port,
    warehouse_database_database_name, warehouse_database_username,
    metadata_store_kind, metadata_store_engine, metadata_store_region,
    metadata_store_endpoint, metadata_store_port, metadata_store_database_name, metadata_store_username,
    s3_provider, s3_region, s3_bucket, s3_path_prefix, s3_endpoint, s3_use_ssl, s3_url_style,
    iceberg_enabled, iceberg_table_bucket_arn, iceberg_region, iceberg_namespace,
    worker_identity_namespace, worker_identity_service_account_name, worker_identity_iam_role_arn,
    warehouse_database_credentials_namespace, warehouse_database_credentials_name, warehouse_database_credentials_key,
    metadata_store_credentials_namespace, metadata_store_credentials_name, metadata_store_credentials_key,
    s3_credentials_namespace, s3_credentials_name, s3_credentials_key,
    runtime_config_namespace, runtime_config_name, runtime_config_key,
    state, status_message,
    warehouse_database_state, warehouse_database_status_message,
    metadata_store_state, metadata_store_status_message,
    s3_state, s3_status_message,
    iceberg_state, iceberg_status_message,
    identity_state, identity_status_message,
    secrets_state, secrets_status_message,
    ready_at, failed_at, created_at, updated_at
) VALUES (
    '%s', '', 0, 0,
    '%s', 'local-warehouse-db', 5432, 'duckgres_local', 'duckgres',
    'dedicated_rds', 'postgres', '%s',
    'duckgres-local-ducklake-metadata', 5432, 'ducklake_metadata_iceberg', 'ducklake',
    'aws', '%s', '%s', 'orgs/iceberg-test/',
    's3.%s.amazonaws.com', true, 'vhost',
    true, '%s', '%s', '%s',
    'duckgres', 'duckgres-local-worker', 'arn:aws:iam::000000000000:role/duckgres-iceberg-test',
    'duckgres', 'iceberg-test-warehouse-db', 'dsn',
    'duckgres', 'iceberg-test-metadata', 'dsn',
    'duckgres', 'iceberg-test-s3', 'credentials',
    'duckgres', 'iceberg-test-runtime', 'duckgres.yaml',
    'ready', 'iceberg integration test',
    'ready', '', 'ready', '', 'ready', '',
    'ready', 'iceberg bucket sandbox',
    'ready', '', 'ready', '',
    NOW(), NULL, NOW(), NOW()
) ON CONFLICT (org_id) DO UPDATE SET
    s3_region = EXCLUDED.s3_region,
    s3_bucket = EXCLUDED.s3_bucket,
    s3_endpoint = EXCLUDED.s3_endpoint,
    s3_use_ssl = EXCLUDED.s3_use_ssl,
    s3_url_style = EXCLUDED.s3_url_style,
    iceberg_enabled = EXCLUDED.iceberg_enabled,
    iceberg_table_bucket_arn = EXCLUDED.iceberg_table_bucket_arn,
    iceberg_region = EXCLUDED.iceberg_region,
    iceberg_namespace = EXCLUDED.iceberg_namespace,
    iceberg_state = EXCLUDED.iceberg_state,
    state = EXCLUDED.state,
    updated_at = NOW();

INSERT INTO duckgres_org_users (username, password, org_id, created_at, updated_at)
VALUES ('%s', '%s', '%s', NOW(), NOW())
ON CONFLICT (org_id, username) DO UPDATE SET password = EXCLUDED.password, updated_at = NOW();
`,
		icebergTenantName, icebergTenantName,
		icebergTenantName,
		cfg.region, cfg.region,
		cfg.region, cfg.dataBucket, cfg.region,
		cfg.tableBucketARN, cfg.region, cfg.namespace,
		icebergTenantName, icebergTenantPasswordHash, icebergTenantName,
	)
}

// applyConfigStoreSeedInline pipes a SQL string into the config-store
// container — mirrors applyConfigStoreSeedFixture but takes a string
// instead of a file, since the seed is parameterized by env-var values.
func applyConfigStoreSeedInline(sql string) error {
	cmd := exec.Command(
		"docker", "exec", "-i", configStoreContainer,
		"psql", "-v", "ON_ERROR_STOP=1", "-U", "duckgres", "-d", "duckgres_config",
	)
	cmd.Stdin = strings.NewReader(sql)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("psql exec: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
