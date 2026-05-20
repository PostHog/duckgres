//go:build k8s_integration

package k8s_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestK8sIcebergRoundTrip exercises the per-tenant Iceberg-on-S3-Tables
// activation path against REAL AWS — kind cluster, real control plane,
// real worker pod, real ATTACH 'arn:aws:s3tables:...' (TYPE iceberg,
// ENDPOINT_TYPE 's3_tables') against actual S3 Tables. This is the only
// way to gain high confidence in the iceberg mode: every alternative
// (LocalStack community, moto, REST-catalog substitutes) exercises a
// DIFFERENT code path in the DuckDB iceberg extension. The 's3_tables'
// endpoint is derived from the ARN's region and goes straight to AWS; no
// environment flag overrides it.
//
// SCOPE — what this test covers and what it doesn't, and why.
//
// Covers (the wiring underneath the catalog SQL surface). This is what
// the iceberg integration test is *for* — every piece of glue between
// the control plane, the worker pod, and AWS:
//   - control plane reads the tenant's S3 credentials secret, including
//     the STS session_token (regression-tests the fix in PR #569 where
//     ASIA…-prefixed temporary credentials were dropping the token and
//     getting 403s from the iceberg REST endpoint)
//   - worker activation loads the iceberg extension, creates the
//     SigV4-bearing TYPE S3 secret, ATTACHes the per-tenant S3 Tables
//     bucket using the OIDC-vended CI role, and runs the
//     `SHOW TABLES FROM iceberg` probe that hits real S3 Tables APIs
//     (ListNamespaces + ListTables under the hood)
//   - the catalog ends up visible in `duckdb_databases()` from a
//     subsequent client session — i.e. the activation actually
//     completed and didn't silently DETACH
//
// Does NOT cover (blocked upstream — DuckDB iceberg extension bug):
//   - `USE iceberg.<ns>`         → "No catalog + schema named ... found"
//   - `CREATE TABLE iceberg.ns.t` → "Schema with name "" not found"
//   - `SELECT FROM iceberg.ns.t`  → "schema main does not exist"
//   - `INSERT INTO iceberg.ns.t`  → same
//
// Reproduced against plain `duckdb-go` v2 (no duckgres) on both the
// stable (v11fea8ed) and core_nightly (v10e97957) iceberg extensions.
// `information_schema.schemata`, `duckdb_schemas()`, `SHOW TABLES FROM
// iceberg` and `SHOW ALL TABLES` all see the namespace+table, but the
// schema-by-name lookup path used by USE/CREATE/SELECT/INSERT disagrees
// and reports the schema as missing. So once a tenant's iceberg catalog
// is attached, the only thing currently usable through DuckDB SQL is
// metadata listing — actual table read/write requires going through
// PyIceberg/Spark/Athena/etc until the upstream bug is fixed. Expand
// this test once that resolves.
//
// Why the test doesn't pre-create a probe table for read-side coverage:
// AttachIcebergCatalog runs a `SHOW TABLES FROM iceberg` probe right
// after ATTACH; if it errors with a "no namespace / no such table"
// pattern, the activator treats the catalog as freshly empty and
// DETACHes it. With an empty-metadata table present in the namespace
// (the shape `aws s3tables create-table` produces — no data files
// yet), the worker's iceberg ext hits one of those probe errors and
// detaches, leaving `duckdb_databases()` count = 0. A workaround
// would have to either populate the table via Spark/PyIceberg in test
// setup (heavyweight) or relax the activator's detach heuristic
// (touches production code for one test). Until we resolve that
// trade-off, the test stays at activation-level coverage — which is
// where the wiring bugs we just spent days fixing actually live.
//
// This test is INTENTIONALLY NOT SKIPPABLE. If the required env vars
// aren't set in whatever CI lane runs the k8s integration suite, the
// test fails openly with a clear diagnostic. A silent skip would hide
// two failure modes that matter more than the test itself:
//
//  1. CI misconfiguration — a secret rotates, an env var name changes,
//     the sandbox bucket gets renamed, and the test silently stops
//     running. With a skip, nobody notices until someone actively
//     looks at the test output; with a fatal, the next PR catches it.
//  2. A real iceberg regression that happens to coincide with an
//     env-var gap — even worse, because the regression hides behind
//     the same "skipped — missing env vars" line.
//
// Required env vars (test fails the whole job when any is empty):
//
//	DUCKGRES_K8S_ICEBERG_TABLE_BUCKET_ARN   — arn:aws:s3tables:<region>:<acct>:bucket/<name>
//	DUCKGRES_K8S_ICEBERG_REGION             — must match the ARN's region
//	DUCKGRES_K8S_ICEBERG_DATA_BUCKET        — real S3 bucket name for DuckLake parquet
//	                                          (DuckLake is attached alongside iceberg;
//	                                          empty ObjectStore would skip the attach
//	                                          but we want both code paths exercised)
//	AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY — credentials with s3tables:* on the table
//	                                          bucket and s3:* on the data bucket
//	AWS_SESSION_TOKEN                       — required for STS-vended creds (the OIDC
//	                                          assume-role path always returns one)
//
// Optional:
//
//	DUCKGRES_K8S_ICEBERG_NAMESPACE          — defaults to "main"
//
// CI sandbox bucket guidance:
//   - Use a SINGLE persistent table bucket per CI environment; the test
//     creates+drops a uniquely-named probe table on each run. Avoids the
//     10-bucket-per-region service quota and saves ~30s/run vs.
//     create-bucket-per-run.
//   - The IAM principal needs s3tables:CreateTable, GetTable, DeleteTable,
//     ListTables, GetNamespace on the table bucket, plus s3:GetObject/
//     PutObject on the data bucket.
//   - DO NOT reuse a production bucket. The test creates and drops tables;
//     a leaked DROP would target whatever bucket the env var pointed at.
func TestK8sIcebergRoundTrip(t *testing.T) {
	cfg := loadIcebergTestConfig(t)

	if err := seedIcebergTenantFixture(cfg); err != nil {
		t.Fatalf("seed iceberg tenant fixture: %v", err)
	}

	// Wait for the new tenant DB to be reachable — the control plane's
	// configstore poll picks up the new org on its next tick. This
	// triggers worker activation, which in turn runs AttachIcebergCatalog
	// against the real S3 Tables bucket using the OIDC-vended creds.
	if err := waitForTenantDBReady(icebergTenantName, icebergTenantPassword, initialDBReadyTimeout); err != nil {
		t.Fatalf("iceberg tenant login not ready: %v", err)
	}

	// Confirm the iceberg catalog actually attached on the worker. If
	// the ATTACH failed (wrong region, missing s3tables permission,
	// missing session_token for STS-vended creds) the activation path
	// logs+returns rather than silently skipping, so the catalog either
	// shows up here or the session never came up.
	//
	// A count == 1 result demonstrates the full activation pipeline
	// worked end-to-end: control plane resolved the tenant config,
	// looked up the S3 credentials secret (with session_token plumbing,
	// the regression in PR #569), STS broker minted vended credentials
	// where applicable, worker pod started, iceberg extension installed,
	// TYPE S3 PROVIDER config secret created with SigV4 region, ATTACH
	// hit S3 Tables, the post-attach `SHOW TABLES FROM iceberg` probe
	// reached the listing endpoint without a detach-on-empty error, and
	// a client session's flight call to `duckdb_databases()` was routed
	// to the activated worker.
	//
	// Why we poll instead of a one-shot query: waitForTenantDBReady
	// returns as soon as `SELECT 1` succeeds, which can race the worker
	// activation's AttachIcebergCatalog step — the auth/SELECT path
	// completes before the iceberg ext finishes its
	// install+secret+ATTACH+probe sequence against AWS. A one-shot count
	// query against that window returns 0 spuriously. Retrying until
	// the catalog shows up (with a hard upper bound) catches genuine
	// activation failures while tolerating the ordinary multi-second
	// ATTACH latency.
	if err := pollIcebergAttached(60 * time.Second); err != nil {
		t.Fatalf("iceberg catalog not attached after activation: %v (ATTACH against %s probably failed — check control-plane logs)", err, cfg.tableBucketARN)
	}
}

// pollIcebergAttached polls the iceberg tenant's session for the
// attached-catalog count until it reads 1 or the timeout elapses. See
// the call site for the race this paves over.
func pollIcebergAttached(timeout time.Duration) error {
	const q = "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'iceberg'"
	deadline := time.Now().Add(timeout)
	for {
		got, err := queryIntWithReconnectAs(icebergTenantName, icebergTenantPassword, q, timeout)
		if err == nil && got == 1 {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("last query error: %w", err)
			}
			return fmt.Errorf("catalog still not attached (last count=%d)", got)
		}
		time.Sleep(2 * time.Second)
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

// loadIcebergTestConfig reads the required env vars and fails the test
// loudly if any are missing. There is no skip path — see the
// TestK8sIcebergRoundTrip godoc for the rationale.
//
// Note that the env vars must be present *and non-empty*; an empty
// value is treated as missing. This matters when CI passes secrets
// through templated workflow files: a rotated-out secret typically
// renders as empty rather than absent, and an empty value here would
// silently fail the AWS call rather than the env check.
func loadIcebergTestConfig(t *testing.T) icebergTestConfig {
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
		t.Fatalf(`iceberg integration test cannot run — required env vars are unset or empty: %s.

This test is intentionally NOT skippable: a silent skip would hide CI
misconfiguration (rotated secret, renamed bucket, dropped env var) and,
worse, would mask any real iceberg regression that happened to land at
the same time as the env-var gap.

To wire the iceberg CI lane:
  - provision a persistent sandbox S3 Tables bucket + companion data bucket
    in your sandbox AWS account
  - grant the CI IAM principal s3tables:* on the table bucket and
    s3:GetObject/PutObject on the data bucket
  - set all of the env vars above as CI secrets

See TestK8sIcebergRoundTrip godoc for the full setup notes. Until the
iceberg lane is wired, this failure is the correct signal that work
remains.`, strings.Join(missing, ", "))
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
	}
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
// state='ready' so the activator doesn't wait on the provisioner), in
// the S3 endpoint (real AWS regional endpoint instead of MinIO), and in
// having s3_delta_catalog_enabled = false.
//
// Why disable Delta: ManagedWarehouseS3.DeltaCatalogEnabled defaults to
// true (GORM `default:true` on the column), and during activation the
// worker runs a `_delta_log/_last_checkpoint` probe via the DuckDB
// delta extension to discover whether the catalog exists. On this
// tenant the probe issues a GET that resolves to a URL whose bucket
// segment isn't the tenant's data bucket — first observed as a
// 403 against `https://s3.us-east-1.amazonaws.com/orgs/delta/_delta_log/_last_checkpoint`
// despite the IAM role granting s3:GetObject on the data bucket. The
// iceberg integration test doesn't need Delta (it tests
// iceberg-on-S3-Tables + DuckLake only), so we turn the probe off
// rather than chase the URL-construction bug here. Re-enable + chase
// the underlying delta path bug when this test grows a delta scenario.
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
    s3_delta_catalog_enabled,
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
    false,
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
    s3_delta_catalog_enabled = EXCLUDED.s3_delta_catalog_enabled,
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

