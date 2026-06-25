package integration

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
)

// rotationScenario drives the shared credential-rotation experiment:
//
// A compute-heavy glob scan over many parquet files starts with user-A
// credentials; mid-statement the secret is rotated to user B via
// server.RefreshS3Secret (exactly what the credential-refresh scheduler
// pushes to workers) and user A is then DISABLED in MinIO (simulating STS
// token expiry). Whether the in-flight statement survives depends on the
// httpfs build — see the two tests below.
type rotationScenario struct {
	// loadHTTPFS installs/loads httpfs on a fresh DB handle. Distinguishes
	// the stock extension (INSTALL httpfs from the default repo) from a
	// locally-built patched one (LOAD '/path/httpfs.duckdb_extension').
	loadHTTPFS func(t *testing.T, db *sql.DB)
	// dsn for sql.Open("duckdb", ...) — the patched build is unsigned and
	// needs allow_unsigned_extensions.
	dsn string
}

type rotationResult struct {
	cnt       int64
	err       error
	duration  time.Duration
	rotatedAt time.Duration
}

const (
	rotationMinioAddr      = "localhost:39000"
	rotationMinioContainer = "duckgres-test-minio"
	rotationBucket         = "rotation-pin"
	rotationUserA          = "rotuser-a"
	rotationUserB          = "rotuser-b"
	rotationPassword       = "rotating-pass-12345"
	rotationNumFiles       = 48
	rotationRowsPerFile    = 60000
)

func runRotationScenario(t *testing.T, sc rotationScenario) rotationResult {
	t.Helper()
	if testing.Short() {
		t.Skip("credential rotation test skipped in -short mode")
	}

	if conn, err := net.DialTimeout("tcp", rotationMinioAddr, 2*time.Second); err != nil {
		t.Skipf("MinIO not reachable on %s (run docker compose in tests/integration): %v", rotationMinioAddr, err)
	} else {
		_ = conn.Close()
	}

	mc := func(args ...string) (string, error) {
		out, err := exec.Command("docker", append([]string{"exec", rotationMinioContainer, "mc"}, args...)...).CombinedOutput()
		return string(out), err
	}
	if out, err := mc("ready", "local"); err != nil {
		t.Skipf("mc not available in MinIO container (%v): %s", err, out)
	}
	// The image's built-in `local` alias is anonymous (enough for the compose
	// healthcheck, not for admin ops) — register an authenticated alias.
	if out, err := mc("alias", "set", "localadmin", "http://localhost:9000", "minioadmin", "minioadmin"); err != nil {
		t.Skipf("could not configure authenticated mc alias (%v): %s", err, out)
	}

	// --- MinIO fixtures: private bucket + two users with readwrite policy ---
	if out, err := mc("mb", "localadmin/"+rotationBucket, "--ignore-existing"); err != nil {
		t.Fatalf("create bucket: %v: %s", err, out)
	}
	for _, u := range []string{rotationUserA, rotationUserB} {
		_, _ = mc("admin", "user", "rm", "localadmin", u) // idempotent re-runs
		if out, err := mc("admin", "user", "add", "localadmin", u, rotationPassword); err != nil {
			t.Fatalf("add user %s: %v: %s", u, err, out)
		}
		if out, err := mc("admin", "policy", "attach", "localadmin", "readwrite", "--user", u); err != nil &&
			!strings.Contains(out, "already") {
			t.Fatalf("attach policy to %s: %v: %s", u, err, out)
		}
	}
	t.Cleanup(func() {
		_, _ = mc("admin", "user", "enable", "localadmin", rotationUserA)
		_, _ = mc("admin", "user", "rm", "localadmin", rotationUserA)
		_, _ = mc("admin", "user", "rm", "localadmin", rotationUserB)
		_, _ = mc("rb", "--force", "localadmin/"+rotationBucket)
	})

	// --- Seed parquet data with admin credentials (separate DuckDB) ---
	{
		seedDB, err := sql.Open("duckdb", sc.dsn)
		if err != nil {
			t.Fatalf("open seed duckdb: %v", err)
		}
		defer func() { _ = seedDB.Close() }()
		sc.loadHTTPFS(t, seedDB)
		if _, err := seedDB.Exec(fmt.Sprintf(`CREATE SECRET seed (
			TYPE s3, PROVIDER config,
			KEY_ID 'minioadmin', SECRET 'minioadmin', SESSION_TOKEN '',
			ENDPOINT '%s', USE_SSL false, URL_STYLE 'path', REGION 'us-east-1')`, rotationMinioAddr)); err != nil {
			t.Fatalf("create seed secret: %v", err)
		}
		for i := 0; i < rotationNumFiles; i++ {
			if _, err := seedDB.Exec(fmt.Sprintf(
				`COPY (SELECT md5(range::VARCHAR) AS s, range AS val FROM range(%d)) TO 's3://%s/data/part_%d.parquet'`,
				rotationRowsPerFile, rotationBucket, i)); err != nil {
				t.Fatalf("seed part_%d: %v", i, err)
			}
		}
	}

	// --- System under test: production-shape PROVIDER config secret ---
	dlCfg := server.DuckLakeConfig{
		ObjectStore:    fmt.Sprintf("s3://%s/", rotationBucket),
		S3Provider:     "config",
		S3Endpoint:     rotationMinioAddr,
		S3Region:       "us-east-1",
		S3URLStyle:     "path",
		S3UseSSL:       false,
		S3AccessKey:    rotationUserA,
		S3SecretKey:    rotationPassword,
		S3SessionToken: "", // MinIO users have no session token; explicit empty is the contract
	}

	db, err := sql.Open("duckdb", sc.dsn)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4) // conn = pinned query conn; refresh uses a side conn (mirrors worker controlDB)

	sc.loadHTTPFS(t, db)
	// SET threads is session-scoped; pin one connection for the scan so the
	// setting and the long statement share a connection, and file opens are
	// spread across the whole scan (sequential reads).
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("pin query conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(t.Context(), "SET threads = 1"); err != nil {
		t.Fatalf("set threads: %v", err)
	}

	// Initial secret exactly as a worker activation creates it.
	if err := server.RefreshS3Secret(db, dlCfg, nil); err != nil {
		t.Fatalf("create initial secret: %v", err)
	}

	// MD5-chain scan: compute-heavy enough that the statement is still
	// running when we rotate at T+4s, sequentially opening ~1 file at a time.
	scanQuery := fmt.Sprintf(
		`SELECT count(*), max(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(md5(s))))))))))))))))))))) FROM read_parquet('s3://%s/data/*.parquet')`,
		rotationBucket)

	done := make(chan rotationResult, 1)
	started := time.Now()
	go func() {
		var r rotationResult
		var maxOut string
		r.err = conn.QueryRowContext(t.Context(), scanQuery).Scan(&r.cnt, &maxOut)
		r.duration = time.Since(started)
		done <- r
	}()

	// T+4s: rotate to user B exactly the way the credential-refresh
	// scheduler's push lands on a worker (CREATE OR REPLACE SECRET on a side
	// connection), then kill user A — simulating the old STS token expiring
	// right after a rotation that any reasonable design would hope protects
	// the running statement.
	time.Sleep(4 * time.Second)
	rotated := dlCfg
	rotated.S3AccessKey = rotationUserB
	if err := server.RefreshS3Secret(db, rotated, nil); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}
	if out, err := mc("admin", "user", "disable", "localadmin", rotationUserA); err != nil {
		t.Fatalf("disable %s: %v: %s", rotationUserA, err, out)
	}
	rotatedAt := time.Since(started)

	r := <-done
	r.rotatedAt = rotatedAt
	if r.err == nil && r.duration <= rotatedAt {
		t.Fatalf("scan finished in %v, before the rotation at %v — the test proved nothing, increase data size",
			r.duration, rotatedAt)
	}

	// The other half of the contract — and what the credential-refresh
	// scheduler actually guarantees: a FRESH statement on the same session
	// (user A still disabled) works off the rotated secret.
	var n int64
	if err := conn.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM read_parquet('s3://%s/data/part_0.parquet')`, rotationBucket)).Scan(&n); err != nil {
		t.Fatalf("post-rotation fresh statement failed — secret rotation is broken for NEW statements too: %v", err)
	}
	if want := int64(rotationRowsPerFile); n != want {
		t.Fatalf("post-rotation fresh statement returned %d rows, want %d", n, want)
	}
	return r
}

// loadStockHTTPFS installs the stock httpfs extension from the default
// extension repository (needs network; skips when unavailable).
func loadStockHTTPFS(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{"INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.Exec(stmt); err != nil {
			t.Skipf("httpfs unavailable (network/sandbox): %v", err)
		}
	}
}

// TestInFlightScanDiesOnCredentialRotation PINS a stock-DuckDB limitation that
// the control plane's STS freshness floor (stsCacheSafetyMargin in
// controlplane/sts_broker.go) is designed around:
//
//	An in-flight scan does NOT pick up rotated S3 credentials, even when the
//	secret is CREATE OR REPLACE'd with fresh credentials before the old ones
//	are revoked. A statement lives or dies on the credentials it captured at
//	statement start.
//
// Two independent DuckDB v1.5.3 behaviors combine to make this true:
//
//  1. Statements resolve secrets through their MVCC snapshot of the secret
//     catalog — a CREATE OR REPLACE SECRET committed by another connection
//     mid-statement is never visible to the in-flight statement.
//  2. httpfs' refresh-on-403 escape hatch (TryRefreshS3Secret) only runs in
//     S3FileHandle::Initialize, and only when the open performs a network
//     request. Glob scans never do: the S3 glob (ListObjectsV2) pre-populates
//     file_size/etag/last_modified in extended_info, which marks the handle
//     initialized and skips the HEAD entirely (httpfs httpfs.cpp,
//     HTTPFileHandle ctor). DuckLake file lists deliberately do the same with
//     dummy etag/last_modified. The first auth failure
//     therefore surfaces on a range GET inside the read path, which has no
//     refresh handling — the statement dies.
//
// The PostHog httpfs fork (PostHog/duckdb-httpfs, cred-refresh patch) FIXES
// this in the read path — that build is covered by
// TestInFlightScanSurvivesRotationWithPatchedHTTPFS below. This pin stays on
// the stock extension so we notice when upstream DuckDB gains the same
// recovery (at which point the fork patch and the freshness-floor caveats in
// controlplane/sts_broker.go can both be revisited).
//
// Requires the integration docker compose stack (MinIO with admin `mc` in the
// container); skips otherwise.
func TestInFlightScanDiesOnCredentialRotation(t *testing.T) {
	r := runRotationScenario(t, rotationScenario{loadHTTPFS: loadStockHTTPFS})
	if r.err == nil {
		t.Fatalf("in-flight statement SURVIVED credential rotation (%d rows in %v, rotated at %v). "+
			"Stock DuckDB/httpfs gained mid-statement credential recovery — revisit the STS freshness floor "+
			"(stsCacheSafetyMargin in controlplane/sts_broker.go), its capture-at-statement-start caveats, "+
			"and whether the PostHog httpfs fork's cred-refresh patch is still needed.",
			r.cnt, r.duration, r.rotatedAt)
	}
	errText := r.err.Error()
	if !strings.Contains(errText, "403") && !strings.Contains(strings.ToLower(errText), "forbidden") &&
		!strings.Contains(errText, "InvalidAccessKeyId") {
		t.Fatalf("in-flight statement failed, but not with the expected auth error: %v", r.err)
	}
	t.Logf("pinned: in-flight statement died on rotation as expected after %v (rotated at %v): %v",
		r.duration, r.rotatedAt, r.err)
}

// TestInFlightScanSurvivesRotationWithPatchedHTTPFS asserts the PostHog httpfs
// fork's mid-statement credential refresh: on an auth failure (400/403) in any
// S3 request path, the patched extension re-resolves the LATEST COMMITTED
// secret (bypassing the statement's MVCC snapshot) and retries once with the
// refreshed credentials. Combined with the control plane's credential-refresh
// scheduler — which keeps committing fresh STS credentials via CREATE OR
// REPLACE SECRET on a side connection — this removes the statement-length
// ceiling that the STS freshness floor (stsCacheSafetyMargin) could otherwise
// only push to ~35-60min: the in-flight scan survives rotation + revocation of
// the credentials it started with.
//
// Runs only when DUCKGRES_TEST_PATCHED_HTTPFS points at a locally-built
// extension binary (see PostHog/duckdb-httpfs branch cred-refresh-read-path):
//
//	DUCKGRES_TEST_PATCHED_HTTPFS=/path/to/httpfs.duckdb_extension \
//	  go test ./tests/integration/ -run SurvivesRotationWithPatchedHTTPFS -v
//
// Once a fork release with the patch is pinned in Dockerfile.worker
// (HTTPFS_EXTENSION_TAG), the e2e harness exercises the same behavior against
// real STS tokens; this test is the fast local proof.
func TestInFlightScanSurvivesRotationWithPatchedHTTPFS(t *testing.T) {
	extPath := os.Getenv("DUCKGRES_TEST_PATCHED_HTTPFS")
	if extPath == "" {
		t.Skip("DUCKGRES_TEST_PATCHED_HTTPFS not set (path to patched httpfs.duckdb_extension)")
	}
	if _, err := os.Stat(extPath); err != nil {
		t.Fatalf("DUCKGRES_TEST_PATCHED_HTTPFS=%s not readable: %v", extPath, err)
	}
	r := runRotationScenario(t, rotationScenario{
		dsn: "?allow_unsigned_extensions=true",
		loadHTTPFS: func(t *testing.T, db *sql.DB) {
			t.Helper()
			if _, err := db.Exec(fmt.Sprintf("LOAD '%s'", extPath)); err != nil {
				t.Fatalf("load patched httpfs from %s: %v", extPath, err)
			}
		},
	})
	if r.err != nil {
		t.Fatalf("in-flight statement DIED on rotation despite the patched httpfs (rotated at %v): %v",
			r.rotatedAt, r.err)
	}
	if want := int64(rotationNumFiles * rotationRowsPerFile); r.cnt != want {
		t.Fatalf("survived scan returned %d rows, want %d — retry must not lose or duplicate data", r.cnt, want)
	}
	t.Logf("patched httpfs: in-flight scan survived rotation (%d rows in %v, rotated at %v)",
		r.cnt, r.duration, r.rotatedAt)
}
