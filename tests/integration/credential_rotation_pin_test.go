package integration

import (
	"database/sql"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
)

// TestInFlightScanDiesOnCredentialRotation PINS a DuckDB limitation that the
// control plane's STS freshness floor (stsCacheSafetyMargin in
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
//     HTTPFileHandle ctor). DuckLake and Iceberg file lists deliberately do
//     the same with dummy etag/last_modified. The first auth failure
//     therefore surfaces on a range GET inside the read path, which has no
//     refresh handling — the statement dies.
//
// The scenario: a compute-heavy scan over many parquet files starts with
// user-A credentials; mid-statement the secret is rotated to user B via
// server.RefreshS3Secret (exactly what the credential-refresh scheduler
// pushes to workers) and user A is then DISABLED in MinIO (simulating STS
// token expiry). The in-flight statement is EXPECTED TO FAIL with an auth
// error on the next file it touches, while a fresh statement afterwards
// succeeds on the rotated secret.
//
// IF THIS TEST EVER FAILS WITH "in-flight statement SURVIVED": a DuckDB /
// httpfs upgrade added mid-statement credential recovery (e.g. refresh-on-403
// in the read path). That is good news — revisit the freshness floor and the
// "long statements die at token expiry" caveats in controlplane/sts_broker.go,
// which would then be over-conservative.
//
// Requires the integration docker compose stack (MinIO with admin `mc` in the
// container); skips otherwise.
func TestInFlightScanDiesOnCredentialRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("credential rotation pin test skipped in -short mode")
	}
	const (
		minioAddr      = "localhost:39000"
		minioContainer = "duckgres-test-minio"
		bucket         = "rotation-pin"
		userA          = "rotuser-a"
		userB          = "rotuser-b"
		password       = "rotating-pass-12345"
	)

	if conn, err := net.DialTimeout("tcp", minioAddr, 2*time.Second); err != nil {
		t.Skipf("MinIO not reachable on %s (run docker compose in tests/integration): %v", minioAddr, err)
	} else {
		_ = conn.Close()
	}

	mc := func(args ...string) (string, error) {
		out, err := exec.Command("docker", append([]string{"exec", minioContainer, "mc"}, args...)...).CombinedOutput()
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
	if out, err := mc("mb", "localadmin/"+bucket, "--ignore-existing"); err != nil {
		t.Fatalf("create bucket: %v: %s", err, out)
	}
	for _, u := range []string{userA, userB} {
		_, _ = mc("admin", "user", "rm", "localadmin", u) // idempotent re-runs
		if out, err := mc("admin", "user", "add", "localadmin", u, password); err != nil {
			t.Fatalf("add user %s: %v: %s", u, err, out)
		}
		if out, err := mc("admin", "policy", "attach", "localadmin", "readwrite", "--user", u); err != nil &&
			!strings.Contains(out, "already") {
			t.Fatalf("attach policy to %s: %v: %s", u, err, out)
		}
	}
	t.Cleanup(func() {
		_, _ = mc("admin", "user", "enable", "localadmin", userA)
		_, _ = mc("admin", "user", "rm", "localadmin", userA)
		_, _ = mc("admin", "user", "rm", "localadmin", userB)
		_, _ = mc("rb", "--force", "localadmin/"+bucket)
	})

	// --- Seed parquet data with admin credentials (separate DuckDB) ---
	const numFiles = 48
	{
		seedDB, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("open seed duckdb: %v", err)
		}
		defer func() { _ = seedDB.Close() }()
		for _, stmt := range []string{"INSTALL httpfs", "LOAD httpfs"} {
			if _, err := seedDB.Exec(stmt); err != nil {
				t.Skipf("httpfs unavailable (network/sandbox): %v", err)
			}
		}
		if _, err := seedDB.Exec(fmt.Sprintf(`CREATE SECRET seed (
			TYPE s3, PROVIDER config,
			KEY_ID 'minioadmin', SECRET 'minioadmin', SESSION_TOKEN '',
			ENDPOINT '%s', USE_SSL false, URL_STYLE 'path', REGION 'us-east-1')`, minioAddr)); err != nil {
			t.Fatalf("create seed secret: %v", err)
		}
		for i := 0; i < numFiles; i++ {
			if _, err := seedDB.Exec(fmt.Sprintf(
				`COPY (SELECT md5(range::VARCHAR) AS s, range AS val FROM range(40000)) TO 's3://%s/data/part_%d.parquet'`,
				bucket, i)); err != nil {
				t.Fatalf("seed part_%d: %v", i, err)
			}
		}
	}

	// --- System under test: production-shape PROVIDER config secret ---
	dlCfg := server.DuckLakeConfig{
		ObjectStore:    fmt.Sprintf("s3://%s/", bucket),
		S3Provider:     "config",
		S3Endpoint:     minioAddr,
		S3Region:       "us-east-1",
		S3URLStyle:     "path",
		S3UseSSL:       false,
		S3AccessKey:    userA,
		S3SecretKey:    password,
		S3SessionToken: "", // MinIO users have no session token; explicit empty is the contract
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4) // conn = pinned query conn; refresh uses a side conn (mirrors worker controlDB)

	for _, stmt := range []string{"INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.Exec(stmt); err != nil {
			t.Skipf("httpfs unavailable (network/sandbox): %v", err)
		}
	}
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
		bucket)

	type result struct {
		cnt      int64
		max      string
		err      error
		duration time.Duration
	}
	done := make(chan result, 1)
	started := time.Now()
	go func() {
		var r result
		r.err = conn.QueryRowContext(t.Context(), scanQuery).Scan(&r.cnt, &r.max)
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
	rotated.S3AccessKey = userB
	if err := server.RefreshS3Secret(db, rotated, nil); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}
	if out, err := mc("admin", "user", "disable", "localadmin", userA); err != nil {
		t.Fatalf("disable %s: %v: %s", userA, err, out)
	}
	rotatedAt := time.Since(started)

	r := <-done
	if r.err == nil {
		if r.duration <= rotatedAt {
			t.Fatalf("scan finished in %v, before the rotation at %v — the test proved nothing, increase data size",
				r.duration, rotatedAt)
		}
		t.Fatalf("in-flight statement SURVIVED credential rotation (%d rows in %v, rotated at %v). "+
			"DuckDB/httpfs gained mid-statement credential recovery — revisit the STS freshness floor "+
			"(stsCacheSafetyMargin in controlplane/sts_broker.go) and its capture-at-statement-start caveats.",
			r.cnt, r.duration, rotatedAt)
	}
	errText := r.err.Error()
	if !strings.Contains(errText, "403") && !strings.Contains(strings.ToLower(errText), "forbidden") &&
		!strings.Contains(errText, "InvalidAccessKeyId") {
		t.Fatalf("in-flight statement failed, but not with the expected auth error: %v", r.err)
	}
	t.Logf("pinned: in-flight statement died on rotation as expected after %v (rotated at %v): %v",
		r.duration, rotatedAt, r.err)

	// The other half of the contract — and what the credential-refresh
	// scheduler actually guarantees: a FRESH statement on the same session
	// (user A still disabled) works off the rotated secret.
	var n int64
	if err := conn.QueryRowContext(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM read_parquet('s3://%s/data/part_0.parquet')`, bucket)).Scan(&n); err != nil {
		t.Fatalf("post-rotation fresh statement failed — secret rotation is broken for NEW statements too: %v", err)
	}
	if want := int64(40000); n != want {
		t.Fatalf("post-rotation fresh statement returned %d rows, want %d", n, want)
	}
}
