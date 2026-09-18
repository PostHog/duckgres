//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// The publisher's real credential against its real grants.
//
// Infra provisions a role with privileges on the cell's schema ONLY, and its
// connection URL names a database with no schema at all - so every unqualified
// statement the publisher issues resolves against `public`, where that role can
// neither create nor read. A test that uses an administrator DSN with a
// preconfigured search_path cannot see that: it has rights everywhere. This one
// builds the writer through the production builder, as a role that is scoped
// exactly as the real one is.
func TestCatalogWriterPublishesInsideItsGrantedSchema(t *testing.T) {
	adminDSN := adminPostgresURL(t)
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer func() { _ = admin.Close() }()

	suffix := randomSuffix(t)
	schema := "trino_cell_" + suffix
	role := "dgpub_" + suffix
	password := "pw_" + randomSuffix(t)

	mustExec(t, admin, fmt.Sprintf(`CREATE SCHEMA %s`, schema))
	mustExec(t, admin, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, password))
	t.Cleanup(func() {
		_, _ = admin.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema))
		_, _ = admin.Exec(fmt.Sprintf(`REASSIGN OWNED BY %s TO CURRENT_USER`, role))
		_, _ = admin.Exec(fmt.Sprintf(`DROP OWNED BY %s`, role))
		_, _ = admin.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	// Exactly the shape infra grants: the cell's schema, and nothing in public.
	mustExec(t, admin, fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA %s TO %s`, schema, role))
	mustExec(t, admin, fmt.Sprintf(`REVOKE ALL ON SCHEMA public FROM %s`, role))

	dsnFile := filepath.Join(t.TempDir(), "publisher.dsn")
	if err := os.WriteFile(dsnFile, []byte(scopedDSN(t, adminDSN, role, password)), 0o600); err != nil {
		t.Fatalf("write dsn file: %v", err)
	}

	t.Setenv(envTrinoPoolCatalogWriter, "true")
	t.Setenv(envTrinoPoolCatalogBootstrap, "true")
	t.Setenv(envTrinoPoolCatalogDSNFile, dsnFile)
	t.Setenv(envTrinoPoolCatalogSchema, schema)

	lease := configstore.TrinoPoolLease{PoolID: "registered:cell-001", Owner: "cp-test", Epoch: 1}
	writer, err := buildTrinoPoolCatalogWriter("cell-001", nil,
		func() (configstore.TrinoPoolLease, bool) { return lease, true }, nil)
	if err != nil {
		t.Fatalf("build the catalog writer as the scoped publisher role: %v", err)
	}
	if writer == nil {
		t.Fatal("the writer was not built")
	}
	t.Cleanup(func() { _ = writer.db.Close() })

	ctx := context.Background()
	if err := writer.ClaimWriter(ctx); err != nil {
		t.Fatalf("claim the writer fence: %v", err)
	}
	if err := writer.CreateCatalog(ctx, "org_acme", map[string]string{
		"connector.name":     "ducklake",
		"ducklake.data-path": "s3://bucket/prefix/",
	}); err != nil {
		t.Fatalf("publish a catalog as the scoped publisher role: %v", err)
	}

	// The rows are in the GRANTED schema, which is where the coordinators read.
	var catalogs int
	if err := admin.QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s.trino_catalogs WHERE catalog_name = 'org_acme'`, schema),
	).Scan(&catalogs); err != nil {
		t.Fatalf("read the published catalog from %s: %v", schema, err)
	}
	if catalogs != 1 {
		t.Fatalf("%d catalogs in %s, want the published one", catalogs, schema)
	}
	// And nothing was created in public, which the role cannot write anyway -
	// a writer that fell back there would be publishing where nobody reads.
	var inPublic int
	if err := admin.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'trino_catalogs'`,
	).Scan(&inPublic); err != nil {
		t.Fatalf("inspect public: %v", err)
	}
	if inPublic != 0 {
		t.Fatal("the publisher created its tables in public")
	}

	// The reader side resolves the same way.
	names, err := writer.ListCatalogs(ctx)
	if err != nil {
		t.Fatalf("list published catalogs: %v", err)
	}
	if len(names) != 1 || names[0] != "org_acme" {
		t.Fatalf("published catalogs = %v", names)
	}
}

// A schema that is not configured at all is refused at build time rather than
// at the first publication, where it would surface as a permission error on a
// path an operator has no reason to suspect.
func TestCatalogWriterRefusesAnUnconfiguredSchema(t *testing.T) {
	dsnFile := filepath.Join(t.TempDir(), "publisher.dsn")
	if err := os.WriteFile(dsnFile, []byte("postgres://user:pw@example.invalid:5432/duckgres"), 0o600); err != nil {
		t.Fatalf("write dsn file: %v", err)
	}
	t.Setenv(envTrinoPoolCatalogWriter, "true")
	t.Setenv(envTrinoPoolCatalogBootstrap, "false")
	t.Setenv(envTrinoPoolCatalogDSNFile, dsnFile)
	t.Setenv(envTrinoPoolCatalogSchema, "")

	if _, err := buildTrinoPoolCatalogWriter("cell-001", nil,
		func() (configstore.TrinoPoolLease, bool) { return configstore.TrinoPoolLease{}, false }, nil); err == nil {
		t.Fatal("the writer was built with no schema configured")
	}

	// And a name that would need quoting is refused rather than escaped: it is
	// interpolated into a connection parameter.
	t.Setenv(envTrinoPoolCatalogSchema, `weird"; DROP TABLE x --`)
	if _, err := buildTrinoPoolCatalogWriter("cell-001", nil,
		func() (configstore.TrinoPoolLease, bool) { return configstore.TrinoPoolLease{}, false }, nil); err == nil {
		t.Fatal("a schema name needing quotes was accepted")
	}
}

// adminPostgresURL is the administrative connection these tests create the
// scoped role from. It follows the same convention as the other real-PostgreSQL
// suites: the shared instance unless DUCKGRES_TEST_PG_DSN points elsewhere.
func adminPostgresURL(t *testing.T) string {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv("DUCKGRES_TEST_PG_DSN")); dsn != "" {
		return dsn
	}
	const shared = "postgres://postgres:postgres@127.0.0.1:35432/testdb?sslmode=disable"
	db, err := sql.Open("pgx", shared)
	if err != nil {
		t.Skipf("no administrative PostgreSQL available: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Ping(); err != nil {
		t.Skipf("no administrative PostgreSQL available: %v", err)
	}
	return shared
}

func mustExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	buffer := make([]byte, 6)
	if _, err := rand.Read(buffer); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(buffer)
}

// scopedDSN rewrites the admin URL to authenticate as the scoped role, keeping
// the host and database. It deliberately carries NO search_path: that is the
// shape infra provisions, and pinning it is the writer's job.
func scopedDSN(t *testing.T, adminDSN, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Skipf("DUCKGRES_TEST_PG_DSN is not a URL (%v); this test needs one", err)
	}
	parsed.User = url.UserPassword(role, password)
	query := parsed.Query()
	query.Del("search_path")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
