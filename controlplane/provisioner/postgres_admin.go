package provisioner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// EnsureDatabase creates dbName on the Postgres server addressed by adminDSN
// if it does not already exist. Idempotent: returns nil when the database is
// already present. Caller owns the DSN's credential lifetime.
//
// CREATE DATABASE cannot run in a transaction and Postgres does not support
// CREATE DATABASE IF NOT EXISTS, so we probe pg_database first and only fire
// CREATE DATABASE when the row is missing. There is a TOCTOU race against
// concurrent callers; we handle the duplicate_database SQLSTATE (42P04) as
// a benign collision rather than an error.
func EnsureDatabase(ctx context.Context, adminDSN, dbName string) error {
	if !isSafePGIdent(dbName) {
		return fmt.Errorf("ensure database: unsafe identifier %q", dbName)
	}
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&exists); err != nil {
		return fmt.Errorf("probe pg_database: %w", err)
	}
	if exists {
		return nil
	}

	// Identifier validated above. CREATE DATABASE does not accept parameters.
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(dbName)); err != nil {
		if isDuplicateDatabase(err) {
			return nil
		}
		return fmt.Errorf("create database %s: %w", dbName, err)
	}
	return nil
}

// EnsureRole creates a login role with the given password, or rotates the
// password if the role already exists. Used by the Lakekeeper provisioner to
// make sure Lakekeeper's pod can connect with the credentials stored in its
// K8s Secret — a freshly-created database has no users by default.
//
// On a re-run with the same password the ALTER ROLE is a no-op for Postgres
// internals. On a re-run with a different password we explicitly rotate;
// callers must keep the password in their Secret in sync with whatever was
// last passed here. The Lakekeeper provisioner achieves this by reading the
// existing Secret on every run (resolveOrGenerateSecret) rather than
// regenerating, so the same password threads through to EnsureRole.
//
// Grants the role ALL PRIVILEGES on the named database. Cluster admin
// permissions are not granted.
func EnsureRole(ctx context.Context, adminDSN, role, password, ownedDB string) error {
	if !isSafePGIdent(role) {
		return fmt.Errorf("ensure role: unsafe role name %q", role)
	}
	if !isSafePGIdent(ownedDB) {
		return fmt.Errorf("ensure role: unsafe db name %q", ownedDB)
	}
	if password == "" {
		return fmt.Errorf("ensure role: empty password")
	}
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
		return fmt.Errorf("probe pg_roles: %w", err)
	}
	if !exists {
		// CREATE ROLE accepts password as a literal — pgx's parameter
		// binding doesn't apply to DDL. Validate as identifier so embedded
		// quotes can't break out (passwords from mustRandomHex are pure
		// hex so this is belt-and-suspenders).
		if !isSafePGPassword(password) {
			return fmt.Errorf("ensure role: password contains unsafe characters")
		}
		stmt := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s", quoteIdent(role), quoteLiteral(password))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if isDuplicateObject(err) {
				// Concurrent creator beat us — fall through to ALTER + GRANT.
			} else {
				return fmt.Errorf("create role %s: %w", role, err)
			}
		}
	} else {
		// Role exists; rotate the password to whatever the caller passed
		// (matches the contract that the secret + role stay in sync).
		if !isSafePGPassword(password) {
			return fmt.Errorf("ensure role: password contains unsafe characters")
		}
		stmt := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s", quoteIdent(role), quoteLiteral(password))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter role %s: %w", role, err)
		}
	}

	// GRANT is idempotent — Postgres ignores a re-grant of an existing privilege.
	if _, err := db.ExecContext(ctx, "GRANT ALL PRIVILEGES ON DATABASE "+quoteIdent(ownedDB)+" TO "+quoteIdent(role)); err != nil {
		return fmt.Errorf("grant on database %s to %s: %w", ownedDB, role, err)
	}

	// Postgres 15+ revokes CREATE on the public schema for non-owner roles.
	// Lakekeeper's `migrate` step needs DDL inside that schema, so make the
	// role the database OWNER (which carries schema-creation privileges by
	// default). Also ALTER SCHEMA public OWNER as belt-and-suspenders for
	// older PG versions where the database OWNER doesn't automatically own
	// pre-existing schemas in the new DB.
	if _, err := db.ExecContext(ctx, "ALTER DATABASE "+quoteIdent(ownedDB)+" OWNER TO "+quoteIdent(role)); err != nil {
		return fmt.Errorf("alter database owner %s -> %s: %w", ownedDB, role, err)
	}
	// Run the schema-owner ALTER inside the target database — schema
	// ownership is local to each database.
	dbScoped, err := sql.Open("pgx", reDSN(adminDSN, ownedDB))
	if err != nil {
		return fmt.Errorf("open admin connection to %s: %w", ownedDB, err)
	}
	defer func() { _ = dbScoped.Close() }()
	if _, err := dbScoped.ExecContext(ctx, "ALTER SCHEMA public OWNER TO "+quoteIdent(role)); err != nil {
		return fmt.Errorf("alter schema public owner -> %s: %w", role, err)
	}
	return nil
}

// DropDatabase removes dbName on the Postgres server addressed by adminDSN.
// Idempotent: returns nil when the database is already absent (3D000). Forces
// disconnection of any active sessions on the target DB so DROP DATABASE
// can't hang waiting for clients to drain — necessary at duckling teardown
// time because the per-tenant Lakekeeper pod may still be alive when the
// drop runs (the k8s teardown is fire-and-forget and the operator's
// reconciliation lag means connections linger).
//
// Reassigns ownership to CURRENT_USER before the drop so the admin role
// can issue DROP DATABASE even when it doesn't own the target. EnsureRole
// runs ALTER DATABASE ... OWNER TO <role>, which means a non-superuser
// admin (e.g. the ducklingexample master on a shared RDS) wouldn't
// otherwise have permission — 42501 must be owner of database. GRANT
// role-membership first so CURRENT_USER inherits the necessary privileges
// to ALTER OWNER (which itself requires being a member of the new owner
// role on Postgres 14+).
//
// Caller must connect via a privileged DSN against a different database
// than dbName (the admin DSN's path is OK to be `postgres`).
func DropDatabase(ctx context.Context, adminDSN, dbName string) error {
	if !isSafePGIdent(dbName) {
		return fmt.Errorf("drop database: unsafe identifier %q", dbName)
	}
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Probe first so the IF EXISTS hides the missing-DB case cleanly,
	// and so we don't run the ownership reassignment against a phantom.
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&exists); err != nil {
		return fmt.Errorf("probe pg_database: %w", err)
	}
	if !exists {
		return nil
	}

	// EnsureRole made the tenant role the database owner. To DROP we must
	// either be that role or a superuser; on RDS the admin is neither.
	// Take role-membership of the current owner so we can hand ownership
	// back to ourselves, then drop. Both GRANT and ALTER OWNER are
	// idempotent / best-effort: if the role doesn't exist (e.g. we are
	// already the owner) the GRANT 0LP01-likes and ALTER fail cleanly,
	// and we continue to the DROP, which will report the real obstacle.
	if owner, ok := databaseOwner(ctx, db, dbName); ok && owner != "" && isSafePGIdent(owner) {
		_, _ = db.ExecContext(ctx, "GRANT "+quoteIdent(owner)+" TO CURRENT_USER")
		_, _ = db.ExecContext(ctx, "ALTER DATABASE "+quoteIdent(dbName)+" OWNER TO CURRENT_USER")
	}

	// FORCE terminates active backends as part of DROP DATABASE (Postgres
	// 13+). Without it a single lingering Lakekeeper connection blocks the
	// drop until backoff.
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)+" WITH (FORCE)"); err != nil {
		if isInvalidCatalogName(err) {
			return nil
		}
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

// databaseOwner returns the rolname that owns dbName, or "" if the lookup
// fails. Used by DropDatabase to reassign ownership before DROP.
func databaseOwner(ctx context.Context, db *sql.DB, dbName string) (string, bool) {
	var owner string
	err := db.QueryRowContext(ctx,
		"SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1",
		dbName).Scan(&owner)
	if err != nil {
		return "", false
	}
	return owner, true
}

// DropRole removes role on the Postgres server addressed by adminDSN.
// Idempotent: returns nil when the role is already absent. Best-effort
// REASSIGN/DROP OWNED first so any object the role owns (e.g. grants on
// the maintenance DB, default privileges) doesn't block DROP ROLE with
// 2BP01 ("role cannot be dropped because some objects depend on it").
//
// Requires role-membership in `role` for REASSIGN OWNED + DROP OWNED to
// run (Postgres 14+); the GRANT is best-effort because if `role` is
// already gone the GRANT itself fails. The caller's admin must either be
// a superuser or already a member of role; on RDS we explicitly GRANT
// the membership first since the admin is neither.
//
// Caller must connect via a privileged DSN.
func DropRole(ctx context.Context, adminDSN, role string) error {
	if !isSafePGIdent(role) {
		return fmt.Errorf("drop role: unsafe role name %q", role)
	}
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return fmt.Errorf("open admin connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
		return fmt.Errorf("probe pg_roles: %w", err)
	}
	if !exists {
		return nil
	}

	// Inherit the role's privileges so REASSIGN/DROP OWNED can run on
	// shared RDS where the admin is not a superuser. Best-effort: the
	// GRANT can fail when the admin is already a member (cycle) or is
	// the superuser, in which case it isn't needed anyway.
	_, _ = db.ExecContext(ctx, "GRANT "+quoteIdent(role)+" TO CURRENT_USER")

	// REASSIGN handles owned database objects in this DB; DROP OWNED
	// CASCADE then handles cluster-wide grants/default privileges. Both
	// can fail without blocking the final DROP ROLE — if there's a real
	// remaining dependency, DROP ROLE will surface it.
	_, _ = db.ExecContext(ctx, "REASSIGN OWNED BY "+quoteIdent(role)+" TO CURRENT_USER")
	if _, err := db.ExecContext(ctx, "DROP OWNED BY "+quoteIdent(role)+" CASCADE"); err != nil {
		if isUndefinedObject(err) {
			return nil
		}
		// Continue to DROP ROLE — there may be nothing to drop. Surface
		// the underlying error only if DROP ROLE itself fails too.
		_ = err
	}
	if _, err := db.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteIdent(role)); err != nil {
		return fmt.Errorf("drop role %s: %w", role, err)
	}
	return nil
}

// reDSN rewrites the dbname component of a Postgres URL-style DSN. Used to
// connect to a specific database with the same admin credentials.
func reDSN(dsn, dbName string) string {
	// pgx accepts both URL and keyword/value DSNs. Detect the URL form by
	// the postgres:// prefix.
	const urlPrefix = "postgres://"
	const urlPrefix2 = "postgresql://"
	if strings.HasPrefix(dsn, urlPrefix) || strings.HasPrefix(dsn, urlPrefix2) {
		// Find the last "/" after the "@" — that's the path component.
		at := strings.Index(dsn, "@")
		slash := -1
		if at >= 0 {
			slash = strings.Index(dsn[at:], "/")
			if slash >= 0 {
				slash += at
			}
		}
		if slash < 0 {
			// No dbname segment; append one.
			if q := strings.Index(dsn, "?"); q >= 0 {
				return dsn[:q] + "/" + dbName + dsn[q:]
			}
			return dsn + "/" + dbName
		}
		// Replace the segment between slash+1 and the next "?" (or end).
		rest := dsn[slash+1:]
		q := strings.Index(rest, "?")
		if q < 0 {
			return dsn[:slash+1] + dbName
		}
		return dsn[:slash+1] + dbName + rest[q:]
	}
	// Keyword/value form: replace dbname=... or append.
	return strings.NewReplacer("dbname="+extractDBName(dsn), "dbname="+dbName).Replace(dsn)
}

func extractDBName(dsn string) string {
	// Best-effort extract for the keyword/value form. We only use this when
	// pgx URL prefix isn't present, which is rare in our codebase.
	for _, kv := range strings.Fields(dsn) {
		if strings.HasPrefix(kv, "dbname=") {
			return strings.TrimPrefix(kv, "dbname=")
		}
	}
	return ""
}

// isSafePGPassword restricts passwords we generate to a printable ASCII
// subset that can't break out of a single-quoted SQL literal. Our generator
// (mustRandomHex) produces pure hex which trivially passes; the check
// exists so an external caller can't sneak in newlines / quotes.
var safePGPassword = regexp.MustCompile(`^[A-Za-z0-9_\-+=./]{1,256}$`)

func isSafePGPassword(s string) bool { return safePGPassword.MatchString(s) }

// quoteLiteral wraps a string in single quotes and doubles any embedded
// single quotes per Postgres rules. Identifiers use double quotes
// (quoteIdent); string literals use single quotes.
func quoteLiteral(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '\'')
	return string(out)
}

// isDuplicateObject reports whether err is Postgres 42710 (duplicate_object)
// — used for the CREATE ROLE concurrent-create race.
func isDuplicateObject(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "42710"
}

// isSafePGIdent restricts database names to a conservative whitelist. Names
// we generate look like "lakekeeper_<orgid>" where orgid is itself
// constrained; the regex catches accidental typos and any attempt to inject
// SQL via the name. Real escaping is still done with quoteIdent below — this
// is a belt-and-suspenders check.
var safePGIdent = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

func isSafePGIdent(s string) bool { return safePGIdent.MatchString(s) }

// quoteIdent wraps an identifier in double quotes and escapes embedded quotes
// per Postgres rules. Used after isSafePGIdent so this is defensive.
func quoteIdent(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}

// isDuplicateDatabase reports whether err is a Postgres 42P04 (database
// already exists) — the only race outcome we consider benign. Uses
// errors.As so multi-error chains (e.g. errors.Join) are handled too.
func isDuplicateDatabase(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "42P04"
}

// isInvalidCatalogName reports whether err is Postgres 3D000 (database does
// not exist) — what DROP DATABASE returns when the target is already gone.
// Without the IF EXISTS clause this would matter; we keep the check anyway
// because IF EXISTS is silent on missing-DB and an actual no-such-database
// error can also surface from the connection attempt itself.
func isInvalidCatalogName(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "3D000"
}

// isUndefinedObject reports whether err is Postgres 42704 (undefined_object)
// — what DROP OWNED returns when the role doesn't exist. Benign.
func isUndefinedObject(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "42704"
}
