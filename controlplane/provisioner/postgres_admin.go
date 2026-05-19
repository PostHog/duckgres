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
	defer db.Close()

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
	defer db.Close()

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
	defer dbScoped.Close()
	if _, err := dbScoped.ExecContext(ctx, "ALTER SCHEMA public OWNER TO "+quoteIdent(role)); err != nil {
		return fmt.Errorf("alter schema public owner -> %s: %w", role, err)
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
