package provisioner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

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
