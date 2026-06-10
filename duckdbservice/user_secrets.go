package duckdbservice

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/posthog/duckgres/server/usersecrets"
)

// secretDBHandle is the subset of *sql.Conn / *sql.DB needed for secret
// hygiene and replay.
type secretDBHandle interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

const userSecretOpTimeout = 15 * time.Second

// wipeUserPersistentSecrets drops every persistent-storage DuckDB secret on
// the shared per-org instance. System secrets (ducklake_s3, iceberg_sigv4,
// iceberg_oauth) are created with plain CREATE OR REPLACE SECRET and live in
// in-memory storage, so a persistent-only wipe never touches them — anything
// persistent was replayed (or created) by a user session and must not leak
// into the next session, which may belong to a different user of the org.
// Returns the names that were dropped.
func wipeUserPersistentSecrets(ctx context.Context, h secretDBHandle) ([]string, error) {
	rows, err := h.QueryContext(ctx, "SELECT name FROM duckdb_secrets() WHERE persistent")
	if err != nil {
		return nil, fmt.Errorf("list persistent secrets: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	var dropped []string
	for _, name := range names {
		if _, err := h.ExecContext(ctx, "DROP PERSISTENT SECRET IF EXISTS "+quoteSecretIdent(name)); err != nil {
			return dropped, fmt.Errorf("drop persistent secret %q: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}

// quoteSecretIdent double-quotes a DuckDB identifier, escaping embedded quotes.
func quoteSecretIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// replayUserSecrets applies the control-plane-supplied persistent secret
// statements to a fresh session. Failures are collected as warnings rather
// than failing the session: a stale or broken stored secret must not lock the
// user out of their warehouse. Statement text is never logged — only the
// classified secret name.
func replayUserSecrets(ctx context.Context, h secretDBHandle, username string, statements []string) []string {
	var warnings []string
	for _, stmt := range statements {
		name := usersecrets.Classify(stmt).Name
		if name == "" {
			name = "(unknown)"
		}
		if _, err := h.ExecContext(ctx, stmt); err != nil {
			slog.Warn("Failed to replay user persistent secret.",
				"user", username, "secret", name, "error", err)
			warnings = append(warnings, fmt.Sprintf("persistent secret %q could not be restored: %v", name, err))
			continue
		}
		slog.Debug("Replayed user persistent secret.", "user", username, "secret", name)
	}
	return warnings
}
