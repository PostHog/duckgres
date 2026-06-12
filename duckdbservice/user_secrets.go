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

// wipeUserSecrets drops every user-created DuckDB secret on the shared per-org
// instance — both persistent ones (replayed/created by a user session) AND
// non-persistent ones created via plain CREATE SECRET / CREATE TEMPORARY
// SECRET, which the control plane passes through verbatim to the worker.
//
// DuckDB secrets are instance-global and survive connection eviction, so a
// temporary secret created by user A of an org would otherwise linger on the
// hot-idle worker pod and be inherited by the next user B of the same org —
// who could then read A's private buckets or enumerate A's credentials via
// duckdb_secrets(). That makes a persistent-only wipe a cross-user isolation
// leak; this wipe closes it by dropping temporary secrets too.
//
// System-managed secrets (ducklake_s3, iceberg_sigv4, iceberg_oauth, plus the
// reserved __default_*/duckgres_* prefixes) are preserved: activation
// re-creates them and dropping them would break the org's own catalog wiring.
// The allowlist is usersecrets.IsReservedName, the same set the control plane
// forbids users from creating, so no user secret can hide behind it.
//
// Returns the names that were dropped.
func wipeUserSecrets(ctx context.Context, h secretDBHandle) ([]string, error) {
	rows, err := h.QueryContext(ctx, "SELECT name, persistent FROM duckdb_secrets()")
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	type secretRow struct {
		name       string
		persistent bool
	}
	var secrets []secretRow
	for rows.Next() {
		var s secretRow
		if err := rows.Scan(&s.name, &s.persistent); err != nil {
			_ = rows.Close()
			return nil, err
		}
		secrets = append(secrets, s)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	var dropped []string
	for _, s := range secrets {
		// Preserve system-managed secrets; activation re-creates these.
		if usersecrets.IsReservedName(s.name) {
			continue
		}
		// DuckDB requires the storage backend in DROP when more than one
		// backend is in play: persistent secrets live on disk, temporary ones
		// in memory, and a plain "DROP SECRET" is ambiguous across them. Branch
		// on the per-row persistent flag to issue the unambiguous form.
		var drop string
		if s.persistent {
			drop = "DROP PERSISTENT SECRET IF EXISTS " + quoteSecretIdent(s.name)
		} else {
			drop = "DROP TEMPORARY SECRET IF EXISTS " + quoteSecretIdent(s.name)
		}
		if _, err := h.ExecContext(ctx, drop); err != nil {
			return dropped, fmt.Errorf("drop secret %q (persistent=%t): %w", s.name, s.persistent, err)
		}
		dropped = append(dropped, s.name)
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
