//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ReshardSourceFencer removes sessions which bypass Duckgres after the
// composition has made the tenant role NOLOGIN. It is deliberately separate
// from the ordinary drain: callers must first prove that Duckgres owns no
// leases, queued requests, or workers.
type ReshardSourceFencer interface {
	TerminateAndWait(ctx context.Context, maintenance CatalogEndpoint, tenantUser, tenantDatabase string) error
	DisableMaintenanceAndTerminate(ctx context.Context, maintenance CatalogEndpoint, disableAndWait func() error) error
}

type PGReshardSourceFencer struct{}

func (PGReshardSourceFencer) TerminateAndWait(ctx context.Context, maintenance CatalogEndpoint, tenantUser, tenantDatabase string) error {
	conn, err := pgx.Connect(ctx, maintenance.DSN())
	if err != nil {
		return fmt.Errorf("connect with reshard maintenance identity %s: %w", maintenance.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	return terminateRoleSessionsAndWait(ctx, conn, tenantUser, tenantDatabase)
}

func (PGReshardSourceFencer) DisableMaintenanceAndTerminate(ctx context.Context, maintenance CatalogEndpoint, disableAndWait func() error) error {
	conn, err := pgx.Connect(ctx, maintenance.DSN())
	if err != nil {
		return fmt.Errorf("connect before disabling reshard maintenance identity %s: %w", maintenance.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	return disableMaintenanceAndTerminateOnSession(ctx, conn, maintenance.User, disableAndWait)
}

type reshardSQLSession interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func disableMaintenanceAndTerminateOnSession(ctx context.Context, session reshardSQLSession, username string, disableAndWait func() error) error {
	// PgBouncer can accept the client before it has authenticated a PostgreSQL
	// backend. Force that authentication while the maintenance role still has
	// LOGIN. CNPG's shard Pooler uses session mode, so this backend remains
	// attached without holding a transaction across the reconciliation wait.
	if _, err := session.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("authenticate backend before disabling reshard maintenance identity: %w", err)
	}

	// Once PostgreSQL confirms NOLOGIN, this already-authenticated backend can
	// terminate every other maintenance session and then close itself. There
	// is no gap in which a new privileged session can race cleanup.
	if err := disableAndWait(); err != nil {
		return err
	}
	return terminateRoleSessionsAndWait(ctx, session, username, "")
}

func terminateRoleSessionsAndWait(ctx context.Context, conn reshardSQLSession, username, database string) error {
	const terminate = `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE usename = $1
  AND ($2 = '' OR datname = $2)
  AND pid <> pg_backend_pid()`
	if _, err := conn.Exec(ctx, terminate, username, database); err != nil {
		return fmt.Errorf("terminate role sessions: %w", err)
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		var remaining int64
		if err := conn.QueryRow(ctx, `
SELECT count(*)
FROM pg_stat_activity
WHERE usename = $1 AND ($2 = '' OR datname = $2) AND pid <> pg_backend_pid()`,
			username, database).Scan(&remaining); err != nil {
			return fmt.Errorf("count role sessions: %w", err)
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for role sessions to terminate: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
