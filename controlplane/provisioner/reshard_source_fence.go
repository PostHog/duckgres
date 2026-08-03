//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
	// Callers provide a direct PostgreSQL endpoint so the final administrative
	// session is never returned to a pool after NOLOGIN. Once PostgreSQL
	// confirms NOLOGIN, this already-authenticated session can terminate every
	// other maintenance session and then close its own backend.
	conn, err := pgx.Connect(ctx, maintenance.DSN())
	if err != nil {
		return fmt.Errorf("connect before disabling reshard maintenance identity %s: %w", maintenance.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if err := disableAndWait(); err != nil {
		return err
	}
	return terminateRoleSessionsAndWait(ctx, conn, maintenance.User, "")
}

func terminateRoleSessionsAndWait(ctx context.Context, conn *pgx.Conn, username, database string) error {
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
