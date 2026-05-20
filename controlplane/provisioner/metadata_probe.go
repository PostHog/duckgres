package provisioner

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"database/sql"
)

// ProbeMetadataStore does a real end-to-end connect through the duckling's
// pgbouncer to the metadata Postgres and runs SELECT 1.
//
// Purpose: AWS Aurora reports a cluster as Available before the cluster's DNS
// record has propagated to in-cluster CoreDNS, and even further before
// pgbouncer's resolver picks it up. If the provisioner flips the warehouse
// to state=ready on AWS-Available alone, worker activations during the next
// 3-5 minutes hit "DuckLake migration check failed → connect to metadata
// store: DNS lookup failed". This probe closes that window — we only flip
// to ready when the whole pgbouncer → RDS path actually works.
//
// `pgbouncerEndpoint` is expected to be "<host>:<port>", as written by the
// Crossplane composition to status.metadataStore.pgbouncerEndpoint.
// `sslmode` should be "disable" — the duckling pgbouncer is configured for
// plaintext between CP/worker and pgbouncer (and pgbouncer brings TLS to RDS).
func ProbeMetadataStore(ctx context.Context, pgbouncerEndpoint, user, password, database string) error {
	host, port, err := net.SplitHostPort(pgbouncerEndpoint)
	if err != nil {
		return fmt.Errorf("parse pgbouncer endpoint %q: %w", pgbouncerEndpoint, err)
	}
	if user == "" || password == "" || database == "" {
		return fmt.Errorf("missing metadata-store credentials: user=%q db=%q password_set=%v", user, database, password != "")
	}

	dsn := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable&connect_timeout=5",
	}).String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open pgbouncer dsn: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Bound the entire probe (connect + query) so a slow DNS retry inside
	// pgbouncer doesn't hold up the reconcile loop.
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	var one int
	if err := db.QueryRowContext(probeCtx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("SELECT 1 via pgbouncer: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("unexpected SELECT 1 result: %d", one)
	}
	return nil
}
