package provisioner

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// defaultMetadataPort is the Postgres port to assume when the duckling's
// metadataStore.endpoint is just a hostname. Aurora Postgres defaults to
// 5432; the Crossplane composition currently writes only the hostname into
// status.metadataStore.endpoint, so we default.
const defaultMetadataPort = "5432"

// ProbeMetadataStore does a real end-to-end connect to the metadata Postgres
// and runs SELECT 1 to confirm the path is reachable from a CP pod.
//
// Purpose: AWS Aurora reports a cluster as Available before the cluster's
// DNS record has propagated to in-cluster CoreDNS, and even further before
// pgbouncer's resolver picks it up. If the provisioner flips the warehouse
// to state=ready on AWS-Available alone, worker activations during the next
// 3-5 minutes hit "DuckLake migration check failed → connect to metadata
// store: DNS lookup failed". This probe closes that window — we only flip
// to ready when the actual path the workers will use actually works.
//
// `endpoint` is "<host>[:<port>]"; missing port defaults to 5432.
// `sslMode` should be:
//   - "disable" when probing through the duckling's pgbouncer (the CR's
//     pgbouncer pod is configured for plaintext between worker and pooler
//     and brings TLS to RDS itself)
//   - "require" when probing the metadata RDS endpoint directly (Aurora
//     requires TLS; AWS issues the cert from the AWS-RDS-Root CA chain)
//
// The caller (provisioner controller) decides which path to probe based on
// the warehouse's pgbouncer.enabled toggle — ducklings can run with or
// without the pgbouncer pooler and the chosen path must match.
func ProbeMetadataStore(ctx context.Context, endpoint, user, password, database, sslMode string) error {
	if endpoint == "" {
		return fmt.Errorf("missing metadata-store endpoint")
	}
	if user == "" || password == "" || database == "" {
		return fmt.Errorf("missing metadata-store credentials: user=%q db=%q password_set=%v", user, database, password != "")
	}
	if sslMode == "" {
		sslMode = "disable"
	}

	host, port := endpoint, defaultMetadataPort
	if strings.Contains(endpoint, ":") {
		h, p, err := net.SplitHostPort(endpoint)
		if err != nil {
			return fmt.Errorf("parse endpoint %q: %w", endpoint, err)
		}
		host, port = h, p
	}

	dsn := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + database,
		RawQuery: "sslmode=" + sslMode + "&connect_timeout=5",
	}).String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open metadata dsn: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Bound the entire probe (connect + query) so a slow DNS retry doesn't
	// hold up the reconcile loop.
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	var one int
	if err := db.QueryRowContext(probeCtx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("SELECT 1: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("unexpected SELECT 1 result: %d", one)
	}
	return nil
}
