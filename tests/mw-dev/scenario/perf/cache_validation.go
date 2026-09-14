package perf

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

// validateTrinoCacheMode reads the isolated benchmark catalog store, not the
// requested environment flag: an older control plane can silently ignore it.
func validateTrinoCacheMode(ctx context.Context, dsn, catalog string, protocol perfcore.Protocol, cellID string) error {
	if dsn == "" {
		return fmt.Errorf("trino benchmarks require a catalog store DSN (DUCKGRES_SCENARIO_TRINO_CATALOG_STORE_DSN) to verify the deployed cache mode")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		// Connection errors may contain credentials. Never include them in artifacts.
		return fmt.Errorf("connect to isolated Trino catalog store for cache verification")
	}
	defer func() { _ = conn.Close(context.Background()) }()
	return checkTrinoCatalogCacheMode(ctx, conn, catalog, protocol, cellID)
}

type catalogCacheQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func checkTrinoCatalogCacheMode(ctx context.Context, db catalogCacheQuery, catalog string, protocol perfcore.Protocol, cellID string) error {
	var count int64
	var mode *string
	if cellID == "" {
		return fmt.Errorf("cache verification requires an explicit Trino catalog store cell ID")
	}
	err := db.QueryRow(ctx, `SELECT count(*), min(properties::jsonb ->> 'fs.cache.enabled')
 FROM trino_catalogs WHERE catalog_name = $1 AND cell_id = $2`, catalog, cellID).Scan(&count, &mode)
	if err != nil {
		return fmt.Errorf("read Trino catalog cache setting from isolated catalog store")
	}
	if count != 1 {
		return fmt.Errorf("cache verification requires exactly one matching Trino catalog; found %d", count)
	}
	if mode == nil {
		return fmt.Errorf("trino catalog has no explicit fs.cache.enabled; rebuild the control plane and provision a fresh benchmark namespace")
	}
	expected := "false"
	if protocol == perfcore.ProtocolTrinoCached {
		expected = "true"
	}
	if *mode != expected {
		return fmt.Errorf("benchmark target %s requires fs.cache.enabled=%s; provision a fresh namespace with the matching cache mode", protocol, expected)
	}
	return nil
}
