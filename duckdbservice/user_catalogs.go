package duckdbservice

import (
	"context"
	"fmt"
	"strings"
)

// reservedCatalogNames are the attached catalogs that must survive the
// session-create wipe: the org's DuckLake/Delta catalogs (activation owns
// them — detaching would break the session's metadata init) and `memory`,
// which carries the pg_catalog compatibility layer (memory.main.*).
// `system`/`temp` are excluded structurally instead: duckdb_databases()
// marks them internal, and DuckDB refuses to detach them.
var reservedCatalogNames = map[string]bool{
	"ducklake": true,
	"delta":    true,
	"memory":   true,
}

// wipeUserCatalogs detaches every user-attached catalog on the shared per-org
// DuckDB instance. DuckDB ATTACH is instance-global and a hot-idle worker is
// reused across sessions of an org, so without this a catalog attached by one
// session (say an external Postgres source with a live, authenticated
// connection pool inside postgres_scanner) is silently inherited by the next
// session — the same cross-user isolation boundary as wipeUserSecrets, one
// level up. (A stale inherited attach also carries its creation-time
// connection-pool configuration: postgres_scanner's pg_pool_max_connections
// SET only applies at pool creation, so the inheriting session cannot
// reconfigure the pool it just inherited.)
//
// A wipe failure fails the session: handing user A's attached sources (and
// their authenticated upstream connections) to user B is not acceptable.
// Returns the names that were detached.
func wipeUserCatalogs(ctx context.Context, h secretDBHandle) ([]string, error) {
	rows, err := h.QueryContext(ctx, "SELECT database_name, internal FROM duckdb_databases()")
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		var internal bool
		if err := rows.Scan(&name, &internal); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if internal || reservedCatalogNames[name] {
			continue
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close()

	var wiped []string
	for _, name := range names {
		// Catalog names are user-controlled; quote defensively.
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if _, err := h.ExecContext(ctx, "DETACH DATABASE IF EXISTS "+quoted); err != nil {
			return wiped, fmt.Errorf("detach catalog %q: %w", name, err)
		}
		wiped = append(wiped, name)
	}
	return wiped, nil
}
