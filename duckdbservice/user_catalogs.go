package duckdbservice

import (
	"context"
	"fmt"
	"strings"
)

// systemCatalogNames are the attached databases the worker itself manages on a
// shared-warm instance: activation attaches (and re-attaches) them, so a
// session-boundary detach must leave them alone.
var systemCatalogNames = map[string]struct{}{
	"ducklake": {},
	"delta":    {},
}

// systemCatalogPrefixes covers catalogs DuckDB extensions attach on the
// worker's behalf — DuckLake attaches its metadata store under this prefix.
var systemCatalogPrefixes = []string{"__ducklake_metadata_"}

// isSystemCatalog reports whether an attached database is worker-managed.
// DuckDB catalog names are case-insensitive.
func isSystemCatalog(name string) bool {
	name = strings.ToLower(name)
	if _, ok := systemCatalogNames[name]; ok {
		return true
	}
	for _, p := range systemCatalogPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// detachUserCatalogs detaches every database a client ATTACHed on the shared
// per-org instance, preserving only what the worker manages itself: DuckDB's
// internal catalogs (system/temp), the instance's primary database, and the
// isSystemCatalog allowlist.
//
// Attached databases are instance-global, exactly like secrets, so an ATTACH
// survives its session on a hot-idle worker and is inherited by whichever
// session the worker serves next. Two distinct failures follow:
//
//   - Cross-user isolation: an attached catalog freezes its connection string —
//     credentials included — at ATTACH time, so it keeps working after
//     wipeUserSecrets has dropped the secret it was built from. User B of the
//     org could query user A's attached Postgres without ever holding A's
//     credential.
//   - Silent mis-routing: clients attach with `ATTACH IF NOT EXISTS ... AS db`,
//     which is a no-op when a previous session left a `db` behind, so the new
//     session reads whatever target the PREVIOUS session chose. On 2026-09-18 a
//     tenant's sqlmesh run inherited an analyst's `db` (a different endpoint)
//     and its parallel postgres scans failed with `SET TRANSACTION SNAPSHOT
//     ... snapshot does not exist`.
//
// Like the secrets wipe this is only safe with one session per worker: with a
// concurrent session it would detach a catalog out from under a live query.
//
// Returns the names that were detached.
func detachUserCatalogs(ctx context.Context, h secretDBHandle) ([]string, error) {
	// The primary database is the one the instance was opened on — the lowest
	// non-internal oid, since everything else is attached afterwards. It is
	// `memory` on k8s workers but a file stem when DataDir is set, so resolve it
	// rather than hard-coding a name.
	rows, err := h.QueryContext(ctx, `
		SELECT database_name,
		       database_oid = (SELECT MIN(database_oid) FROM duckdb_databases() WHERE NOT internal) AS is_primary
		FROM duckdb_databases()
		WHERE NOT internal`)
	if err != nil {
		return nil, fmt.Errorf("list attached databases: %w", err)
	}
	var candidates []string
	for rows.Next() {
		var name string
		var isPrimary bool
		if err := rows.Scan(&name, &isPrimary); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if isPrimary || isSystemCatalog(name) {
			continue
		}
		candidates = append(candidates, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	var detached []string
	for _, name := range candidates {
		if _, err := h.ExecContext(ctx, "DETACH DATABASE IF EXISTS "+quoteSecretIdent(name)); err != nil {
			return detached, fmt.Errorf("detach database %q: %w", name, err)
		}
		detached = append(detached, name)
	}
	return detached, nil
}
