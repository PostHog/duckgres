package controlplane

import (
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/iceberg"
)

type sessionSearchPathSource string

const (
	sessionSearchPathSourceClient         sessionSearchPathSource = "client"
	sessionDefaultSourceConfiguredCatalog sessionSearchPathSource = "configured_catalog"
)

// physicalDuckLakeCatalog is the name the per-tenant DuckLake catalog is
// attached as on the worker (the `ATTACH ... AS ducklake` performed during
// activation; matches server.physicalDuckLakeCatalog). Keep this and the
// HasAttachedCatalog probe in control.go in sync.
const physicalDuckLakeCatalog = "ducklake"

// physicalIcebergCatalog is the name the per-tenant Iceberg catalog is attached
// as on the worker (the `ATTACH ... AS iceberg` during activation).
const physicalIcebergCatalog = iceberg.CatalogName

// effectiveSessionDefaultCommand returns the connect-time command for a
// non-passthrough session, given the resolved real catalog the session defaults
// to (effectiveCatalog, one of "ducklake"/"iceberg"). A client-supplied
// search_path always wins. For DuckLake the catalog switch is owned by
// InitSessionDatabaseMetadata's defer (which also restores memory.main on the
// search_path so the pg_catalog compat macros stay resolvable), so this returns
// "" — re-issuing `USE ducklake` here would clobber that search_path.
func effectiveSessionDefaultCommand(clientSearchPath, effectiveCatalog string) (string, sessionSearchPathSource) {
	switch {
	case clientSearchPath != "":
		return fmt.Sprintf("SET search_path = '%s'", ensureMemoryMainInSearchPath(clientSearchPath)), sessionSearchPathSourceClient
	case effectiveCatalog == iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema), sessionDefaultSourceConfiguredCatalog
	default:
		return "", ""
	}
}

// passthroughSessionDefaultCatalogCommand returns the connect-time command that
// points a passthrough session at the catalog it selected (effectiveCatalog).
// Passthrough users skip InitSessionDatabaseMetadata (whose defer issues the
// catalog `USE` for the standard path), so without this the session stays in
// DuckDB's empty in-memory catalog — current_database() reports "memory" and
// unqualified DDL/DML never reaches the warehouse. Mirrors
// server.setIcebergDefault / setDuckLakeDefault used by the standalone
// passthrough path.
func passthroughSessionDefaultCatalogCommand(effectiveCatalog string) string {
	switch effectiveCatalog {
	case iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema)
	case physicalDuckLakeCatalog:
		return "USE " + physicalDuckLakeCatalog
	default:
		return ""
	}
}

// resolveEffectiveCatalog picks the real catalog a session should default to.
// requested is the validated startup selection ("" → use the per-user/attached
// default, "ducklake", or "iceberg"). defaultCatalog is the per-user configured
// default ("" or "iceberg"). duckLakeAttached/icebergAttached reflect what the
// worker actually attached for this session. The bool is false when the
// requested catalog isn't attached (caller should fail the connection 3D000) or
// nothing is attached at all.
func resolveEffectiveCatalog(requested, defaultCatalog string, duckLakeAttached, icebergAttached bool) (string, bool) {
	switch requested {
	case physicalDuckLakeCatalog:
		if duckLakeAttached {
			return physicalDuckLakeCatalog, true
		}
		return "", false
	case iceberg.CatalogName:
		if icebergAttached {
			return iceberg.CatalogName, true
		}
		return "", false
	}
	// requested == "": fall back to the per-user default, then to whatever is
	// attached (DuckLake preferred, then Iceberg for iceberg-only orgs).
	if defaultCatalog == iceberg.CatalogName && icebergAttached {
		return iceberg.CatalogName, true
	}
	switch {
	case duckLakeAttached:
		return physicalDuckLakeCatalog, true
	case icebergAttached:
		return iceberg.CatalogName, true
	default:
		return "", false
	}
}

func ensureMemoryMainInSearchPath(searchPath string) string {
	if strings.Contains(strings.ToLower(searchPath), "memory.main") {
		return searchPath
	}
	return searchPath + ",memory.main"
}
