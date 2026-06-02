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
// non-passthrough session, given the resolved physical catalog. A
// client-supplied search_path always wins. For DuckLake the catalog switch is
// owned by InitSessionDatabaseMetadata's defer (which also restores memory.main
// on the search_path so the pg_catalog compat macros stay resolvable), so this
// returns "" — re-issuing `USE ducklake` here would clobber that search_path.
func effectiveSessionDefaultCommand(clientSearchPath, physicalCatalog string) (string, sessionSearchPathSource) {
	switch {
	case clientSearchPath != "":
		return fmt.Sprintf("SET search_path = '%s'", ensureMemoryMainInSearchPath(clientSearchPath)), sessionSearchPathSourceClient
	case physicalCatalog == iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema), sessionDefaultSourceConfiguredCatalog
	default:
		return "", ""
	}
}

// passthroughSessionDefaultCatalogCommand returns the connect-time command that
// points a passthrough session at the selected physical catalog.
// Passthrough users skip InitSessionDatabaseMetadata (whose defer issues the
// catalog `USE` for the standard path), so without this the session stays in
// DuckDB's empty in-memory catalog — current_database() reports "memory" and
// unqualified DDL/DML never reaches the warehouse. Mirrors
// server.setIcebergDefault / setDuckLakeDefault used by the standalone
// passthrough path.
func passthroughSessionDefaultCatalogCommand(physicalCatalog string) string {
	switch physicalCatalog {
	case iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema)
	case physicalDuckLakeCatalog:
		return "USE " + physicalDuckLakeCatalog
	default:
		return ""
	}
}

func ensureMemoryMainInSearchPath(searchPath string) string {
	if strings.Contains(strings.ToLower(searchPath), "memory.main") {
		return searchPath
	}
	return searchPath + ",memory.main"
}
