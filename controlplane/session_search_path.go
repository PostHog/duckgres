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

func effectiveSessionDefaultCommand(clientSearchPath, defaultCatalog string) (string, sessionSearchPathSource) {
	switch {
	case clientSearchPath != "":
		return fmt.Sprintf("SET search_path = '%s'", ensureMemoryMainInSearchPath(clientSearchPath)), sessionSearchPathSourceClient
	case defaultCatalog == iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema), sessionDefaultSourceConfiguredCatalog
	default:
		return "", ""
	}
}

// passthroughSessionDefaultCatalogCommand returns the connect-time command that
// points a passthrough session at the tenant's catalog. Passthrough users skip
// InitSessionDatabaseMetadata (whose defer issues `USE ducklake` for the
// standard path), so without this the session stays in DuckDB's empty in-memory
// catalog — current_database() reports "memory" and unqualified DDL/DML never
// reaches the warehouse. Mirrors server.setIcebergDefault / setDuckLakeDefault
// used by the standalone passthrough path.
func passthroughSessionDefaultCatalogCommand(defaultCatalog string, duckLakeAttached bool) string {
	switch {
	case defaultCatalog == iceberg.CatalogName:
		return fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema)
	case duckLakeAttached:
		return "USE ducklake"
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
