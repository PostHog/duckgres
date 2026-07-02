package controlplane

import (
	"fmt"
	"strings"
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

// effectiveSessionDefaultCommand returns the connect-time command for a
// non-passthrough session, given the resolved real catalog the session defaults
// to (effectiveCatalog, "ducklake").
//
// For DuckLake the catalog switch is owned by InitSessionDatabaseMetadata's
// defer (which also restores memory.main on the search_path so the pg_catalog
// compat macros stay resolvable), so a DuckLake session only needs a command
// when the client supplied its own search_path.
func effectiveSessionDefaultCommand(clientSearchPath, effectiveCatalog string) (string, sessionSearchPathSource) {
	if clientSearchPath != "" {
		searchPath := fmt.Sprintf("SET search_path = '%s'", ensureMemoryMainInSearchPath(clientSearchPath))
		return searchPath, sessionSearchPathSourceClient
	}
	return "", ""
}

// passthroughSessionDefaultCatalogCommand returns the connect-time command that
// points a passthrough session at the catalog it selected (effectiveCatalog).
// Passthrough users skip InitSessionDatabaseMetadata (whose defer issues the
// catalog `USE` for the standard path), so without this the session stays in
// DuckDB's empty in-memory catalog — current_database() reports "memory" and
// unqualified DDL/DML never reaches the warehouse. Mirrors
// server.setDuckLakeDefault used by the standalone passthrough path.
func passthroughSessionDefaultCatalogCommand(effectiveCatalog string) string {
	if effectiveCatalog == physicalDuckLakeCatalog {
		return "USE " + physicalDuckLakeCatalog
	}
	return ""
}

// resolveEffectiveCatalog picks the real catalog a session should default to.
// requested is the validated startup selection ("" → use the attached default,
// or "ducklake"). duckLakeAttached reflects what the worker actually attached
// for this session. The bool is false when the requested catalog isn't
// attached (caller should fail the connection 3D000) or nothing is attached
// at all.
func resolveEffectiveCatalog(requested string, duckLakeAttached bool) (string, bool) {
	switch requested {
	case physicalDuckLakeCatalog, "":
		if duckLakeAttached {
			return physicalDuckLakeCatalog, true
		}
		return "", false
	}
	return "", false
}

func ensureMemoryMainInSearchPath(searchPath string) string {
	if strings.Contains(strings.ToLower(searchPath), "memory.main") {
		return searchPath
	}
	return searchPath + ",memory.main"
}
