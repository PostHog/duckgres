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

func ensureMemoryMainInSearchPath(searchPath string) string {
	if strings.Contains(strings.ToLower(searchPath), "memory.main") {
		return searchPath
	}
	return searchPath + ",memory.main"
}
