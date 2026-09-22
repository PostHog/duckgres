//go:build kubernetes

package controlplane

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const envTrinoDefaultCell = "DUCKGRES_TRINO_DEFAULT_CELL"

// resolveTrinoDefaultCell validates placement before any fleet bootstrap writes.
// Only new assignments use this default; it never changes a persisted owner.
func resolveTrinoDefaultCell(cells []trinoRegisteredCell) (string, error) {
	target := strings.TrimSpace(os.Getenv(envTrinoDefaultCell))
	if target == "" {
		return "", nil
	}
	for _, cell := range cells {
		if cell.ID != target {
			continue
		}
		if strings.TrimSpace(cell.Mode) != trinoPoolModeShared || cell.Pool == nil || !cell.Pool.TenantAdmission {
			return "", fmt.Errorf("%s requires a registered shared-pool cell with tenant_admission enabled", envTrinoDefaultCell)
		}
		for _, key := range []string{envTrinoPoolEnabled, envTrinoPoolOperatorEnabled, envTrinoPoolCatalogWriter} {
			enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(key)))
			if err != nil || !enabled {
				return "", fmt.Errorf("%s requires %s=true", envTrinoDefaultCell, key)
			}
		}
		return registeredTrinoCellPrefix + cell.ID, nil
	}
	return "", fmt.Errorf("%s does not name a registered cell", envTrinoDefaultCell)
}
