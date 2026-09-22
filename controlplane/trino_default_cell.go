//go:build kubernetes

package controlplane

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const envTrinoDefaultCell = "DUCKGRES_TRINO_DEFAULT_CELL"

// configureTrinoDefaultCell validates placement before any fleet bootstrap writes.
// Only new assignments use this default; it never changes a persisted owner.
func configureTrinoDefaultCell(cells []trinoCell) error {
	target := strings.TrimSpace(os.Getenv(envTrinoDefaultCell))
	if target == "" {
		return nil
	}
	for i := range cells {
		cell := &cells[i]
		if cell.PublicID != target {
			continue
		}
		if cell.Mode != trinoPoolModeShared || !cell.TenantAdmission {
			return fmt.Errorf("%s requires a registered shared-pool cell with tenant_admission enabled", envTrinoDefaultCell)
		}
		for _, key := range []string{envTrinoPoolEnabled, envTrinoPoolOperatorEnabled, envTrinoPoolCatalogWriter} {
			enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(key)))
			if err != nil || !enabled {
				return fmt.Errorf("%s requires %s=true", envTrinoDefaultCell, key)
			}
		}
		cell.DefaultPlacement = true
		return nil
	}
	return fmt.Errorf("%s does not name a registered cell", envTrinoDefaultCell)
}

func (f trinoFleet) defaultCellID() string {
	for _, wire := range f {
		if wire.Cell.DefaultPlacement {
			return wire.Cell.ID
		}
	}
	return ""
}
