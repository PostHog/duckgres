//go:build kubernetes

package controlplane

import (
	"os"
	"strings"
	"testing"
)

func TestTrinoDefaultCellConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, target, mode          string
		admission, gates, wantError bool
	}{
		{"unset", "", "", false, false, false},
		{"shared pool", "pool-test", trinoPoolModeShared, true, true, false},
		{"padded mode", "pool-test", " shared-pool ", true, true, false},
		{"unknown", "missing", trinoPoolModeShared, true, true, true},
		{"legacy", "legacy", trinoPoolModeShared, true, true, true},
		{"fixed", "pool-test", trinoPoolModeFixed, true, true, true},
		{"no admission", "pool-test", trinoPoolModeShared, false, true, true},
		{"disabled gates", "pool-test", trinoPoolModeShared, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envTrinoDefaultCell, tc.target)
			for _, key := range []string{envTrinoPoolEnabled, envTrinoPoolOperatorEnabled, envTrinoPoolCatalogWriter} {
				value := "false"
				if tc.gates {
					value = "true"
				}
				t.Setenv(key, value)
			}
			cells := []trinoRegisteredCell{{ID: "pool-test", Mode: tc.mode, Pool: &trinoRegisteredPool{TenantAdmission: tc.admission}}}
			id, err := resolveTrinoDefaultCell(cells)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v", err)
			}
			if err == nil && tc.target != "" && id != "registered:pool-test" {
				t.Fatal("default not selected")
			}
			if tc.target == "" && id != "" {
				t.Fatal("unset default selected a cell")
			}
		})
	}
}

func TestTrinoDefaultCellRegistryWiring(t *testing.T) {
	path := sharedPoolRegistry(t, blueprintFile(t))
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), `"desired_instances":3`, `"tenant_admission":true,"desired_instances":3`, 1))
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	withPoolEnv(t, map[string]string{envTrinoCellsFile: path, envTrinoRegistryOnly: "true", envTrinoCoordinatorURL: "", envTrinoDefaultCell: "cell-001", envTrinoPoolEnabled: "true", envTrinoPoolOperatorEnabled: "true", envTrinoPoolCatalogWriter: "true"})
	cells, id, err := resolveTrinoCells()
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || id != "registered:cell-001" || cells[0].ID != "registered:cell-001" {
		t.Fatalf("wrong cells: %+v", cells)
	}
}
