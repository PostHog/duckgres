//go:build kubernetes

package controlplane

import (
	"errors"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
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
			cells := []trinoCell{{ID: "legacy-owner"}, {ID: "registered:pool-test", PublicID: "pool-test", Mode: tc.mode, TenantAdmission: tc.admission}}
			err := configureTrinoDefaultCell(cells)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v", err)
			}
			if err == nil && tc.target != "" && !cells[1].DefaultPlacement {
				t.Fatal("default not selected")
			}
			if cells[0].DefaultPlacement {
				t.Fatal("legacy selected")
			}
		})
	}
}

func TestTrinoDefaultCellAdmissionPreservesOwnership(t *testing.T) {
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{ID: "legacy-owner"}}, &trinoWiring{Cell: trinoCell{ID: "registered:pool-test", PublicID: "pool-test", DefaultPlacement: true}}}
	for _, owner := range []string{"", "legacy-owner", "registered:pool-test"} {
		store := registryOnlyOrgStore{row: nil}
		if owner != "" {
			store.row = &configstore.ManagedWarehouseTrino{TrinoCellID: owner}
		}
		check := fleet.enablementCheck(store)
		if check == nil || check("tenant") != nil {
			t.Fatalf("owner %q rejected", owner)
		}
	}
	if fleet.defaultCellID() != "registered:pool-test" {
		t.Fatal("wrong durable ID")
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
	cells, err := resolveTrinoCells()
	if err != nil {
		t.Fatal(err)
	}
	if err := configureTrinoDefaultCell(cells); err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || !cells[0].DefaultPlacement || cells[0].ID != "registered:cell-001" {
		t.Fatalf("wrong cells: %+v", cells)
	}
}
func TestTrinoDefaultCellAdmissionDoesNotHideUnknownOwnerOrReadFailure(t *testing.T) {
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{ID: "registered:pool-test", PublicID: "pool-test", DefaultPlacement: true}}, &trinoWiring{Cell: trinoCell{ID: "legacy-owner"}}}
	unknown := registryOnlyOrgStore{row: &configstore.ManagedWarehouseTrino{TrinoCellID: "registered:removed"}}
	if err := fleet.enablementCheck(unknown)("tenant"); !errors.Is(err, provisioning.ErrTrinoCellNotConfigured) {
		t.Fatalf("unknown owner: %v", err)
	}
	readErr := errors.New("store unavailable")
	if err := fleet.enablementCheck(registryOnlyOrgStore{err: readErr})("tenant"); !errors.Is(err, readErr) {
		t.Fatalf("read failure: %v", err)
	}
}
