//go:build kubernetes

package controlplane

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrinoRegistryRuntimePreservesLegacyAndSkipsStoppedBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cells.json")
	if err := os.WriteFile(path, []byte(testTrinoRegistryJSON), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envTrinoCellsFile, path)
	t.Setenv(envTrinoCoordinatorURL, "https://legacy.example.test")
	t.Setenv(envTrinoCellID, "cell-001")
	t.Setenv(envTrinoNamespace, "trino-legacy")
	cells, err := resolveTrinoCells()
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 2 || cells[0].ID != "cell-001" || cells[0].consoleCell().ID != "legacy" {
		t.Fatalf("legacy ownership changed: %+v", cells)
	}
	if cells[1].ID != "registered:cell-test" || cells[1].consoleCell().ID != "cell-test" {
		t.Fatalf("registry storage identity collision: %+v", cells[1])
	}
	if cells[1].CoordinatorURL != "https://blue.example.test" || len(cells[1].Backends) != 2 {
		t.Fatal("runtime discarded blue or stopped green")
	}
	t.Setenv(envTrinoCoordinatorURL, "")
	if _, err := resolveTrinoCells(); err == nil {
		t.Fatal("registry silently removed legacy")
	}
}

const testTrinoRegistryJSON = `{"cells":[{"id":"cell-test","namespace":"trino-test","client_url":"https://gateway.example.test","routing_group":"cell-test","backends":[{"id":"blue","coordinator_url":"https://blue.example.test","running":true,"routing_active":true,"internal_secret_name":"blue-internal"},{"id":"green","coordinator_url":"https://green.example.test","running":false,"routing_active":false,"internal_secret_name":"green-internal"}]}]}`

func TestTrinoRegistryPreservesStoppedBackend(t *testing.T) {
	cells, err := parseTrinoCellRegistry([]byte(testTrinoRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || cells[0].ID != "cell-test" {
		t.Fatalf("unexpected cell identities: %+v", cells)
	}
	if len(cells[0].Backends) != 2 || !cells[0].Backends[0].Running || cells[0].Backends[1].Running || cells[0].Backends[1].RoutingActive {
		t.Fatalf("stopped backend configuration changed: %+v", cells[0].Backends)
	}
}

func TestTrinoRegistryRejectsUnsafeConfiguration(t *testing.T) {
	tests := map[string]string{
		"unknown field":                strings.Replace(testTrinoRegistryJSON, `"cells":`, `"typo":`, 1),
		"reserved legacy identity":     strings.Replace(testTrinoRegistryJSON, `"id":"cell-test"`, `"id":"legacy"`, 1),
		"unsafe namespace":             strings.Replace(testTrinoRegistryJSON, `"namespace":"trino-test"`, `"namespace":"../other"`, 1),
		"credentials field":            strings.Replace(testTrinoRegistryJSON, `"cells":`, `"password":"secret","cells":`, 1),
		"empty registry":               `{"cells":[]}`,
		"no backends":                  `{"cells":[{"id":"cell-test","namespace":"trino-test","client_url":"https://gateway.example.test","routing_group":"cell-test","backends":[]}]}`,
		"plain HTTP":                   strings.Replace(testTrinoRegistryJSON, `https://blue.example.test`, `http://blue.example.test`, 1),
		"embedded credentials":         strings.Replace(testTrinoRegistryJSON, `https://blue.example.test`, `https://user:secret@blue.example.test`, 1),
		"URL query":                    strings.Replace(testTrinoRegistryJSON, `https://blue.example.test`, `https://blue.example.test?token=value`, 1),
		"URL path":                     strings.Replace(testTrinoRegistryJSON, `https://blue.example.test`, `https://blue.example.test/catalogs`, 1),
		"active backend stopped":       strings.Replace(testTrinoRegistryJSON, `"running":true`, `"running":false`, 1),
		"no active backend":            strings.Replace(testTrinoRegistryJSON, `"routing_active":true`, `"routing_active":false`, 1),
		"two active backends":          strings.Replace(testTrinoRegistryJSON, `"running":false,"routing_active":false`, `"running":true,"routing_active":true`, 1),
		"duplicate backend identity":   strings.Replace(testTrinoRegistryJSON, `"id":"green"`, `"id":"blue"`, 1),
		"duplicate endpoint":           strings.Replace(testTrinoRegistryJSON, `https://green.example.test`, `https://blue.example.test`, 1),
		"duplicate canonical endpoint": strings.Replace(testTrinoRegistryJSON, `https://green.example.test`, `https://BLUE.example.test.:0443/`, 1),
		"shared internal secret":       strings.Replace(testTrinoRegistryJSON, `green-internal`, `blue-internal`, 1),
		"invalid internal secret":      strings.Replace(testTrinoRegistryJSON, `green-internal`, `../other`, 1),
		"header injection":             strings.Replace(testTrinoRegistryJSON, `"routing_group":"cell-test"`, `"routing_group":"cell-test\r\nHost: other"`, 1),
		"trailing document":            testTrinoRegistryJSON + `{}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTrinoCellRegistry([]byte(data)); err == nil {
				t.Fatal("unsafe registry accepted")
			}
		})
	}
}

func TestTrinoRegistryRuntimeRejectsLegacyCollisions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cells.json")
	if err := os.WriteFile(path, []byte(testTrinoRegistryJSON), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envTrinoCellsFile, path)
	t.Setenv(envTrinoCoordinatorURL, "https://legacy.example.test")
	t.Setenv(envTrinoNamespace, "legacy")
	t.Setenv(envTrinoCellID, "legacy-owned")
	for _, tc := range []struct{ name, key, value string }{
		{"namespace", envTrinoNamespace, "trino-test"},
		{"endpoint", envTrinoCoordinatorURL, "https://blue.example.test.:0443/"},
		{"storage prefix", envTrinoCellID, "registered:cell-test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := resolveTrinoCells(); err == nil {
				t.Fatal("unsafe registry accepted")
			}
		})
	}
}

func TestTrinoRegistryRejectsDuplicateCellOwnership(t *testing.T) {
	cell := strings.TrimSuffix(strings.TrimPrefix(testTrinoRegistryJSON, `{"cells":[`), `]}`)
	other := strings.NewReplacer("cell-test", "cell-other", "trino-test", "trino-other", "blue.example", "other-blue.example", "green.example", "other-green.example").Replace(cell)
	for name, second := range map[string]string{
		"identity":      strings.Replace(other, `"id":"cell-other"`, `"id":"cell-test"`, 1),
		"namespace":     strings.Replace(other, "trino-other", "trino-test", 1),
		"routing group": strings.Replace(other, `"routing_group":"cell-other"`, `"routing_group":"cell-test"`, 1),
		"endpoint":      strings.Replace(other, "other-blue.example", "blue.example", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTrinoCellRegistry([]byte(`{"cells":[` + cell + `,` + second + `]}`)); err == nil {
				t.Fatal("duplicate cell ownership accepted")
			}
		})
	}
}
