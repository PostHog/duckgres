//go:build kubernetes

package controlplane

import (
	"strings"
	"testing"
)

const testTrinoRegistryJSON = `{"cells":[{"id":"cell-test","namespace":"trino-test","client_url":"https://gateway.example.test","routing_group":"cell-test","backends":[{"id":"blue","coordinator_url":"https://blue.example.test","running":true,"routing_active":true,"internal_secret_name":"blue-internal"},{"id":"green","coordinator_url":"https://green.example.test","running":false,"routing_active":false,"internal_secret_name":"green-internal"}]}]}`
const testTrinoPoolRegistryJSON = `{"cells":[{"id":"cell-test","namespace":"trino-test","client_url":"https://gateway.example.test","routing_group":"cell-test","mode":"shared-pool","pool":{"coordinator_service_port":8080}}]}`

func TestTrinoRegistryAcceptsSharedPools(t *testing.T) {
	for _, endpoint := range []string{"https://gateway.example.test", "https://{database_name}.example.test"} {
		cells, err := parseTrinoCellRegistry([]byte(strings.Replace(testTrinoPoolRegistryJSON, "https://gateway.example.test", endpoint, 1)))
		if err != nil || len(cells) != 1 || cells[0].ID != "cell-test" {
			t.Fatalf("cells=%+v error=%v", cells, err)
		}
	}
}

func TestTrinoRegistryRejectsUnsafeConfiguration(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field":     strings.Replace(testTrinoPoolRegistryJSON, `"cells":`, `"typo":`, 1),
		"unsafe namespace":  strings.Replace(testTrinoPoolRegistryJSON, `"namespace":"trino-test"`, `"namespace":"../other"`, 1),
		"invalid group":     strings.Replace(testTrinoPoolRegistryJSON, `"routing_group":"cell-test"`, `"routing_group":"bad group"`, 1),
		"legacy identity":   strings.Replace(testTrinoPoolRegistryJSON, `"id":"cell-test"`, `"id":"legacy"`, 1),
		"empty registry":    `{"cells":[]}`,
		"fixed topology":    testTrinoRegistryJSON,
		"unknown topology":  strings.Replace(testTrinoPoolRegistryJSON, "shared-pool", "unknown", 1),
		"credentials":       strings.Replace(testTrinoPoolRegistryJSON, "https://gateway.example.test", "https://user:secret@example.test", 1),
		"plain HTTP":        strings.Replace(testTrinoPoolRegistryJSON, "https://gateway.example.test", "http://gateway.example.test", 1),
		"bad placeholder":   strings.Replace(testTrinoPoolRegistryJSON, "https://gateway.example.test", "https://gateway.{database_name}.example.test", 1),
		"trailing document": testTrinoPoolRegistryJSON + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTrinoCellRegistry([]byte(body)); err == nil {
				t.Fatal("unsafe registry accepted")
			}
		})
	}
}

func TestTrinoRegistryRejectsDuplicateCellOwnership(t *testing.T) {
	cell := strings.TrimSuffix(strings.TrimPrefix(testTrinoPoolRegistryJSON, `{"cells":[`), `]}`)
	other := strings.NewReplacer("cell-test", "cell-other", "trino-test", "trino-other").Replace(cell)
	for name, second := range map[string]string{
		"identity":      strings.Replace(other, `"id":"cell-other"`, `"id":"cell-test"`, 1),
		"namespace":     strings.Replace(other, "trino-other", "trino-test", 1),
		"routing group": strings.Replace(other, `"routing_group":"cell-other"`, `"routing_group":"cell-test"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTrinoCellRegistry([]byte(`{"cells":[` + cell + `,` + second + `]}`)); err == nil {
				t.Fatal("duplicate ownership accepted")
			}
		})
	}
}
