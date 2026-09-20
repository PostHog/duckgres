//go:build kubernetes

package controlplane

import (
	"os"
	"path/filepath"
	"testing"
)

func writeRegistry(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cells.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write registry: %v", err)
	}
	return path
}

func sharedPoolRegistry(t *testing.T, blueprintPath string) string {
	t.Helper()
	return writeRegistry(t, `{"cells":[{
		"id":"cell-001",
		"namespace":"trino-cell-001",
		"client_url":"https://{database_name}.example.invalid",
		"routing_group":"cell-001",
		"mode":"shared-pool",
		"pool":{
			"desired_instances":3,
			"min_serving":3,
			"max_surge":1,
			"max_repair":1,
			"blueprint_file":"`+blueprintPath+`",
			"coordinator_service_port":8443,
			"node_environment":"mw_dev_pool_001"
		}
	}]}`)
}

func blueprintFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(path, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	return path
}

func withPoolEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for name, value := range values {
		t.Setenv(name, value)
	}
}

// A registry that declares a shared pool while the feature flag is off must
// fail startup, not silently fall back to legacy behavior: the operator asked
// for a pool and would otherwise get fixed blue/green without being told.
func TestSharedPoolCellRequiresTheFeatureFlag(t *testing.T) {
	withPoolEnv(t, map[string]string{
		envTrinoCellsFile:    sharedPoolRegistry(t, blueprintFile(t)),
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "",
	})
	if _, err := resolveTrinoPoolConfigs(); err == nil {
		t.Fatal("a shared-pool cell was accepted with the feature disabled")
	}
}

func TestSharedPoolConfigResolves(t *testing.T) {
	withPoolEnv(t, map[string]string{
		envTrinoCellsFile:    sharedPoolRegistry(t, blueprintFile(t)),
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "true",
	})
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("resolved %d pools", len(configs))
	}
	config := configs[0]
	// The pool keeps the cell's existing LOGICAL ASSIGNMENT identity: the pool
	// id, the routing group and the namespace are all unchanged, because compute
	// replacement must not rewrite warehouse assignment. The catalog store's
	// partition is a separate, separately configured identity and is resolved
	// nowhere near here.
	if config.PoolID != registeredTrinoCellPrefix+"cell-001" || config.PublicID != "cell-001" {
		t.Fatalf("pool identity = %q / %q", config.PoolID, config.PublicID)
	}
	if config.RoutingGroup != "cell-001" || config.Namespace != "trino-cell-001" {
		t.Fatalf("routing group / namespace = %q / %q", config.RoutingGroup, config.Namespace)
	}
	if config.Spec.DesiredInstances != 3 || config.Spec.MinServing != 3 || config.Spec.MaxSurge != 1 || config.Spec.MaxRepair != 1 {
		t.Fatalf("spec = %+v", config.Spec)
	}
}

// A cell with no mode is the existing fixed blue/green cell, byte for byte.
func TestFixedCellsAreNotPools(t *testing.T) {
	withPoolEnv(t, map[string]string{
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "true",
		envTrinoCellsFile: writeRegistry(t, `{"cells":[{
			"id":"cell-002","namespace":"trino-cell-002",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-002",
			"backends":[
				{"id":"blue","coordinator_url":"https://blue.invalid","running":true,"routing_active":true,"internal_secret_name":"blue-secret"},
				{"id":"green","coordinator_url":"https://green.invalid","running":false,"routing_active":false,"internal_secret_name":"green-secret"}
			]}]}`),
	})
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("a fixed cell produced %d pools", len(configs))
	}
}

func TestSharedPoolRegistryValidation(t *testing.T) {
	blueprint := blueprintFile(t)
	cases := map[string]string{
		"backends on a pooled cell": `{"cells":[{"id":"cell-001","namespace":"trino-cell-001",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-001","mode":"shared-pool",
			"pool":{"desired_instances":3,"min_serving":3,"max_surge":1,"max_repair":1,"blueprint_file":"` + blueprint + `","coordinator_service_port":8443,"node_environment":"e"},
			"backends":[{"id":"blue","coordinator_url":"https://blue.invalid","running":true,"routing_active":true,"internal_secret_name":"s"}]}]}`,
		"missing pool block": `{"cells":[{"id":"cell-001","namespace":"trino-cell-001",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-001","mode":"shared-pool"}]}`,
		"unknown mode": `{"cells":[{"id":"cell-001","namespace":"trino-cell-001",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-001","mode":"elastic",
			"pool":{"desired_instances":3,"min_serving":3,"max_surge":1,"max_repair":1,"blueprint_file":"` + blueprint + `","coordinator_service_port":8443,"node_environment":"e"}}]}`,
		// A serving floor above the desired count can never be satisfied, so
		// the pool would refuse every planned drain forever.
		"floor above desired": `{"cells":[{"id":"cell-001","namespace":"trino-cell-001",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-001","mode":"shared-pool",
			"pool":{"desired_instances":2,"min_serving":3,"max_surge":1,"max_repair":1,"blueprint_file":"` + blueprint + `","coordinator_service_port":8443,"node_environment":"e"}}]}`,
		"zero desired instances": `{"cells":[{"id":"cell-001","namespace":"trino-cell-001",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-001","mode":"shared-pool",
			"pool":{"desired_instances":0,"min_serving":0,"max_surge":1,"max_repair":1,"blueprint_file":"` + blueprint + `","coordinator_service_port":8443,"node_environment":"e"}}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			withPoolEnv(t, map[string]string{
				envTrinoCellsFile:    writeRegistry(t, body),
				envTrinoRegistryOnly: "true",
				envTrinoPoolEnabled:  "true",
			})
			if _, err := resolveTrinoPoolConfigs(); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

// An unreadable or invalid blueprint FREEZES the pool at its last-good state.
// It must never resolve to a desired count of zero, which would delete the
// fleet, and it must not abort startup, which would take the control plane
// down over a config-map problem.
func TestUnreadableBlueprintFreezesTheConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.json")
	withPoolEnv(t, map[string]string{
		envTrinoCellsFile:    sharedPoolRegistry(t, missing),
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "true",
	})
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("resolved %d pools", len(configs))
	}
	config := configs[0]
	if !config.Frozen || config.FrozenReason == "" {
		t.Fatal("an unreadable blueprint did not freeze the pool")
	}
	if config.Blueprint != nil {
		t.Fatal("a frozen pool carries a blueprint")
	}
	if config.Spec.DesiredInstances == 0 {
		t.Fatal("a frozen pool resolved to a desired count of zero")
	}
}

func TestInvalidBlueprintFreezesTheConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(path, []byte(`{"blueprint_version":1}`), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	withPoolEnv(t, map[string]string{
		envTrinoCellsFile:    sharedPoolRegistry(t, path),
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "true",
	})
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !configs[0].Frozen {
		t.Fatal("an invalid blueprint did not freeze the pool")
	}
}

// The blueprint's namespace and the cell's namespace must agree, or duckgres
// would create instances somewhere the pool's shared trust boundary does not
// exist.
func TestBlueprintNamespaceMustMatchTheCell(t *testing.T) {
	withPoolEnv(t, map[string]string{
		envTrinoCellsFile: writeRegistry(t, `{"cells":[{"id":"cell-009","namespace":"trino-cell-009",
			"client_url":"https://{database_name}.example.invalid","routing_group":"cell-009","mode":"shared-pool",
			"pool":{"desired_instances":3,"min_serving":3,"max_surge":1,"max_repair":1,"blueprint_file":"`+blueprintFile(t)+`","coordinator_service_port":8443,"node_environment":"e"}}]}`),
		envTrinoRegistryOnly: "true",
		envTrinoPoolEnabled:  "true",
	})
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !configs[0].Frozen {
		t.Fatal("a blueprint for another namespace was accepted")
	}
}
