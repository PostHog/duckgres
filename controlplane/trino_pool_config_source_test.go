//go:build kubernetes

package controlplane

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// poolRegistryJSON is the registry document, parameterised by the values a
// settings-only edit would change. The image is deliberately NOT one of them:
// the case that matters is a configuration change that moves nothing an image
// or release ordering could order.
func poolRegistryJSON(blueprintPath string, desired, minServing int) string {
	return `{"cells":[{
		"id":"cell-001",
		"namespace":"trino-pool-example",
		"client_url":"https://{database_name}.example.invalid",
		"routing_group":"cell-001",
		"mode":"shared-pool",
		"pool":{
			"desired_instances":` + itoa(desired) + `,
			"min_serving":` + itoa(minServing) + `,
			"max_surge":1,
			"max_repair":1,
			"blueprint_file":"` + blueprintPath + `",
			"coordinator_service_port":8443,
			"node_environment":"mw_dev_pool_001"
		}
	}]}`
}

func itoa(value int) string {
	if value < 10 {
		return string(rune('0' + value))
	}
	return string(rune('0'+value/10)) + string(rune('0'+value%10))
}

// mountedRegistry writes ONE replica's projected copy of the configuration.
// Each call gets its own directory, because the point of these tests is that
// two replicas hold INDEPENDENT copies: a kubelet refreshes each pod's volume
// on its own schedule, and a subPath mount is never refreshed at all.
func mountedRegistry(t *testing.T, desired, minServing int) string {
	t.Helper()
	directory := t.TempDir()
	blueprint := filepath.Join(directory, "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	registry := filepath.Join(directory, "cells.json")
	if err := os.WriteFile(registry, []byte(poolRegistryJSON(blueprint, desired, minServing)), 0o600); err != nil {
		t.Fatalf("write registry: %v", err)
	}
	return registry
}

// poolConfigMap is the authoritative object: ONE value the whole fleet reads,
// with the same keys a ConfigMap volume would project as files.
func poolConfigMap(t *testing.T, desired, minServing int) kubernetes.Interface {
	t.Helper()
	return fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "duckgres-trino-pool", Namespace: "trino-pool-example"},
		Data: map[string]string{
			"cells.json":     poolRegistryJSON("/etc/duckgres/trino/blueprint.json", desired, minServing),
			"blueprint.json": string(testBlueprintJSON(t)),
		},
	})
}

func setPoolConfigMap(t *testing.T, client kubernetes.Interface, desired, minServing int) {
	t.Helper()
	configMap, err := client.CoreV1().ConfigMaps("trino-pool-example").Get(context.Background(), "duckgres-trino-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get config map: %v", err)
	}
	configMap.Data["cells.json"] = poolRegistryJSON("/etc/duckgres/trino/blueprint.json", desired, minServing)
	if _, err := client.CoreV1().ConfigMaps("trino-pool-example").Update(context.Background(), configMap, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update config map: %v", err)
	}
}

func poolAPIReader(t *testing.T, client kubernetes.Interface) trinoPoolConfigReader {
	t.Helper()
	t.Setenv(envTrinoPoolConfigMap, "duckgres-trino-pool")
	reader, err := newTrinoPoolAPIConfigReader(client, "trino-pool-example")
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	return reader
}

// Two replicas with independent projected copies read the SAME desired
// configuration, because both derive it from the API object rather than from
// their own mount.
//
// This is the case a per-tick file re-read cannot answer: replica A's volume
// still holds the previous configuration, and re-reading it more often does not
// make it current. Only the API object is one value for the whole fleet.
func TestDesiredStateComesFromTheAPIObjectNotTheMount(t *testing.T) {
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	client := poolConfigMap(t, 5, 4)

	// Replica A's mount is stale; replica B's is current but irrelevant.
	staleMount := mountedRegistry(t, 3, 3)
	freshMount := mountedRegistry(t, 5, 4)

	for _, mount := range []string{staleMount, freshMount} {
		t.Setenv(envTrinoCellsFile, mount)
		// What this replica would have published from its own copy.
		mounted, err := resolveTrinoPoolConfigByID(context.Background(), trinoPoolFileConfigReader{}, "cell-001")
		if err != nil {
			t.Fatalf("resolve from the mount: %v", err)
		}
		resolved, err := resolveTrinoPoolConfigByID(context.Background(), poolAPIReader(t, client), "cell-001")
		if err != nil {
			t.Fatalf("resolve from the API: %v", err)
		}
		if resolved.Spec.DesiredInstances != 5 || resolved.Spec.MinServing != 4 {
			t.Fatalf("resolved %d/%d from %s, want the API object's 5/4",
				resolved.Spec.DesiredInstances, resolved.Spec.MinServing, mount)
		}
		if mount == staleMount && mounted.Spec.DesiredInstances == resolved.Spec.DesiredInstances {
			t.Fatal("the stale mount and the API object agreed; this test is not exercising the divergence")
		}
	}
}

// A configuration change is picked up without anything re-reading a file and
// without the process restarting: the next resolution reads the object again.
func TestDesiredStateFollowsTheAPIObjectAsItChanges(t *testing.T) {
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoCellsFile, mountedRegistry(t, 3, 3))
	client := poolConfigMap(t, 3, 3)
	reader := poolAPIReader(t, client)

	first, err := resolveTrinoPoolConfigByID(context.Background(), reader, "cell-001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if first.Spec.DesiredInstances != 3 {
		t.Fatalf("desired instances = %d, want 3", first.Spec.DesiredInstances)
	}

	setPoolConfigMap(t, client, 5, 4)

	second, err := resolveTrinoPoolConfigByID(context.Background(), reader, "cell-001")
	if err != nil {
		t.Fatalf("re-resolve: %v", err)
	}
	if second.Spec.DesiredInstances != 5 || second.Spec.MinServing != 4 {
		t.Fatalf("desired = %d/%d, want the updated 5/4", second.Spec.DesiredInstances, second.Spec.MinServing)
	}
}

// An unreadable object is an error the caller turns into a freeze. It must
// never resolve to an empty configuration, because "no pools" and "desired
// zero" would delete a running fleet over an API blip.
func TestUnreadableDesiredStateSourceIsAnError(t *testing.T) {
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	client := fake.NewClientset()
	if _, err := resolveTrinoPoolConfigByID(context.Background(), poolAPIReader(t, client), "cell-001"); err == nil {
		t.Fatal("a missing ConfigMap resolved successfully")
	}

	missingKey := fake.NewClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "duckgres-trino-pool", Namespace: "trino-pool-example"},
		Data:       map[string]string{"cells.json": poolRegistryJSON("/etc/duckgres/trino/blueprint.json", 3, 3)},
	})
	// A registry that resolves but whose blueprint does not is the FROZEN case,
	// not the error case: the pool is known, its release is not, so it holds
	// its last-good state instead of being reshaped by a half-readable source.
	config, err := resolveTrinoPoolConfigByID(context.Background(), poolAPIReader(t, missingKey), "cell-001")
	if err != nil {
		t.Fatalf("resolve with an unreadable blueprint: %v", err)
	}
	if !config.Frozen || config.Blueprint != nil {
		t.Fatalf("config = %+v, want a frozen pool with no blueprint", config)
	}
}

// The blueprint key is the declared path's last element, which is exactly the
// mapping a ConfigMap volume performs. One declaration therefore addresses the
// file this process booted from and the object it publishes from, with no
// second naming scheme that could drift.
func TestBlueprintKeyIsTheDeclaredFileName(t *testing.T) {
	reader := poolAPIReader(t, poolConfigMap(t, 3, 3))
	data, err := reader.Blueprint(context.Background(), "/etc/duckgres/trino/blueprint.json")
	if err != nil {
		t.Fatalf("read blueprint: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("the blueprint key resolved to nothing")
	}
}
