//go:build kubernetes

package controlplane

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func fakeKubeInterface() kubernetes.Interface { return fake.NewClientset() }

// With no pooled cell in the registry the wiring produces nothing. This is what
// "ships disabled" has to mean at the startup boundary: the code path is
// constructed on every boot, and on a fleet that has not opted in it resolves
// to zero operators and zero side effects.
func TestPoolWiringIsInertWithoutAPooledCell(t *testing.T) {
	t.Setenv(envTrinoCellsFile, "")
	t.Setenv(envTrinoPoolEnabled, "")
	t.Setenv(envTrinoPoolOperatorEnabled, "")

	operators, err := buildTrinoPoolOperators(nil, nil, nil, "cp-test")
	if err != nil {
		t.Fatalf("wiring failed on a fleet without pools: %v", err)
	}
	if len(operators) != 0 {
		t.Fatalf("built %d operators", len(operators))
	}
}

// A configured pool with the operator enabled but no Gateway would create
// compute it can never admit, drain or retire. Refusing to start is the
// smaller failure.
func TestPoolWiringRefusesAnOperatorWithoutAGateway(t *testing.T) {
	blueprint := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprint))
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoPoolOperatorEnabled, "true")
	t.Setenv(envTrinoPoolGatewayURL, "")
	t.Setenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL", "")

	_, err := buildTrinoPoolOperators(nil, fakeKubeInterface(), func() (string, string) { return "observer", "secret" }, "cp-test")
	if err == nil {
		t.Fatal("an enabled operator without a Gateway was accepted")
	}
}

// A pooled cell with the operator disabled still wires: the durable desired
// state is kept in sync and nothing external is touched.
func TestPoolWiringBuildsAReadOnlyOperator(t *testing.T) {
	blueprint := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprint))
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoPoolOperatorEnabled, "false")
	t.Setenv(envTrinoPoolGatewayURL, "")
	t.Setenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL", "")

	operators, err := buildTrinoPoolOperators(nil, fakeKubeInterface(), func() (string, string) { return "observer", "secret" }, "cp-test")
	if err != nil {
		t.Fatalf("wiring failed: %v", err)
	}
	if len(operators) != 1 {
		t.Fatalf("built %d operators", len(operators))
	}
	if operators[0].operatorEnabled {
		t.Fatal("the operator was built enabled")
	}
	if operators[0].owner != "cp-test" {
		t.Fatalf("owner = %q", operators[0].owner)
	}
}
