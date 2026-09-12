package e2emwdev_test

import (
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestControlPlaneRetirementPreservesRoutingWithdrawalWindow(t *testing.T) {
	raw, err := os.ReadFile("manifests.tmpl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	rendered := os.Expand(string(raw), func(string) string { return "fixture-value" })
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	for {
		var manifest map[string]any
		if err := decoder.Decode(&manifest); err == io.EOF {
			t.Fatal("control-plane Deployment not found")
		} else if err != nil {
			t.Fatal(err)
		}
		if manifest["kind"] != "Deployment" || manifestName(manifest) != "duckgres-control-plane" {
			continue
		}
		deployment := manifest["spec"].(map[string]any)
		pod := deployment["template"].(map[string]any)["spec"].(map[string]any)
		container := pod["containers"].([]any)[0].(map[string]any)
		lifecycle, ok := container["lifecycle"].(map[string]any)
		if !ok {
			t.Fatal("idle control plane can exit before Service routing withdraws: missing preStop lifecycle")
		}
		preStop, _ := lifecycle["preStop"].(map[string]any)
		exec, _ := preStop["exec"].(map[string]any)
		if got := stringSlice(exec["command"]); !reflect.DeepEqual(got, []string{"sleep", "5"}) {
			t.Fatalf("preStop must leave the listener serving for the documented 5-second withdrawal window, got %v", got)
		}
		// The hook runs inside the pod's grace budget. Preserve the previous
		// default 30 seconds for PG drain after its five-second hook.
		if grace, _ := pod["terminationGracePeriodSeconds"].(float64); grace < 35 {
			t.Fatalf("termination budget must include withdrawal plus the existing 30-second drain budget, got %v", grace)
		}
		strategy, _ := deployment["strategy"].(map[string]any)
		rolling, _ := strategy["rollingUpdate"].(map[string]any)
		if strategy["type"] != "RollingUpdate" || rolling["maxUnavailable"] != float64(0) || rolling["maxSurge"] != float64(1) {
			t.Fatalf("single-replica control plane must retain overlap during retirement, got %v", strategy)
		}
		return
	}
}
