package main

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinopool"
	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func fixture(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../manifests.trino.tmpl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"NAMESPACE": "fixture", "PR_NUMBER": "123", "TRINO_IMAGE": "example.invalid/trino@sha256:" + strings.Repeat("a", 64),
		"TRINO_TLS_PASSWORD": "fixture", "TRINO_CA_CERT_B64": "dGVzdA==", "TRINO_SERVER_P12_B64": "dGVzdA==",
		"CONFIG_STORE_PASSWORD": "fixture", "TRINO_WORKER_CPU": "1", "TRINO_WORKER_MEMORY": "4Gi",
		"TRINO_WORKER_HEAP": "3G", "TRINO_QUERY_MEMORY": "6GB", "TRINO_WORKER_QUERY_MEMORY": "2GB",
		"TRINO_WORKER_THREADS": "24", "TRINO_WORKER_MIN_DRIVERS": "48",
	}
	result := string(raw)
	for k, v := range values {
		result = strings.ReplaceAll(result, "${"+k+"}", v)
	}
	return result
}

func TestPoolFixtureProducesValidIndependentBlueprint(t *testing.T) {
	var output bytes.Buffer
	publisher := "example.invalid/controlplane@sha256:" + strings.Repeat("b", 64)
	runScript, err := os.ReadFile("../run.sh")
	if err != nil {
		t.Fatal(err)
	}
	promotedImage := regexp.MustCompile(`ghcr\.io/posthog/trino:[a-f0-9]{40}@sha256:[a-f0-9]{64}`).FindString(string(runScript))
	if promotedImage == "" {
		t.Fatal("promoted Trino image missing")
	}
	input := strings.ReplaceAll(fixture(t), "example.invalid/trino@sha256:"+strings.Repeat("a", 64), promotedImage)
	if err := render(strings.NewReader(input), &output, "fixture", publisher); err != nil {
		t.Fatal(err)
	}
	var list struct{ Items []json.RawMessage }
	if err := utilyaml.NewYAMLOrJSONDecoder(&output, 4096).Decode(&list); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, raw := range list.Items {
		var object struct {
			Kind     string
			Metadata struct {
				Name      string
				Namespace string
			}
		}
		if err := json.Unmarshal(raw, &object); err != nil {
			t.Fatal(err)
		}
		if object.Metadata.Namespace != "fixture" {
			t.Fatalf("resource escapes fixture namespace: %s", object.Metadata.Name)
		}
		if object.Kind == "Deployment" {
			t.Fatal("renderer must not create static Trino deployments")
		}
		if object.Metadata.Name != "trino-pool-config" {
			continue
		}
		found = true
		var config corev1.ConfigMap
		if err := json.Unmarshal(raw, &config); err != nil {
			t.Fatal(err)
		}
		b, err := trinopool.ParseBlueprint([]byte(config.Data["blueprint.json"]))
		if err != nil {
			t.Fatal(err)
		}
		if config.Data["publisher-image"] != publisher {
			t.Fatal("publisher fence diverges from deployment image")
		}
		if b.Image != promotedImage {
			t.Fatal("blueprint did not retain the promoted Trino image")
		}
		for role, workload := range map[string]trinopool.BlueprintWorkload{"coordinator": b.Coordinator, "worker": b.Worker} {
			foundTrino := false
			for _, container := range workload.PodTemplate.Spec.Containers {
				if container.Name == "trino-"+role {
					foundTrino = true
					if container.Image != promotedImage {
						t.Errorf("%s does not use the promoted Trino image", role)
					}
				}
			}
			if !foundTrino {
				t.Errorf("%s Trino container missing", role)
			}
		}
		if b.Worker.Replicas != 3 {
			t.Fatal("distributed execution worker count changed")
		}
		if !strings.Contains(b.ConfigFiles["coordinator"]["config.properties"], "catalog.sync.enabled=true") || !strings.Contains(b.ConfigFiles["coordinator"]["catalog-store.properties"], "catalog-store.read-only=true") {
			t.Fatal("pool coordinator must synchronize the publisher-owned catalog")
		}
		if !strings.Contains(config.Data["cells.json"], `"tenant_admission":true`) {
			t.Fatal("fixture bypasses tenant admission")
		}
		if !strings.Contains(b.ConfigFiles["coordinator"]["access-control.properties"], "opa.policy.revision-uri=http://localhost:8181/v1/data/trino/revision") {
			t.Fatal("pool admission cannot verify the OPA projection revision")
		}
	}
	if !found {
		t.Fatal("pool configuration missing")
	}
}

func TestPoolFixtureRejectsMutableImages(t *testing.T) {
	for _, tc := range []struct{ body, publisher string }{
		{fixture(t), "example.invalid/controlplane:latest"},
		{strings.ReplaceAll(fixture(t), "example.invalid/trino@sha256:"+strings.Repeat("a", 64), "example.invalid/trino:latest"), "example.invalid/controlplane@sha256:" + strings.Repeat("b", 64)},
	} {
		var output bytes.Buffer
		if err := render(strings.NewReader(tc.body), &output, "fixture", tc.publisher); err == nil {
			t.Fatal("mutable image accepted")
		}
		if output.Len() != 0 {
			t.Fatal("invalid blueprint emitted partial resources")
		}
	}
}
