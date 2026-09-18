package trinopool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

var testImage = "registry.invalid/trino@sha256:" + strings.Repeat("ab", 32)

func validBlueprint() *Blueprint {
	return &Blueprint{
		BlueprintVersion:   1,
		ReleaseID:          "2026-09-18.1",
		ChartVersion:       "trino-0.1.0",
		Image:              testImage,
		Namespace:          "trino-cell-a",
		ServiceAccountName: "trino",
		Coordinator: BlueprintWorkload{PodTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "trino-coordinator", Image: testImage}},
		}}},
		Worker: BlueprintWorkload{Replicas: 4, PodTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "trino-worker", Image: testImage}},
		}}},
		ConfigFiles: map[string]map[string]string{
			"coordinator": {
				"config.properties": "coordinator=true\ndiscovery.uri=${ENV:TRINO_DISCOVERY_URI}\n",
				"node.properties":   "node.environment=${ENV:TRINO_NODE_ENVIRONMENT}\n",
			},
			"worker": {
				"config.properties": "coordinator=false\ndiscovery.uri=${ENV:TRINO_DISCOVERY_URI}\n",
				"node.properties":   "node.environment=${ENV:TRINO_NODE_ENVIRONMENT}\n",
			},
		},
		SharedResources: BlueprintSharedResources{
			Secrets:    []string{"trino-auth", "trino-tenant-secrets"},
			ConfigMaps: []string{"trino-resource-groups"},
		},
		IdentityBinding: BlueprintIdentityBinding{
			InstanceLabel:            "posthog.com/trino-instance",
			ComponentLabel:           "app.kubernetes.io/component",
			DiscoveryURIEnv:          "TRINO_DISCOVERY_URI",
			NodeEnvironmentEnv:       "TRINO_NODE_ENVIRONMENT",
			InstanceIDEnv:            "DUCKGRES_TRINO_INSTANCE_ID",
			CoordinatorHTTPPortName:  "https",
			CoordinatorContainerName: "trino-coordinator",
			WorkerContainerName:      "trino-worker",
		},
	}
}

func TestBlueprintValidateAcceptsAWellFormedDocument(t *testing.T) {
	if err := validBlueprint().Validate(); err != nil {
		t.Fatalf("valid blueprint rejected: %v", err)
	}
}

func TestBlueprintValidateRejects(t *testing.T) {
	cases := map[string]func(*Blueprint){
		// An unpinned image makes the "immutable instance" claim false: the same
		// spec digest could resolve to different bytes on every pod start.
		"tag-only image": func(b *Blueprint) {
			b.Image = "registry.invalid/trino:latest"
			b.Coordinator.PodTemplate.Spec.Containers[0].Image = b.Image
			b.Worker.PodTemplate.Spec.Containers[0].Image = b.Image
		},
		"coordinator image differs from the pinned release": func(b *Blueprint) {
			b.Coordinator.PodTemplate.Spec.Containers[0].Image = "registry.invalid/other@sha256:" + strings.Repeat("cd", 32)
		},
		"unpinned sidecar image": func(b *Blueprint) {
			b.Coordinator.PodTemplate.Spec.Containers = append(b.Coordinator.PodTemplate.Spec.Containers,
				corev1.Container{Name: "opa", Image: "openpolicyagent/opa:latest"})
		},
		"missing coordinator container": func(b *Blueprint) {
			b.Coordinator.PodTemplate.Spec.Containers[0].Name = "something-else"
		},
		// Duckgres owns these env vars; a template that already sets one would
		// silently win or lose depending on ordering.
		"template already binds the identity env": func(b *Blueprint) {
			b.Coordinator.PodTemplate.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "TRINO_DISCOVERY_URI", Value: "https://elsewhere.invalid"}}
		},
		"template already sets the instance label": func(b *Blueprint) {
			b.Worker.PodTemplate.Labels = map[string]string{"posthog.com/trino-instance": "pinned"}
		},
		"template pins a node": func(b *Blueprint) { b.Worker.PodTemplate.Spec.NodeName = "ip-10-0-0-1" },
		"template uses host networking": func(b *Blueprint) {
			b.Coordinator.PodTemplate.Spec.HostNetwork = true
		},
		"template names the object": func(b *Blueprint) { b.Coordinator.PodTemplate.Name = "trino-coordinator" },
		"template pins a namespace": func(b *Blueprint) { b.Worker.PodTemplate.Namespace = "elsewhere" },
		// Without the ${ENV:...} reference the instance would inherit whatever
		// discovery URI the chart baked in - i.e. another instance's coordinator.
		"coordinator config does not bind the discovery URI": func(b *Blueprint) {
			b.ConfigFiles["coordinator"]["config.properties"] = "coordinator=true\ndiscovery.uri=https://baked-in.invalid\n"
		},
		"worker config does not bind the discovery URI": func(b *Blueprint) {
			b.ConfigFiles["worker"]["config.properties"] = "coordinator=false\n"
		},
		"missing node.properties": func(b *Blueprint) { delete(b.ConfigFiles["coordinator"], "node.properties") },
		"unknown config file role": func(b *Blueprint) {
			b.ConfigFiles["sidecar"] = map[string]string{"x.properties": "a=b"}
		},
		"config file name escapes the mount": func(b *Blueprint) {
			b.ConfigFiles["worker"]["../evil.properties"] = "a=b"
		},
		"config payload exceeds the ConfigMap budget": func(b *Blueprint) {
			b.ConfigFiles["worker"]["big.properties"] = strings.Repeat("x", 1<<20)
		},
		"zero workers":                  func(b *Blueprint) { b.Worker.Replicas = 0 },
		"invalid namespace":             func(b *Blueprint) { b.Namespace = "Trino_Cell" },
		"unsupported blueprint version": func(b *Blueprint) { b.BlueprintVersion = 2 },
		"missing release id":            func(b *Blueprint) { b.ReleaseID = "" },
		"identity env is not an environment variable name": func(b *Blueprint) {
			b.IdentityBinding.DiscoveryURIEnv = "trino discovery"
		},
		"identity envs collide": func(b *Blueprint) {
			b.IdentityBinding.InstanceIDEnv = b.IdentityBinding.DiscoveryURIEnv
		},
		"shared resource is not a valid object name": func(b *Blueprint) {
			b.SharedResources.Secrets = []string{"Not A Secret"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			blueprint := validBlueprint()
			mutate(blueprint)
			if err := blueprint.Validate(); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestParseBlueprintRejectsUnknownFields(t *testing.T) {
	data, err := json.Marshal(validBlueprint())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	document["future_field"] = true
	extended, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := ParseBlueprint(extended); err == nil {
		t.Fatal("expected an unknown blueprint field to be rejected")
	}
}

func TestParseBlueprintRejectsTrailingDocuments(t *testing.T) {
	data, err := json.Marshal(validBlueprint())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := ParseBlueprint(append(data, []byte("{}")...)); err == nil {
		t.Fatal("expected a second JSON document to be rejected")
	}
}

func TestParseBlueprintRoundTrip(t *testing.T) {
	data, err := json.Marshal(validBlueprint())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := ParseBlueprint(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Digest() != validBlueprint().Digest() {
		t.Fatal("round-tripping the blueprint changed its digest")
	}
}

// The digest is what pins an instance to its exact execution configuration, so
// it has to be stable across process restarts and sensitive to every field.
func TestBlueprintDigestIsStableAndSensitive(t *testing.T) {
	base := validBlueprint().Digest()
	if base != validBlueprint().Digest() {
		t.Fatal("digest is not deterministic")
	}
	if len(base) != 64 {
		t.Fatalf("digest %q is not a sha256 hex string", base)
	}
	changed := validBlueprint()
	changed.Worker.Replicas = 5
	if changed.Digest() == base {
		t.Fatal("worker replica count did not change the digest")
	}
}

// The file a real deployment mounts has to parse and validate; a fixture that
// only exists in Go proves nothing about the document Argo delivers.
func TestParseBlueprintAcceptsTheCheckedInFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "blueprint.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	blueprint, err := ParseBlueprint(data)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if blueprint.ReleaseID == "" || blueprint.Worker.Replicas < 1 {
		t.Fatal("fixture did not populate the release identity")
	}
}
