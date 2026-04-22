package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

type deploymentManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Name           string `yaml:"name"`
					ReadinessProbe struct {
						HTTPGet struct {
							Path string `yaml:"path"`
							Port any    `yaml:"port"`
						} `yaml:"httpGet"`
					} `yaml:"readinessProbe"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestLiveControlPlaneManifestsReadinessProbeTargetsAPIHealthEndpoint(t *testing.T) {
	paths := []string{
		filepath.Join("k8s", "control-plane-multitenant-local.yaml"),
		filepath.Join("k8s", "kind", "control-plane.yaml"),
	}

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", path, err)
		}

		var doc deploymentManifest
		if err := yaml.Unmarshal(data, &doc); err != nil {
			t.Fatalf("Unmarshal(%s): %v", path, err)
		}
		if doc.Kind != "Deployment" {
			t.Fatalf("%s: expected first manifest document to be a Deployment, got %q", path, doc.Kind)
		}
		if len(doc.Spec.Template.Spec.Containers) == 0 {
			t.Fatalf("%s: expected at least one container in deployment manifest", path)
		}
		probe := doc.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet
		if probe.Path != "/health" {
			t.Fatalf("%s: expected readiness probe path /health, got %q", path, probe.Path)
		}
		if port, ok := probe.Port.(string); !ok || port != "api" {
			t.Fatalf("%s: expected readiness probe port api, got %#v", path, probe.Port)
		}
	}
}

func TestNetworkPolicyManifestRestrictsWorkerIngressAndEgressAndProtectsControlPlane(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("k8s", "networkpolicy.yaml"))
	if err != nil {
		t.Fatalf("ReadFile(networkpolicy.yaml): %v", err)
	}

	var docs []map[string]any
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Decode(networkpolicy.yaml): %v", err)
		}
		if len(doc) != 0 {
			docs = append(docs, doc)
		}
	}

	if len(docs) < 2 {
		t.Fatalf("expected worker and control-plane NetworkPolicy documents, got %d", len(docs))
	}

	var sawWorkerPort80 bool
	for _, doc := range docs {
		metadata, _ := doc["metadata"].(map[string]any)
		if metadata == nil || metadata["name"] != "duckgres-worker-ingress" {
			continue
		}

		spec, _ := doc["spec"].(map[string]any)
		if spec == nil {
			t.Fatal("worker network policy missing spec")
			return
		}
		egress, _ := spec["egress"].([]any)
		for _, rule := range egress {
			ruleMap, _ := rule.(map[string]any)
			ports, _ := ruleMap["ports"].([]any)
			for _, entry := range ports {
				portMap, _ := entry.(map[string]any)
				if port, ok := portMap["port"].(int); ok && port == 80 {
					sawWorkerPort80 = true
				}
			}
		}
	}
	if !sawWorkerPort80 {
		t.Fatal("expected worker network policy to allow outbound TCP port 80 for DuckDB extension bootstrap")
	}
}
