package main

import (
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
