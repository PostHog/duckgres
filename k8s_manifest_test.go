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

func TestControlPlaneDeploymentReadinessProbeTargetsAPIHealthEndpoint(t *testing.T) {
	path := filepath.Join("k8s", "control-plane-deployment.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}

	var doc deploymentManifest
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("Unmarshal(%s): %v", path, err)
	}
	if doc.Kind != "Deployment" {
		t.Fatalf("expected first manifest document to be a Deployment, got %q", doc.Kind)
	}
	if len(doc.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("expected at least one container in deployment manifest")
	}
	probe := doc.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet
	if probe.Path != "/health" {
		t.Fatalf("expected readiness probe path /health, got %q", probe.Path)
	}
	if port, ok := probe.Port.(string); !ok || port != "api" {
		t.Fatalf("expected readiness probe port api, got %#v", probe.Port)
	}
}
