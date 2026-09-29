// Command poolfixture renders the isolated shared-pool test configuration.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func main() {
	if err := render(os.Stdin, os.Stdout, os.Getenv("NAMESPACE"), os.Getenv("CONTROLPLANE_IMAGE")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func render(input io.Reader, output io.Writer, namespace, publisher string) error {
	if namespace == "" || !strings.Contains(publisher, "@sha256:") {
		return fmt.Errorf("pool fixture needs a namespace and digest-pinned publisher image")
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(input, 4096)
	objects := []json.RawMessage{}
	b := trinopool.Blueprint{
		BlueprintVersion: 1, Generation: 1, ReleaseID: "fixture-v1", ChartVersion: "fixture-v1",
		Namespace: namespace, ServiceAccountName: "trino",
		ConfigFiles: map[string]map[string]string{},
		SharedResources: trinopool.BlueprintSharedResources{
			Secrets:    []string{"trino-auth", "trino-tenant-secrets", "trino-internal-communication", "trino-opa-bundle-token", "duckgres-trino-tls", "duckgres-trino-catalog-store"},
			ConfigMaps: []string{"trino-resource-groups", "duckgres-trino-opa"},
		},
		IdentityBinding: trinopool.BlueprintIdentityBinding{
			InstanceLabel: "posthog.com/trino-instance", ComponentLabel: "app.kubernetes.io/component",
			DiscoveryURIEnv: "TRINO_DISCOVERY_URI", NodeEnvironmentEnv: "TRINO_NODE_ENVIRONMENT",
			InstanceIDEnv: "DUCKGRES_TRINO_INSTANCE_ID", CoordinatorHTTPPortName: "http",
			CoordinatorContainerName: "trino-coordinator", WorkerContainerName: "trino-worker",
		},
	}
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		var meta struct {
			Kind     string
			Metadata metav1.ObjectMeta
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			return err
		}
		role := strings.TrimPrefix(meta.Metadata.Name, "duckgres-trino-")
		if meta.Kind == "Deployment" && (role == "coordinator" || role == "worker") {
			var d appsv1.Deployment
			if err := json.Unmarshal(raw, &d); err != nil {
				return err
			}
			for _, c := range d.Spec.Template.Spec.Containers {
				if c.Name == "trino-"+role {
					b.Image = c.Image
				}
			}
			w := trinopool.BlueprintWorkload{PodTemplate: d.Spec.Template}
			if role == "coordinator" {
				b.Coordinator = w
			} else {
				w.Replicas = 3
				b.Worker = w
			}
			continue
		}
		if meta.Kind == "ConfigMap" && (role == "coordinator" || role == "worker") {
			var cm corev1.ConfigMap
			if err := json.Unmarshal(raw, &cm); err != nil {
				return err
			}
			for key, body := range cm.Data {
				lines := strings.Split(body, "\n")
				for i, line := range lines {
					if strings.HasPrefix(line, "discovery.uri=") {
						lines[i] = "discovery.uri=${ENV:TRINO_DISCOVERY_URI}"
					}
					if strings.HasPrefix(line, "node.environment=") {
						lines[i] = "node.environment=${ENV:TRINO_NODE_ENVIRONMENT}"
					}
				}
				cm.Data[key] = strings.Join(lines, "\n")
			}
			if role == "coordinator" {
				cm.Data["config.properties"] += "http-server.process-forwarded=true\ncatalog.sync.enabled=true\n"
				cm.Data["catalog-store.properties"] += "catalog-store.read-only=true\n"
			}
			b.ConfigFiles[role] = cm.Data
			continue
		}
		if meta.Kind == "Service" && meta.Metadata.Name == "duckgres-trino" {
			continue
		}
		objects = append(objects, raw)
	}
	if err := b.Validate(); err != nil {
		return fmt.Errorf("fixture blueprint: %w", err)
	}
	blueprint, err := json.Marshal(b)
	if err != nil {
		return err
	}
	registry := map[string]any{"cells": []any{map[string]any{
		"id": "pool-test", "namespace": namespace, "routing_group": "pool-test", "mode": "shared-pool",
		"client_url": "https://duckgres-trino-gateway." + namespace + ".svc:8443",
		"pool": map[string]any{"desired_instances": 1, "min_serving": 1, "max_surge": 1, "max_repair": 1,
			"blueprint_file": "/etc/duckgres/trino-pool/blueprint.json", "coordinator_service_port": 8080,
			"node_environment": "isolated_fixture", "tenant_admission": true},
	}}}
	registryJSON, err := json.Marshal(registry)
	if err != nil {
		return err
	}
	cm := corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "trino-pool-config", Namespace: namespace},
		Data:       map[string]string{"cells.json": string(registryJSON), "blueprint.json": string(blueprint), "publisher-image": publisher}}
	raw, err := json.Marshal(cm)
	if err != nil {
		return err
	}
	objects = append(objects, raw)
	if _, err := fmt.Fprintln(output, "---"); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "items": objects})
}
