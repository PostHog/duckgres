//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var allowedSharedStartupConfigKeys = map[string]struct{}{
	"data_dir":             {},
	"extensions":           {},
	"idle_timeout":         {},
	"log_level":            {},
	"memory_budget":        {},
	"memory_limit":         {},
	"memory_rebalance":     {},
	"query_log":            {},
	"threads":              {},
	"worker_idle_timeout":  {},
	"worker_queue_timeout": {},
}

func validateSharedStartupConfig(data []byte) error {
	if strings.TrimSpace(string(data)) == "" {
		return nil
	}

	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("parse shared startup config: %w", err)
	}
	if len(root.Content) == 0 {
		return nil
	}
	mapping := root.Content[0]
	if mapping.Kind != yaml.MappingNode {
		return fmt.Errorf("shared startup config must be a YAML mapping")
	}

	var invalid []string
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		key := mapping.Content[i].Value
		if _, ok := allowedSharedStartupConfigKeys[key]; !ok {
			invalid = append(invalid, key)
		}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return fmt.Errorf("shared startup config contains tenant-specific or unsupported keys: %s", strings.Join(invalid, ", "))
	}
	return nil
}

func validateSharedWorkerConfig(data []byte) error {
	return validateSharedStartupConfig(data)
}

func sharedStartupConfigKey(configPath string) string {
	key := filepath.Base(configPath)
	if key == "." || key == string(filepath.Separator) || key == "" {
		return "duckgres.yaml"
	}
	return key
}

func (p *K8sWorkerPool) validateSharedStartupConfigMap(ctx context.Context) error {
	if p.configMap == "" {
		return nil
	}
	configMap, err := p.clientset.CoreV1().ConfigMaps(p.namespace).Get(ctx, p.configMap, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get shared worker configmap %s: %w", p.configMap, err)
	}
	key := sharedStartupConfigKey(p.configPath)
	data, ok := configMap.Data[key]
	if !ok {
		return fmt.Errorf("configmap %s missing %q", p.configMap, key)
	}
	if err := validateSharedStartupConfig([]byte(data)); err != nil {
		return err
	}
	return nil
}
