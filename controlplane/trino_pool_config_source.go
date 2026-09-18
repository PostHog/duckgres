//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Where the pool's desired configuration is READ from.
//
// A mounted file is not an authoritative answer to "what does the cluster want
// right now". The kubelet refreshes a projected ConfigMap volume on its own
// schedule, independently per pod, and a `subPath` mount is never refreshed at
// all - so two replicas can hold different contents indefinitely, and an idle
// pod can hold yesterday's. Re-reading such a file more often does not make it
// current; it only makes it re-read.
//
// The operator therefore publishes desired state from the API object itself.
// Files remain how the process BOOTS (they are what tells it a pool exists at
// all), and the API read is what every desired-state publication is derived
// from afterwards. The two inputs are the same documents: the registry the cell
// is declared in, and the blueprint it names.
const (
	envTrinoPoolConfigMap    = "DUCKGRES_TRINO_POOL_CONFIG_CONFIGMAP"
	envTrinoPoolConfigNS     = "DUCKGRES_TRINO_POOL_CONFIG_NAMESPACE"
	envTrinoPoolRegistryKey  = "DUCKGRES_TRINO_POOL_CONFIG_REGISTRY_KEY"
	defaultTrinoPoolRegistry = "cells.json"

	// One API read per pool per tick, from the leader only. The budget is short
	// because a slow answer must not hold the reconcile loop: the pool keeps
	// its last-good desired state and the next tick tries again.
	trinoPoolConfigReadBudget = 10 * time.Second
)

// trinoPoolConfigReader supplies the raw desired-configuration documents.
type trinoPoolConfigReader interface {
	// Registry returns the Trino cell registry document.
	Registry(ctx context.Context) ([]byte, error)
	// Blueprint returns the blueprint the registry entry names. The argument is
	// the declared blueprint path; a ConfigMap-backed reader uses its last
	// element as the data key, which is exactly the mapping a ConfigMap volume
	// mount performs, so one declaration addresses both sources.
	Blueprint(ctx context.Context, declaredPath string) ([]byte, error)
	// Describe names the source in operator-facing errors.
	Describe() string
}

// trinoPoolFileConfigReader reads the mounted documents. It is the BOOT source:
// it is what tells the process a pool exists. It is deliberately not used for
// desired-state publication.
type trinoPoolFileConfigReader struct{}

func (trinoPoolFileConfigReader) Registry(context.Context) ([]byte, error) {
	location := strings.TrimSpace(os.Getenv(envTrinoCellsFile))
	if location == "" {
		return nil, nil
	}
	data, err := os.ReadFile(location)
	if err != nil {
		return nil, fmt.Errorf("read Trino registry: %w", err)
	}
	return data, nil
}

func (trinoPoolFileConfigReader) Blueprint(_ context.Context, declaredPath string) ([]byte, error) {
	info, err := os.Stat(declaredPath)
	if err != nil {
		return nil, fmt.Errorf("blueprint is unreadable: %w", err)
	}
	if info.Size() > maxBlueprintFileBytes {
		return nil, fmt.Errorf("blueprint exceeds the size limit")
	}
	data, err := os.ReadFile(declaredPath)
	if err != nil {
		return nil, fmt.Errorf("blueprint is unreadable: %w", err)
	}
	return data, nil
}

func (trinoPoolFileConfigReader) Describe() string { return "the mounted configuration files" }

// trinoPoolAPIConfigReader reads the SAME documents from the ConfigMap the
// chart projects them from, through the Kubernetes API.
//
// This is the authoritative source: the API object is one value shared by every
// replica, so two control planes cannot disagree about what is configured, and
// a pod that has been idle since boot reads what the cluster says now rather
// than what its kubelet last projected.
type trinoPoolAPIConfigReader struct {
	client      kubernetes.Interface
	namespace   string
	name        string
	registryKey string
}

func (r trinoPoolAPIConfigReader) Describe() string {
	return fmt.Sprintf("ConfigMap %s/%s", r.namespace, r.name)
}

func (r trinoPoolAPIConfigReader) Registry(ctx context.Context) ([]byte, error) {
	return r.key(ctx, r.registryKey)
}

func (r trinoPoolAPIConfigReader) Blueprint(ctx context.Context, declaredPath string) ([]byte, error) {
	// A ConfigMap volume mounts each key as a file of that name, so the key is
	// the declared path's last element. One registry declaration therefore
	// addresses the file the process booted from AND the API object it is
	// published from, with no second naming scheme to keep in sync.
	return r.key(ctx, path.Base(strings.TrimSpace(declaredPath)))
}

func (r trinoPoolAPIConfigReader) key(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("no key named in %s", r.Describe())
	}
	ctx, cancel := context.WithTimeout(ctx, trinoPoolConfigReadBudget)
	defer cancel()

	configMap, err := r.client.CoreV1().ConfigMaps(r.namespace).Get(ctx, r.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", r.Describe(), err)
	}
	if value, present := configMap.Data[key]; present {
		if len(value) > maxBlueprintFileBytes {
			return nil, fmt.Errorf("%s key %q exceeds the size limit", r.Describe(), key)
		}
		return []byte(value), nil
	}
	if value, present := configMap.BinaryData[key]; present {
		if len(value) > maxBlueprintFileBytes {
			return nil, fmt.Errorf("%s key %q exceeds the size limit", r.Describe(), key)
		}
		return value, nil
	}
	// An absent key is an error, never an empty document: an empty registry
	// would read as "this pool no longer exists" and an empty blueprint as "no
	// release", and neither is something a missing key may assert.
	return nil, fmt.Errorf("%s has no key %q", r.Describe(), key)
}

// newTrinoPoolAPIConfigReader builds the authoritative reader for one pool.
//
// It is REQUIRED for a shared-pool cell. Falling back to the mounted files
// would mean publishing desired state from a per-pod snapshot that can lag
// indefinitely, which is the staleness this exists to remove - and a silent
// fallback is worse than a refusal, because nothing would say which source a
// running pool is being driven from.
func newTrinoPoolAPIConfigReader(client kubernetes.Interface, defaultNamespace string) (trinoPoolConfigReader, error) {
	name := strings.TrimSpace(os.Getenv(envTrinoPoolConfigMap))
	if name == "" {
		return nil, fmt.Errorf("a shared Trino pool requires %s: desired state is published from the ConfigMap, not from a mounted copy of it", envTrinoPoolConfigMap)
	}
	if client == nil {
		return nil, fmt.Errorf("a shared Trino pool requires a Kubernetes client to read %s", name)
	}
	namespace := strings.TrimSpace(os.Getenv(envTrinoPoolConfigNS))
	if namespace == "" {
		namespace = defaultNamespace
	}
	if namespace == "" {
		return nil, fmt.Errorf("a shared Trino pool requires a namespace for %s", name)
	}
	registryKey := strings.TrimSpace(os.Getenv(envTrinoPoolRegistryKey))
	if registryKey == "" {
		registryKey = defaultTrinoPoolRegistry
	}
	return trinoPoolAPIConfigReader{client: client, namespace: namespace, name: name, registryKey: registryKey}, nil
}
