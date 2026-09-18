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

// trinoPoolConfigReader supplies one CONSISTENT set of desired-configuration
// documents.
//
// Snapshot is deliberately the only entry point. Reading the registry and the
// blueprint as two separate API calls can straddle an update and produce a
// configuration that never existed - a new registry entry paired with the
// previous release, say - and that mixture would be published as desired state.
// One object read once cannot do that.
type trinoPoolConfigReader interface {
	// Snapshot reads every desired-configuration document at one instant.
	Snapshot(ctx context.Context) (trinoPoolConfigSnapshot, error)
	// Describe names the source in operator-facing errors.
	Describe() string
}

// trinoPoolConfigSnapshot is one point-in-time set of documents.
type trinoPoolConfigSnapshot interface {
	// Registry returns the Trino cell registry document, or nil when this
	// deployment declares no registry at all.
	Registry() []byte
	// Blueprint returns the blueprint the registry entry names. The argument is
	// the declared blueprint path; a ConfigMap-backed snapshot uses its last
	// element as the data key, which is exactly the mapping a ConfigMap volume
	// mount performs, so one declaration addresses both sources.
	Blueprint(declaredPath string) ([]byte, error)
}

// trinoPoolFileConfigReader reads the mounted documents. It is the BOOT source:
// it is what tells the process a pool exists. It is deliberately not used for
// desired-state publication.
type trinoPoolFileConfigReader struct{}

// Snapshot reads the registry now and each blueprint when it is asked for.
// Files are the BOOT source only, where there is no desired-state publication
// to make inconsistent: the process is deciding whether a pool exists at all.
func (r trinoPoolFileConfigReader) Snapshot(context.Context) (trinoPoolConfigSnapshot, error) {
	location := strings.TrimSpace(os.Getenv(envTrinoCellsFile))
	if location == "" {
		return trinoPoolFileSnapshot{}, nil
	}
	data, err := os.ReadFile(location)
	if err != nil {
		return nil, fmt.Errorf("read Trino registry: %w", err)
	}
	return trinoPoolFileSnapshot{registry: data}, nil
}

type trinoPoolFileSnapshot struct{ registry []byte }

func (s trinoPoolFileSnapshot) Registry() []byte { return s.registry }

func (trinoPoolFileSnapshot) Blueprint(declaredPath string) ([]byte, error) {
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

// Snapshot reads the whole object ONCE.
//
// The registry and the blueprint are keys of the same ConfigMap and are taken
// from one read, so a resolution cannot pair a new registry entry with the
// previous release: a two-call reader can straddle an update and publish a
// configuration that never existed. No ordering is assumed between reads
// either - there is only one.
func (r trinoPoolAPIConfigReader) Snapshot(ctx context.Context) (trinoPoolConfigSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolConfigReadBudget)
	defer cancel()

	configMap, err := r.client.CoreV1().ConfigMaps(r.namespace).Get(ctx, r.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", r.Describe(), err)
	}
	snapshot := trinoPoolAPISnapshot{source: r.Describe(), data: map[string][]byte{}}
	for key, value := range configMap.Data {
		snapshot.data[key] = []byte(value)
	}
	for key, value := range configMap.BinaryData {
		snapshot.data[key] = value
	}
	registry, err := snapshot.key(r.registryKey)
	if err != nil {
		return nil, err
	}
	snapshot.registry = registry
	return snapshot, nil
}

// trinoPoolAPISnapshot is one ConfigMap read, held as the complete set of
// documents that read contained.
type trinoPoolAPISnapshot struct {
	source   string
	data     map[string][]byte
	registry []byte
}

func (s trinoPoolAPISnapshot) Registry() []byte { return s.registry }

func (s trinoPoolAPISnapshot) Blueprint(declaredPath string) ([]byte, error) {
	// A ConfigMap volume mounts each key as a file of that name, so the key is
	// the declared path's last element. One registry declaration therefore
	// addresses the file the process booted from AND the API object it is
	// published from, with no second naming scheme to keep in sync.
	return s.key(path.Base(strings.TrimSpace(declaredPath)))
}

func (s trinoPoolAPISnapshot) key(key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("no key named in %s", s.source)
	}
	value, present := s.data[key]
	if !present {
		// An absent key is an error, never an empty document: an empty registry
		// would read as "this pool no longer exists" and an empty blueprint as
		// "no release", and neither is something a missing key may assert.
		return nil, fmt.Errorf("%s has no key %q", s.source, key)
	}
	if len(value) > maxBlueprintFileBytes {
		return nil, fmt.Errorf("%s key %q exceeds the size limit", s.source, key)
	}
	return value, nil
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
