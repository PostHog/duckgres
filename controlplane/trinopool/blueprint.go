package trinopool

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// A blueprint is the immutable, versioned rendering of the Golden Chart that
// Argo delivers and duckgres instantiates. It is deliberately NOT a template
// language: duckgres injects instance identity through env vars, labels,
// selectors and object names only, and never edits a rendered body. Everything
// else — JVM flags, probes, resources, volumes, sidecars — is the chart's, so
// an instantiated blueprint can be diffed against an ordinary chart render.
const (
	// SupportedBlueprintVersion is the only document version this build
	// accepts. A newer document is refused rather than partially understood.
	SupportedBlueprintVersion = 1

	maxBlueprintBytes     = 1 << 20
	maxConfigFileBytes    = 256 << 10
	maxConfigFilesTotal   = 768 << 10 // a ConfigMap caps at ~1MiB including keys
	maxSharedResources    = 32
	maxWorkerReplicas     = 100
	maxIdentifierLength   = 253
	configFileRoleCoord   = "coordinator"
	configFileRoleWorker  = "worker"
	requiredConfigFile    = "config.properties"
	requiredNodeFile      = "node.properties"
	blueprintDigestLength = 64
)

var (
	digestPinnedImage  = regexp.MustCompile(`^[^\s@:]+(:[0-9]+)?/?[^\s@]*@sha256:[a-f0-9]{64}$`)
	environmentVarName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	configFileName     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)
	releaseIdentifier  = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:+-]{0,127}$`)
)

// Blueprint is the document Argo mounts for a shared-pool cell.
type Blueprint struct {
	BlueprintVersion int `json:"blueprint_version"`
	// Generation orders desired state across control planes. It must increase
	// with every published release.
	//
	// It has to come from the config source, because nothing in the control
	// plane can order releases on its own: any replica may win the pool
	// authority, and each one knows only the files it has mounted. Holding the
	// fence proves who may write, not that what they hold is current - so
	// without an externally supplied ordinal, a replica carrying yesterday's
	// blueprint could legitimately publish it over today's.
	//
	// Absent or zero is accepted and simply does not order anything, which is
	// the behavior before any generator emits it.
	Generation         int64                        `json:"generation,omitempty"`
	ReleaseID          string                       `json:"release_id"`
	ChartVersion       string                       `json:"chart_version"`
	Image              string                       `json:"image"`
	Namespace          string                       `json:"namespace"`
	ServiceAccountName string                       `json:"service_account_name"`
	Coordinator        BlueprintWorkload            `json:"coordinator"`
	Worker             BlueprintWorkload            `json:"worker"`
	ConfigFiles        map[string]map[string]string `json:"config_files"`
	SharedResources    BlueprintSharedResources     `json:"shared_resources"`
	IdentityBinding    BlueprintIdentityBinding     `json:"identity_binding"`
}

// BlueprintWorkload carries one validated PodTemplateSpec. Replicas is only
// meaningful for workers; a serving instance always has exactly one
// coordinator, which is why the coordinator has no replica field at all.
type BlueprintWorkload struct {
	Replicas    int32                  `json:"replicas,omitempty"`
	PodTemplate corev1.PodTemplateSpec `json:"pod_template"`
}

// BlueprintSharedResources names objects the pool owns as a whole. Duckgres
// mounts and reads them, and every instance-scoped delete path excludes them:
// retiring an instance must never take the pool's auth Secret with it.
type BlueprintSharedResources struct {
	Secrets    []string `json:"secrets,omitempty"`
	ConfigMaps []string `json:"config_maps,omitempty"`
}

// BlueprintIdentityBinding is the whole contract for instance-specific values.
// Each field names where duckgres may write, so the set of mutations it can
// perform on a chart render is enumerable and reviewable.
type BlueprintIdentityBinding struct {
	InstanceLabel            string `json:"instance_label"`
	ComponentLabel           string `json:"component_label"`
	DiscoveryURIEnv          string `json:"discovery_uri_env"`
	NodeEnvironmentEnv       string `json:"node_environment_env"`
	InstanceIDEnv            string `json:"instance_id_env"`
	CoordinatorHTTPPortName  string `json:"coordinator_http_port_name"`
	CoordinatorContainerName string `json:"coordinator_container_name"`
	WorkerContainerName      string `json:"worker_container_name"`
}

// ParseBlueprint decodes and validates one blueprint document. Unknown fields
// and trailing documents are refused so a newer Argo render cannot be silently
// half-applied by an older binary.
func ParseBlueprint(data []byte) (*Blueprint, error) {
	if len(data) == 0 || len(data) > maxBlueprintBytes {
		return nil, errors.New("blueprint must be a non-empty document within the size limit")
	}
	var blueprint Blueprint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&blueprint); err != nil {
		return nil, fmt.Errorf("decode blueprint: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("blueprint must contain exactly one JSON document")
	}
	if err := blueprint.Validate(); err != nil {
		return nil, err
	}
	return &blueprint, nil
}

// Digest is the stable identity of the execution configuration. It is stored on
// every instance row and annotated onto every object the instance owns, so a
// stale leader's create is recognizable as a foreign or outdated object rather
// than adopted as a serving replacement.
func (b *Blueprint) Digest() string {
	encoded, err := json.Marshal(b)
	if err != nil {
		// Only unencodable types could fail here, and every field is JSON
		// data. Fall back to a digest that can never collide with a real one.
		return strings.Repeat("0", blueprintDigestLength)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// Validate refuses anything that would make an instance non-reproducible,
// non-isolated, or dependent on values duckgres cannot control.
func (b *Blueprint) Validate() error {
	if b.BlueprintVersion != SupportedBlueprintVersion {
		return fmt.Errorf("unsupported blueprint version %d", b.BlueprintVersion)
	}
	if !releaseIdentifier.MatchString(b.ReleaseID) {
		return errors.New("blueprint requires a release identifier")
	}
	if !releaseIdentifier.MatchString(b.ChartVersion) {
		return errors.New("blueprint requires a chart version")
	}
	if b.Generation < 0 {
		return errors.New("blueprint generation must not be negative")
	}
	// An unpinned image breaks the central promise of the model: the same spec
	// digest must always mean the same bytes, on every pod start, forever.
	if !digestPinnedImage.MatchString(b.Image) || len(b.Image) > 512 {
		return errors.New("blueprint image must be pinned to a sha256 digest")
	}
	if len(validation.IsDNS1123Label(b.Namespace)) != 0 {
		return errors.New("blueprint namespace must be a DNS label")
	}
	if len(validation.IsDNS1123Subdomain(b.ServiceAccountName)) != 0 {
		return errors.New("blueprint service account must be a DNS subdomain")
	}
	if err := b.IdentityBinding.validate(); err != nil {
		return err
	}
	if b.Worker.Replicas < 1 || b.Worker.Replicas > maxWorkerReplicas {
		return fmt.Errorf("blueprint worker replicas must be between 1 and %d", maxWorkerReplicas)
	}
	if err := b.validateWorkload(b.Coordinator, b.IdentityBinding.CoordinatorContainerName); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	if err := b.validateWorkload(b.Worker, b.IdentityBinding.WorkerContainerName); err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	if err := b.validateConfigFiles(); err != nil {
		return err
	}
	return b.SharedResources.validate()
}

func (i BlueprintIdentityBinding) validate() error {
	labels := []string{i.InstanceLabel, i.ComponentLabel}
	for _, label := range labels {
		if len(validation.IsQualifiedName(label)) != 0 {
			return fmt.Errorf("identity binding label %q is not a qualified name", label)
		}
	}
	if labels[0] == labels[1] {
		return errors.New("identity binding labels must differ")
	}
	names := map[string]bool{}
	for _, name := range i.EnvNames() {
		if !environmentVarName.MatchString(name) || len(name) > 128 {
			return fmt.Errorf("identity binding %q is not an environment variable name", name)
		}
		if names[name] {
			return fmt.Errorf("identity binding environment variable %q is bound twice", name)
		}
		names[name] = true
	}
	if len(validation.IsValidPortName(i.CoordinatorHTTPPortName)) != 0 {
		return errors.New("identity binding coordinator port name is invalid")
	}
	containers := []string{i.CoordinatorContainerName, i.WorkerContainerName}
	for _, name := range containers {
		if len(validation.IsDNS1123Label(name)) != 0 {
			return fmt.Errorf("identity binding container name %q is invalid", name)
		}
	}
	return nil
}

// EnvNames lists every environment variable duckgres injects. A pod template
// must leave all of them unset.
func (i BlueprintIdentityBinding) EnvNames() []string {
	return []string{i.DiscoveryURIEnv, i.NodeEnvironmentEnv, i.InstanceIDEnv}
}

func (b *Blueprint) validateWorkload(workload BlueprintWorkload, mainContainer string) error {
	template := workload.PodTemplate
	// The object's identity is duckgres's to assign; a template that names
	// itself would make two instances collide on one name.
	if template.Name != "" || template.GenerateName != "" || template.Namespace != "" {
		return errors.New("pod template must not name or namespace itself")
	}
	if len(template.OwnerReferences) != 0 {
		return errors.New("pod template must not declare owner references")
	}
	if _, pinned := template.Labels[b.IdentityBinding.InstanceLabel]; pinned {
		return errors.New("pod template must not set the instance label")
	}
	if _, pinned := template.Labels[b.IdentityBinding.ComponentLabel]; pinned {
		return errors.New("pod template must not set the component label")
	}
	spec := template.Spec
	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		return errors.New("pod template must not share host namespaces")
	}
	if spec.NodeName != "" {
		return errors.New("pod template must not pin a node")
	}
	if spec.ServiceAccountName != "" && spec.ServiceAccountName != b.ServiceAccountName {
		return errors.New("pod template service account must match the blueprint")
	}
	if len(spec.Containers) == 0 || len(spec.Containers) > 8 {
		return errors.New("pod template must declare between one and eight containers")
	}

	injected := map[string]bool{}
	for _, name := range b.IdentityBinding.EnvNames() {
		injected[name] = true
	}
	main := 0
	for _, container := range append(append([]corev1.Container{}, spec.Containers...), spec.InitContainers...) {
		if container.Name == mainContainer {
			main++
			if container.Image != b.Image {
				return errors.New("main container image must be the blueprint release image")
			}
		}
		// Every image, sidecars included, has to be pinned: an unpinned OPA or
		// exporter tag silently changes what a "immutable" instance runs.
		if !digestPinnedImage.MatchString(container.Image) {
			return fmt.Errorf("container %q image is not pinned to a digest", container.Name)
		}
		for _, env := range container.Env {
			if injected[env.Name] {
				return fmt.Errorf("container %q already binds the injected variable %q", container.Name, env.Name)
			}
		}
	}
	if main != 1 {
		return fmt.Errorf("pod template must declare exactly one %q container", mainContainer)
	}
	return nil
}

func (b *Blueprint) validateConfigFiles() error {
	roles := map[string]bool{configFileRoleCoord: true, configFileRoleWorker: true}
	total := 0
	for role, files := range b.ConfigFiles {
		if !roles[role] {
			return fmt.Errorf("unknown config file role %q", role)
		}
		delete(roles, role)
		for name, body := range files {
			if !configFileName.MatchString(name) {
				return fmt.Errorf("config file name %q is not a plain file name", name)
			}
			if len(body) > maxConfigFileBytes {
				return fmt.Errorf("config file %q exceeds the per-file limit", name)
			}
			if strings.IndexFunc(body, func(r rune) bool { return r != '\n' && r != '\t' && r != '\r' && unicode.IsControl(r) }) != -1 {
				return fmt.Errorf("config file %q contains control characters", name)
			}
			total += len(name) + len(body)
		}
		for _, required := range []string{requiredConfigFile, requiredNodeFile} {
			if _, present := files[required]; !present {
				return fmt.Errorf("config files for %q must include %s", role, required)
			}
		}
		// Without these references the instance would inherit whatever
		// discovery URI and node environment the chart baked in - i.e. it
		// would join another instance's cluster.
		if !strings.Contains(files[requiredConfigFile], b.IdentityBinding.envReference(b.IdentityBinding.DiscoveryURIEnv)) {
			return fmt.Errorf("%s for %q must bind the discovery URI to %s", requiredConfigFile, role, b.IdentityBinding.DiscoveryURIEnv)
		}
		if !strings.Contains(files[requiredNodeFile], b.IdentityBinding.envReference(b.IdentityBinding.NodeEnvironmentEnv)) {
			return fmt.Errorf("%s for %q must bind the node environment to %s", requiredNodeFile, role, b.IdentityBinding.NodeEnvironmentEnv)
		}
	}
	if len(roles) != 0 {
		missing := make([]string, 0, len(roles))
		for role := range roles {
			missing = append(missing, role)
		}
		sort.Strings(missing)
		return fmt.Errorf("blueprint is missing config files for %s", strings.Join(missing, ", "))
	}
	if total > maxConfigFilesTotal {
		return errors.New("config files exceed the ConfigMap budget")
	}
	return nil
}

func (i BlueprintIdentityBinding) envReference(name string) string {
	return "${ENV:" + name + "}"
}

func (s BlueprintSharedResources) validate() error {
	if len(s.Secrets)+len(s.ConfigMaps) > maxSharedResources {
		return errors.New("blueprint declares too many shared resources")
	}
	for _, name := range append(append([]string{}, s.Secrets...), s.ConfigMaps...) {
		if len(validation.IsDNS1123Subdomain(name)) != 0 || len(name) > maxIdentifierLength {
			return fmt.Errorf("shared resource %q is not a valid object name", name)
		}
	}
	return nil
}

// Protects reports whether the named object belongs to the pool rather than to
// a single instance. Every instance-scoped delete consults this.
func (s BlueprintSharedResources) Protects(name string) bool {
	for _, shared := range append(append([]string{}, s.Secrets...), s.ConfigMaps...) {
		if shared == name {
			return true
		}
	}
	return false
}

// MarshalSnapshot returns the blueprint as a JSON document for durable storage
// alongside an instance. The instance keeps its own copy because Argo may
// replace or prune the source ConfigMap for a new release, and a PREPARING,
// SERVING or DRAINING instance must keep running the configuration it was
// created with.
func (b *Blueprint) MarshalSnapshot() (string, error) {
	encoded, err := json.Marshal(b)
	if err != nil {
		return "", fmt.Errorf("encode blueprint snapshot: %w", err)
	}
	return string(encoded), nil
}
