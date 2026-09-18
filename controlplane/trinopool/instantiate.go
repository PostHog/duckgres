package trinopool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Labels and annotations duckgres stamps on everything it owns. The epoch and
// spec digest are what make a stale leader's write recognizable: a conditional
// update compares them before touching an existing object, so a create that
// arrives late is at worst an unadmitted orphan, never a serving replacement.
const (
	LabelInstance  = "posthog.com/trino-instance"
	LabelPool      = "posthog.com/trino-pool"
	LabelManagedBy = "app.kubernetes.io/managed-by"
	ManagedByValue = "duckgres-trino-pool"

	AnnotationSpecDigest     = "posthog.com/trino-spec-digest"
	AnnotationAuthorityEpoch = "posthog.com/trino-authority-epoch"
	AnnotationReleaseID      = "posthog.com/trino-release-id"

	componentCoordinator = "coordinator"
	componentWorker      = "worker"
)

// Identity is everything instance-specific duckgres injects. It is a closed
// set on purpose: these fields, and nothing else, are what distinguishes one
// instantiated blueprint from another.
type Identity struct {
	PoolID           string
	PoolLabelValue   string
	InstanceID       string
	NodeEnvironment  string
	AuthorityEpoch   int64
	CoordinatorPort  int32
	DiscoveryURIHost string
}

func (i Identity) validate() error {
	if len(validation.IsDNS1123Label(i.InstanceID)) != 0 {
		return errors.New("instance identity must be a DNS label")
	}
	if len(validation.IsDNS1123Label(i.PoolLabelValue)) != 0 {
		return errors.New("pool label value must be a DNS label")
	}
	if i.NodeEnvironment == "" || len(i.NodeEnvironment) > 128 {
		return errors.New("instance identity requires a node environment")
	}
	if len(validation.IsDNS1123Subdomain(i.DiscoveryURIHost)) != 0 {
		return errors.New("instance identity requires a discovery host")
	}
	if i.CoordinatorPort < 1 || i.CoordinatorPort > 65535 {
		return errors.New("instance identity requires a valid coordinator port")
	}
	return nil
}

// Objects is one instance's complete Kubernetes inventory. Pool-shared objects
// (namespace, service accounts, auth Secret, TLS, resource groups) are NOT here
// and are never created or deleted by the instance lifecycle.
type Objects struct {
	ConfigMap             *corev1.ConfigMap
	WorkerConfigMap       *corev1.ConfigMap
	Service               *corev1.Service
	CoordinatorDeployment *appsv1.Deployment
	WorkerDeployment      *appsv1.Deployment
}

// All returns every object, for uniform stamping and inventory checks.
func (o Objects) All() []metav1.Object {
	return []metav1.Object{o.ConfigMap, o.WorkerConfigMap, o.Service, o.CoordinatorDeployment, o.WorkerDeployment}
}

// SpecDigest identifies the exact execution configuration of one instance: the
// blueprint plus the identity injected into it. It is stored on the instance
// row and annotated on every object, so "is this object mine and current?" is a
// string comparison rather than a deep diff.
func (b *Blueprint) SpecDigest(identity Identity) string {
	encoded, err := json.Marshal(struct {
		Blueprint string   `json:"blueprint"`
		Identity  Identity `json:"identity"`
	}{Blueprint: b.Digest(), Identity: identity})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// Instantiate renders one instance's objects from the blueprint.
//
// This is deliberately not a template engine. The pod templates and config file
// bodies are carried through byte for byte; duckgres only sets object names,
// labels, selectors, replica counts and the three identity env vars the
// blueprint's identity binding declares. That is what lets an instantiated
// blueprint be diffed against an ordinary chart render in a test.
func (b *Blueprint) Instantiate(identity Identity) (Objects, error) {
	if err := b.Validate(); err != nil {
		return Objects{}, err
	}
	if err := identity.validate(); err != nil {
		return Objects{}, err
	}

	digest := b.SpecDigest(identity)
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name:      name,
			Namespace: b.Namespace,
			Labels: map[string]string{
				LabelInstance:  identity.InstanceID,
				LabelPool:      identity.PoolLabelValue,
				LabelManagedBy: ManagedByValue,
			},
			Annotations: map[string]string{
				AnnotationSpecDigest:     digest,
				AnnotationAuthorityEpoch: strconv.FormatInt(identity.AuthorityEpoch, 10),
				AnnotationReleaseID:      b.ReleaseID,
			},
		}
	}

	coordinatorConfig := &corev1.ConfigMap{
		ObjectMeta: meta(identity.InstanceID + "-coordinator-config"),
		Data:       b.ConfigFiles[configFileRoleCoord],
	}
	workerConfig := &corev1.ConfigMap{
		ObjectMeta: meta(identity.InstanceID + "-worker-config"),
		Data:       b.ConfigFiles[configFileRoleWorker],
	}

	// One Service per instance, selecting only that instance's coordinator.
	// Load-balancing across coordinators would hand a worker to a foreign
	// cluster and silently merge two instances.
	service := &corev1.Service{
		ObjectMeta: meta(identity.InstanceID),
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				LabelInstance:                    identity.InstanceID,
				b.IdentityBinding.ComponentLabel: componentCoordinator,
			},
			Ports: []corev1.ServicePort{{
				Name:       b.IdentityBinding.CoordinatorHTTPPortName,
				Port:       identity.CoordinatorPort,
				TargetPort: intOrStringFromName(b.IdentityBinding.CoordinatorHTTPPortName),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}

	discoveryURI := fmt.Sprintf("https://%s:%d", identity.DiscoveryURIHost, identity.CoordinatorPort)
	coordinator := b.deployment(identity, meta(identity.InstanceID+"-coordinator"), componentCoordinator,
		b.Coordinator, 1, coordinatorConfig.Name, discoveryURI)
	worker := b.deployment(identity, meta(identity.InstanceID+"-worker"), componentWorker,
		b.Worker, b.Worker.Replicas, workerConfig.Name, discoveryURI)

	return Objects{
		ConfigMap:             coordinatorConfig,
		WorkerConfigMap:       workerConfig,
		Service:               service,
		CoordinatorDeployment: coordinator,
		WorkerDeployment:      worker,
	}, nil
}

func (b *Blueprint) deployment(
	identity Identity,
	objectMeta metav1.ObjectMeta,
	component string,
	workload BlueprintWorkload,
	replicas int32,
	configMapName string,
	discoveryURI string,
) *appsv1.Deployment {
	template := *workload.PodTemplate.DeepCopy()
	if template.Labels == nil {
		template.Labels = map[string]string{}
	}
	template.Labels[LabelInstance] = identity.InstanceID
	template.Labels[LabelPool] = identity.PoolLabelValue
	template.Labels[b.IdentityBinding.ComponentLabel] = component
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	// Roll the pods when the instance's own config changes. In practice an
	// instance's config never changes — a new release is a new instance — so
	// this only matters as a safety net.
	template.Annotations[AnnotationSpecDigest] = objectMeta.Annotations[AnnotationSpecDigest]
	template.Spec.ServiceAccountName = b.ServiceAccountName

	// Scope every pod anti-affinity term to THIS instance.
	//
	// The chart spreads workers across nodes with a selector that matches the
	// whole Trino app. Left alone, that selector would also match the workers
	// of every other instance in the pool, so a three-instance pool would
	// fight itself for nodes and the surge instance might never schedule. The
	// fix is to narrow the existing terms, not to drop them: spreading one
	// instance's workers is still what we want.
	scopeAntiAffinityToInstance(&template.Spec, identity.InstanceID)

	identityEnv := []corev1.EnvVar{
		{Name: b.IdentityBinding.DiscoveryURIEnv, Value: discoveryURI},
		{Name: b.IdentityBinding.NodeEnvironmentEnv, Value: identity.NodeEnvironment},
		{Name: b.IdentityBinding.InstanceIDEnv, Value: identity.InstanceID},
	}
	for index := range template.Spec.Containers {
		template.Spec.Containers[index].Env = append(template.Spec.Containers[index].Env, identityEnv...)
	}
	for index := range template.Spec.InitContainers {
		template.Spec.InitContainers[index].Env = append(template.Spec.InitContainers[index].Env, identityEnv...)
	}
	// The config volume, if the chart declared one, is pointed at THIS
	// instance's ConfigMap. Argo may replace the blueprint's source ConfigMap
	// for a new release; a running instance must keep reading its own copy.
	for index := range template.Spec.Volumes {
		volume := &template.Spec.Volumes[index]
		if volume.Name == "config" && volume.ConfigMap != nil {
			volume.ConfigMap.Name = configMapName
		}
	}
	if !hasVolume(template.Spec.Volumes, "config") {
		template.Spec.Volumes = append(template.Spec.Volumes, corev1.Volume{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
				},
			},
		})
	}

	strategy := appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	count := replicas
	return &appsv1.Deployment{
		ObjectMeta: objectMeta,
		Spec: appsv1.DeploymentSpec{
			Replicas: &count,
			// Recreate for both: a serving instance is never rolled in place.
			// Replacement means a new instance with a new identity, which is
			// what keeps "immutable instance" true.
			Strategy: strategy,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				LabelInstance:                    identity.InstanceID,
				b.IdentityBinding.ComponentLabel: component,
			}},
			Template: template,
		},
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

// intOrStringFromName targets the container port by NAME, so the chart stays
// free to change the numeric port without duckgres rewriting its render.
func intOrStringFromName(name string) intstr.IntOrString {
	return intstr.FromString(name)
}

// scopeAntiAffinityToInstance narrows each anti-affinity term's label selector
// with the instance label, preserving the chart's topology keys and weights.
// Terms that already select on the instance label are left alone.
func scopeAntiAffinityToInstance(spec *corev1.PodSpec, instanceID string) {
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		return
	}
	antiAffinity := spec.Affinity.PodAntiAffinity
	for index := range antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
		scopeAffinityTerm(&antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[index], instanceID)
	}
	for index := range antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		scopeAffinityTerm(&antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[index].PodAffinityTerm, instanceID)
	}
}

func scopeAffinityTerm(term *corev1.PodAffinityTerm, instanceID string) {
	if term.LabelSelector == nil {
		term.LabelSelector = &metav1.LabelSelector{}
	}
	if term.LabelSelector.MatchLabels == nil {
		term.LabelSelector.MatchLabels = map[string]string{}
	}
	term.LabelSelector.MatchLabels[LabelInstance] = instanceID
}
