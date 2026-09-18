package trinopool

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func testIdentity() Identity {
	return Identity{
		PoolID:           "registered:cell-001",
		PoolLabelValue:   "cell-001",
		InstanceID:       "cell-001-a1b2c3d4",
		NodeEnvironment:  "mw_dev_pool_001",
		AuthorityEpoch:   7,
		CoordinatorPort:  8443,
		DiscoveryURIHost: "cell-001-a1b2c3d4.trino-cell-001.svc.cluster.local",
	}
}

func instantiate(t *testing.T) Objects {
	t.Helper()
	objects, err := validBlueprint().Instantiate(testIdentity())
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	return objects
}

func TestInstantiateNamesEverythingAfterTheInstance(t *testing.T) {
	objects := instantiate(t)
	cases := map[string]string{
		"config map":             objects.ConfigMap.Name,
		"service":                objects.Service.Name,
		"coordinator deployment": objects.CoordinatorDeployment.Name,
		"worker deployment":      objects.WorkerDeployment.Name,
	}
	for what, name := range cases {
		if !strings.HasPrefix(name, "cell-001-a1b2c3d4") {
			t.Errorf("%s is named %q, which is not scoped to the instance", what, name)
		}
	}
	if objects.CoordinatorDeployment.Name == objects.WorkerDeployment.Name {
		t.Fatal("coordinator and worker deployments share a name")
	}
}

// Workers must discover only their own coordinator. A shared Service or a
// discovery URI pointing at another instance would silently merge two clusters.
func TestInstantiateBindsWorkersToTheirOwnCoordinator(t *testing.T) {
	objects := instantiate(t)

	selector := objects.Service.Spec.Selector
	if selector["posthog.com/trino-instance"] != "cell-001-a1b2c3d4" {
		t.Fatalf("service selector %v is not instance-scoped", selector)
	}
	if selector["app.kubernetes.io/component"] != "coordinator" {
		t.Fatalf("service selector %v does not pin the coordinator", selector)
	}

	discovery := ""
	for _, env := range objects.WorkerDeployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "TRINO_DISCOVERY_URI" {
			discovery = env.Value
		}
	}
	if !strings.Contains(discovery, "cell-001-a1b2c3d4") {
		t.Fatalf("worker discovery URI %q does not point at its own coordinator", discovery)
	}
}

// Every object carries the identity and the spec digest, so a stale leader's
// create is recognizable as foreign or outdated rather than adopted.
func TestInstantiateStampsOwnershipAndSpecDigest(t *testing.T) {
	objects := instantiate(t)
	digest := validBlueprint().SpecDigest(testIdentity())
	for _, object := range objects.All() {
		labels := object.GetLabels()
		if labels["posthog.com/trino-instance"] != "cell-001-a1b2c3d4" {
			t.Errorf("%s is missing the instance label", object.GetName())
		}
		if labels["posthog.com/trino-pool"] != "cell-001" {
			t.Errorf("%s is missing the pool label", object.GetName())
		}
		annotations := object.GetAnnotations()
		if annotations[AnnotationSpecDigest] != digest {
			t.Errorf("%s carries spec digest %q, want %q", object.GetName(), annotations[AnnotationSpecDigest], digest)
		}
		if annotations[AnnotationAuthorityEpoch] != "7" {
			t.Errorf("%s carries authority epoch %q", object.GetName(), annotations[AnnotationAuthorityEpoch])
		}
		if object.GetNamespace() != "trino-cell-a" {
			t.Errorf("%s landed in namespace %q", object.GetName(), object.GetNamespace())
		}
	}
}

// The coordinator is a singleton: two coordinators behind one discovery URI
// would race. Recreate, never a rolling update inside a serving instance.
func TestInstantiateMakesTheCoordinatorASingleton(t *testing.T) {
	objects := instantiate(t)
	if objects.CoordinatorDeployment.Spec.Replicas == nil || *objects.CoordinatorDeployment.Spec.Replicas != 1 {
		t.Fatal("coordinator is not a single replica")
	}
	if objects.CoordinatorDeployment.Spec.Strategy.Type != "Recreate" {
		t.Fatalf("coordinator strategy = %q, want Recreate", objects.CoordinatorDeployment.Spec.Strategy.Type)
	}
	if objects.WorkerDeployment.Spec.Replicas == nil || *objects.WorkerDeployment.Spec.Replicas != 4 {
		t.Fatal("worker replica count does not come from the blueprint")
	}
}

// The chart's own containers, volumes and probes must survive instantiation
// untouched: duckgres injects identity, it does not rewrite execution config.
func TestInstantiatePreservesTheChartRender(t *testing.T) {
	blueprint := validBlueprint()
	blueprint.Coordinator.PodTemplate.Spec.Containers = append(blueprint.Coordinator.PodTemplate.Spec.Containers,
		corev1.Container{Name: "opa", Image: testImage})
	blueprint.Coordinator.PodTemplate.Spec.Containers[0].Args = []string{"--flag"}

	objects, err := blueprint.Instantiate(testIdentity())
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	containers := objects.CoordinatorDeployment.Spec.Template.Spec.Containers
	if len(containers) != 2 || containers[1].Name != "opa" {
		t.Fatalf("sidecar was dropped: %+v", containers)
	}
	if len(containers[0].Args) != 1 || containers[0].Args[0] != "--flag" {
		t.Fatal("chart-provided args were rewritten")
	}
	if containers[0].Image != blueprint.Image {
		t.Fatal("container image was rewritten")
	}
}

// Identity env vars go on EVERY container of the pod, because a sidecar may
// consume them too, and the blueprint validator has already refused templates
// that set them.
func TestInstantiateInjectsIdentityIntoEveryContainer(t *testing.T) {
	blueprint := validBlueprint()
	blueprint.Coordinator.PodTemplate.Spec.Containers = append(blueprint.Coordinator.PodTemplate.Spec.Containers,
		corev1.Container{Name: "opa", Image: testImage})
	objects, err := blueprint.Instantiate(testIdentity())
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	for _, container := range objects.CoordinatorDeployment.Spec.Template.Spec.Containers {
		found := map[string]bool{}
		for _, env := range container.Env {
			found[env.Name] = true
		}
		for _, name := range []string{"TRINO_DISCOVERY_URI", "TRINO_NODE_ENVIRONMENT", "DUCKGRES_TRINO_INSTANCE_ID"} {
			if !found[name] {
				t.Errorf("container %q is missing %s", container.Name, name)
			}
		}
	}
}

// Config files are mounted from the instance's OWN ConfigMap, so a new release
// cannot change what a running instance reads.
func TestInstantiateMountsTheInstanceConfigMap(t *testing.T) {
	objects := instantiate(t)
	if objects.ConfigMap.Data["config.properties"] == "" || objects.ConfigMap.Data["node.properties"] == "" {
		t.Fatalf("config map is missing coordinator files: %v", objects.ConfigMap.Data)
	}
	if objects.WorkerConfigMap.Data["config.properties"] == "" {
		t.Fatalf("worker config map is missing files: %v", objects.WorkerConfigMap.Data)
	}
	if objects.ConfigMap.Name == objects.WorkerConfigMap.Name {
		t.Fatal("coordinator and worker share one config map")
	}
}

// The spec digest must change when anything that affects execution changes,
// and stay identical otherwise: it is what a conditional write compares.
func TestSpecDigestIsSensitiveToIdentityAndBlueprint(t *testing.T) {
	blueprint := validBlueprint()
	base := blueprint.SpecDigest(testIdentity())
	if base != blueprint.SpecDigest(testIdentity()) {
		t.Fatal("spec digest is not deterministic")
	}

	other := testIdentity()
	other.InstanceID = "cell-001-ffffffff"
	if blueprint.SpecDigest(other) == base {
		t.Fatal("a different instance produced the same spec digest")
	}

	changed := validBlueprint()
	changed.Worker.Replicas = 5
	if changed.SpecDigest(testIdentity()) == base {
		t.Fatal("a blueprint change produced the same spec digest")
	}
}

func TestInstantiateRejectsAnIncompleteIdentity(t *testing.T) {
	cases := map[string]func(*Identity){
		"missing instance":        func(i *Identity) { i.InstanceID = "" },
		"instance is not a label": func(i *Identity) { i.InstanceID = "Not_A_Label" },
		"missing discovery host":  func(i *Identity) { i.DiscoveryURIHost = "" },
		"missing node env":        func(i *Identity) { i.NodeEnvironment = "" },
		"missing pool label":      func(i *Identity) { i.PoolLabelValue = "" },
		"invalid port":            func(i *Identity) { i.CoordinatorPort = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			identity := testIdentity()
			mutate(&identity)
			if _, err := validBlueprint().Instantiate(identity); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}
