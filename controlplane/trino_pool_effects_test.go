//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// testBlueprintJSON reads the same checked-in fixture the blueprint decoder
// tests use, so the effects layer is exercised against the document shape
// charts actually generates rather than a hand-made struct.
func testBlueprintJSON(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("trinopool", "testdata", "blueprint.json"))
	if err != nil {
		t.Fatalf("read blueprint fixture: %v", err)
	}
	return data
}

func testPoolObjects(t *testing.T, epoch int64) (*trinopool.Blueprint, trinopool.Identity, trinopool.Objects) {
	t.Helper()
	blueprint, err := trinopool.ParseBlueprint(testBlueprintJSON(t))
	if err != nil {
		t.Fatalf("parse blueprint: %v", err)
	}
	identity := trinopool.Identity{
		PoolID: "registered:cell-001", PoolLabelValue: "cell-001",
		InstanceID: "cell-001-a1b2c3d4", NodeEnvironment: "mw_dev_pool_001",
		AuthorityEpoch: epoch, CoordinatorPort: 8443,
		DiscoveryURIHost: "cell-001-a1b2c3d4.trino-pool-example.svc.cluster.local",
	}
	objects, err := blueprint.Instantiate(identity)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	return blueprint, identity, objects
}

// emulateUIDAssignment makes the fake clientset behave like a real API server
// for the one property this code depends on: every created object gets a UID.
// Without it the recorded inventory would be empty and every delete would lose
// its precondition, which is exactly the safety property under test.
func emulateUIDAssignment(clientset *fake.Clientset) {
	var sequence int
	clientset.PrependReactor("create", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		object, err := meta.Accessor(create.GetObject())
		if err != nil || object.GetUID() != "" {
			return false, nil, nil
		}
		sequence++
		object.SetUID(types.UID(fmt.Sprintf("uid-%d", sequence)))
		return false, nil, nil
	})
}

func newEffects(t *testing.T, epoch int64) (*trinoPoolEffects, *fake.Clientset) {
	t.Helper()
	clientset := fake.NewClientset()
	emulateUIDAssignment(clientset)
	blueprint, _, _ := testPoolObjects(t, epoch)
	return newTrinoPoolEffects(clientset, blueprint.Namespace, blueprint.SharedResources, epoch), clientset
}

func TestEffectsApplyCreatesTheWholeInventory(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)

	inventory, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if inventory.ServiceUID == "" || inventory.CoordinatorDeploymentUID == "" || inventory.WorkerDeploymentUID == "" {
		t.Fatalf("inventory is missing recorded UIDs: %+v", inventory)
	}
	deployments, err := clientset.AppsV1().Deployments("trino-pool-example").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(deployments.Items) != 2 {
		t.Fatalf("deployments = %v (err %v)", deployments, err)
	}
}

// A lost create response must not produce a second instance. The names are
// deterministic and already recorded, so the next attempt reads the object back
// and adopts its own object.
func TestEffectsApplyIsIdempotent(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)

	first, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	second, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if first.CoordinatorDeploymentUID != second.CoordinatorDeploymentUID {
		t.Fatal("a repeated apply replaced the coordinator deployment")
	}
	deployments, _ := clientset.AppsV1().Deployments("trino-pool-example").List(context.Background(), metav1.ListOptions{})
	if len(deployments.Items) != 2 {
		t.Fatalf("a repeated apply created %d deployments", len(deployments.Items))
	}
}

// An object that exists but belongs to something else is never adopted: doing
// so would let duckgres take over a workload another authority manages.
func TestEffectsRefuseToAdoptForeignObjects(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)

	foreign := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      objects.Service.Name,
		Namespace: objects.Service.Namespace,
		Labels:    map[string]string{"app.kubernetes.io/managed-by": "somebody-else"},
	}}
	if _, err := clientset.CoreV1().Services(foreign.Namespace).Create(context.Background(), foreign, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed foreign service: %v", err)
	}
	if _, err := effects.Apply(context.Background(), objects); !errors.Is(err, errTrinoPoolForeignObject) {
		t.Fatalf("apply error = %v, want errTrinoPoolForeignObject", err)
	}
}

// A superseded leader must not be able to mutate the current leader's objects.
func TestEffectsRefuseToWriteUnderAStaleEpoch(t *testing.T) {
	current, clientset := newEffects(t, 9)
	_, _, objects := testPoolObjects(t, 9)
	if _, err := current.Apply(context.Background(), objects); err != nil {
		t.Fatalf("apply: %v", err)
	}

	blueprint, _, staleObjects := testPoolObjects(t, 4)
	stale := newTrinoPoolEffects(clientset, blueprint.Namespace, blueprint.SharedResources, 4)
	if _, err := stale.Apply(context.Background(), staleObjects); !errors.Is(err, errTrinoPoolStaleEpoch) {
		t.Fatalf("stale apply error = %v, want errTrinoPoolStaleEpoch", err)
	}

	service, err := clientset.CoreV1().Services("trino-pool-example").Get(context.Background(), objects.Service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Annotations[trinopool.AnnotationAuthorityEpoch] != "9" {
		t.Fatalf("stale leader lowered the epoch to %q", service.Annotations[trinopool.AnnotationAuthorityEpoch])
	}
}

// Deleting pods while their controller still exists just makes new pods. Stop
// the controllers first, then verify absence.
func TestEffectsDeleteRemovesControllersAndRecordsAbsence(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)
	inventory, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if err := effects.Delete(context.Background(), inventory); err != nil {
		t.Fatalf("delete: %v", err)
	}
	deployments, _ := clientset.AppsV1().Deployments("trino-pool-example").List(context.Background(), metav1.ListOptions{})
	if len(deployments.Items) != 0 {
		t.Fatalf("%d deployments survived deletion", len(deployments.Items))
	}
	services, _ := clientset.CoreV1().Services("trino-pool-example").List(context.Background(), metav1.ListOptions{})
	if len(services.Items) != 0 {
		t.Fatalf("%d services survived deletion", len(services.Items))
	}

	absent, err := effects.ResourcesAbsent(context.Background(), inventory)
	if err != nil || !absent {
		t.Fatalf("absent = %v (err %v)", absent, err)
	}
}

// Deleting an object whose UID moved on would destroy somebody else's
// replacement object that happens to share the name.
func TestEffectsDeleteIsUIDPreconditioned(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)
	inventory, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	inventory.ServiceUID = "some-other-uid"

	if err := effects.Delete(context.Background(), inventory); err == nil {
		t.Fatal("delete with a mismatched UID was accepted")
	}
	if _, err := clientset.CoreV1().Services("trino-pool-example").Get(context.Background(), objects.Service.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("service was deleted despite the UID mismatch: %v", err)
	}
}

// Instance cleanup must never remove the pool's shared trust boundary.
func TestEffectsNeverDeleteSharedResources(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	shared := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trino-auth", Namespace: "trino-pool-example"}}
	if _, err := clientset.CoreV1().Secrets(shared.Namespace).Create(context.Background(), shared, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed shared secret: %v", err)
	}

	// A malformed inventory that names a shared object must be refused rather
	// than executed.
	if err := effects.Delete(context.Background(), trinoPoolInventory{
		Namespace: "trino-pool-example", ServiceName: "trino-auth", ServiceUID: "x",
	}); !errors.Is(err, errTrinoPoolSharedResource) {
		t.Fatalf("delete error = %v, want errTrinoPoolSharedResource", err)
	}
	if _, err := clientset.CoreV1().Secrets("trino-pool-example").Get(context.Background(), "trino-auth", metav1.GetOptions{}); err != nil {
		t.Fatalf("shared secret was removed: %v", err)
	}
}

func TestEffectsObserveReportsWorkerReadiness(t *testing.T) {
	effects, clientset := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)
	inventory, err := effects.Apply(context.Background(), objects)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	worker, err := clientset.AppsV1().Deployments(inventory.Namespace).Get(context.Background(), inventory.WorkerDeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker deployment: %v", err)
	}
	worker.Status = appsv1.DeploymentStatus{ReadyReplicas: 4, ObservedGeneration: worker.Generation}
	if _, err := clientset.AppsV1().Deployments(inventory.Namespace).UpdateStatus(context.Background(), worker, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}

	observed, err := effects.Observe(context.Background(), inventory)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if observed.ReadyWorkers != 4 {
		t.Fatalf("ready workers = %d, want 4", observed.ReadyWorkers)
	}
}
