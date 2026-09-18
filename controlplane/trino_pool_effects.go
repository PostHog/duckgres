//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Kubernetes effects for one shared-pool instance.
//
// There is no atomic handover across the duckgres database, the Gateway
// database and Kubernetes, so every write here is conditional and every delete
// carries a precondition. The three rules:
//
//   - Never adopt an object that is not ours. On AlreadyExists we compare
//     ownership and identity; a foreign or mismatched object is an error, not a
//     thing to overwrite.
//   - Never lower the authority epoch. A superseded leader that has not noticed
//     yet is refused at the object level, not just at the loop entry.
//   - Never delete by name alone. A UID precondition is what stops a delete from
//     removing a replacement object that happens to share the name.
const (
	trinoPoolRequestBudget = 10 * time.Second
	trinoPoolListLimit     = 500
)

var (
	errTrinoPoolForeignObject  = errors.New("kubernetes object is not managed by this trino pool")
	errTrinoPoolStaleEpoch     = errors.New("kubernetes object carries a newer authority epoch")
	errTrinoPoolSharedResource = errors.New("refusing to touch a pool-shared resource")
)

// trinoPoolInventory is the recorded Kubernetes identity of one instance. It is
// persisted before any delete, so a delete always knows exactly what it may
// remove.
type trinoPoolInventory struct {
	Namespace                 string
	ConfigMapName             string
	ConfigMapUID              string
	WorkerConfigMapName       string
	WorkerConfigMapUID        string
	ServiceName               string
	ServiceUID                string
	CoordinatorDeploymentName string
	CoordinatorDeploymentUID  string
	WorkerDeploymentName      string
	WorkerDeploymentUID       string
}

// trinoPoolObservation is what the cluster currently reports about an instance.
type trinoPoolObservation struct {
	ReadyWorkers      int
	DesiredWorkers    int
	CoordinatorReady  bool
	CoordinatorPodUID string
	PodsPresent       int
}

type trinoPoolEffects struct {
	clientset kubernetes.Interface
	namespace string
	shared    trinopool.BlueprintSharedResources
	epoch     int64
}

func newTrinoPoolEffects(clientset kubernetes.Interface, namespace string, shared trinopool.BlueprintSharedResources, epoch int64) *trinoPoolEffects {
	return &trinoPoolEffects{clientset: clientset, namespace: namespace, shared: shared, epoch: epoch}
}

// Apply creates or adopts this instance's objects and returns the recorded
// inventory. It is idempotent: a lost create response is resolved by reading the
// deterministic name back, never by creating a second instance.
func (e *trinoPoolEffects) Apply(ctx context.Context, objects trinopool.Objects) (trinoPoolInventory, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()

	inventory := trinoPoolInventory{Namespace: e.namespace}

	configMapUID, err := e.applyConfigMap(ctx, objects.ConfigMap)
	if err != nil {
		return inventory, err
	}
	inventory.ConfigMapName, inventory.ConfigMapUID = objects.ConfigMap.Name, configMapUID

	workerConfigUID, err := e.applyConfigMap(ctx, objects.WorkerConfigMap)
	if err != nil {
		return inventory, err
	}
	inventory.WorkerConfigMapName, inventory.WorkerConfigMapUID = objects.WorkerConfigMap.Name, workerConfigUID

	serviceUID, err := e.applyService(ctx, objects.Service)
	if err != nil {
		return inventory, err
	}
	inventory.ServiceName, inventory.ServiceUID = objects.Service.Name, serviceUID

	// The coordinator is created before the workers so the discovery endpoint
	// exists by the time a worker tries to register.
	coordinatorUID, err := e.applyDeployment(ctx, objects.CoordinatorDeployment)
	if err != nil {
		return inventory, err
	}
	inventory.CoordinatorDeploymentName, inventory.CoordinatorDeploymentUID = objects.CoordinatorDeployment.Name, coordinatorUID

	workerUID, err := e.applyDeployment(ctx, objects.WorkerDeployment)
	if err != nil {
		return inventory, err
	}
	inventory.WorkerDeploymentName, inventory.WorkerDeploymentUID = objects.WorkerDeployment.Name, workerUID

	return inventory, nil
}

func (e *trinoPoolEffects) applyConfigMap(ctx context.Context, desired *corev1.ConfigMap) (string, error) {
	if err := e.guard(desired.Name); err != nil {
		return "", err
	}
	created, err := e.clientset.CoreV1().ConfigMaps(e.namespace).Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return string(created.UID), nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create config map %s: %w", desired.Name, err)
	}
	existing, err := e.clientset.CoreV1().ConfigMaps(e.namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read back config map %s: %w", desired.Name, err)
	}
	if err := e.checkOwnership(existing.ObjectMeta, desired.ObjectMeta); err != nil {
		return "", err
	}
	return string(existing.UID), nil
}

func (e *trinoPoolEffects) applyService(ctx context.Context, desired *corev1.Service) (string, error) {
	if err := e.guard(desired.Name); err != nil {
		return "", err
	}
	created, err := e.clientset.CoreV1().Services(e.namespace).Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return string(created.UID), nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create service %s: %w", desired.Name, err)
	}
	existing, err := e.clientset.CoreV1().Services(e.namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read back service %s: %w", desired.Name, err)
	}
	if err := e.checkOwnership(existing.ObjectMeta, desired.ObjectMeta); err != nil {
		return "", err
	}
	return string(existing.UID), nil
}

func (e *trinoPoolEffects) applyDeployment(ctx context.Context, desired *appsv1.Deployment) (string, error) {
	if err := e.guard(desired.Name); err != nil {
		return "", err
	}
	created, err := e.clientset.AppsV1().Deployments(e.namespace).Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return string(created.UID), nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create deployment %s: %w", desired.Name, err)
	}
	existing, err := e.clientset.AppsV1().Deployments(e.namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read back deployment %s: %w", desired.Name, err)
	}
	if err := e.checkOwnership(existing.ObjectMeta, desired.ObjectMeta); err != nil {
		return "", err
	}
	// An existing object with OUR identity and the same spec digest is this
	// operation's own earlier attempt. It is adopted as-is and never patched:
	// a serving instance's execution configuration is immutable by design.
	return string(existing.UID), nil
}

// checkOwnership decides whether an existing object is this instance's own.
func (e *trinoPoolEffects) checkOwnership(existing, desired metav1.ObjectMeta) error {
	if existing.Labels[trinopool.LabelManagedBy] != trinopool.ManagedByValue ||
		existing.Labels[trinopool.LabelInstance] != desired.Labels[trinopool.LabelInstance] {
		return fmt.Errorf("%w: %s", errTrinoPoolForeignObject, existing.Name)
	}
	recorded, err := strconv.ParseInt(existing.Annotations[trinopool.AnnotationAuthorityEpoch], 10, 64)
	if err != nil {
		return fmt.Errorf("%w: %s has no readable authority epoch", errTrinoPoolForeignObject, existing.Name)
	}
	if recorded > e.epoch {
		return fmt.Errorf("%w: %s is owned at epoch %d, this leader holds %d", errTrinoPoolStaleEpoch, existing.Name, recorded, e.epoch)
	}
	if existing.Annotations[trinopool.AnnotationSpecDigest] != desired.Annotations[trinopool.AnnotationSpecDigest] {
		return fmt.Errorf("%w: %s carries a different spec digest", errTrinoPoolForeignObject, existing.Name)
	}
	return nil
}

// guard refuses to touch anything the pool shares. Instance cleanup must never
// be able to remove the pool's trust boundary, so the check is here rather than
// only in the caller.
func (e *trinoPoolEffects) guard(name string) error {
	if e.shared.Protects(name) {
		return fmt.Errorf("%w: %s", errTrinoPoolSharedResource, name)
	}
	return nil
}

// Delete removes the instance's objects. Controllers go first: deleting pods
// while their Deployment still exists just produces new pods.
//
// The caller must hold an irreversible retirement receipt for this exact
// incarnation before calling this. Nothing here checks that, because nothing
// here can - it is enforced by the instance phase, which only permits deletion
// from RETIRING onwards.
func (e *trinoPoolEffects) Delete(ctx context.Context, inventory trinoPoolInventory) error {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()

	for _, name := range []string{
		inventory.CoordinatorDeploymentName, inventory.WorkerDeploymentName,
		inventory.ServiceName, inventory.ConfigMapName, inventory.WorkerConfigMapName,
	} {
		if err := e.guard(name); err != nil {
			return err
		}
	}

	deploymentUID := func(ctx context.Context, name string) (string, error) {
		object, err := e.clientset.AppsV1().Deployments(inventory.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return string(object.UID), nil
	}
	serviceUID := func(ctx context.Context, name string) (string, error) {
		object, err := e.clientset.CoreV1().Services(inventory.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return string(object.UID), nil
	}
	configMapUID := func(ctx context.Context, name string) (string, error) {
		object, err := e.clientset.CoreV1().ConfigMaps(inventory.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return string(object.UID), nil
	}
	deleteDeployment := func(ctx context.Context, name string, options metav1.DeleteOptions) error {
		return e.clientset.AppsV1().Deployments(inventory.Namespace).Delete(ctx, name, options)
	}
	deleteService := func(ctx context.Context, name string, options metav1.DeleteOptions) error {
		return e.clientset.CoreV1().Services(inventory.Namespace).Delete(ctx, name, options)
	}
	deleteConfigMap := func(ctx context.Context, name string, options metav1.DeleteOptions) error {
		return e.clientset.CoreV1().ConfigMaps(inventory.Namespace).Delete(ctx, name, options)
	}

	// Workload controllers first: deleting pods while their Deployment still
	// exists just makes the Deployment create new ones.
	deletions := []struct {
		kind    string
		name    string
		uid     string
		observe func(context.Context, string) (string, error)
		call    func(context.Context, string, metav1.DeleteOptions) error
	}{
		{"deployment", inventory.CoordinatorDeploymentName, inventory.CoordinatorDeploymentUID, deploymentUID, deleteDeployment},
		{"deployment", inventory.WorkerDeploymentName, inventory.WorkerDeploymentUID, deploymentUID, deleteDeployment},
		{"service", inventory.ServiceName, inventory.ServiceUID, serviceUID, deleteService},
		{"config map", inventory.ConfigMapName, inventory.ConfigMapUID, configMapUID, deleteConfigMap},
		{"config map", inventory.WorkerConfigMapName, inventory.WorkerConfigMapUID, configMapUID, deleteConfigMap},
	}
	for _, deletion := range deletions {
		if deletion.name == "" {
			continue
		}
		// Verify the UID ourselves before asking the API server to, so the
		// check holds even against an implementation that ignores
		// preconditions. A name whose UID has moved on belongs to a different
		// object, and deleting it would destroy somebody else's workload.
		current, err := deletion.observe(ctx, deletion.name)
		switch {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return fmt.Errorf("read back %s %s: %w", deletion.kind, deletion.name, err)
		case deletion.uid != "" && current != deletion.uid:
			return fmt.Errorf("%w: %s %s is now %s, recorded %s",
				errTrinoPoolForeignObject, deletion.kind, deletion.name, current, deletion.uid)
		}

		options := metav1.DeleteOptions{}
		if deletion.uid != "" {
			uid := types.UID(deletion.uid)
			options.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		err = deletion.call(ctx, deletion.name, options)
		switch {
		case err == nil, apierrors.IsNotFound(err):
		default:
			// A UID mismatch surfaces as a conflict. It means the name now
			// belongs to a different object, which must not be deleted.
			return fmt.Errorf("delete %s %s: %w", deletion.kind, deletion.name, err)
		}
	}
	return nil
}

// ResourcesAbsent reports whether every recorded object is gone, INCLUDING
// pods that are still terminating. Retirement completes only on verified
// absence; a Deployment that has been deleted while its pods still run is not
// an absent instance.
func (e *trinoPoolEffects) ResourcesAbsent(ctx context.Context, inventory trinoPoolInventory) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()

	for _, name := range []string{inventory.CoordinatorDeploymentName, inventory.WorkerDeploymentName} {
		if name == "" {
			continue
		}
		_, err := e.clientset.AppsV1().Deployments(inventory.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("observe deployment %s: %w", name, err)
		}
	}
	if inventory.ServiceName != "" {
		_, err := e.clientset.CoreV1().Services(inventory.Namespace).Get(ctx, inventory.ServiceName, metav1.GetOptions{})
		if err == nil {
			return false, nil
		}
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("observe service %s: %w", inventory.ServiceName, err)
		}
	}
	pods, err := e.instancePods(ctx, inventory)
	if err != nil {
		return false, err
	}
	return len(pods) == 0, nil
}

// Observe reports the cluster's current view of the instance.
func (e *trinoPoolEffects) Observe(ctx context.Context, inventory trinoPoolInventory) (trinoPoolObservation, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()

	var observation trinoPoolObservation
	worker, err := e.clientset.AppsV1().Deployments(inventory.Namespace).Get(ctx, inventory.WorkerDeploymentName, metav1.GetOptions{})
	if err != nil {
		return observation, fmt.Errorf("observe worker deployment: %w", err)
	}
	observation.ReadyWorkers = int(worker.Status.ReadyReplicas)
	if worker.Spec.Replicas != nil {
		observation.DesiredWorkers = int(*worker.Spec.Replicas)
	}

	coordinator, err := e.clientset.AppsV1().Deployments(inventory.Namespace).Get(ctx, inventory.CoordinatorDeploymentName, metav1.GetOptions{})
	if err != nil {
		return observation, fmt.Errorf("observe coordinator deployment: %w", err)
	}
	observation.CoordinatorReady = coordinator.Status.ReadyReplicas == 1

	pods, err := e.instancePods(ctx, inventory)
	if err != nil {
		return observation, err
	}
	observation.PodsPresent = len(pods)
	for _, pod := range pods {
		if pod.Labels["app.kubernetes.io/component"] == "coordinator" && pod.DeletionTimestamp == nil {
			observation.CoordinatorPodUID = string(pod.UID)
		}
	}
	return observation, nil
}

// instancePods lists the instance's pods, terminating ones included: a pod that
// is going away still occupies the instance until it is actually gone.
func (e *trinoPoolEffects) instancePods(ctx context.Context, inventory trinoPoolInventory) ([]corev1.Pod, error) {
	instance := instanceLabelFromInventory(inventory)
	if instance == "" {
		return nil, nil
	}
	pods, err := e.clientset.CoreV1().Pods(inventory.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: trinopool.LabelInstance + "=" + instance,
		Limit:         trinoPoolListLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("observe instance pods: %w", err)
	}
	if pods.Continue != "" {
		return nil, errors.New("instance pod inventory exceeds the listing limit")
	}
	return pods.Items, nil
}

// instanceLabelFromInventory recovers the instance id from the recorded names.
// Every object is named "<instance>-<role>" except the Service, which is named
// exactly after the instance.
func instanceLabelFromInventory(inventory trinoPoolInventory) string {
	return inventory.ServiceName
}
