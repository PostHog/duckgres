//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

// trinoBenchmarkManager is the concrete Kubernetes TrinoBenchmarkLifecycle: it
// renders one coordinator, exactly the requested number of workers, a ClusterIP
// Service, the coordinator/worker/catalog ConfigMaps, and a short-lived Secret
// holding ONLY the charts-created metadata reader password.
//
// Everything it creates is labelled with the cluster ID and the owning org, and
// every read/delete is filtered by those labels — so a benchmark teardown can
// never reach a worker pod, another benchmark cluster, or the charts-created
// reader Secret itself.
//
// There is no cluster state outside Kubernetes: the Service is the ownership
// anchor and its annotations record the configuration fingerprint, so any
// control-plane replica can answer status and run cleanup.

const (
	// trinoBenchmarkAppLabelKey/Value mark every object the manager owns.
	trinoBenchmarkAppLabelKey   = "app.kubernetes.io/name"
	trinoBenchmarkAppLabelValue = "duckgres-trino-benchmark"
	// trinoBenchmarkClusterLabel is the per-cluster ownership label. Cleanup
	// selects on it, so an object without it is never deleted.
	trinoBenchmarkClusterLabel = "duckgres.posthog.com/trino-benchmark-cluster"
	// trinoBenchmarkOrgLabel records which warehouse the cluster reads.
	trinoBenchmarkOrgLabel = "duckgres.posthog.com/org"
	// trinoBenchmarkRoleLabel separates the coordinator from the workers; the
	// Service selects on it so statements only ever reach the coordinator.
	trinoBenchmarkRoleLabel       = "duckgres.posthog.com/trino-role"
	trinoBenchmarkRoleCoordinator = "coordinator"
	trinoBenchmarkRoleWorker      = "worker"

	// Configuration fingerprint. A repeat provision with a different value is a
	// conflict, never a silent adoption.
	trinoBenchmarkImageAnnotation   = "duckgres.posthog.com/trino-image"
	trinoBenchmarkWorkersAnnotation = "duckgres.posthog.com/trino-workers"
	trinoBenchmarkRunIDAnnotation   = "duckgres.posthog.com/run-id"
	trinoBenchmarkOwnerAnnotation   = "duckgres.posthog.com/owner"

	// trinoBenchmarkSecretPasswordKey is the only key the short-lived Secret
	// ever holds.
	trinoBenchmarkSecretPasswordKey = "metadata-password"
	// trinoBenchmarkPasswordEnv is the env var the catalog properties
	// interpolate with ${ENV:...}.
	trinoBenchmarkPasswordEnv = "TRINO_DUCKLAKE_DB_PASSWORD"

	trinoBenchmarkHTTPPort = 8080
	// trinoBenchmarkCatalogName is the Trino catalog the benchmark queries
	// address (ducklake.<schema>.<table>).
	trinoBenchmarkCatalogName = "ducklake"

	// defaultTrinoBenchmarkWorkers is the comparison shape the dev benchmark
	// was designed around: one Duckgres worker vs four Trino workers.
	defaultTrinoBenchmarkWorkers = 4

	// Documented resource defaults. requests == limits (Guaranteed QoS) so
	// benchmark pods neither burst into nor get throttled by whatever else
	// shares the node — the Duckgres worker being compared included.
	defaultTrinoCoordinatorCPU    = "2"
	defaultTrinoCoordinatorMemory = "8Gi"
	defaultTrinoWorkerCPU         = "2"
	defaultTrinoWorkerMemory      = "8Gi"

	defaultTrinoBenchmarkPullPolicy = string(corev1.PullIfNotPresent)
)

// TrinoBenchmarkManagerConfig is the explicit control-plane configuration for
// benchmark clusters. Nothing here is caller-supplied: the pinned image, the
// pod shape, and the namespace come from deployment configuration only.
type TrinoBenchmarkManagerConfig struct {
	Namespace       string
	Image           string // pinned Trino+Brikk image (digest preferred); required
	ImagePullPolicy string // default IfNotPresent
	ServiceAccount  string // ServiceAccount whose IAM identity may assume the reader role
	DefaultWorkers  int    // default 4

	CoordinatorCPU    string // default 2
	CoordinatorMemory string // default 8Gi
	WorkerCPU         string // default 2
	WorkerMemory      string // default 8Gi

	// ControlPlaneID is recorded as the owner annotation, so an operator can
	// see which control plane created a leftover cluster.
	ControlPlaneID string
}

func (c *TrinoBenchmarkManagerConfig) applyDefaults() {
	if c.DefaultWorkers <= 0 {
		c.DefaultWorkers = defaultTrinoBenchmarkWorkers
	}
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = defaultTrinoBenchmarkPullPolicy
	}
	if c.CoordinatorCPU == "" {
		c.CoordinatorCPU = defaultTrinoCoordinatorCPU
	}
	if c.CoordinatorMemory == "" {
		c.CoordinatorMemory = defaultTrinoCoordinatorMemory
	}
	if c.WorkerCPU == "" {
		c.WorkerCPU = defaultTrinoWorkerCPU
	}
	if c.WorkerMemory == "" {
		c.WorkerMemory = defaultTrinoWorkerMemory
	}
}

type trinoBenchmarkManager struct {
	clientset kubernetes.Interface
	resolver  TrinoReaderResolver
	cfg       TrinoBenchmarkManagerConfig
}

var _ TrinoBenchmarkLifecycle = (*trinoBenchmarkManager)(nil)

// newTrinoBenchmarkManager fails closed: without a pinned image or a reader
// resolver there is no safe cluster to build, so the deployment gets no
// lifecycle at all rather than a partially configured one.
func newTrinoBenchmarkManager(clientset kubernetes.Interface, resolver TrinoReaderResolver, cfg TrinoBenchmarkManagerConfig) (*trinoBenchmarkManager, error) {
	if clientset == nil {
		return nil, fmt.Errorf("%w: no Kubernetes client", ErrTrinoBenchmarkConfig)
	}
	if resolver == nil {
		return nil, fmt.Errorf("%w: no Trino reader identity resolver", ErrTrinoBenchmarkConfig)
	}
	if strings.TrimSpace(cfg.Image) == "" {
		return nil, fmt.Errorf("%w: no pinned Trino benchmark image", ErrTrinoBenchmarkConfig)
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, fmt.Errorf("%w: no Trino benchmark namespace", ErrTrinoBenchmarkConfig)
	}
	// Validate the pod shape up front so a bad quantity surfaces at startup
	// rather than mid-scenario.
	cfg.applyDefaults()
	for name, quantity := range map[string]string{
		"coordinator CPU": cfg.CoordinatorCPU, "coordinator memory": cfg.CoordinatorMemory,
		"worker CPU": cfg.WorkerCPU, "worker memory": cfg.WorkerMemory,
	} {
		if _, err := resource.ParseQuantity(quantity); err != nil {
			return nil, fmt.Errorf("%w: invalid Trino benchmark %s %q", ErrTrinoBenchmarkConfig, name, quantity)
		}
	}
	if cfg.DefaultWorkers > maxTrinoBenchmarkWorkers {
		return nil, fmt.Errorf("%w: default worker count %d exceeds the %d maximum",
			ErrTrinoBenchmarkConfig, cfg.DefaultWorkers, maxTrinoBenchmarkWorkers)
	}
	return &trinoBenchmarkManager{clientset: clientset, resolver: resolver, cfg: cfg}, nil
}

// TrinoBenchmarkClusterID is the deterministic per-org cluster name. One
// benchmark cluster per warehouse at a time, so a repeat provision converges
// instead of piling up clusters.
func TrinoBenchmarkClusterID(orgID string) string {
	return "trino-bench-" + orgID
}

func (m *trinoBenchmarkManager) endpoint(clusterID string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", clusterID, m.cfg.Namespace, trinoBenchmarkHTTPPort)
}

func (m *trinoBenchmarkManager) ownershipLabels(clusterID, orgID string) map[string]string {
	return map[string]string{
		trinoBenchmarkAppLabelKey:  trinoBenchmarkAppLabelValue,
		trinoBenchmarkClusterLabel: clusterID,
		trinoBenchmarkOrgLabel:     orgID,
	}
}

// ownedSelector is the ONLY selector cleanup and status use. It requires both
// the app label and the cluster label, so a stray object carrying one of them
// by accident is still out of reach.
func (m *trinoBenchmarkManager) ownedSelector(clusterID string) string {
	return trinoBenchmarkAppLabelKey + "=" + trinoBenchmarkAppLabelValue + "," +
		trinoBenchmarkClusterLabel + "=" + clusterID
}

// ProvisionTrinoBenchmark creates the cluster, or converges onto an existing
// one with identical ownership and configuration. A same-named cluster with a
// different org, image, or worker count is a conflict.
func (m *trinoBenchmarkManager) ProvisionTrinoBenchmark(ctx context.Context, orgID string, request TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error) {
	clusterID := TrinoBenchmarkClusterID(orgID)
	workers := request.Workers
	if workers == 0 {
		workers = m.cfg.DefaultWorkers
	}
	if workers < 1 || workers > maxTrinoBenchmarkWorkers {
		return TrinoBenchmarkProvisionResult{}, fmt.Errorf(
			"%w: worker count %d is outside 1..%d", ErrTrinoBenchmarkInvalidRequest, workers, maxTrinoBenchmarkWorkers)
	}

	existing, err := m.clientset.CoreV1().Services(m.cfg.Namespace).Get(ctx, clusterID, metav1.GetOptions{})
	switch {
	case err == nil:
		if conflict := trinoBenchmarkOwnershipConflict(existing, orgID, m.cfg.Image, workers); conflict != nil {
			return TrinoBenchmarkProvisionResult{}, conflict
		}
		// Idempotent repeat: converge any object a previous attempt missed,
		// then report the cluster without claiming to have created it.
		if err := m.applyClusterResources(ctx, clusterID, orgID, workers, request.RunID); err != nil {
			return TrinoBenchmarkProvisionResult{}, err
		}
		cluster, err := m.TrinoBenchmarkStatus(ctx, clusterID)
		if err != nil {
			return TrinoBenchmarkProvisionResult{}, err
		}
		return TrinoBenchmarkProvisionResult{Cluster: cluster, Created: false}, nil
	case !apierrors.IsNotFound(err):
		return TrinoBenchmarkProvisionResult{}, fmt.Errorf("read Trino benchmark service %s: %w", clusterID, err)
	}

	if err := m.applyClusterResources(ctx, clusterID, orgID, workers, request.RunID); err != nil {
		return TrinoBenchmarkProvisionResult{}, err
	}
	slog.Info("Provisioned Trino benchmark cluster.",
		"cluster_id", clusterID, "org", orgID, "workers", workers, "image", m.cfg.Image)
	return TrinoBenchmarkProvisionResult{
		Cluster: TrinoBenchmarkCluster{
			ID:               clusterID,
			State:            TrinoBenchmarkStatePending,
			RequestedWorkers: workers,
			Image:            m.cfg.Image,
		},
		Created: true,
	}, nil
}

// applyClusterResources creates every object the cluster needs, treating an
// AlreadyExists as success. Ordering puts the Service (the ownership anchor)
// first so a crash mid-provision still leaves something cleanup can find.
func (m *trinoBenchmarkManager) applyClusterResources(ctx context.Context, clusterID, orgID string, workers int, runID string) error {
	identity, err := m.resolver.ResolveTrinoReader(ctx, orgID)
	if err != nil {
		// Fail closed. Nothing has been created at this point, and there is no
		// writer-credential fallback by design.
		return fmt.Errorf("resolve Trino reader identity for org %s: %w", orgID, err)
	}

	if err := m.applyService(ctx, clusterID, orgID, workers, runID); err != nil {
		return err
	}
	if err := m.applyReaderSecret(ctx, clusterID, orgID, identity); err != nil {
		return err
	}
	if err := m.applyConfigMaps(ctx, clusterID, orgID, identity); err != nil {
		return err
	}
	if err := m.applyDeployment(ctx, clusterID, orgID, trinoBenchmarkRoleCoordinator, 1); err != nil {
		return err
	}
	return m.applyDeployment(ctx, clusterID, orgID, trinoBenchmarkRoleWorker, workers)
}

func trinoBenchmarkOwnershipConflict(service *corev1.Service, orgID, image string, workers int) error {
	if got := service.Labels[trinoBenchmarkOrgLabel]; got != orgID {
		return fmt.Errorf("%w: cluster %s is owned by org %q, not %q",
			ErrTrinoBenchmarkConflict, service.Name, got, orgID)
	}
	if got := service.Annotations[trinoBenchmarkImageAnnotation]; got != image {
		return fmt.Errorf("%w: cluster %s runs image %q, not the configured pinned image",
			ErrTrinoBenchmarkConflict, service.Name, got)
	}
	if got := service.Annotations[trinoBenchmarkWorkersAnnotation]; got != strconv.Itoa(workers) {
		return fmt.Errorf("%w: cluster %s was provisioned with %q workers, not %d",
			ErrTrinoBenchmarkConflict, service.Name, got, workers)
	}
	return nil
}

func (m *trinoBenchmarkManager) applyService(ctx context.Context, clusterID, orgID string, workers int, runID string) error {
	labels := m.ownershipLabels(clusterID, orgID)
	selector := map[string]string{
		trinoBenchmarkClusterLabel: clusterID,
		trinoBenchmarkRoleLabel:    trinoBenchmarkRoleCoordinator,
	}
	annotations := map[string]string{
		trinoBenchmarkImageAnnotation:   m.cfg.Image,
		trinoBenchmarkWorkersAnnotation: strconv.Itoa(workers),
		trinoBenchmarkOwnerAnnotation:   m.cfg.ControlPlaneID,
	}
	if runID != "" {
		annotations[trinoBenchmarkRunIDAnnotation] = runID
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterID,
			Namespace:   m.cfg.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       trinoBenchmarkHTTPPort,
				TargetPort: intstr.FromInt32(trinoBenchmarkHTTPPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	_, err := m.clientset.CoreV1().Services(m.cfg.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create Trino benchmark service %s: %w", clusterID, err)
	}
	return nil
}

// applyReaderSecret copies the charts-created reader password into a
// short-lived, cluster-owned Secret. This is the ONLY point where a credential
// value exists in control-plane memory: it is read by exact reference and
// written straight into the Secret. It is never logged, returned, or stored
// on any struct.
func (m *trinoBenchmarkManager) applyReaderSecret(ctx context.Context, clusterID, orgID string, identity TrinoReaderIdentity) error {
	ref := identity.MetadataPasswordSecret
	source, err := m.clientset.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) {
			// The companion charts reader resources are not deployed (or the
			// RBAC grant is missing). Fail closed.
			return fmt.Errorf("%w: metadata reader password Secret %s is unavailable",
				ErrTrinoBenchmarkConfig, ref)
		}
		return fmt.Errorf("read metadata reader password Secret %s: %w", ref, err)
	}
	password, ok := source.Data[ref.Key]
	if !ok || len(password) == 0 {
		return fmt.Errorf("%w: metadata reader password Secret %s has no value", ErrTrinoBenchmarkConfig, ref)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterID + "-metadata",
			Namespace: m.cfg.Namespace,
			Labels:    m.ownershipLabels(clusterID, orgID),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{trinoBenchmarkSecretPasswordKey: password},
	}
	_, err = m.clientset.CoreV1().Secrets(m.cfg.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		// Never wrap the Secret object here: %w on a create error is fine, but
		// the object itself must not reach a log line.
		return fmt.Errorf("create Trino benchmark metadata Secret for cluster %s: %w", clusterID, err)
	}
	return nil
}

func (m *trinoBenchmarkManager) applyConfigMaps(ctx context.Context, clusterID, orgID string, identity TrinoReaderIdentity) error {
	discovery := m.endpoint(clusterID)
	configMaps := []*corev1.ConfigMap{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterID + "-coordinator-config",
				Namespace: m.cfg.Namespace,
				Labels:    m.ownershipLabels(clusterID, orgID),
			},
			Data: map[string]string{
				"config.properties": renderTrinoServerConfig(trinoBenchmarkRoleCoordinator, discovery),
				"node.properties":   renderTrinoNodeProperties(clusterID),
				"jvm.config":        renderTrinoJVMConfig(m.cfg.CoordinatorMemory),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterID + "-worker-config",
				Namespace: m.cfg.Namespace,
				Labels:    m.ownershipLabels(clusterID, orgID),
			},
			Data: map[string]string{
				"config.properties": renderTrinoServerConfig(trinoBenchmarkRoleWorker, discovery),
				"node.properties":   renderTrinoNodeProperties(clusterID),
				"jvm.config":        renderTrinoJVMConfig(m.cfg.WorkerMemory),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterID + "-catalog",
				Namespace: m.cfg.Namespace,
				Labels:    m.ownershipLabels(clusterID, orgID),
			},
			Data: map[string]string{
				trinoBenchmarkCatalogName + ".properties": renderTrinoCatalogProperties(identity, clusterID),
			},
		},
	}
	for _, cm := range configMaps {
		_, err := m.clientset.CoreV1().ConfigMaps(m.cfg.Namespace).Create(ctx, cm, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create Trino benchmark ConfigMap %s: %w", cm.Name, err)
		}
	}
	return nil
}

func (m *trinoBenchmarkManager) applyDeployment(ctx context.Context, clusterID, orgID, role string, replicas int) error {
	cpu, memory := m.cfg.WorkerCPU, m.cfg.WorkerMemory
	configMapName := clusterID + "-worker-config"
	if role == trinoBenchmarkRoleCoordinator {
		cpu, memory = m.cfg.CoordinatorCPU, m.cfg.CoordinatorMemory
		configMapName = clusterID + "-coordinator-config"
	}
	cpuQuantity, err := resource.ParseQuantity(cpu)
	if err != nil {
		return fmt.Errorf("%w: invalid Trino %s CPU %q", ErrTrinoBenchmarkConfig, role, cpu)
	}
	memoryQuantity, err := resource.ParseQuantity(memory)
	if err != nil {
		return fmt.Errorf("%w: invalid Trino %s memory %q", ErrTrinoBenchmarkConfig, role, memory)
	}
	// requests == limits: Guaranteed QoS, so a benchmark pod's numbers are not
	// a function of whatever else happens to share the node.
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuQuantity,
			corev1.ResourceMemory: memoryQuantity,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuQuantity.DeepCopy(),
			corev1.ResourceMemory: memoryQuantity.DeepCopy(),
		},
	}

	labels := m.ownershipLabels(clusterID, orgID)
	podLabels := map[string]string{trinoBenchmarkRoleLabel: role}
	for k, v := range labels {
		podLabels[k] = v
	}
	selector := map[string]string{
		trinoBenchmarkClusterLabel: clusterID,
		trinoBenchmarkRoleLabel:    role,
	}
	replicaCount := int32(replicas)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterID + "-" + role,
			Namespace: m.cfg.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicaCount,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ServiceAccountName: m.cfg.ServiceAccount,
					Containers: []corev1.Container{{
						Name:            "trino",
						Image:           m.cfg.Image,
						ImagePullPolicy: corev1.PullPolicy(m.cfg.ImagePullPolicy),
						Ports: []corev1.ContainerPort{{
							Name:          "http",
							ContainerPort: trinoBenchmarkHTTPPort,
							Protocol:      corev1.ProtocolTCP,
						}},
						Env: []corev1.EnvVar{{
							// By reference only — the value never appears in a
							// pod spec.
							Name: trinoBenchmarkPasswordEnv,
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: clusterID + "-metadata"},
									Key:                  trinoBenchmarkSecretPasswordKey,
								},
							},
						}},
						Resources: resources,
						VolumeMounts: []corev1.VolumeMount{
							{Name: "trino-config", MountPath: "/etc/trino/config.properties", SubPath: "config.properties"},
							{Name: "trino-config", MountPath: "/etc/trino/node.properties", SubPath: "node.properties"},
							{Name: "trino-config", MountPath: "/etc/trino/jvm.config", SubPath: "jvm.config"},
							{Name: "trino-catalog", MountPath: "/etc/trino/catalog"},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPtr(false),
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "trino-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
								},
							},
						},
						{
							Name: "trino-catalog",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: clusterID + "-catalog"},
								},
							},
						},
					},
				},
			},
		},
	}
	_, err = m.clientset.AppsV1().Deployments(m.cfg.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create Trino benchmark deployment %s: %w", deployment.Name, err)
	}
	return nil
}

// TrinoBenchmarkStatus reports ready ONLY when the coordinator is ready AND the
// full requested worker replica count is ready — a four-worker comparison run
// against three workers is not the benchmark that was asked for.
func (m *trinoBenchmarkManager) TrinoBenchmarkStatus(ctx context.Context, clusterID string) (TrinoBenchmarkCluster, error) {
	service, err := m.clientset.CoreV1().Services(m.cfg.Namespace).Get(ctx, clusterID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return TrinoBenchmarkCluster{}, fmt.Errorf("%w: %s", ErrTrinoBenchmarkNotFound, clusterID)
	}
	if err != nil {
		return TrinoBenchmarkCluster{}, fmt.Errorf("read Trino benchmark service %s: %w", clusterID, err)
	}
	if service.Labels[trinoBenchmarkAppLabelKey] != trinoBenchmarkAppLabelValue {
		// Same name, not ours. Never report on something we do not own.
		return TrinoBenchmarkCluster{}, fmt.Errorf("%w: %s", ErrTrinoBenchmarkNotFound, clusterID)
	}

	requested, _ := strconv.Atoi(service.Annotations[trinoBenchmarkWorkersAnnotation])
	cluster := TrinoBenchmarkCluster{
		ID:               clusterID,
		State:            TrinoBenchmarkStatePending,
		RequestedWorkers: requested,
		Image:            service.Annotations[trinoBenchmarkImageAnnotation],
	}

	coordinator, coordinatorErr := m.clientset.AppsV1().Deployments(m.cfg.Namespace).Get(ctx, clusterID+"-coordinator", metav1.GetOptions{})
	worker, workerErr := m.clientset.AppsV1().Deployments(m.cfg.Namespace).Get(ctx, clusterID+"-worker", metav1.GetOptions{})
	for _, err := range []error{coordinatorErr, workerErr} {
		if err != nil && !apierrors.IsNotFound(err) {
			return TrinoBenchmarkCluster{}, fmt.Errorf("read Trino benchmark deployments for %s: %w", clusterID, err)
		}
	}
	if workerErr == nil {
		cluster.ReadyWorkers = int(worker.Status.ReadyReplicas)
	}

	// Terminal failure short-circuits polling: a deployment that blew its
	// progress deadline or cannot create replicas will not recover on its own.
	if (coordinatorErr == nil && trinoDeploymentFailed(coordinator)) || (workerErr == nil && trinoDeploymentFailed(worker)) {
		cluster.State = TrinoBenchmarkStateFailed
		return cluster, nil
	}
	if coordinatorErr != nil || workerErr != nil {
		// A partial provision: still converging (or awaiting cleanup).
		return cluster, nil
	}
	if coordinator.Status.ReadyReplicas >= 1 && requested > 0 && cluster.ReadyWorkers >= requested {
		cluster.State = TrinoBenchmarkStateReady
		cluster.Endpoint = m.endpoint(clusterID)
	}
	return cluster, nil
}

func trinoDeploymentFailed(deployment *appsv1.Deployment) bool {
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == "ProgressDeadlineExceeded" {
			return true
		}
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// DeprovisionTrinoBenchmark deletes exactly the objects labelled as owned by
// clusterID. It is idempotent, safe after a partial provision, and never
// touches the charts-created reader Secret (different namespace, and no
// ownership labels).
func (m *trinoBenchmarkManager) DeprovisionTrinoBenchmark(ctx context.Context, clusterID string) error {
	selector := metav1.ListOptions{LabelSelector: m.ownedSelector(clusterID)}
	var errs []string

	deployments, err := m.clientset.AppsV1().Deployments(m.cfg.Namespace).List(ctx, selector)
	if err != nil {
		errs = append(errs, fmt.Sprintf("list deployments: %v", err))
	} else {
		for _, item := range deployments.Items {
			if err := m.clientset.AppsV1().Deployments(m.cfg.Namespace).Delete(ctx, item.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Sprintf("delete deployment %s: %v", item.Name, err))
			}
		}
	}

	services, err := m.clientset.CoreV1().Services(m.cfg.Namespace).List(ctx, selector)
	if err != nil {
		errs = append(errs, fmt.Sprintf("list services: %v", err))
	} else {
		for _, item := range services.Items {
			if err := m.clientset.CoreV1().Services(m.cfg.Namespace).Delete(ctx, item.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Sprintf("delete service %s: %v", item.Name, err))
			}
		}
	}

	configMaps, err := m.clientset.CoreV1().ConfigMaps(m.cfg.Namespace).List(ctx, selector)
	if err != nil {
		errs = append(errs, fmt.Sprintf("list configmaps: %v", err))
	} else {
		for _, item := range configMaps.Items {
			if err := m.clientset.CoreV1().ConfigMaps(m.cfg.Namespace).Delete(ctx, item.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Sprintf("delete configmap %s: %v", item.Name, err))
			}
		}
	}

	// The short-lived credential Secret is deleted LAST so an earlier failure
	// still leaves it reachable for a retry, and its deletion is the step the
	// control plane most needs to be sure about.
	secrets, err := m.clientset.CoreV1().Secrets(m.cfg.Namespace).List(ctx, selector)
	if err != nil {
		errs = append(errs, fmt.Sprintf("list secrets: %v", err))
	} else {
		for _, item := range secrets.Items {
			if err := m.clientset.CoreV1().Secrets(m.cfg.Namespace).Delete(ctx, item.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Sprintf("delete secret %s: %v", item.Name, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("deprovision Trino benchmark cluster %s: %s", clusterID, strings.Join(errs, "; "))
	}
	slog.Info("Deprovisioned Trino benchmark cluster.", "cluster_id", clusterID)
	return nil
}

// renderTrinoServerConfig produces config.properties for one role. Workers and
// the coordinator share one discovery URI (the coordinator Service), which is
// what makes this a real multi-node cluster rather than four isolated nodes;
// the coordinator is excluded from scheduling so worker parallelism is what is
// actually measured.
func renderTrinoServerConfig(role, discoveryURI string) string {
	lines := []string{
		"http-server.http.port=" + strconv.Itoa(trinoBenchmarkHTTPPort),
		"discovery.uri=" + discoveryURI,
	}
	if role == trinoBenchmarkRoleCoordinator {
		lines = append([]string{
			"coordinator=true",
			"node-scheduler.include-coordinator=false",
		}, lines...)
	} else {
		lines = append([]string{"coordinator=false"}, lines...)
	}
	return strings.Join(lines, "\n") + "\n"
}

// renderTrinoNodeProperties pins the environment name to the cluster so nodes
// from two benchmark clusters can never join each other's discovery.
func renderTrinoNodeProperties(clusterID string) string {
	return "node.environment=" + trinoNodeEnvironment(clusterID) + "\n"
}

// trinoNodeEnvironment sanitizes the cluster ID into Trino's node.environment
// alphabet (lowercase alphanumeric and underscore).
func trinoNodeEnvironment(clusterID string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(clusterID) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// renderTrinoJVMConfig sizes the heap at ~70% of the container memory limit and
// pins the JVM to UTC, so Trino and Duckgres interpret the same TIMESTAMPTZ
// predicates identically.
func renderTrinoJVMConfig(memory string) string {
	heapMB := 4096
	if quantity, err := resource.ParseQuantity(memory); err == nil {
		if mb := quantity.Value() / (1 << 20) * 70 / 100; mb > 512 {
			heapMB = int(mb)
		}
	}
	return strings.Join([]string{
		"-server",
		fmt.Sprintf("-Xmx%dM", heapMB),
		"-XX:+UseG1GC",
		"-XX:G1HeapRegionSize=32M",
		"-XX:+ExplicitGCInvokesConcurrent",
		"-XX:+ExitOnOutOfMemoryError",
		"-XX:-OmitStackTraceInFastThrow",
		"-XX:ReservedCodeCacheSize=512M",
		"-Djdk.attach.allowAttachSelf=true",
		"-Dfile.encoding=UTF-8",
		// The benchmark compares UTC results across engines; a JVM default
		// timezone would silently shift date_trunc and partition predicates.
		"-Duser.timezone=UTC",
	}, "\n") + "\n"
}

// renderTrinoCatalogProperties configures the Brikk DuckLake connector against
// the warehouse's own metadata Postgres and S3 data path, using ONLY the
// charts-created read-only identity:
//
//   - the metadata password is interpolated from the env var backed by the
//     short-lived Secret, never written here; and
//   - S3 access is an assumed IAM role (renewable credentials), never a static
//     access key, and never the tenant's writer role.
func renderTrinoCatalogProperties(identity TrinoReaderIdentity, clusterID string) string {
	return strings.Join([]string{
		"connector.name=ducklake",
		"ducklake.catalog.database-url=" + identity.JDBCURL(),
		"ducklake.catalog.database-user=" + identity.MetadataUser,
		"ducklake.catalog.database-password=${ENV:" + trinoBenchmarkPasswordEnv + "}",
		"ducklake.data-path=" + identity.DataPath,
		"fs.native-s3.enabled=true",
		"s3.region=" + identity.Region,
		"s3.iam-role=" + identity.ReadOnlyRoleARN,
		"s3.role-session-name=" + clusterID,
	}, "\n") + "\n"
}
