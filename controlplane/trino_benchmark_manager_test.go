//go:build kubernetes

package controlplane

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	trinoTestNamespace     = "duckgres"
	trinoTestImage         = "123456789012.dkr.ecr.us-east-1.amazonaws.com/trino-brikk@sha256:0123456789abcdef"
	trinoTestReaderSecret  = "duckling-bench-org-trino-reader"
	trinoTestReaderPass    = "reader-password-never-logged"
	trinoTestWriterRoleARN = "arn:aws:iam::123456789012:role/duckling-bench-org"
)

// fakeTrinoReaderResolver stands in for the charts-backed resolver.
type fakeTrinoReaderResolver struct {
	identity TrinoReaderIdentity
	err      error
	calls    int
}

func (r *fakeTrinoReaderResolver) ResolveTrinoReader(context.Context, string) (TrinoReaderIdentity, error) {
	r.calls++
	return r.identity, r.err
}

func testTrinoReaderIdentity(t *testing.T) TrinoReaderIdentity {
	t.Helper()
	identity, err := buildTrinoReaderIdentity(TrinoReaderSource{
		MetadataEndpoint: "duckling-bench-org-pgbouncer.ducklings.svc.cluster.local:6432",
		MetadataDatabase: "ducklake_bench_org",
		MetadataUser:     "trino_reader_bench_org",
		MetadataPasswordSecret: TrinoReaderSecretRef{
			Name: trinoTestReaderSecret, Namespace: "ducklings", Key: "password",
		},
		Bucket:          "posthog-duckling-benchorg-dev",
		Region:          "us-east-1",
		ReadOnlyRoleARN: "arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader",
		SSLMode:         "disable",
		WriterRoleARN:   trinoTestWriterRoleARN,
		WriterUser:      "ducklake_bench_org",
	})
	if err != nil {
		t.Fatalf("build test reader identity: %v", err)
	}
	return identity
}

func newTrinoBenchmarkTestManager(t *testing.T, objects ...runtime.Object) (*trinoBenchmarkManager, kubernetes.Interface, *fakeTrinoReaderResolver) {
	t.Helper()
	// The charts-created reader Secret lives in the ducklings namespace; the
	// control plane may read it by exact name and nothing else.
	seeded := []runtime.Object{
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: trinoTestReaderSecret, Namespace: "ducklings"},
			Data:       map[string][]byte{"password": []byte(trinoTestReaderPass)},
		},
	}
	seeded = append(seeded, objects...)
	clientset := fake.NewSimpleClientset(seeded...)
	resolver := &fakeTrinoReaderResolver{identity: testTrinoReaderIdentity(t)}
	manager, err := newTrinoBenchmarkManager(clientset, resolver, TrinoBenchmarkManagerConfig{
		Namespace:      trinoTestNamespace,
		Image:          trinoTestImage,
		ControlPlaneID: "duckgres-control-plane-0",
	})
	if err != nil {
		t.Fatalf("newTrinoBenchmarkManager: %v", err)
	}
	return manager, clientset, resolver
}

func TestTrinoBenchmarkManagerRequiresPinnedImage(t *testing.T) {
	_, err := newTrinoBenchmarkManager(fake.NewSimpleClientset(), &fakeTrinoReaderResolver{}, TrinoBenchmarkManagerConfig{
		Namespace: trinoTestNamespace,
	})
	if !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig for a missing pinned image", err)
	}
}

func TestTrinoBenchmarkManagerRequiresResolver(t *testing.T) {
	_, err := newTrinoBenchmarkManager(fake.NewSimpleClientset(), nil, TrinoBenchmarkManagerConfig{
		Namespace: trinoTestNamespace, Image: trinoTestImage,
	})
	if !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig without a reader resolver", err)
	}
}

func TestTrinoBenchmarkManagerProvisionRendersCoordinatorAndRequestedWorkers(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 4, RunID: "run-1"})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if !result.Created {
		t.Fatal("first provision should report Created")
	}
	clusterID := result.Cluster.ID
	if clusterID != "trino-bench-bench-org" {
		t.Fatalf("cluster id = %q", clusterID)
	}
	if result.Cluster.State != TrinoBenchmarkStatePending {
		t.Fatalf("state = %q, want pending immediately after provision", result.Cluster.State)
	}
	if result.Cluster.RequestedWorkers != 4 {
		t.Fatalf("requested workers = %d", result.Cluster.RequestedWorkers)
	}

	coordinator, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(context.Background(), clusterID+"-coordinator", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get coordinator: %v", err)
	}
	if coordinator.Spec.Replicas == nil || *coordinator.Spec.Replicas != 1 {
		t.Fatalf("coordinator replicas = %v, want exactly 1", coordinator.Spec.Replicas)
	}
	worker, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(context.Background(), clusterID+"-worker", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workers: %v", err)
	}
	if worker.Spec.Replicas == nil || *worker.Spec.Replicas != 4 {
		t.Fatalf("worker replicas = %v, want exactly the requested 4", worker.Spec.Replicas)
	}

	// The Service selects ONLY the coordinator: a client statement must never
	// land on a worker.
	service, err := clientset.CoreV1().Services(trinoTestNamespace).Get(context.Background(), clusterID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", service.Spec.Type)
	}
	if service.Spec.Selector[trinoBenchmarkRoleLabel] != trinoBenchmarkRoleCoordinator {
		t.Fatalf("service selector = %v, want the coordinator role", service.Spec.Selector)
	}
	if service.Spec.Selector[trinoBenchmarkClusterLabel] != clusterID {
		t.Fatalf("service selector = %v, want the cluster label", service.Spec.Selector)
	}

	// Every object carries the ownership labels cleanup keys off.
	for _, labels := range []map[string]string{
		coordinator.Labels, worker.Labels, service.Labels,
	} {
		if labels[trinoBenchmarkClusterLabel] != clusterID {
			t.Fatalf("labels = %v missing the cluster label", labels)
		}
		if labels[trinoBenchmarkOrgLabel] != "bench-org" {
			t.Fatalf("labels = %v missing the org label", labels)
		}
		if labels[trinoBenchmarkAppLabelKey] != trinoBenchmarkAppLabelValue {
			t.Fatalf("labels = %v missing the app label", labels)
		}
	}
}

func TestTrinoBenchmarkManagerDefaultsToFourWorkers(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if result.Cluster.RequestedWorkers != defaultTrinoBenchmarkWorkers {
		t.Fatalf("requested workers = %d, want the %d default", result.Cluster.RequestedWorkers, defaultTrinoBenchmarkWorkers)
	}
	worker, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(context.Background(), result.Cluster.ID+"-worker", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workers: %v", err)
	}
	if *worker.Spec.Replicas != int32(defaultTrinoBenchmarkWorkers) {
		t.Fatalf("worker replicas = %d, want %d", *worker.Spec.Replicas, defaultTrinoBenchmarkWorkers)
	}
}

func TestTrinoBenchmarkManagerPinsImageAndSetsExplicitResources(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 2})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if result.Cluster.Image != trinoTestImage {
		t.Fatalf("reported image = %q, want the pinned image", result.Cluster.Image)
	}

	for _, name := range []string{result.Cluster.ID + "-coordinator", result.Cluster.ID + "-worker"} {
		deployment, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		container := deployment.Spec.Template.Spec.Containers[0]
		if container.Image != trinoTestImage {
			t.Fatalf("%s image = %q, want the pinned image", name, container.Image)
		}
		if container.Resources.Requests.Cpu().IsZero() || container.Resources.Requests.Memory().IsZero() {
			t.Fatalf("%s has no explicit CPU/memory requests: %v", name, container.Resources.Requests)
		}
		if container.Resources.Limits.Cpu().IsZero() || container.Resources.Limits.Memory().IsZero() {
			t.Fatalf("%s has no explicit CPU/memory limits: %v", name, container.Resources.Limits)
		}
	}
}

func TestTrinoBenchmarkManagerConfiguresMultiNodeDiscoveryAndUTC(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID
	discovery := "http://" + clusterID + "." + trinoTestNamespace + ".svc.cluster.local:8080"

	coordinatorCM, err := clientset.CoreV1().ConfigMaps(trinoTestNamespace).Get(context.Background(), clusterID+"-coordinator-config", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get coordinator config: %v", err)
	}
	coordinatorConfig := coordinatorCM.Data["config.properties"]
	for _, want := range []string{
		"coordinator=true",
		"node-scheduler.include-coordinator=false",
		"discovery.uri=" + discovery,
	} {
		if !strings.Contains(coordinatorConfig, want) {
			t.Fatalf("coordinator config.properties missing %q:\n%s", want, coordinatorConfig)
		}
	}
	if !strings.Contains(coordinatorCM.Data["jvm.config"], "-Duser.timezone=UTC") {
		t.Fatalf("coordinator jvm.config must pin UTC:\n%s", coordinatorCM.Data["jvm.config"])
	}

	workerCM, err := clientset.CoreV1().ConfigMaps(trinoTestNamespace).Get(context.Background(), clusterID+"-worker-config", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker config: %v", err)
	}
	workerConfig := workerCM.Data["config.properties"]
	if !strings.Contains(workerConfig, "coordinator=false") {
		t.Fatalf("worker config.properties must not declare a coordinator:\n%s", workerConfig)
	}
	if !strings.Contains(workerConfig, "discovery.uri="+discovery) {
		t.Fatalf("worker config.properties must point at the coordinator Service:\n%s", workerConfig)
	}
	if !strings.Contains(workerCM.Data["jvm.config"], "-Duser.timezone=UTC") {
		t.Fatalf("worker jvm.config must pin UTC:\n%s", workerCM.Data["jvm.config"])
	}
}

func TestTrinoBenchmarkManagerWiresReadOnlyRoleAndReaderSecret(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 2})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID

	catalogCM, err := clientset.CoreV1().ConfigMaps(trinoTestNamespace).Get(context.Background(), clusterID+"-catalog", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get catalog config: %v", err)
	}
	catalog := catalogCM.Data["ducklake.properties"]
	for _, want := range []string{
		"connector.name=ducklake",
		"ducklake.catalog.database-url=jdbc:postgresql://duckling-bench-org-pgbouncer.ducklings.svc.cluster.local:6432/ducklake_bench_org?sslmode=disable",
		"ducklake.catalog.database-user=trino_reader_bench_org",
		"ducklake.catalog.database-password=${ENV:TRINO_DUCKLAKE_DB_PASSWORD}",
		"ducklake.data-path=s3://posthog-duckling-benchorg-dev/",
		"fs.native-s3.enabled=true",
		"s3.region=us-east-1",
		"s3.iam-role=arn:aws:iam::123456789012:role/duckling-bench-org-trino-reader",
	} {
		if !strings.Contains(catalog, want) {
			t.Fatalf("catalog properties missing %q:\n%s", want, catalog)
		}
	}
	// Static long-lived keys would defeat the renewable read-only identity.
	for _, banned := range []string{"s3.aws-access-key", "s3.aws-secret-key", "s3.session-token"} {
		if strings.Contains(catalog, banned) {
			t.Fatalf("catalog properties must not carry static S3 credentials (%q):\n%s", banned, catalog)
		}
	}
	// The password lives in the short-lived Secret, never in the ConfigMap.
	if strings.Contains(catalog, trinoTestReaderPass) {
		t.Fatal("catalog ConfigMap contains the reader password")
	}

	secret, err := clientset.CoreV1().Secrets(trinoTestNamespace).Get(context.Background(), clusterID+"-metadata", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get benchmark secret: %v", err)
	}
	if string(secret.Data[trinoBenchmarkSecretPasswordKey]) != trinoTestReaderPass {
		t.Fatal("benchmark Secret does not carry the charts-created reader password")
	}
	if len(secret.Data) != 1 {
		t.Fatalf("benchmark Secret carries %d keys, want only the reader password", len(secret.Data))
	}
	if secret.Labels[trinoBenchmarkClusterLabel] != clusterID {
		t.Fatal("benchmark Secret is not owned by the cluster")
	}

	// Both roles read the password through a secretKeyRef, never a literal.
	for _, name := range []string{clusterID + "-coordinator", clusterID + "-worker"} {
		deployment, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		var found bool
		for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name != "TRINO_DUCKLAKE_DB_PASSWORD" {
				continue
			}
			found = true
			if env.Value != "" {
				t.Fatalf("%s passes the reader password by value", name)
			}
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				t.Fatalf("%s does not use a secretKeyRef for the reader password", name)
			}
			if env.ValueFrom.SecretKeyRef.Name != clusterID+"-metadata" {
				t.Fatalf("%s reads the wrong Secret %q", name, env.ValueFrom.SecretKeyRef.Name)
			}
		}
		if !found {
			t.Fatalf("%s has no reader password env var", name)
		}
	}
}

func TestTrinoBenchmarkManagerRendersNoWriterCredentials(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)

	result, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 2})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID

	configMaps, err := clientset.CoreV1().ConfigMaps(trinoTestNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: trinoBenchmarkClusterLabel + "=" + clusterID,
	})
	if err != nil {
		t.Fatalf("list config maps: %v", err)
	}
	if len(configMaps.Items) == 0 {
		t.Fatal("expected the cluster's ConfigMaps")
	}
	// Compare property VALUES exactly: the reader ARN legitimately has the
	// writer ARN as a prefix, so a substring check would be meaningless here.
	for _, cm := range configMaps.Items {
		for key, value := range cm.Data {
			for _, line := range strings.Split(value, "\n") {
				name, propertyValue, ok := strings.Cut(strings.TrimSpace(line), "=")
				if !ok {
					continue
				}
				if propertyValue == trinoTestWriterRoleARN {
					t.Fatalf("%s/%s property %s uses the warehouse WRITER role", cm.Name, key, name)
				}
				// The DuckLake writer login is the org's own catalog role.
				if name == "ducklake.catalog.database-user" && propertyValue == "ducklake_bench_org" {
					t.Fatalf("%s/%s uses the warehouse writer database user", cm.Name, key)
				}
			}
		}
	}
}

func TestTrinoBenchmarkManagerFailsClosedWithoutReaderIdentity(t *testing.T) {
	manager, clientset, resolver := newTrinoBenchmarkTestManager(t)
	resolver.err = ErrTrinoBenchmarkConfig

	_, err := manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig", err)
	}
	deployments, err := clientset.AppsV1().Deployments(trinoTestNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deployments.Items) != 0 {
		t.Fatalf("a fail-closed provision created %d deployments", len(deployments.Items))
	}
}

func TestTrinoBenchmarkManagerFailsClosedWhenReaderSecretIsAbsent(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	manager, err := newTrinoBenchmarkManager(clientset, &fakeTrinoReaderResolver{identity: testTrinoReaderIdentity(t)}, TrinoBenchmarkManagerConfig{
		Namespace: trinoTestNamespace, Image: trinoTestImage,
	})
	if err != nil {
		t.Fatalf("newTrinoBenchmarkManager: %v", err)
	}

	_, err = manager.ProvisionTrinoBenchmark(context.Background(), "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if !errors.Is(err, ErrTrinoBenchmarkConfig) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkConfig when the charts Secret is missing", err)
	}
}

func TestTrinoBenchmarkManagerProvisionIsIdempotent(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()

	first, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	second, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if err != nil {
		t.Fatalf("second provision: %v", err)
	}
	if second.Created {
		t.Fatal("second provision must not report Created")
	}
	if second.Cluster.ID != first.Cluster.ID {
		t.Fatalf("cluster id changed: %q -> %q", first.Cluster.ID, second.Cluster.ID)
	}

	deployments, err := clientset.AppsV1().Deployments(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list deployments: %v", err)
	}
	if len(deployments.Items) != 2 {
		t.Fatalf("deployments = %d, want exactly the coordinator and worker", len(deployments.Items))
	}
}

func TestTrinoBenchmarkManagerRejectsConflictingOwnershipOrConfiguration(t *testing.T) {
	ctx := context.Background()

	t.Run("different worker count", func(t *testing.T) {
		manager, _, _ := newTrinoBenchmarkTestManager(t)
		if _, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4}); err != nil {
			t.Fatalf("provision: %v", err)
		}
		_, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 2})
		if !errors.Is(err, ErrTrinoBenchmarkConflict) {
			t.Fatalf("error = %v, want ErrTrinoBenchmarkConflict", err)
		}
	})

	t.Run("different owning org", func(t *testing.T) {
		manager, clientset, _ := newTrinoBenchmarkTestManager(t)
		if _, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4}); err != nil {
			t.Fatalf("provision: %v", err)
		}
		service, err := clientset.CoreV1().Services(trinoTestNamespace).Get(ctx, "trino-bench-bench-org", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get service: %v", err)
		}
		service.Labels[trinoBenchmarkOrgLabel] = "someone-else"
		if _, err := clientset.CoreV1().Services(trinoTestNamespace).Update(ctx, service, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update service: %v", err)
		}
		_, err = manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
		if !errors.Is(err, ErrTrinoBenchmarkConflict) {
			t.Fatalf("error = %v, want ErrTrinoBenchmarkConflict", err)
		}
	})

	t.Run("different pinned image", func(t *testing.T) {
		manager, clientset, resolver := newTrinoBenchmarkTestManager(t)
		if _, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4}); err != nil {
			t.Fatalf("provision: %v", err)
		}
		repinned, err := newTrinoBenchmarkManager(clientset, resolver, TrinoBenchmarkManagerConfig{
			Namespace: trinoTestNamespace, Image: "registry.example/trino-brikk@sha256:beef",
		})
		if err != nil {
			t.Fatalf("newTrinoBenchmarkManager: %v", err)
		}
		_, err = repinned.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
		if !errors.Is(err, ErrTrinoBenchmarkConflict) {
			t.Fatalf("error = %v, want ErrTrinoBenchmarkConflict on an image change", err)
		}
	})
}

func TestTrinoBenchmarkManagerStatusRequiresCoordinatorAndEveryWorker(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()

	result, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID

	pending, err := manager.TrinoBenchmarkStatus(ctx, clusterID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if pending.State != TrinoBenchmarkStatePending || pending.Endpoint != "" {
		t.Fatalf("status = %+v, want pending with no endpoint", pending)
	}

	setTrinoDeploymentReady(t, clientset, clusterID+"-coordinator", 1)
	setTrinoDeploymentReady(t, clientset, clusterID+"-worker", 3)
	partial, err := manager.TrinoBenchmarkStatus(ctx, clusterID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if partial.State != TrinoBenchmarkStatePending {
		t.Fatalf("state = %q with 3/4 workers ready, want pending", partial.State)
	}
	if partial.ReadyWorkers != 3 || partial.RequestedWorkers != 4 {
		t.Fatalf("worker counts = %d/%d", partial.ReadyWorkers, partial.RequestedWorkers)
	}

	setTrinoDeploymentReady(t, clientset, clusterID+"-worker", 4)
	ready, err := manager.TrinoBenchmarkStatus(ctx, clusterID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if ready.State != TrinoBenchmarkStateReady {
		t.Fatalf("state = %q with all workers ready, want ready", ready.State)
	}
	if ready.Endpoint != "http://"+clusterID+"."+trinoTestNamespace+".svc.cluster.local:8080" {
		t.Fatalf("endpoint = %q", ready.Endpoint)
	}
	if ready.Image != trinoTestImage {
		t.Fatalf("image = %q, want the pinned image recorded for artifacts", ready.Image)
	}
}

func TestTrinoBenchmarkManagerStatusReportsTerminalFailure(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()

	result, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 2})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID

	deployment, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(ctx, clusterID+"-worker", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get workers: %v", err)
	}
	deployment.Status.Conditions = []appsv1.DeploymentCondition{{
		Type:   appsv1.DeploymentProgressing,
		Status: corev1.ConditionFalse,
		Reason: "ProgressDeadlineExceeded",
	}}
	if _, err := clientset.AppsV1().Deployments(trinoTestNamespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}

	status, err := manager.TrinoBenchmarkStatus(ctx, clusterID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.State != TrinoBenchmarkStateFailed {
		t.Fatalf("state = %q, want failed", status.State)
	}
}

func TestTrinoBenchmarkManagerStatusReportsNotFound(t *testing.T) {
	manager, _, _ := newTrinoBenchmarkTestManager(t)

	_, err := manager.TrinoBenchmarkStatus(context.Background(), "trino-bench-missing")
	if !errors.Is(err, ErrTrinoBenchmarkNotFound) {
		t.Fatalf("error = %v, want ErrTrinoBenchmarkNotFound", err)
	}
}

func TestTrinoBenchmarkManagerCleanupIsIdempotentAndDeletesEverythingItOwns(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()

	result, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 4})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	clusterID := result.Cluster.ID

	for i := 0; i < 2; i++ {
		if err := manager.DeprovisionTrinoBenchmark(ctx, clusterID); err != nil {
			t.Fatalf("deprovision %d: %v", i, err)
		}
	}

	deployments, _ := clientset.AppsV1().Deployments(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	services, _ := clientset.CoreV1().Services(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	configMaps, _ := clientset.CoreV1().ConfigMaps(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	secrets, _ := clientset.CoreV1().Secrets(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	if len(deployments.Items) != 0 || len(services.Items) != 0 || len(configMaps.Items) != 0 || len(secrets.Items) != 0 {
		t.Fatalf("cleanup left resources: deployments=%d services=%d configmaps=%d secrets=%d",
			len(deployments.Items), len(services.Items), len(configMaps.Items), len(secrets.Items))
	}
}

func TestTrinoBenchmarkManagerCleanupIsSafeAfterPartialProvision(t *testing.T) {
	manager, clientset, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()

	// Simulate a provision that died after the Service and catalog ConfigMap.
	if _, err := clientset.CoreV1().Services(trinoTestNamespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trino-bench-bench-org",
			Namespace: trinoTestNamespace,
			Labels: map[string]string{
				trinoBenchmarkAppLabelKey:  trinoBenchmarkAppLabelValue,
				trinoBenchmarkClusterLabel: "trino-bench-bench-org",
				trinoBenchmarkOrgLabel:     "bench-org",
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed partial service: %v", err)
	}

	if err := manager.DeprovisionTrinoBenchmark(ctx, "trino-bench-bench-org"); err != nil {
		t.Fatalf("deprovision after partial provision: %v", err)
	}
	services, _ := clientset.CoreV1().Services(trinoTestNamespace).List(ctx, metav1.ListOptions{})
	if len(services.Items) != 0 {
		t.Fatalf("partial-provision cleanup left %d services", len(services.Items))
	}
}

func TestTrinoBenchmarkManagerCleanupNeverTouchesUnownedResources(t *testing.T) {
	foreignService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "duckgres-control-plane",
			Namespace: trinoTestNamespace,
			Labels:    map[string]string{"app": "duckgres-control-plane"},
		},
	}
	otherCluster := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "trino-bench-other-org-catalog",
			Namespace: trinoTestNamespace,
			Labels: map[string]string{
				trinoBenchmarkAppLabelKey:  trinoBenchmarkAppLabelValue,
				trinoBenchmarkClusterLabel: "trino-bench-other-org",
				trinoBenchmarkOrgLabel:     "other-org",
			},
		},
	}
	manager, clientset, _ := newTrinoBenchmarkTestManager(t, foreignService, otherCluster)
	ctx := context.Background()

	if _, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 2}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if err := manager.DeprovisionTrinoBenchmark(ctx, "trino-bench-bench-org"); err != nil {
		t.Fatalf("deprovision: %v", err)
	}

	if _, err := clientset.CoreV1().Services(trinoTestNamespace).Get(ctx, "duckgres-control-plane", metav1.GetOptions{}); err != nil {
		t.Fatalf("cleanup deleted an unrelated Service: %v", err)
	}
	if _, err := clientset.CoreV1().ConfigMaps(trinoTestNamespace).Get(ctx, "trino-bench-other-org-catalog", metav1.GetOptions{}); err != nil {
		t.Fatalf("cleanup deleted another benchmark cluster's ConfigMap: %v", err)
	}
	// And the charts-created reader Secret in the ducklings namespace survives.
	if _, err := clientset.CoreV1().Secrets("ducklings").Get(ctx, trinoTestReaderSecret, metav1.GetOptions{}); err != nil {
		t.Fatalf("cleanup deleted the charts-created reader Secret: %v", err)
	}
}

func TestTrinoBenchmarkManagerLogsNoSecretValues(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	manager, _, _ := newTrinoBenchmarkTestManager(t)
	ctx := context.Background()
	result, err := manager.ProvisionTrinoBenchmark(ctx, "bench-org", TrinoBenchmarkRequest{Workers: 2})
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if _, err := manager.TrinoBenchmarkStatus(ctx, result.Cluster.ID); err != nil {
		t.Fatalf("status: %v", err)
	}
	if err := manager.DeprovisionTrinoBenchmark(ctx, result.Cluster.ID); err != nil {
		t.Fatalf("deprovision: %v", err)
	}

	if strings.Contains(logs.String(), trinoTestReaderPass) {
		t.Fatalf("the reader password reached the logs:\n%s", logs.String())
	}
}

func setTrinoDeploymentReady(t *testing.T, clientset kubernetes.Interface, name string, ready int32) {
	t.Helper()
	ctx := context.Background()
	deployment, err := clientset.AppsV1().Deployments(trinoTestNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	deployment.Status.ReadyReplicas = ready
	deployment.Status.AvailableReplicas = ready
	deployment.Status.Replicas = ready
	if _, err := clientset.AppsV1().Deployments(trinoTestNamespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update %s status: %v", name, err)
	}
}
