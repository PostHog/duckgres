//go:build kubernetes

package controlplane

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// This boundary test uses real stores and Kubernetes, but not a SQL engine.
// The runner owns an isolated namespace and seeds synthetic retained work.
func TestTrinoPoolRecoveryIsolatedBoundary(t *testing.T) {
	namespace := os.Getenv("TRINO_RECOVERY_E2E_NAMESPACE")
	if namespace == "" {
		t.Skip("run just test-trino-recovery-isolated with disposable fixtures")
	}
	if !strings.HasPrefix(namespace, "trino-recovery-e2e-") {
		t.Fatal("fixture namespace prefix required")
	}
	if os.Getenv("TRINO_RECOVERY_E2E_CONTEXT") == "" {
		t.Fatal("explicit fixture Kubernetes context required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	require := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading,
		&clientcmd.ConfigOverrides{CurrentContext: os.Getenv("TRINO_RECOVERY_E2E_CONTEXT")}).ClientConfig()
	require(err)
	kube, err := kubernetes.NewForConfig(kubeConfig)
	require(err)
	ns, err := kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require(err)
	if run := os.Getenv("TRINO_RECOVERY_E2E_RUN_ID"); run == "" || ns.Labels["duckgres.io/recovery-e2e"] != run {
		t.Fatal("fixture namespace ownership required")
	}
	store, err := configstore.NewConfigStore(os.Getenv("TRINO_RECOVERY_E2E_CONFIG_DSN"), time.Hour)
	require(err)
	configDB, err := store.DB().DB()
	require(err)
	t.Cleanup(func() { _ = configDB.Close() })
	db, err := sql.Open("postgres", os.Getenv("TRINO_RECOVERY_E2E_GATEWAY_DSN"))
	require(err)
	t.Cleanup(func() { _ = db.Close() })
	gateway, err := trinogateway.NewClient(trinogateway.Config{BaseURL: os.Getenv("TRINO_RECOVERY_E2E_GATEWAY_URL"),
		AdminToken: os.Getenv("TRINO_RECOVERY_E2E_TOKEN"), AllowPlaintext: true})
	require(err)
	const poolID, group = "registered:recovery-test", "recovery-test"
	spec := configstore.TrinoPoolSpec{PoolID: poolID, PublicID: group, APIMode: configstore.TrinoPoolAPIModeShared,
		DesiredReleaseID: "fixture-release", DesiredBlueprintDigest: "fixture-digest", DesiredInstances: 1, MinServing: 1, MaxSurge: 1, MaxRepair: 1}
	require(store.SeedTrinoPool(ctx, spec))
	lease, err := store.AcquireTrinoPoolAuthority(ctx, poolID, "fixture-leader-a")
	require(err)
	configure := func(lease configstore.TrinoPoolLease) {
		t.Helper()
		_, err := gateway.ConfigurePool(ctx, group, trinogateway.ConfigurePoolRequest{
			Step:    trinogateway.Step{OperationID: "fixture-configure", StepID: fmt.Sprintf("epoch-%d", lease.Epoch), ControllerEpoch: lease.Epoch, OwnerIdentity: lease.Owner},
			APIMode: "POOLED", MinServing: 1, DesiredMembers: 1, MaxSurge: 1, MaxRepair: 1, DesiredRevision: "fixture-release"})
		require(err)
	}
	configure(lease)
	effects := func(epoch int64) trinoPoolKube {
		return newTrinoPoolEffects(kube, namespace, trinopool.BlueprintSharedResources{}, epoch)
	}
	operator := &trinoPoolOperator{config: trinoPoolConfig{PoolID: poolID, PublicID: group, RoutingGroup: group, Namespace: namespace, Spec: spec},
		store: store, gateway: gateway, kube: effects, lease: lease, owner: lease.Owner}
	waitPod := func(id, oldUID string) corev1.Pod {
		t.Helper()
		for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
			pods, err := kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: trinopool.LabelInstance + "=" + id + ",app.kubernetes.io/component=coordinator"})
			require(err)
			for _, pod := range pods.Items {
				if string(pod.UID) == oldUID || pod.DeletionTimestamp != nil {
					continue
				}
				for _, status := range pod.Status.ContainerStatuses {
					if status.Ready && status.ContainerID != "" {
						return pod
					}
				}
			}
		}
		t.Fatal("fixture coordinator did not become ready with a new UID")
		return corev1.Pod{}
	}
	seed := func(id string, phase trinopool.Phase, state string) (configstore.TrinoPoolInstance, corev1.Pod) {
		t.Helper()
		objects := recoveryFixtureObjects(namespace, id, os.Getenv("TRINO_RECOVERY_E2E_IMAGE"), lease.Epoch)
		inventory, err := effects(lease.Epoch).(*trinoPoolEffects).Apply(ctx, objects)
		require(err)
		pod := waitPod(id, "")
		snapshot, err := json.Marshal(map[string]string{"namespace": namespace})
		require(err)
		endpoint := "http://" + id + "." + namespace + ".svc:8080"
		require(store.CreateTrinoPoolInstance(ctx, lease, configstore.TrinoPoolInstanceSpec{InstanceID: id, PoolID: poolID,
			ReleaseID: "fixture-release", SpecDigest: "fixture-digest", BlueprintSnapshot: string(snapshot), Phase: phase, EndpointURL: endpoint}))
		incarnation := uuid.NewString()
		_, err = db.ExecContext(ctx, `INSERT INTO transaction_backend
			(incarnation,name,current_name,backend_url,external_url,routing_group,node_id,coordinator_id,state,generation,pool_id,instance_id,pod_uid,boot_id,config_revision,certified_revision,auth_revision)
			VALUES($1,$2,$2,$3,$3,$4,$5,$6,$7,2,$4,$2,$8,$9,'fixture-release','fixture-release','fixture-auth')`,
			incarnation, id, endpoint, group, id+"-node", id+"-coordinator", state, string(pod.UID), id+"-boot")
		require(err)
		updates := inventoryUpdates(inventory)
		for key, value := range map[string]any{"coordinator_pod_uid": string(pod.UID), "coordinator_container_id": pod.Status.ContainerStatuses[0].ContainerID,
			"coordinator_boot_id": id + "-boot", "coordinator_node_id": id + "-node", "coordinator_id": id + "-coordinator",
			"gateway_incarnation": incarnation, "gateway_state": state, "gateway_generation": int64(2)} {
			updates[key] = value
		}
		require(store.RecordTrinoPoolInstanceFields(ctx, lease, id, updates))
		instance, err := store.GetTrinoPoolInstance(ctx, id)
		require(err)
		return *instance, pod
	}
	healthy, healthyPod := seed("healthy", trinopool.PhaseServing, "ACTIVE")
	target, admittedPod := seed("candidate", trinopool.PhaseDraining, "DRAINING")
	_, err = db.ExecContext(ctx, `INSERT INTO transaction_binding(transaction_id,owner_hash,incarnation,start_query_id,state) VALUES('fixture-tx','fixture-owner',$1,'fixture-query','OPEN')`, target.GatewayIncarnation)
	require(err)
	_, err = db.ExecContext(ctx, `INSERT INTO transaction_query(query_id,owner_hash,incarnation,transaction_id,terminal) VALUES('fixture-query','fixture-owner',$1,'fixture-tx',false)`, target.GatewayIncarnation)
	require(err)
	_, err = db.ExecContext(ctx, `INSERT INTO transaction_admission(admission_id,incarnation,owner_hash,state) VALUES($1,$2,'fixture-owner','PENDING')`, uuid.NewString(), target.GatewayIncarnation)
	require(err)
	request, err := store.RequestTrinoPoolRecovery(ctx, poolID, target.InstanceID, configstore.TrinoPoolRecovery{OperationID: uuid.NewString(), ExpectedGeneration: 2,
		Incarnation: target.GatewayIncarnation, PodUID: target.CoordinatorPodUID, BootID: target.CoordinatorBootID,
		NodeID: target.CoordinatorNodeID, CoordinatorID: target.CoordinatorID, RequestedBy: "fixture@example.invalid",
		Reason: "Retire the isolated absent-process fixture", DestructiveAuthorization: true})
	require(err)
	if _, err = operator.recoverInstance(ctx, target, *request); err == nil {
		t.Fatal("live process with retained requests and transactions was not protected")
	}
	member, err := gateway.GetMember(ctx, group, target.InstanceID)
	require(err)
	if member.Phase != "DRAINING" {
		t.Fatalf("blocked recovery changed Gateway phase: %s", member.Phase)
	}
	t.Log("live admitted process protected; obligations and authorization seeded")
	uid := admittedPod.UID
	require(kube.CoreV1().Pods(namespace).Delete(ctx, admittedPod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}))
	replacementPod := waitPod(target.InstanceID, string(uid))
	if replacementPod.UID == uid {
		t.Fatal("replacement reused the admitted UID")
	}
	t.Log("original UID absent; replacement coordinator Pod is running")
	claimed := false
	for deadline := time.Now().Add(65 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		_, err = operator.recoverInstance(ctx, target, *request)
		if err == nil {
			claimed = true
			break
		}
	}
	if !claimed {
		t.Fatalf("absent admitted process remained stuck: %v", err)
	}
	member, err = gateway.GetMember(ctx, group, target.InstanceID)
	require(err)
	if member.Phase != "RETIRING" || member.RetirementKind != "FAILED" {
		t.Fatalf("missing failed retirement claim: %+v", member)
	}
	t.Log("real Gateway committed failed retirement before resource deletion")
	oldOperator := operator
	lease, err = store.AcquireTrinoPoolAuthority(ctx, poolID, "fixture-leader-b")
	require(err)
	configure(lease)
	operator = &trinoPoolOperator{config: oldOperator.config, store: store, gateway: gateway, kube: effects, lease: lease, owner: lease.Owner}
	current, err := store.GetTrinoPoolInstance(ctx, target.InstanceID)
	require(err)
	if _, err = oldOperator.recoverInstance(ctx, *current, *request); err == nil {
		t.Fatal("superseded recovery leader was not fenced")
	}
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		current, err = store.GetTrinoPoolInstance(ctx, target.InstanceID)
		require(err)
		if current.Phase == string(trinopool.PhaseFailureRetired) {
			break
		}
		_, err = operator.recoverInstance(ctx, *current, *request)
		require(err)
	}
	if current.Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatalf("local retirement did not finish: %s", current.Phase)
	}
	member, err = gateway.GetMember(ctx, group, target.InstanceID)
	require(err)
	if member.Phase != "RETIRED" || member.RetirementKind != "FAILED" {
		t.Fatalf("Gateway retirement did not finish: %+v", member)
	}
	absent, err := effects(lease.Epoch).ResourcesAbsent(ctx, inventoryOf(target))
	require(err)
	if !absent {
		t.Fatal("target inventory or Pods survived retirement")
	}
	for _, name := range []string{target.ConfigMapName, target.WorkerConfigMapName} {
		_, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("target ConfigMap survived retirement: %v", err)
		}
	}
	var evidence string
	var admissions, transactions, queries int
	require(db.QueryRowContext(ctx, `SELECT evidence,outstanding_admissions,outstanding_transactions,outstanding_queries FROM pool_failure_receipt WHERE incarnation=$1`, target.GatewayIncarnation).Scan(&evidence, &admissions, &transactions, &queries))
	if evidence != "DESTRUCTIVE_OVERRIDE" || admissions != 1 || transactions != 1 || queries != 1 {
		t.Fatalf("failure receipt lost retained work: %s %d/%d/%d", evidence, admissions, transactions, queries)
	}
	for _, table := range []string{"transaction_admission", "transaction_binding", "transaction_query"} {
		var count int
		require(db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE incarnation=$1", target.GatewayIncarnation).Scan(&count))
		if count != 1 {
			t.Fatalf("%s evidence was erased", table)
		}
	}
	after, err := store.GetTrinoPoolRecovery(ctx, poolID, target.InstanceID)
	require(err)
	if *after != *request {
		t.Fatal("immutable recovery authorization changed")
	}
	healthyAfter, err := gateway.GetMember(ctx, group, healthy.InstanceID)
	require(err)
	if healthyAfter.Phase != "ACTIVE" || healthyAfter.Generation != 2 {
		t.Fatal("healthy Gateway member changed")
	}
	liveHealthy := waitPod(healthy.InstanceID, "")
	if liveHealthy.UID != healthyPod.UID {
		t.Fatal("healthy coordinator was replaced")
	}
	for name, uid := range map[string]string{healthy.CoordinatorDeploymentName: healthy.CoordinatorDeploymentUID, healthy.WorkerDeploymentName: healthy.WorkerDeploymentUID} {
		deployment, err := kube.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		require(err)
		if string(deployment.UID) != uid {
			t.Fatal("healthy Deployment identity changed")
		}
		if deployment.Status.ReadyReplicas != 1 {
			t.Fatal("healthy fixture Deployment lost readiness")
		}
	}
	for name, uid := range map[string]string{healthy.ConfigMapName: healthy.ConfigMapUID, healthy.WorkerConfigMapName: healthy.WorkerConfigMapUID} {
		configMap, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		require(err)
		if string(configMap.UID) != uid {
			t.Fatal("healthy ConfigMap identity changed")
		}
	}
	service, err := kube.CoreV1().Services(namespace).Get(ctx, healthy.ServiceName, metav1.GetOptions{})
	require(err)
	if string(service.UID) != healthy.ServiceUID {
		t.Fatal("healthy Service identity changed")
	}
	t.Log("PASS: leader replay, both terminal stores, retained evidence, exact cleanup, and healthy sibling protection")
}

func recoveryFixtureObjects(namespace, id, image string, epoch int64) trinopool.Objects {
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: namespace,
			Labels:      map[string]string{trinopool.LabelInstance: id, trinopool.LabelPool: "recovery-test", trinopool.LabelManagedBy: trinopool.ManagedByValue},
			Annotations: map[string]string{trinopool.AnnotationSpecDigest: "fixture-digest", trinopool.AnnotationAuthorityEpoch: fmt.Sprint(epoch)}}
	}
	deployment := func(component string) *appsv1.Deployment {
		labels := map[string]string{trinopool.LabelInstance: id, "app.kubernetes.io/component": component}
		one, grace := int32(1), int64(1)
		return &appsv1.Deployment{ObjectMeta: meta(id + "-" + component), Spec: appsv1.DeploymentSpec{Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: &grace, Containers: []corev1.Container{{Name: "fixture", Image: image, Command: []string{"sleep", "3600"},
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
						Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")}}}}}}}}
	}
	return trinopool.Objects{ConfigMap: &corev1.ConfigMap{ObjectMeta: meta(id + "-config")}, WorkerConfigMap: &corev1.ConfigMap{ObjectMeta: meta(id + "-worker-config")},
		Service:               &corev1.Service{ObjectMeta: meta(id), Spec: corev1.ServiceSpec{Selector: map[string]string{trinopool.LabelInstance: id, "app.kubernetes.io/component": "coordinator"}, Ports: []corev1.ServicePort{{Port: 8080}}}},
		CoordinatorDeployment: deployment("coordinator"), WorkerDeployment: deployment("worker")}
}
