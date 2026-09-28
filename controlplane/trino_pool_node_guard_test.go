//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func nodeGuardFixture(t *testing.T) (*trinoPoolNodeGuard, *fake.Clientset, trinoPoolInventory, *trinoNodeClaim) {
	t.Helper()
	ctx := context.Background()
	effects, client := newEffects(t, 7)
	_, _, objects := testPoolObjects(t, 7)
	inventory, err := effects.Apply(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	claim := &trinoNodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "compute-claim", UID: "claim-uid"}}
	claim.Status.NodeName = "compute-node"
	claim.Status.ProviderID = "test://compute"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "compute-node", UID: "node-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: claim.Name, UID: claim.UID}}}, Spec: corev1.NodeSpec{ProviderID: claim.Status.ProviderID}}
	if _, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{inventory.CoordinatorDeploymentName, inventory.WorkerDeploymentName} {
		deployment, err := client.AppsV1().Deployments(inventory.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		controller := true
		rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name + "-rs", Namespace: inventory.Namespace, UID: types.UID(name + "-rs-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: name, UID: deployment.UID, Controller: &controller}}}}
		if _, err := client.AppsV1().ReplicaSets(inventory.Namespace).Create(ctx, rs, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		for index := int32(0); index < *deployment.Spec.Replicas; index++ {
			pod := &corev1.Pod{ObjectMeta: *deployment.Spec.Template.ObjectMeta.DeepCopy(), Spec: *deployment.Spec.Template.Spec.DeepCopy()}
			pod.Name = fmt.Sprintf("%s-%d", name, index)
			pod.Namespace = inventory.Namespace
			pod.UID = types.UID(pod.Name + "-uid")
			pod.ResourceVersion = "1"
			pod.Spec.NodeName = node.Name
			pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &controller}}
			delete(pod.Annotations, trinopool.AnnotationDoNotDisrupt)
			if _, err := client.CoreV1().Pods(inventory.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	g := newTrinoPoolNodeGuard(client)
	g.epoch = 7
	g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return []trinoNodeClaim{*claim}, nil }
	g.getClaim = func(ctx context.Context, name string) (trinoNodeClaim, error) {
		claims, err := g.listClaims(ctx)
		if err != nil {
			return trinoNodeClaim{}, err
		}
		for _, candidate := range claims {
			if candidate.Name == name {
				return candidate, nil
			}
		}
		return trinoNodeClaim{}, apierrors.NewNotFound(schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}, name)
	}
	g.reset()
	return g, client, inventory, claim
}

func TestTrinoNodeGuardProtectsOwnedCoordinatorAndWorkersWithoutRolling(t *testing.T) {
	g, client, inventory, _ := nodeGuardFixture(t)
	ctx := context.Background()
	before, _ := client.AppsV1().Deployments(inventory.Namespace).List(ctx, metav1.ListOptions{})
	evidence, err := g.inspect(ctx, inventory)
	if err != nil || len(evidence) != 0 {
		t.Fatalf("inspect = %v, %v", evidence, err)
	}
	pods, _ := client.CoreV1().Pods(inventory.Namespace).List(ctx, metav1.ListOptions{})
	if len(pods.Items) < 2 {
		t.Fatal("fixture does not cover both roles")
	}
	for _, pod := range pods.Items {
		if pod.Annotations[trinopool.AnnotationDoNotDisrupt] != "true" {
			t.Fatalf("pod not protected: %s", pod.Name)
		}
	}
	after, _ := client.AppsV1().Deployments(inventory.Namespace).List(ctx, metav1.ListOptions{})
	if !reflect.DeepEqual(before, after) {
		t.Fatal("pod protection changed a Deployment")
	}
}

func TestTrinoNodeGuardFailsClosedOnAmbiguousOwnershipAndNodeState(t *testing.T) {
	for _, scenario := range []string{"deployment-uid", "replicaset-uid", "claim-uid", "provider-id", "claim-read-error", "finite-grace", "missing-pods", "newer-epoch"} {
		t.Run(scenario, func(t *testing.T) {
			g, client, inventory, claim := nodeGuardFixture(t)
			ctx := context.Background()
			switch scenario {
			case "deployment-uid":
				inventory.WorkerDeploymentUID = "different"
			case "replicaset-uid":
				rs, _ := client.AppsV1().ReplicaSets(inventory.Namespace).Get(ctx, inventory.CoordinatorDeploymentName+"-rs", metav1.GetOptions{})
				rs.UID = "different"
				if _, err := client.AppsV1().ReplicaSets(inventory.Namespace).Update(ctx, rs, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "claim-uid":
				claim.UID = "different"
			case "provider-id":
				claim.Status.ProviderID = "test://other"
			case "claim-read-error":
				g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return nil, errors.New("unavailable") }
			case "finite-grace":
				value := "1h"
				claim.Spec.TerminationGracePeriod = &value
			case "missing-pods":
				if err := client.CoreV1().Pods(inventory.Namespace).Delete(ctx, inventory.WorkerDeploymentName+"-0", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			case "newer-epoch":
				g.epoch = 6
			}
			if evidence, err := g.inspect(ctx, inventory); err == nil || len(evidence) != 0 {
				t.Fatalf("unsafe inspection: %v, %v", evidence, err)
			}
			node, _ := client.CoreV1().Nodes().Get(ctx, "compute-node", metav1.GetOptions{})
			if node.Spec.Unschedulable {
				t.Fatal("observation error cordoned node")
			}
		})
	}
}

func TestTrinoNodeGuardDriftAndDeletingClaimsKeepWorkRunning(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprint(deleting), func(t *testing.T) {
			g, client, inventory, claim := nodeGuardFixture(t)
			claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
			if deleting {
				stamp := metav1.Now()
				claim.DeletionTimestamp = &stamp
				claim.Status.Conditions = nil
			}
			evidence, err := g.inspect(context.Background(), inventory)
			if err != nil || len(evidence) != 1 {
				t.Fatalf("evidence: %v, %v", evidence, err)
			}
			if err := g.cordon(context.Background(), evidence[0], "pool", "instance"); err != nil {
				t.Fatal(err)
			}
			node, _ := client.CoreV1().Nodes().Get(context.Background(), "compute-node", metav1.GetOptions{})
			if !node.Spec.Unschedulable || node.Annotations[trinoNodeRetirementAnnotation] == "" {
				t.Fatal("node lacks owned scheduling exclusion")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" || action.GetVerb() == "delete-collection" {
					t.Fatalf("node replacement deleted a resource: %v", action)
				}
			}
		})
	}
}

func TestTrinoNodeGuardRejectsChangedNodeBeforeCordon(t *testing.T) {
	g, client, inventory, claim := nodeGuardFixture(t)
	claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
	evidence, err := g.inspect(context.Background(), inventory)
	if err != nil {
		t.Fatal(err)
	}
	node, _ := client.CoreV1().Nodes().Get(context.Background(), "compute-node", metav1.GetOptions{})
	node.UID = "replacement-node"
	if _, err := client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := g.cordon(context.Background(), evidence[0], "pool", "instance"); err == nil {
		t.Fatal("cordoned a replacement node")
	}
}

func TestTrinoNodeGuardSharesClaimInventoryPerPass(t *testing.T) {
	g, _, inventory, claim := nodeGuardFixture(t)
	calls := 0
	g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { calls++; return []trinoNodeClaim{*claim}, nil }
	for range 3 {
		if _, err := g.inspect(context.Background(), inventory); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("claim inventory fetched %d times", calls)
	}
	g.reset()
	if _, err := g.inspect(context.Background(), inventory); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("new pass reused old NodeClaim inventory")
	}
}

func TestTrinoNodeClaimInventoryRequestsJSONAndAllPages(t *testing.T) {
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/karpenter.sh/v1/nodeclaims" || r.Header.Get("Accept") != "application/json" || r.URL.Query().Get("limit") != "500" {
			t.Errorf("unexpected request: %s accept=%s", r.URL, r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		pages++
		if r.URL.Query().Get("continue") == "" {
			_, _ = w.Write([]byte(`{"metadata":{"continue":"page-2"},"items":[{"metadata":{"name":"claim-a","uid":"uid-a"}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"metadata":{},"items":[{"metadata":{"name":"claim-b","uid":"uid-b"}}]}`))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := newTrinoPoolNodeGuard(client).listClaims(context.Background())
	if err != nil || len(claims) != 2 || pages != 2 {
		t.Fatalf("claims=%v pages=%d err=%v", claims, pages, err)
	}
	encoded, _ := json.Marshal(claims)
	if len(encoded) == 0 {
		t.Fatal("empty encoded claims")
	}
}

func TestTrinoNodeGuardCollectsDriftPastUnrelatedCordon(t *testing.T) {
	for _, name := range []string{"a-cordoned", "z-cordoned"} {
		t.Run(name, func(t *testing.T) {
			g, client, inventory, claim := nodeGuardFixture(t)
			ctx := context.Background()
			claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
			other := *claim
			other.Name, other.UID = "other-claim", "other-uid"
			other.Status.NodeName, other.Status.ProviderID, other.Status.Conditions = name, "test://other", nil
			g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return []trinoNodeClaim{*claim, other}, nil }
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "other-node-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: other.Name, UID: other.UID}}}, Spec: corev1.NodeSpec{ProviderID: other.Status.ProviderID, Unschedulable: true}}
			if _, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			pod, _ := client.CoreV1().Pods(inventory.Namespace).Get(ctx, inventory.WorkerDeploymentName+"-0", metav1.GetOptions{})
			pod.Spec.NodeName = name
			if _, err := client.CoreV1().Pods(inventory.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			evidence, err := g.inspect(ctx, inventory)
			if !errors.Is(err, errTrinoUnschedulableNode) || len(evidence) != 1 || evidence[0].NodeUID != "node-uid" {
				t.Fatalf("order-dependent observation: %v %v", evidence, err)
			}
		})
	}
}

func TestTrinoNodeNamedClaimRejectsCachedWrongUID(t *testing.T) {
	g, client, _, claim := nodeGuardFixture(t)
	g.namedClaims = true
	claim.UID = "replaced-claim-uid"
	node, _ := client.CoreV1().Nodes().Get(context.Background(), "compute-node", metav1.GetOptions{})
	for range 2 {
		if _, err := g.nodeClaim(context.Background(), node); !errors.Is(err, errTrinoNodePlacementInvalid) {
			t.Fatalf("accepted wrong cached UID: %v", err)
		}
	}
}

func TestTrinoNodeNamedClaimRequestsFreshJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/karpenter.sh/v1/nodeclaims/claim-a" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected request: %s accept=%s", r.URL, r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"name":"claim-a","uid":"uid-a"}}`))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := newTrinoPoolNodeGuard(client).getClaim(context.Background(), "claim-a")
	if err != nil || claim.Name != "claim-a" || claim.UID != "uid-a" {
		t.Fatalf("claim=%v err=%v", claim, err)
	}
}
