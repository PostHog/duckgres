//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/controlplane/trinopool"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const trinoNodeRetirementAnnotation = "posthog.com/trino-node-retirement"

var errTrinoUnschedulableNode = errors.New("pool pod is on an unschedulable node")
var errTrinoPodProtectionIncomplete = errors.New("pool pod protection inventory is not complete")

type trinoNodeClaim struct {
	metav1.ObjectMeta `json:"metadata"`
	Spec              struct {
		TerminationGracePeriod *string `json:"terminationGracePeriod"`
	} `json:"spec"`
	Status struct {
		NodeName   string             `json:"nodeName"`
		ProviderID string             `json:"providerID"`
		Conditions []metav1.Condition `json:"conditions"`
	} `json:"status"`
}

type trinoPoolNodeGuard struct {
	client     kubernetes.Interface
	listClaims func(context.Context) ([]trinoNodeClaim, error)
	claims     map[string]trinoNodeClaim
	claimsErr  error
	loaded     bool
	nodes      map[string]*corev1.Node
	epoch      int64
}

func newTrinoPoolNodeGuard(client kubernetes.Interface) *trinoPoolNodeGuard {
	g := &trinoPoolNodeGuard{client: client}
	g.listClaims = func(ctx context.Context) ([]trinoNodeClaim, error) {
		ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
		defer cancel()
		var claims []trinoNodeClaim
		continuation := ""
		for {
			request := client.CoreV1().RESTClient().Get().AbsPath("/apis/karpenter.sh/v1/nodeclaims").SetHeader("Accept", "application/json").Param("limit", "500")
			if continuation != "" {
				request = request.Param("continue", continuation)
			}
			raw, err := request.Do(ctx).Raw()
			if err != nil {
				return nil, err
			}
			var page struct {
				Metadata metav1.ListMeta  `json:"metadata"`
				Items    []trinoNodeClaim `json:"items"`
			}
			if err := json.Unmarshal(raw, &page); err != nil {
				return nil, err
			}
			claims = append(claims, page.Items...)
			if len(claims) > 10000 {
				return nil, errors.New("NodeClaim inventory exceeds safety limit")
			}
			if page.Metadata.Continue == "" {
				return claims, nil
			}
			if page.Metadata.Continue == continuation {
				return nil, errors.New("NodeClaim pagination did not advance")
			}
			continuation = page.Metadata.Continue
		}
	}
	return g
}

func (g *trinoPoolNodeGuard) reset() {
	g.loaded = false
	g.claims = nil
	g.claimsErr = nil
	g.nodes = make(map[string]*corev1.Node)
}

func (g *trinoPoolNodeGuard) nodeClaim(ctx context.Context, node *corev1.Node) (trinoNodeClaim, error) {
	if !g.loaded {
		g.loaded = true
		claims, err := g.listClaims(ctx)
		g.claimsErr = err
		g.claims = make(map[string]trinoNodeClaim)
		for _, claim := range claims {
			g.claims[string(claim.UID)] = claim
		}
	}
	if g.claimsErr != nil {
		return trinoNodeClaim{}, g.claimsErr
	}
	for _, owner := range node.OwnerReferences {
		if owner.APIVersion != "karpenter.sh/v1" || owner.Kind != "NodeClaim" {
			continue
		}
		claim, ok := g.claims[string(owner.UID)]
		if !ok || owner.UID == "" || claim.Name != owner.Name || claim.Status.NodeName != node.Name || node.Spec.ProviderID == "" || claim.Status.ProviderID != node.Spec.ProviderID {
			return trinoNodeClaim{}, errors.New("node lacks an exact matching NodeClaim")
		}
		if claim.Spec.TerminationGracePeriod != nil {
			return trinoNodeClaim{}, errors.New("finite NodeClaim termination grace period can override query protection")
		}
		return claim, nil
	}
	return trinoNodeClaim{}, errors.New("pool pod node is not owned by a NodeClaim")
}

// inspect protects existing pods without modifying their Deployment templates.
// Labels select candidates; exact Deployment and ReplicaSet UIDs establish ownership.
func (g *trinoPoolNodeGuard) inspect(ctx context.Context, inventory trinoPoolInventory) ([]trinopool.NodeReplacementEvidence, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()
	deployments := make(map[types.UID]*appsv1.Deployment)
	for _, wanted := range []struct{ name, uid string }{
		{inventory.CoordinatorDeploymentName, inventory.CoordinatorDeploymentUID},
		{inventory.WorkerDeploymentName, inventory.WorkerDeploymentUID},
	} {
		if wanted.name == "" || wanted.uid == "" {
			return nil, errors.New("pod protection requires recorded Deployment identities")
		}
		deployment, err := g.client.AppsV1().Deployments(inventory.Namespace).Get(ctx, wanted.name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if string(deployment.UID) != wanted.uid || deployment.Labels[trinopool.LabelInstance] != inventory.ServiceName || deployment.Labels[trinopool.LabelManagedBy] != trinopool.ManagedByValue {
			return nil, errTrinoPoolForeignObject
		}
		epoch, err := strconv.ParseInt(deployment.Annotations[trinopool.AnnotationAuthorityEpoch], 10, 64)
		if err != nil || epoch > g.epoch {
			return nil, errTrinoPoolStaleEpoch
		}
		if deployment.Spec.Replicas == nil {
			return nil, errTrinoPodProtectionIncomplete
		}
		deployments[deployment.UID] = deployment
	}
	pods, err := g.client.CoreV1().Pods(inventory.Namespace).List(ctx, metav1.ListOptions{LabelSelector: trinopool.LabelInstance + "=" + inventory.ServiceName, Limit: trinoPoolListLimit})
	if err != nil {
		return nil, err
	}
	if pods.Continue != "" {
		return nil, errors.New("pool pod protection inventory exceeds listing limit")
	}
	replicaSets := make(map[types.UID]types.UID)
	scheduled := make(map[types.UID]int)
	nodeNames := make(map[string]bool)
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.UID == "" {
			return nil, errTrinoPoolForeignObject
		}
		if replicaSets[owner.UID] == "" {
			rs, err := g.client.AppsV1().ReplicaSets(inventory.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			parent := metav1.GetControllerOf(rs)
			if rs.UID != owner.UID || parent == nil || parent.APIVersion != "apps/v1" || parent.Kind != "Deployment" || deployments[parent.UID] == nil || deployments[parent.UID].Name != parent.Name {
				return nil, errTrinoPoolForeignObject
			}
			replicaSets[owner.UID] = parent.UID
		}
		if pod.UID == "" || pod.ResourceVersion == "" {
			return nil, errors.New("pod protection requires UID and resource version")
		}
		if pod.Annotations[trinopool.AnnotationDoNotDisrupt] != "true" {
			patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": pod.UID, "resourceVersion": pod.ResourceVersion, "annotations": map[string]string{trinopool.AnnotationDoNotDisrupt: "true"}}})
			if _, err := g.client.CoreV1().Pods(inventory.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
				return nil, err
			}
		}
		if pod.Spec.NodeName != "" {
			nodeNames[pod.Spec.NodeName] = true
			if pod.DeletionTimestamp == nil {
				scheduled[replicaSets[owner.UID]]++
			}
		}
	}
	var evidence []trinopool.NodeReplacementEvidence
	ordered := make([]string, 0, len(nodeNames))
	for name := range nodeNames {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	for _, name := range ordered {
		node := g.nodes[name]
		if node == nil {
			node, err = g.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if g.nodes == nil {
				g.nodes = make(map[string]*corev1.Node)
			}
			g.nodes[name] = node
		}
		claim, err := g.nodeClaim(ctx, node)
		if err != nil {
			return nil, err
		}
		reason := ""
		for _, condition := range claim.Status.Conditions {
			if condition.Type == "Drifted" && condition.Status == metav1.ConditionTrue {
				reason = "Drifted"
			}
		}
		if node.DeletionTimestamp != nil || claim.DeletionTimestamp != nil {
			reason = "Deleting"
		}
		if reason != "" {
			e := trinopool.NodeReplacementEvidence{NodeName: node.Name, NodeUID: string(node.UID), NodeClaimName: claim.Name, NodeClaimUID: string(claim.UID), Reason: reason}
			if err := e.Validate(); err != nil {
				return nil, err
			}
			evidence = append(evidence, e)
		} else if node.Spec.Unschedulable {
			return nil, errTrinoUnschedulableNode
		}
	}
	for uid, deployment := range deployments {
		if scheduled[uid] != int(*deployment.Spec.Replicas) {
			return evidence, errTrinoPodProtectionIncomplete
		}
	}
	return evidence, nil
}

// cordon only excludes future scheduling. It never evicts pods or deletes nodes.
// The durable replacement request precedes this UID-bound, optimistic update.
func (g *trinoPoolNodeGuard) cordon(ctx context.Context, evidence trinopool.NodeReplacementEvidence, poolID, instanceID string) error {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolRequestBudget)
	defer cancel()
	node, err := g.client.CoreV1().Nodes().Get(ctx, evidence.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(node.UID) != evidence.NodeUID || node.ResourceVersion == "" {
		return errors.New("node identity changed before cordon")
	}
	claim, err := g.nodeClaim(ctx, node)
	if err != nil {
		return err
	}
	if string(claim.UID) != evidence.NodeClaimUID || claim.Name != evidence.NodeClaimName {
		return errors.New("NodeClaim identity changed before cordon")
	}
	if node.Spec.Unschedulable {
		return nil
	}
	owner := strings.Join([]string{poolID, instanceID, evidence.NodeClaimUID, evidence.Reason}, "/")
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": node.UID, "resourceVersion": node.ResourceVersion, "annotations": map[string]string{trinoNodeRetirementAnnotation: owner}}, "spec": map[string]any{"unschedulable": true}})
	_, err = g.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}
