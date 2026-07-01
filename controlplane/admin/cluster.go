//go:build kubernetes

package admin

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The cluster-topology endpoints back the admin console's live "Nodes" view (a
// port of the standalone peepernetes visualizer). That UI talks the native
// Kubernetes list shape (an envelope of `{items: [...]}` whose objects carry
// metadata/spec/status), so instead of proxying raw K8s objects — heavy, and it
// would leak fields the browser never reads — we project each node/pod/event
// down to exactly the fields the view consumes and re-emit them under the SAME
// JSON paths. The frontend classification/rendering logic then works unchanged.
//
// All three list endpoints (plus the karpenter nodepool passthrough) are
// read-only GETs, so RoleGate lets viewers reach them; no mutation path exists.

// listLimit bounds each list call so a large cluster can't return an unbounded
// payload. Nodes are few; pods/events are capped generously but finitely.
const (
	clusterNodeLimit  = 2000
	clusterPodLimit   = 5000
	clusterEventLimit = 2000
)

// clusterHandler serves the node-overview topology reads from the in-cluster
// kubernetes client the control plane already owns.
type clusterHandler struct {
	client kubernetes.Interface
}

// registerClusterAPI wires the read-only cluster-topology endpoints. It is only
// called when a kubernetes client is available (remote/k8s backend); the
// process/standalone backends have no cluster to introspect.
func registerClusterAPI(r *gin.RouterGroup, client kubernetes.Interface) {
	h := &clusterHandler{client: client}
	r.GET("/cluster/nodes", h.nodes)
	r.GET("/cluster/pods", h.pods)
	r.GET("/cluster/events", h.events)
	r.GET("/cluster/nodepools", h.nodepools)
}

// --- projection types: the minimal K8s-shaped subset the node view reads ---

type objMeta struct {
	UID               string            `json:"uid"`
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerReferences   []ownerRef        `json:"ownerReferences,omitempty"`
}

type ownerRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type nodeProj struct {
	Metadata objMeta    `json:"metadata"`
	Spec     nodeSpec   `json:"spec"`
	Status   nodeStatus `json:"status"`
}

type nodeSpec struct {
	Unschedulable bool    `json:"unschedulable,omitempty"`
	Taints        []taint `json:"taints,omitempty"`
}

type taint struct {
	Key    string `json:"key"`
	Effect string `json:"effect,omitempty"`
}

type nodeStatus struct {
	Allocatable map[string]string `json:"allocatable,omitempty"`
	Conditions  []nodeCondition   `json:"conditions,omitempty"`
}

type nodeCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type podProj struct {
	Metadata objMeta   `json:"metadata"`
	Spec     podSpec   `json:"spec"`
	Status   podStatus `json:"status"`
}

type podSpec struct {
	NodeName   string      `json:"nodeName,omitempty"`
	Priority   *int32      `json:"priority,omitempty"`
	Containers []container `json:"containers,omitempty"`
}

type container struct {
	Image     string    `json:"image,omitempty"`
	Resources resources `json:"resources"`
}

type resources struct {
	Requests map[string]string `json:"requests,omitempty"`
}

type podStatus struct {
	Phase string `json:"phase,omitempty"`
}

type eventProj struct {
	Metadata       objMeta        `json:"metadata"`
	Reason         string         `json:"reason,omitempty"`
	Message        string         `json:"message,omitempty"`
	Type           string         `json:"type,omitempty"`
	InvolvedObject involvedObject `json:"involvedObject"`
}

type involvedObject struct {
	Kind      string `json:"kind,omitempty"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// mirrorPodAnnotation is the only annotation the node view reads (it marks
// static/mirror pods so they classify as system + are ignored for empty-node
// reclaim). Projecting just this key keeps last-applied-config and other bulky
// annotations out of the payload.
const mirrorPodAnnotation = "kubernetes.io/config.mirror"

func projMeta(m metav1.ObjectMeta) objMeta {
	om := objMeta{
		UID:       string(m.UID),
		Name:      m.Name,
		Namespace: m.Namespace,
		Labels:    m.Labels,
	}
	if !m.CreationTimestamp.IsZero() {
		om.CreationTimestamp = m.CreationTimestamp.UTC().Format(time.RFC3339)
	}
	if m.DeletionTimestamp != nil {
		om.DeletionTimestamp = m.DeletionTimestamp.UTC().Format(time.RFC3339)
	}
	if v, ok := m.Annotations[mirrorPodAnnotation]; ok {
		om.Annotations = map[string]string{mirrorPodAnnotation: v}
	}
	for _, o := range m.OwnerReferences {
		om.OwnerReferences = append(om.OwnerReferences, ownerRef{Kind: o.Kind, Name: o.Name})
	}
	return om
}

// resourceStrings projects a ResourceList down to the cpu/memory quantity
// strings the view parses (K8s canonical form: "500m", "8", "32861692Ki", …).
func resourceStrings(rl corev1.ResourceList) map[string]string {
	if len(rl) == 0 {
		return nil
	}
	out := map[string]string{}
	if q, ok := rl[corev1.ResourceCPU]; ok {
		out["cpu"] = q.String()
	}
	if q, ok := rl[corev1.ResourceMemory]; ok {
		out["memory"] = q.String()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// clusterListError renders a topology-list error. An RBAC Forbidden degrades to
// an empty {items:[]} (200) — the view shows nothing rather than erroring — and
// logs once so a missing ClusterRole is visible in operator logs. Any other
// error is a real 500. This keeps the read-only view robust when the
// duckgres-control-plane-cluster-topology ClusterRole hasn't been granted (e.g.
// the e2e CP, whose SA can't be given cluster-scoped reads).
func clusterListError(c *gin.Context, resource string, err error) {
	if apierrors.IsForbidden(err) {
		slog.Warn("admin: cluster topology read forbidden — grant the duckgres-control-plane-cluster-topology ClusterRole",
			"resource", resource, "error", err)
		c.JSON(http.StatusOK, gin.H{"items": []any{}})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

func (h *clusterHandler) nodes(c *gin.Context) {
	list, err := h.client.CoreV1().Nodes().List(c.Request.Context(), metav1.ListOptions{Limit: clusterNodeLimit})
	if err != nil {
		clusterListError(c, "nodes", err)
		return
	}
	items := make([]nodeProj, 0, len(list.Items))
	for i := range list.Items {
		n := &list.Items[i]
		np := nodeProj{
			Metadata: projMeta(n.ObjectMeta),
			Spec: nodeSpec{
				Unschedulable: n.Spec.Unschedulable,
			},
			Status: nodeStatus{
				Allocatable: resourceStrings(n.Status.Allocatable),
			},
		}
		for _, t := range n.Spec.Taints {
			np.Spec.Taints = append(np.Spec.Taints, taint{Key: t.Key, Effect: string(t.Effect)})
		}
		for _, cond := range n.Status.Conditions {
			np.Status.Conditions = append(np.Status.Conditions, nodeCondition{
				Type:   string(cond.Type),
				Status: string(cond.Status),
			})
		}
		items = append(items, np)
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *clusterHandler) pods(c *gin.Context) {
	list, err := h.client.CoreV1().Pods(metav1.NamespaceAll).List(c.Request.Context(), metav1.ListOptions{Limit: clusterPodLimit})
	if err != nil {
		clusterListError(c, "pods", err)
		return
	}
	items := make([]podProj, 0, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		pp := podProj{
			Metadata: projMeta(p.ObjectMeta),
			Spec: podSpec{
				NodeName: p.Spec.NodeName,
				Priority: p.Spec.Priority,
			},
			Status: podStatus{Phase: string(p.Status.Phase)},
		}
		for _, ct := range p.Spec.Containers {
			pp.Spec.Containers = append(pp.Spec.Containers, container{
				Image:     ct.Image,
				Resources: resources{Requests: resourceStrings(ct.Resources.Requests)},
			})
		}
		items = append(items, pp)
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *clusterHandler) events(c *gin.Context) {
	list, err := h.client.CoreV1().Events(metav1.NamespaceAll).List(c.Request.Context(), metav1.ListOptions{Limit: clusterEventLimit})
	if err != nil {
		clusterListError(c, "events", err)
		return
	}
	items := make([]eventProj, 0, len(list.Items))
	for i := range list.Items {
		e := &list.Items[i]
		items = append(items, eventProj{
			Metadata: objMeta{UID: string(e.UID), Name: e.Name, Namespace: e.Namespace},
			Reason:   e.Reason,
			Message:  e.Message,
			Type:     e.Type,
			InvolvedObject: involvedObject{
				Kind:      e.InvolvedObject.Kind,
				Name:      e.InvolvedObject.Name,
				Namespace: e.InvolvedObject.Namespace,
			},
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// nodepools proxies the karpenter NodePool list (cluster-scoped CRD) verbatim.
// The view reads spec.disruption.{consolidationPolicy,consolidateAfter} to draw
// the empty-node reclaim countdown. We try v1 then v1beta1, and degrade to an
// empty list when karpenter isn't installed so the view simply omits countdowns
// rather than erroring.
func (h *clusterHandler) nodepools(c *gin.Context) {
	// The typed clientset has no karpenter CRD client; go through the discovery
	// REST client's raw path. It can be nil (fake clientset in tests) — degrade
	// to an empty list rather than panicking.
	rc := h.client.Discovery().RESTClient()
	if rc != nil {
		for _, gv := range []string{"karpenter.sh/v1", "karpenter.sh/v1beta1"} {
			raw, err := rc.Get().AbsPath("/apis/" + gv + "/nodepools").DoRaw(c.Request.Context())
			if err == nil {
				c.Data(http.StatusOK, "application/json; charset=utf-8", raw)
				return
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": []any{}})
}
