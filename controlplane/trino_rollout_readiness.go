//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const rolloutCapabilityHeader = "X-Gateway-Transaction-Admin-Token"
const rolloutReadinessPrefix = "/internal/trino/rollout-readiness/"

type rolloutImage struct {
	SpecImage      string `json:"specImage"`
	RuntimeImageID string `json:"runtimeImageID"`
}

type rolloutPods struct {
	Total             int            `json:"total"`
	Terminating       int            `json:"terminating"`
	Coordinators      int            `json:"coordinators"`
	ReadyCoordinators int            `json:"readyCoordinators"`
	Workers           int            `json:"workers"`
	ReadyWorkers      int            `json:"readyWorkers"`
	Images            []rolloutImage `json:"images"`
}

type rolloutCoordinatorFacts struct {
	NodeID            string `json:"nodeId"`
	CoordinatorID     string `json:"coordinatorId"`
	RegisteredWorkers int    `json:"registeredWorkers"`
	members           []rolloutNodeMember
}

type rolloutNodeMember struct {
	ip          string
	coordinator bool
}

type rolloutCanaryFacts struct {
	Authenticated       bool `json:"authenticated"`
	CatalogMetadataRead bool `json:"catalogMetadataRead"`
}

type rolloutReadinessResponse struct {
	SchemaVersion int                      `json:"schemaVersion"`
	Cell          string                   `json:"cell"`
	RoutingGroup  string                   `json:"routingGroup"`
	Color         string                   `json:"color"`
	BackendName   string                   `json:"backendName"`
	ObservedAt    time.Time                `json:"observedAt"`
	Pods          rolloutPods              `json:"pods"`
	Coordinator   *rolloutCoordinatorFacts `json:"coordinator,omitempty"`
	Canary        *rolloutCanaryFacts      `json:"canary,omitempty"`
}

type rolloutReadinessSlot struct {
	cell, routingGroup, color, namespace, backendName string
	coordinatorURL, tlsServerName                     string
	observer                                          func() (string, string)
	client                                            *http.Client
	canary                                            rolloutCanaryCredential
}

type rolloutCanaryCredential struct {
	Cell      string `json:"cell"`
	OrgID     string `json:"orgID"`
	Principal string `json:"principal"`
	Password  string `json:"password"`
}

type trinoRolloutReadinessHandler struct {
	token   string
	kube    kubernetes.Interface
	slots   map[string]rolloutReadinessSlot
	limit   chan struct{}
	timeout time.Duration
	probe   func(context.Context, rolloutReadinessSlot) (*rolloutCoordinatorFacts, error)
}

func (h *trinoRolloutReadinessHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	tokens := r.Header.Values(rolloutCapabilityHeader)
	if h.token == "" || len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(h.token)) != 1 {
		rolloutReadinessError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if r.Method != http.MethodGet || r.URL.RawQuery != "" {
		rolloutReadinessError(w, http.StatusMethodNotAllowed, "unsupported_request")
		return
	}
	key, prefixOK := strings.CutPrefix(r.URL.Path, rolloutReadinessPrefix)
	slot, exists := h.slots[key]
	if !prefixOK || !exists {
		rolloutReadinessError(w, http.StatusNotFound, "unknown_slot")
		return
	}
	select {
	case h.limit <- struct{}{}:
		defer func() { <-h.limit }()
	default:
		rolloutReadinessError(w, http.StatusServiceUnavailable, "busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	selector := labels.Set{"posthog.com/trino-cell": slot.cell, "posthog.com/trino-color": slot.color}.String()
	pods, err := h.kube.CoreV1().Pods(slot.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 1001})
	if err != nil || pods == nil || len(pods.Items) > 1000 || pods.Continue != "" {
		rolloutReadinessError(w, http.StatusServiceUnavailable, "inventory_unavailable")
		return
	}
	inventory, err := rolloutPodInventory(pods.Items)
	if err != nil {
		rolloutReadinessError(w, http.StatusServiceUnavailable, "inventory_invalid")
		return
	}
	result := rolloutReadinessResponse{SchemaVersion: 1, Cell: slot.cell, RoutingGroup: slot.routingGroup, Color: slot.color, BackendName: slot.backendName, Pods: inventory}
	if inventory.Total > 0 {
		if inventory.Terminating != 0 || inventory.Coordinators != 1 || inventory.ReadyCoordinators != 1 || inventory.Workers == 0 || inventory.ReadyWorkers != inventory.Workers {
			rolloutReadinessError(w, http.StatusServiceUnavailable, "pods_not_ready")
			return
		}
		facts, err := h.probe(ctx, slot)
		if err != nil || facts == nil || facts.RegisteredWorkers != inventory.Workers || !rolloutMembersMatchPods(facts.members, pods.Items) {
			rolloutReadinessError(w, http.StatusServiceUnavailable, "coordinator_not_ready")
			return
		}
		result.Coordinator = facts
		result.Canary = &rolloutCanaryFacts{Authenticated: true, CatalogMetadataRead: true}
	}
	if ctx.Err() != nil {
		rolloutReadinessError(w, http.StatusServiceUnavailable, "observation_expired")
		return
	}
	result.ObservedAt = time.Now().UTC()
	_ = json.NewEncoder(w).Encode(result)
}

func rolloutMembersMatchPods(members []rolloutNodeMember, pods []corev1.Pod) bool {
	if len(members) != len(pods) {
		return false
	}
	expected := make(map[string]bool, len(pods))
	for _, pod := range pods {
		ip, err := netip.ParseAddr(pod.Status.PodIP)
		if err != nil {
			return false
		}
		key := ip.Unmap().String()
		if _, duplicate := expected[key]; duplicate {
			return false
		}
		expected[key] = pod.Labels["app.kubernetes.io/component"] == "coordinator"
	}
	for _, member := range members {
		coordinator, exists := expected[member.ip]
		if !exists || coordinator != member.coordinator {
			return false
		}
		delete(expected, member.ip)
	}
	return len(expected) == 0
}

func rolloutReadinessError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
}

func rolloutPodInventory(pods []corev1.Pod) (rolloutPods, error) {
	result := rolloutPods{Total: len(pods), Images: []rolloutImage{}}
	images := make(map[rolloutImage]bool)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			result.Terminating++
		}
		role := pod.Labels["app.kubernetes.io/component"]
		if role != "coordinator" && role != "worker" {
			return result, errors.New("unknown Trino workload role")
		}
		main := pod.Labels["app.kubernetes.io/name"]
		if main == "" {
			return result, errors.New("missing Trino container identity")
		}
		image := rolloutImage{}
		matches := 0
		for _, container := range pod.Spec.Containers {
			if container.Name == main {
				image.SpecImage = container.Image
				matches++
			}
		}
		ready := false
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == main {
				image.RuntimeImageID = status.ImageID
				ready = status.Ready && status.State.Running != nil
			}
		}
		if matches != 1 || image.SpecImage == "" || image.RuntimeImageID == "" {
			return result, errors.New("missing Trino image identity")
		}
		podReady := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				podReady = condition.Status == corev1.ConditionTrue
			}
		}
		ready = ready && podReady && pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil
		if role == "coordinator" {
			result.Coordinators++
			if ready {
				result.ReadyCoordinators++
			}
		} else {
			result.Workers++
			if ready {
				result.ReadyWorkers++
			}
		}
		images[image] = true
	}
	for image := range images {
		result.Images = append(result.Images, image)
	}
	sort.Slice(result.Images, func(i, j int) bool {
		if result.Images[i].SpecImage != result.Images[j].SpecImage {
			return result.Images[i].SpecImage < result.Images[j].SpecImage
		}
		return result.Images[i].RuntimeImageID < result.Images[j].RuntimeImageID
	})
	return result, nil
}
