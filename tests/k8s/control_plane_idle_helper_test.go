//go:build k8s_integration

package k8s_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// waitForControlPlaneIdle blocks until the control plane's worker pool
// looks quiescent: every worker pod is Running+Ready with no
// DeletionTimestamp set, and the set of worker pod names has been
// stable for `stableTicks` consecutive observations.
//
// This is the test-isolation primitive: tests that delete or otherwise
// disturb a worker pod should call this at the end (typically via
// t.Cleanup) so the next test doesn't start while the control plane is
// still mid-housekeeping. Without it, trailing pod create/delete
// activity from warm-pool replenishment and post-retire cleanup keeps
// the apiserver busy enough that the test's port-forward can drop at
// arbitrary moments, causing the next test's retry layer to re-issue
// in-flight queries — which manifests as "duplicate row" or other
// at-least-once artefacts in tests that don't assume idempotent ops.
//
// The signals here are K8s-API-only: pod phase, the Ready condition,
// DeletionTimestamp, and the name set across ticks. The runtime-store
// transitions (spawning, activating, draining) are not directly
// observed, but every CP-side state change that has visible cluster
// side effects (and thus drives apiserver load) does show up here,
// because the CP cannot move a worker row to a stable state (idle,
// hot, hot_idle) without first making K8s API calls.
//
// `stableTicks` defaults to 3 and `tickInterval` to 1s — three
// consecutive identical observations a second apart is the smallest
// window that reliably filters out the "create pod, immediately mark
// retired, delete pod" churn pattern we see during warm-pool
// reconciliation tick storms.
func waitForControlPlaneIdle(t *testing.T, timeout time.Duration) {
	t.Helper()
	const (
		stableTicks  = 3
		tickInterval = 1 * time.Second
	)
	deadline := time.Now().Add(timeout)
	lastSnapshot := ""
	lastDetail := ""
	stable := 0
	for time.Now().Before(deadline) {
		pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: "app=duckgres-worker",
		})
		if err != nil {
			// Transient API errors during quiescence checks shouldn't
			// fail the test outright — log and retry. A persistent
			// failure will hit the outer deadline.
			t.Logf("waitForControlPlaneIdle: list worker pods: %v", err)
			time.Sleep(tickInterval)
			continue
		}
		snapshot, detail, allHealthy := summarizeWorkerPodsForIdleness(pods.Items)
		if allHealthy && snapshot == lastSnapshot {
			stable++
			if stable >= stableTicks {
				return
			}
		} else {
			stable = 0
			lastSnapshot = snapshot
		}
		lastDetail = detail
		time.Sleep(tickInterval)
	}
	t.Fatalf("waitForControlPlaneIdle: control plane did not quiesce within %s; last observation: %s", timeout, lastDetail)
}

// summarizeWorkerPodsForIdleness reduces a worker-pod list to a stable
// comparison key plus a human-readable detail string. allHealthy is
// false the moment any pod is in a transitional state (deleting, not
// running, or not ready).
func summarizeWorkerPodsForIdleness(pods []corev1.Pod) (snapshot, detail string, allHealthy bool) {
	type podState struct {
		name     string
		phase    corev1.PodPhase
		ready    bool
		deleting bool
	}
	states := make([]podState, 0, len(pods))
	allHealthy = true
	for i := range pods {
		pod := &pods[i]
		s := podState{name: pod.Name, phase: pod.Status.Phase}
		if pod.DeletionTimestamp != nil {
			s.deleting = true
			allHealthy = false
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				s.ready = true
			}
		}
		if pod.Status.Phase != corev1.PodRunning || !s.ready {
			allHealthy = false
		}
		states = append(states, s)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].name < states[j].name })
	names := make([]string, len(states))
	details := make([]string, len(states))
	for i, s := range states {
		names[i] = s.name
		flag := "ok"
		switch {
		case s.deleting:
			flag = "deleting"
		case !s.ready:
			flag = fmt.Sprintf("phase=%s ready=false", s.phase)
		case s.phase != corev1.PodRunning:
			flag = fmt.Sprintf("phase=%s", s.phase)
		}
		details[i] = fmt.Sprintf("%s(%s)", s.name, flag)
	}
	return strings.Join(names, ","), strings.Join(details, " "), allHealthy
}
