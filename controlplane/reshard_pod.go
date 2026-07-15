//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Reshard operations execute in a DEDICATED per-operation pod
// (duckgres-reshard-op-<id>, --mode reshard-runner) instead of inside a
// control-plane process: a reshard pg_dumps and copies whole catalogs, and the
// CP's small resource envelope must never be shared with that (a ~20k-table
// catalog pg_dump OOM-killed a 512Mi CP pod mid-reshard). The pod runs the CP's
// OWN image (it carries pg_dump and the duckgres binary) under the CP's OWN
// ServiceAccount (the runner patches Duckling CRs — the CP SA already holds
// that RBAC; no new grants), and inherits an allowlist of the CP's env spec
// VERBATIM (value or secretKeyRef — so the config-store DSN / internal secret
// stay exactly as secret-referenced as they are on the CP deployment, nothing
// secret is copied by value into the pod spec).

// reshardPodAppLabel / reshardPodOpIDLabel identify reshard runner pods.
const (
	reshardPodAppLabel            = "duckgres-reshard"
	reshardPodOpIDLabel           = "duckgres-op-id"
	reshardPodSpawnedAtAnnotation = "duckgres.posthog.com/runner-spawned-at"
)

// reshardPodDefaultCPU / reshardPodDefaultMemory are the runner pod's resource
// shape (requests=limits, Guaranteed QoS) unless overridden by the env-only
// knobs DUCKGRES_RESHARD_POD_CPU / DUCKGRES_RESHARD_POD_MEMORY.
const (
	reshardPodDefaultCPU    = "2"
	reshardPodDefaultMemory = "8Gi"
)

// reshardPodTerminationGraceSeconds gives a deleted runner pod time to roll an
// in-flight operation back before SIGKILL.
const reshardPodTerminationGraceSeconds = int64(600)

// reshardPodEnvAllowlist is the set of CP-container env vars replicated onto
// the runner pod, verbatim (value or valueFrom). Everything the reshard-runner
// mode reads, nothing more.
var reshardPodEnvAllowlist = []string{
	"DUCKGRES_CONFIG_STORE",
	"DUCKGRES_CONFIG_POLL_INTERVAL",
	"DUCKGRES_INTERNAL_SECRET",
	"DUCKGRES_AWS_REGION",
	"DUCKGRES_LOG_LEVEL",
	"DUCKGRES_RESHARD_FLIP_TIMEOUT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_PATH",
}

// ReshardPodName is the deterministic per-operation pod name.
func ReshardPodName(opID int64) string {
	return fmt.Sprintf("duckgres-reshard-op-%d", opID)
}

// ReshardPodSpawner creates/deletes reshard runner pods. Used by the admin
// start handler (spawn on POST) and by the leader reconciler (respawn dead
// runners, reap pods of terminal ops).
type ReshardPodSpawner struct {
	clientset   kubernetes.Interface
	namespace   string
	selfPodName string // the CP's own pod (image/SA/env template)
	adminPort   int    // the CP admin API port the password URL points at

	cpuRequest    string // env-only DUCKGRES_RESHARD_POD_CPU (default 2)
	memoryRequest string // env-only DUCKGRES_RESHARD_POD_MEMORY (default 8Gi)
}

// NewReshardPodSpawner builds a spawner over the shared in-cluster clientset.
// cpu/memory come from the resolved env-only knobs ("" → defaults).
func NewReshardPodSpawner(clientset kubernetes.Interface, namespace, selfPodName, cpu, memory string) *ReshardPodSpawner {
	if cpu == "" {
		cpu = reshardPodDefaultCPU
	}
	if memory == "" {
		memory = reshardPodDefaultMemory
	}
	return &ReshardPodSpawner{
		clientset:     clientset,
		namespace:     namespace,
		selfPodName:   selfPodName,
		adminPort:     8080,
		cpuRequest:    cpu,
		memoryRequest: memory,
	}
}

// PasswordURLForOp renders the URL the runner pod pulls the ephemeral external
// target password from: THIS replica's pod IP + admin port (the stash is in
// this process's memory, so the URL must point here, not at the service VIP).
// Empty when the pod IP cannot be determined.
func (s *ReshardPodSpawner) PasswordURLForOp(opID int64) string {
	ip := selfPodIP()
	if ip == "" {
		return ""
	}
	host := ip
	if strings.Contains(ip, ":") { // IPv6 literal
		host = "[" + ip + "]"
	}
	return fmt.Sprintf("http://%s:%d/api/v1/reshards/%d/password", host, s.adminPort, opID)
}

// selfPodIP resolves this pod's own IP: the POD_IP downward-API env when the
// deployment sets it, else the first global-unicast interface address (a pod
// netns has exactly its pod IP there).
func selfPodIP() string {
	if v := strings.TrimSpace(os.Getenv("POD_IP")); v != "" {
		return v
	}
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
			return ipnet.IP.String()
		}
	}
	return ""
}

// SpawnReshardPod creates the runner pod for op. It does NOT wait for the pod
// to run — the pod claims the op row itself and the leader reconciler backstops
// a pod that never does. A leftover same-name pod (previous attempt) is deleted
// first.
func (s *ReshardPodSpawner) SpawnReshardPod(ctx context.Context, op *configstore.ReshardOperation) error {
	if s == nil || s.clientset == nil {
		return fmt.Errorf("no reshard pod spawner (kubernetes API unavailable)")
	}
	cpPod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, s.selfPodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read own control-plane pod %s/%s (the runner pod template): %w", s.namespace, s.selfPodName, err)
	}
	if len(cpPod.Spec.Containers) == 0 {
		return fmt.Errorf("control-plane pod %s has no containers", s.selfPodName)
	}
	cpContainer := cpPod.Spec.Containers[0]

	// Inherit the allowlisted env VERBATIM — a secretKeyRef stays a
	// secretKeyRef; nothing secret is copied by value into the pod spec.
	var env []corev1.EnvVar
	for _, name := range reshardPodEnvAllowlist {
		for i := range cpContainer.Env {
			if cpContainer.Env[i].Name == name {
				env = append(env, *cpContainer.Env[i].DeepCopy())
				break
			}
		}
	}
	env = append(env,
		corev1.EnvVar{Name: "DUCKGRES_RESHARD_OP_ID", Value: strconv.FormatInt(op.ID, 10)},
		corev1.EnvVar{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		corev1.EnvVar{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		},
	)
	// The password pull URL (ext targets only; the URL is on the op row so a
	// reconciler respawn re-wires the same handoff).
	if op.PasswordURL != "" {
		env = append(env, corev1.EnvVar{Name: "DUCKGRES_RESHARD_PASSWORD_URL", Value: op.PasswordURL})
	}

	requests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(s.cpuRequest),
		corev1.ResourceMemory: resource.MustParse(s.memoryRequest),
	}
	limits := make(corev1.ResourceList, len(requests))
	for k, v := range requests {
		limits[k] = v.DeepCopy()
	}

	// Scheduling: inherit the CP's nodeSelector and (non-auto-injected)
	// tolerations, and pin the node ARCH to this process's own — the runner
	// runs the CP's image, which is arch-specific, and on a mixed-arch
	// nodepool an unpinned pod can land on the wrong arch (exec format error).
	nodeSelector := map[string]string{}
	for k, v := range cpPod.Spec.NodeSelector {
		nodeSelector[k] = v
	}
	if _, ok := nodeSelector["kubernetes.io/arch"]; !ok {
		nodeSelector["kubernetes.io/arch"] = runtime.GOARCH
	}
	var tolerations []corev1.Toleration
	for _, tol := range cpPod.Spec.Tolerations {
		if strings.HasPrefix(tol.Key, "node.kubernetes.io/") {
			continue // kubelet auto-injected (not-ready/unreachable grace)
		}
		tolerations = append(tolerations, tol)
	}

	podName := ReshardPodName(op.ID)
	grace := reshardPodTerminationGraceSeconds
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: s.namespace,
			Labels: map[string]string{
				"app":               reshardPodAppLabel,
				reshardPodOpIDLabel: strconv.FormatInt(op.ID, 10),
			},
			// A reshard is a long single-shot maintenance operation; don't let
			// node consolidation kill it mid-copy.
			Annotations: map[string]string{
				doNotDisruptAnnotation:        "true",
				reshardPodSpawnedAtAnnotation: time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			// The CP's own SA: the runner patches Duckling CRs and reads the
			// config store — grants the CP already holds. No new RBAC.
			ServiceAccountName: cpPod.Spec.ServiceAccountName,
			NodeSelector:       nodeSelector,
			Tolerations:        tolerations,
			Containers: []corev1.Container{
				{
					Name:            "reshard-runner",
					Image:           cpContainer.Image,
					ImagePullPolicy: cpContainer.ImagePullPolicy,
					Args:            []string{"--mode", "reshard-runner"},
					Env:             env,
					Resources:       corev1.ResourceRequirements{Requests: requests, Limits: limits},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: boolPtr(false),
					},
				},
			},
		},
	}

	// Replace a leftover pod from a previous attempt (reconciler respawn).
	_ = s.clientset.CoreV1().Pods(s.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(0),
	})
	if _, err := s.clientset.CoreV1().Pods(s.namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// The zero-grace delete above hasn't finished; the reconciler
			// retries next tick.
			return fmt.Errorf("previous reshard pod %s still terminating: %w", podName, err)
		}
		return fmt.Errorf("create reshard pod %s: %w", podName, err)
	}
	return nil
}

// GetReshardPod returns the runner pod for opID, or nil when absent.
func (s *ReshardPodSpawner) GetReshardPod(ctx context.Context, opID int64) (*corev1.Pod, error) {
	pod, err := s.clientset.CoreV1().Pods(s.namespace).Get(ctx, ReshardPodName(opID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return pod, nil
}

// DeleteReshardPod removes the runner pod for opID (graceful; the pod's
// termination grace gives an in-flight op time to roll back).
func (s *ReshardPodSpawner) DeleteReshardPod(ctx context.Context, opID int64) error {
	err := s.clientset.CoreV1().Pods(s.namespace).Delete(ctx, ReshardPodName(opID), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// ListReshardPods returns all reshard runner pods in the namespace.
func (s *ReshardPodSpawner) ListReshardPods(ctx context.Context) ([]corev1.Pod, error) {
	list, err := s.clientset.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + reshardPodAppLabel,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}
