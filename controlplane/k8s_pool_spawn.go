//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SpawnWorker creates a new worker pod and waits for it to become ready.
// It acquires the spawn semaphore to limit concurrent K8s API calls and
// retries transient API errors with exponential backoff.
func (p *K8sWorkerPool) SpawnWorker(ctx context.Context, id int, image string) error {
	return p.spawnWorker(ctx, id, image, WorkerProfile{}, true)
}

func (p *K8sWorkerPool) spawnWorker(ctx context.Context, id int, image string, profile WorkerProfile, publishIdle bool) error {
	// Test seam: lets unit tests stub pod creation (register the worker in
	// p.workers themselves) without a real K8s spawn / pod-ready wait.
	if p.spawnWorkerFunc != nil {
		return p.spawnWorkerFunc(ctx, id, image, profile)
	}
	// Acquire spawn semaphore to limit concurrent pod creates.
	select {
	case p.spawnSem <- struct{}{}:
		defer func() { <-p.spawnSem }()
	case <-ctx.Done():
		observeSpawnFailure(SpawnFailureReasonContextCanceled, image)
		return ctx.Err()
	}

	if err := p.validateSharedStartupConfigMap(ctx); err != nil {
		observeSpawnFailure(SpawnFailureReasonConfigMap, image)
		return err
	}

	podName := p.podNameForWorker(id)
	_ = p.deleteWorkerRPCSecret(ctx, podName)
	secretName, err := p.ensureWorkerRPCSecret(ctx, podName)
	if err != nil {
		observeSpawnFailure(SpawnFailureReasonSecretCreate, image)
		return fmt.Errorf("ensure worker RPC secret: %w", err)
	}

	// Build pod labels
	podLabels := map[string]string{
		"app":                     "duckgres-worker",
		"duckgres/control-plane":  p.cpID,
		"duckgres/cp-instance-id": controlPlaneIDLabelValue(p.cpInstanceID),
		"duckgres/worker-id":      strconv.Itoa(id),
		"duckgres/owner-epoch":    "0",
	}
	if p.orgID != "" {
		podLabels["duckgres/org"] = p.orgID
	}

	// Build pod spec
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: p.namespace,
			Labels:    podLabels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: int64Ptr(workerTerminationGracePeriodSeconds),
			ServiceAccountName:            p.workerServiceAccountName(),
			AutomountServiceAccountToken:  boolPtr(false),
			PriorityClassName:             p.workerPriorityClassName,
			NodeSelector:                  p.workerNodeSelector,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: boolPtr(true),
				RunAsUser:    int64Ptr(1000),
			},
			Containers: []corev1.Container{
				{
					Name:            "duckdb-worker",
					Image:           image,
					ImagePullPolicy: p.imagePullPolicy,
					Args: []string{
						"--mode", "duckdb-service",
						"--duckdb-listen", fmt.Sprintf(":%d", p.workerPort),
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "grpc",
							ContainerPort: int32(p.workerPort),
							Protocol:      corev1.ProtocolTCP,
						},
					},
					Env: []corev1.EnvVar{
						{
							Name: "DUCKGRES_DUCKDB_TOKEN",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "bearer-token",
								},
							},
						},
						{
							Name:  "DUCKGRES_MODE",
							Value: "duckdb-service",
						},
						{
							Name:  "DUCKGRES_CERT",
							Value: workerRPCMountDir + "/" + workerRPCCertKey,
						},
						{
							Name:  "DUCKGRES_KEY",
							Value: workerRPCMountDir + "/" + workerRPCKeyKey,
						},
						{
							// One client query session per worker pod: the pod's full
							// resources (workerDuckDBLimits gives the session ~75% of pod
							// RAM + all cores) belong to a single query, so queries never
							// contend and a heavy query can't be OOM'd by a co-resident
							// one. The CP scheduler (OrgReservedPool) already never
							// co-assigns; this is the hard worker-side guarantee — a 2nd
							// CreateSession is rejected rather than silently overcommitting.
							// Internal control/maintenance work runs on the worker's side
							// connections (controlDB/warmupDB), which are NOT counted
							// sessions, so this does not starve them.
							Name:  "DUCKGRES_DUCKDB_MAX_SESSIONS",
							Value: "1",
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: boolPtr(false),
					},
					Resources: p.workerResourcesForProfile(profile),
				},
			},
		},
	}
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  "DUCKGRES_SHARED_WARM_WORKER",
		Value: "true",
	})

	// Stamp every log line with pod and node identifiers via the Downward API.
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env,
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

	// Pass OTEL trace config to worker pods.
	for _, envName := range []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_PATH",
	} {
		if v := os.Getenv(envName); v != "" {
			pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
				Name:  envName,
				Value: v,
			})
		}
	}

	// Cache proxy integration: when enabled, workers need to reach the
	// DaemonSet proxy on the same node via the node IP + fixed hostPort.
	// Inject NODE_IP via the Downward API so the worker process can resolve
	// the proxy address at runtime.
	if os.Getenv("DUCKGRES_CACHE_ENABLED") == "true" {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env,
			corev1.EnvVar{
				Name:  "DUCKGRES_CACHE_ENABLED",
				Value: "true",
			},
			corev1.EnvVar{
				Name: "NODE_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.hostIP",
					},
				},
			},
		)
	}

	// Add toleration if configured.
	tolKey, tolValue := p.workerTolerationKey, p.workerTolerationValue
	if tolKey != "" {
		tol := corev1.Toleration{
			Key:    tolKey,
			Effect: corev1.TaintEffectNoSchedule,
		}
		if tolValue != "" {
			tol.Operator = corev1.TolerationOpEqual
			tol.Value = tolValue
		}
		pod.Spec.Tolerations = []corev1.Toleration{tol}
	}

	// Add writable data directory for DuckDB databases
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "data",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "data",
		MountPath: "/data",
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "worker-rpc-tls",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
				Items: []corev1.KeyToPath{
					{Key: workerRPCCertKey, Path: workerRPCCertKey},
					{Key: workerRPCKeyKey, Path: workerRPCKeyKey},
				},
			},
		},
	})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "worker-rpc-tls",
		MountPath: workerRPCMountDir,
		ReadOnly:  true,
	})

	// Add config from ConfigMap if specified
	if p.configMap != "" {
		pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args,
			"--config", p.configPath,
		)
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "duckgres-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: p.configMap},
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "duckgres-config",
			MountPath: "/etc/duckgres",
			ReadOnly:  true,
		})
	}

	// Delete stale pod with the same name if it exists (from a previous run)
	_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(0),
	})

	if publishIdle {
		spawnRecord := p.workerRecordFor(id, nil, 0, configstore.WorkerStateSpawning, "", nil)
		spawnRecord.ProfileCPU = profile.CPU
		spawnRecord.ProfileMemory = profile.Memory
		spawnRecord.TTLMinutes = int(profile.TTL.Minutes())
		_ = p.persistWorkerRecord(spawnRecord)
	}

	// Create pod with exponential backoff on transient errors.
	if err := p.createPodWithBackoff(ctx, pod); err != nil {
		_ = p.deleteWorkerRPCSecret(ctx, podName)
		observeSpawnFailure(SpawnFailureReasonPodCreate, image)
		return err
	}

	// Wait for pod to get an IP via informer (no polling).
	ready, err := p.waitForPodReady(ctx, podName, workerPodReadyTimeout)
	if err != nil {
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		_ = p.deleteWorkerRPCSecret(ctx, podName)
		observeSpawnFailure(SpawnFailureReasonPodReady, image)
		return fmt.Errorf("worker pod %s failed to start: %w", podName, err)
	}

	// Connect gRPC client
	addr := fmt.Sprintf("%s:%d", ready.ip, p.workerPort)
	token, serverCertPEM, err := p.readWorkerRPCSecurity(ctx, podName)
	if err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(cleanupCtx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		_ = p.deleteWorkerRPCSecret(cleanupCtx, podName)
		observeSpawnFailure(SpawnFailureReasonSecretRead, image)
		return fmt.Errorf("read worker RPC security: %w", err)
	}
	client, err := waitForWorkerTCP(addr, token, serverCertPEM, workerSpawnConnectTimeout)
	if err != nil {
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		_ = p.deleteWorkerRPCSecret(ctx, podName)
		observeSpawnFailure(SpawnFailureReasonGRPCConnect, image)
		return fmt.Errorf("worker %d gRPC connection failed: %w", id, err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:          id,
		podName:     podName,
		nodeName:    ready.nodeName,
		image:       image,
		bearerToken: token,
		client:      client,
		done:        done,
	}

	p.mu.Lock()
	p.workers[id] = w
	p.stampNodeFirstSeenLocked(ready.nodeName)
	workerCount := len(p.workers)
	p.mu.Unlock()
	if publishIdle {
		_ = p.persistWorkerRecord(p.workerRecordFor(id, w, w.OwnerEpoch(), configstore.WorkerStateIdle, "", nil))
	}
	observeControlPlaneWorkers(workerCount)

	slog.Info("K8s worker spawned.", "id", id, "worker_pod", podName, "addr", addr)
	return nil
}

func controlPlaneIDLabelValue(cpInstanceID string) string {
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.':
			return r
		default:
			return '-'
		}
	}, cpInstanceID)

	sanitized = strings.TrimFunc(sanitized, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	})
	if sanitized == "" {
		sum := sha1.Sum([]byte(cpInstanceID))
		return hex.EncodeToString(sum[:])[:12]
	}
	if len(sanitized) <= 63 {
		return sanitized
	}

	sum := sha1.Sum([]byte(cpInstanceID))
	suffix := hex.EncodeToString(sum[:])[:12]
	prefixLen := 63 - len(suffix) - 1
	if prefixLen < 1 {
		prefixLen = 1
	}
	prefix := strings.TrimRightFunc(sanitized[:prefixLen], func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	})
	if prefix == "" {
		return suffix
	}
	return prefix + "-" + suffix
}

// createPodWithBackoff creates a pod, retrying transient K8s API errors
// with exponential backoff (500ms, 1s, 2s, 4s).
func (p *K8sWorkerPool) createPodWithBackoff(ctx context.Context, pod *corev1.Pod) error {
	backoff := 500 * time.Millisecond
	const maxRetries = 4

	for attempt := 0; attempt <= maxRetries; attempt++ {
		_, err := p.clientset.CoreV1().Pods(p.namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err == nil {
			return nil
		}
		// Don't retry on permanent errors (invalid spec, quota exceeded, etc.)
		if errors.IsInvalid(err) || errors.IsForbidden(err) || errors.IsAlreadyExists(err) {
			return fmt.Errorf("create worker pod %s: %w", pod.Name, err)
		}
		if attempt == maxRetries {
			return fmt.Errorf("create worker pod %s after %d retries: %w", pod.Name, maxRetries, err)
		}
		slog.Warn("Transient K8s API error creating pod, retrying.",
			"worker_pod", pod.Name, "attempt", attempt+1, "backoff", backoff, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return nil // unreachable
}

// podReadyInfo is signaled through podReady when a pod becomes Running with
// an IP. Carrying nodeName alongside the IP lets the spawn path stamp
// nodeName onto the ManagedWorker without a second API round-trip.
type podReadyInfo struct {
	ip       string
	nodeName string
}

// waitForPodReady waits for a pod to become Running with an IP, using the
// informer instead of polling the API. Falls back to a single API check
// in case the informer event fired before we registered the channel.
func (p *K8sWorkerPool) waitForPodReady(ctx context.Context, podName string, timeout time.Duration) (podReadyInfo, error) {
	ch := make(chan podReadyInfo, 1)
	p.podReady.Store(podName, ch)
	defer p.podReady.Delete(podName)

	// Check once in case the pod is already running (informer event already fired).
	pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil && pod.Status.PodIP != "" && pod.Status.Phase == corev1.PodRunning {
		return podReadyInfo{ip: pod.Status.PodIP, nodeName: pod.Spec.NodeName}, nil
	}
	if err == nil && pod.Status.Phase == corev1.PodFailed {
		return podReadyInfo{}, fmt.Errorf("pod %s failed", podName)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case info, ok := <-ch:
		if !ok || info.ip == "" {
			return podReadyInfo{}, fmt.Errorf("pod %s failed or was deleted", podName)
		}
		return info, nil
	case <-timer.C:
		return podReadyInfo{}, fmt.Errorf("timeout waiting for pod %s to become ready", podName)
	case <-ctx.Done():
		return podReadyInfo{}, ctx.Err()
	}
}

// waitForWorkerTCP connects to a worker over TCP and verifies its health.
func waitForWorkerTCP(addr, bearerToken string, serverCertPEM []byte, timeout time.Duration) (*flightsql.Client, error) {
	return waitForWorkerTCPWithMetadata(addr, bearerToken, serverCertPEM, timeout, server.WorkerHealthCheckPayload{})
}

// waitForWorkerTCPWithMetadata connects to a worker over TCP and verifies its
// health using the provided metadata payload. For adopted hot-idle workers,
// the payload must include the claimed epoch so the worker doesn't reject the
// health check with "stale owner epoch".
func waitForWorkerTCPWithMetadata(addr, bearerToken string, serverCertPEM []byte, timeout time.Duration, hcPayload server.WorkerHealthCheckPayload) (*flightsql.Client, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	attempts := 0
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCertPEM) {
		return nil, fmt.Errorf("parse worker RPC server certificate")
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: workerRPCDNSName,
	}

	for time.Now().Before(deadline) {
		var dialOpts []grpc.DialOption
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(flightclient.MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(flightclient.MaxGRPCMessageSize),
		))
		dialOpts = append(dialOpts, server.OTELGRPCClientHandler())
		if bearerToken != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&workerTLSBearerCreds{token: bearerToken}))
		}

		client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = doHealthCheckWithMetadata(ctx, client, hcPayload)
			cancel()
			if err == nil {
				return client, nil
			}
			lastErr = err
			_ = client.Close()
		} else {
			lastErr = fmt.Errorf("grpc dial: %w", err)
		}
		attempts++
		if attempts <= 3 || attempts%10 == 0 {
			slog.Debug("waitForWorkerTCP health check attempt failed.", "addr", addr, "attempt", attempts, "error", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("timeout connecting to worker at %s (last error: %v, attempts: %d)", addr, lastErr, attempts)
}

// spawnWorkerBackground spawns a worker pod without blocking AcquireWorker.
// The new worker becomes available for future sessions once ready.
func (p *K8sWorkerPool) spawnWorkerBackground(id int, image string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	err := p.spawnWorker(ctx, id, image, WorkerProfile{}, true)

	p.mu.Lock()
	p.spawning--
	p.mu.Unlock()

	if err != nil {
		slog.Warn("Background worker spawn failed.", "worker", id, "error", err)
	}
}

// spawnReservedWorkerForSlot foreground-spawns the pod for an already-allocated
// spawning slot (see reserveSharedWorkerDecision, which created the slot and
// enforced the per-org + global caps via CreateSpawningWorkerSlot) and reserves
// it, returning it in Reserved state (the caller activates it, same as a claimed
// worker). This is the only spawn path now that the warm pool is gone: a request
// either reuses a hot-idle worker (claim) or spawns one here. The pod is sized
// from the request profile, or the pool-global default for a default (nil
// profile) request (workerResourcesForProfile); the reserved record round-trips
// the profile + TTL.
func (p *K8sWorkerPool) spawnReservedWorkerForSlot(ctx context.Context, id int, assignment *WorkerAssignment) (_ *ManagedWorker, err error) {
	if assignment == nil {
		return nil, fmt.Errorf("spawnReservedWorkerForSlot requires an assignment")
	}
	// spawn covers pod create → pod-ready (incl. node provisioning) → gRPC
	// connect → reserve. Named-return defer so every failure stage observes.
	spawnStart := time.Now()
	defer func() {
		observeAcquirePhase("spawn", time.Since(spawnStart), err)
	}()
	// A default (nil profile) request spawns a default-sized worker: the zero
	// profile makes workerResourcesForProfile fall back to the pool-global request.
	var profile WorkerProfile
	if assignment.Profile != nil {
		profile = *assignment.Profile
	}

	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		// The slot row (if any) stays in spawning state; the janitor's
		// stale-spawning sweep reconciles it, same as a failed spawn below.
		return nil, fmt.Errorf("pool is shutting down")
	}
	p.spawning++
	p.mu.Unlock()
	err = p.spawnWorker(ctx, id, assignment.Image, profile, false)
	p.mu.Lock()
	p.spawning--
	p.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("spawn sized worker: %w", err)
	}

	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return nil, fmt.Errorf("sized worker %d missing after spawn", id)
	}
	// spawnWorker does not stamp the profile on the in-memory worker; do it here
	// so reuse-matching and the reserved record carry the size + TTL.
	w.profile = profile
	nextState, err := w.SharedState().Transition(WorkerLifecycleReserved, assignment)
	if err != nil {
		p.mu.Unlock()
		p.retireWorkerWithReason(id, RetireReasonCrash, LifecycleOriginReserveFailure)
		return nil, err
	}
	if err := w.SetSharedState(nextState); err != nil {
		p.mu.Unlock()
		p.retireWorkerWithReason(id, RetireReasonCrash, LifecycleOriginReserveFailure)
		return nil, err
	}
	w.SetOwnerCPInstanceID(p.cpInstanceID)
	w.IncrementOwnerEpoch()
	w.reservedAt = time.Now()
	reservedRecord := p.workerRecordFor(id, w, w.OwnerEpoch(), configstore.WorkerStateReserved, "", nil)
	p.mu.Unlock()
	_ = p.persistWorkerRecord(reservedRecord)
	return w, nil
}

// workerResources returns resource requests and limits for a worker pod.
// Set via DUCKGRES_K8S_WORKER_CPU_REQUEST / DUCKGRES_K8S_WORKER_MEMORY_REQUEST.
// Returns empty (BestEffort) if neither is set.
// When set, limits are equal to requests (Guaranteed QoS).
func (p *K8sWorkerPool) workerResources() corev1.ResourceRequirements {
	return p.workerResourcesForProfile(WorkerProfile{})
}

// workerResourcesForProfile builds the pod resource requirements for a worker of
// the given profile: the profile's CPU/Memory when set, otherwise the pool-global
// request (today's behavior). Requests are mirrored to Limits so the pod is
// Guaranteed QoS — required before bin-packing colocated workers. A colocated
// profile always carries non-empty CPU/Memory (resolver-enforced), so it never
// degrades to BestEffort.
func (p *K8sWorkerPool) workerResourcesForProfile(profile WorkerProfile) corev1.ResourceRequirements {
	// Workers must never be BestEffort: with no pod anti-affinity, resource
	// requests are the ONLY thing keeping two workers from overcommitting a
	// node (each worker's single session sizes itself off the whole pod —
	// workerDuckDBLimits — so co-resident requestless workers would each
	// believe they own the node's RAM). Fall back to the built-in default
	// shape when neither the profile nor the pool-global request sets one.
	cpuReq := profile.CPU
	if cpuReq == "" {
		cpuReq = p.workerCPURequest
	}
	if cpuReq == "" {
		cpuReq = defaultWorkerCPU
	}
	memReq := profile.Memory
	if memReq == "" {
		memReq = p.workerMemoryRequest
	}
	if memReq == "" {
		memReq = defaultWorkerMemory
	}
	requests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpuReq),
		corev1.ResourceMemory: resource.MustParse(memReq),
	}
	limits := make(corev1.ResourceList, len(requests))
	for k, v := range requests {
		limits[k] = v.DeepCopy()
	}
	return corev1.ResourceRequirements{Requests: requests, Limits: limits}
}
