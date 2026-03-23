//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const defaultActivatingTimeout = 2 * time.Minute

// K8sWorkerPool manages worker pods in Kubernetes.
type K8sWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*ManagedWorker
	nextWorkerID int
	spawning     int
	maxWorkers   int
	minWorkers   int
	idleTimeout  time.Duration
	shuttingDown bool
	shutdownCh   chan struct{}

	clientset            kubernetes.Interface
	namespace            string
	cpID                 string
	cpUID                types.UID
	workerImage          string
	workerPort           int
	secretName           string
	configMap            string
	configPath           string
	imagePullPolicy      corev1.PullPolicy
	serviceAccount       string
	memoryBudget         int64      // total memory budget in bytes
	orgID                string     // org ID for pod labels (multi-tenant mode)
	workerIDGenerator    func() int // shared ID generator across orgs (nil = internal counter)
	sharedWarmActivation bool
	cachedToken          string // cached bearer token (immutable after setup)
	informer             cache.SharedIndexInformer
	stopInform           chan struct{}
	spawnSem             chan struct{} // limits concurrent pod creates to avoid overwhelming the K8s API
	podReady             sync.Map      // podName -> chan string (pod IP); signaled by informer

	spawnWarmWorkerFunc           func(ctx context.Context, id int) error
	spawnWarmWorkerBackgroundFunc func(id int)
	activateTenantFunc            func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error

	activatingTimeout time.Duration // max time a worker can stay in reserved/activating before being reaped
}

// NewK8sWorkerPool creates a K8sWorkerPool using in-cluster credentials.
func NewK8sWorkerPool(cfg K8sWorkerPoolConfig) (*K8sWorkerPool, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}
	// Default client-go limits are 5 QPS / burst 10, which triggers
	// client-side throttling when spawning multiple workers concurrently.
	restCfg.QPS = 50
	restCfg.Burst = 100
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return newK8sWorkerPool(cfg, clientset)
}

// newK8sWorkerPool is the internal constructor that accepts an injectable clientset (for testing).
func newK8sWorkerPool(cfg K8sWorkerPoolConfig, clientset kubernetes.Interface) (*K8sWorkerPool, error) {
	if cfg.WorkerImage == "" {
		return nil, fmt.Errorf("k8s worker image is required")
	}
	if cfg.Namespace == "" {
		// Auto-detect namespace from service account
		ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return nil, fmt.Errorf("k8s namespace not set and auto-detection failed: %w", err)
		}
		cfg.Namespace = string(ns)
	}
	if cfg.CPID == "" {
		hostname, _ := os.Hostname()
		cfg.CPID = hostname
	}
	if cfg.WorkerPort == 0 {
		cfg.WorkerPort = 8816
	}
	if cfg.ConfigPath == "" {
		cfg.ConfigPath = "/etc/duckgres/duckgres.yaml"
	}

	// Allow up to 3 concurrent pod creates to limit K8s API pressure.
	spawnConcurrency := 3
	pool := &K8sWorkerPool{
		workers:              make(map[int]*ManagedWorker),
		maxWorkers:           cfg.MaxWorkers,
		idleTimeout:          cfg.IdleTimeout,
		shutdownCh:           make(chan struct{}),
		stopInform:           make(chan struct{}),
		clientset:            clientset,
		namespace:            cfg.Namespace,
		cpID:                 cfg.CPID,
		workerImage:          cfg.WorkerImage,
		workerPort:           cfg.WorkerPort,
		secretName:           cfg.SecretName,
		configMap:            cfg.ConfigMap,
		configPath:           cfg.ConfigPath,
		imagePullPolicy:      corev1.PullPolicy(cfg.ImagePullPolicy),
		serviceAccount:       cfg.ServiceAccount,
		memoryBudget:         cfg.MemoryBudget,
		orgID:                cfg.OrgID,
		workerIDGenerator:    cfg.WorkerIDGenerator,
		sharedWarmActivation: cfg.SharedWarmActivation,
		spawnSem:             make(chan struct{}, spawnConcurrency),
	}

	// Resolve CP pod UID for owner references
	if err := pool.resolveCPUID(context.Background()); err != nil {
		slog.Warn("Could not resolve CP pod UID for owner references. Worker pods will not be garbage-collected if CP is deleted.", "error", err)
	}

	// Ensure bearer token secret exists
	if err := pool.ensureBearerTokenSecret(context.Background()); err != nil {
		return nil, fmt.Errorf("ensure bearer token secret: %w", err)
	}

	// Start SharedInformer for watching worker pods
	pool.startInformer()

	observeControlPlaneWorkers(0)
	go pool.idleReaper()

	return pool, nil
}

func (p *K8sWorkerPool) resolveCPUID(ctx context.Context) error {
	pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, p.cpID, metav1.GetOptions{})
	if err != nil {
		return err
	}
	p.cpUID = pod.UID
	return nil
}

// ensureBearerTokenSecret ensures the bearer token K8s Secret exists.
// If no secret name is configured, it generates one named "duckgres-worker-token-<cpID>".
// If the secret doesn't exist, it creates one with a random 32-byte hex token.
func (p *K8sWorkerPool) ensureBearerTokenSecret(ctx context.Context) error {
	if p.secretName == "" {
		p.secretName = "duckgres-worker-token-" + p.cpID
	}

	existing, err := p.clientset.CoreV1().Secrets(p.namespace).Get(ctx, p.secretName, metav1.GetOptions{})
	if err == nil {
		// Secret exists — verify it has the bearer-token key
		if _, ok := existing.Data["bearer-token"]; ok {
			return nil
		}
		// Secret exists but missing bearer-token key — populate it
		slog.Info("Bearer token secret exists but missing bearer-token key, populating.", "name", p.secretName)
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return fmt.Errorf("generate bearer token: %w", err)
		}
		if existing.Data == nil {
			existing.Data = make(map[string][]byte)
		}
		existing.Data["bearer-token"] = []byte(hex.EncodeToString(b))
		_, err = p.clientset.CoreV1().Secrets(p.namespace).Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("update secret %s with bearer token: %w", p.secretName, err)
		}
		slog.Info("Populated bearer token in existing secret.", "name", p.secretName)
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("get secret %s: %w", p.secretName, err)
	}

	// Generate a random bearer token
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return fmt.Errorf("generate bearer token: %w", err)
	}
	token := hex.EncodeToString(b)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.secretName,
			Namespace: p.namespace,
			Labels: map[string]string{
				"app":                    "duckgres",
				"duckgres/control-plane": p.cpID,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"bearer-token": []byte(token),
		},
	}

	_, err = p.clientset.CoreV1().Secrets(p.namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create secret %s: %w", p.secretName, err)
	}
	slog.Info("Created bearer token secret.", "name", p.secretName)
	return nil
}

// readBearerToken returns the bearer token, using a cached value after the first read.
// The token is immutable after ensureBearerTokenSecret sets it up at pool creation.
func (p *K8sWorkerPool) readBearerToken(ctx context.Context) (string, error) {
	if p.cachedToken != "" {
		return p.cachedToken, nil
	}
	secret, err := p.clientset.CoreV1().Secrets(p.namespace).Get(ctx, p.secretName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get secret %s: %w", p.secretName, err)
	}
	token, ok := secret.Data["bearer-token"]
	if !ok {
		return "", fmt.Errorf("secret %s missing 'bearer-token' key", p.secretName)
	}
	p.cachedToken = string(token)
	return p.cachedToken, nil
}

// startInformer starts a SharedIndexInformer to watch worker pods.
func (p *K8sWorkerPool) startInformer() {
	labelSelector := fmt.Sprintf("duckgres/control-plane=%s", p.cpID)
	if p.orgID != "" {
		labelSelector += fmt.Sprintf(",duckgres/org=%s", p.orgID)
	}
	factory := informers.NewSharedInformerFactoryWithOptions(
		p.clientset,
		30*time.Second,
		informers.WithNamespace(p.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector
		}),
	)
	p.informer = factory.Core().V1().Pods().Informer()

	p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			// Signal pod readiness to waiters (replaces polling waitForPodIP)
			if newPod.Status.PodIP != "" && newPod.Status.Phase == corev1.PodRunning {
				if ch, ok := p.podReady.LoadAndDelete(newPod.Name); ok {
					select {
					case ch.(chan string) <- newPod.Status.PodIP:
					default:
					}
				}
			}
			// Detect pod phase transition to Failed/Succeeded (crash/OOM)
			if newPod.Status.Phase == corev1.PodFailed || newPod.Status.Phase == corev1.PodSucceeded {
				// Unblock any waiter with an error signal
				if ch, ok := p.podReady.LoadAndDelete(newPod.Name); ok {
					close(ch.(chan string))
				}
				p.onPodTerminated(newPod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				pod, ok = tombstone.Obj.(*corev1.Pod)
				if !ok {
					return
				}
			}
			if ch, loaded := p.podReady.LoadAndDelete(pod.Name); loaded {
				close(ch.(chan string))
			}
			p.onPodTerminated(pod)
		},
	})

	go p.informer.Run(p.stopInform)
}

// onPodTerminated handles a worker pod being terminated (crash, eviction, etc.).
// It closes the done channel on the corresponding ManagedWorker so the health
// check loop and session manager detect the loss.
func (p *K8sWorkerPool) onPodTerminated(pod *corev1.Pod) {
	idStr := pod.Labels["duckgres/worker-id"]
	if idStr == "" {
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return
	}
	p.mu.RLock()
	w, ok := p.workers[id]
	p.mu.RUnlock()
	if !ok {
		return
	}
	// Close the done channel to signal crash (idempotent via select)
	select {
	case <-w.done:
		// Already closed
	default:
		slog.Warn("Worker pod terminated.", "id", id, "pod", pod.Name, "phase", pod.Status.Phase)
		close(w.done)
	}
}

// SpawnWorker creates a new worker pod and waits for it to become ready.
// It acquires the spawn semaphore to limit concurrent K8s API calls and
// retries transient API errors with exponential backoff.
func (p *K8sWorkerPool) SpawnWorker(ctx context.Context, id int) error {
	// Acquire spawn semaphore to limit concurrent pod creates.
	select {
	case p.spawnSem <- struct{}{}:
		defer func() { <-p.spawnSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	token, err := p.readBearerToken(ctx)
	if err != nil {
		return fmt.Errorf("read bearer token: %w", err)
	}

	podName := p.podNameForWorker(id)

	// Build owner references for GC on CP deletion
	var ownerRefs []metav1.OwnerReference
	if p.cpUID != "" {
		ownerRefs = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       p.cpID,
				UID:        p.cpUID,
			},
		}
	}

	// Build pod labels
	podLabels := map[string]string{
		"app":                    "duckgres-worker",
		"duckgres/control-plane": p.cpID,
		"duckgres/worker-id":     strconv.Itoa(id),
	}
	if p.orgID != "" {
		podLabels["duckgres/org"] = p.orgID
	}

	// Build pod spec
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       p.namespace,
			Labels:          podLabels,
			OwnerReferences: ownerRefs,
		},
		Spec: corev1.PodSpec{
			RestartPolicy:      corev1.RestartPolicyNever,
			ServiceAccountName: p.serviceAccount,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: boolPtr(true),
				RunAsUser:    int64Ptr(1000),
			},
			Containers: []corev1.Container{
				{
					Name:            "duckdb-worker",
					Image:           p.workerImage,
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
									LocalObjectReference: corev1.LocalObjectReference{Name: p.secretName},
									Key:                  "bearer-token",
								},
							},
						},
						{
							Name:  "DUCKGRES_MODE",
							Value: "duckdb-service",
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: boolPtr(false),
					},
					Resources: p.workerResources(),
				},
			},
		},
	}
	if p.sharedWarmActivation {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  "DUCKGRES_SHARED_WARM_WORKER",
			Value: "true",
		})
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

	// Create pod with exponential backoff on transient errors.
	if err := p.createPodWithBackoff(ctx, pod); err != nil {
		return err
	}

	// Wait for pod to get an IP via informer (no polling).
	podIP, err := p.waitForPodReady(ctx, podName, 90*time.Second)
	if err != nil {
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		return fmt.Errorf("worker pod %s failed to start: %w", podName, err)
	}

	// Connect gRPC client
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	client, err := waitForWorkerTCP(addr, token, 90*time.Second)
	if err != nil {
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		return fmt.Errorf("worker %d gRPC connection failed: %w", id, err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:          id,
		bearerToken: token,
		client:      client,
		done:        done,
	}

	p.mu.Lock()
	p.workers[id] = w
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	slog.Info("K8s worker spawned.", "id", id, "pod", podName, "addr", addr)
	return nil
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
			"pod", pod.Name, "attempt", attempt+1, "backoff", backoff, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return nil // unreachable
}

// waitForPodReady waits for a pod to become Running with an IP, using the
// informer instead of polling the API. Falls back to a single API check
// in case the informer event fired before we registered the channel.
func (p *K8sWorkerPool) waitForPodReady(ctx context.Context, podName string, timeout time.Duration) (string, error) {
	ch := make(chan string, 1)
	p.podReady.Store(podName, ch)
	defer p.podReady.Delete(podName)

	// Check once in case the pod is already running (informer event already fired).
	pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil && pod.Status.PodIP != "" && pod.Status.Phase == corev1.PodRunning {
		return pod.Status.PodIP, nil
	}
	if err == nil && pod.Status.Phase == corev1.PodFailed {
		return "", fmt.Errorf("pod %s failed", podName)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case ip, ok := <-ch:
		if !ok || ip == "" {
			return "", fmt.Errorf("pod %s failed or was deleted", podName)
		}
		return ip, nil
	case <-timer.C:
		return "", fmt.Errorf("timeout waiting for pod %s to become ready", podName)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// waitForWorkerTCP connects to a worker over TCP and verifies its health.
func waitForWorkerTCP(addr, bearerToken string, timeout time.Duration) (*flightsql.Client, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	attempts := 0

	for time.Now().Before(deadline) {
		var dialOpts []grpc.DialOption
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(server.MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(server.MaxGRPCMessageSize),
		))
		if bearerToken != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&workerBearerCreds{token: bearerToken}))
		}

		client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = doHealthCheck(ctx, client)
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

// AcquireWorker returns a worker for a new session.
func (p *K8sWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.mu.Lock()
		if p.shuttingDown {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}

		p.cleanDeadWorkersLocked()

		// 1. Try to claim an idle worker
		idle := p.findIdleWorkerLocked()
		if idle != nil {
			idle.activeSessions++
			if idle.activeSessions > idle.peakSessions {
				idle.peakSessions = idle.activeSessions
			}
			p.mu.Unlock()
			slog.Debug("Reusing idle worker.", "worker", idle.ID, "active_sessions", idle.activeSessions)
			return idle, nil
		}

		// 2. No idle worker — check if we have any live workers at all
		liveCount := p.liveWorkerCountLocked()
		canSpawn := p.maxWorkers == 0 || liveCount < p.maxWorkers

		if liveCount > 0 {
			// We have live workers. Assign to the least-loaded one immediately
			// and spawn a new worker in the background if below capacity.
			w := p.leastLoadedWorkerLocked()
			if w != nil {
				w.activeSessions++
				if w.activeSessions > w.peakSessions {
					w.peakSessions = w.activeSessions
				}
				if canSpawn {
					id := p.allocateWorkerIDLocked()
					p.spawning++
					p.mu.Unlock()
					slog.Debug("Assigned to least-loaded worker, spawning new worker in background.",
						"worker", w.ID, "active_sessions", w.activeSessions, "background_worker", id)
					go p.spawnWorkerBackground(id)
				} else {
					p.mu.Unlock()
					slog.Debug("Assigned to least-loaded worker (at capacity).",
						"worker", w.ID, "active_sessions", w.activeSessions)
				}
				return w, nil
			}
		}

		// 3. No live workers at all (cold start or all dead) — must block on spawn
		if canSpawn {
			id := p.allocateWorkerIDLocked()
			p.spawning++
			p.mu.Unlock()

			slog.Info("No live workers, blocking on spawn.", "worker", id)
			err := p.SpawnWorker(ctx, id)

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			if err != nil {
				return nil, err
			}

			w, ok := p.Worker(id)
			if !ok {
				return nil, fmt.Errorf("worker %d not found after spawn", id)
			}
			p.mu.Lock()
			w.activeSessions++
			if w.activeSessions > w.peakSessions {
				w.peakSessions = w.activeSessions
			}
			p.mu.Unlock()
			return w, nil
		}

		// At capacity with all workers dead (spawning in progress) — wait and retry
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// spawnWorkerBackground spawns a worker pod without blocking AcquireWorker.
// The new worker becomes available for future sessions once ready.
func (p *K8sWorkerPool) spawnWorkerBackground(id int) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	err := p.SpawnWorker(ctx, id)

	p.mu.Lock()
	p.spawning--
	p.mu.Unlock()

	if err != nil {
		slog.Warn("Background worker spawn failed.", "worker", id, "error", err)
	}
}

// ReleaseWorker decrements the active session count for a worker.
func (p *K8sWorkerPool) ReleaseWorker(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[id]
	if ok {
		if w.activeSessions > 0 {
			w.activeSessions--
		}
		w.lastUsed = time.Now()
	}
}

// RetireWorker removes a worker from the pool and deletes its pod.
func (p *K8sWorkerPool) RetireWorker(id int) {
	p.retireWorkerWithReason(id, RetireReasonNormal)
}

func (p *K8sWorkerPool) retireWorkerWithReason(id int, reason string) {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	p.markWorkerRetiredLocked(w, reason)
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	go p.retireWorkerPod(id, w)
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
func (p *K8sWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return p.retireWorkerIfNoSessionsWithReason(id, RetireReasonNormal)
}

func (p *K8sWorkerPool) retireWorkerIfNoSessionsWithReason(id int, reason string) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	if w.activeSessions == 0 {
		p.markWorkerRetiredLocked(w, reason)
		delete(p.workers, id)
		workerCount := len(p.workers)
		p.mu.Unlock()
		observeControlPlaneWorkers(workerCount)
		go p.retireWorkerPod(id, w)
		return true
	}
	p.mu.Unlock()
	return false
}

// Worker returns a worker by ID.
func (p *K8sWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, ok := p.workers[id]
	return w, ok
}

// ActivateReservedWorker transitions a reserved worker through activating to hot.
// Failed activations retire the worker immediately.
func (p *K8sWorkerPool) ActivateReservedWorker(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
	p.mu.Lock()
	var err error
	switch worker.SharedState().NormalizedLifecycle() {
	case WorkerLifecycleReserved:
		nextState, transitionErr := worker.SharedState().Transition(WorkerLifecycleActivating, nil)
		if transitionErr == nil {
			transitionErr = worker.SetSharedState(nextState)
		}
		err = transitionErr
	case WorkerLifecycleActivating:
		err = nil
	default:
		err = fmt.Errorf("worker %d is not reserved for activation", worker.ID)
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}

	activate := p.activateTenantFunc
	if activate == nil {
		activate = func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
			return worker.ActivateTenant(ctx, server.WorkerActivationPayload{
				OrgID:          payload.OrgID,
				LeaseExpiresAt: payload.LeaseExpiresAt,
				DuckLake:       payload.DuckLake,
			})
		}
	}

	if err := activate(ctx, worker, payload); err != nil {
		p.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure)
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if worker.SharedState().NormalizedLifecycle() == WorkerLifecycleHot {
		return nil
	}
	nextState, err := worker.SharedState().Transition(WorkerLifecycleHot, nil)
	if err != nil {
		return err
	}
	if setErr := worker.SetSharedState(nextState); setErr != nil {
		return setErr
	}
	observeWarmPoolLifecycleGauges(p.workers)
	return nil
}

// ReserveSharedWorker reserves a neutral warm worker for later tenant activation.
func (p *K8sWorkerPool) ReserveSharedWorker(ctx context.Context, assignment *WorkerAssignment) (*ManagedWorker, error) {
	if err := validateWorkerAssignment(assignment); err != nil {
		return nil, err
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.mu.Lock()
		if p.shuttingDown {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}

		p.cleanDeadWorkersLocked()

		idle := p.findReservableWarmWorkerLocked()
		if idle != nil {
			nextState, err := idle.SharedState().Transition(WorkerLifecycleReserved, assignment)
			if err != nil {
				p.mu.Unlock()
				return nil, err
			}
			if err := idle.SetSharedState(nextState); err != nil {
				p.mu.Unlock()
				return nil, err
			}
			idle.reservedAt = time.Now()
			observeWarmPoolLifecycleGauges(p.workers)

			if p.shouldReplenishWarmCapacityLocked() {
				id := p.allocateWorkerIDLocked()
				p.spawning++
				p.mu.Unlock()
				p.spawnWarmWorkerBackground(id)
			} else {
				p.mu.Unlock()
			}
			return idle, nil
		}

		liveCount := p.liveWorkerCountLocked()
		if p.maxWorkers == 0 || liveCount < p.maxWorkers {
			id := p.allocateWorkerIDLocked()
			p.spawning++
			p.mu.Unlock()

			err := p.spawnWarmWorker(ctx, id)

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			if err != nil {
				return nil, err
			}
			continue
		}

		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// SpawnMinWorkers pre-warms the pool with count workers.
func (p *K8sWorkerPool) SpawnMinWorkers(count int) error {
	if count <= 0 {
		return nil
	}

	p.mu.Lock()
	if count > p.minWorkers {
		p.minWorkers = count
	}
	p.cleanDeadWorkersLocked()

	idleWarmCount := p.idleWarmWorkerCountLocked()
	missing := count - idleWarmCount
	if missing <= 0 {
		p.mu.Unlock()
		return nil
	}

	ids := make([]int, 0, missing)
	for i := 0; i < missing; i++ {
		ids = append(ids, p.allocateWorkerIDLocked())
		p.spawning++
	}
	p.mu.Unlock()

	ctx := context.Background()

	for _, id := range ids {
		if err := p.spawnWarmWorker(ctx, id); err != nil {
			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.spawning--
		p.mu.Unlock()
	}
	return nil
}

// HealthCheckLoop periodically checks worker health.
func (p *K8sWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var mu sync.Mutex
	failures := make(map[int]int)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			if p.shuttingDown {
				p.mu.RUnlock()
				return
			}
			workers := make([]*ManagedWorker, 0, len(p.workers))
			for _, w := range p.workers {
				workers = append(workers, w)
			}
			p.mu.RUnlock()

			var wg sync.WaitGroup
			for _, w := range workers {
				wg.Add(1)
				go func(w *ManagedWorker) {
					defer wg.Done()

					select {
					case <-ctx.Done():
						return
					case <-w.done:
						// Pod terminated (detected by informer)
						mu.Lock()
						delete(failures, w.ID)
						mu.Unlock()

						p.mu.Lock()
						removedWorker, workerCount, replacementID, shouldReplenish := p.removeWorkerLocked(w.ID)
						p.mu.Unlock()
						if removedWorker == nil {
							return
						}
						observeControlPlaneWorkers(workerCount)
						slog.Warn("K8s worker crashed.", "id", w.ID)
						if onCrash != nil {
							onCrash(w.ID)
						}
						if w.client != nil {
							_ = w.client.Close()
						}
						// Delete the failed pod from K8s
						podName := p.podNameForWorker(w.ID)
						delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
						_ = p.clientset.CoreV1().Pods(p.namespace).Delete(delCtx, podName, metav1.DeleteOptions{
							GracePeriodSeconds: int64Ptr(0),
						})
						delCancel()
						if shouldReplenish {
							p.spawnWarmWorkerBackground(replacementID)
						}
					default:
						// Worker alive, do health check
						var healthErr error
						var hcResult *healthCheckResult
						func() {
							defer recoverWorkerPanic(&healthErr)
							hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
							hcResult, healthErr = doHealthCheck(hctx, w.client)
							cancel()
						}()

						if healthErr != nil {
							mu.Lock()
							failures[w.ID]++
							count := failures[w.ID]
							mu.Unlock()

							slog.Warn("K8s worker health check failed.", "id", w.ID, "error", healthErr, "consecutive_failures", count)

							if count >= maxConsecutiveHealthFailures {
								mu.Lock()
								delete(failures, w.ID)
								mu.Unlock()

								p.mu.Lock()
								removedWorker, workerCount, replacementID, shouldReplenish := p.removeWorkerLocked(w.ID)
								p.mu.Unlock()
								if removedWorker == nil {
									return
								}
								observeControlPlaneWorkers(workerCount)

								slog.Error("K8s worker unresponsive, deleting pod.", "id", w.ID, "consecutive_failures", count)
								if onCrash != nil {
									onCrash(w.ID)
								}
								// Delete the pod to force cleanup
								podName := p.podNameForWorker(w.ID)
								_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
									GracePeriodSeconds: int64Ptr(10),
								})
								if w.client != nil {
									_ = w.client.Close()
								}
								if shouldReplenish {
									p.spawnWarmWorkerBackground(replacementID)
								}
							}
						} else {
							mu.Lock()
							delete(failures, w.ID)
							mu.Unlock()

							// Forward progress data to the control plane.
							if onProgress != nil && hcResult != nil {
								if sp := hcResult.toSessionProgress(); len(sp) > 0 {
									onProgress(w.ID, sp)
								}
							}
						}
					}
				}(w)
			}
			wg.Wait()
		}
	}
}

// ShutdownAll stops all workers by deleting their pods.
func (p *K8sWorkerPool) ShutdownAll() {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return
	}
	p.shuttingDown = true
	workers := make([]*ManagedWorker, 0, len(p.workers))
	for _, w := range p.workers {
		p.markWorkerRetiredLocked(w, RetireReasonShutdown)
		workers = append(workers, w)
	}
	p.mu.Unlock()

	close(p.shutdownCh)
	close(p.stopInform)

	ctx := context.Background()
	for _, w := range workers {
		podName := p.podNameForWorker(w.ID)
		gracePeriod := int64(10)
		slog.Info("Shutting down K8s worker.", "id", w.ID, "pod", podName)
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		})
		if w.client != nil {
			_ = w.client.Close()
		}
	}

	p.mu.Lock()
	p.workers = make(map[int]*ManagedWorker)
	p.mu.Unlock()
	observeControlPlaneWorkers(0)
}

// retireWorkerPod closes the gRPC client and deletes the worker pod.
func (p *K8sWorkerPool) retireWorkerPod(id int, w *ManagedWorker) {
	slog.Info("Retiring K8s worker.", "id", id)
	if w.client != nil {
		_ = w.client.Close()
	}
	podName := p.podNameForWorker(id)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(10),
	})
}

// idleReaper periodically retires workers that have been idle too long and
// reaps stuck activating/reserved workers.
func (p *K8sWorkerPool) idleReaper() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			if p.idleTimeout > 0 {
				p.reapIdleWorkers()
			}
			p.reapStuckActivatingWorkers()
		}
	}
}

func (p *K8sWorkerPool) reapIdleWorkers() {
	p.mu.Lock()
	var toRetire []struct {
		id int
		w  *ManagedWorker
	}
	now := time.Now()
	idleCount := 0
	for _, w := range p.workers {
		if p.isWarmIdleWorkerLocked(w) {
			idleCount++
		}
	}
	for id, w := range p.workers {
		if idleCount <= p.minWorkers {
			break
		}
		if p.isWarmIdleWorkerLocked(w) && !w.lastUsed.IsZero() && now.Sub(w.lastUsed) > p.idleTimeout {
			p.markWorkerRetiredLocked(w, RetireReasonIdleTimeout)
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
			idleCount--
		}
	}
	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		slog.Info("Reaping idle K8s workers.", "count", len(toRetire))
		observeControlPlaneWorkers(workerCount)
		for _, entry := range toRetire {
			go p.retireWorkerPod(entry.id, entry.w)
		}
	}
}

// reapStuckActivatingWorkers retires workers that have been in reserved or
// activating state for longer than the activating timeout.
func (p *K8sWorkerPool) reapStuckActivatingWorkers() {
	timeout := p.activatingTimeout
	if timeout <= 0 {
		timeout = defaultActivatingTimeout
	}

	p.mu.Lock()
	var toRetire []struct {
		id int
		w  *ManagedWorker
	}
	now := time.Now()
	for id, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		lifecycle := w.SharedState().NormalizedLifecycle()
		if (lifecycle == WorkerLifecycleReserved || lifecycle == WorkerLifecycleActivating) &&
			!w.reservedAt.IsZero() && now.Sub(w.reservedAt) > timeout {
			p.markWorkerRetiredLocked(w, RetireReasonStuckActivating)
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
		}
	}

	var spawnIDs []int
	for range toRetire {
		if p.shouldReplenishWarmCapacityLocked() {
			id := p.allocateWorkerIDLocked()
			p.spawning++
			spawnIDs = append(spawnIDs, id)
		}
	}
	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		slog.Warn("Reaping stuck activating workers.", "count", len(toRetire))
		observeControlPlaneWorkers(workerCount)
		for _, entry := range toRetire {
			go p.retireWorkerPod(entry.id, entry.w)
		}
		for _, id := range spawnIDs {
			p.spawnWarmWorkerBackground(id)
		}
	}
}

// --- Shared scheduling helpers (same logic as FlightWorkerPool) ---

func (p *K8sWorkerPool) findIdleWorkerLocked() *ManagedWorker {
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if p.isWarmIdleWorkerLocked(w) {
			return w
		}
	}
	return nil
}

func (p *K8sWorkerPool) leastLoadedWorkerLocked() *ManagedWorker {
	var best *ManagedWorker
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.isGenericSessionSchedulableWorkerLocked(w) {
			continue
		}
		if best == nil || w.activeSessions < best.activeSessions {
			best = w
		}
	}
	return best
}

func (p *K8sWorkerPool) liveWorkerCountLocked() int {
	count := p.spawning
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
			count++
		}
	}
	return count
}

func (p *K8sWorkerPool) cleanDeadWorkersLocked() {
	var spawnIDs []int
	removedAny := false
	for id, w := range p.workers {
		select {
		case <-w.done:
			removedWorker, _, replacementID, shouldReplenish := p.removeWorkerLocked(id)
			if removedWorker == nil {
				continue
			}
			removedAny = true
			if shouldReplenish {
				spawnIDs = append(spawnIDs, replacementID)
			}
			if w.client != nil {
				go func(c *flightsql.Client) { _ = c.Close() }(w.client)
			}
			// Delete the failed pod from K8s to avoid accumulating terminated pods
			go func(workerID int) {
				podName := p.podNameForWorker(workerID)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
					GracePeriodSeconds: int64Ptr(0),
				})
			}(id)
		default:
		}
	}
	if removedAny {
		observeControlPlaneWorkers(len(p.workers))
		for _, id := range spawnIDs {
			go p.spawnWarmWorkerBackground(id)
		}
	}
}

// workerResources computes resource requests/limits for a worker pod.
// If a memory budget is configured, each worker gets budget/maxWorkers as
// memory limit (request = 50% of limit for Burstable QoS). If no budget
// is set, returns an empty ResourceRequirements (BestEffort).
func (p *K8sWorkerPool) workerResources() corev1.ResourceRequirements {
	if p.memoryBudget <= 0 || p.maxWorkers <= 0 {
		return corev1.ResourceRequirements{}
	}
	perWorkerBytes := p.memoryBudget / int64(p.maxWorkers)
	memLimit := resource.NewQuantity(perWorkerBytes, resource.BinarySI)
	memRequest := resource.NewQuantity(perWorkerBytes/2, resource.BinarySI)

	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: *memRequest,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: *memLimit,
		},
	}
}

// --- Helpers ---

// allocateWorkerID returns the next worker ID, using the shared generator
// if configured (multi-tenant mode) or the pool's internal counter.
// Must be called with p.mu held.
func (p *K8sWorkerPool) allocateWorkerIDLocked() int {
	if p.workerIDGenerator != nil {
		return p.workerIDGenerator()
	}
	id := p.nextWorkerID
	p.nextWorkerID++
	return id
}

func (p *K8sWorkerPool) removeWorkerLocked(id int) (*ManagedWorker, int, int, bool) {
	w, ok := p.workers[id]
	if !ok {
		return nil, len(p.workers), 0, false
	}
	p.markWorkerRetiredLocked(w, RetireReasonCrash)
	delete(p.workers, id)
	workerCount := len(p.workers)
	if !p.shouldReplenishWarmCapacityLocked() {
		return w, workerCount, 0, false
	}
	replacementID := p.allocateWorkerIDLocked()
	p.spawning++
	return w, workerCount, replacementID, true
}

func (p *K8sWorkerPool) isGenericSessionSchedulableWorkerLocked(w *ManagedWorker) bool {
	return w.SharedState().NormalizedLifecycle() == WorkerLifecycleIdle
}

func (p *K8sWorkerPool) isWarmIdleWorkerLocked(w *ManagedWorker) bool {
	return w.activeSessions == 0 && p.isGenericSessionSchedulableWorkerLocked(w)
}

func (p *K8sWorkerPool) findReservableWarmWorkerLocked() *ManagedWorker {
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if p.isWarmIdleWorkerLocked(w) {
			return w
		}
	}
	return nil
}

func (p *K8sWorkerPool) idleWarmWorkerCountLocked() int {
	count := 0
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if p.isWarmIdleWorkerLocked(w) {
			count++
		}
	}
	return count
}

func (p *K8sWorkerPool) shouldReplenishWarmCapacityLocked() bool {
	if p.minWorkers <= 0 {
		return false
	}
	if p.idleWarmWorkerCountLocked() >= p.minWorkers {
		return false
	}
	liveCount := p.liveWorkerCountLocked()
	return p.maxWorkers == 0 || liveCount < p.maxWorkers
}

func (p *K8sWorkerPool) spawnWarmWorker(ctx context.Context, id int) error {
	if p.spawnWarmWorkerFunc != nil {
		return p.spawnWarmWorkerFunc(ctx, id)
	}
	return p.SpawnWorker(ctx, id)
}

func (p *K8sWorkerPool) spawnWarmWorkerBackground(id int) {
	if p.spawnWarmWorkerBackgroundFunc != nil {
		p.spawnWarmWorkerBackgroundFunc(id)
		return
	}
	go p.spawnWorkerBackground(id)
}

func (p *K8sWorkerPool) markWorkerRetiredLocked(w *ManagedWorker, reason string) {
	if w.SharedState().NormalizedLifecycle() == WorkerLifecycleHot {
		observeHotWorkerSessions(w.peakSessions)
	}
	nextState, err := w.SharedState().Transition(WorkerLifecycleRetired, nil)
	if err != nil {
		return
	}
	_ = w.SetSharedState(nextState)
	observeWorkerRetirement(reason)
	observeWarmPoolLifecycleGauges(p.workers)
}

// podNameForWorker returns the pod name for a given worker ID,
// including the org ID if set (multi-tenant mode).
func (p *K8sWorkerPool) podNameForWorker(id int) string {
	if p.orgID != "" {
		return fmt.Sprintf("duckgres-worker-%s-%s-%d", p.cpID, p.orgID, id)
	}
	return fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, id)
}

// SetMaxWorkers updates the maximum number of workers. 0 means unlimited.
func (p *K8sWorkerPool) SetMaxWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxWorkers = n
}

// SetWarmCapacityTarget updates the number of neutral idle workers the shared
// pool should try to keep available. Scale-down is handled lazily by the idle reaper.
func (p *K8sWorkerPool) SetWarmCapacityTarget(n int) {
	if n < 0 {
		n = 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.minWorkers = n
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }

// Compile-time interface check.
var _ WorkerPool = (*K8sWorkerPool)(nil)
