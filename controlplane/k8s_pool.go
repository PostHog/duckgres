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

// K8sWorkerPool manages worker pods in Kubernetes.
type K8sWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*ManagedWorker
	nextWorkerID int
	spawning     int
	maxWorkers   int
	idleTimeout  time.Duration
	shuttingDown bool
	shutdownCh   chan struct{}

	clientset       kubernetes.Interface
	namespace       string
	cpID            string
	cpUID           types.UID
	workerImage     string
	workerPort      int
	secretName      string
	configMap       string
	configPath      string
	imagePullPolicy corev1.PullPolicy
	serviceAccount  string
	memoryBudget    int64 // total memory budget in bytes
	cachedToken     string // cached bearer token (immutable after setup)
	informer        cache.SharedIndexInformer
	stopInform      chan struct{}
}

// NewK8sWorkerPool creates a K8sWorkerPool using in-cluster credentials.
func NewK8sWorkerPool(cfg K8sWorkerPoolConfig) (*K8sWorkerPool, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}
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

	pool := &K8sWorkerPool{
		workers:         make(map[int]*ManagedWorker),
		maxWorkers:      cfg.MaxWorkers,
		idleTimeout:     cfg.IdleTimeout,
		shutdownCh:      make(chan struct{}),
		stopInform:      make(chan struct{}),
		clientset:       clientset,
		namespace:       cfg.Namespace,
		cpID:            cfg.CPID,
		workerImage:     cfg.WorkerImage,
		workerPort:      cfg.WorkerPort,
		secretName:      cfg.SecretName,
		configMap:       cfg.ConfigMap,
		configPath:      cfg.ConfigPath,
		imagePullPolicy: corev1.PullPolicy(cfg.ImagePullPolicy),
		serviceAccount:  cfg.ServiceAccount,
		memoryBudget:    cfg.MemoryBudget,
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
	factory := informers.NewSharedInformerFactoryWithOptions(
		p.clientset,
		30*time.Second,
		informers.WithNamespace(p.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("duckgres/control-plane=%s", p.cpID)
		}),
	)
	p.informer = factory.Core().V1().Pods().Informer()

	p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod, ok := newObj.(*corev1.Pod)
			if !ok {
				return
			}
			// Detect pod phase transition to Failed/Succeeded (crash/OOM)
			if newPod.Status.Phase == corev1.PodFailed || newPod.Status.Phase == corev1.PodSucceeded {
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
func (p *K8sWorkerPool) SpawnWorker(ctx context.Context, id int) error {
	token, err := p.readBearerToken(ctx)
	if err != nil {
		return fmt.Errorf("read bearer token: %w", err)
	}

	podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, id)

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

	// Build pod spec
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: p.namespace,
			Labels: map[string]string{
				"app":                    "duckgres-worker",
				"duckgres/control-plane": p.cpID,
				"duckgres/worker-id":     strconv.Itoa(id),
			},
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

	_, err = p.clientset.CoreV1().Pods(p.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create worker pod %s: %w", podName, err)
	}

	// Wait for pod to get an IP and become ready
	podIP, err := p.waitForPodIP(ctx, podName, 60*time.Second)
	if err != nil {
		// Clean up the failed pod
		_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: int64Ptr(0),
		})
		return fmt.Errorf("worker pod %s failed to start: %w", podName, err)
	}

	// Connect gRPC client
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	client, err := waitForWorkerTCP(addr, token, 30*time.Second)
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

// waitForPodIP polls for the pod to have a PodIP assigned and be in Running phase.
func (p *K8sWorkerPool) waitForPodIP(ctx context.Context, podName string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("get pod %s: %w", podName, err)
		}

		if pod.Status.Phase == corev1.PodFailed {
			return "", fmt.Errorf("pod %s failed", podName)
		}

		if pod.Status.PodIP != "" && pod.Status.Phase == corev1.PodRunning {
			return pod.Status.PodIP, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return "", fmt.Errorf("timeout waiting for pod %s IP", podName)
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
			err = doHealthCheck(ctx, client)
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
			p.mu.Unlock()
			return idle, nil
		}

		// 2. If below the process cap, spawn a new worker
		liveCount := p.liveWorkerCountLocked()
		if p.maxWorkers == 0 || liveCount < p.maxWorkers {
			id := p.nextWorkerID
			p.nextWorkerID++
			p.spawning++
			p.mu.Unlock()

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
			p.mu.Unlock()
			return w, nil
		}

		// 3. At capacity — assign to the least-loaded worker
		w := p.leastLoadedWorkerLocked()
		if w != nil {
			w.activeSessions++
			p.mu.Unlock()
			return w, nil
		}

		// All workers dead but at capacity (spawning in progress) — wait and retry
		liveCount = p.liveWorkerCountLocked()
		if p.maxWorkers > 0 && liveCount >= p.maxWorkers {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		// Below capacity with all workers dead — spawn a replacement
		id := p.nextWorkerID
		p.nextWorkerID++
		p.spawning++
		p.mu.Unlock()

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
		p.mu.Unlock()
		return w, nil
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
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	go p.retireWorkerPod(id, w)
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
func (p *K8sWorkerPool) RetireWorkerIfNoSessions(id int) bool {
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

// SpawnMinWorkers pre-warms the pool with count workers.
func (p *K8sWorkerPool) SpawnMinWorkers(count int) error {
	var wg sync.WaitGroup
	errs := make(chan error, count)
	ctx := context.Background()

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := p.SpawnWorker(ctx, id); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	p.mu.Lock()
	if count > p.nextWorkerID {
		p.nextWorkerID = count
	}
	p.mu.Unlock()
	return nil
}

// HealthCheckLoop periodically checks worker health.
func (p *K8sWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash ...WorkerCrashHandler) {
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
						_, stillInPool := p.workers[w.ID]
						if stillInPool {
							delete(p.workers, w.ID)
						}
						workerCount := len(p.workers)
						p.mu.Unlock()
						observeControlPlaneWorkers(workerCount)
						if !stillInPool {
							return
						}
						slog.Warn("K8s worker crashed.", "id", w.ID)
						for _, h := range onCrash {
							h(w.ID)
						}
						if w.client != nil {
							_ = w.client.Close()
						}
						// Delete the failed pod from K8s
						podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, w.ID)
						delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
						_ = p.clientset.CoreV1().Pods(p.namespace).Delete(delCtx, podName, metav1.DeleteOptions{
							GracePeriodSeconds: int64Ptr(0),
						})
						delCancel()
					default:
						// Worker alive, do health check
						var healthErr error
						func() {
							defer recoverWorkerPanic(&healthErr)
							hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
							healthErr = doHealthCheck(hctx, w.client)
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
								_, stillInPool := p.workers[w.ID]
								if stillInPool {
									delete(p.workers, w.ID)
								}
								workerCount := len(p.workers)
								p.mu.Unlock()
								observeControlPlaneWorkers(workerCount)

								if stillInPool {
									slog.Error("K8s worker unresponsive, deleting pod.", "id", w.ID, "consecutive_failures", count)
									for _, h := range onCrash {
										h(w.ID)
									}
									// Delete the pod to force cleanup
									podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, w.ID)
									_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
										GracePeriodSeconds: int64Ptr(10),
									})
									if w.client != nil {
										_ = w.client.Close()
									}
								}
							}
						} else {
							mu.Lock()
							delete(failures, w.ID)
							mu.Unlock()
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
		workers = append(workers, w)
	}
	p.mu.Unlock()

	close(p.shutdownCh)
	close(p.stopInform)

	ctx := context.Background()
	for _, w := range workers {
		podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, w.ID)
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
	podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, id)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(10),
	})
}

// idleReaper periodically retires workers that have been idle too long.
func (p *K8sWorkerPool) idleReaper() {
	if p.idleTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.reapIdleWorkers()
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
	for id, w := range p.workers {
		if w.activeSessions == 0 && !w.lastUsed.IsZero() && now.Sub(w.lastUsed) > p.idleTimeout {
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
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

// --- Shared scheduling helpers (same logic as FlightWorkerPool) ---

func (p *K8sWorkerPool) findIdleWorkerLocked() *ManagedWorker {
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.activeSessions == 0 {
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
	for id, w := range p.workers {
		select {
		case <-w.done:
			delete(p.workers, id)
			if w.client != nil {
				go func(c *flightsql.Client) { _ = c.Close() }(w.client)
			}
			// Delete the failed pod from K8s to avoid accumulating terminated pods
			go func(workerID int) {
				podName := fmt.Sprintf("duckgres-worker-%s-%d", p.cpID, workerID)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
					GracePeriodSeconds: int64Ptr(0),
				})
			}(id)
		default:
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

func boolPtr(b bool) *bool       { return &b }
func int64Ptr(i int64) *int64     { return &i }

// Compile-time interface check.
var _ WorkerPool = (*K8sWorkerPool)(nil)
