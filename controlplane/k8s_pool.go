//go:build kubernetes

package controlplane

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const defaultActivatingTimeout = 2 * time.Minute

// workerPodReadyTimeout bounds how long a spawn waits for a worker pod to become
// Ready. A cold spawn may need Karpenter to provision a fresh node (a single-
// tenant exclusive 46/360 worker gets a dedicated large instance), so this is
// generous.
const workerPodReadyTimeout = 5 * time.Minute

// workerSpawnActivateTimeout bounds the DETACHED background spawn+activate run
// for an org cold acquisition (OrgReservedPool.AcquireWorker slow path). The
// spawn and activation deliberately do NOT run on the requester's ctx: if node
// provisioning reliably exceeds the client's worker-queue budget, cancelling
// the wait would delete the in-flight pod and every retry would spawn-wait-
// delete a fresh one without ever converging (doomed-spawn thrash). Budget =
// pod-ready wait (workerPodReadyTimeout, may include a Karpenter node
// provision) + gRPC connect (up to 90s) + tenant activation (ATTACH against
// cloud storage, defaultActivatingTimeout-scale).
const workerSpawnActivateTimeout = workerPodReadyTimeout + 3*time.Minute

const workerTerminationGracePeriodSeconds int64 = 3600

var errStaleRuntimeWorkerClaim = stderrors.New("stale runtime worker claim")

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

	clientset               kubernetes.Interface
	namespace               string
	cpID                    string
	cpInstanceID            string
	cpUID                   types.UID
	workerImage             string
	workerPort              int
	secretName              string
	configMap               string
	configPath              string
	imagePullPolicy         corev1.PullPolicy
	serviceAccount          string
	workerCPURequest        string            // CPU request for worker pods (e.g., "500m")
	workerMemoryRequest     string            // memory request for worker pods (e.g., "1Gi")
	workerNodeSelector      map[string]string // node selector for worker pods
	workerTolerationKey     string            // taint key for NoSchedule toleration
	workerTolerationValue   string            // taint value for NoSchedule toleration
	workerPriorityClassName string            // PriorityClass for worker pods (preempts overprovision pause pods)

	// Headroom controller: keep headroomPercent% of worker-nodepool allocatable
	// CPU+mem held by low-priority placeholder pods so a worker spawn schedules
	// immediately (preempting placeholders) instead of waiting on a fresh node.
	headroomPercent              int
	placeholderImage             string
	placeholderCPU               string
	placeholderMemory            string
	placeholderPriorityClassName string

	orgID             string                                       // org ID for pod labels (multi-tenant mode)
	workerIDGenerator func() int                                   // shared ID generator across orgs (nil = internal counter)
	resolveOrgConfig  func(string) (*configstore.OrgConfig, error) // resolve org config for per-tenant image reaping
	informer          cache.SharedIndexInformer
	stopInform        chan struct{}
	spawnSem          chan struct{} // limits concurrent pod creates to avoid overwhelming the K8s API
	retireSem         chan struct{} // limits concurrent pod deletes to avoid overwhelming the K8s API
	podReady          sync.Map      // podName -> chan podReadyInfo; signaled by informer

	// nodeFirstSeen tracks the first time this CP observed a worker on each
	// node. Used to prefer reaping workers on the newest-seen nodes and
	// claiming idle workers on the oldest-seen nodes, so cached parquet pages
	// on the local NVMe cache-proxy stay useful for longer. Per-CP state;
	// CPs don't coordinate this (see idleReaper/findIdleWorker).
	nodeFirstSeen map[string]time.Time

	spawnWorkerFunc    func(ctx context.Context, id int, image string, profile WorkerProfile) error // test seam for stubbing pod creation
	activateTenantFunc func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error
	healthCheckFunc    func(context.Context, *ManagedWorker) error
	connectWorkerFunc  func(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error)
	runtimeStore       RuntimeWorkerStore
	lifecycle          *WorkerLifecycle // typed lifecycle transitions; lazily wired via ensureLifecycle().
	lifecycleOnce      sync.Once        // guards lazy initialization of lifecycle from concurrent first-callers.

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
	if strings.TrimSpace(cfg.ServiceAccount) == "" {
		cfg.ServiceAccount = DefaultK8sWorkerServiceAccount
	}

	// Limit concurrent K8s API calls to avoid overwhelming the API server.
	// The K8s client above is configured for QPS=50 / Burst=100, so 50 in-flight
	// pod creates fits comfortably within client-side throttling while letting
	// a large demand burst (e.g. a 30-tenant cold ramp) spawn workers in
	// parallel rather than serializing. Pod scheduling latency on the node side
	// is bounded by Karpenter (~30s) and the kubelet, neither of which cares
	// about how many pods we create in parallel from the CP.
	spawnConcurrency := 50
	retireConcurrency := 5
	pool := &K8sWorkerPool{
		workers:                 make(map[int]*ManagedWorker),
		maxWorkers:              cfg.MaxWorkers,
		idleTimeout:             cfg.IdleTimeout,
		shutdownCh:              make(chan struct{}),
		stopInform:              make(chan struct{}),
		clientset:               clientset,
		namespace:               cfg.Namespace,
		cpID:                    cfg.CPID,
		cpInstanceID:            cfg.CPInstanceID,
		workerImage:             cfg.WorkerImage,
		workerPort:              cfg.WorkerPort,
		secretName:              cfg.SecretName,
		configMap:               cfg.ConfigMap,
		configPath:              cfg.ConfigPath,
		imagePullPolicy:         corev1.PullPolicy(cfg.ImagePullPolicy),
		serviceAccount:          cfg.ServiceAccount,
		workerCPURequest:        cfg.WorkerCPURequest,
		workerMemoryRequest:     cfg.WorkerMemoryRequest,
		workerNodeSelector:      cfg.WorkerNodeSelector,
		workerTolerationKey:     cfg.WorkerTolerationKey,
		workerTolerationValue:   cfg.WorkerTolerationValue,
		workerPriorityClassName: cfg.WorkerPriorityClassName,

		headroomPercent:              cfg.HeadroomPercent,
		placeholderImage:             cfg.PlaceholderImage,
		placeholderCPU:               cfg.PlaceholderCPU,
		placeholderMemory:            cfg.PlaceholderMemory,
		placeholderPriorityClassName: cfg.PlaceholderPriorityClassName,

		orgID:             cfg.OrgID,
		workerIDGenerator: cfg.WorkerIDGenerator,
		resolveOrgConfig:  cfg.ResolveOrgConfig,
		runtimeStore:      cfg.RuntimeStore,
		spawnSem:          make(chan struct{}, spawnConcurrency),
		retireSem:         make(chan struct{}, retireConcurrency),
		nodeFirstSeen:     make(map[string]time.Time),
	}
	if cfg.RuntimeStore != nil {
		// The lifecycle service is the typed seam for every post-CAS
		// pod cleanup. Wire it to the pool's own DeleteWorkerArtifacts
		// so callers schedule the standard K8s cleanup goroutine.
		// RuntimeWorkerStore intentionally omits the worker-id-only
		// CAS methods (MarkWorkerDraining/RetireDrainingWorker/
		// BumpWorkerEpoch) — they're reachable only through the
		// lifecycle. We extract them here via a type assertion against
		// the concrete store. Tests that bypass this constructor and
		// set runtimeStore directly trigger lazy initialization via
		// ensureLifecycle().
		if ls, ok := cfg.RuntimeStore.(workerLifecycleStore); ok {
			pool.lifecycle = NewWorkerLifecycle(ls, pool)
		}
	}

	// Resolve CP pod UID for owner references
	if err := pool.resolveCPUID(context.Background()); err != nil {
		slog.Warn("Could not resolve CP pod UID for owner references. Worker pods will not be garbage-collected if CP is deleted.", "error", err)
	}
	if pool.cpInstanceID == "" {
		pool.cpInstanceID = pool.cpID
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

func (p *K8sWorkerPool) workerServiceAccountName() string {
	if strings.TrimSpace(p.serviceAccount) == "" {
		return DefaultK8sWorkerServiceAccount
	}
	return p.serviceAccount
}

// ensureLifecycle returns the pool's lifecycle service, lazily wiring
// it on first use when a runtimeStore is set but lifecycle is nil.
// Production paths go through newK8sWorkerPool which initializes
// lifecycle eagerly; this lazy path exists for tests that construct
// the pool struct directly and set runtimeStore after the fact.
// Returns nil when no runtime store is available, or when the store
// doesn't satisfy workerLifecycleStore (process backend / minimal
// mocks that intentionally don't implement the CAS methods).
//
// Guarded by sync.Once so concurrent first-callers (e.g. idleReaper
// and janitor goroutines) don't race on the field assignment. The Once
// is per-pool, so test pools that build a fresh pool per test still
// get fresh lifecycle initialization.
func (p *K8sWorkerPool) ensureLifecycle() *WorkerLifecycle {
	p.lifecycleOnce.Do(func() {
		if p.lifecycle != nil {
			return
		}
		if p.runtimeStore == nil {
			return
		}
		if ls, ok := p.runtimeStore.(workerLifecycleStore); ok {
			p.lifecycle = NewWorkerLifecycle(ls, p)
		}
	})
	return p.lifecycle
}

// SetMaxWorkers updates the maximum number of workers. 0 means unlimited.
func (p *K8sWorkerPool) SetMaxWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxWorkers = n
}

// SetWorkerResources updates the CPU and memory requests (and limits) applied
// to newly spawned worker pods. Existing pods are not affected.
//
// In multi-tenant mode, multiple orgs may call this — the last write wins.
// This is acceptable when all orgs share a uniform node pool. If orgs need
// genuinely different worker sizes, per-org dedicated pools are required.
func (p *K8sWorkerPool) SetWorkerResources(cpu, memory string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if (p.workerCPURequest != "" && cpu != p.workerCPURequest) ||
		(p.workerMemoryRequest != "" && memory != p.workerMemoryRequest) {
		slog.Warn("Overwriting shared pool worker resources — multiple orgs set different values.",
			"old_cpu", p.workerCPURequest, "new_cpu", cpu,
			"old_memory", p.workerMemoryRequest, "new_memory", memory)
	}
	p.workerCPURequest = cpu
	p.workerMemoryRequest = memory
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }

// Compile-time interface check.
var _ WorkerPool = (*K8sWorkerPool)(nil)
