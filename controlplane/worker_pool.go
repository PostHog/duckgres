package controlplane

import (
	"context"
	"fmt"
	"time"
)

// WorkerPool abstracts the lifecycle and scheduling of Flight SQL workers.
// Two implementations exist:
//   - FlightWorkerPool: spawns workers as local child processes (default)
//   - K8sWorkerPool:    creates workers as Kubernetes pods (build tag: kubernetes)
type WorkerPool interface {
	// AcquireWorker returns a worker for a new session. It may reuse an idle
	// worker, spawn a new one, or assign to the least-loaded worker.
	AcquireWorker(ctx context.Context) (*ManagedWorker, error)

	// ReleaseWorker decrements the active session count for a worker.
	ReleaseWorker(id int)

	// RetireWorker removes a worker from the pool and shuts it down.
	RetireWorker(id int)

	// RetireWorkerIfNoSessions retires a worker only if it has zero active
	// sessions after releasing the caller's claim. Returns true if retired.
	RetireWorkerIfNoSessions(id int) bool

	// Worker returns a worker by ID, or false if not found.
	Worker(id int) (*ManagedWorker, bool)

	// SpawnMinWorkers pre-warms the pool with count workers at startup.
	SpawnMinWorkers(count int) error

	// HealthCheckLoop runs periodic health checks on all workers.
	// onCrash is called when a worker crash is detected.
	// onProgress is called with per-session progress data after each successful check.
	// Either callback may be nil.
	HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler)

	// SetMaxWorkers updates the maximum number of workers. 0 means unlimited.
	SetMaxWorkers(n int)

	// ShutdownAll stops all workers gracefully.
	ShutdownAll()
}

// K8sWorkerPoolConfig holds the configuration for creating a K8sWorkerPool.
type K8sWorkerPoolConfig struct {
	Namespace            string
	CPID                 string // Control plane pod name, used in labels
	WorkerImage          string
	WorkerPort           int
	SecretName           string // K8s Secret name containing bearer token
	ConfigMap            string // ConfigMap name for duckgres.yaml
	MaxWorkers           int
	IdleTimeout          time.Duration
	ConfigPath           string     // Path inside worker pod where config is mounted
	ImagePullPolicy      string     // Image pull policy for worker pods (e.g., "Never", "IfNotPresent", "Always")
	ServiceAccount       string     // ServiceAccount name for worker pods (default: "default")
	MemoryBudget         int64      // Total memory budget in bytes; used to derive per-worker resource limits
	OrgID                string     // Org ID for pod labels (multi-tenant mode)
	WorkerIDGenerator    func() int // Shared ID generator across orgs (nil = internal counter)
}

// K8sPoolFactory creates a K8sWorkerPool. Registered at init time by the
// kubernetes build tag. Nil when the binary is built without -tags kubernetes.
type K8sPoolFactory func(cfg K8sWorkerPoolConfig) (WorkerPool, error)

var k8sPoolFactory K8sPoolFactory

// RegisterK8sPoolFactory is called from an init() in k8s_factory.go (build tag: kubernetes).
func RegisterK8sPoolFactory(f K8sPoolFactory) {
	k8sPoolFactory = f
}

// CreateK8sPool creates a Kubernetes worker pool if the kubernetes build tag is enabled.
func CreateK8sPool(cfg K8sWorkerPoolConfig) (WorkerPool, error) {
	if k8sPoolFactory == nil {
		return nil, fmt.Errorf("remote worker backend not available: binary was not built with -tags kubernetes")
	}
	return k8sPoolFactory(cfg)
}
