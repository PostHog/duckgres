package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const DefaultK8sWorkerServiceAccount = "duckgres-worker"
const DefaultWarmCapacityRetryAfter = 45 * time.Second
const DefaultWarmCapacityMissWindow = 2 * time.Minute
const DefaultWarmCapacityMissesPerWorker = 8
const DefaultWarmCapacityDemandTTL = 15 * time.Minute

// WarmAcquireRetryInterval is how often a session-acquire that missed the warm
// pool re-attempts the claim while waiting (within WarmAcquireTimeout) for the
// warm pool to replenish.
const WarmAcquireRetryInterval = 2 * time.Second

// WarmCapacityExhaustedError is returned when a user request misses the ready
// warm pool. The caller should fail fast with a retryable capacity response
// instead of waiting for a foreground cold worker spawn.
type WarmCapacityExhaustedError struct {
	// RetryAfter is the client-facing retry hint for protocol-specific error responses.
	RetryAfter time.Duration
	// Reason classifies why the warm-capacity claim missed.
	Reason configstore.WorkerClaimMissReason
}

func (e *WarmCapacityExhaustedError) Error() string {
	return warmCapacityMissPolicyForReason(e.missReason()).errorString(e.RetryAfter)
}

func (e *WarmCapacityExhaustedError) missReason() configstore.WorkerClaimMissReason {
	if e.Reason == configstore.WorkerClaimMissReasonNone {
		return configstore.WorkerClaimMissReasonNoIdle
	}
	return e.Reason
}

func NewWarmCapacityExhaustedError(retryAfter time.Duration) error {
	return NewWarmCapacityExhaustedErrorForReason(configstore.WorkerClaimMissReasonNoIdle, retryAfter)
}

func NewWarmCapacityExhaustedErrorForReason(reason configstore.WorkerClaimMissReason, retryAfter time.Duration) error {
	if retryAfter <= 0 {
		retryAfter = DefaultWarmCapacityRetryAfter
	}
	if reason == configstore.WorkerClaimMissReasonNone {
		reason = configstore.WorkerClaimMissReasonNoIdle
	}
	return &WarmCapacityExhaustedError{RetryAfter: retryAfter, Reason: reason}
}

// WorkerPool abstracts the lifecycle and scheduling of Flight SQL workers.
// Two implementations exist:
//   - FlightWorkerPool: spawns workers as local child processes (default)
//   - K8sWorkerPool:    creates workers as Kubernetes pods (build tag: kubernetes)
type WorkerPool interface {
	// AcquireWorker returns a worker for a new session. It may reuse an idle
	// worker, spawn a new one, or assign to the least-loaded worker. profile is
	// the requested pod shape (nil => the default exclusive profile); only the
	// multi-tenant OrgReservedPool acts on it — the flat/process pools ignore it.
	AcquireWorker(ctx context.Context, profile *WorkerProfile) (*ManagedWorker, error)

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
	Namespace    string
	CPID         string // Control plane pod name, used in labels
	CPInstanceID string // Durable control-plane instance ID (<pod_uid>:<boot_id>)
	WorkerImage  string
	WorkerPort   int
	SecretName   string // Base name for per-worker K8s Secrets containing RPC bearer token and TLS material
	ConfigMap    string // ConfigMap name for duckgres.yaml
	MaxWorkers   int
	// WarmAcquireTimeout: how long a session-acquire blocks server-side waiting
	// for a warm worker to become available before returning the retryable
	// "no warm worker" backpressure. 0 = fail fast (legacy behavior). Bounded
	// per-request by the client's connection context, so a client with a short
	// deadline still fails fast.
	WarmAcquireTimeout       time.Duration
	IdleTimeout              time.Duration
	ConfigPath               string                                       // Path inside worker pod where config is mounted
	ImagePullPolicy          string                                       // Image pull policy for worker pods (e.g., "Never", "IfNotPresent", "Always")
	ServiceAccount           string                                       // Neutral ServiceAccount name for worker pods (default: "duckgres-worker")
	WorkerCPURequest         string                                       // CPU request for worker pods (e.g., "500m"). Empty = BestEffort.
	WorkerMemoryRequest      string                                       // Memory request for worker pods (e.g., "1Gi"). Empty = BestEffort.
	WorkerNodeSelector       map[string]string                            // Node selector for worker pods. Nil = no selector.
	WorkerTolerationKey      string                                       // Taint key for worker pod NoSchedule toleration. Empty = no toleration.
	WorkerTolerationValue    string                                       // Taint value for worker pod NoSchedule toleration.
	WorkerExclusiveNode      bool                                         // One worker per node via pod anti-affinity.
	WorkerPriorityClassName  string                                       // PriorityClass for worker pods (so they preempt overprovision pause pods). Empty = none.
	ColocatedNodeSelector    map[string]string                            // Node selector for colocate=true (bin-pack) worker pods. Nil = no selector.
	ColocatedTolerationKey   string                                       // Taint key for colocated worker pods. Empty = no toleration.
	ColocatedTolerationValue string                                       // Taint value for colocated worker pods.
	ColocatedWarmShapes      []ColocatedWarmShape                         // Colocated shapes to keep warm (each {cpu,memory,target}).
	OrgID                    string                                       // Org ID for pod labels (multi-tenant mode)
	WorkerIDGenerator        func() int                                   // Shared ID generator across orgs (nil = internal counter)
	ResolveOrgConfig         func(string) (*configstore.OrgConfig, error) // Optional: resolve org config for version-aware reaping
	RuntimeStore             RuntimeWorkerStore
}

// RuntimeWorkerStore is the durable-store surface exposed to the K8s
// worker pool. Every lifecycle CAS method is now absent from this
// interface — they are reachable only through WorkerLifecycle so
// callers physically cannot bypass the typed snapshot/lease seam.
//
// The lifecycle service uses workerLifecycleStore (defined in
// worker_lifecycle.go), which is the larger interface that includes
// the CAS methods. Production wires it via a type assertion in
// newK8sWorkerPool / ensureLifecycle, because *configstore.ConfigStore
// satisfies both interfaces.
type RuntimeWorkerStore interface {
	UpsertWorkerRecord(record *configstore.WorkerRecord) error
	ClaimIdleWorker(ownerCPInstanceID, orgID, image string, profileCPU, profileMemory string, profileColocate bool, maxOrgWorkers, maxGlobalWorkers int, maxColocatedCPU int, maxColocatedMemBytes uint64) (*configstore.WorkerRecord, configstore.WorkerClaimMissReason, error)
	ClaimHotIdleWorker(ownerCPInstanceID, orgID string, profileCPU, profileMemory string, profileColocate bool, maxOrgWorkers int) (*configstore.WorkerRecord, configstore.WorkerClaimMissReason, error)
	RecordWarmCapacityMiss(scope string, reason configstore.WorkerClaimMissReason, now time.Time) error
	CreateSpawningWorkerSlot(ownerCPInstanceID, orgID, image string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error)
	CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix, image string, profileCPU, profileMemory string, profileColocate bool, targetWarmWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error)
	CreateNeutralWarmWorkerSlotForImage(ownerCPInstanceID, podNamePrefix, image string, perImageTarget, maxGlobalWorkers int) (*configstore.WorkerRecord, error)
	GetWorkerRecord(workerID int) (*configstore.WorkerRecord, error)
	ObserveWorker(workerID int) (*configstore.WorkerSnapshot, error)
	TakeOverWorker(workerID int, ownerCPInstanceID, orgID string, expectedOwnerEpoch int64) (*configstore.WorkerRecord, error)
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
