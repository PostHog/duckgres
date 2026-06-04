//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/x509"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
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

// warmSpawnReconcileTimeout bounds a warm-pool replenish/reconcile spawn. It MUST
// exceed workerPodReadyTimeout: a warm spawn does the same waitForPodReady, so a
// shorter deadline cancels every cold spawn before its node can provision. The
// old 30s value meant the warm reconcile could never refill an exclusive worker
// that needed a fresh node — after a CP restart the default/exclusive pool would
// sit empty, churning "context deadline exceeded", while only colocated (small,
// fast, bin-packed) shapes refilled. See TestWarmSpawnTimeoutExceedsPodReady.
const warmSpawnReconcileTimeout = 6 * time.Minute

var errStaleRuntimeWorkerClaim = stderrors.New("stale runtime worker claim")

// K8sWorkerPool manages worker pods in Kubernetes.
type K8sWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*ManagedWorker
	nextWorkerID int
	spawning     int
	maxWorkers   int
	// warmAcquireTimeout: server-side block window for a session-acquire that
	// missed the warm pool (0 = fail fast). Read by OrgReservedPool.AcquireWorker.
	warmAcquireTimeout time.Duration
	minWorkers         int
	// perImageWarmTarget is an additive floor on top of minWorkers: for each
	// image listed, the pool aims to keep at least N warm-idle workers of
	// that exact image alive so a per-org pin always has a hot pod waiting.
	// minWorkers still drives the cluster-default warm count separately.
	perImageWarmTarget map[string]int
	idleTimeout        time.Duration
	shuttingDown       bool
	shutdownCh         chan struct{}

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
	workerExclusiveNode     bool              // one worker per node via anti-affinity
	workerPriorityClassName string            // PriorityClass for worker pods (preempts overprovision pause pods)

	// Colocated (bin-pack) scheduling for colocate=true worker profiles. When a
	// spawned worker's profile is colocated, these replace the default
	// nodeSelector/toleration and the exclusive-node anti-affinity is dropped so
	// many small pods pack onto a shared node.
	colocatedWorkerNodeSelector    map[string]string
	colocatedWorkerTolerationKey   string
	colocatedWorkerTolerationValue string
	colocatedWarmShapes            []ColocatedWarmShape                         // colocated shapes to keep warm (each {cpu,memory,target})
	orgID                          string                                       // org ID for pod labels (multi-tenant mode)
	workerIDGenerator              func() int                                   // shared ID generator across orgs (nil = internal counter)
	resolveOrgConfig               func(string) (*configstore.OrgConfig, error) // resolve org config for per-tenant image reaping
	informer                       cache.SharedIndexInformer
	stopInform                     chan struct{}
	spawnSem                       chan struct{} // limits concurrent pod creates to avoid overwhelming the K8s API
	retireSem                      chan struct{} // limits concurrent pod deletes to avoid overwhelming the K8s API
	podReady                       sync.Map      // podName -> chan podReadyInfo; signaled by informer

	// nodeFirstSeen tracks the first time this CP observed a worker on each
	// node. Used to prefer reaping workers on the newest-seen nodes and
	// claiming idle workers on the oldest-seen nodes, so warm parquet pages
	// on the local NVMe cache-proxy stay useful for longer. Per-CP state;
	// CPs don't coordinate this (see idleReaper/findIdleWorker).
	nodeFirstSeen map[string]time.Time

	spawnWarmWorkerFunc           func(ctx context.Context, id int) error
	spawnWarmWorkerBackgroundFunc func(id int)
	activateTenantFunc            func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error
	healthCheckFunc               func(context.Context, *ManagedWorker) error
	connectWorkerFunc             func(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error)
	runtimeStore                  RuntimeWorkerStore
	lifecycle                     *WorkerLifecycle // typed lifecycle transitions; lazily wired via ensureLifecycle().
	lifecycleOnce                 sync.Once        // guards lazy initialization of lifecycle from concurrent first-callers.

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
	// the warm-pool reconciler close large demand gaps (e.g. 30-tenant cold
	// ramp) in one tick instead of creeping up at 3-per-pod-startup. Pod
	// scheduling latency on the node side is bounded by Karpenter (~30s) and
	// the kubelet, neither of which cares about how many pods we create
	// in parallel from the CP.
	spawnConcurrency := 50
	retireConcurrency := 5
	pool := &K8sWorkerPool{
		workers:                 make(map[int]*ManagedWorker),
		maxWorkers:              cfg.MaxWorkers,
		warmAcquireTimeout:      cfg.WarmAcquireTimeout,
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
		workerExclusiveNode:     cfg.WorkerExclusiveNode,
		workerPriorityClassName: cfg.WorkerPriorityClassName,

		colocatedWorkerNodeSelector:    cfg.ColocatedNodeSelector,
		colocatedWorkerTolerationKey:   cfg.ColocatedTolerationKey,
		colocatedWorkerTolerationValue: cfg.ColocatedTolerationValue,
		colocatedWarmShapes:            cfg.ColocatedWarmShapes,

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
	go pool.disruptionGuardReconciler()

	return pool, nil
}

// RetireOneMismatchedVersionWorker scans all shared warm-worker pods in the
// namespace for one whose duckgres/control-plane label identifies a different
// Deployment ReplicaSet (pod-template-hash) than this CP, atomically marks
// its idle runtime-store row retired, and deletes the pod.
//
// The janitor leader is expected to invoke this once per tick: together with
// warm-pool replenishment via reconcileWarmCapacity, old-version workers are
// replaced one at a time with pods spawned from the leader's current binary.
// This gives gradual rolling worker replacement driven entirely by leader
// election — no cross-CP sweeps needed.
//
// Retirement is limited to workers currently in WorkerStateIdle. Busy or
// hot-idle workers are left alone so in-flight sessions aren't disturbed;
// they'll be retired on a subsequent tick once they become idle (or when
// their hot-idle TTL expires).
//
// Returns true when a worker was retired this call.
func (p *K8sWorkerPool) RetireOneMismatchedVersionWorker(ctx context.Context) bool {
	if p.runtimeStore == nil || p.clientset == nil {
		return false
	}
	myVersion := trimK8sPodHashSuffix(p.cpID)
	if myVersion == p.cpID {
		// cpID doesn't carry a Deployment pod-template-hash suffix (e.g.
		// bare pod or StatefulSet). Without that we can't compare versions
		// across pods, so nothing to do.
		return false
	}

	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		slog.Warn("Version-aware reaper failed to list worker pods.", "error", err)
		return false
	}

	for _, pod := range pods.Items {
		label := pod.Labels["duckgres/control-plane"]
		if label == "" {
			continue
		}
		idStr := pod.Labels["duckgres/worker-id"]
		if idStr == "" {
			continue
		}
		workerID, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		var snap *configstore.WorkerSnapshot
		if p.runtimeStore != nil {
			var err error
			snap, err = p.runtimeStore.ObserveWorker(workerID)
			if err != nil {
				slog.Warn("Version-aware reaper failed to observe worker.", "worker_id", workerID, "error", err)
			}
		}

		// Resolve the target version for this specific pod.
		// For neutral workers, the target is the global binary version.
		// For assigned workers, the target is the tenant's configured image.
		var isMismatched bool
		podOrgID := pod.Labels["duckgres/org"]
		if podOrgID == "" && snap != nil {
			podOrgID = snap.OrgID()
		}

		if podOrgID != "" && p.resolveOrgConfig != nil {
			// Per-tenant version check
			org, err := p.resolveOrgConfig(podOrgID)
			if err != nil {
				// Best-effort: skip if we can't resolve config this tick
				continue
			}
			targetImage := p.workerImage
			if org.Warehouse != nil && org.Warehouse.Image != "" {
				targetImage = org.Warehouse.Image
			}

			actualImage := workerImageForPod(&pod)
			if actualImage == "" && snap != nil {
				actualImage = snap.Image()
			}
			if actualImage != "" && actualImage != targetImage {
				isMismatched = true
				slog.Debug("Detected per-tenant worker image mismatch.",
					"org", podOrgID, "worker_pod", pod.Name, "actual", actualImage, "target", targetImage)
			}
		} else {
			// Global CP binary version check
			if trimK8sPodHashSuffix(label) != myVersion {
				isMismatched = true
			}
		}

		if !isMismatched {
			continue
		}

		lifecycle := p.ensureLifecycle()
		if snap == nil || lifecycle == nil {
			continue
		}
		outcome, err := lifecycle.RetireIdleVariantFromSnapshot(*snap, RetireReasonMismatchedVersion, LifecycleOriginMismatchedVersionReaper)
		if err != nil {
			slog.Warn("Version-aware reaper failed to retire idle row.", "worker_id", workerID, "error", err)
			continue
		}
		if !outcome.Transitioned {
			// Not currently idle or hot-idle (busy, reserved, or already
			// retired). Leave it for a later tick. The lifecycle service
			// also skips physical pod delete on a CAS miss.
			continue
		}
		slog.Info("Retiring mismatched-version worker pod.",
			"worker_id", workerID,
			"org", podOrgID,
			"worker_pod", pod.Name,
		)
		// The lifecycle service already scheduled an async pod + secret
		// delete via DeleteWorkerArtifacts. We additionally issue a
		// synchronous Delete here so the reaper's one-per-call contract
		// is observable to callers (and to tests) without waiting for
		// the async cleanup goroutine. The second async Delete is
		// idempotent — it returns NotFound on the already-gone pod and
		// just logs at Warn, which is harmless.
		gracePeriod := int64(10)
		if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Version-aware reaper failed to delete pod.", "worker_pod", pod.Name, "error", err)
		}
		return true
	}
	return false
}

func workerImageForPod(pod *corev1.Pod) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == "duckdb-worker" {
			return container.Image
		}
	}
	slog.Debug("workerImageForPod: duckdb-worker container not found in pod spec.", "worker_pod", pod.Name)
	return ""
}

// cleanupOrphanedWorkerPods deletes worker pods whose DB row is in a terminal
// state (retired/lost) or has no DB row at all, reconciling K8s against the
// state store. Runs from the janitor loop (leader-only).
//
// This closes a gap between ShutdownAll and the janitor's orphan sweep.
// ShutdownAll marks the worker row retired in the DB before issuing the K8s
// pod delete, and the delete is fire-and-forget — so if the delete fails (API
// hiccup) or the CP is SIGKILL'd mid-shutdown, the pod survives while the DB
// row is already terminal. ListOrphanedWorkers explicitly excludes
// terminal-state rows, so without this reconciler those pods live forever.
// Bare worker pods have no owner reference, so nothing else in the cluster
// reaps them either.
//
// minAge protects newly-spawned pods: the spawn path creates the pod BEFORE
// upserting the DB row, so there's a brief window where a live pod has no DB
// record. Without the age gate the reconciler would race the spawner.
//
// Returns the number of pods deleted this call. Per-pod delete failures
// are recorded against
// duckgres_worker_stranded_pods_reconciled_total{outcome="delete_failed"}
// and do not bubble up here — they don't disqualify the sweep, just one
// candidate.
func (p *K8sWorkerPool) cleanupOrphanedWorkerPods(ctx context.Context, minAge time.Duration) int {
	if p.runtimeStore == nil || p.clientset == nil {
		return 0
	}
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		slog.Warn("Stranded-pod reconciler failed to list worker pods.", "error", err)
		return 0
	}
	cutoff := time.Now().Add(-minAge)
	deleted := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		idStr := pod.Labels["duckgres/worker-id"]
		if idStr == "" {
			continue
		}
		workerID, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		rec, err := p.runtimeStore.GetWorkerRecord(workerID)
		if err != nil {
			slog.Warn("Stranded-pod reconciler failed to load worker record.", "worker_id", workerID, "error", err)
			continue
		}
		dbState := "missing"
		outcome := StrandedOutcomeDeletedMissingRow
		if rec != nil {
			if rec.State != configstore.WorkerStateRetired && rec.State != configstore.WorkerStateLost {
				// Pod is still claimed by an active runtime row — not
				// stranded. Record it under "kept" so dashboards can
				// see reconciler saturation (large kept counts =
				// reaper iterating many healthy pods per tick).
				observeStrandedPodReconciled(StrandedOutcomeKept)
				continue
			}
			dbState = string(rec.State)
			outcome = StrandedOutcomeDeletedTerminalRow
		}
		gracePeriod := int64(10)
		if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Stranded-pod reconciler failed to delete pod.", "worker_pod", pod.Name, "worker_id", workerID, "error", err)
			observeStrandedPodReconciled(StrandedOutcomeDeleteFailed)
			continue
		}
		_ = p.deleteWorkerRPCSecret(ctx, pod.Name)
		slog.Info("Stranded worker pod reconciled.", "worker_pod", pod.Name, "worker_id", workerID, "db_state", dbState)
		observeStrandedPodReconciled(outcome)
		deleted++
	}
	return deleted
}

// cleanupOrphanedWorkerSecrets deletes worker RPC secrets whose pod
// no longer exists. Closes the recovery gap where a CP crashes
// between secret creation and pod creation — the secret would
// otherwise leak indefinitely because cleanupOrphanedWorkerPods
// only iterates pods. Runs from the janitor leader on the same
// tick as cleanupOrphanedWorkerPods.
//
// minAge protects newly-created secrets the spawner is still using
// (the spawn flow creates the secret first, then the pod).
//
// Returns the number of secrets deleted this call.
func (p *K8sWorkerPool) cleanupOrphanedWorkerSecrets(ctx context.Context, minAge time.Duration) int {
	if p.clientset == nil {
		return 0
	}
	// Worker RPC secrets are labeled at creation (worker_rpc_security.go
	// ensureWorkerRPCSecret) with app=duckgres +
	// duckgres/control-plane=<p.cpID> + duckgres/worker-pod=<podName>.
	// The selector narrows by control-plane so each CP only reaps
	// its own secrets — critical during rolling restarts and blue/green
	// deployments where multiple CP replicas share the namespace and
	// a peer's freshly-created secret (whose pod hasn't been spawned
	// yet) must not be reaped. p.cpID is used unsanitized to match
	// the value the creation path stamps.
	secrets, err := p.clientset.CoreV1().Secrets(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=duckgres,duckgres/control-plane=%s,duckgres/worker-pod", p.cpID),
	})
	if err != nil {
		slog.Warn("Stranded-secret reconciler failed to list worker RPC secrets.", "error", err)
		return 0
	}
	cutoff := time.Now().Add(-minAge)
	deleted := 0
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if secret.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		podName := secret.Labels["duckgres/worker-pod"]
		if podName == "" {
			continue
		}
		if _, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, podName, metav1.GetOptions{}); err == nil {
			// Pod exists — leave the secret alone, the regular
			// cleanupOrphanedWorkerPods path will reap both
			// together when the pod's DB row is terminal. Recorded
			// as "kept" for symmetry with the pods sweep, so
			// dashboards comparing the two reapers' kept counts
			// see parallel behavior.
			observeStrandedSecretReconciled(StrandedOutcomeKept)
			continue
		} else if !errors.IsNotFound(err) {
			slog.Warn("Stranded-secret reconciler failed to load worker pod.", "worker_pod", podName, "error", err)
			continue
		}
		if err := p.clientset.CoreV1().Secrets(p.namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			slog.Warn("Stranded-secret reconciler failed to delete secret.", "secret", secret.Name, "worker_pod", podName, "error", err)
			observeStrandedSecretReconciled(StrandedOutcomeDeleteFailed)
			continue
		}
		slog.Info("Stranded worker RPC secret reconciled.", "secret", secret.Name, "worker_pod", podName)
		observeStrandedSecretReconciled(StrandedOutcomeDeleted)
		deleted++
	}
	return deleted
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
					case ch.(chan podReadyInfo) <- podReadyInfo{ip: newPod.Status.PodIP, nodeName: newPod.Spec.NodeName}:
					default:
					}
				}
			}
			// Detect pod phase transition to Failed/Succeeded (crash/OOM)
			if newPod.Status.Phase == corev1.PodFailed || newPod.Status.Phase == corev1.PodSucceeded {
				// Unblock any waiter with an error signal
				if ch, ok := p.podReady.LoadAndDelete(newPod.Name); ok {
					close(ch.(chan podReadyInfo))
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
				close(ch.(chan podReadyInfo))
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
		slog.Warn("Worker pod terminated.", "id", id, "worker_pod", pod.Name, "phase", pod.Status.Phase)
		close(w.done)
	}
}

// SpawnWorker creates a new worker pod and waits for it to become ready.
// It acquires the spawn semaphore to limit concurrent K8s API calls and
// retries transient API errors with exponential backoff.
func (p *K8sWorkerPool) SpawnWorker(ctx context.Context, id int, image string) error {
	return p.spawnWorker(ctx, id, image, WorkerProfile{}, true)
}

func (p *K8sWorkerPool) spawnReservedWorker(ctx context.Context, slot *configstore.WorkerRecord) error {
	if slot == nil {
		return fmt.Errorf("missing reserved worker slot")
	}
	if p.spawnWarmWorkerFunc != nil {
		return p.spawnWarmWorkerFunc(ctx, slot.WorkerID)
	}
	// Reconstruct the pod shape from the persisted slot so a reserved colocated
	// worker is spawned bin-packed; default/legacy slots yield the zero profile.
	profile := WorkerProfile{CPU: slot.ProfileCPU, Memory: slot.ProfileMemory, Colocate: slot.ProfileColocate}
	return p.spawnWorker(ctx, slot.WorkerID, slot.Image, profile, false)
}

func (p *K8sWorkerPool) spawnWorker(ctx context.Context, id int, image string, profile WorkerProfile, publishIdle bool) error {
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
			RestartPolicy: corev1.RestartPolicyNever,
			// Give in-flight queries a real drain window on SIGTERM instead of
			// the 30s k8s default. Pairs with karpenter.sh/do-not-disrupt
			// (set on busy workers) so a node roll defers to the query; this is
			// the fallback for the residual race and involuntary eviction.
			TerminationGracePeriodSeconds: int64Ptr(workerTerminationGracePeriodSeconds),
			ServiceAccountName:            p.workerServiceAccountName(),
			AutomountServiceAccountToken:  boolPtr(false),
			PriorityClassName:             p.workerPriorityClassName,
			NodeSelector:                  p.nodeSelectorForProfile(profile),
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
	//
	// Colocated workers are exempt: they bin-pack onto the dedicated colocated
	// nodepool, which runs no cache-proxy DaemonSet (and has no NVMe to back
	// one). If they inherited DUCKGRES_CACHE_ENABLED they would block forever in
	// waitForCacheProxy() waiting on a proxy that never comes up, never answer
	// the control plane's gRPC health check, and churn. They talk to S3 directly.
	if os.Getenv("DUCKGRES_CACHE_ENABLED") == "true" && !profile.Colocate {
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

	// Add toleration if configured (colocated profiles use the bin-pack pool's taint)
	tolKey, tolValue := p.workerTolerationKey, p.workerTolerationValue
	if profile.Colocate {
		tolKey, tolValue = p.colocatedWorkerTolerationKey, p.colocatedWorkerTolerationValue
	}
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

	// One worker per instance (only for exclusive profiles on a dedicated node
	// pool). Colocated profiles deliberately skip anti-affinity so pods bin-pack.
	if p.workerExclusiveNode && !profile.Colocate {
		pod.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "duckgres-worker"},
					},
					TopologyKey: "kubernetes.io/hostname",
				}},
			},
		}
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
		spawnRecord.ProfileColocate = profile.Colocate
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
	client, err := waitForWorkerTCP(addr, token, serverCertPEM, 90*time.Second)
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

// AcquireWorker returns a worker for a new session.
func (p *K8sWorkerPool) AcquireWorker(ctx context.Context, _ *WorkerProfile) (*ManagedWorker, error) {
	// The flat K8s pool is the single-tenant backend; worker-profile selection is
	// a multi-tenant (OrgReservedPool) feature, so the profile is ignored here.
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
					id := p.allocateBackgroundSpawnIDLocked()
					p.spawning++
					p.mu.Unlock()
					slog.Debug("Assigned to least-loaded worker, spawning new worker in background.",
						"worker", w.ID, "active_sessions", w.activeSessions, "background_worker", id)
					go p.spawnWorkerBackground(id, p.workerImage)
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
			id := p.allocateBackgroundSpawnIDLocked()
			p.spawning++
			p.mu.Unlock()

			slog.Info("No live workers, blocking on spawn.", "worker", id)
			err := p.SpawnWorker(ctx, id, p.workerImage)

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
func (p *K8sWorkerPool) spawnWorkerBackground(id int, image string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	err := p.spawnWarmWorker(ctx, id, image, WorkerProfile{})

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
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	w.lastUsed = time.Now()
	p.mu.Unlock()
}

// RetireWorker removes a worker from the pool and deletes its pod.
func (p *K8sWorkerPool) RetireWorker(id int) {
	p.retireWorkerWithReason(id, RetireReasonNormal, LifecycleOriginPublicAPI)
}

// retireWorkerWithReason retires a worker and deletes its pod.
// Returns true if the worker was found and retired. origin labels the
// originating subsystem on the retire_local metric; it is required and
// has no default fallback so every call site is forced to declare its
// context.
func (p *K8sWorkerPool) retireWorkerWithReason(id int, reason string, origin LifecycleOrigin) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}
	if !p.markWorkerRetiredLocked(w, reason, origin) {
		p.mu.Unlock()
		return false
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	go p.retireWorkerPod(id, w)
	return true
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
func (p *K8sWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	return p.retireWorkerIfNoSessionsWithReason(id, RetireReasonNormal, LifecycleOriginPublicAPI)
}

func (p *K8sWorkerPool) retireWorkerIfNoSessionsWithReason(id int, reason string, origin LifecycleOrigin) bool {
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
		if !p.markWorkerRetiredLocked(w, reason, origin) {
			p.mu.Unlock()
			return false
		}
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

// RetireIfDrainingAndEmpty retires a worker that is draining and has no active
// sessions. Does NOT decrement activeSessions (caller must have already done so).
// Used by ReleaseWorker when TransitionToHotIdleIfNoSessions skips a non-hot worker.
//
// The transition is draining → retired, which has to go through the
// lifecycle service's dedicated RetireDrainingWorker CAS — NOT the
// generic UpsertWorkerRecord path that markWorkerRetiredLocked uses,
// because UpsertWorkerRecord's ON CONFLICT WHERE clause rejects
// updates against existing rows in {draining, retired, lost}. Routing
// this transition through UpsertWorkerRecord would always fence-miss
// and (post-P1-gate) leave the drained empty worker stuck in both the
// in-memory map and on K8s.
//
// On CAS miss / DB error the durable row stays in draining for the
// orphan sweep on a peer leader to reconcile; we don't touch the
// in-memory worker or the K8s pod because we haven't proven we still
// own the lease — the same peer-pod-delete safety that motivated the
// markWorkerRetiredLocked gate.
func (p *K8sWorkerPool) RetireIfDrainingAndEmpty(id int, origin LifecycleOrigin) {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok || w.activeSessions > 0 {
		p.mu.Unlock()
		return
	}
	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleDraining {
		p.mu.Unlock()
		return
	}
	lifecycle := p.ensureLifecycle()
	if lifecycle == nil {
		// No durable store wired (process backend / minimal test pool):
		// fall back to the in-memory + upsert path. persistWorkerRecord
		// is a no-op when runtimeStore is nil, so there's no fence-miss
		// to worry about.
		if !p.markWorkerRetiredLocked(w, RetireReasonNormal, origin) {
			p.mu.Unlock()
			return
		}
	} else {
		lease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
		outcome, err := lifecycle.RetireDrained(lease, RetireReasonNormal, origin)
		if err != nil {
			slog.Warn("RetireIfDrainingAndEmpty: CAS to retired failed; orphan sweep will reconcile.",
				"worker_id", id, "error", err)
			p.mu.Unlock()
			return
		}
		if !outcome.Transitioned {
			slog.Debug("RetireIfDrainingAndEmpty: retire-drained CAS missed; orphan sweep will reconcile.",
				"worker_id", id)
			p.mu.Unlock()
			return
		}
		p.markWorkerRetiredInMemoryLocked(w)
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)
	go p.retireWorkerPod(id, w)
}

// TransitionToHotIdleIfNoSessions decrements the worker's active session count
// and transitions a hot worker to hot_idle when its last session ends. The worker
// keeps its org assignment and DuckLake attachment so it can be quickly reclaimed
// for the same org. Returns true if the worker transitioned to hot_idle.
// The session decrement always happens regardless of the return value.
func (p *K8sWorkerPool) TransitionToHotIdleIfNoSessions(id int) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}
	if w.activeSessions > 0 {
		w.activeSessions--
	}
	if w.activeSessions != 0 {
		p.mu.Unlock()
		return false
	}
	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleHot {
		p.mu.Unlock()
		return false
	}
	nextState, err := w.SharedState().Transition(WorkerLifecycleHotIdle, nil)
	if err != nil {
		p.mu.Unlock()
		return false
	}
	if err := w.SetSharedState(nextState); err != nil {
		p.mu.Unlock()
		return false
	}
	w.lastUsed = time.Now()
	hotIdleRecord := p.workerRecordFor(id, w, w.OwnerEpoch(), configstore.WorkerStateHotIdle, "", nil)
	p.mu.Unlock()
	_ = p.persistWorkerRecord(hotIdleRecord)
	return true
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
	var prevState SharedWorkerState
	hadPrevState := false
	var activatingRecord *configstore.WorkerRecord
	switch worker.SharedState().NormalizedLifecycle() {
	case WorkerLifecycleReserved:
		prevState = worker.SharedState()
		hadPrevState = true
		nextState, transitionErr := worker.SharedState().Transition(WorkerLifecycleActivating, nil)
		if transitionErr == nil {
			transitionErr = worker.SetSharedState(nextState)
		}
		if transitionErr == nil {
			now := time.Now()
			activatingRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateActivating, "", &now)
		}
		err = transitionErr
	case WorkerLifecycleActivating:
		err = nil
		now := time.Now()
		activatingRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateActivating, "", &now)
	default:
		err = fmt.Errorf("worker %d is not reserved for activation", worker.ID)
	}
	p.mu.Unlock()
	if err != nil {
		return err
	}
	_ = p.persistWorkerRecord(activatingRecord)

	activate := p.activateTenantFunc
	if activate == nil {
		activate = func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
			return worker.ActivateTenant(ctx, server.WorkerActivationPayload{
				WorkerControlMetadata: server.WorkerControlMetadata{
					WorkerID:     worker.ID,
					OwnerEpoch:   worker.OwnerEpoch(),
					CPInstanceID: worker.OwnerCPInstanceID(),
				},
				OrgID:    payload.OrgID,
				DuckLake: payload.DuckLake,
				Iceberg:  payload.Iceberg,
			})
		}
	}

	if err := activate(ctx, worker, payload); err != nil {
		if hadPrevState {
			p.mu.Lock()
			_ = worker.SetSharedState(prevState)
			p.mu.Unlock()
		}
		p.retireWorkerWithReason(worker.ID, RetireReasonActivationFailure, LifecycleOriginActivationFailure)
		return err
	}

	p.mu.Lock()
	// Cache the activation payload for potential hot-idle reuse.
	cached := payload
	worker.cachedActivationPayload = &cached

	if worker.SharedState().NormalizedLifecycle() == WorkerLifecycleHot {
		p.mu.Unlock()
		return nil
	}
	nextState, err := worker.SharedState().Transition(WorkerLifecycleHot, nil)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if setErr := worker.SetSharedState(nextState); setErr != nil {
		p.mu.Unlock()
		return setErr
	}
	hotRecord := p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateHot, "", nil)
	p.mu.Unlock()
	_ = p.persistWorkerRecord(hotRecord)
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

		if p.runtimeStore != nil {
			// Try reclaiming a hot-idle worker for the same org first (fast path:
			// DuckLake is already attached, only needs epoch bump).
			if assignment.OrgID != "" {
				hotProfileCPU, hotProfileMem, hotProfileColo := assignment.Profile.Parts()
				hotClaimed, hotMissReason, err := p.runtimeStore.ClaimHotIdleWorker(p.cpInstanceID, assignment.OrgID, hotProfileCPU, hotProfileMem, hotProfileColo, assignment.MaxWorkers)
				if err != nil {
					return nil, err
				}
				if hotClaimed == nil && hotMissReason != configstore.WorkerClaimMissReasonNone && hotMissReason != configstore.WorkerClaimMissReasonNoIdle {
					p.recordWarmCapacityMiss(assignment, hotMissReason)
					return nil, NewWarmCapacityExhaustedErrorForReason(hotMissReason, DefaultWarmCapacityRetryAfter)
				}
				if hotClaimed != nil {
					// ClaimHotIdleWorker already filters by profile, so a reclaimed
					// hot-idle worker always matches the requested shape. It is NOT
					// filtered by image, so still retire on a version mismatch (e.g.
					// after an image bump) rather than serve a stale-version worker.
					if assignment.Image != "" && hotClaimed.Image != assignment.Image {
						slog.Info("Hot-idle worker image mismatch, retiring mismatched worker.", "worker_id", hotClaimed.WorkerID, "expected", assignment.Image, "got", hotClaimed.Image)
						p.retireClaimedWorker(hotClaimed, RetireReasonMismatchedVersion, LifecycleOriginReserveImageMismatch)
						// Fall through to neutral idle claim or capacity backpressure.
					} else {
						worker, reserveErr := p.reserveClaimedWorker(ctx, hotClaimed, assignment)
						if reserveErr == nil {
							worker.hotIdleReclaimed = true
							return worker, nil
						}
						if stderrors.Is(reserveErr, errStaleRuntimeWorkerClaim) {
							slog.Warn("Hot-idle worker claim was stale, retrying.", "worker_id", hotClaimed.WorkerID, "error", reserveErr)
							continue
						}
						slog.Warn("Hot-idle worker could not be reserved, retiring.", "worker_id", hotClaimed.WorkerID, "error", reserveErr)
						p.retireClaimedWorker(hotClaimed, RetireReasonCrash, LifecycleOriginReserveFailure)
						// Fall through to neutral idle claim.
					}
				}
			}

			p.mu.Lock()
			maxGlobalWorkers := p.maxWorkers
			p.mu.Unlock()

			profileCPU, profileMemory, profileColocate := assignment.Profile.Parts()
			claimed, missReason, err := p.runtimeStore.ClaimIdleWorker(p.cpInstanceID, assignment.OrgID, assignment.Image, profileCPU, profileMemory, profileColocate, assignment.MaxWorkers, maxGlobalWorkers, assignment.MaxColocatedCPU, assignment.MaxColocatedMemBytes)
			if err != nil {
				return nil, err
			}
			if claimed != nil {
				worker, reserveErr := p.reserveClaimedWorker(ctx, claimed, assignment)
				if reserveErr == nil {
					p.triggerPerImageReplenish(claimed.Image)
					if claimed.ProfileColocate {
						p.triggerColocatedWarmReplenish(WorkerProfile{CPU: claimed.ProfileCPU, Memory: claimed.ProfileMemory, Colocate: true})
					}
					return worker, nil
				}
				if stderrors.Is(reserveErr, errStaleRuntimeWorkerClaim) {
					slog.Warn("Idle worker claim was stale, retrying.", "worker_id", claimed.WorkerID, "worker_pod", claimed.PodName, "error", reserveErr)
					continue
				}
				slog.Warn("Claimed idle worker could not be reserved, retiring claimed pod.", "worker_id", claimed.WorkerID, "worker_pod", claimed.PodName, "error", reserveErr)
				p.retireClaimedWorker(claimed, RetireReasonCrash, LifecycleOriginReserveFailure)
				continue
			}

			p.recordWarmCapacityMiss(assignment, missReason)
			return nil, NewWarmCapacityExhaustedErrorForReason(missReason, DefaultWarmCapacityRetryAfter)
		}

		p.mu.Lock()
		if p.shuttingDown {
			p.mu.Unlock()
			return nil, fmt.Errorf("pool is shutting down")
		}

		p.cleanDeadWorkersLocked()

		// Runtime-store-less K8s mode uses the local worker map as its source
		// of truth. Filter by assignment.Image so a per-org pin is honored: if
		// the assignment names a specific image and no in-memory warm worker
		// matches, fail fast with warm-capacity backpressure. Warm capacity is
		// supplied by configured warm reconciliation rather than by the
		// foreground user connection.
		// Without this filter, a pinned org could be handed a default-image
		// warm worker, and activation would fail with a DuckLake/extension
		// version mismatch.
		idle := p.findReservableWarmWorkerLocked(assignment.Image, assignment.Profile)
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
			idle.SetOwnerCPInstanceID(p.cpInstanceID)
			idle.IncrementOwnerEpoch()
			idle.reservedAt = time.Now()
			reservedRecord := p.workerRecordFor(idle.ID, idle, idle.OwnerEpoch(), configstore.WorkerStateReserved, "", nil)
			shouldReplenish := p.shouldReplenishWarmCapacityLocked()
			var replenishID int
			if shouldReplenish {
				replenishID = p.allocateBackgroundSpawnIDLocked()
				p.spawning++
			}
			p.mu.Unlock()
			_ = p.persistWorkerRecord(reservedRecord)

			if err := p.checkReservedWorkerLiveness(ctx, idle); err != nil {
				slog.Warn("Reserved warm worker failed liveness recheck.", "worker", idle.ID, "error", err)
				p.retireWorkerWithReason(idle.ID, RetireReasonCrash, LifecycleOriginReserveFailure)
				p.mu.Lock()
				if shouldReplenish {
					p.spawning--
				}
				p.mu.Unlock()
				continue
			}

			if shouldReplenish {
				p.spawnWarmWorkerBackground(replenishID, p.workerImage)
			}
			p.triggerPerImageReplenish(idle.image)
			if idle.profile.Colocate {
				p.triggerColocatedWarmReplenish(idle.profile)
			}
			return idle, nil
		}

		p.mu.Unlock()

		p.recordWarmCapacityMiss(assignment, configstore.WorkerClaimMissReasonNoIdle)
		return nil, NewWarmCapacityExhaustedError(DefaultWarmCapacityRetryAfter)
	}
}

func (p *K8sWorkerPool) recordWarmCapacityMiss(assignment *WorkerAssignment, reason configstore.WorkerClaimMissReason) {
	// A server-side acquire wait polls every WarmAcquireRetryInterval; it sets
	// this to record demand + the miss metric at most once per throttle interval
	// rather than on every poll, so one waiting connection doesn't inflate the
	// demand signal and miss counter ~Nx.
	if assignment != nil && assignment.SuppressWarmMissRecord {
		return
	}
	policy := warmCapacityMissPolicyForReason(reason)
	image := ""
	if assignment != nil {
		image = strings.TrimSpace(assignment.Image)
	}
	if image == "" {
		image = strings.TrimSpace(p.workerImage)
	}
	if image == "" {
		return
	}

	observeWarmCapacityMiss(image, policy.reason)
	if !policy.recordDynamicDemand {
		return
	}
	if p.runtimeStore == nil {
		return
	}
	scope := warmCapacityScopeForImage(image)
	if err := p.runtimeStore.RecordWarmCapacityMiss(scope, policy.reason, time.Now()); err != nil {
		slog.Warn("Failed to record warm capacity miss.", "image", image, "reason", policy.reason, "error", err)
	}
}

func (p *K8sWorkerPool) reserveClaimedWorker(ctx context.Context, claimed *configstore.WorkerRecord, assignment *WorkerAssignment) (*ManagedWorker, error) {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}
	p.cleanDeadWorkersLocked()
	worker, ok := p.workers[claimed.WorkerID]
	p.mu.Unlock()

	if !ok {
		adopted, err := p.adoptClaimedWorker(ctx, claimed)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		if existing, exists := p.workers[claimed.WorkerID]; exists {
			p.mu.Unlock()
			if adopted.client != nil {
				_ = adopted.client.Close()
			}
			worker = existing
		} else {
			p.workers[claimed.WorkerID] = adopted
			p.mu.Unlock()
			worker = adopted
		}
	}

	p.mu.Lock()
	var reservedRecord *configstore.WorkerRecord
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}
	if claimed.PodName != "" {
		worker.podName = claimed.PodName
	}
	// Carry the worker's persisted pod-shape so the reserved record and any later
	// reconciliation round-trip it. Default/legacy rows yield the zero profile.
	worker.profile = WorkerProfile{CPU: claimed.ProfileCPU, Memory: claimed.ProfileMemory, Colocate: claimed.ProfileColocate}
	if worker.OwnerEpoch() > 0 && claimed.OwnerEpoch < worker.OwnerEpoch() {
		currentEpoch := worker.OwnerEpoch()
		p.mu.Unlock()
		return nil, fmt.Errorf("worker %d claimed with stale owner epoch %d behind current %d: %w", claimed.WorkerID, claimed.OwnerEpoch, currentEpoch, errStaleRuntimeWorkerClaim)
	}
	if isDuplicateRuntimeWorkerClaim(worker, claimed, assignment) {
		p.mu.Unlock()
		return nil, fmt.Errorf("worker %d already has in-flight claim for owner epoch %d: %w", claimed.WorkerID, claimed.OwnerEpoch, errStaleRuntimeWorkerClaim)
	}
	worker.SetOwnerCPInstanceID(claimed.OwnerCPInstanceID)
	worker.SetOwnerEpoch(claimed.OwnerEpoch)
	nextState, err := worker.SharedState().Transition(WorkerLifecycleReserved, assignment)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if err := worker.SetSharedState(nextState); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	worker.reservedAt = time.Now()
	if claimed.State != configstore.WorkerStateReserved {
		reservedRecord = p.workerRecordFor(worker.ID, worker, worker.OwnerEpoch(), configstore.WorkerStateReserved, "", nil)
	}
	p.mu.Unlock()
	if reservedRecord != nil {
		_ = p.persistWorkerRecord(reservedRecord)
	}
	if err := p.checkReservedWorkerLiveness(ctx, worker); err != nil {
		slog.Warn("Claimed worker failed liveness recheck.", "worker", worker.ID, "worker_pod", worker.PodName(), "error", err)
		p.retireWorkerWithReason(worker.ID, RetireReasonCrash, LifecycleOriginReserveFailure)
		return nil, err
	}
	return worker, nil
}

func isDuplicateRuntimeWorkerClaim(worker *ManagedWorker, claimed *configstore.WorkerRecord, assignment *WorkerAssignment) bool {
	if worker == nil || claimed == nil || assignment == nil {
		return false
	}
	if worker.OwnerCPInstanceID() != claimed.OwnerCPInstanceID || worker.OwnerEpoch() != claimed.OwnerEpoch {
		return false
	}
	state := worker.SharedState()
	switch state.NormalizedLifecycle() {
	case WorkerLifecycleReserved, WorkerLifecycleActivating:
	default:
		return false
	}
	if state.Assignment == nil {
		return false
	}
	return state.Assignment.OrgID == assignment.OrgID
}

func (p *K8sWorkerPool) claimSpecificWorker(ctx context.Context, workerID int, expectedOwnerEpoch int64, assignment *WorkerAssignment) (*ManagedWorker, error) {
	if p.runtimeStore == nil {
		return nil, fmt.Errorf("runtime worker store is not configured")
	}
	if err := validateWorkerAssignment(assignment); err != nil {
		return nil, err
	}
	record, err := p.runtimeStore.TakeOverWorker(workerID, p.cpInstanceID, assignment.OrgID, expectedOwnerEpoch)
	if err != nil {
		if stderrors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
			return nil, fmt.Errorf("worker %d ownership changed before takeover: %w", workerID, err)
		}
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("worker %d could not be claimed", workerID)
	}

	if assignment.Image != "" && record.Image != assignment.Image {
		return nil, fmt.Errorf("worker %d image mismatch (expected %q, got %q)", workerID, assignment.Image, record.Image)
	}
	if expCPU, expMem, expColo := assignment.Profile.Parts(); record.ProfileCPU != expCPU || record.ProfileMemory != expMem || record.ProfileColocate != expColo {
		return nil, fmt.Errorf("worker %d profile mismatch (expected %s/%s/colocate=%v, got %s/%s/colocate=%v)",
			workerID, expCPU, expMem, expColo, record.ProfileCPU, record.ProfileMemory, record.ProfileColocate)
	}

	return p.reserveClaimedWorker(ctx, record, assignment)
}

func (p *K8sWorkerPool) adoptClaimedWorker(ctx context.Context, claimed *configstore.WorkerRecord) (*ManagedWorker, error) {
	token, _, err := p.readWorkerRPCSecurity(ctx, claimed.PodName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	pod, err := p.clientset.CoreV1().Pods(p.namespace).Get(ctx, claimed.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get claimed worker pod %s: %w", claimed.PodName, err)
	}

	// For hot-idle workers, skip the epoch-validated health check. The worker's
	// epoch and CP instance ID are from the previous owner, and ClaimHotIdleWorker
	// already bumped the epoch in the DB. The health check requires exact epoch
	// match, so neither the old epoch (worker has N, we'd send N+1) nor the new
	// CP instance ID will pass. ActivateTenant validates the epoch properly
	// (accepts > current). For fresh neutral workers (epoch 0), use the normal
	// health-checked path.
	var client *flightsql.Client
	if claimed.OwnerEpoch > 1 {
		client, err = p.connectWorkerDirect(ctx, claimed.PodName, pod.Status.PodIP, token)
	} else {
		client, err = p.connectWorker(ctx, claimed.PodName, pod.Status.PodIP, token)
	}
	if err != nil {
		return nil, err
	}
	worker := &ManagedWorker{
		ID:          claimed.WorkerID,
		podName:     claimed.PodName,
		image:       claimed.Image,
		bearerToken: token,
		client:      client,
		done:        make(chan struct{}),
	}
	worker.SetOwnerCPInstanceID(claimed.OwnerCPInstanceID)
	worker.SetOwnerEpoch(claimed.OwnerEpoch)
	return worker, nil
}

func (p *K8sWorkerPool) connectWorker(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
	return p.connectWorkerWithHealthCheck(ctx, podName, podIP, bearerToken, server.WorkerHealthCheckPayload{})
}

// connectWorkerDirect establishes a gRPC connection without running a health
// check. Used for hot-idle adoption where the worker's epoch/CP metadata
// doesn't match the new owner yet (ActivateTenant will update it).
func (p *K8sWorkerPool) connectWorkerDirect(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
	if p.connectWorkerFunc != nil {
		return p.connectWorkerFunc(ctx, podName, podIP, bearerToken)
	}
	if podIP == "" {
		return nil, fmt.Errorf("worker pod %s has no IP", podName)
	}
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	_, serverCertPEM, err := p.readWorkerRPCSecurity(ctx, podName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(serverCertPEM) {
		return nil, fmt.Errorf("parse worker RPC server certificate")
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: workerRPCDNSName,
	}
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
	if err != nil {
		return nil, fmt.Errorf("connect to claimed worker %s: %w", podName, err)
	}
	return client, nil
}

func (p *K8sWorkerPool) connectWorkerWithHealthCheck(ctx context.Context, podName, podIP, bearerToken string, hcPayload server.WorkerHealthCheckPayload) (*flightsql.Client, error) {
	if p.connectWorkerFunc != nil {
		return p.connectWorkerFunc(ctx, podName, podIP, bearerToken)
	}
	if podIP == "" {
		return nil, fmt.Errorf("worker pod %s has no IP", podName)
	}
	addr := fmt.Sprintf("%s:%d", podIP, p.workerPort)
	_, serverCertPEM, err := p.readWorkerRPCSecurity(ctx, podName)
	if err != nil {
		return nil, fmt.Errorf("read worker RPC security: %w", err)
	}
	client, err := waitForWorkerTCPWithMetadata(addr, bearerToken, serverCertPEM, 30*time.Second, hcPayload)
	if err != nil {
		return nil, fmt.Errorf("connect to claimed worker %s: %w", podName, err)
	}
	return client, nil
}

func (p *K8sWorkerPool) retireCurrentRuntimeWorker(claimed *configstore.WorkerRecord, reason string, origin LifecycleOrigin) {
	if claimed == nil {
		return
	}
	current := claimed
	if p.runtimeStore != nil {
		// Refresh the row so the snapshot we hand to retireClaimedWorker
		// (and through it, lifecycle.RetireFromSnapshot) carries the
		// latest updated_at — without this, an inter-spawn touch could
		// CAS-miss even though we still own the row at the right epoch.
		// We could rely solely on the CAS to fence (and it does), but
		// the extra read is cheap and lets us short-circuit before
		// scheduling pod cleanup against a moved row.
		refreshed, err := p.runtimeStore.ObserveWorker(claimed.WorkerID)
		switch {
		case err != nil:
			slog.Warn("Failed to refresh worker runtime row before retirement.",
				"worker_id", claimed.WorkerID, "reason", reason, "origin", origin, "error", err)
		case refreshed == nil:
			slog.Debug("Worker runtime row missing before retirement; skipping cleanup.",
				"worker_id", claimed.WorkerID, "reason", reason, "origin", origin)
			return
		default:
			if refreshed.OwnerCPInstanceID() != claimed.OwnerCPInstanceID || refreshed.OwnerEpoch() != claimed.OwnerEpoch {
				slog.Debug("Worker ownership changed before retirement; skipping cleanup.",
					"worker_id", claimed.WorkerID, "reason", reason, "origin", origin)
				return
			}
			rec := refreshed.Record()
			current = &rec
		}
	}
	p.retireClaimedWorker(current, reason, origin)
}

func (p *K8sWorkerPool) retireClaimedWorker(claimed *configstore.WorkerRecord, reason string, origin LifecycleOrigin) {
	if claimed == nil {
		return
	}
	lifecycle := p.ensureLifecycle()
	if lifecycle == nil {
		return
	}
	terminalState := configstore.WorkerStateRetired
	if reason == RetireReasonCrash {
		terminalState = configstore.WorkerStateLost
	}
	// The claimed record is fresh enough to act as a snapshot — it came
	// back from a Claim*/TakeOver*/Create*Slot call and any concurrent
	// change will simply CAS-miss inside MarkWorkerTerminalIfCurrent.
	// Pod cleanup is scheduled by the lifecycle on a successful CAS via
	// the WorkerPhysicalCleanup (this pool's DeleteWorkerArtifacts).
	snap := configstore.NewWorkerSnapshot(*claimed)
	if _, err := lifecycle.RetireFromSnapshot(snap, terminalState, reason, origin); err != nil {
		slog.Warn("Failed to retire claimed worker.",
			"worker_id", claimed.WorkerID, "reason", reason, "origin", origin, "error", err)
	}
}

// DeleteWorkerArtifacts implements WorkerPhysicalCleanup. Schedules pod
// + RPC secret + local-pool cleanup for the named worker, called by
// WorkerLifecycle after a durable CAS to terminal has landed.
func (p *K8sWorkerPool) DeleteWorkerArtifacts(workerID int, podName, reason string) {
	p.deleteRetiredRuntimeWorker(&configstore.WorkerRecord{WorkerID: workerID, PodName: podName}, reason)
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

// deleteRetiredRuntimeWorker performs the post-CAS cleanup for a worker whose
// runtime-store row is already terminal. It intentionally avoids another DB
// transition so janitor paths can retire once, then reliably remove local state
// and delete the pod.
func (p *K8sWorkerPool) deleteRetiredRuntimeWorker(record *configstore.WorkerRecord, reason string) {
	if record == nil {
		return
	}

	worker := &ManagedWorker{
		ID:      record.WorkerID,
		podName: record.PodName,
		done:    make(chan struct{}),
	}
	worker.SetOwnerCPInstanceID(record.OwnerCPInstanceID)
	worker.SetOwnerEpoch(record.OwnerEpoch)

	removedLocal := false
	workerCount := 0
	p.mu.Lock()
	if local, ok := p.workers[record.WorkerID]; ok {
		worker = local
		p.markWorkerRetiredInMemoryLocked(worker)
		delete(p.workers, record.WorkerID)
		workerCount = len(p.workers)
		removedLocal = true
	}
	p.mu.Unlock()
	if removedLocal {
		observeControlPlaneWorkers(workerCount)
	}

	go p.retireWorkerPod(record.WorkerID, worker)
}

func (p *K8sWorkerPool) checkReservedWorkerLiveness(ctx context.Context, worker *ManagedWorker) error {
	check := p.healthCheckFunc
	if check == nil {
		check = func(ctx context.Context, worker *ManagedWorker) error {
			if worker == nil || worker.client == nil {
				return fmt.Errorf("worker client is not available")
			}
			hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := doHealthCheckWithMetadata(hctx, worker.client, p.healthCheckPayloadForWorker(worker))
			return err
		}
	}
	return check(ctx, worker)
}

// SpawnMinWorkers pre-warms the pool with count workers.
func (p *K8sWorkerPool) SpawnMinWorkers(count int) error {
	if count <= 0 {
		return nil
	}

	if p.runtimeStore != nil {
		p.mu.Lock()
		if count > p.minWorkers {
			p.minWorkers = count
		}
		p.mu.Unlock()

		// Claim all slots first, then spawn in parallel.
		var slots []*configstore.WorkerRecord
		for i := 0; i < count; i++ {
			slot, err := p.runtimeStore.CreateNeutralWarmWorkerSlot(
				p.cpInstanceID,
				p.workerPodNamePrefix(),
				p.workerImage,
				"", "", false,
				count,
				p.maxWorkers,
			)
			if err != nil {
				// Retire already-claimed slots so they don't strand
				// as durable spawning rows with no pod behind them.
				for _, s := range slots {
					p.retireCurrentRuntimeWorker(s, RetireReasonCrash, LifecycleOriginSpawnFailure)
				}
				return err
			}
			if slot == nil {
				break
			}
			slots = append(slots, slot)
		}

		if len(slots) == 0 {
			return nil
		}

		p.mu.Lock()
		p.spawning += len(slots)
		p.mu.Unlock()

		ctx := context.Background()
		var wg sync.WaitGroup
		errs := make([]error, len(slots))
		for i, slot := range slots {
			wg.Add(1)
			go func(i int, slot *configstore.WorkerRecord) {
				defer wg.Done()
				err := p.spawnWarmWorker(ctx, slot.WorkerID, slot.Image, WorkerProfile{CPU: slot.ProfileCPU, Memory: slot.ProfileMemory, Colocate: slot.ProfileColocate})

				p.mu.Lock()
				p.spawning--
				p.mu.Unlock()

				if err != nil {
					p.retireCurrentRuntimeWorker(slot, RetireReasonCrash, LifecycleOriginSpawnFailure)
					errs[i] = err
				}
			}(i, slot)
		}
		wg.Wait()
		return stderrors.Join(errs...)
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
		ids = append(ids, p.allocateBackgroundSpawnIDLocked())
		p.spawning++
	}
	p.mu.Unlock()

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, len(ids))
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id int) {
			defer wg.Done()
			err := p.spawnWarmWorker(ctx, id, p.workerImage, WorkerProfile{})

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			errs[i] = err
		}(i, id)
	}
	wg.Wait()
	return stderrors.Join(errs...)
}

// triggerPerImageReplenish kicks off a background spawn for image if the
// per-image warm floor isn't met. Fire-and-forget; the janitor catches any
// we miss on its next 5s tick. Cuts the gap between consuming a pinned warm
// worker and seeing a fresh one in its place from up-to-5s down to ~now.
func (p *K8sWorkerPool) triggerPerImageReplenish(image string) {
	if strings.TrimSpace(image) == "" || p.runtimeStore == nil {
		return
	}
	p.mu.RLock()
	target := p.perImageWarmTarget[image]
	p.mu.RUnlock()
	if target <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), warmSpawnReconcileTimeout)
		defer cancel()
		if err := p.SpawnMinWorkersForImage(ctx, image, target); err != nil {
			slog.Warn("Per-image warm replenish failed.", "image", image, "error", err)
		}
	}()
}

// SpawnMinWorkersForImage ensures at least count warm-idle workers exist for
// the given image, additive to (and independent of) the cluster-default warm
// pool managed by SpawnMinWorkers. Per-image floor lets a per-org image pin
// always find a hot pod waiting instead of paying a cold spawn on first
// connection.
//
// Image-aware DB count and the per-image advisory lock keep this safe for
// concurrent reconcilers across multiple control planes.
func (p *K8sWorkerPool) SpawnMinWorkersForImage(ctx context.Context, image string, count int) error {
	if count <= 0 || strings.TrimSpace(image) == "" {
		return nil
	}
	if p.runtimeStore == nil {
		// Single-CP / no-runtime-store mode is not multi-tenant, so per-image
		// pinning doesn't apply — caller's image == p.workerImage.
		return nil
	}

	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil
	}
	p.cleanDeadWorkersLocked()
	idleByImage := p.idleWarmWorkerCountByImageLocked()
	missing := count - idleByImage[image]
	if missing <= 0 {
		p.mu.Unlock()
		return nil
	}
	// Exclusive-only room: this spawns default-shape (exclusive) warm workers,
	// so colocated workers must not shrink the budget — consistent with the
	// exclusive-only DB-side global cap in CreateNeutralWarmWorkerSlotForImage.
	if p.maxWorkers > 0 {
		room := p.maxWorkers - p.liveExclusiveWorkerCountLocked()
		if room <= 0 {
			p.mu.Unlock()
			return nil
		}
		if missing > room {
			missing = room
		}
	}
	p.mu.Unlock()

	var slots []*configstore.WorkerRecord
	for i := 0; i < missing; i++ {
		slot, err := p.runtimeStore.CreateNeutralWarmWorkerSlotForImage(
			p.cpInstanceID,
			p.workerPodNamePrefix(),
			image,
			count,
			p.maxWorkers,
		)
		if err != nil {
			for _, s := range slots {
				p.retireCurrentRuntimeWorker(s, RetireReasonCrash, LifecycleOriginSpawnFailure)
			}
			return err
		}
		if slot == nil {
			break
		}
		slots = append(slots, slot)
	}

	if len(slots) == 0 {
		return nil
	}

	p.mu.Lock()
	p.spawning += len(slots)
	p.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(slots))
	for i, slot := range slots {
		wg.Add(1)
		go func(i int, slot *configstore.WorkerRecord) {
			defer wg.Done()
			err := p.spawnWarmWorker(ctx, slot.WorkerID, slot.Image, WorkerProfile{CPU: slot.ProfileCPU, Memory: slot.ProfileMemory, Colocate: slot.ProfileColocate})

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			if err != nil {
				p.retireCurrentRuntimeWorker(slot, RetireReasonCrash, LifecycleOriginSpawnFailure)
				errs[i] = err
			}
		}(i, slot)
	}
	wg.Wait()
	return stderrors.Join(errs...)
}

// idleColocatedWarmWorkerCountLocked counts warm-idle workers matching the given
// colocated shape. Caller holds p.mu.
func (p *K8sWorkerPool) idleColocatedWarmWorkerCountLocked(profile WorkerProfile) int {
	want := profile.MatchKey()
	count := 0
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.profile.MatchKey() != want {
			continue
		}
		if p.isWarmIdleWorkerLocked(w) {
			count++
		}
	}
	return count
}

// SpawnMinColocatedWorkers ensures at least `target` warm-idle workers of the
// given colocated shape (default image) exist, so a matching colocate=true
// request bursts into a ready pod instead of paying a cold spawn. Mirrors
// SpawnMinWorkersForImage but for a specific colocated shape; a no-op without a
// runtime store, an incomplete/non-colocated shape, or target <= 0.
func (p *K8sWorkerPool) SpawnMinColocatedWorkers(ctx context.Context, profile WorkerProfile, target int) error {
	if target <= 0 || p.runtimeStore == nil || profile.CPU == "" || profile.Memory == "" || !profile.Colocate {
		return nil
	}

	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil
	}
	p.cleanDeadWorkersLocked()
	missing := target - p.idleColocatedWarmWorkerCountLocked(profile)
	if missing <= 0 {
		p.mu.Unlock()
		return nil
	}
	// No exclusive worker-count cap here: colocated workers bin-pack and are
	// intentionally unbounded (the DB-side global cap is exclusive-only and
	// skips colocated too). `missing` is already bounded by this shape's warm
	// target, and CreateNeutralWarmWorkerSlot enforces that target per shape,
	// so the spawn loop self-limits without consulting p.maxWorkers.
	p.mu.Unlock()

	var slots []*configstore.WorkerRecord
	for i := 0; i < missing; i++ {
		slot, err := p.runtimeStore.CreateNeutralWarmWorkerSlot(
			p.cpInstanceID,
			p.workerPodNamePrefix(),
			p.workerImage,
			profile.CPU,
			profile.Memory,
			profile.Colocate,
			target,
			p.maxWorkers,
		)
		if err != nil {
			for _, s := range slots {
				p.retireCurrentRuntimeWorker(s, RetireReasonCrash, LifecycleOriginSpawnFailure)
			}
			return err
		}
		if slot == nil {
			break
		}
		slots = append(slots, slot)
	}

	if len(slots) == 0 {
		return nil
	}

	p.mu.Lock()
	p.spawning += len(slots)
	p.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(slots))
	for i, slot := range slots {
		wg.Add(1)
		go func(i int, slot *configstore.WorkerRecord) {
			defer wg.Done()
			err := p.spawnWarmWorker(ctx, slot.WorkerID, slot.Image, WorkerProfile{CPU: slot.ProfileCPU, Memory: slot.ProfileMemory, Colocate: slot.ProfileColocate})

			p.mu.Lock()
			p.spawning--
			p.mu.Unlock()

			if err != nil {
				p.retireCurrentRuntimeWorker(slot, RetireReasonCrash, LifecycleOriginSpawnFailure)
				errs[i] = err
			}
		}(i, slot)
	}
	wg.Wait()
	return stderrors.Join(errs...)
}

// reconcileColocatedWarm tops every configured colocated warm shape up to its
// target. Called from the periodic warm-capacity reconcile.
func (p *K8sWorkerPool) reconcileColocatedWarm(ctx context.Context) {
	for _, shape := range p.colocatedWarmShapes {
		profile := WorkerProfile{CPU: shape.CPU, Memory: shape.Memory, Colocate: true}
		if err := p.SpawnMinColocatedWorkers(ctx, profile, shape.Target); err != nil {
			slog.Warn("Colocated warm reconcile failed.", "cpu", shape.CPU, "memory", shape.Memory, "target", shape.Target, "error", err)
		}
	}
}

// triggerColocatedWarmReplenish tops the just-claimed colocated shape's warm pool
// back up in the background, so the next matching request still finds a ready pod.
// A no-op if the claimed shape isn't a configured warm shape.
func (p *K8sWorkerPool) triggerColocatedWarmReplenish(profile WorkerProfile) {
	if p.runtimeStore == nil {
		return
	}
	target := 0
	for _, shape := range p.colocatedWarmShapes {
		if shape.CPU == profile.CPU && shape.Memory == profile.Memory {
			target = shape.Target
			break
		}
	}
	if target <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), warmSpawnReconcileTimeout)
		defer cancel()
		if err := p.SpawnMinColocatedWorkers(ctx, profile, target); err != nil {
			slog.Warn("Colocated warm replenish failed.", "cpu", profile.CPU, "memory", profile.Memory, "error", err)
		}
	}()
}

// HealthCheckLoop periodically checks worker health.
func (p *K8sWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var mu sync.Mutex
	failures := make(map[workerLeaseSnapshot]int)

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
			type healthCheckTarget struct {
				worker *ManagedWorker
				lease  workerLeaseSnapshot
			}
			targets := make([]healthCheckTarget, 0, len(p.workers))
			for _, w := range p.workers {
				targets = append(targets, healthCheckTarget{
					worker: w,
					lease:  p.workerLeaseSnapshot(w),
				})
			}
			p.mu.RUnlock()

			var wg sync.WaitGroup
			for _, target := range targets {
				wg.Add(1)
				go func(w *ManagedWorker, lease workerLeaseSnapshot) {
					defer wg.Done()

					select {
					case <-ctx.Done():
						return
					case <-w.done:
						// Pod terminated (detected by informer) — distinct
						// from the periodic-probe path below, which uses
						// LifecycleOriginHealthCheckCrash.
						mu.Lock()
						delete(failures, lease)
						mu.Unlock()

						lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginInformerCrash)
						if err != nil {
							slog.Error("K8s worker terminated but lease validation failed; leaving cleanup to retry.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
							return
						}
						if lostDisposition == workerLostLeaseRetry {
							slog.Warn("K8s worker terminated while runtime lease is newer for this CP; leaving cleanup to retry.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch)
							return
						}
						if lostDisposition == workerLostLeaseStale {
							p.mu.Lock()
							removedWorker, workerCount := p.dropLocalWorkerIfSameLeaseLocked(lease)
							p.mu.Unlock()
							if removedWorker == nil {
								return
							}
							observeControlPlaneWorkers(workerCount)
							slog.Warn("K8s worker terminated under stale lease; dropping local worker without pod delete.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch)
							if removedWorker.client != nil {
								_ = removedWorker.client.Close()
							}
							return
						}

						p.mu.Lock()
						removedWorker, workerCount, replacementID, shouldReplenish := p.removeWorkerAfterLostLeaseLocked(lease)
						p.mu.Unlock()
						if removedWorker == nil {
							return
						}
						observeControlPlaneWorkers(workerCount)
						slog.Warn("K8s worker crashed.", "id", lease.workerID)
						if onCrash != nil {
							onCrash(lease.workerID)
						}
						if removedWorker.client != nil {
							_ = removedWorker.client.Close()
						}
						// Delete the failed pod from K8s
						podName := p.workerPodName(removedWorker)
						delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
						_ = p.clientset.CoreV1().Pods(p.namespace).Delete(delCtx, podName, metav1.DeleteOptions{
							GracePeriodSeconds: int64Ptr(0),
						})
						delCancel()
						if shouldReplenish {
							p.spawnWarmWorkerBackground(replacementID, p.workerImage)
						}
					default:
						// Worker alive, do health check
						var healthErr error
						var hcResult *healthCheckResult
						func() {
							defer recoverWorkerPanic(&healthErr)
							hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
							hcResult, healthErr = doHealthCheckWithMetadata(hctx, w.client, p.healthCheckPayloadForLease(lease))
							cancel()
						}()
						// Skip both metric emission AND failure-counter
						// increment when the parent loop ctx was canceled
						// (CP shutting down). Without this guard:
						// (a) every graceful rollout would spike
						//     duckgres_worker_health_checks_total{result=fail}
						//     indistinguishably from real worker crashes;
						// (b) a worker one failure short of
						//     maxConsecutiveHealthFailures could be tipped
						//     over by a shutdown-induced Canceled and
						//     erroneously marked Lost.
						// Per-probe deadline (DeadlineExceeded) is a real
						// failure and still counted via the else branch.
						if stderrors.Is(healthErr, context.Canceled) {
							return
						}
						if healthErr != nil {
							observeHealthCheck(HealthCheckResultFail, lease.image)
						} else {
							observeHealthCheck(HealthCheckResultPass, lease.image)
						}

						if healthErr != nil {
							mu.Lock()
							failures[lease]++
							count := failures[lease]
							mu.Unlock()

							slog.Warn("K8s worker health check failed.", "id", lease.workerID, "error", healthErr, "consecutive_failures", count)

							if count >= maxConsecutiveHealthFailures {
								// Planned node disruption (Karpenter drift/
								// consolidation, kubelet drain) is not a worker
								// crash: the pod is already Terminating. Marking a
								// BUSY worker Lost here cancels its in-flight query
								// after ~3 health intervals (~5s), even though the
								// worker is draining and its node typically lives
								// ~100s+ longer. For a busy worker, defer to the
								// informer-driven w.done path above (fires when the
								// pod is actually gone and runs the same cleanup) so
								// the query drains, bounded by the pod's
								// terminationGracePeriodSeconds. Cache-only read.
								//
								// Gate on activeSessions>0: an idle worker has no
								// query to protect, and leaving a Terminating-but-
								// not-yet-deleted worker in the pool would let
								// AcquireWorker route a new session to a shutting-
								// down pod (findIdle/leastLoaded only skip workers
								// whose done channel is closed, i.e. already gone).
								// So idle workers fall through and are marked Lost
								// promptly, exactly as before this change.
								p.mu.RLock()
								active := w.activeSessions
								p.mu.RUnlock()
								if active > 0 && p.workerPodTerminating(p.workerPodName(w)) {
									slog.Warn("Busy K8s worker failing health checks while pod is Terminating (planned node drain); deferring to graceful drain instead of canceling its query.",
										"id", lease.workerID, "pod", p.workerPodName(w), "active_sessions", active, "consecutive_failures", count)
									return
								}
								lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginHealthCheckCrash)
								if err != nil {
									slog.Error("K8s worker unresponsive but lease validation failed; leaving cleanup to retry.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count, "error", err)
									return
								}

								if lostDisposition == workerLostLeaseRetry {
									slog.Warn("K8s worker unresponsive while runtime lease is newer for this CP; leaving cleanup to retry.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count)
									return
								}

								mu.Lock()
								delete(failures, lease)
								mu.Unlock()

								if lostDisposition == workerLostLeaseStale {
									p.mu.Lock()
									removedWorker, workerCount := p.dropLocalWorkerIfSameLeaseLocked(lease)
									p.mu.Unlock()
									if removedWorker == nil {
										return
									}
									observeControlPlaneWorkers(workerCount)
									slog.Warn("K8s worker unresponsive under stale lease; dropping local worker without crash notification or pod delete.", "id", lease.workerID, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "consecutive_failures", count)
									if removedWorker.client != nil {
										_ = removedWorker.client.Close()
									}
									return
								}

								p.mu.Lock()
								removedWorker, workerCount, replacementID, shouldReplenish := p.removeWorkerAfterLostLeaseLocked(lease)
								p.mu.Unlock()
								if removedWorker == nil {
									return
								}
								observeControlPlaneWorkers(workerCount)

								slog.Error("K8s worker unresponsive, deleting pod.", "id", lease.workerID, "consecutive_failures", count)
								if onCrash != nil {
									onCrash(lease.workerID)
								}
								// Delete the pod to force cleanup
								podName := p.workerPodName(removedWorker)
								_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
									GracePeriodSeconds: int64Ptr(10),
								})
								if removedWorker.client != nil {
									_ = removedWorker.client.Close()
								}
								if shouldReplenish {
									p.spawnWarmWorkerBackground(replacementID, p.workerImage)
								}
							}
						} else {
							mu.Lock()
							delete(failures, lease)
							mu.Unlock()

							// Forward progress data to the control plane.
							if onProgress != nil && hcResult != nil {
								if sp := hcResult.toSessionProgress(); len(sp) > 0 {
									onProgress(lease.workerID, sp)
								}
							}
						}
					}
				}(target.worker, target.lease)
			}
			wg.Wait()
		}
	}
}

// ShutdownAll stops all workers by deleting their pods. Per worker it runs a
// 3-step CAS chain against the runtime store and K8s API:
//
//  1. MarkWorkerDraining — atomic SQL CAS from a non-terminal state to
//     draining. Fences the worker against claims by other CPs: their claim
//     queries match state=idle/hot_idle, which no longer apply. If the CAS
//     misses (row already terminal or owned by another CP) the worker is
//     skipped entirely.
//  2. K8s pod delete. Only reached after the CAS succeeds. NotFound is
//     treated as success (the pod is gone by some other path).
//  3. RetireDrainingWorker — atomic SQL CAS draining → retired. Only reached
//     on successful pod delete. On delete failure the row stays in
//     draining, which lets ListOrphanedWorkers pick it up once the CP's
//     heartbeat expires, and lets cleanupOrphanedWorkerPods delete the pod
//     by label on the next janitor tick.
//
// This ordering closes the old race where the DB row was flipped to retired
// before the pod delete: if the delete failed, the pod survived forever
// because terminal-state rows are excluded from ListOrphanedWorkers.
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
	preserved := make(map[int]*ManagedWorker)
	for _, w := range workers {
		podName := p.workerPodName(w)

		// A worker that's serving sessions must not be torn down during
		// CP shutdown — pod-deletion would kill in-flight customer queries
		// at exactly the moment ShutdownAll runs (the failure mode hit by
		// the production worker-40761 incident on a 15-minute drain wall).
		// Leave the worker in `hot` state, owned by this dying CP. Two
		// downstream guarantees keep this safe:
		//   (1) Flight clients can reconnect by session token; a peer CP
		//       claims via TakeOverWorker and the query resumes.
		//   (2) ListOrphanedWorkers' JOIN against flight_session_records
		//       prevents peer CPs' janitors from retiring the worker
		//       while a session is still active or reconnecting; once the
		//       session record is expired the worker is retired normally.
		// pgwire customers connected to this CP have already lost their
		// connection (the CP socket is going away) — protecting the
		// worker doesn't help them, but it doesn't hurt either.
		if w.activeSessions > 0 {
			slog.Info("ShutdownAll: worker has active sessions; leaving pod alive for Flight reconnect.",
				"id", w.ID, "worker_pod", podName, "active_sessions", w.activeSessions)
			preserved[w.ID] = w
			continue
		}

		slog.Info("Shutting down K8s worker.", "id", w.ID, "worker_pod", podName)

		// Step 1: CAS to draining. Skip the worker on CAS miss or error —
		// there's no safe way to proceed if we don't own the row.
		// Mint the lease right before the CAS so the in-memory epoch
		// reflects any concurrent cred-refresh bump that landed between
		// the workers-list snapshot above and now. Step 3 below re-mints
		// for the same reason — see the comment there.
		lifecycle := p.ensureLifecycle()
		if lifecycle == nil {
			continue
		}
		// Wrap the per-worker drain in a closure. Returns true when
		// the in-memory worker should be flipped to Retired — i.e.
		// whenever this CP has taken any irreversible local action
		// against the worker (closed its gRPC client). That covers
		// both the fully-successful drain and the pod-delete-error
		// branch (where the pod is still alive but we closed the
		// client because this CP is shutting down). Returns false
		// only when Drain itself missed and no local mutation
		// happened.
		shouldMarkRetired := func() bool {
			drainStart := time.Now()

			lease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
			outcome, err := lifecycle.Drain(lease, LifecycleOriginShutdownAll)
			if err != nil {
				slog.Warn("ShutdownAll: CAS to draining failed; orphan sweep will reconcile.",
					"worker_id", w.ID, "error", err)
				return false
			}
			if !outcome.Transitioned {
				slog.Debug("ShutdownAll: worker not owned by us or already terminal; skipping.",
					"worker_id", w.ID)
				return false
			}

			// Step 2: delete pod. On API error other than NotFound we
			// leave the durable row in draining for the orphan sweep
			// AND close the client — this CP is going away regardless
			// and the gRPC client serves no further purpose. Returning
			// true keeps the in-memory state consistent with the
			// closed client (no goroutine holding `w` should see
			// Idle/Hot while reaching for a dead connection).
			gracePeriod := int64(10)
			if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
				GracePeriodSeconds: &gracePeriod,
			}); err != nil && !errors.IsNotFound(err) {
				slog.Warn("ShutdownAll: pod delete failed; worker left in draining for orphan sweep/reconciler.",
					"id", w.ID, "worker_pod", podName, "error", err)
				if w.client != nil {
					_ = w.client.Close()
				}
				return true
			}
			_ = p.deleteWorkerRPCSecret(ctx, podName)
			if w.client != nil {
				_ = w.client.Close()
			}
			// Record drain duration at the pod-delete-success
			// milestone — every sample reflects a real
			// gone-from-the-cluster event, so p99 alerts track the
			// operationally meaningful tail. The terminal CAS below
			// is best-effort cleanup against an already-dead worker.
			observeDrainTotalDuration(time.Since(drainStart))

			// Step 3: best-effort terminal CAS. Re-read w.OwnerEpoch()
			// so a cred-refresh bump that landed between Step 1 and
			// now uses the fresh epoch. CAS-miss (Transitioned=false,
			// err=nil) is logged at Debug for diagnostic
			// continuity — the durable row stays in draining for the
			// orphan sweep on a peer CP to reconcile.
			lateLease := configstore.NewWorkerLease(w.ID, p.cpInstanceID, w.OwnerEpoch(), w.image)
			retireOutcome, err := lifecycle.RetireDrained(lateLease, RetireReasonShutdown, LifecycleOriginShutdownAll)
			switch {
			case err != nil:
				slog.Warn("ShutdownAll: CAS to retired failed; orphan sweep will reconcile durable row.",
					"worker_id", w.ID, "error", err)
			case !retireOutcome.Transitioned:
				slog.Debug("ShutdownAll: retire-drained CAS missed; orphan sweep will reconcile durable row.",
					"worker_id", w.ID)
			}
			return true
		}()

		if !shouldMarkRetired {
			continue
		}

		// In-memory lifecycle bookkeeping. Intentionally skips
		// persistence (the CAS chain already persisted retired/draining)
		// and the retire_local metric (lifecycle.RetireDrained already
		// observed retire_drained for this transition).
		p.mu.Lock()
		p.markWorkerRetiredInMemoryLocked(w)
		p.mu.Unlock()
	}

	// Workers preserved due to active sessions stay in the in-memory map
	// so any remaining session bookkeeping inside this CP still finds them
	// during the residual shutdown window. The map is wiped on process
	// exit; preservation is purely about not yanking the rug here.
	p.mu.Lock()
	p.workers = preserved
	p.mu.Unlock()
	observeControlPlaneWorkers(len(preserved))
}

// retireWorkerPod closes the gRPC client and deletes the worker pod.
// Acquires the retire semaphore to limit concurrent K8s API calls.
func (p *K8sWorkerPool) retireWorkerPod(id int, w *ManagedWorker) {
	p.retireSem <- struct{}{}
	defer func() { <-p.retireSem }()

	podName := p.workerPodName(w)
	slog.Info("Retiring K8s worker.", "id", id, "worker_pod", podName)
	if w.client != nil {
		_ = w.client.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: int64Ptr(10),
	}); err != nil {
		slog.Warn("Failed to delete worker pod.", "id", id, "worker_pod", podName, "error", err)
	}
	if err := p.deleteWorkerRPCSecret(ctx, podName); err != nil {
		slog.Warn("Failed to delete worker RPC secret.", "id", id, "worker_pod", podName, "error", err)
	}
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

	// Build the list of reap-eligible workers and sort by the first time
	// this CP saw their node — newest node first. We prefer evicting workers
	// on nodes we only just started using so older nodes keep their warm
	// NVMe parquet cache for longer. Workers with no known nodeName sort as
	// "new" (they're reaped first) to avoid stalling on stale state.
	type candidate struct {
		id     int
		w      *ManagedWorker
		seenAt time.Time
	}
	var candidates []candidate
	for id, w := range p.workers {
		if p.isWarmIdleWorkerLocked(w) && !w.lastUsed.IsZero() && now.Sub(w.lastUsed) > p.idleTimeout {
			candidates = append(candidates, candidate{id: id, w: w, seenAt: p.nodeSeenAtLocked(w.nodeName, now)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		// Newest-seen nodes reaped first.
		return candidates[i].seenAt.After(candidates[j].seenAt)
	})

	for _, c := range candidates {
		if idleCount <= p.minWorkers {
			break
		}
		{
			id, w := c.id, c.w
			if !p.markWorkerRetiredLocked(w, RetireReasonIdleTimeout, LifecycleOriginIdleTimeout) {
				continue
			}
			toRetire = append(toRetire, struct {
				id int
				w  *ManagedWorker
			}{id, w})
			delete(p.workers, id)
			p.pruneNodeFirstSeenLocked(w.nodeName)
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
			if !p.markWorkerRetiredLocked(w, RetireReasonStuckActivating, LifecycleOriginPoolStuckActivating) {
				continue
			}
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
			id := p.allocateBackgroundSpawnIDLocked()
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
			p.spawnWarmWorkerBackground(id, p.workerImage)
		}
	}
}

// --- Shared scheduling helpers (same logic as FlightWorkerPool) ---

// stampNodeFirstSeenLocked records now as the first-seen time for nodeName
// if we haven't seen it before. Caller must hold p.mu.
func (p *K8sWorkerPool) stampNodeFirstSeenLocked(nodeName string) {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return
	}
	if _, ok := p.nodeFirstSeen[nodeName]; !ok {
		p.nodeFirstSeen[nodeName] = time.Now()
	}
}

// nodeSeenAtLocked returns the first-seen time for nodeName. Missing or
// unknown entries return `fallback` (typically time.Now()) so callers can
// treat them as "new" for ranking — newer sorts later, so unknown nodes
// get reaped first and claimed last, which is the safe direction.
// Caller must hold p.mu.
func (p *K8sWorkerPool) nodeSeenAtLocked(nodeName string, fallback time.Time) time.Time {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return fallback
	}
	if t, ok := p.nodeFirstSeen[nodeName]; ok {
		return t
	}
	return fallback
}

// pruneNodeFirstSeenLocked drops nodeName from the first-seen map when no
// remaining worker references it, to keep the map bounded as nodes turn over.
// Caller must hold p.mu.
func (p *K8sWorkerPool) pruneNodeFirstSeenLocked(nodeName string) {
	if nodeName == "" || p.nodeFirstSeen == nil {
		return
	}
	for _, w := range p.workers {
		if w.nodeName == nodeName {
			return
		}
	}
	delete(p.nodeFirstSeen, nodeName)
}

func (p *K8sWorkerPool) findIdleWorkerLocked() *ManagedWorker {
	// Prefer the idle worker on the oldest-seen node so sessions land on
	// cache-warm NVMe rather than a cold new node. Workers with no known
	// nodeName sort as "new" (claimed last) to avoid stalling the claim
	// path on stale bookkeeping.
	now := time.Now()
	var best *ManagedWorker
	var bestSeenAt time.Time
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if !p.isWarmIdleWorkerLocked(w) {
			continue
		}
		seen := p.nodeSeenAtLocked(w.nodeName, now)
		if best == nil || seen.Before(bestSeenAt) {
			best = w
			bestSeenAt = seen
		}
	}
	return best
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

// liveExclusiveWorkerCountLocked counts only the live workers that consume the
// exclusive worker-count budget: every non-colocated worker plus in-flight
// background (default-shape) spawns. Colocated workers bin-pack and are
// intentionally unbounded, so they must not be charged against the cap — this
// mirrors the exclusive-only count used by the DB-side cap checks.
func (p *K8sWorkerPool) liveExclusiveWorkerCountLocked() int {
	count := p.spawning
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.profile.Colocate {
			continue
		}
		count++
	}
	return count
}

func (p *K8sWorkerPool) cleanDeadWorkersLocked() {
	var spawnIDs []int
	removedAny := false
	for id, w := range p.workers {
		select {
		case <-w.done:
			var removedWorker *ManagedWorker
			var replacementID int
			var shouldReplenish bool
			deletePod := true
			if p.runtimeStore != nil {
				lease := p.workerLeaseSnapshot(w)
				// cleanDeadWorkersLocked sweeps for pods whose informer
				// fired w.done — cluster-driven termination (eviction,
				// OOM, manual delete), not our health-check decision.
				lostDisposition, err := p.markWorkerLostForHealthLease(lease, LifecycleOriginInformerCrash)
				if err != nil {
					slog.Warn("Clean dead worker: lease validation failed; leaving cleanup to retry.",
						"id", id, "owner_cp_instance_id", lease.ownerCPInstanceID, "owner_epoch", lease.ownerEpoch, "error", err)
					continue
				}
				switch lostDisposition {
				case workerLostLeaseStale:
					deletePod = false
					removedWorker, _ = p.dropLocalWorkerIfSameLeaseLocked(lease)
				case workerLostLeaseCurrent, workerLostLeaseAlreadyLost, workerLostLeaseRetry:
					continue
				}
			} else {
				removedWorker, _, replacementID, shouldReplenish = p.removeWorkerLocked(id)
			}
			if removedWorker == nil {
				continue
			}
			removedAny = true
			if shouldReplenish {
				spawnIDs = append(spawnIDs, replacementID)
			}
			if removedWorker.client != nil {
				go func(c *flightsql.Client) { _ = c.Close() }(removedWorker.client)
			}
			if !deletePod {
				continue
			}
			// Delete the failed pod from K8s to avoid accumulating terminated pods
			go func(worker *ManagedWorker) {
				podName := p.workerPodName(worker)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, podName, metav1.DeleteOptions{
					GracePeriodSeconds: int64Ptr(0),
				})
			}(removedWorker)
		default:
		}
	}
	if removedAny {
		observeControlPlaneWorkers(len(p.workers))
		for _, id := range spawnIDs {
			go p.spawnWarmWorkerBackground(id, p.workerImage)
		}
	}
}

// workerResources returns resource requests and limits for a worker pod.
// Set via DUCKGRES_K8S_WORKER_CPU_REQUEST / DUCKGRES_K8S_WORKER_MEMORY_REQUEST.
// Returns empty (BestEffort) if neither is set.
// When set, limits are equal to requests (Guaranteed QoS).
func (p *K8sWorkerPool) workerResources() corev1.ResourceRequirements {
	return p.workerResourcesForProfile(WorkerProfile{})
}

// nodeSelectorForProfile returns the node selector a worker of the given profile
// should schedule onto: the colocated (bin-pack) selector for a colocated
// profile, otherwise the default pool selector.
func (p *K8sWorkerPool) nodeSelectorForProfile(profile WorkerProfile) map[string]string {
	if profile.Colocate {
		return p.colocatedWorkerNodeSelector
	}
	return p.workerNodeSelector
}

// workerResourcesForProfile builds the pod resource requirements for a worker of
// the given profile: the profile's CPU/Memory when set, otherwise the pool-global
// request (today's behavior). Requests are mirrored to Limits so the pod is
// Guaranteed QoS — required before bin-packing colocated workers. A colocated
// profile always carries non-empty CPU/Memory (resolver-enforced), so it never
// degrades to BestEffort.
func (p *K8sWorkerPool) workerResourcesForProfile(profile WorkerProfile) corev1.ResourceRequirements {
	cpuReq := profile.CPU
	if cpuReq == "" {
		cpuReq = p.workerCPURequest
	}
	memReq := profile.Memory
	if memReq == "" {
		memReq = p.workerMemoryRequest
	}
	requests := corev1.ResourceList{}
	if cpuReq != "" {
		requests[corev1.ResourceCPU] = resource.MustParse(cpuReq)
	}
	if memReq != "" {
		requests[corev1.ResourceMemory] = resource.MustParse(memReq)
	}
	if len(requests) == 0 {
		return corev1.ResourceRequirements{}
	}
	limits := make(corev1.ResourceList, len(requests))
	for k, v := range requests {
		limits[k] = v.DeepCopy()
	}
	return corev1.ResourceRequirements{Requests: requests, Limits: limits}
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

func (p *K8sWorkerPool) allocateBackgroundSpawnIDLocked() int {
	if p.runtimeStore != nil {
		return 0
	}
	return p.allocateWorkerIDLocked()
}

func (p *K8sWorkerPool) removeWorkerLocked(id int) (*ManagedWorker, int, int, bool) {
	w, ok := p.workers[id]
	if !ok {
		return nil, len(p.workers), 0, false
	}
	// Reached from cleanDeadWorkersLocked's no-runtime-store fallback
	// (informer fired w.done). Not the periodic health-check path —
	// that goes through markWorkerLostForHealthLease above and never
	// calls into here.
	if !p.markWorkerRetiredLocked(w, RetireReasonCrash, LifecycleOriginInformerCrash) {
		return nil, len(p.workers), 0, false
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	if !p.shouldReplenishWarmCapacityLocked() {
		return w, workerCount, 0, false
	}
	replacementID := p.allocateBackgroundSpawnIDLocked()
	p.spawning++
	return w, workerCount, replacementID, true
}

type workerLeaseSnapshot struct {
	workerID          int
	ownerCPInstanceID string
	ownerEpoch        int64
	image             string
}

func (p *K8sWorkerPool) workerLeaseSnapshot(w *ManagedWorker) workerLeaseSnapshot {
	ownerCPInstanceID := w.OwnerCPInstanceID()
	if ownerCPInstanceID == "" {
		ownerCPInstanceID = p.cpInstanceID
	}
	return workerLeaseSnapshot{
		workerID:          w.ID,
		ownerCPInstanceID: ownerCPInstanceID,
		ownerEpoch:        w.OwnerEpoch(),
		image:             w.image,
	}
}

func (p *K8sWorkerPool) workerMatchesLease(w *ManagedWorker, lease workerLeaseSnapshot) bool {
	return w != nil && p.workerLeaseSnapshot(w) == lease
}

type workerLostLeaseDisposition int

const (
	workerLostLeaseCurrent workerLostLeaseDisposition = iota
	workerLostLeaseStale
	workerLostLeaseRetry
	workerLostLeaseAlreadyLost
)

func (p *K8sWorkerPool) markWorkerLostForHealthLease(lease workerLeaseSnapshot, origin LifecycleOrigin) (workerLostLeaseDisposition, error) {
	if p.runtimeStore == nil {
		return workerLostLeaseCurrent, nil
	}
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return workerLostLeaseStale, nil
	}
	currentLease, err := p.markWorkerLostIfCurrentLease(lease, origin)
	if err != nil {
		return workerLostLeaseRetry, err
	}
	if currentLease {
		return workerLostLeaseCurrent, nil
	}

	record, err := p.runtimeStore.GetWorkerRecord(lease.workerID)
	if err != nil {
		return workerLostLeaseRetry, err
	}
	if record == nil || record.OwnerCPInstanceID != p.cpInstanceID {
		return workerLostLeaseStale, nil
	}
	if record.OwnerEpoch != lease.ownerEpoch {
		return workerLostLeaseRetry, nil
	}
	if record.State == configstore.WorkerStateLost && record.RetireReason == RetireReasonCrash {
		return workerLostLeaseAlreadyLost, nil
	}
	return workerLostLeaseStale, nil
}

func (p *K8sWorkerPool) markWorkerLostIfCurrentLease(lease workerLeaseSnapshot, origin LifecycleOrigin) (bool, error) {
	lc := p.ensureLifecycle()
	if lc == nil {
		// No durable store wired (process backend / minimal test pool).
		// The health-check caller treats true here as "CAS landed";
		// for the no-store case we let it proceed to its in-memory
		// cleanup path, which is the same behavior the old direct-
		// store call had when runtimeStore was nil.
		return true, nil
	}
	if lease.ownerCPInstanceID != p.cpInstanceID {
		return false, nil
	}
	outcome, err := lc.MarkLostFromLease(
		configstore.NewWorkerLease(lease.workerID, p.cpInstanceID, lease.ownerEpoch, lease.image),
		RetireReasonCrash,
		origin,
	)
	if err != nil {
		return false, err
	}
	return outcome.Transitioned, nil
}

func (p *K8sWorkerPool) removeWorkerAfterLostLeaseLocked(lease workerLeaseSnapshot) (*ManagedWorker, int, int, bool) {
	current, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(current, lease) {
		return nil, len(p.workers), 0, false
	}
	p.markWorkerRetiredInMemoryLocked(current)
	delete(p.workers, current.ID)
	workerCount := len(p.workers)
	if !p.shouldReplenishWarmCapacityLocked() {
		return current, workerCount, 0, false
	}
	replacementID := p.allocateBackgroundSpawnIDLocked()
	p.spawning++
	return current, workerCount, replacementID, true
}

func (p *K8sWorkerPool) dropLocalWorkerIfSameLeaseLocked(lease workerLeaseSnapshot) (*ManagedWorker, int) {
	current, ok := p.workers[lease.workerID]
	if !ok || !p.workerMatchesLease(current, lease) {
		return nil, len(p.workers)
	}
	delete(p.workers, current.ID)
	return current, len(p.workers)
}

func (p *K8sWorkerPool) isGenericSessionSchedulableWorkerLocked(w *ManagedWorker) bool {
	return w.SharedState().NormalizedLifecycle() == WorkerLifecycleIdle
}

func (p *K8sWorkerPool) isWarmIdleWorkerLocked(w *ManagedWorker) bool {
	return w.activeSessions == 0 && p.isGenericSessionSchedulableWorkerLocked(w)
}

// findReservableWarmWorkerLocked returns a warm-idle worker that is safe to
// reserve. When image is non-empty, only workers whose image matches are
// considered — used by ReserveSharedWorker so per-org image pins aren't
// silently bypassed when the in-memory map and runtime store disagree.
// Pass "" to skip the image filter (legacy non-pinned callers). The profile
// filter always applies: a request only reuses a warm worker of the same shape
// (nil profile == the default/zero shape, which is what warm workers carry).
func (p *K8sWorkerPool) findReservableWarmWorkerLocked(image string, profile *WorkerProfile) *ManagedWorker {
	want := profile.MatchKey()
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if image != "" && w.image != image {
			continue
		}
		if w.profile.MatchKey() != want {
			continue
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

// idleWarmWorkerCountByImageLocked returns warm-idle counts bucketed by the
// image each worker was spawned with. Used by the per-image warm floor to
// decide whether to spawn a new worker of a specific pinned image.
func (p *K8sWorkerPool) idleWarmWorkerCountByImageLocked() map[string]int {
	// Only DEFAULT-shape workers count toward the per-image warm target. Colocated
	// workers share p.workerImage but belong to their own warm pool; counting them
	// here would starve the exclusive per-image pool. Mirrors the SQL filter in
	// countNeutralWarmWorkersForImage.
	defaultKey := (&WorkerProfile{}).MatchKey()
	counts := make(map[string]int)
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.profile.MatchKey() != defaultKey {
			continue
		}
		if p.isWarmIdleWorkerLocked(w) {
			counts[w.image]++
		}
	}
	return counts
}

func (p *K8sWorkerPool) shouldReplenishWarmCapacityLocked() bool {
	if p.runtimeStore != nil {
		return false
	}
	if p.minWorkers <= 0 {
		return false
	}
	if p.idleWarmWorkerCountLocked() >= p.minWorkers {
		return false
	}
	liveCount := p.liveWorkerCountLocked()
	return p.maxWorkers == 0 || liveCount < p.maxWorkers
}

func (p *K8sWorkerPool) spawnWarmWorker(ctx context.Context, id int, image string, profile WorkerProfile) error {
	if id <= 0 && p.runtimeStore != nil {
		if image == "" {
			image = p.workerImage
		}
		slot, err := p.runtimeStore.CreateNeutralWarmWorkerSlot(
			p.cpInstanceID,
			p.workerPodNamePrefix(),
			image,
			profile.CPU,
			profile.Memory,
			profile.Colocate,
			p.minWorkers,
			p.maxWorkers,
		)
		if err != nil {
			observeSpawnFailure(SpawnFailureReasonRuntimeStore, image)
			return err
		}
		if slot == nil {
			return nil
		}
		id = slot.WorkerID
		image = slot.Image
		profile = WorkerProfile{CPU: slot.ProfileCPU, Memory: slot.ProfileMemory, Colocate: slot.ProfileColocate}
	}
	if p.spawnWarmWorkerFunc != nil {
		return p.spawnWarmWorkerFunc(ctx, id)
	}
	return p.spawnWorker(ctx, id, image, profile, true)
}

func (p *K8sWorkerPool) spawnWarmWorkerBackground(id int, image string) {
	if p.spawnWarmWorkerBackgroundFunc != nil {
		p.spawnWarmWorkerBackgroundFunc(id)
		return
	}
	go p.spawnWorkerBackground(id, image)
}

// markWorkerRetiredLocked is the in-memory retire wrapper for paths
// that bypass the WorkerLifecycle CAS service: it upserts the durable
// row first, then (on success) transitions the in-memory shared state
// and emits a duckgres_worker_lifecycle_transitions_total sample under
// operation=retire_local with the caller-supplied origin. Used by
// retireWorkerWithReason (idle reaper, stuck-activating reaper, public
// RetireWorker, activation-failure / liveness-recheck fallbacks,
// per-org ShutdownAll). Callers that have already gone through the
// lifecycle service (deleteRetiredRuntimeWorker, ShutdownAll's drain
// chain, removeWorkerAfterLostLeaseLocked) call
// markWorkerRetiredInMemoryLocked directly to avoid double-counting.
//
// origin is required so the metric label reflects the actual call site
// (e.g. pool-local reaper vs. janitor reaper, per-org vs. CP-wide
// shutdown) — the prior reason→origin mapping was lossy because the
// same RetireReason* constant is reused across distinct call sites.
//
// Ordering: persist BEFORE the in-memory transition so a fence-miss
// (peer CP advanced the lease) doesn't leave this CP with a stale
// in-memory view that pretends we still own the worker. On any
// persistWorkerRecord error (fence-miss or real DB error), the
// in-memory state is left untouched, a retire_local failure outcome is
// emitted, and callers must skip local removal plus pod deletion.
func (p *K8sWorkerPool) markWorkerRetiredLocked(w *ManagedWorker, reason string, origin LifecycleOrigin) bool {
	workerState := configstore.WorkerStateRetired
	if reason == RetireReasonCrash {
		workerState = configstore.WorkerStateLost
	}
	if err := p.persistWorkerRecord(p.workerRecordFor(w.ID, w, w.OwnerEpoch(), workerState, reason, nil)); err != nil {
		outcome := configstore.TransitionOutcomeStoreError
		if stderrors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
			outcome = configstore.TransitionOutcomeFenceMissLease
		}
		observeLifecycleTransition(
			LifecycleOpRetireLocal,
			outcome,
			w.image,
			origin,
		)
		return false
	}
	// markWorkerRetiredInMemoryLocked returns false only when the
	// SharedState machine refuses Retired (e.g., already terminal).
	// Under p.mu that's unreachable in practice — the worker is in
	// p.workers and no one else can have flipped the state — but we
	// don't gate the return on it for two reasons:
	// (1) the durable retire HAS landed (persist succeeded above), so
	//     "transitioned" is honest at the lifecycle level even if the
	//     local mirror was a no-op;
	// (2) returning false would strand the now-durably-retired worker
	//     in p.workers (caller skips delete()) with the K8s pod still
	//     up — the orphan reconciler would eventually delete the pod
	//     but never the in-memory entry, breaking local capacity
	//     accounting.
	// The discarded bool stays for diagnostic value if the state
	// machine ever surfaces a real refusal.
	_ = p.markWorkerRetiredInMemoryLocked(w)
	observeLifecycleTransition(
		LifecycleOpRetireLocal,
		configstore.TransitionOutcomeTransitioned,
		w.image,
		origin,
	)
	return true
}

// markWorkerRetiredInMemoryLocked performs only the in-memory lifecycle
// transition for a worker retirement, without persisting to the runtime
// store and without emitting any metric. Used by callers that have
// already advanced the DB state via a scoped CAS (e.g. ShutdownAll's
// draining chain, the lifecycle service's post-CAS cleanup hook,
// removeWorkerAfterLostLease) and don't want an unconditional
// UpsertWorkerRecord to overwrite fields set by that CAS. Returns true
// when the transition actually happened.
//
// Metric emission lives in markWorkerRetiredLocked (the wrapper that
// also persists) because the CAS-bypassing path is the only one not
// already observed via the lifecycle service.
func (p *K8sWorkerPool) markWorkerRetiredInMemoryLocked(w *ManagedWorker) bool {
	nextState, err := w.SharedState().Transition(WorkerLifecycleRetired, nil)
	if err != nil {
		return false
	}
	_ = w.SetSharedState(nextState)
	return true
}

// persistWorkerRecord upserts the record and returns the underlying
// error (including ErrWorkerRecordUpsertFenceMiss when the CP no longer
// owns the lease). Callers that don't care about the result discard it
// with `_ =`; markWorkerRetiredLocked uses it to gate metric emission
// so retire_local samples reflect transitions that actually persisted.
func (p *K8sWorkerPool) persistWorkerRecord(record *configstore.WorkerRecord) error {
	if p.runtimeStore == nil || record == nil {
		return nil
	}
	err := p.runtimeStore.UpsertWorkerRecord(record)
	if err == nil {
		return nil
	}
	// Fence misses are expected when a peer CP has already advanced the
	// worker's lease (terminal state, newer owner_epoch, or different
	// owner at the same epoch). They prove the fence is doing its job —
	// log at Debug so they don't masquerade as persistence failures.
	if stderrors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
		slog.Debug("Worker runtime upsert fenced by newer lease.", "worker_id", record.WorkerID, "state", record.State, "error", err)
		return err
	}
	slog.Warn("Persisting worker runtime record failed.", "worker_id", record.WorkerID, "state", record.State, "error", err)
	return err
}

func (p *K8sWorkerPool) workerRecordFor(id int, worker *ManagedWorker, ownerEpoch int64, state configstore.WorkerState, retireReason string, activationStartedAt *time.Time) *configstore.WorkerRecord {
	record := &configstore.WorkerRecord{
		WorkerID:          id,
		PodName:           p.podNameForWorker(id),
		Image:             p.workerImage,
		State:             state,
		OwnerCPInstanceID: p.cpInstanceID,
		OwnerEpoch:        ownerEpoch,
		LastHeartbeatAt:   time.Now(),
		RetireReason:      retireReason,
	}
	if activationStartedAt != nil {
		startedAt := *activationStartedAt
		record.ActivationStartedAt = &startedAt
	}
	if worker == nil {
		// OwnerCPInstanceID is already this CP's id from the struct literal
		// above. Stamping warm/idle workers with the creating CP keeps
		// last_heartbeat_at fresh via the CP heartbeat — without it, the
		// orphan reconciler matches case (2) (NULLIF(owner_cp_instance_id,
		// '') IS NULL AND last_heartbeat_at <= before) the moment the row
		// crosses orphanGrace, so warm workers get reaped on a ~30s loop.
		return record
	}
	record.PodName = p.workerPodName(worker)
	if owner := worker.OwnerCPInstanceID(); owner != "" {
		record.OwnerCPInstanceID = owner
	}
	if worker.image != "" {
		record.Image = worker.image
	}
	record.ProfileCPU = worker.profile.CPU
	record.ProfileMemory = worker.profile.Memory
	record.ProfileColocate = worker.profile.Colocate
	if assignment := worker.SharedState().Assignment; assignment != nil {
		record.OrgID = assignment.OrgID
	}
	if state == configstore.WorkerStateIdle {
		record.OrgID = ""
	}
	return record
}

func (p *K8sWorkerPool) healthCheckPayloadForWorker(worker *ManagedWorker) server.WorkerHealthCheckPayload {
	return p.healthCheckPayloadForLease(p.workerLeaseSnapshot(worker))
}

func (p *K8sWorkerPool) healthCheckPayloadForLease(lease workerLeaseSnapshot) server.WorkerHealthCheckPayload {
	payload := server.WorkerHealthCheckPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			WorkerID:     lease.workerID,
			OwnerEpoch:   lease.ownerEpoch,
			CPInstanceID: lease.ownerCPInstanceID,
		},
	}
	return payload
}

// podNameForWorker returns the pod name for a given worker ID,
// including the org ID if set (multi-tenant mode).
func (p *K8sWorkerPool) podNameForWorker(id int) string {
	return fmt.Sprintf("%s-%d", p.workerPodNamePrefix(), id)
}

func (p *K8sWorkerPool) workerPodName(worker *ManagedWorker) string {
	if worker != nil && worker.PodName() != "" {
		return worker.PodName()
	}
	if worker == nil {
		return ""
	}
	return p.podNameForWorker(worker.ID)
}

func (p *K8sWorkerPool) workerPodNamePrefix() string {
	base := trimK8sPodHashSuffix(p.cpID)
	if p.orgID != "" {
		return fmt.Sprintf("%s-worker-%s", base, p.orgID)
	}
	return fmt.Sprintf("%s-worker", base)
}

// trimK8sPodHashSuffix removes the trailing 5-character random pod-hash
// segment from a K8s deployment pod name (e.g. "duckgres-7b667c7bfd-7745x"
// → "duckgres-7b667c7bfd"). Names that don't end in a plausible pod-hash
// segment are returned unchanged.
func trimK8sPodHashSuffix(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx <= 0 {
		return name
	}
	suffix := name[idx+1:]
	if len(suffix) != 5 {
		return name
	}
	for _, r := range suffix {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return name
		}
	}
	return name[:idx]
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

func (p *K8sWorkerPool) WarmCapacityTarget() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.minWorkers
}

// SetPerImageWarmTargets replaces the per-image warm-worker floor. Each entry
// asks the pool to keep at least N warm-idle workers running with the given
// image. This is layered on top of SetWarmCapacityTarget — the per-image floor
// guarantees coverage for pinned org images that wouldn't otherwise be served
// by the cluster-default warm pool. Pass an empty map to disable.
func (p *K8sWorkerPool) SetPerImageWarmTargets(targets map[string]int) {
	clean := make(map[string]int, len(targets))
	for image, n := range targets {
		if image == "" || n <= 0 {
			continue
		}
		clean[image] = n
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.perImageWarmTarget = clean
}

// PerImageWarmTargets returns a snapshot of the per-image warm floor.
func (p *K8sWorkerPool) PerImageWarmTargets() map[string]int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]int, len(p.perImageWarmTarget))
	for k, v := range p.perImageWarmTarget {
		out[k] = v
	}
	return out
}

func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }

// Compile-time interface check.
var _ WorkerPool = (*K8sWorkerPool)(nil)
