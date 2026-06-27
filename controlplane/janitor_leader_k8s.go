//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const defaultJanitorLeaseName = "duckgres-janitor"

type leaderElectorRunner interface {
	Run(context.Context)
}

// defaultElectorRecontendBackoff is the pause between losing the janitor lease
// and re-contending for it. Short enough that a lost leader rejoins the
// election promptly (so reaping is never long without a leader), long enough
// that a persistently failing Run() doesn't hot-loop the API server.
const defaultElectorRecontendBackoff = 2 * time.Second

type JanitorLeaderManager struct {
	elector          leaderElectorRunner
	leaderLoop       *leaderOnlyLoop
	recontendBackoff time.Duration
	mu               sync.Mutex
	cancel           context.CancelFunc
}

func NewJanitorLeaderManager(namespace, identity string, janitor *ControlPlaneJanitor) (*JanitorLeaderManager, error) {
	if janitor == nil {
		return nil, nil
	}
	if namespace == "" {
		return nil, fmt.Errorf("leader election namespace is required")
	}
	if identity == "" {
		hostname, _ := os.Hostname()
		identity = hostname
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config for leader election: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client for leader election: %w", err)
	}

	return newJanitorLeaderManagerFromClients(
		namespace,
		defaultJanitorLeaseName,
		identity,
		clientset.CoreV1(),
		clientset.CoordinationV1(),
		janitor,
	)
}

func newJanitorLeaderManagerFromClients(
	namespace, leaseName, identity string,
	coreClient corev1client.CoreV1Interface,
	coordClient coordinationv1client.CoordinationV1Interface,
	janitor *ControlPlaneJanitor,
) (*JanitorLeaderManager, error) {
	lock, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		namespace,
		leaseName,
		coreClient,
		coordClient,
		resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create leader-election lease lock: %w", err)
	}

	leaderLoop := newLeaderOnlyLoop(janitor.Run)
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   20 * time.Second,
		RenewDeadline:   15 * time.Second,
		RetryPeriod:     5 * time.Second,
		ReleaseOnCancel: true,
		Name:            "duckgres-janitor",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				setJanitorIsLeader(true)
				leaderLoop.onStartedLeading(ctx)
			},
			OnStoppedLeading: func() {
				setJanitorIsLeader(false)
				leaderLoop.onStoppedLeading()
				slog.Info("Lost janitor leadership.")
			},
			OnNewLeader: func(current string) {
				slog.Debug("Janitor leader observed.", "identity", current)
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create leader elector: %w", err)
	}

	return &JanitorLeaderManager{
		elector:          elector,
		leaderLoop:       leaderLoop,
		recontendBackoff: defaultElectorRecontendBackoff,
	}, nil
}

func (m *JanitorLeaderManager) Start(ctx context.Context) error {
	if m == nil || m.elector == nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	m.cancel = cancel
	backoff := m.recontendBackoff
	m.mu.Unlock()
	if backoff <= 0 {
		backoff = defaultElectorRecontendBackoff
	}

	// client-go's LeaderElector.Run returns as soon as this CP loses (or fails
	// to renew) the lease — it does NOT re-contend on its own. Running it only
	// once (the prior behavior) meant a single transient renewal failure
	// (API-server blip > RenewDeadline, GC pause, network hiccup) permanently
	// dropped this replica out of the election. Because every fleet-wide
	// reclamation pass (hot-idle TTL reap, orphan/stuck sweeps, stranded-pod
	// cleanup, headroom scale-down) is leader-gated, enough leadership churn
	// would leave the lease stale with no contender and the reaper dark
	// fleet-wide. Re-contend in a loop so a lost leader always rejoins; the
	// elector is safe to Run again after it returns. The loop exits only when
	// runCtx is cancelled (Stop or parent shutdown).
	go func() {
		for {
			m.elector.Run(runCtx)
			// Run returned: either runCtx was cancelled (Stop/shutdown → exit)
			// or we lost the lease. Re-contend after a short backoff. Checking
			// the error before the backoff means a cancelled ctx exits promptly
			// without an extra Run call.
			if runCtx.Err() != nil {
				return
			}
			select {
			case <-runCtx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}()
	return nil
}

func (m *JanitorLeaderManager) Stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if m.leaderLoop == nil {
		return
	}
	m.leaderLoop.onStoppedLeading()
}
