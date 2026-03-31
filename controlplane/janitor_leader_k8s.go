//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const defaultJanitorLeaseName = "duckgres-janitor"

type leaderElectorRunner interface {
	Run(context.Context)
}

type JanitorLeaderManager struct {
	elector    leaderElectorRunner
	leaderLoop *leaderOnlyLoop
	mu         sync.Mutex
	cancel     context.CancelFunc
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
		Lock:          lock,
		LeaseDuration: 20 * time.Second,
		RenewDeadline: 15 * time.Second,
		RetryPeriod:   5 * time.Second,
		ReleaseOnCancel: true,
		Name:          "duckgres-janitor",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: leaderLoop.onStartedLeading,
			OnStoppedLeading: func() {
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
		elector:    elector,
		leaderLoop: leaderLoop,
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
	m.mu.Unlock()
	go m.elector.Run(runCtx)
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
