//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"
)

type captureLeaderElector struct {
	ctxDone chan struct{}
}

func (e *captureLeaderElector) Run(ctx context.Context) {
	<-ctx.Done()
	close(e.ctxDone)
}

func TestJanitorLeaderManagerStopCancelsLeaderElection(t *testing.T) {
	elector := &captureLeaderElector{ctxDone: make(chan struct{})}
	manager := &JanitorLeaderManager{
		elector:    elector,
		leaderLoop: newLeaderOnlyLoop(func(context.Context) {}),
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	manager.Stop()

	select {
	case <-elector.ctxDone:
	case <-time.After(time.Second):
		t.Fatal("expected Stop to cancel leader-election context")
	}
}
