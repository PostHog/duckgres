package controlplane

import (
	"context"
	"testing"
	"time"
)

func TestLeaderOnlyLoopStartsAndStopsBackgroundRun(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	loop := newLeaderOnlyLoop(func(ctx context.Context) {
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
	})

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loop.onStartedLeading(rootCtx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader loop did not start")
	}

	loop.onStoppedLeading()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("leader loop did not stop after leadership loss")
	}
}
