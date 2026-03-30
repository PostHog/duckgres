package controlplane

import (
	"context"
	"sync"
)

type leaderOnlyLoop struct {
	run func(context.Context)

	mu     sync.Mutex
	cancel context.CancelFunc
}

func newLeaderOnlyLoop(run func(context.Context)) *leaderOnlyLoop {
	return &leaderOnlyLoop{run: run}
}

func (l *leaderOnlyLoop) onStartedLeading(ctx context.Context) {
	if l == nil || l.run == nil {
		return
	}

	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	leaderCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.mu.Unlock()

	go l.run(leaderCtx)
}

func (l *leaderOnlyLoop) onStoppedLeading() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel := l.cancel
	l.cancel = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
