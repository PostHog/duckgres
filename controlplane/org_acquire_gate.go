//go:build kubernetes

package controlplane

import (
	"context"
	"sync"
)

// orgAcquireGate is a cancellable FIFO turnstile. It serializes the slow
// worker-acquisition path (no idle worker → claim/spawn) for one org so that a
// newly-spawned or freed worker is handed to the EARLIEST waiting connection,
// and a later-arriving connection cannot snatch it. Plain sync.Mutex is
// unsuitable: it is not strictly FIFO and a goroutine blocked in Lock() cannot
// abort when its request context is cancelled (client disconnect / deadline).
//
// Holders must call release() exactly once (defer) after acquire() returns nil.
type orgAcquireGate struct {
	mu    sync.Mutex
	held  bool
	queue []*gateWaiter
}

type gateWaiter struct {
	ready    chan struct{} // closed when the gate is granted to this waiter
	canceled bool          // set under mu when the waiter abandoned before grant
}

func newOrgAcquireGate() *orgAcquireGate { return &orgAcquireGate{} }

// acquire blocks until this caller owns the gate (FIFO) or ctx is done. On a nil
// return the caller owns the gate and MUST call release().
func (g *orgAcquireGate) acquire(ctx context.Context) error {
	g.mu.Lock()
	if !g.held {
		g.held = true
		g.mu.Unlock()
		return nil
	}
	w := &gateWaiter{ready: make(chan struct{})}
	g.queue = append(g.queue, w)
	g.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		select {
		case <-w.ready:
			// Granted concurrently with cancellation: we now own the gate, so we
			// must pass it on rather than leak it.
			g.mu.Unlock()
			g.release()
		default:
			w.canceled = true
			g.mu.Unlock()
		}
		return ctx.Err()
	}
}

// release hands the gate to the next live waiter (FIFO), or marks it free.
func (g *orgAcquireGate) release() {
	g.mu.Lock()
	for len(g.queue) > 0 {
		w := g.queue[0]
		g.queue = g.queue[1:]
		if w.canceled {
			continue // waiter gave up; skip it
		}
		close(w.ready) // grant; held stays true (ownership transfers)
		g.mu.Unlock()
		return
	}
	g.held = false
	g.mu.Unlock()
}
