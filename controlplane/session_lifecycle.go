package controlplane

import (
	"context"
	"sync"
)

type sessionLifecycle struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	nextID  int64
	active  int
	cancels map[int64]context.CancelFunc
}

func newSessionLifecycle() *sessionLifecycle {
	l := &sessionLifecycle{
		cancels: make(map[int64]context.CancelFunc),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *sessionLifecycle) initializeLocked() {
	if l.cond == nil {
		l.cond = sync.NewCond(&l.mu)
	}
	if l.cancels == nil {
		l.cancels = make(map[int64]context.CancelFunc)
	}
}

// begin registers one lifecycle operation. finish atomically unregisters it
// and reports whether close linearized first. The result remains stable across
// repeated finish calls.
func (l *sessionLifecycle) begin(ctx context.Context) (context.Context, func() bool, error) {
	l.mu.Lock()
	l.initializeLocked()
	if l.closed {
		l.mu.Unlock()
		return nil, nil, ErrSessionManagerDraining
	}
	ctx, cancel := context.WithCancel(ctx)
	l.nextID++
	id := l.nextID
	l.cancels[id] = cancel
	l.active++
	l.mu.Unlock()

	var once sync.Once
	closed := false
	finish := func() bool {
		once.Do(func() {
			l.mu.Lock()
			closed = l.closed
			delete(l.cancels, id)
			l.active--
			l.cond.Broadcast()
			l.mu.Unlock()
			cancel()
		})
		return closed
	}
	return ctx, finish, nil
}

func (l *sessionLifecycle) beginCleanup() func() {
	l.mu.Lock()
	l.initializeLocked()
	l.active++
	l.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.active--
			l.cond.Broadcast()
			l.mu.Unlock()
		})
	}
}

func (l *sessionLifecycle) close() {
	l.mu.Lock()
	l.initializeLocked()
	l.closed = true
	cancels := make([]context.CancelFunc, 0, len(l.cancels))
	for _, cancel := range l.cancels {
		cancels = append(cancels, cancel)
	}
	l.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (l *sessionLifecycle) closeAndWait() {
	l.close()
	l.mu.Lock()
	l.initializeLocked()
	for l.active > 0 {
		l.cond.Wait()
	}
	l.mu.Unlock()
}

func (l *sessionLifecycle) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.initializeLocked()
	return l.closed
}
