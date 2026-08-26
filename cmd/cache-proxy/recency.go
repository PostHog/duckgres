package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultRecencyGranularity   = time.Minute
	defaultRecencyQueueCapacity = 65_536
	defaultRecencyWorkerCount   = 1
)

var (
	cacheRecencyTouchAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_recency_touch_attempts_total",
		Help: "Cache accesses considered for durable coarse-recency persistence",
	})
	cacheRecencyTouchSuccessesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_recency_touch_successes_total",
		Help: "Coarse cache-recency timestamps successfully persisted to disk",
	})
	cacheRecencyTouchFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_recency_touch_failures_total",
		Help: "Coarse cache-recency timestamps that failed to persist",
	})
	cacheRecencyTouchCoalescedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_recency_touch_coalesced_total",
		Help: "Cache accesses coalesced with an existing or same-bucket durable-recency update",
	})
	cacheRecencyTouchDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_recency_touch_dropped_total",
		Help: "Durable-recency updates dropped because the bounded queue was full or closed",
	})
	cacheRecencyQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_recency_queue_depth",
		Help: "Unique cache keys with queued or in-flight durable-recency work",
	})
	cacheRecencyLastSuccessTimestampSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_recency_last_successful_persistence_timestamp_seconds",
		Help: "Unix timestamp of the last successful durable-recency metadata update",
	})
)

type recencyPersistence func(string, time.Time, time.Time) error

type recencyState struct {
	latest time.Time
}

// recencyWriter persists coarse access timestamps without ever making request
// handlers wait for filesystem metadata I/O. The pending map is bounded by the
// queue capacity and provides one coalescing slot per opaque cache key.
type recencyWriter struct {
	dir     string
	persist recencyPersistence

	mu         sync.Mutex
	pending    map[string]*recencyState
	jobs       chan string
	maxPending int
	closed     bool
	done       chan struct{}
	wg         sync.WaitGroup
}

func newRecencyWriter(dir string, capacity, workers int, persist recencyPersistence) *recencyWriter {
	if capacity <= 0 {
		capacity = defaultRecencyQueueCapacity
	}
	if workers <= 0 {
		workers = defaultRecencyWorkerCount
	}
	if persist == nil {
		persist = os.Chtimes
	}
	w := &recencyWriter{
		dir:        dir,
		persist:    persist,
		pending:    make(map[string]*recencyState, capacity),
		jobs:       make(chan string, capacity),
		maxPending: capacity + workers,
		done:       make(chan struct{}),
	}
	w.wg.Add(workers)
	for range workers {
		go w.run()
	}
	go func() {
		w.wg.Wait()
		close(w.done)
	}()
	return w
}

// submit is deliberately nonblocking. false means the bounded durability
// budget was exhausted; callers keep their exact in-memory LRU update.
func (w *recencyWriter) submit(key string, timestamp time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		cacheRecencyTouchDroppedTotal.Inc()
		return false
	}
	if state, ok := w.pending[key]; ok {
		if timestamp.After(state.latest) {
			state.latest = timestamp
		}
		cacheRecencyTouchCoalescedTotal.Inc()
		return true
	}
	if len(w.pending) >= w.maxPending {
		cacheRecencyTouchDroppedTotal.Inc()
		return false
	}
	w.pending[key] = &recencyState{latest: timestamp}
	select {
	case w.jobs <- key:
		cacheRecencyQueueDepth.Set(float64(len(w.pending)))
		return true
	default:
		delete(w.pending, key)
		cacheRecencyQueueDepth.Set(float64(len(w.pending)))
		cacheRecencyTouchDroppedTotal.Inc()
		return false
	}
}

func (w *recencyWriter) run() {
	defer w.wg.Done()
	for key := range w.jobs {
		w.persistKey(key)
	}
}

func (w *recencyWriter) persistKey(key string) {
	for {
		w.mu.Lock()
		state, ok := w.pending[key]
		if !ok {
			w.mu.Unlock()
			return
		}
		timestamp := state.latest
		w.mu.Unlock()

		err := w.persist(cachePath(w.dir, key), timestamp, timestamp)
		switch {
		case err == nil:
			cacheRecencyTouchSuccessesTotal.Inc()
			cacheRecencyLastSuccessTimestampSeconds.Set(float64(time.Now().Unix()))
		case errors.Is(err, os.ErrNotExist):
			// Eviction may race accepted metadata work. The missing entry is
			// already absent and is not a persistence failure.
		default:
			cacheRecencyTouchFailuresTotal.Inc()
			slog.Warn("Unable to persist cache recency.", "key", key, "error", err)
		}

		w.mu.Lock()
		state, ok = w.pending[key]
		if ok && state.latest.After(timestamp) {
			w.mu.Unlock()
			continue
		}
		delete(w.pending, key)
		cacheRecencyQueueDepth.Set(float64(len(w.pending)))
		w.mu.Unlock()
		return
	}
}

func (w *recencyWriter) beginClose() <-chan struct{} {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.jobs)
	}
	done := w.done
	w.mu.Unlock()
	return done
}

func (w *recencyWriter) waitIdle(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		w.mu.Lock()
		idle := len(w.pending) == 0
		w.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func cachePath(dir, key string) string {
	// key has already passed IsValidCacheKey before it enters the index.
	return dir + string(os.PathSeparator) + key
}
