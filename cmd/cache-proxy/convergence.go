package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cacheConvergenceActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_convergence_active",
		Help: "Whether the cache is above its active soft entry or byte target",
	})
	cacheConvergenceExcessEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_convergence_excess_entries",
		Help: "Exact-index entries above the active soft target",
	})
	cacheConvergenceExcessBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_convergence_excess_bytes",
		Help: "Committed cache bytes above the active byte target",
	})
	cacheConvergenceEvictionAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_convergence_eviction_attempts_total",
		Help: "Rate-limited background convergence eviction attempts",
	})
	cacheConvergenceEvictionFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_convergence_eviction_failures_total",
		Help: "Rate-limited background convergence eviction attempts that failed",
	})
)

func (c *DiskCache) startConvergence(parent context.Context, permits <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		interval = defaultConvergenceInterval
	}
	ctx, cancel := context.WithCancel(parent)
	c.convergenceCancel = cancel
	c.convergenceDone = make(chan struct{})
	c.convergenceWake = make(chan struct{}, 1)
	go func() {
		defer close(c.convergenceDone)
		if permits != nil {
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-permits:
					if !ok {
						return
					}
					c.convergeOne()
				}
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.convergenceWake:
			}
			for {
				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				stillOver, succeeded := c.convergeOne()
				if !stillOver {
					break
				}
				if !succeeded {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
				}
			}
		}
	}()
	c.mu.Lock()
	c.updateConvergenceMetricsLocked()
	c.mu.Unlock()
}

func (c *DiskCache) convergeOne() (bool, bool) {
	c.mu.Lock()
	reason, over := c.overLimitReasonLocked()
	if !over {
		c.updateConvergenceMetricsLocked()
		c.mu.Unlock()
		return false, true
	}
	victimElement := c.oldestEvictableLocked()
	if victimElement == nil {
		c.updateConvergenceMetricsLocked()
		c.mu.Unlock()
		return true, false
	}
	victim := victimElement.Value.(*cacheEntry)
	victim.evictionInFlight = true
	victimPath := filepath.Join(c.dir, victim.key)
	cacheConvergenceEvictionAttemptsTotal.Inc()
	c.mu.Unlock()

	_, removeErr := c.removeCommittedFile(victimPath, cacheEvictionPhaseBackground, reason)

	c.mu.Lock()
	currentElement, stillIndexed := c.index[victim.key]
	sameEntry := stillIndexed && currentElement == victimElement
	succeeded := removeErr == nil && sameEntry
	switch {
	case removeErr != nil:
		if sameEntry {
			victim.evictionInFlight = false
		}
		cacheConvergenceEvictionFailuresTotal.Inc()
	case !sameEntry:
		cacheConvergenceEvictionFailuresTotal.Inc()
		slog.Error("Cache convergence victim changed during deletion.", "key", victim.key)
	default:
		c.forgetEntryLocked(victimElement)
	}
	c.updateConvergenceMetricsLocked()
	_, stillOver := c.overLimitReasonLocked()
	c.mu.Unlock()
	return stillOver, succeeded
}

func (c *DiskCache) overLimitReasonLocked() (cacheEvictionReason, bool) {
	if c.currentSize > c.maxBytes {
		return cacheEvictionReasonByte, true
	}
	if c.order.Len() > c.maxEntries {
		return cacheEvictionReasonEntry, true
	}
	return "", false
}

func (c *DiskCache) updateConvergenceMetricsLocked() {
	excessEntries := c.order.Len() - c.maxEntries
	if excessEntries < 0 {
		excessEntries = 0
	}
	excessBytes := c.currentSize - c.maxBytes
	if excessBytes < 0 {
		excessBytes = 0
	}
	cacheConvergenceExcessEntries.Set(float64(excessEntries))
	cacheConvergenceExcessBytes.Set(float64(excessBytes))
	if excessEntries > 0 || excessBytes > 0 {
		cacheConvergenceActive.Set(1)
		if c.convergenceWake != nil {
			select {
			case c.convergenceWake <- struct{}{}:
			default:
			}
		}
	} else {
		cacheConvergenceActive.Set(0)
	}
}
