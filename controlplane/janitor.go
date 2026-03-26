package controlplane

import (
	"context"
	"log/slog"
	"time"
)

type controlPlaneExpiryStore interface {
	ExpireControlPlaneInstances(cutoff time.Time) (int64, error)
}

type ControlPlaneJanitor struct {
	store         controlPlaneExpiryStore
	interval      time.Duration
	expiryTimeout time.Duration
	now           func() time.Time
}

func NewControlPlaneJanitor(store controlPlaneExpiryStore, interval, expiryTimeout time.Duration) *ControlPlaneJanitor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if expiryTimeout <= 0 {
		expiryTimeout = 20 * time.Second
	}
	return &ControlPlaneJanitor{
		store:         store,
		interval:      interval,
		expiryTimeout: expiryTimeout,
		now:           time.Now,
	}
}

func (j *ControlPlaneJanitor) Run(ctx context.Context) {
	if j == nil || j.store == nil {
		return
	}

	j.runOnce()

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce()
		}
	}
}

func (j *ControlPlaneJanitor) runOnce() {
	cutoff := j.now().Add(-j.expiryTimeout)
	expired, err := j.store.ExpireControlPlaneInstances(cutoff)
	if err != nil {
		slog.Warn("Janitor failed to expire stale control-plane instances.", "error", err)
		return
	}
	if expired > 0 {
		slog.Info("Janitor expired stale control-plane instances.", "count", expired, "cutoff", cutoff)
	}
}
