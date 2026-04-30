//go:build kubernetes

package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// credentialRefreshScheduleStore is the slice of the runtime store the
// scheduler depends on. Defined narrowly so unit tests can fake it without
// pulling in the full store.
type credentialRefreshScheduleStore interface {
	ListWorkersDueForCredentialRefresh(ownerCPInstanceID string, cutoff time.Time) ([]configstore.WorkerRecord, error)
}

// credentialRefreshScheduler re-brokers STS-backed S3 credentials for the
// workers this CP currently owns. It runs as a per-CP background goroutine,
// not under the janitor's leader lease — credential expiry is per-worker and
// doesn't coordinate across CPs, so each CP must refresh its own workers
// regardless of whether it holds the janitor leader lease.
type credentialRefreshScheduler struct {
	interval       time.Duration
	lookahead      time.Duration
	cpInstanceID   string
	store          credentialRefreshScheduleStore
	workerByID     func(int) (*ManagedWorker, bool)
	refresh        func(ctx context.Context, w *ManagedWorker) error
	refreshTimeout time.Duration
	now            func() time.Time
}

func (s *credentialRefreshScheduler) Run(ctx context.Context) {
	if s == nil || s.store == nil || s.workerByID == nil || s.refresh == nil {
		return
	}
	if s.interval <= 0 {
		s.interval = 5 * time.Second
	}
	if s.refreshTimeout <= 0 {
		s.refreshTimeout = 30 * time.Second
	}
	if s.now == nil {
		s.now = time.Now
	}

	s.tick(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *credentialRefreshScheduler) tick(ctx context.Context) {
	cutoff := s.now().Add(s.lookahead)
	due, err := s.store.ListWorkersDueForCredentialRefresh(s.cpInstanceID, cutoff)
	if err != nil {
		slog.Warn("Credential refresh scheduler failed to list due workers.", "error", err)
		return
	}
	if len(due) == 0 {
		return
	}
	slog.Info("Refreshing S3 credentials on workers nearing expiry.", "count", len(due))
	for i := range due {
		rec := due[i]
		worker, ok := s.workerByID(rec.WorkerID)
		if !ok {
			// Worker isn't in this CP's in-memory pool right now — could be
			// mid-takeover, mid-retire, or just briefly dropped. The DB row
			// still says we own it; if it comes back to us next tick we'll
			// pick it up then. Until then, the worker either retires or its
			// owner_cp_instance_id changes and the next tick's query won't
			// return it.
			continue
		}
		refreshCtx, cancel := context.WithTimeout(ctx, s.refreshTimeout)
		if err := s.refresh(refreshCtx, worker); err != nil {
			slog.Warn("Failed to refresh worker S3 credentials.",
				"worker_id", rec.WorkerID, "org", rec.OrgID, "error", err)
		}
		cancel()
	}
}
