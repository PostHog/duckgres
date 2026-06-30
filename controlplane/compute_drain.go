package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const (
	// computeDrainTick is how often the leader scans for closed buckets to ship.
	computeDrainTick = 60 * time.Second
	// computeBucketGrace waits out CP-pod flush lag + clock skew so every
	// contribution to a bucket has landed before it is shipped. Must be >=
	// in-proc flush interval + clock-skew margin (15s + 15s → 30s).
	computeBucketGrace = 30 * time.Second
)

// computeDrainStore is the durable-buffer surface the drainer needs.
type computeDrainStore interface {
	ListDrainableComputeBuckets(closedBefore time.Time) ([]configstore.ComputeUsageBucket, error)
	MarkComputeBucketDrained(orgID string, bucketStart time.Time) error
	SweepDrainedComputeBuckets() (int64, error)
}

// computeShipper ships one bucket to ingestion. Any error means "keep the row,
// retry next tick".
type computeShipper interface {
	Ship(ctx context.Context, b computeUsageBucket) error
}

// computeDrainer is the leader-only loop that ships closed compute-usage
// buckets to PostHog (ship-then-delete, at-least-once). Runs alongside the
// janitor under the same leader lease.
type computeDrainer struct {
	store   computeDrainStore
	shipper computeShipper
	now     func() time.Time
}

func newComputeDrainer(store computeDrainStore, shipper computeShipper) *computeDrainer {
	return &computeDrainer{store: store, shipper: shipper, now: time.Now}
}

// Run scans for closed buckets every tick until ctx is cancelled. Leader-only:
// the caller wires it through a leaderOnlyLoop so exactly one CP pod drains.
func (d *computeDrainer) Run(ctx context.Context) {
	if d == nil || d.store == nil || d.shipper == nil {
		return
	}
	d.runOnce(ctx)
	ticker := time.NewTicker(computeDrainTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnce(ctx)
		}
	}
}

func (d *computeDrainer) runOnce(ctx context.Context) {
	closedBefore := d.now().Add(-computeBucketWidth - computeBucketGrace)
	buckets, err := d.store.ListDrainableComputeBuckets(closedBefore)
	if err != nil {
		slog.Warn("Compute-usage drain: failed to list drainable buckets.", "error", err)
		return
	}

	var shipped, failed int
	for _, b := range buckets {
		if ctx.Err() != nil {
			return
		}
		evt := computeUsageBucket{
			OrgID:         b.OrgID,
			BucketStart:   b.BucketStart,
			CPUSeconds:    b.CPUSeconds,
			MemorySeconds: b.MemorySeconds,
		}
		if err := d.shipper.Ship(ctx, evt); err != nil {
			// Ship-then-delete: a failure keeps the row for the next tick.
			slog.Warn("Compute-usage drain: ship failed; will retry next tick.", "org", b.OrgID, "bucket", b.BucketStart, "error", err)
			failed++
			continue
		}
		// 2xx confirmed: advance high-water + delete the row, atomically.
		if err := d.store.MarkComputeBucketDrained(b.OrgID, b.BucketStart); err != nil {
			// The event was accepted but we couldn't record it. The deterministic
			// uuid makes a re-ship next tick the same ClickHouse row, so this is
			// safe (at-least-once collapses to effectively-once).
			slog.Warn("Compute-usage drain: shipped but failed to mark drained; may re-ship (idempotent).", "org", b.OrgID, "bucket", b.BucketStart, "error", err)
			failed++
			continue
		}
		shipped++
	}

	if swept, err := d.store.SweepDrainedComputeBuckets(); err != nil {
		slog.Warn("Compute-usage drain: sweep of already-drained buckets failed.", "error", err)
	} else if swept > 0 {
		slog.Info("Compute-usage drain: swept already-drained buffer rows.", "rows", swept)
	}

	if shipped > 0 || failed > 0 {
		slog.Info("Compute-usage drain tick complete.", "shipped", shipped, "failed", failed)
	}
}
