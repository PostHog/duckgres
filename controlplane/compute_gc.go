package controlplane

import (
	"context"
	"log/slog"
	"time"
)

const (
	// computeBucketGrace waits out CP-pod flush lag + clock skew so every
	// contribution to a bucket has landed before the bucket is served by the
	// pull API (and can be deleted by an ack). Must be >= in-proc flush
	// interval + clock-skew margin (15s + 15s → 30s).
	computeBucketGrace = 30 * time.Second

	// computeUsageRetention is the safety-GC cap: buckets older than this are
	// hard-deleted even if billing never acked them, bounding table growth if
	// billing stops pulling entirely.
	computeUsageRetention = 30 * 24 * time.Hour

	// computeGCTick is how often the leader runs the safety GC.
	computeGCTick = time.Hour
)

// computeGCStore is the config-store surface the safety GC needs.
type computeGCStore interface {
	GCComputeUsage(olderThan time.Time) (int64, error)
}

// runComputeUsageGC hard-deletes compute-usage buckets past the retention cap
// on a slow tick until ctx is cancelled. Leader-only: the caller wires it
// through the janitor leader lease so exactly one CP pod sweeps. A nonzero
// drop count means billing is not pulling/acking — that data is gone for
// billing, so it is logged at WARN (alertable).
func runComputeUsageGC(ctx context.Context, store computeGCStore) {
	if store == nil {
		return
	}
	tick := func() {
		dropped, err := store.GCComputeUsage(time.Now().Add(-computeUsageRetention))
		if err != nil {
			slog.Warn("Compute-usage safety GC failed.", "error", err)
			return
		}
		if dropped > 0 {
			slog.Warn("Compute-usage safety GC dropped unacked buckets older than retention — billing is not keeping up.",
				"rows", dropped, "retention", computeUsageRetention.String())
		}
	}
	tick()
	ticker := time.NewTicker(computeGCTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}
