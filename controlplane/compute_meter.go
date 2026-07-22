package controlplane

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Compute-seconds billing intervals (see docs/design/billing-compute-seconds-plan.md).
const (
	// computeBucketWidth aligns connection-end times into billing buckets.
	computeBucketWidth = 60 * time.Second
	// computeFlushInterval is how often the in-process counter flushes to the
	// durable buffer.
	computeFlushInterval = 15 * time.Second
)

// computeUsageKey identifies one accumulator bucket: the full billing key —
// org, the org's default team, the session's query source and the provisioned
// worker size (milli-units) — plus an aligned time-bucket (connection-end time
// floored to computeBucketWidth). Distinct sizes/teams/sources accumulate (and
// bill) separately.
type computeUsageKey struct {
	orgID       string
	teamID      int64
	querySource string
	millicores  int64
	mib         int64
	bucket      time.Time
}

// computeUsageCounter is the in-process, best-effort per-org compute-usage
// accumulator. Connection-end records add millicore-seconds / MiB-seconds here
// (integer math, no truncation); a periodic flusher converts whole vCPU-/GiB-
// seconds into the durable config-store buffer and resets the in-process
// fractional remainder forward, so nothing is lost to rounding across flushes.
//
// LOAD-BEARING: every method is best-effort and must never block or fail the
// connection-teardown path. The mutex is held only for microseconds around a
// map update — no I/O under the lock.
type computeUsageCounter struct {
	mu      sync.Mutex
	buckets map[computeUsageKey]*computeUsageAccum
}

type computeUsageAccum struct {
	// Accumulated in integer milli-units to avoid truncation when a worker is
	// sized in fractional cores / sub-GiB memory:
	//   milliCPUSeconds = millicores × ceil(conn_seconds)
	//   mibSeconds      = MiB        × ceil(conn_seconds)
	milliCPUSeconds int64
	mibSeconds      int64
}

func newComputeUsageCounter() *computeUsageCounter {
	return &computeUsageCounter{buckets: make(map[computeUsageKey]*computeUsageAccum)}
}

// computeConnectionUsage returns the raw billable usage for one connection in
// integer milli-units, using the *provisioned* worker size over the whole
// connection lifetime. millicores and mib are the worker pod's provisioned CPU
// (millicores) and memory (MiB). A single ceil over the connection's wall-clock
// seconds is shared by both metrics (per the design). Returns (0, 0) when the
// worker size is unknown (millicores == 0) — non-remote / unknown profile, so
// metering is skipped.
func computeConnectionUsage(millicores, mib int64, dur time.Duration) (milliCPUSeconds, mibSeconds int64) {
	if millicores <= 0 {
		return 0, 0
	}
	secs := int64(math.Ceil(dur.Seconds()))
	if secs < 1 {
		secs = 1 // a connection that ends within the same second still bills one second
	}
	return millicores * secs, mib * secs
}

// Record adds one connection's usage to the in-process counter, bucketed by
// connection-end time. Best-effort: a zero/unknown size is a no-op. Never
// errors.
func (c *computeUsageCounter) Record(orgID, querySource string, teamID, millicores, mib int64, endTime time.Time, dur time.Duration) {
	if c == nil || orgID == "" || millicores <= 0 {
		return
	}
	milliCPU, mibSec := computeConnectionUsage(millicores, mib, dur)
	if milliCPU == 0 && mibSec == 0 {
		return
	}
	key := computeUsageKey{
		orgID:       orgID,
		teamID:      teamID,
		querySource: querySource,
		millicores:  millicores,
		mib:         mib,
		bucket:      endTime.UTC().Truncate(computeBucketWidth),
	}

	c.mu.Lock()
	a := c.buckets[key]
	if a == nil {
		a = &computeUsageAccum{}
		c.buckets[key] = a
	}
	a.milliCPUSeconds += milliCPU
	a.mibSeconds += mibSec
	c.mu.Unlock()
}

// drainWholeUnits converts the accumulated milli-units into whole vCPU-/GiB-
// second deltas to flush, carrying the sub-unit remainder forward so it is not
// lost across flushes. Buckets that have no whole units yet are kept; buckets
// fully drained to zero are removed.
func (c *computeUsageCounter) drainWholeUnits() []configstore.ComputeUsageDelta {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []configstore.ComputeUsageDelta
	for key, a := range c.buckets {
		cpuWhole := a.milliCPUSeconds / 1000
		memWhole := a.mibSeconds / 1024
		if cpuWhole == 0 && memWhole == 0 {
			continue
		}
		out = append(out, configstore.ComputeUsageDelta{
			OrgID:         key.orgID,
			TeamID:        key.teamID,
			QuerySource:   key.querySource,
			Millicores:    key.millicores,
			MiB:           key.mib,
			BucketStart:   key.bucket,
			CPUSeconds:    cpuWhole,
			MemorySeconds: memWhole,
		})
		// Carry the remainder forward.
		a.milliCPUSeconds -= cpuWhole * 1000
		a.mibSeconds -= memWhole * 1024
		if a.milliCPUSeconds == 0 && a.mibSeconds == 0 {
			delete(c.buckets, key)
		}
	}
	return out
}

// computeUsageFlusher persists the in-process counter to the durable buffer.
type computeUsageStore interface {
	FlushComputeUsage([]configstore.ComputeUsageDelta) error
}

// computeMeter wires the per-org counter to a flusher. Nil-safe: a disabled
// meter (non-remote backend) leaves this nil and every call site no-ops.
// resolveTeam maps an org to its billing PostHog team id at record time (a
// config-snapshot read — no I/O); 0 is tolerated per the OrgBillingTeamID
// contract and the bucket then carries team_id 0. Every org create path
// establishes a billing team row, so 0 can only mean an unknown org or a
// not-yet-loaded snapshot — never a stored "no billing team".
type computeMeter struct {
	counter     *computeUsageCounter
	store       computeUsageStore
	resolveTeam func(orgID string) int64
}

func newComputeMeter(store computeUsageStore, resolveTeam func(orgID string) int64) *computeMeter {
	return &computeMeter{counter: newComputeUsageCounter(), store: store, resolveTeam: resolveTeam}
}

// Record forwards a connection-end record to the in-process counter. Best-effort.
func (m *computeMeter) Record(orgID, querySource string, millicores, mib int64, endTime time.Time, dur time.Duration) {
	if m == nil {
		return
	}
	teamID := int64(0)
	if m.resolveTeam != nil {
		teamID = m.resolveTeam(orgID)
	}
	m.counter.Record(orgID, querySource, teamID, millicores, mib, endTime, dur)
}

// Flush drains the in-process counter into the durable buffer once. Best-effort:
// on error it logs and returns; the unflushed counts stay in the counter (the
// delta was not removed unless it was handed off cleanly — drainWholeUnits
// removes it before the write, so a failed write loses at most one interval of
// counts, never the request path). Returns the number of (org, bucket) rows
// attempted.
func (m *computeMeter) Flush() int {
	if m == nil || m.store == nil {
		return 0
	}
	deltas := m.counter.drainWholeUnits()
	if len(deltas) == 0 {
		return 0
	}
	if err := m.store.FlushComputeUsage(deltas); err != nil {
		slog.Warn("Compute-usage flush to durable buffer failed; counts for this interval dropped.", "rows", len(deltas), "error", err)
	}
	return len(deltas)
}

// Run flushes on a fixed interval until ctx is cancelled, then does a final
// flush so a graceful shutdown lands the last interval's counts.
func (m *computeMeter) Run(ctx context.Context) {
	if m == nil {
		return
	}
	ticker := time.NewTicker(computeFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.Flush() // final flush on graceful shutdown
			return
		case <-ticker.C:
			m.Flush()
		}
	}
}
