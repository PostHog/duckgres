package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestComputeConnectionUsage(t *testing.T) {
	tests := []struct {
		name       string
		millicores int64
		mib        int64
		dur        time.Duration
		wantCPU    int64 // milli-cpu-seconds
		wantMem    int64 // mib-seconds
	}{
		{
			// 8 vCPU / 16 GiB held 9.2s → ceil 10 → 80 vCPU-s, 160 GiB-s.
			name: "8cpu_16gib_9.2s", millicores: 8000, mib: 16 * 1024, dur: 9200 * time.Millisecond,
			wantCPU: 8000 * 10, wantMem: 16 * 1024 * 10,
		},
		{
			// Exactly 10s, no ceil bump.
			name: "8cpu_64gib_10s", millicores: 8000, mib: 64 * 1024, dur: 10 * time.Second,
			wantCPU: 8000 * 10, wantMem: 64 * 1024 * 10,
		},
		{
			// Sub-second connection still bills one whole second.
			name: "2cpu_4gib_0.3s", millicores: 2000, mib: 4 * 1024, dur: 300 * time.Millisecond,
			wantCPU: 2000 * 1, wantMem: 4 * 1024 * 1,
		},
		{
			// Fractional core (500m) — no truncation thanks to millicore math.
			name: "500m_512Mi_3s", millicores: 500, mib: 512, dur: 3 * time.Second,
			wantCPU: 500 * 3, wantMem: 512 * 3,
		},
		{
			// Unknown worker size → skipped.
			name: "unknown_size", millicores: 0, mib: 0, dur: 30 * time.Second,
			wantCPU: 0, wantMem: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCPU, gotMem := computeConnectionUsage(tt.millicores, tt.mib, tt.dur)
			if gotCPU != tt.wantCPU || gotMem != tt.wantMem {
				t.Fatalf("computeConnectionUsage(%d,%d,%v) = (%d,%d), want (%d,%d)",
					tt.millicores, tt.mib, tt.dur, gotCPU, gotMem, tt.wantCPU, tt.wantMem)
			}
		})
	}
}

// keyA is the billing key most tests share: orgA's default team, standard
// source, an 8-vCPU/16-GiB worker.
func keyA(bucket time.Time) computeUsageKey {
	return computeUsageKey{orgID: "orgA", teamID: "42", querySource: "standard", millicores: 8000, mib: 16 * 1024, bucket: bucket}
}

func TestComputeUsageCounterBucketing(t *testing.T) {
	c := newComputeUsageCounter()
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	// Two connections in the same minute-bucket for the same key accumulate.
	c.Record("orgA", "42", "standard", 8000, 16*1024, base.Add(5*time.Second), 10*time.Second)  // 80 cpu-s, 160 gib-s
	c.Record("orgA", "42", "standard", 8000, 16*1024, base.Add(40*time.Second), 10*time.Second) // +80, +160
	// A connection in the next bucket is separate.
	c.Record("orgA", "42", "standard", 8000, 16*1024, base.Add(65*time.Second), 5*time.Second) // 40 cpu-s, 80 gib-s
	// Different org, same bucket.
	c.Record("orgB", "7", "standard", 1000, 1024, base.Add(5*time.Second), 2*time.Second) // 2 cpu-s, 2 gib-s
	// Same org+bucket but a different query source is a separate key.
	c.Record("orgA", "42", "endpoints", 8000, 16*1024, base.Add(10*time.Second), 10*time.Second) // 80, 160
	// Same org+bucket but a different worker size is a separate key.
	c.Record("orgA", "42", "standard", 2000, 4*1024, base.Add(10*time.Second), 10*time.Second) // 20, 40

	deltas := c.drainWholeUnits()
	got := map[computeUsageKey]configstore.ComputeUsageDelta{}
	for _, d := range deltas {
		k := computeUsageKey{orgID: d.OrgID, teamID: d.TeamID, querySource: d.QuerySource, millicores: d.Millicores, mib: d.MiB, bucket: d.BucketStart}
		got[k] = d
	}

	bucket0 := base.Truncate(computeBucketWidth)
	bucket1 := base.Add(60 * time.Second).Truncate(computeBucketWidth)

	if d := got[keyA(bucket0)]; d.CPUSeconds != 160 || d.MemorySeconds != 320 {
		t.Errorf("orgA bucket0 = (%d,%d), want (160,320)", d.CPUSeconds, d.MemorySeconds)
	}
	if d := got[keyA(bucket1)]; d.CPUSeconds != 40 || d.MemorySeconds != 80 {
		t.Errorf("orgA bucket1 = (%d,%d), want (40,80)", d.CPUSeconds, d.MemorySeconds)
	}
	if d := got[computeUsageKey{orgID: "orgB", teamID: "7", querySource: "standard", millicores: 1000, mib: 1024, bucket: bucket0}]; d.CPUSeconds != 2 || d.MemorySeconds != 2 {
		t.Errorf("orgB bucket0 = (%d,%d), want (2,2)", d.CPUSeconds, d.MemorySeconds)
	}
	endpointsKey := keyA(bucket0)
	endpointsKey.querySource = "endpoints"
	if d := got[endpointsKey]; d.CPUSeconds != 80 || d.MemorySeconds != 160 {
		t.Errorf("orgA endpoints bucket0 = (%d,%d), want (80,160) — query_source must be its own key", d.CPUSeconds, d.MemorySeconds)
	}
	smallKey := keyA(bucket0)
	smallKey.millicores, smallKey.mib = 2000, 4*1024
	if d := got[smallKey]; d.CPUSeconds != 20 || d.MemorySeconds != 40 {
		t.Errorf("orgA small-worker bucket0 = (%d,%d), want (20,40) — worker size must be its own key", d.CPUSeconds, d.MemorySeconds)
	}
}

func TestComputeUsageCounterRemainderCarry(t *testing.T) {
	c := newComputeUsageCounter()
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	// 500 millicores × 1s = 500 milli-cpu-seconds = 0 whole vCPU-seconds yet,
	// and 512 MiB × 1s = 512 MiB-seconds = 0 whole GiB-seconds yet.
	c.Record("orgA", "42", "standard", 500, 512, base, 1*time.Second)
	if deltas := c.drainWholeUnits(); len(deltas) != 0 {
		t.Fatalf("expected no whole units yet, got %v", deltas)
	}
	// Add another 500 milli-cpu-s (→ 1000 = 1 vCPU-s) and 512 MiB-s (→ 1024 = 1 GiB-s).
	c.Record("orgA", "42", "standard", 500, 512, base, 1*time.Second)
	deltas := c.drainWholeUnits()
	if len(deltas) != 1 || deltas[0].CPUSeconds != 1 || deltas[0].MemorySeconds != 1 {
		t.Fatalf("expected exactly 1 vCPU-s + 1 GiB-s after carry, got %v", deltas)
	}
	// The delta must carry the worker size for the NUMERIC key columns.
	if deltas[0].Millicores != 500 || deltas[0].MiB != 512 {
		t.Fatalf("delta size = (%d,%d), want (500,512)", deltas[0].Millicores, deltas[0].MiB)
	}
	// Remainder is now zero — the bucket should be gone.
	if deltas := c.drainWholeUnits(); len(deltas) != 0 {
		t.Fatalf("expected bucket drained to empty, got %v", deltas)
	}
}

// fakeFlushStore records flushed deltas and can be made to fail.
type fakeFlushStore struct {
	flushed []configstore.ComputeUsageDelta
	err     error
}

func (f *fakeFlushStore) FlushComputeUsage(d []configstore.ComputeUsageDelta) error {
	if f.err != nil {
		return f.err
	}
	f.flushed = append(f.flushed, d...)
	return nil
}

func teamResolverA(orgID string) string {
	if orgID == "orgA" {
		return "42"
	}
	return ""
}

func TestComputeMeterFlush(t *testing.T) {
	store := &fakeFlushStore{}
	m := newComputeMeter(store, teamResolverA)
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	m.Record("orgA", "standard", 8000, 16*1024, base, 10*time.Second)

	if n := m.Flush(); n != 1 {
		t.Fatalf("Flush rows = %d, want 1", n)
	}
	if len(store.flushed) != 1 || store.flushed[0].CPUSeconds != 80 || store.flushed[0].MemorySeconds != 160 {
		t.Fatalf("flushed = %v, want one (80,160)", store.flushed)
	}
	// The meter resolves the org's default team at record time.
	if store.flushed[0].TeamID != "42" || store.flushed[0].QuerySource != "standard" {
		t.Fatalf("flushed key = (team=%q, source=%q), want (42, standard)", store.flushed[0].TeamID, store.flushed[0].QuerySource)
	}
	// Nothing left to flush.
	if n := m.Flush(); n != 0 {
		t.Fatalf("second Flush rows = %d, want 0", n)
	}
}

func TestComputeMeterUnknownTeamTolerated(t *testing.T) {
	// An org with no default_team_id (or a nil resolver) still meters — the
	// bucket carries an empty team_id rather than dropping usage.
	store := &fakeFlushStore{}
	m := newComputeMeter(store, teamResolverA)
	m.Record("orgB", "standard", 1000, 1024, time.Now(), 2*time.Second)
	if n := m.Flush(); n != 1 {
		t.Fatalf("Flush rows = %d, want 1", n)
	}
	if store.flushed[0].TeamID != "" {
		t.Fatalf("team = %q, want empty for unknown org", store.flushed[0].TeamID)
	}
}

func TestComputeMeterFlushErrorIsBestEffort(t *testing.T) {
	store := &fakeFlushStore{err: errors.New("db down")}
	m := newComputeMeter(store, teamResolverA)
	m.Record("orgA", "standard", 8000, 16*1024, time.Now(), 10*time.Second)
	// A flush error must not panic; the count for this interval is dropped.
	_ = m.Flush()
}

func TestComputeMeterRecordNeverPanicsOnNilOrDisabled(t *testing.T) {
	// A nil meter (metering disabled) is a no-op.
	var m *computeMeter
	m.Record("orgA", "standard", 8000, 16*1024, time.Now(), time.Second)
	if got := m.Flush(); got != 0 {
		t.Fatalf("nil meter Flush = %d, want 0", got)
	}

	// A meter with a nil store (and nil resolver) flushes nothing and never
	// panics.
	m2 := &computeMeter{counter: newComputeUsageCounter()}
	m2.Record("orgA", "standard", 8000, 16*1024, time.Now(), time.Second)
	if got := m2.Flush(); got != 0 {
		t.Fatalf("nil-store meter Flush = %d, want 0", got)
	}
}

func TestComputeMeterRunFinalFlushOnCancel(t *testing.T) {
	store := &fakeFlushStore{}
	m := newComputeMeter(store, teamResolverA)
	m.Record("orgA", "standard", 8000, 16*1024, time.Now(), 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if len(store.flushed) != 1 {
		t.Fatalf("expected final flush on cancel, got %d rows", len(store.flushed))
	}
}
