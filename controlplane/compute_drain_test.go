package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeDrainStore is an in-memory stand-in for the durable buffer.
type fakeDrainStore struct {
	buckets   []configstore.ComputeUsageBucket
	highWater map[string]time.Time
	marked    []configstore.ComputeUsageBucket
	swept     int64
}

func newFakeDrainStore(b ...configstore.ComputeUsageBucket) *fakeDrainStore {
	return &fakeDrainStore{buckets: b, highWater: map[string]time.Time{}}
}

func (f *fakeDrainStore) ListDrainableComputeBuckets(closedBefore time.Time) ([]configstore.ComputeUsageBucket, error) {
	var out []configstore.ComputeUsageBucket
	for _, b := range f.buckets {
		if b.BucketStart.After(closedBefore) {
			continue // not closed yet
		}
		if hw, ok := f.highWater[b.OrgID]; ok && !b.BucketStart.After(hw) {
			continue // already drained
		}
		out = append(out, b)
	}
	return out, nil
}

func (f *fakeDrainStore) MarkComputeBucketDrained(orgID string, bucketStart time.Time) error {
	if hw, ok := f.highWater[orgID]; !ok || bucketStart.After(hw) {
		f.highWater[orgID] = bucketStart
	}
	f.marked = append(f.marked, configstore.ComputeUsageBucket{OrgID: orgID, BucketStart: bucketStart})
	// Remove the row from the buffer (DELETE half of the txn).
	var remaining []configstore.ComputeUsageBucket
	for _, b := range f.buckets {
		if b.OrgID == orgID && b.BucketStart.Equal(bucketStart) {
			continue
		}
		remaining = append(remaining, b)
	}
	f.buckets = remaining
	return nil
}

func (f *fakeDrainStore) SweepDrainedComputeBuckets() (int64, error) {
	return f.swept, nil
}

// fakeShipper records what it ships and can be made to fail per-org.
type fakeShipper struct {
	shipped []computeUsageBucket
	failOrg string
}

func (s *fakeShipper) Ship(_ context.Context, b computeUsageBucket) error {
	if s.failOrg != "" && b.OrgID == s.failOrg {
		return errors.New("ingestion down")
	}
	s.shipped = append(s.shipped, b)
	return nil
}

func TestComputeDrainerShipThenDelete(t *testing.T) {
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeDrainStore(
		configstore.ComputeUsageBucket{OrgID: "orgA", BucketStart: base, CPUSeconds: 80, MemorySeconds: 160},
	)
	shipper := &fakeShipper{}
	d := newComputeDrainer(store, shipper)
	// Pin "now" well past the bucket close window.
	d.now = func() time.Time { return base.Add(10 * time.Minute) }

	d.runOnce(context.Background())

	if len(shipper.shipped) != 1 {
		t.Fatalf("shipped = %d, want 1", len(shipper.shipped))
	}
	if shipper.shipped[0].CPUSeconds != 80 || shipper.shipped[0].MemorySeconds != 160 {
		t.Fatalf("shipped payload = %+v", shipper.shipped[0])
	}
	// Ship succeeded → high-water advanced + row deleted.
	if len(store.marked) != 1 {
		t.Fatalf("marked drained = %d, want 1", len(store.marked))
	}
	if len(store.buckets) != 0 {
		t.Fatalf("buffer rows after drain = %d, want 0", len(store.buckets))
	}
	if !store.highWater["orgA"].Equal(base) {
		t.Fatalf("high-water = %v, want %v", store.highWater["orgA"], base)
	}
}

func TestComputeDrainerKeepsRowOnShipFailure(t *testing.T) {
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeDrainStore(
		configstore.ComputeUsageBucket{OrgID: "orgA", BucketStart: base, CPUSeconds: 80, MemorySeconds: 160},
	)
	shipper := &fakeShipper{failOrg: "orgA"}
	d := newComputeDrainer(store, shipper)
	d.now = func() time.Time { return base.Add(10 * time.Minute) }

	d.runOnce(context.Background())

	// Ship failed → no mark, row retained for retry, no high-water advance.
	if len(store.marked) != 0 {
		t.Fatalf("marked drained = %d, want 0 on ship failure", len(store.marked))
	}
	if len(store.buckets) != 1 {
		t.Fatalf("buffer rows = %d, want 1 retained for retry", len(store.buckets))
	}
	if _, ok := store.highWater["orgA"]; ok {
		t.Fatalf("high-water advanced despite ship failure")
	}
}

func TestComputeDrainerSkipsOpenBuckets(t *testing.T) {
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeDrainStore(
		configstore.ComputeUsageBucket{OrgID: "orgA", BucketStart: base, CPUSeconds: 1, MemorySeconds: 1},
	)
	shipper := &fakeShipper{}
	d := newComputeDrainer(store, shipper)
	// "now" is only 30s past bucket start — within width(60s)+grace(30s), still open.
	d.now = func() time.Time { return base.Add(30 * time.Second) }

	d.runOnce(context.Background())
	if len(shipper.shipped) != 0 {
		t.Fatalf("shipped an open bucket: %d", len(shipper.shipped))
	}
}

func TestComputeDrainerHighWaterSkipsDrained(t *testing.T) {
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	store := newFakeDrainStore(
		configstore.ComputeUsageBucket{OrgID: "orgA", BucketStart: base, CPUSeconds: 1, MemorySeconds: 1},
	)
	store.highWater["orgA"] = base // already drained at or past this bucket
	shipper := &fakeShipper{}
	d := newComputeDrainer(store, shipper)
	d.now = func() time.Time { return base.Add(10 * time.Minute) }

	d.runOnce(context.Background())
	if len(shipper.shipped) != 0 {
		t.Fatalf("re-shipped an already-drained bucket: %d", len(shipper.shipped))
	}
}

func TestComputeEventUUIDDeterministicAndDistinct(t *testing.T) {
	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	u1 := computeEventUUID("orgA", base)
	u2 := computeEventUUID("orgA", base)
	if u1 != u2 {
		t.Fatalf("uuid not stable across calls: %q vs %q", u1, u2)
	}
	if len(u1) != 36 {
		t.Fatalf("uuid not UUID-shaped: %q", u1)
	}
	// Version nibble must be 4.
	if u1[14] != '4' {
		t.Fatalf("uuid version nibble = %c, want 4 (%q)", u1[14], u1)
	}
	if computeEventUUID("orgB", base) == u1 {
		t.Fatal("different org produced same uuid")
	}
	if computeEventUUID("orgA", base.Add(time.Minute)) == u1 {
		t.Fatal("different bucket produced same uuid")
	}
}
