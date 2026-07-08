package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeStorageStore struct {
	mu      sync.Mutex
	samples []storageSampleRecord
	err     error
}

type storageSampleRecord struct {
	orgID       string
	teamID      int64
	bucketStart time.Time
	byteSeconds int64
}

func (f *fakeStorageStore) UpsertStorageSample(orgID string, teamID int64, bucketStart time.Time, byteSeconds int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.samples = append(f.samples, storageSampleRecord{orgID, teamID, bucketStart, byteSeconds})
	return nil
}

func newTestStorageSampler(store storageUsageStore, orgs []storageOrg, footprint func(ctx context.Context, dsn string) (int64, int64, error)) *storageSampler {
	s := newStorageSampler(store, 30*time.Minute,
		func() []storageOrg { return orgs },
		func(_ context.Context, orgID string) (string, error) { return "postgres://meta/" + orgID, nil },
	)
	s.queryFootprint = footprint
	s.now = func() time.Time { return time.Date(2026, 7, 8, 12, 0, 47, 0, time.UTC) }
	return s
}

func TestStorageSamplerCreditsOneIntervalPerOrg(t *testing.T) {
	store := &fakeStorageStore{}
	s := newTestStorageSampler(store,
		[]storageOrg{{OrgID: "orgA", TeamID: 42}, {OrgID: "orgB", TeamID: 0}},
		func(_ context.Context, dsn string) (int64, int64, error) {
			if dsn == "postgres://meta/orgA" {
				return 10 * 1024 * 1024 * 1024, 0, nil // 10 GiB
			}
			return 512, 3, nil
		})

	s.sampleAll(context.Background())

	if len(store.samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(store.samples))
	}
	wantBucket := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC) // sample minute, floored
	a := store.samples[0]
	if a.orgID != "orgA" || a.teamID != 42 || !a.bucketStart.Equal(wantBucket) {
		t.Fatalf("orgA sample = %+v", a)
	}
	// 10 GiB × 1800s.
	if want := int64(10*1024*1024*1024) * 1800; a.byteSeconds != want {
		t.Fatalf("orgA byteSeconds = %d, want %d (bytes × interval)", a.byteSeconds, want)
	}
	if b := store.samples[1]; b.orgID != "orgB" || b.teamID != 0 || b.byteSeconds != 512*1800 {
		t.Fatalf("orgB sample = %+v", b)
	}
}

func TestStorageSamplerSkipsFailingOrg(t *testing.T) {
	store := &fakeStorageStore{}
	s := newTestStorageSampler(store,
		[]storageOrg{{OrgID: "broken", TeamID: 1}, {OrgID: "healthy", TeamID: 2}},
		func(_ context.Context, dsn string) (int64, int64, error) {
			if dsn == "postgres://meta/broken" {
				return 0, 0, errors.New("connection refused")
			}
			return 1024 * 1024 * 1024, 0, nil
		})

	s.sampleAll(context.Background())

	// The broken org is skipped (under-billed), the healthy one still lands.
	if len(store.samples) != 1 || store.samples[0].orgID != "healthy" {
		t.Fatalf("samples = %+v, want exactly the healthy org", store.samples)
	}
}

func TestStorageSamplerStoreErrorIsBestEffort(t *testing.T) {
	store := &fakeStorageStore{err: errors.New("db down")}
	s := newTestStorageSampler(store,
		[]storageOrg{{OrgID: "orgA", TeamID: 42}},
		func(_ context.Context, _ string) (int64, int64, error) { return 1, 0, nil })
	// Must not panic; the interval is dropped.
	s.sampleAll(context.Background())
}

func TestStorageSamplerRunStopsOnCancel(t *testing.T) {
	store := &fakeStorageStore{}
	// Short interval: the first pass fires after one tick (never on entry —
	// see TestStorageSamplerLeadershipChurnNeverOverCredits).
	s := newStorageSampler(store, 20*time.Millisecond,
		func() []storageOrg { return []storageOrg{{OrgID: "orgA", TeamID: 42}} },
		func(_ context.Context, orgID string) (string, error) { return "postgres://meta/" + orgID, nil },
	)
	s.queryFootprint = func(_ context.Context, _ string) (int64, int64, error) { return 2048, 0, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	if got := waitForSamples(store, 1, 2*time.Second); got < 1 {
		t.Fatal("ticker sample pass never ran")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sampler did not stop on cancel")
	}
}

func TestStorageSampleIntervalFromEnv(t *testing.T) {
	t.Setenv("DUCKGRES_STORAGE_SAMPLE_INTERVAL", "")
	if got := storageSampleIntervalFromEnv(); got != defaultStorageSampleInterval {
		t.Fatalf("unset = %v, want default", got)
	}
	t.Setenv("DUCKGRES_STORAGE_SAMPLE_INTERVAL", "60s")
	if got := storageSampleIntervalFromEnv(); got != time.Minute {
		t.Fatalf("60s = %v, want 1m", got)
	}
	t.Setenv("DUCKGRES_STORAGE_SAMPLE_INTERVAL", "banana")
	if got := storageSampleIntervalFromEnv(); got != defaultStorageSampleInterval {
		t.Fatalf("invalid = %v, want default", got)
	}
}

// waitForSamples polls until the store holds at least n samples or the
// deadline passes; returns the count seen last.
func waitForSamples(store *fakeStorageStore, n int, deadline time.Duration) int {
	stop := time.After(deadline)
	for {
		store.mu.Lock()
		got := len(store.samples)
		store.mu.Unlock()
		if got >= n {
			return got
		}
		select {
		case <-stop:
			return got
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestStorageSamplerLeadershipChurnNeverOverCredits(t *testing.T) {
	// Every leadership acquisition re-enters Run (leader_loop.go). Each
	// successful sample credits a FULL interval of byte-seconds regardless of
	// elapsed wall time, so sampling on acquisition over-bills: leader A
	// samples, loses the lease seconds later (deploy, lease flap), leader B
	// samples again -> two full intervals credited for seconds of real time.
	// The documented contract is the opposite direction ("a missed sample
	// (org unreachable, leader failover) under-bills one interval").
	store := &fakeStorageStore{}
	s := newTestStorageSampler(store,
		[]storageOrg{{OrgID: "orgA", TeamID: 42}},
		func(_ context.Context, _ string) (int64, int64, error) { return 1024, 0, nil })

	churns := 2
	for i := 0; i < churns; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { s.Run(ctx); close(done) }()
		// Give an eager implementation ample time to (wrongly) sample; the
		// 30-minute ticker cannot fire in this window either way.
		time.Sleep(150 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("sampler did not stop on cancel")
		}
	}

	store.mu.Lock()
	credited := len(store.samples)
	store.mu.Unlock()
	// Seconds of wall time may never credit more than one interval, however
	// many times leadership changes hands.
	if credited > 1 {
		t.Fatalf("leadership churn credited %d full intervals for ~0.3s of wall time (over-billing); want <= 1", credited)
	}
}
