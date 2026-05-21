//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type captureWarmCapacityMissLister struct {
	aggregates []configstore.WarmCapacityMissAggregate
	err        error
	calls      int
	since      time.Time
	reasons    []configstore.WorkerClaimMissReason
}

func (l *captureWarmCapacityMissLister) ListWarmCapacityMissesSince(since time.Time, reasons ...configstore.WorkerClaimMissReason) ([]configstore.WarmCapacityMissAggregate, error) {
	l.calls++
	l.since = since
	l.reasons = append([]configstore.WorkerClaimMissReason(nil), reasons...)
	if l.err != nil {
		return nil, l.err
	}
	return l.aggregates, nil
}

func TestComputeEffectiveWarmCapacityTargetsDisabledSkipsDemandRead(t *testing.T) {
	lister := &captureWarmCapacityMissLister{
		err: errors.New("should not be called"),
	}

	got, err := computeEffectiveWarmCapacityTargets(
		map[string]int{"posthog/duckgres:default": 4},
		lister,
		K8sConfig{DynamicWarmCapacityEnabled: false},
		time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("expected disabled dynamic mode to skip demand read, got err %v", err)
	}
	if lister.calls != 0 {
		t.Fatalf("expected disabled dynamic mode not to read demand buckets, got %d calls", lister.calls)
	}
	want := map[string]int{"posthog/duckgres:default": 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected base targets %v, got %v", want, got)
	}
}

func TestComputeEffectiveWarmCapacityTargetsReadsBucketsForImageTargets(t *testing.T) {
	now := time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC)
	lister := &captureWarmCapacityMissLister{
		aggregates: []configstore.WarmCapacityMissAggregate{
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonNoIdle, 8),
			warmCapacityMissAggregate("image:posthog/duckgres:v1.5.1", configstore.WorkerClaimMissReasonNoIdle, 16),
		},
	}

	got, err := computeEffectiveWarmCapacityTargets(
		map[string]int{
			"posthog/duckgres:default": 4,
			"posthog/duckgres:v1.5.1":  1,
		},
		lister,
		K8sConfig{
			DynamicWarmCapacityEnabled:  true,
			WarmCapacityMissWindow:      2 * time.Minute,
			WarmCapacityMissesPerWorker: 8,
		},
		now,
	)
	if err != nil {
		t.Fatalf("compute effective warm targets: %v", err)
	}
	if lister.calls != 1 {
		t.Fatalf("expected one demand bucket read, got %d", lister.calls)
	}
	if wantSince := now.Add(-2 * time.Minute); !lister.since.Equal(wantSince) {
		t.Fatalf("expected demand read since %v, got %v", wantSince, lister.since)
	}
	wantReasons := []configstore.WorkerClaimMissReason{configstore.WorkerClaimMissReasonNoIdle}
	if !reflect.DeepEqual(lister.reasons, wantReasons) {
		t.Fatalf("expected demand read reasons %v, got %v", wantReasons, lister.reasons)
	}
	want := map[string]int{
		"posthog/duckgres:default": 5,
		"posthog/duckgres:v1.5.1":  3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected dynamic targets %v, got %v", want, got)
	}
}

func TestComputeEffectiveWarmCapacityTargetsFallsBackToBaseOnReadError(t *testing.T) {
	lister := &captureWarmCapacityMissLister{
		err: errors.New("configstore read failed"),
	}

	got, err := computeEffectiveWarmCapacityTargets(
		map[string]int{"posthog/duckgres:default": 4},
		lister,
		K8sConfig{
			DynamicWarmCapacityEnabled:  true,
			WarmCapacityMissWindow:      2 * time.Minute,
			WarmCapacityMissesPerWorker: 8,
		},
		time.Date(2026, time.March, 26, 15, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("expected demand read error")
	}
	want := map[string]int{"posthog/duckgres:default": 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected base targets on read error %v, got %v", want, got)
	}
}

func TestReconcileWarmCapacityImageTargetsSpawnsPerImageTargets(t *testing.T) {
	pool, _ := newTestK8sPool(t, 0)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store

	var mu sync.Mutex
	nextID := 1000
	slotCounts := map[string]int{}
	store.perImageSpawnedFunc = func(image string) *configstore.WorkerRecord {
		mu.Lock()
		defer mu.Unlock()
		nextID++
		slotCounts[image]++
		return &configstore.WorkerRecord{
			WorkerID:          nextID,
			PodName:           fmt.Sprintf("duckgres-worker-test-cp-%d", nextID),
			Image:             image,
			State:             configstore.WorkerStateSpawning,
			OwnerCPInstanceID: pool.cpInstanceID,
		}
	}
	var spawned int
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		mu.Lock()
		defer mu.Unlock()
		spawned++
		return nil
	}

	reconcileWarmCapacityImageTargets(pool, map[string]int{
		"posthog/duckgres:default": 2,
		"posthog/duckgres:v1.5.1":  1,
	})

	mu.Lock()
	gotSlots := map[string]int{}
	for image, count := range slotCounts {
		gotSlots[image] = count
	}
	gotSpawned := spawned
	mu.Unlock()
	wantSlots := map[string]int{
		"posthog/duckgres:default": 2,
		"posthog/duckgres:v1.5.1":  1,
	}
	if !reflect.DeepEqual(gotSlots, wantSlots) {
		t.Fatalf("expected per-image slot requests %v, got %v", wantSlots, gotSlots)
	}
	if gotSpawned != 3 {
		t.Fatalf("expected 3 per-image warm spawns, got %d", gotSpawned)
	}
	if store.neutralSpawnCalls != 0 {
		t.Fatalf("expected no image-blind neutral warm spawns, got %d", store.neutralSpawnCalls)
	}
}
