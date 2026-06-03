//go:build linux || darwin

package configstore_test

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestRuntimeStorePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	runtimeSchema := store.RuntimeSchema()
	if runtimeSchema == "" {
		t.Fatal("expected runtime schema to be configured")
	}

	for _, table := range []string{"cp_instances", "worker_records", "flight_session_records", "warm_capacity_miss_buckets"} {
		var count int64
		if err := store.DB().Raw(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			runtimeSchema,
			table,
		).Scan(&count).Error; err != nil {
			t.Fatalf("lookup %s.%s: %v", runtimeSchema, table, err)
		}
		if count != 1 {
			t.Fatalf("expected runtime table %s.%s to exist", runtimeSchema, table)
		}
	}

	startedAt := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	heartbeatAt := startedAt.Add(5 * time.Second)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-1:boot-a",
		PodName:         "duckgres-abc",
		PodUID:          "pod-uid-1",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance: %v", err)
	}

	cp, err := store.GetControlPlaneInstance("cp-1:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance: %v", err)
	}
	if cp.PodName != "duckgres-abc" {
		t.Fatalf("expected pod name duckgres-abc, got %q", cp.PodName)
	}
	if !cp.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Fatalf("expected heartbeat %v, got %v", heartbeatAt, cp.LastHeartbeatAt)
	}

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          42,
		PodName:           "duckgres-worker-42",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-1:boot-a",
		OwnerEpoch:        7,
		LastHeartbeatAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	worker, err := store.GetWorkerRecord(42)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if worker.State != configstore.WorkerStateIdle {
		t.Fatalf("expected worker state idle, got %q", worker.State)
	}
	if worker.OwnerEpoch != 7 {
		t.Fatalf("expected owner epoch 7, got %d", worker.OwnerEpoch)
	}

	sessionExpiry := startedAt.Add(5 * time.Minute)
	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-token-1",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   7,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    sessionExpiry,
		LastSeenAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	session, err := store.GetFlightSessionRecord("flight-token-1")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if session.WorkerID != 42 {
		t.Fatalf("expected worker id 42, got %d", session.WorkerID)
	}
	if session.Username != "postgres" {
		t.Fatalf("expected username postgres, got %q", session.Username)
	}
	if session.State != configstore.FlightSessionStateActive {
		t.Fatalf("expected session state active, got %q", session.State)
	}
}

func TestRecordWarmCapacityMissAggregatesByBucketScopeAndReasonPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	other := newConfigStoreOnSameSchema(t, store)

	now := time.Date(2026, time.March, 26, 14, 0, 5, 0, time.UTC)
	for _, recorder := range []*configstore.ConfigStore{store, other} {
		if err := recorder.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, now); err != nil {
			t.Fatalf("RecordWarmCapacityMiss(default): %v", err)
		}
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, now); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(pinned): %v", err)
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonGlobalCap, now); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(default/global-cap): %v", err)
	}

	bucketStart := now.Truncate(configstore.WarmCapacityMissBucketSize)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, bucketStart, 2)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:default", configstore.WorkerClaimMissReasonGlobalCap, bucketStart, 1)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, bucketStart, 1)
}

func TestRecordWarmCapacityMissConcurrentWritersPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	other := newConfigStoreOnSameSchema(t, store)

	now := time.Date(2026, time.March, 26, 14, 5, 5, 0, time.UTC)
	recorders := []*configstore.ConfigStore{store, other}
	errs := make(chan error, 40)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		recorder := recorders[i%len(recorders)]
		go func() {
			defer wg.Done()
			errs <- recorder.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, now)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RecordWarmCapacityMiss concurrent writer: %v", err)
		}
	}

	bucketStart := now.Truncate(configstore.WarmCapacityMissBucketSize)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, bucketStart, 40)
}

func TestListWarmCapacityMissesSinceAggregatesByScopeAndReasonPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	other := newConfigStoreOnSameSchema(t, store)

	now := time.Date(2026, time.March, 26, 14, 10, 5, 0, time.UTC)
	old := now.Add(-5 * time.Minute)
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, old); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(old): %v", err)
	}
	for _, recorder := range []*configstore.ConfigStore{store, other} {
		if err := recorder.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, now); err != nil {
			t.Fatalf("RecordWarmCapacityMiss(default): %v", err)
		}
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, now.Add(11*time.Second)); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(default next bucket): %v", err)
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, now); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(pinned): %v", err)
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonGlobalCap, now); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(global cap): %v", err)
	}

	aggregates, err := store.ListWarmCapacityMissesSince(now.Add(-time.Minute), configstore.WorkerClaimMissReasonNoIdle)
	if err != nil {
		t.Fatalf("ListWarmCapacityMissesSince: %v", err)
	}
	got := warmCapacityMissAggregateCounts(aggregates)
	want := map[string]int64{
		"image:duckgres:default|no_idle": 3,
		"image:duckgres:pinned|no_idle":  1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected no-idle aggregates %v, got %v", want, got)
	}

	aggregates, err = store.ListWarmCapacityMissesSince(now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ListWarmCapacityMissesSince unfiltered: %v", err)
	}
	got = warmCapacityMissAggregateCounts(aggregates)
	want = map[string]int64{
		"image:duckgres:default|global_cap": 1,
		"image:duckgres:default|no_idle":    3,
		"image:duckgres:pinned|no_idle":     1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected unfiltered aggregates %v, got %v", want, got)
	}
}

func TestPruneWarmCapacityMissBucketsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	oldNow := time.Date(2026, time.March, 26, 14, 0, 5, 0, time.UTC)
	newNow := oldNow.Add(15 * time.Minute)
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, oldNow); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(old): %v", err)
	}
	if err := store.RecordWarmCapacityMiss("image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, newNow); err != nil {
		t.Fatalf("RecordWarmCapacityMiss(new): %v", err)
	}

	pruned, err := store.PruneWarmCapacityMissBuckets(newNow.Add(-10 * time.Minute))
	if err != nil {
		t.Fatalf("PruneWarmCapacityMissBuckets: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected one old bucket pruned, got %d", pruned)
	}

	oldBucketStart := oldNow.Truncate(configstore.WarmCapacityMissBucketSize)
	newBucketStart := newNow.Truncate(configstore.WarmCapacityMissBucketSize)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, oldBucketStart, 0)
	assertWarmCapacityMissBucketCount(t, store, "image:duckgres:default", configstore.WorkerClaimMissReasonNoIdle, newBucketStart, 1)
}

func TestListWorkerLifecycleStatsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 26, 14, 45, 0, 0, time.UTC)
	records := []configstore.WorkerRecord{
		{
			WorkerID:          1101,
			PodName:           "duckgres-worker-test-cp-1101",
			Image:             "duckgres:default",
			State:             configstore.WorkerStateIdle,
			OwnerCPInstanceID: "cp-a",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1102,
			PodName:           "duckgres-worker-test-cp-1102",
			Image:             "duckgres:default",
			State:             configstore.WorkerStateSpawning,
			OwnerCPInstanceID: "cp-a",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1103,
			PodName:           "duckgres-worker-test-cp-1103",
			Image:             "duckgres:default",
			State:             configstore.WorkerStateHot,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-a",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1104,
			PodName:           "duckgres-worker-test-cp-1104",
			Image:             "duckgres:default",
			State:             configstore.WorkerStateHotIdle,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-a",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1105,
			PodName:           "duckgres-worker-test-cp-1105",
			Image:             "duckgres:pinned",
			State:             configstore.WorkerStateDraining,
			OrgID:             "science",
			OwnerCPInstanceID: "cp-b",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1106,
			PodName:           "duckgres-worker-test-cp-1106",
			Image:             "duckgres:pinned",
			State:             configstore.WorkerStateLost,
			OwnerCPInstanceID: "cp-b",
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          1107,
			PodName:           "duckgres-worker-test-cp-1107",
			State:             configstore.WorkerStateHot,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-a",
			LastHeartbeatAt:   now,
		},
	}
	for _, record := range records {
		if err := store.UpsertWorkerRecord(&record); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", record.WorkerID, err)
		}
	}

	stats, err := store.ListWorkerLifecycleStats()
	if err != nil {
		t.Fatalf("ListWorkerLifecycleStats: %v", err)
	}

	got := map[string]int64{}
	for _, stat := range stats {
		got[fmt.Sprintf("%s|%s|%s", stat.Image, stat.State, stat.Binding)] = stat.Count
	}
	want := map[string]int64{
		"duckgres:default|idle|neutral":       1,
		"duckgres:default|spawning|neutral":   1,
		"duckgres:default|hot|org_bound":      1,
		"duckgres:default|hot_idle|org_bound": 1,
		"duckgres:pinned|draining|org_bound":  1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected worker lifecycle stats %v, got %v", want, got)
	}
}

func warmCapacityMissAggregateCounts(aggregates []configstore.WarmCapacityMissAggregate) map[string]int64 {
	out := make(map[string]int64, len(aggregates))
	for _, aggregate := range aggregates {
		out[fmt.Sprintf("%s|%s", aggregate.Scope, aggregate.Reason)] = aggregate.Count
	}
	return out
}

func TestClaimIdleWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 13, 0, 0, 0, time.UTC)
	heartbeatAt := startedAt.Add(5 * time.Second)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   heartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", "", "", false, 0, 1)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected idle worker claim to succeed")
		return
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason on successful claim, got %q", missReason)
	}
	if claimed.WorkerID != 7 {
		t.Fatalf("expected worker id 7, got %d", claimed.WorkerID)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}
	if claimed.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", claimed.OwnerCPInstanceID)
	}
	if claimed.OwnerEpoch != 3 {
		t.Fatalf("expected owner epoch 3, got %d", claimed.OwnerEpoch)
	}
	if claimed.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", claimed.OrgID)
	}
	persisted, err := store.GetWorkerRecord(7)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateReserved {
		t.Fatalf("expected persisted reserved state, got %q", persisted.State)
	}
	if persisted.OwnerEpoch != 3 {
		t.Fatalf("expected persisted owner epoch 3, got %d", persisted.OwnerEpoch)
	}
}

func TestClaimIdleWorkerReturnsNilWhenNoIdleWorkerExists(t *testing.T) {
	store := newIsolatedConfigStore(t)

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", "", "", false, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNoIdle {
		t.Fatalf("expected no-idle miss reason, got %q", missReason)
	}
}

func TestClaimIdleWorkerReturnsGlobalCapWhenNoIdleAndGlobalCapReached(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 15, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          8,
		PodName:           "duckgres-worker-8",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot): %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "billing", "", "", "", false, 0, 1)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonGlobalCap {
		t.Fatalf("expected global-cap miss reason, got %q", missReason)
	}
}

func TestClaimIdleWorkerReturnsNoIdleWhenBelowGlobalCap(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 20, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          8,
		PodName:           "duckgres-worker-8",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot): %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "billing", "", "", "", false, 0, 2)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNoIdle {
		t.Fatalf("expected no-idle miss reason below global cap, got %q", missReason)
	}
}

func TestClaimIdleWorkerReturnsGlobalCapForImageMissAtGlobalCap(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 25, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          8,
		PodName:           "duckgres-worker-v1",
		State:             configstore.WorkerStateIdle,
		Image:             "duckgres:v1",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(idle): %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "billing", "duckgres:v2", "", "", false, 0, 1)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonGlobalCap {
		t.Fatalf("expected global-cap miss reason for image miss, got %q", missReason)
	}
}

func TestClaimIdleWorkerClaimsMatchingIdleWorkerAtGlobalCap(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 27, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          9,
		PodName:           "duckgres-worker-v2",
		State:             configstore.WorkerStateIdle,
		Image:             "duckgres:v2",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(idle): %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "billing", "duckgres:v2", "", "", false, 0, 1)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected matching idle worker claim to succeed at global cap")
		return
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason on successful claim, got %q", missReason)
	}
	if claimed.WorkerID != 9 {
		t.Fatalf("expected worker id 9, got %d", claimed.WorkerID)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}
	if claimed.OrgID != "billing" {
		t.Fatalf("expected org billing, got %q", claimed.OrgID)
	}
	if claimed.Image != "duckgres:v2" {
		t.Fatalf("expected image duckgres:v2, got %q", claimed.Image)
	}
}

func TestClaimIdleWorkerRespectsOrgCapPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 13, 30, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateIdle,
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(idle): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          8,
		PodName:           "duckgres-worker-8",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot): %v", err)
	}

	claimed, missReason, err := store.ClaimIdleWorker("cp-new:boot-b", "analytics", "", "", "", false, 1, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected org cap to block claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonOrgCap {
		t.Fatalf("expected org-cap miss reason, got %q", missReason)
	}

	persisted, err := store.GetWorkerRecord(7)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateIdle {
		t.Fatalf("expected worker to remain idle, got %q", persisted.State)
	}
	if persisted.OrgID != "" {
		t.Fatalf("expected idle worker org to remain empty, got %q", persisted.OrgID)
	}
}

func TestClaimIdleWorkerRespectsImageAffinity(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID: 7,
		PodName:  "duckgres-worker-v1",
		State:    configstore.WorkerStateIdle,
		Image:    "duckgres:v1",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID: 8,
		PodName:  "duckgres-worker-v2",
		State:    configstore.WorkerStateIdle,
		Image:    "duckgres:v2",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	// Try claiming v2
	claimed, missReason, err := store.ClaimIdleWorker("cp-1", "org-1", "duckgres:v2", "", "", false, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil || claimed.WorkerID != 8 {
		t.Fatalf("expected to claim worker 8 (v2), got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason on successful image claim, got %q", missReason)
	}

	// Try claiming v3 (none exist)
	claimed, missReason, err = store.ClaimIdleWorker("cp-1", "org-1", "duckgres:v3", "", "", false, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim for v3, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNoIdle {
		t.Fatalf("expected no-idle miss reason for unmatched image, got %q", missReason)
	}

	// Neutral claim (no image filter) - should get v1 (lowest ID)
	claimed, missReason, err = store.ClaimIdleWorker("cp-1", "org-1", "", "", "", false, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker: %v", err)
	}
	if claimed == nil || claimed.WorkerID != 7 {
		t.Fatalf("expected to claim worker 7 (neutral), got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason on successful neutral claim, got %q", missReason)
	}
}

// TestClaimIdleWorkerRespectsProfileAffinity proves the worker-profile match
// dimension against real Postgres: a request only claims an idle worker of its
// own shape. The default request ("","",false) matches default/legacy rows; a
// colocated request only matches colocated rows.
func TestClaimIdleWorkerRespectsProfileAffinity(t *testing.T) {
	store := newIsolatedConfigStore(t)

	// A default-profile idle worker (also models a legacy row: empty profile).
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID: 11,
		PodName:  "duckgres-worker-default",
		State:    configstore.WorkerStateIdle,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(default): %v", err)
	}
	// A colocated idle worker.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:        12,
		PodName:         "duckgres-worker-colocated",
		State:           configstore.WorkerStateIdle,
		ProfileCPU:      "4",
		ProfileMemory:   "16Gi",
		ProfileColocate: true,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(colocated): %v", err)
	}

	// A colocated request claims only the colocated worker (never the default).
	claimed, _, err := store.ClaimIdleWorker("cp-1", "org-1", "", "4", "16Gi", true, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker(colocated): %v", err)
	}
	if claimed == nil || claimed.WorkerID != 12 {
		t.Fatalf("expected to claim colocated worker 12, got %#v", claimed)
	}

	// A default request claims only the default worker (never the colocated one).
	claimed, _, err = store.ClaimIdleWorker("cp-1", "org-1", "", "", "", false, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker(default): %v", err)
	}
	if claimed == nil || claimed.WorkerID != 11 {
		t.Fatalf("expected to claim default worker 11, got %#v", claimed)
	}

	// With both workers now reserved, a colocated request that finds no matching
	// idle worker misses (rather than crossing shapes onto a default worker).
	claimed, missReason, err := store.ClaimIdleWorker("cp-1", "org-1", "", "8", "48Gi", true, 0, 0)
	if err != nil {
		t.Fatalf("ClaimIdleWorker(unmatched colocated): %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim for an unmatched colocated profile, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNoIdle {
		t.Fatalf("expected no-idle miss for unmatched profile, got %q", missReason)
	}
}

func TestClaimHotIdleWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 14, 0, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          9,
		PodName:           "duckgres-worker-hot-idle",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		Image:             "duckgres:v2",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        5,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          10,
		PodName:           "duckgres-worker-hot-idle-later",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		Image:             "duckgres:v2",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        3,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(second): %v", err)
	}

	claimed, missReason, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 0)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected hot-idle worker claim to succeed")
		return
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason on successful hot-idle claim, got %q", missReason)
	}
	if claimed.WorkerID != 9 {
		t.Fatalf("expected worker id 9, got %d", claimed.WorkerID)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}
	if claimed.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", claimed.OwnerCPInstanceID)
	}
	if claimed.OwnerEpoch != 6 {
		t.Fatalf("expected owner epoch 6, got %d", claimed.OwnerEpoch)
	}
	if claimed.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", claimed.OrgID)
	}
	if claimed.Image != "duckgres:v2" {
		t.Fatalf("expected image to be preserved, got %q", claimed.Image)
	}

	unclaimed, err := store.GetWorkerRecord(10)
	if err != nil {
		t.Fatalf("GetWorkerRecord(unclaimed): %v", err)
	}
	if unclaimed.State != configstore.WorkerStateHotIdle {
		t.Fatalf("expected later hot-idle worker to remain hot_idle, got %q", unclaimed.State)
	}
}

func TestClaimHotIdleWorkerReturnsNoIdleWhenNoHotIdleWorkerExists(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 14, 15, 0, 0, time.UTC)
	for _, record := range []configstore.WorkerRecord{
		{
			WorkerID:          11,
			PodName:           "duckgres-worker-other-org-hot-idle",
			State:             configstore.WorkerStateHotIdle,
			OrgID:             "billing",
			OwnerCPInstanceID: "cp-old:boot-a",
			OwnerEpoch:        2,
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          12,
			PodName:           "duckgres-worker-requested-org-idle",
			State:             configstore.WorkerStateIdle,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-old:boot-a",
			OwnerEpoch:        2,
			LastHeartbeatAt:   now,
		},
		{
			WorkerID:          13,
			PodName:           "duckgres-worker-requested-org-hot",
			State:             configstore.WorkerStateHot,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-old:boot-a",
			OwnerEpoch:        2,
			LastHeartbeatAt:   now,
		},
	} {
		if err := store.UpsertWorkerRecord(&record); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", record.WorkerID, err)
		}
	}

	claimed, missReason, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 0)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonNoIdle {
		t.Fatalf("expected no-idle miss reason, got %q", missReason)
	}

	for _, workerID := range []int{11, 12, 13} {
		persisted, err := store.GetWorkerRecord(workerID)
		if err != nil {
			t.Fatalf("GetWorkerRecord(%d): %v", workerID, err)
		}
		if persisted.OwnerCPInstanceID != "cp-old:boot-a" {
			t.Fatalf("expected worker %d owner to remain unchanged, got %q", workerID, persisted.OwnerCPInstanceID)
		}
		if persisted.OwnerEpoch != 2 {
			t.Fatalf("expected worker %d owner epoch to remain 2, got %d", workerID, persisted.OwnerEpoch)
		}
	}
}

func TestClaimHotIdleWorkerRespectsOrgCapPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 14, 20, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          14,
		PodName:           "duckgres-worker-active-org-slot",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-active:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(active): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          15,
		PodName:           "duckgres-worker-hot-idle-over-cap",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot-idle): %v", err)
	}

	claimed, missReason, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 1)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected org cap to block hot-idle claim, got %#v", claimed)
	}
	if missReason != configstore.WorkerClaimMissReasonOrgCap {
		t.Fatalf("expected org-cap miss reason, got %q", missReason)
	}

	persisted, err := store.GetWorkerRecord(15)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHotIdle {
		t.Fatalf("expected hot-idle worker to remain hot_idle, got %q", persisted.State)
	}
	if persisted.OwnerCPInstanceID != "cp-old:boot-a" || persisted.OwnerEpoch != 2 {
		t.Fatalf("expected hot-idle owner to remain cp-old epoch 2, got owner=%q epoch=%d", persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestClaimHotIdleWorkerAllowsOnlyHotIdleAtCapPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 14, 25, 0, 0, time.UTC)
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          16,
		PodName:           "duckgres-worker-only-hot-idle",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	claimed, missReason, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 1)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected sole hot-idle worker to be reclaimable at org cap")
		return
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected no miss reason, got %q", missReason)
	}
	if claimed.WorkerID != 16 || claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected worker 16 to be reserved, got %#v", claimed)
	}
}

func TestClaimHotIdleWorkerSerializesOrgCapPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	now := time.Date(2026, time.March, 26, 14, 30, 0, 0, time.UTC)
	for _, workerID := range []int{17, 18} {
		if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
			WorkerID:          workerID,
			PodName:           fmt.Sprintf("duckgres-worker-hot-idle-%d", workerID),
			State:             configstore.WorkerStateHotIdle,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-old:boot-a",
			OwnerEpoch:        2,
			LastHeartbeatAt:   now,
		}); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", workerID, err)
		}
	}

	first, missReason, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 1)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker(first): %v", err)
	}
	if first == nil || first.WorkerID != 17 {
		t.Fatalf("expected first claim to reserve worker 17, got %#v", first)
	}
	if missReason != configstore.WorkerClaimMissReasonNone {
		t.Fatalf("expected first claim to have no miss reason, got %q", missReason)
	}

	second, missReason, err := store.ClaimHotIdleWorker("cp-other:boot-c", "analytics", 1)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker(second): %v", err)
	}
	if second != nil {
		t.Fatalf("expected org cap to block second hot-idle claim, got %#v", second)
	}
	if missReason != configstore.WorkerClaimMissReasonOrgCap {
		t.Fatalf("expected org-cap miss reason on second claim, got %q", missReason)
	}

	persisted, err := store.GetWorkerRecord(18)
	if err != nil {
		t.Fatalf("GetWorkerRecord(18): %v", err)
	}
	if persisted.State != configstore.WorkerStateHotIdle {
		t.Fatalf("expected second hot-idle worker to remain hot_idle, got %q", persisted.State)
	}
}

func TestGetWorkerRecordReturnsNilNilForMissingRow(t *testing.T) {
	// cleanupOrphanedWorkerPods in k8s_pool.go treats (nil, nil) as "no DB
	// row — this pod is fully orphaned and safe to delete" and a non-nil
	// error as "skip this tick and retry." If GetWorkerRecord wrapped
	// gorm.ErrRecordNotFound as an error, the missing-row branch of the
	// reconciler would be unreachable in production and stranded pods with
	// no DB row would never be cleaned up. This test pins the contract.
	store := newIsolatedConfigStore(t)

	record, err := store.GetWorkerRecord(99999)
	if err != nil {
		t.Fatalf("expected (nil, nil) for missing row, got err=%v", err)
	}
	if record != nil {
		t.Fatalf("expected nil record for missing row, got %#v", record)
	}
}

func TestExpireControlPlaneInstancesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 14, 0, 0, 0, time.UTC)
	staleHeartbeat := startedAt.Add(5 * time.Second)
	freshHeartbeat := startedAt.Add(40 * time.Second)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-stale:boot-a",
		PodName:         "duckgres-stale",
		PodUID:          "pod-stale",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: staleHeartbeat,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(stale): %v", err)
	}
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-fresh:boot-b",
		PodName:         "duckgres-fresh",
		PodUID:          "pod-fresh",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       startedAt,
		LastHeartbeatAt: freshHeartbeat,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(fresh): %v", err)
	}

	expired, err := store.ExpireControlPlaneInstances(startedAt.Add(20 * time.Second))
	if err != nil {
		t.Fatalf("ExpireControlPlaneInstances: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 expired instance, got %d", expired)
	}

	stale, err := store.GetControlPlaneInstance("cp-stale:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(stale): %v", err)
	}
	if stale.State != configstore.ControlPlaneInstanceStateExpired {
		t.Fatalf("expected stale instance to be expired, got %q", stale.State)
	}
	if stale.ExpiredAt == nil {
		t.Fatal("expected expired_at to be set for stale instance")
	}

	fresh, err := store.GetControlPlaneInstance("cp-fresh:boot-b")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(fresh): %v", err)
	}
	if fresh.State != configstore.ControlPlaneInstanceStateActive {
		t.Fatalf("expected fresh instance to stay active, got %q", fresh.State)
	}
	if fresh.ExpiredAt != nil {
		t.Fatalf("expected fresh instance expired_at to stay nil, got %v", fresh.ExpiredAt)
	}
}

func TestListLiveControlPlaneInstanceIDsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 9, 12, 0, 0, 0, time.UTC)

	// Active CP — should appear.
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-active:boot-a",
		PodName:         "duckgres-active",
		PodUID:          "pod-active",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(active): %v", err)
	}
	// Draining CP — must also appear (still serving in-flight queries).
	drainingAt := now.Add(-time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining:boot-b",
		PodName:         "duckgres-draining",
		PodUID:          "pod-draining",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-30 * time.Second),
		DrainingAt:      &drainingAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(draining): %v", err)
	}
	// Expired CP — must NOT appear.
	expiredAt := now.Add(-2 * time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-expired:boot-c",
		PodName:         "duckgres-expired",
		PodUID:          "pod-expired",
		BootID:          "boot-c",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-5 * time.Minute),
		ExpiredAt:       &expiredAt,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(expired): %v", err)
	}

	ids, err := store.ListLiveControlPlaneInstanceIDs()
	if err != nil {
		t.Fatalf("ListLiveControlPlaneInstanceIDs: %v", err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["cp-active:boot-a"] {
		t.Error("expected active CP to be listed as live")
	}
	if !got["cp-draining:boot-b"] {
		t.Error("expected draining CP to be listed as live (still serving in-flight queries)")
	}
	if got["cp-expired:boot-c"] {
		t.Error("expected expired CP to NOT be listed as live")
	}
}

func TestExpireDrainingControlPlaneInstancesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	startedAt := time.Date(2026, time.March, 26, 14, 0, 0, 0, time.UTC)
	oldDrain := startedAt.Add(5 * time.Minute)
	recentDrain := startedAt.Add(20 * time.Minute)
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining-old:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       startedAt,
		LastHeartbeatAt: recentDrain,
		DrainingAt:      &oldDrain,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(old draining): %v", err)
	}
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-draining-recent:boot-b",
		PodName:         "duckgres-recent",
		PodUID:          "pod-recent",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateDraining,
		StartedAt:       startedAt,
		LastHeartbeatAt: recentDrain,
		DrainingAt:      &recentDrain,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(recent draining): %v", err)
	}

	expired, err := store.ExpireDrainingControlPlaneInstances(startedAt.Add(15 * time.Minute))
	if err != nil {
		t.Fatalf("ExpireDrainingControlPlaneInstances: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 overdue draining instance, got %d", expired)
	}

	old, err := store.GetControlPlaneInstance("cp-draining-old:boot-a")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(old draining): %v", err)
	}
	if old.State != configstore.ControlPlaneInstanceStateExpired {
		t.Fatalf("expected old draining instance to be expired, got %q", old.State)
	}
	if old.ExpiredAt == nil {
		t.Fatal("expected expired_at to be set for old draining instance")
	}

	recent, err := store.GetControlPlaneInstance("cp-draining-recent:boot-b")
	if err != nil {
		t.Fatalf("GetControlPlaneInstance(recent draining): %v", err)
	}
	if recent.State != configstore.ControlPlaneInstanceStateDraining {
		t.Fatalf("expected recent draining instance to remain draining, got %q", recent.State)
	}
	if recent.ExpiredAt != nil {
		t.Fatalf("expected recent draining instance expired_at to stay nil, got %v", recent.ExpiredAt)
	}
}

func TestCreateSpawningWorkerSlotPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	slot, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", "duckgres:test", 1, "duckgres-worker-test-cp", 3, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot: %v", err)
	}
	if slot == nil {
		t.Fatal("expected spawning worker slot to be created")
		return
	}
	if slot.WorkerID <= 0 {
		t.Fatalf("expected positive worker id, got %d", slot.WorkerID)
	}
	if slot.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected spawning state, got %q", slot.State)
	}
	if slot.PodName != "duckgres-worker-test-cp-"+strconv.Itoa(slot.WorkerID) {
		t.Fatalf("unexpected pod name %q for worker id %d", slot.PodName, slot.WorkerID)
	}
	if slot.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", slot.OwnerCPInstanceID)
	}
	if slot.OwnerEpoch != 1 {
		t.Fatalf("expected owner epoch 1, got %d", slot.OwnerEpoch)
	}
	if slot.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", slot.OrgID)
	}

	persisted, err := store.GetWorkerRecord(slot.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected persisted spawning state, got %q", persisted.State)
	}
}

func TestCreateSpawningWorkerSlotRespectsOrgAndGlobalCaps(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 13, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          9,
		PodName:           "duckgres-worker-existing-9",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(existing): %v", err)
	}

	orgLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "analytics", "duckgres:test", 1, "duckgres-worker-test-cp", 1, 5)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(org cap): %v", err)
	}
	if orgLimited != nil {
		t.Fatalf("expected org cap to block spawning, got %#v", orgLimited)
	}

	globalLimited, err := store.CreateSpawningWorkerSlot("cp-new:boot-b", "sales", "duckgres:test", 1, "duckgres-worker-test-cp", 2, 1)
	if err != nil {
		t.Fatalf("CreateSpawningWorkerSlot(global cap): %v", err)
	}
	if globalLimited != nil {
		t.Fatalf("expected global cap to block spawning, got %#v", globalLimited)
	}
}

func TestCreateNeutralWarmWorkerSlotRespectsSharedWarmTarget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 13, 30, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          10,
		PodName:           "duckgres-worker-existing-10",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(existing neutral): %v", err)
	}

	blocked, err := store.CreateNeutralWarmWorkerSlot("cp-new:boot-b", "duckgres-worker-test-cp", "duckgres:test", "", "", false, 1, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlot(shared target): %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected shared warm target to block spawning, got %#v", blocked)
	}

	slot, err := store.CreateNeutralWarmWorkerSlot("cp-new:boot-b", "duckgres-worker-test-cp", "duckgres:test", "", "", false, 2, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlot(expand target): %v", err)
	}
	if slot == nil {
		t.Fatal("expected neutral warm slot to be created")
		return
	}
	if slot.OrgID != "" {
		t.Fatalf("expected neutral warm slot org to be empty, got %q", slot.OrgID)
	}
	if slot.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected spawning state, got %q", slot.State)
	}
}

func TestCreateNeutralWarmWorkerSlotForImageEnforcesPerImageTarget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 7, 12, 0, 0, 0, time.UTC)

	// One existing warm-idle worker on a DIFFERENT image: should not block
	// spawning a fresh per-image slot for "duckgres:v1.5.1".
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          20,
		PodName:           "duckgres-worker-existing-20",
		Image:             "duckgres:v1.4.0",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "cp-a:boot-1",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(other-image): %v", err)
	}

	slot, err := store.CreateNeutralWarmWorkerSlotForImage("cp-b:boot-2", "duckgres-worker-test-cp", "duckgres:v1.5.1", 1, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlotForImage: %v", err)
	}
	if slot == nil {
		t.Fatal("expected per-image slot to be created when no warm worker for that image exists")
		return
	}
	if slot.Image != "duckgres:v1.5.1" {
		t.Fatalf("expected slot image duckgres:v1.5.1, got %q", slot.Image)
	}
	if slot.State != configstore.WorkerStateSpawning {
		t.Fatalf("expected spawning state, got %q", slot.State)
	}
	if slot.OrgID != "" {
		t.Fatalf("expected neutral org, got %q", slot.OrgID)
	}

	// Second call with the same target=1 should be a no-op — the just-spawned
	// row counts as a warm-or-spawning worker for this image.
	again, err := store.CreateNeutralWarmWorkerSlotForImage("cp-b:boot-2", "duckgres-worker-test-cp", "duckgres:v1.5.1", 1, 5)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlotForImage(repeat): %v", err)
	}
	if again != nil {
		t.Fatalf("expected per-image target to block second spawn, got %#v", again)
	}

	// Global cap still applies: with maxGlobalWorkers=2 and two existing rows
	// (the v1.4.0 idle plus the v1.5.1 spawning), a third image's request
	// must be blocked.
	capped, err := store.CreateNeutralWarmWorkerSlotForImage("cp-b:boot-2", "duckgres-worker-test-cp", "duckgres:v1.6.0", 1, 2)
	if err != nil {
		t.Fatalf("CreateNeutralWarmWorkerSlotForImage(global cap): %v", err)
	}
	if capped != nil {
		t.Fatalf("expected global cap to block third image spawn, got %#v", capped)
	}
}

func TestListOrphanedAndStuckWorkersPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-expired:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now.Add(-time.Minute),
		ExpiredAt:       ptrTime(now.Add(-time.Minute)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(expired): %v", err)
	}
	// Insert the live CP that owns worker 62 below. Without this row, the
	// dangling-owner branch of ListOrphanedWorkers would (correctly) flag
	// worker 62 as an orphan; we need it to remain in the "stuck with a
	// live owner" bucket here, since that's what ListStuckWorkers covers.
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-live:boot-b",
		PodName:         "duckgres-live",
		PodUID:          "pod-live",
		BootID:          "boot-b",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       now.Add(-time.Hour),
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(live): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          61,
		PodName:           "duckgres-worker-61",
		State:             configstore.WorkerStateReserved,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(orphaned): %v", err)
	}
	// Retired and lost rows whose owning CP is expired must NOT be returned:
	// their pods are already deleted (or will be cleaned up by the K8s label
	// scan on the next CP startup), so re-listing them would loop the janitor
	// on a 404 from the K8s pod delete forever.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          63,
		PodName:           "duckgres-worker-63",
		State:             configstore.WorkerStateRetired,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        3,
		LastHeartbeatAt:   now.Add(-time.Minute),
		RetireReason:      "normal",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(retired orphan): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          64,
		PodName:           "duckgres-worker-64",
		State:             configstore.WorkerStateLost,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-expired:boot-a",
		OwnerEpoch:        4,
		LastHeartbeatAt:   now.Add(-time.Minute),
		RetireReason:      "crash",
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(lost orphan): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          62,
		PodName:           "duckgres-worker-62",
		State:             configstore.WorkerStateActivating,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-live:boot-b",
		OwnerEpoch:        1,
		LastHeartbeatAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(stuck): %v", err)
	}
	if err := store.DB().Table(store.RuntimeSchema()+".worker_records").
		Where("worker_id = ?", 62).
		Update("updated_at", now.Add(-3*time.Minute)).Error; err != nil {
		t.Fatalf("age stuck worker: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 61 {
		t.Fatalf("expected only active orphaned worker 61, got %#v", orphaned)
	}

	stuck, err := store.ListStuckWorkers(now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("ListStuckWorkers: %v", err)
	}
	if len(stuck) != 1 || stuck[0].WorkerID != 62 {
		t.Fatalf("expected stuck worker 62, got %#v", stuck)
	}
}

// TestListOrphanedWorkersIncludesOwnerlessIdleWorkers seeds a row that
// reproduces the mw-dev incident: a neutral idle worker whose
// owner_cp_instance_id is the empty string. Today's INNER JOIN against
// the cp_instances table excludes such rows, so the orphan janitor never
// retires them, the warm-target check counts them as live capacity, and
// no new warm workers ever spawn. The fix LEFT JOINs and adds an explicit
// "ownerless and stale" branch.
func TestListOrphanedWorkersIncludesOwnerlessIdleWorkers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	// Stale ownerless idle worker — exactly the row that gets stuck today.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          77,
		PodName:           "duckgres-worker-77",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(ownerless idle): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 77 {
		t.Fatalf("expected ownerless idle worker 77 to be returned as an orphan, got %#v", orphaned)
	}
}

func TestRetireOrphanWorkerHandlesNullOwnerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 14, 5, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          101,
		PodName:           "duckgres-worker-101",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(ownerless idle): %v", err)
	}
	if err := store.DB().Table(store.RuntimeSchema()+".worker_records").
		Where("worker_id = ?", 101).
		Update("owner_cp_instance_id", nil).Error; err != nil {
		t.Fatalf("set owner_cp_instance_id null: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 101 || orphaned[0].OwnerCPInstanceID != "" {
		t.Fatalf("expected null-owner worker 101 to be listed with empty owner, got %#v", orphaned)
	}

	retired, err := store.RetireOrphanWorker(&orphaned[0], "orphaned")
	if err != nil {
		t.Fatalf("RetireOrphanWorker(null owner): %v", err)
	}
	if !retired {
		t.Fatal("expected null-owner orphan snapshot to retire")
	}
	assertWorkerStateAndReason(t, store, 101, configstore.WorkerStateRetired, "orphaned")
}

// TestListOrphanedWorkersIncludesDanglingOwnerWorkers covers the
// "owner_cp_instance_id is set but no matching cp_instances row exists"
// case — the CP row was hard-deleted (or somehow skipped insertion) but
// the worker row is still active. Same blast radius as the ownerless
// case: invisible to today's INNER JOIN.
func TestListOrphanedWorkersIncludesDanglingOwnerWorkers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          88,
		PodName:           "duckgres-worker-88",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "ghost-cp:boot-x", // no matching cp_instances row
		OwnerEpoch:        0,
		LastHeartbeatAt:   now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(dangling owner): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 88 {
		t.Fatalf("expected dangling-owner idle worker 88 to be returned as an orphan, got %#v", orphaned)
	}
}

// TestListOrphanedWorkersExcludesFreshOwnerlessWorker guards against
// over-eager cleanup. The spawn path creates the worker pod first, then
// inserts the DB row with a fresh heartbeat. There's also a brief window
// during slot creation before the owner is stamped. Either way, a freshly-
// stamped ownerless row must NOT be treated as an orphan, or we'd race
// every spawn into the orphan retirement path.
func TestListOrphanedWorkersExcludesFreshOwnerlessWorker(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          99,
		PodName:           "duckgres-worker-99",
		State:             configstore.WorkerStateIdle,
		OrgID:             "",
		OwnerCPInstanceID: "",
		OwnerEpoch:        0,
		LastHeartbeatAt:   now, // fresh
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(fresh ownerless): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 0 {
		t.Fatalf("expected fresh ownerless worker NOT to be returned as orphan, got %#v", orphaned)
	}
}

// TestRetireOrphanWorkerHandlesAllActiveStates exercises the full set of
// states that ListOrphanedWorkers can return. The existing
// RetireIdleOrHotIdleWorker only transitions idle/hot_idle; for an orphan
// stuck in spawning/reserved/activating/hot/draining, that's a no-op and
// the row stays counted as live capacity. The new RetireOrphanWorker must
// transition any of these to retired.
func TestRetireOrphanWorkerHandlesAllActiveStates(t *testing.T) {
	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"spawning", configstore.WorkerStateSpawning},
		{"idle", configstore.WorkerStateIdle},
		{"reserved", configstore.WorkerStateReserved},
		{"activating", configstore.WorkerStateActivating},
		{"hot", configstore.WorkerStateHot},
		{"hot_idle", configstore.WorkerStateHotIdle},
		{"draining", configstore.WorkerStateDraining},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			workerID := 1000 + i

			if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
				WorkerID:          workerID,
				PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
				State:             tc.state,
				OwnerCPInstanceID: "",
				LastHeartbeatAt:   time.Now().Add(-1 * time.Hour),
			}); err != nil {
				t.Fatalf("UpsertWorkerRecord(%s): %v", tc.state, err)
			}

			observed, err := store.GetWorkerRecord(workerID)
			if err != nil {
				t.Fatalf("GetWorkerRecord(%s): %v", tc.state, err)
			}

			retired, err := store.RetireOrphanWorker(observed, "test_orphan_cleanup")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s): %v", tc.state, err)
			}
			if !retired {
				t.Fatalf("expected RetireOrphanWorker(%s) to transition the row, returned false", tc.state)
			}

			// Verify final DB state via a follow-up no-op call (returns false).
			retiredAgain, err := store.RetireOrphanWorker(observed, "test_orphan_cleanup")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s) follow-up: %v", tc.state, err)
			}
			if retiredAgain {
				t.Fatalf("RetireOrphanWorker(%s) was called twice; the second call should be a no-op", tc.state)
			}
		})
	}
}

// TestRetireOrphanWorkerNoOpOnTerminalStates: terminal rows (retired/lost)
// must not be touched. They're already done; transitioning them again
// would clobber retire_reason / updated_at and could mask original
// failure data.
func TestRetireOrphanWorkerNoOpOnTerminalStates(t *testing.T) {
	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"retired", configstore.WorkerStateRetired},
		{"lost", configstore.WorkerStateLost},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			workerID := 2000 + i

			if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
				WorkerID:          workerID,
				PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
				State:             tc.state,
				OwnerCPInstanceID: "",
				LastHeartbeatAt:   time.Now().Add(-1 * time.Hour),
				RetireReason:      "original_reason",
			}); err != nil {
				t.Fatalf("UpsertWorkerRecord(%s): %v", tc.state, err)
			}

			observed, err := store.GetWorkerRecord(workerID)
			if err != nil {
				t.Fatalf("GetWorkerRecord(%s): %v", tc.state, err)
			}

			retired, err := store.RetireOrphanWorker(observed, "should_be_ignored")
			if err != nil {
				t.Fatalf("RetireOrphanWorker(%s): %v", tc.state, err)
			}
			if retired {
				t.Fatalf("RetireOrphanWorker on terminal %s should be a no-op, but returned true", tc.state)
			}
		})
	}
}

// TestListWorkersDueForCredentialRefreshScopesByOwner: only workers owned
// by the calling CP are returned. Workers owned by another CP — even if
// their creds have expired — are that CP's problem.
func TestListWorkersDueForCredentialRefreshScopesByOwner(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-1 * time.Hour)

	mine := &configstore.WorkerRecord{
		WorkerID:               1,
		PodName:                "duckgres-worker-1",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             3,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &expired,
	}
	other := &configstore.WorkerRecord{
		WorkerID:               2,
		PodName:                "duckgres-worker-2",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-other:boot-b",
		OwnerEpoch:             5,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &expired,
	}
	for _, w := range []*configstore.WorkerRecord{mine, other} {
		if err := store.UpsertWorkerRecord(w); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", w.WorkerID, err)
		}
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 1 || due[0].WorkerID != 1 {
		t.Fatalf("expected only worker 1 (mine), got %#v", due)
	}
}

// TestListWorkersDueForCredentialRefreshTreatsNullAsDue: a row with
// s3_credentials_expires_at = NULL needs immediate refresh. This covers
// pre-migration rows and any path where activation forgot to stamp.
func TestListWorkersDueForCredentialRefreshTreatsNullAsDue(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          7,
		PodName:           "duckgres-worker-7",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-me:boot-a",
		OwnerEpoch:        1,
		LastHeartbeatAt:   now,
		// S3CredentialsExpiresAt deliberately left nil
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 1 || due[0].WorkerID != 7 {
		t.Fatalf("expected worker 7 (NULL expiry treated as due), got %#v", due)
	}
}

// TestListWorkersDueForCredentialRefreshSkipsReservedAndActivatingRows
// protects the initial activation path. Reserved/activating rows are org-bound
// before the first ActivateTenant RPC has finished and before the activation
// path stamps s3_credentials_expires_at. If the refresh scheduler treats those
// NULL-expiry rows as due, it can bump owner_epoch under the in-flight
// activation and cause the worker to reject the original owner_epoch as stale.
func TestListWorkersDueForCredentialRefreshSkipsReservedAndActivatingRows(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	pastDue := now.Add(-1 * time.Minute)

	rows := []*configstore.WorkerRecord{
		{
			WorkerID: 8, PodName: "duckgres-worker-8",
			State: configstore.WorkerStateReserved, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now,
		},
		{
			WorkerID: 9, PodName: "duckgres-worker-9",
			State: configstore.WorkerStateActivating, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
		},
		{
			WorkerID: 10, PodName: "duckgres-worker-10",
			State: configstore.WorkerStateHot, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
		},
	}
	for _, w := range rows {
		if err := store.UpsertWorkerRecord(w); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", w.WorkerID, err)
		}
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 1 || due[0].WorkerID != 10 {
		t.Fatalf("expected only activated hot worker 10 to be due, got %#v", due)
	}
}

// TestListWorkersDueForCredentialRefreshSkipsHealthyAndNeutral:
//   - Healthy row (expiry comfortably in the future): not returned.
//   - Neutral warm row (org_id=”): not returned regardless of expiry.
//     A pre-activation worker has no STS creds to refresh.
//   - Terminal row (retired): not returned.
func TestListWorkersDueForCredentialRefreshSkipsHealthyAndNeutral(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	farFuture := now.Add(2 * time.Hour)
	pastDue := now.Add(-1 * time.Minute)

	rows := []*configstore.WorkerRecord{
		{
			WorkerID: 10, PodName: "duckgres-worker-10",
			State: configstore.WorkerStateHot, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &farFuture,
		},
		{
			WorkerID: 11, PodName: "duckgres-worker-11",
			State: configstore.WorkerStateIdle, OrgID: "",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
		},
		{
			WorkerID: 12, PodName: "duckgres-worker-12",
			State: configstore.WorkerStateRetired, OrgID: "acme",
			OwnerCPInstanceID: "cp-me:boot-a", OwnerEpoch: 1,
			LastHeartbeatAt: now, S3CredentialsExpiresAt: &pastDue,
			RetireReason: "normal",
		},
	}
	for _, w := range rows {
		if err := store.UpsertWorkerRecord(w); err != nil {
			t.Fatalf("UpsertWorkerRecord(%d): %v", w.WorkerID, err)
		}
	}

	due, err := store.ListWorkersDueForCredentialRefresh("cp-me:boot-a", now)
	if err != nil {
		t.Fatalf("ListWorkersDueForCredentialRefresh: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("expected no rows returned (healthy / neutral / terminal), got %#v", due)
	}
}

// TestMarkCredentialsRefreshedSucceeds: happy path — same owner, same
// epoch, write goes through and returns true.
func TestMarkCredentialsRefreshedSucceeds(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	oldExpiry := now.Add(-1 * time.Minute)
	newExpiry := now.Add(1 * time.Hour)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:               20,
		PodName:                "duckgres-worker-20",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             3,
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &oldExpiry,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(20, "cp-me:boot-a", 3, newExpiry)
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if !updated {
		t.Fatalf("expected MarkCredentialsRefreshed to update the row, got false")
	}

	persisted, err := store.GetWorkerRecord(20)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.S3CredentialsExpiresAt == nil || !persisted.S3CredentialsExpiresAt.Equal(newExpiry) {
		t.Fatalf("expected expires_at = %v, got %v", newExpiry, persisted.S3CredentialsExpiresAt)
	}
}

// TestMarkCredentialsRefreshedFailsOnEpochMismatch: another CP took over
// the worker (bumped owner_epoch), and our just-minted creds are stale —
// the conditional update returns false rather than overwriting.
func TestMarkCredentialsRefreshedFailsOnEpochMismatch(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	oldExpiry := now.Add(-1 * time.Minute)
	originalExpiryRow := oldExpiry

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:               21,
		PodName:                "duckgres-worker-21",
		State:                  configstore.WorkerStateHot,
		OrgID:                  "acme",
		OwnerCPInstanceID:      "cp-me:boot-a",
		OwnerEpoch:             7, // newer than the caller will pass
		LastHeartbeatAt:        now,
		S3CredentialsExpiresAt: &originalExpiryRow,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(21, "cp-me:boot-a", 6, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if updated {
		t.Fatalf("expected stale-epoch caller to be rejected, got updated=true")
	}

	persisted, err := store.GetWorkerRecord(21)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.S3CredentialsExpiresAt == nil || !persisted.S3CredentialsExpiresAt.Equal(originalExpiryRow) {
		t.Fatalf("expected expires_at to be unchanged after epoch mismatch, got %v", persisted.S3CredentialsExpiresAt)
	}
}

// TestMarkCredentialsRefreshedFailsOnOwnerMismatch: another CP took
// ownership entirely. Same conditional-update protection.
func TestMarkCredentialsRefreshedFailsOnOwnerMismatch(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          22,
		PodName:           "duckgres-worker-22",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-other:boot-b",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	updated, err := store.MarkCredentialsRefreshed(22, "cp-me:boot-a", 2, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("MarkCredentialsRefreshed: %v", err)
	}
	if updated {
		t.Fatalf("expected wrong-owner caller to be rejected, got updated=true")
	}
}

// TestListOrphanedWorkersExcludesWorkersWithActiveFlightSessions: a
// worker whose owning CP has expired is normally an orphan-cleanup
// candidate. But if a Flight client could still reconnect by session
// token (record is in active or reconnecting state), the orphan retire
// would kill an in-flight customer query the moment they reconnect. The
// JOIN onto flight_session_records gives those workers a reprieve until
// the session record itself is expired by ExpireFlightSessionRecords.
func TestListOrphanedWorkersExcludesWorkersWithActiveFlightSessions(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 14, 0, 0, 0, time.UTC)

	// Owner CP is expired long ago — easily past the 30s orphan grace.
	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-old:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-2 * time.Hour),
		LastHeartbeatAt: now.Add(-1 * time.Hour),
		ExpiredAt:       ptrTime(now.Add(-1 * time.Hour)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance: %v", err)
	}

	// A hot worker owned by that expired CP — exactly the shape that
	// today's orphan janitor would retire. With Layer 3, the active Flight
	// session below should spare it.
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          501,
		PodName:           "duckgres-worker-501",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-30 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	// Reclaimable Flight session pointing at that worker.
	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "tok-reclaim-501",
		Username:     "postgres",
		OrgID:        "acme",
		WorkerID:     501,
		OwnerEpoch:   2,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    now.Add(30 * time.Minute), // not yet expired
		LastSeenAt:   now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	for _, w := range orphaned {
		if w.WorkerID == 501 {
			t.Fatalf("worker 501 has an active Flight session; orphan janitor must spare it (got %#v)", orphaned)
		}
	}
}

// TestListOrphanedWorkersIncludesWorkersWithReconnectingFlightSessions:
// the reconnecting state means a customer is mid-handshake from a Flight
// client picking the session back up. Same protection applies — kill the
// worker and you kill the resumption.
func TestListOrphanedWorkersIncludesWorkersWithReconnectingFlightSessions(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-old:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-2 * time.Hour),
		LastHeartbeatAt: now.Add(-1 * time.Hour),
		ExpiredAt:       ptrTime(now.Add(-1 * time.Hour)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance: %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          502,
		PodName:           "duckgres-worker-502",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-30 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}
	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "tok-reconnect-502",
		Username:     "postgres",
		OrgID:        "acme",
		WorkerID:     502,
		OwnerEpoch:   2,
		State:        configstore.FlightSessionStateReconnecting,
		ExpiresAt:    now.Add(30 * time.Minute),
		LastSeenAt:   now.Add(-1 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	for _, w := range orphaned {
		if w.WorkerID == 502 {
			t.Fatalf("worker 502 has a Flight session in reconnecting state; orphan janitor must spare it (got %#v)", orphaned)
		}
	}
}

// TestListOrphanedWorkersIncludesWorkersWithExpiredFlightSessions: once
// the Flight session record has been moved to a terminal state (expired
// or closed), the customer can no longer reclaim. The worker should
// then be retired by the orphan janitor like any other unowned row.
// Without this, a worker would linger forever once its session expired
// and the orphan list filtered it out forever.
func TestListOrphanedWorkersIncludesWorkersWithExpiredFlightSessions(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.April, 30, 14, 0, 0, 0, time.UTC)

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-old:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-2 * time.Hour),
		LastHeartbeatAt: now.Add(-1 * time.Hour),
		ExpiredAt:       ptrTime(now.Add(-1 * time.Hour)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance: %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          503,
		PodName:           "duckgres-worker-503",
		State:             configstore.WorkerStateHot,
		OrgID:             "acme",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-30 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}
	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "tok-gone-503",
		Username:     "postgres",
		OrgID:        "acme",
		WorkerID:     503,
		OwnerEpoch:   2,
		State:        configstore.FlightSessionStateExpired,
		ExpiresAt:    now.Add(-10 * time.Minute),
		LastSeenAt:   now.Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	found := false
	for _, w := range orphaned {
		if w.WorkerID == 503 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("worker 503's Flight session is expired; orphan janitor MUST return it for retirement (got %#v)", orphaned)
	}
}

func TestExpireFlightSessionRecordsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 15, 0, 0, 0, time.UTC)

	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-expire-me",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   7,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    now.Add(-time.Minute),
		LastSeenAt:   now.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	expired, err := store.ExpireFlightSessionRecords(now)
	if err != nil {
		t.Fatalf("ExpireFlightSessionRecords: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected one expired session record, got %d", expired)
	}

	record, err := store.GetFlightSessionRecord("flight-expire-me")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record.State != configstore.FlightSessionStateExpired {
		t.Fatalf("expected expired flight session state, got %q", record.State)
	}
}

func TestGetTouchAndCloseFlightSessionRecordPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 16, 0, 0, 0, time.UTC)

	if err := store.UpsertFlightSessionRecord(&configstore.FlightSessionRecord{
		SessionToken: "flight-touch-close",
		Username:     "postgres",
		OrgID:        "analytics",
		WorkerID:     42,
		OwnerEpoch:   8,
		State:        configstore.FlightSessionStateActive,
		ExpiresAt:    now.Add(time.Hour),
		LastSeenAt:   now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("UpsertFlightSessionRecord: %v", err)
	}

	record, err := store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record == nil || record.Username != "postgres" {
		t.Fatalf("expected durable record with username postgres, got %#v", record)
	}

	touchedAt := now.Add(2 * time.Minute)
	if err := store.TouchFlightSessionRecord("flight-touch-close", touchedAt); err != nil {
		t.Fatalf("TouchFlightSessionRecord: %v", err)
	}
	record, err = store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if !record.LastSeenAt.Equal(touchedAt) {
		t.Fatalf("expected last_seen_at %v, got %v", touchedAt, record.LastSeenAt)
	}

	closedAt := now.Add(3 * time.Minute)
	if err := store.CloseFlightSessionRecord("flight-touch-close", closedAt); err != nil {
		t.Fatalf("CloseFlightSessionRecord: %v", err)
	}
	record, err = store.GetFlightSessionRecord("flight-touch-close")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record.State != configstore.FlightSessionStateClosed {
		t.Fatalf("expected closed state, got %q", record.State)
	}
	if !record.LastSeenAt.Equal(closedAt) {
		t.Fatalf("expected close timestamp %v, got %v", closedAt, record.LastSeenAt)
	}
}

func TestGetFlightSessionRecordReturnsNilWhenMissing(t *testing.T) {
	store := newIsolatedConfigStore(t)

	record, err := store.GetFlightSessionRecord("missing-flight-session")
	if err != nil {
		t.Fatalf("GetFlightSessionRecord: %v", err)
	}
	if record != nil {
		t.Fatalf("expected nil record for missing session, got %#v", record)
	}
}

func TestMarkWorkerLostIfCurrentLeasePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 12, 0, 0, 0, time.UTC)

	liveStates := []configstore.WorkerState{
		configstore.WorkerStateSpawning,
		configstore.WorkerStateIdle,
		configstore.WorkerStateReserved,
		configstore.WorkerStateActivating,
		configstore.WorkerStateHot,
		configstore.WorkerStateHotIdle,
	}
	for i, state := range liveStates {
		t.Run(string(state), func(t *testing.T) {
			workerID := 3100 + i
			upsertMarkLostWorker(t, store, workerID, state, "cp-me:boot-a", 4, "", now)

			updated, err := store.MarkWorkerLostIfCurrentLease(workerID, "cp-me:boot-a", 4, "crash")
			if err != nil {
				t.Fatalf("MarkWorkerLostIfCurrentLease(%s): %v", state, err)
			}
			if !updated {
				t.Fatalf("expected current %s lease to mark worker lost", state)
			}
			assertWorkerStateAndReason(t, store, workerID, configstore.WorkerStateLost, "crash")
		})
	}

	upsertMarkLostWorker(t, store, 3200, configstore.WorkerStateHot, "cp-other:boot-b", 9, "", now)
	updated, err := store.MarkWorkerLostIfCurrentLease(3200, "cp-me:boot-a", 9, "crash")
	if err != nil {
		t.Fatalf("MarkWorkerLostIfCurrentLease owner mismatch: %v", err)
	}
	if updated {
		t.Fatal("expected owner mismatch to return false")
	}
	assertWorkerStateAndReason(t, store, 3200, configstore.WorkerStateHot, "")

	upsertMarkLostWorker(t, store, 3201, configstore.WorkerStateHot, "cp-me:boot-a", 4, "", now)
	updated, err = store.MarkWorkerLostIfCurrentLease(3201, "cp-me:boot-a", 3, "crash")
	if err != nil {
		t.Fatalf("MarkWorkerLostIfCurrentLease epoch mismatch: %v", err)
	}
	if updated {
		t.Fatal("expected epoch mismatch to return false")
	}
	assertWorkerStateAndReason(t, store, 3201, configstore.WorkerStateHot, "")

	noOpStates := []configstore.WorkerState{
		configstore.WorkerStateDraining,
		configstore.WorkerStateRetired,
		configstore.WorkerStateLost,
	}
	for i, state := range noOpStates {
		t.Run("noop_"+string(state), func(t *testing.T) {
			workerID := 3300 + i
			upsertMarkLostWorker(t, store, workerID, state, "cp-me:boot-a", 4, "original", now)

			updated, err := store.MarkWorkerLostIfCurrentLease(workerID, "cp-me:boot-a", 4, "crash")
			if err != nil {
				t.Fatalf("MarkWorkerLostIfCurrentLease(%s): %v", state, err)
			}
			if updated {
				t.Fatalf("expected %s worker to remain owned by its current lifecycle path", state)
			}
			assertWorkerStateAndReason(t, store, workerID, state, "original")
		})
	}
}

func TestUpsertWorkerRecordDoesNotResurrectTerminalWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 12, 30, 0, 0, time.UTC)

	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"draining", configstore.WorkerStateDraining},
		{"lost", configstore.WorkerStateLost},
		{"retired", configstore.WorkerStateRetired},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workerID := 3400 + i
			upsertMarkLostWorker(t, store, workerID, tc.state, "cp-me:boot-a", 4, "original", now)

			err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
				WorkerID:          workerID,
				PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
				State:             configstore.WorkerStateHot,
				OrgID:             "acme",
				OwnerCPInstanceID: "cp-me:boot-a",
				OwnerEpoch:        5,
				RetireReason:      "",
				LastHeartbeatAt:   now.Add(time.Minute),
			})
			if !errors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
				t.Fatalf("expected fenced terminal upsert for worker %d, got %v", workerID, err)
			}

			assertWorkerStateAndReason(t, store, workerID, tc.state, "original")
		})
	}
}

func TestUpsertWorkerRecordDoesNotOverwriteNewerLeasePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 0, 0, 0, time.UTC)

	insertCurrent := func(workerID int) {
		t.Helper()
		if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
			WorkerID:          workerID,
			PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
			State:             configstore.WorkerStateReserved,
			OrgID:             "analytics",
			OwnerCPInstanceID: "cp-new:boot-b",
			OwnerEpoch:        10,
			LastHeartbeatAt:   now,
		}); err != nil {
			t.Fatalf("UpsertWorkerRecord(current %d): %v", workerID, err)
		}
	}

	insertCurrent(3600)
	err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3600,
		PodName:           "duckgres-worker-3600",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        9,
		LastHeartbeatAt:   now.Add(time.Minute),
	})
	if !errors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
		t.Fatalf("expected stale upsert fence miss, got %v", err)
	}

	persisted, err := store.GetWorkerRecord(3600)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateReserved ||
		persisted.OwnerCPInstanceID != "cp-new:boot-b" ||
		persisted.OwnerEpoch != 10 {
		t.Fatalf("stale upsert overwrote newer lease: state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}

	insertCurrent(3606)
	err = store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3606,
		PodName:           "duckgres-worker-3606",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        10,
		LastHeartbeatAt:   now.Add(time.Minute),
	})
	if !errors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
		t.Fatalf("expected equal-epoch different-owner upsert fence miss, got %v", err)
	}

	persisted, err = store.GetWorkerRecord(3606)
	if err != nil {
		t.Fatalf("GetWorkerRecord(equal epoch): %v", err)
	}
	if persisted.State != configstore.WorkerStateReserved ||
		persisted.OwnerCPInstanceID != "cp-new:boot-b" ||
		persisted.OwnerEpoch != 10 {
		t.Fatalf("equal-epoch different-owner upsert overwrote lease: state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestRetireOrphanWorkerRejectsRevivedOwnerControlPlanePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 10, 0, 0, time.UTC)

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-revived:boot-a",
		PodName:         "duckgres-old",
		PodUID:          "pod-old",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now.Add(-2 * time.Hour),
		LastHeartbeatAt: now.Add(-time.Hour),
		ExpiredAt:       ptrTime(now.Add(-time.Hour)),
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(expired): %v", err)
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3604,
		PodName:           "duckgres-worker-3604",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-revived:boot-a",
		OwnerEpoch:        3,
		LastHeartbeatAt:   now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(orphan candidate): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 3604 {
		t.Fatalf("expected worker 3604 orphan snapshot, got %#v", orphaned)
	}

	if err := store.UpsertControlPlaneInstance(&configstore.ControlPlaneInstance{
		ID:              "cp-revived:boot-a",
		PodName:         "duckgres-new",
		PodUID:          "pod-new",
		BootID:          "boot-a",
		State:           configstore.ControlPlaneInstanceStateActive,
		StartedAt:       now.Add(-2 * time.Hour),
		LastHeartbeatAt: now,
		ExpiredAt:       nil,
	}); err != nil {
		t.Fatalf("UpsertControlPlaneInstance(revived): %v", err)
	}

	retired, err := store.RetireOrphanWorker(&orphaned[0], "orphaned")
	if err != nil {
		t.Fatalf("RetireOrphanWorker(revived CP): %v", err)
	}
	if retired {
		t.Fatal("expected revived owner CP to fence orphan retirement")
	}

	persisted, err := store.GetWorkerRecord(3604)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHot || persisted.OwnerCPInstanceID != "cp-revived:boot-a" || persisted.OwnerEpoch != 3 {
		t.Fatalf("expected revived CP's worker to survive, got state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestRetireOrphanWorkerRejectsStaleListSnapshotAfterTakeoverPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 15, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3601,
		PodName:           "duckgres-worker-3601",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        5,
		LastHeartbeatAt:   now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(orphan candidate): %v", err)
	}

	orphaned, err := store.ListOrphanedWorkers(now.Add(-30 * time.Second))
	if err != nil {
		t.Fatalf("ListOrphanedWorkers: %v", err)
	}
	if len(orphaned) != 1 || orphaned[0].WorkerID != 3601 {
		t.Fatalf("expected worker 3601 orphan snapshot, got %#v", orphaned)
	}

	taken, err := store.TakeOverWorker(3601, "cp-new:boot-b", "analytics", 5)
	if err != nil {
		t.Fatalf("TakeOverWorker: %v", err)
	}
	if taken == nil {
		t.Fatal("expected takeover to succeed")
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3601,
		PodName:           "duckgres-worker-3601",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-new:boot-b",
		OwnerEpoch:        taken.OwnerEpoch,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(new owner hot): %v", err)
	}

	retired, err := store.RetireOrphanWorker(&orphaned[0], "orphaned")
	if err != nil {
		t.Fatalf("RetireOrphanWorker(stale snapshot): %v", err)
	}
	if retired {
		t.Fatal("expected stale orphan snapshot not to retire a newer lease")
	}

	persisted, err := store.GetWorkerRecord(3601)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHot ||
		persisted.OwnerCPInstanceID != "cp-new:boot-b" ||
		persisted.OwnerEpoch != 6 {
		t.Fatalf("expected newer hot lease to survive, got state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestRetireHotIdleWorkerRejectsStaleListSnapshotAfterReclaimPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 30, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3602,
		PodName:           "duckgres-worker-3602",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot-idle): %v", err)
	}

	expired, err := store.ListExpiredHotIdleWorkers(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredHotIdleWorkers: %v", err)
	}
	if len(expired) != 1 || expired[0].WorkerID != 3602 {
		t.Fatalf("expected worker 3602 hot-idle snapshot, got %#v", expired)
	}

	claimed, _, err := store.ClaimHotIdleWorker("cp-new:boot-b", "analytics", 0)
	if err != nil {
		t.Fatalf("ClaimHotIdleWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected hot-idle reclaim to succeed")
	}
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3602,
		PodName:           "duckgres-worker-3602",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-new:boot-b",
		OwnerEpoch:        claimed.OwnerEpoch,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(released hot-idle): %v", err)
	}

	retired, err := store.RetireHotIdleWorker(&expired[0])
	if err != nil {
		t.Fatalf("RetireHotIdleWorker(stale snapshot): %v", err)
	}
	if retired {
		t.Fatal("expected stale hot-idle snapshot not to retire a reclaimed worker")
	}

	persisted, err := store.GetWorkerRecord(3602)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHotIdle ||
		persisted.OwnerCPInstanceID != "cp-new:boot-b" ||
		persisted.OwnerEpoch != 3 {
		t.Fatalf("expected reclaimed hot-idle lease to survive, got state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestRetireHotIdleWorkerRejectsStaleListSnapshotAfterTouchPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 35, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          3607,
		PodName:           "duckgres-worker-3607",
		State:             configstore.WorkerStateHotIdle,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        2,
		LastHeartbeatAt:   now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(hot-idle): %v", err)
	}

	expired, err := store.ListExpiredHotIdleWorkers(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ListExpiredHotIdleWorkers: %v", err)
	}
	if len(expired) != 1 || expired[0].WorkerID != 3607 {
		t.Fatalf("expected worker 3607 hot-idle snapshot, got %#v", expired)
	}

	advancedUpdatedAt := expired[0].UpdatedAt.Add(time.Second)
	if err := store.DB().Table(store.RuntimeSchema()+".worker_records").
		Where("worker_id = ?", 3607).
		Update("updated_at", advancedUpdatedAt).Error; err != nil {
		t.Fatalf("advance worker updated_at: %v", err)
	}

	retired, err := store.RetireHotIdleWorker(&expired[0])
	if err != nil {
		t.Fatalf("RetireHotIdleWorker(stale updated_at): %v", err)
	}
	if retired {
		t.Fatal("expected stale hot-idle snapshot not to retire after same-owner touch")
	}

	persisted, err := store.GetWorkerRecord(3607)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHotIdle ||
		persisted.OwnerCPInstanceID != "cp-old:boot-a" ||
		persisted.OwnerEpoch != 2 {
		t.Fatalf("expected touched hot-idle lease to survive, got state=%q owner=%q epoch=%d", persisted.State, persisted.OwnerCPInstanceID, persisted.OwnerEpoch)
	}
}

func TestRetireHotIdleWorkerNoOpOnNonHotIdleSnapshotPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 40, 0, 0, time.UTC)

	upsertMarkLostWorker(t, store, 3604, configstore.WorkerStateHot, "cp-me:boot-a", 4, "", now)
	observed, err := store.GetWorkerRecord(3604)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}

	retired, err := store.RetireHotIdleWorker(observed)
	if err != nil {
		t.Fatalf("RetireHotIdleWorker(non-hot-idle): %v", err)
	}
	if retired {
		t.Fatal("expected non-hot-idle snapshot not to retire through RetireHotIdleWorker")
	}
	assertWorkerStateAndReason(t, store, 3604, configstore.WorkerStateHot, "")
}

func TestMarkWorkerDrainingRejectsStaleSameCPLeasePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 45, 0, 0, time.UTC)

	upsertMarkLostWorker(t, store, 3603, configstore.WorkerStateHot, "cp-me:boot-a", 4, "", now)
	upsertMarkLostWorker(t, store, 3603, configstore.WorkerStateHot, "cp-me:boot-a", 5, "", now.Add(time.Minute))

	draining, err := store.MarkWorkerDraining(3603, "cp-me:boot-a", 4)
	if err != nil {
		t.Fatalf("MarkWorkerDraining(stale epoch): %v", err)
	}
	if draining {
		t.Fatal("expected stale same-CP epoch not to mark worker draining")
	}

	persisted, err := store.GetWorkerRecord(3603)
	if err != nil {
		t.Fatalf("GetWorkerRecord: %v", err)
	}
	if persisted.State != configstore.WorkerStateHot || persisted.OwnerEpoch != 5 {
		t.Fatalf("expected newer hot lease to survive, got state=%q epoch=%d", persisted.State, persisted.OwnerEpoch)
	}
}

func TestRetireDrainingWorkerRejectsStaleSameCPLeasePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 13, 50, 0, 0, time.UTC)

	upsertMarkLostWorker(t, store, 3605, configstore.WorkerStateDraining, "cp-me:boot-a", 5, "", now)

	retired, err := store.RetireDrainingWorker(3605, "cp-me:boot-a", 4, "shutdown")
	if err != nil {
		t.Fatalf("RetireDrainingWorker(stale epoch): %v", err)
	}
	if retired {
		t.Fatal("expected stale same-CP epoch not to retire draining worker")
	}
	assertWorkerStateAndReason(t, store, 3605, configstore.WorkerStateDraining, "")
}

func upsertMarkLostWorker(t *testing.T, store *configstore.ConfigStore, workerID int, state configstore.WorkerState, ownerCPInstanceID string, ownerEpoch int64, retireReason string, lastHeartbeatAt time.Time) {
	t.Helper()
	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          workerID,
		PodName:           fmt.Sprintf("duckgres-worker-%d", workerID),
		State:             state,
		OrgID:             "acme",
		OwnerCPInstanceID: ownerCPInstanceID,
		OwnerEpoch:        ownerEpoch,
		RetireReason:      retireReason,
		LastHeartbeatAt:   lastHeartbeatAt,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord(%d): %v", workerID, err)
	}
}

func assertWorkerStateAndReason(t *testing.T, store *configstore.ConfigStore, workerID int, wantState configstore.WorkerState, wantReason string) {
	t.Helper()
	record, err := store.GetWorkerRecord(workerID)
	if err != nil {
		t.Fatalf("GetWorkerRecord(%d): %v", workerID, err)
	}
	if record == nil {
		t.Fatalf("expected worker %d to exist", workerID)
	}
	if record.State != wantState || record.RetireReason != wantReason {
		t.Fatalf("worker %d = state %q reason %q, want state %q reason %q", workerID, record.State, record.RetireReason, wantState, wantReason)
	}
}

func TestTakeOverWorkerPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.March, 27, 17, 0, 0, 0, time.UTC)

	if err := store.UpsertWorkerRecord(&configstore.WorkerRecord{
		WorkerID:          71,
		PodName:           "duckgres-worker-71",
		State:             configstore.WorkerStateHot,
		OrgID:             "analytics",
		OwnerCPInstanceID: "cp-old:boot-a",
		OwnerEpoch:        5,
		LastHeartbeatAt:   now,
	}); err != nil {
		t.Fatalf("UpsertWorkerRecord: %v", err)
	}

	claimed, err := store.TakeOverWorker(71, "cp-new:boot-b", "analytics", 5)
	if err != nil {
		t.Fatalf("TakeOverWorker: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected takeover to succeed")
		return
	}
	if claimed.OwnerCPInstanceID != "cp-new:boot-b" {
		t.Fatalf("expected owner cp-instance cp-new:boot-b, got %q", claimed.OwnerCPInstanceID)
	}
	if claimed.OwnerEpoch != 6 {
		t.Fatalf("expected owner epoch 6, got %d", claimed.OwnerEpoch)
	}
	if claimed.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved state, got %q", claimed.State)
	}

	missed, err := store.TakeOverWorker(71, "cp-third:boot-c", "analytics", 5)
	if err == nil {
		t.Fatal("expected stale takeover attempt to return an epoch mismatch error")
	}
	if !errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
		t.Fatalf("expected ErrWorkerOwnerEpochMismatch, got %v", err)
	}
	if missed != nil {
		t.Fatalf("expected stale takeover attempt to fail, got %#v", missed)
	}
}

func TestTakeOverWorkerSkipsNonReclaimableStatesPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	now := time.Date(2026, time.May, 22, 12, 45, 0, 0, time.UTC)

	cases := []struct {
		name  string
		state configstore.WorkerState
	}{
		{"draining", configstore.WorkerStateDraining},
		{"retired", configstore.WorkerStateRetired},
		{"lost", configstore.WorkerStateLost},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workerID := 3500 + i
			upsertMarkLostWorker(t, store, workerID, tc.state, "cp-old:boot-a", 5, "original", now)

			claimed, err := store.TakeOverWorker(workerID, "cp-new:boot-b", "analytics", 5)
			if err != nil {
				t.Fatalf("TakeOverWorker(%s): %v", tc.state, err)
			}
			if claimed != nil {
				t.Fatalf("expected %s worker not to be claimed, got %#v", tc.state, claimed)
			}
			assertWorkerStateAndReason(t, store, workerID, tc.state, "original")
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
