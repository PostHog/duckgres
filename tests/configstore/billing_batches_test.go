//go:build linux || darwin

package configstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	cs "github.com/posthog/duckgres/controlplane/configstore"
)

func billingPrincipal(t *testing.T, store *cs.ConfigStore, principal, org string) {
	t.Helper()
	if err := store.RememberTrinoUsagePrincipals([]cs.TrinoEnabledOrg{{OrgID: org, DatabaseName: principal}}); err != nil {
		t.Fatal(err)
	}
}

func billingEvent(query string, n int64) cs.QueryUsageEvent {
	return cs.QueryUsageEvent{ClusterID: "test-cluster", QueryID: query, Principal: "warehouse", State: "FINISHED", CompletedAt: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), PhysicalInputBytes: n, StatisticsComplete: true}
}

func TestBillingBatchesRetainDeduplicateAndBillEveryOutcome(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	billingPrincipal(t, store, "warehouse", "org-a")
	for i, state := range []string{"FINISHED", "FAILED", "CANCELED"} {
		event := billingEvent(state, math.MaxInt64)
		event.State = state
		for attempt := 0; attempt < 2; attempt++ {
			inserted, err := store.RecordTrinoQueryUsage(ctx, event)
			if err != nil || inserted != (attempt == 0) {
				t.Fatalf("record %d/%d: inserted=%v err=%v", i, attempt, inserted, err)
			}
		}
	}
	batch, err := store.NextBillingBatch(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || len(batch.Scans) != 1 || batch.Scans[0].BytesScanned != json.Number("27670116110564327421") || batch.Scans[0].QueryCount != 3 {
		t.Fatalf("batch: %+v", batch)
	}
	if batch.BillingMonth != time.Now().UTC().Format("2006-01") {
		t.Fatalf("billing month %q", batch.BillingMonth)
	}
	repeated, err := store.NextBillingBatch(ctx, 1)
	if err != nil || repeated.ID != batch.ID {
		t.Fatalf("repeat: %+v %v", repeated, err)
	}
	if err := store.AckBillingBatch(ctx, "unknown"); !errors.Is(err, cs.ErrBillingBatchNotFound) {
		t.Fatalf("unknown ack: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
	}
	empty, err := store.NextBillingBatch(ctx, 100)
	if err != nil || empty != nil {
		t.Fatalf("empty: %+v %v", empty, err)
	}
	var count int64
	store.DB().Table("duckgres_trino_query_usage").Count(&count)
	if count != 3 {
		t.Fatalf("usage deleted: %d", count)
	}
	if inserted, err := store.RecordTrinoQueryUsage(ctx, billingEvent("FAILED", 1)); err != nil || inserted {
		t.Fatalf("dedup after ack: %v %v", inserted, err)
	}
}

func TestBillingBatchesStorageDeltaAndLateQueries(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	bucket := time.Now().UTC().Truncate(time.Minute)
	if err := store.UpsertStorageSample("org-a", 1, bucket, 1<<30); err != nil {
		t.Fatal(err)
	}
	batch, err := store.NextBillingBatch(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Storage) != 1 || batch.Storage[0].GiBSeconds != "1" {
		t.Fatalf("storage %+v", batch)
	}
	if err := store.UpsertStorageSample("org-a", 1, bucket, 2<<30); err != nil {
		t.Fatal(err)
	}
	billingPrincipal(t, store, "warehouse", "org-a")
	if _, err := store.RecordTrinoQueryUsage(ctx, billingEvent("late", 10)); err != nil {
		t.Fatal(err)
	}
	repeat, err := store.NextBillingBatch(ctx, 100)
	if err != nil || repeat.ID != batch.ID || len(repeat.Scans) != 0 || repeat.Storage[0].GiBSeconds != "1" {
		t.Fatalf("batch changed %+v %v", repeat, err)
	}
	if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
		t.Fatal(err)
	}
	next, err := store.NextBillingBatch(ctx, 100)
	if err != nil || len(next.Scans) != 1 || next.Storage[0].GiBSeconds != "2" {
		t.Fatalf("late delta %+v %v", next, err)
	}
}

func TestBillingBatchesUnresolvedIdentityAndConcurrentConsumers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	if _, err := store.RecordTrinoQueryUsage(ctx, billingEvent("unresolved", 10)); err != nil {
		t.Fatal(err)
	}
	batch, err := store.NextBillingBatch(ctx, 1)
	if err != nil || batch != nil {
		t.Fatalf("unresolved billed: %+v %v", batch, err)
	}
	billingPrincipal(t, store, "warehouse", "org-a")
	if err := store.RememberTrinoUsagePrincipals([]cs.TrinoEnabledOrg{{OrgID: "org-b", DatabaseName: "warehouse"}}); err == nil {
		t.Fatal("principal reassignment accepted")
	}
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, e := store.NextBillingBatch(ctx, 1)
			if e != nil {
				t.Error(e)
				return
			}
			if b == nil {
				t.Error("missing batch")
				return
			}
			ids <- b.ID
		}()
	}
	wg.Wait()
	close(ids)
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("multiple batches: %s %s", first, id)
		}
	}
}

func TestBillingBatchesBoundedClaims(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	billingPrincipal(t, store, "warehouse", "org-a")
	for _, id := range []string{"a", "b", "c"} {
		if _, err := store.RecordTrinoQueryUsage(ctx, billingEvent(id, 1)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []int64{2, 1} {
		batch, err := store.NextBillingBatch(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if batch.Scans[0].QueryCount != want {
			t.Fatalf("count %+v", batch)
		}
		if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
	}
}

// A transaction can allocate an earlier ID, then commit after a batch has
// claimed later rows. No sequence watermark may make that earlier row disappear.
func TestBillingBatchesIncludeEarlierIDsCommittedAfterExport(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	billingPrincipal(t, store, "warehouse", "org-a")
	tx := store.DB().Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Exec(`INSERT INTO duckgres_trino_query_usage
(cluster_id,query_id,principal,org_id,team_id,source,state,error_code,completed_at,physical_input_bytes,
processed_input_bytes,statistics_complete,trino_version)
VALUES ('test-cluster','slow-commit','warehouse','org-a',0,'','FAILED','',now(),7,0,false,'test')`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordTrinoQueryUsage(ctx, billingEvent("fast-commit", 11)); err != nil {
		t.Fatal(err)
	}
	batch, err := store.NextBillingBatch(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Scans[0].BytesScanned != "11" {
		t.Fatalf("unexpected batch %+v", batch)
	}
	if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	next, err := store.NextBillingBatch(ctx, 100)
	if err != nil || next == nil || next.Scans[0].BytesScanned != "7" {
		t.Fatalf("late commit missing %+v %v", next, err)
	}
	// A retried acknowledgement for the previous batch must not release next.
	if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
		t.Fatal(err)
	}
	repeated, err := store.NextBillingBatch(ctx, 100)
	if err != nil || repeated.ID != next.ID {
		t.Fatalf("old ack released current batch %+v %v", repeated, err)
	}
}

func TestBillingBatchesConcurrentStorageSamplesConserveUsage(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	bucket := time.Now().UTC().Truncate(time.Minute)
	var wg sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := store.UpsertStorageSample("org-a", 1, bucket, 1<<30); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	total := int64(0)
	drain := func() {
		batch, err := store.NextBillingBatch(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			return
		}
		for _, row := range batch.Storage {
			n, err := row.GiBSeconds.Int64()
			if err != nil {
				t.Fatal(err)
			}
			total += n
		}
		if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
	}
	for {
		drain()
		select {
		case <-done:
			drain()
			if total != 80 {
				t.Fatalf("billed %d GiB-seconds, want 80", total)
			}
			return
		default:
		}
	}
}

func TestBillingBatchesReplayAfterAcknowledgement(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	billingPrincipal(t, store, "warehouse", "org-a")
	if _, err := store.RecordTrinoQueryUsage(ctx, billingEvent("replay", 42)); err != nil {
		t.Fatal(err)
	}
	batch, err := store.NextBillingBatch(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckBillingBatch(ctx, batch.ID); err != nil {
		t.Fatal(err)
	}
	replay, err := store.GetBillingBatch(ctx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(replay)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("payload changed: %s != %s", before, after)
	}
	if _, err := store.GetBillingBatch(ctx, "missing"); !errors.Is(err, cs.ErrBillingBatchNotFound) {
		t.Fatalf("missing batch: %v", err)
	}
}
