package controlplane

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	// defaultStorageSampleInterval is how often the leader samples each org's
	// tracked DuckLake storage footprint. Each successful sample credits
	// exactly one interval of byte-seconds — no elapsed-time tracking, so a
	// missed sample under-bills one interval (best-effort, like compute).
	// Env-only override DUCKGRES_STORAGE_SAMPLE_INTERVAL (e2e uses seconds).
	defaultStorageSampleInterval = 30 * time.Minute

	// storageSampleQueryTimeout bounds one org's metadata-Postgres visit
	// (connect + the two-table SUM).
	storageSampleQueryTimeout = 15 * time.Second
)

// storageSampleQuery reads the org's tracked S3 footprint from its DuckLake
// metadata Postgres. Sums data + delete files with NO snapshot filter
// (time-travel-retained files stay billable until snapshot expiry), plus the
// pending-delete count — files between expire_snapshots and
// cleanup_old_files whose size row is already gone (the drift gauge).
// ducklake_table_info()/ducklake_table_stats are deliberately NOT used: the
// former filters to the current snapshot, the latter is approximate. See
// docs/design/billing-pull-api.md "Storage metric".
const storageSampleQuery = `
SELECT COALESCE((SELECT SUM(file_size_bytes) FROM ducklake_data_file), 0)
     + COALESCE((SELECT SUM(file_size_bytes) FROM ducklake_delete_file), 0),
       (SELECT COUNT(*) FROM ducklake_files_scheduled_for_deletion)`

var storageTrackedBytesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_storage_tracked_bytes",
	Help: "Tracked DuckLake S3 footprint per org (bytes), from the last storage-billing sample.",
}, []string{"org"})

var storagePendingDeleteFilesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_storage_pending_delete_files",
	Help: "Files scheduled for deletion but not yet cleaned per org — bytes on S3 invisible to the storage meter. Sustained nonzero = cleanup lagging (drift).",
}, []string{"org"})

var storageSampleErrorsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_storage_sample_errors_total",
	Help: "Storage-billing samples skipped due to an error (org unreachable, query failed). Each miss under-bills one interval.",
}, []string{"org"})

// storageOrg is one samplable org: a Ready DuckLake warehouse and its default
// team (the storage bucket key's team_id; 0 = no default team).
type storageOrg struct {
	OrgID  string
	TeamID int64
}

// storageUsageStore is the config-store surface the sampler needs.
type storageUsageStore interface {
	UpsertStorageSample(orgID string, teamID int64, bucketStart time.Time, byteSeconds int64) error
}

// storageSampler is the leader-only loop converting each org's storage level
// into billable byte-seconds. Strictly best-effort: any per-org failure is
// logged, counted and skipped — it never fails anything user-facing.
type storageSampler struct {
	store    storageUsageStore
	interval time.Duration
	// listOrgs returns the orgs to sample (Ready DuckLake warehouses) with
	// their default team ids, from the config snapshot.
	listOrgs func() []storageOrg
	// resolveDSN maps an org to its metadata-Postgres URL
	// (SharedWorkerActivator.MetadataPostgresURL on the k8s backend).
	resolveDSN func(ctx context.Context, orgID string) (string, error)
	// queryFootprint visits one org's metadata Postgres; swappable in tests.
	queryFootprint func(ctx context.Context, dsn string) (trackedBytes, pendingDeleteFiles int64, err error)
	now            func() time.Time
}

func newStorageSampler(store storageUsageStore, interval time.Duration, listOrgs func() []storageOrg, resolveDSN func(ctx context.Context, orgID string) (string, error)) *storageSampler {
	if interval <= 0 {
		interval = defaultStorageSampleInterval
	}
	return &storageSampler{
		store:          store,
		interval:       interval,
		listOrgs:       listOrgs,
		resolveDSN:     resolveDSN,
		queryFootprint: queryStorageFootprint,
		now:            time.Now,
	}
}

// Run samples every interval until ctx is cancelled. Leader-only: the caller
// attaches it under the janitor leader lease so exactly one CP pod credits
// each interval (a double writer would double-bill — the UPSERT is additive).
//
// Deliberately NO sample on entry: Run re-enters on every leadership
// acquisition, and each successful sample credits a full interval regardless
// of elapsed time — sampling immediately would credit an extra interval per
// leader change (deploys, lease flaps), i.e. systematic over-billing. Waiting
// one full interval for the first tick keeps failover in the documented
// direction: a leadership change under-bills at most one interval.
func (s *storageSampler) Run(ctx context.Context) {
	if s == nil || s.store == nil || s.listOrgs == nil || s.resolveDSN == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleAll(ctx)
		}
	}
}

func (s *storageSampler) sampleAll(ctx context.Context) {
	orgs := s.listOrgs()
	var sampled, failed int
	for _, org := range orgs {
		if ctx.Err() != nil {
			return
		}
		if err := s.sampleOrg(ctx, org); err != nil {
			slog.Warn("Storage-billing sample failed; interval under-billed for this org.", "org", org.OrgID, "error", err)
			storageSampleErrorsCounter.WithLabelValues(org.OrgID).Inc()
			failed++
			continue
		}
		sampled++
	}
	if sampled > 0 || failed > 0 {
		slog.Info("Storage-billing sample pass complete.", "sampled", sampled, "failed", failed)
	}
}

func (s *storageSampler) sampleOrg(ctx context.Context, org storageOrg) error {
	orgCtx, cancel := context.WithTimeout(ctx, storageSampleQueryTimeout)
	defer cancel()

	dsn, err := s.resolveDSN(orgCtx, org.OrgID)
	if err != nil {
		return err
	}
	trackedBytes, pendingDelete, err := s.queryFootprint(orgCtx, dsn)
	if err != nil {
		return err
	}
	storageTrackedBytesGauge.WithLabelValues(org.OrgID).Set(float64(trackedBytes))
	storagePendingDeleteFilesGauge.WithLabelValues(org.OrgID).Set(float64(pendingDelete))

	// Credit exactly one interval: bytes × interval-seconds into the
	// sample-minute's bucket (same 60s bucket_start convention as compute, so
	// the shared closed-bucket/watermark rules apply unchanged).
	byteSeconds := trackedBytes * int64(s.interval/time.Second)
	bucket := s.now().UTC().Truncate(computeBucketWidth)
	return s.store.UpsertStorageSample(org.OrgID, org.TeamID, bucket, byteSeconds)
}

// queryStorageFootprint opens the org's metadata Postgres and runs the
// two-table SUM. One short-lived connection per visit — at a 30min cadence a
// pool per org would be pure overhead.
func queryStorageFootprint(ctx context.Context, dsn string) (trackedBytes, pendingDeleteFiles int64, err error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if err := db.QueryRowContext(ctx, storageSampleQuery).Scan(&trackedBytes, &pendingDeleteFiles); err != nil {
		return 0, 0, err
	}
	return trackedBytes, pendingDeleteFiles, nil
}

// storageSampleIntervalFromEnv reads the env-only sampling-interval override
// (DUCKGRES_STORAGE_SAMPLE_INTERVAL, a Go duration — the e2e harness deploys
// with a short one so the round-trip is assertable in-Job). Unset or invalid
// → the 30min default.
func storageSampleIntervalFromEnv() time.Duration {
	v := os.Getenv("DUCKGRES_STORAGE_SAMPLE_INTERVAL")
	if v == "" {
		return defaultStorageSampleInterval
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		slog.Warn("Invalid DUCKGRES_STORAGE_SAMPLE_INTERVAL; using default.", "value", v, "default", defaultStorageSampleInterval.String())
		return defaultStorageSampleInterval
	}
	return d
}
