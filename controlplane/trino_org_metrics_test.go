//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const trinoOrgMetricsTestCell = "registered:cell-a"

func trinoOrgMetricsTestOrgs() []configstore.TrinoEnabledOrg {
	return []configstore.TrinoEnabledOrg{
		{OrgID: "org-a", DatabaseName: "tenant-a", CellID: trinoOrgMetricsTestCell, RootPasswordHash: "hash"},
		{OrgID: "org-idle", DatabaseName: "tenant-idle", CellID: trinoOrgMetricsTestCell, RootPasswordHash: "hash"},
		{OrgID: "org-elsewhere", DatabaseName: "tenant-elsewhere", CellID: "registered:cell-b", RootPasswordHash: "hash"},
	}
}

func newTrinoOrgMetricsTestCollector(t *testing.T, coordinator *trinoUsageFakeCoordinator) (*trinoUsageCollector, *trinoOrgMetricSet, *prometheus.Registry) {
	t.Helper()
	analytics.SetDefault(&trinoUsageFakeTracker{})
	t.Cleanup(func() { analytics.SetDefault(nil) })
	registry := prometheus.NewRegistry()
	metrics := newTrinoOrgMetrics(registry)
	collector := newTrinoUsageCollector(coordinator, trinoUsageFakeOrgs{orgs: trinoOrgMetricsTestOrgs()}, nil).
		withOrgMetrics(metrics, trinoOrgMetricsTestCell)
	return collector, metrics, registry
}

func trinoOrgHistogramCount(t *testing.T, registry *prometheus.Registry, name, org string) uint64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "org" && label.GetValue() == org {
					return metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestTrinoOrgMetricsCountEachFinishedQueryOnce(t *testing.T) {
	collector, metrics, registry := newTrinoOrgMetricsTestCollector(t, &trinoUsageFakeCoordinator{queries: []admin.TrinoQuery{
		{QueryID: "ok", State: "FINISHED", Principal: "tenant-a", ElapsedMS: 1500, QueuedMS: 250, CPUMS: 4000, PhysicalInputBytes: 1024},
		{QueryID: "user-error", State: "FAILED", Principal: "tenant-a", ErrorType: "USER_ERROR", ElapsedMS: 200, PhysicalInputBytes: 6},
		{QueryID: "new-error-type", State: "FAILED", Principal: "tenant-a", ErrorType: "SOMETHING_NEW"},
		{QueryID: "operator", State: "FINISHED", Principal: "__observer", PhysicalInputBytes: 999},
	}})

	collector.collect(context.Background())
	collector.collect(context.Background())

	for labels, want := range map[[2]string]float64{
		{"success", "none"}:  1,
		{"error", "user"}:    1,
		{"error", "unknown"}: 1,
	} {
		if got := testutil.ToFloat64(metrics.queries.WithLabelValues("org-a", labels[0], labels[1])); got != want {
			t.Errorf("query_total{status=%q,error_type=%q} = %v, want %v", labels[0], labels[1], got, want)
		}
	}
	if got := testutil.ToFloat64(metrics.scannedBytes.WithLabelValues("org-a")); got != 1030 {
		t.Errorf("scanned bytes = %v, want 1030", got)
	}
	if got := testutil.ToFloat64(metrics.cpuSeconds.WithLabelValues("org-a")); got != 4 {
		t.Errorf("cpu seconds = %v, want 4", got)
	}
	for _, name := range []string{"duckgres_trino_org_query_duration_seconds", "duckgres_trino_org_query_queued_seconds"} {
		if got := trinoOrgHistogramCount(t, registry, name, "org-a"); got != 3 {
			t.Errorf("%s sample count = %d, want 3", name, got)
		}
	}
	if got := testutil.CollectAndCount(metrics.queries); got != 3 {
		t.Errorf("query_total series = %d, want 3 (operator queries are not tenant usage)", got)
	}
}

func TestTrinoOrgMetricsGaugeInFlightQueriesForThisCellOnly(t *testing.T) {
	coordinator := &trinoUsageFakeCoordinator{queries: []admin.TrinoQuery{
		{QueryID: "r1", State: "RUNNING", Principal: "tenant-a"},
		{QueryID: "r2", State: "RUNNING", Principal: "tenant-a"},
		{QueryID: "b1", State: "RUNNING", FullyBlocked: true, Principal: "tenant-a"},
		{QueryID: "q1", State: "QUEUED", Principal: "tenant-a"},
		{QueryID: "p1", State: "PLANNING", Principal: "tenant-a"},
		{QueryID: "done", State: "FINISHED", Principal: "tenant-a"},
	}}
	collector, metrics, _ := newTrinoOrgMetricsTestCollector(t, coordinator)

	collector.collect(context.Background())

	for state, want := range map[string]float64{"running": 2, "blocked": 1, "queued": 1, "other": 1} {
		if got := testutil.ToFloat64(metrics.inFlight.WithLabelValues("org-a", state)); got != want {
			t.Errorf("org-a %s = %v, want %v", state, got, want)
		}
	}
	// 4 states each for org-a and the idle org; none for the org another cell owns.
	if got := testutil.CollectAndCount(metrics.inFlight); got != 8 {
		t.Fatalf("in-flight series = %d, want 8", got)
	}

	coordinator.queries = nil
	collector.collect(context.Background())
	if got := testutil.ToFloat64(metrics.inFlight.WithLabelValues("org-a", "running")); got != 0 {
		t.Errorf("org-a running after the queries finished = %v, want 0", got)
	}
}

func TestTrinoOrgMetricsClearGaugesWhenThePollCannotBeTrusted(t *testing.T) {
	t.Run("coordinator read fails", func(t *testing.T) {
		coordinator := &trinoUsageFakeCoordinator{queries: []admin.TrinoQuery{{QueryID: "r1", State: "RUNNING", Principal: "tenant-a"}}}
		collector, metrics, _ := newTrinoOrgMetricsTestCollector(t, coordinator)
		collector.collect(context.Background())

		coordinator.err = errors.New("coordinator unreachable")
		collector.collect(context.Background())

		if got := testutil.CollectAndCount(metrics.inFlight); got != 0 {
			t.Fatalf("in-flight series after a failed poll = %d, want 0", got)
		}
	})

	t.Run("leadership ends", func(t *testing.T) {
		coordinator := &trinoUsageFakeCoordinator{queries: []admin.TrinoQuery{{QueryID: "r1", State: "RUNNING", Principal: "tenant-a"}}}
		collector, metrics, _ := newTrinoOrgMetricsTestCollector(t, coordinator)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		collector.Run(ctx)

		if got := testutil.CollectAndCount(metrics.inFlight); got != 0 {
			t.Fatalf("in-flight series after Run returned = %d, want 0", got)
		}
	})

	t.Run("org leaves the cell", func(t *testing.T) {
		coordinator := &trinoUsageFakeCoordinator{}
		collector, metrics, _ := newTrinoOrgMetricsTestCollector(t, coordinator)
		collector.collect(context.Background())

		collector.orgs = trinoUsageFakeOrgs{orgs: trinoOrgMetricsTestOrgs()[:1]}
		collector.collect(context.Background())

		if got := testutil.CollectAndCount(metrics.inFlight); got != 4 {
			t.Fatalf("in-flight series = %d, want 4 (only org-a remains)", got)
		}
	})
}
