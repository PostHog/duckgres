package publisher

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/perf/core"
)

const serviceMetricsFixture = `query_id,intent_id,measure_iteration,protocol,queue_ms,planning_ms,engine_ms,service_ms,bytes_scanned,dpu_count,result_reused,engine_version,representation,run_label,total_splits,completed_splits,physical_input_rows,cpu_ms,peak_memory_bytes,engine_query_id,stats_source
q_events_total_v5__athena_external,intent_events_total_v5,1,athena,100.000000,200.000000,2000.000000,2500.000000,4096,4,false,Athena engine version 3,,athena,,,,,,,
q_events_total_v5__hoglake_table,intent_events_total_v5,1,trino,1.210000,55.500000,398.000000,412.350000,2097152,,,,,trino,97,97,0,1520.000000,1310720,20260924_101500_00042_abcde,query_info
q_events_total_v5__hoglake_table,intent_events_total_v5,2,trino,1.000000,55.000000,,412.000000,2097152,,,,,trino,92,92,,1520.000000,1310720,20260924_101500_00043_abcde,statement_stats
`

func writeServiceMetricsFile(t *testing.T, runDir, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(runDir, "query_service_metrics.csv"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadArtifactsParsesTrinoAndAthenaServiceMetrics(t *testing.T) {
	runDir := writeFixtureRunDir(t)
	writeServiceMetricsFile(t, runDir, serviceMetricsFixture)

	loaded, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("loadArtifacts returned error: %v", err)
	}
	if len(loaded.ServiceMetrics) != 3 {
		t.Fatalf("service metrics rows = %d, want 3", len(loaded.ServiceMetrics))
	}
	athena, trino, fallback := loaded.ServiceMetrics[0], loaded.ServiceMetrics[1], loaded.ServiceMetrics[2]
	if athena.DPUCount == nil || *athena.DPUCount != 4 || athena.TotalSplits != nil || athena.EngineQueryID != nil {
		t.Fatalf("athena row = %+v, want DPUs and blank Trino columns", athena)
	}
	if trino.TotalSplits == nil || *trino.TotalSplits != 97 || trino.BytesScanned == nil || *trino.BytesScanned != 2<<20 ||
		trino.DPUCount != nil || trino.ResultReused != nil || trino.StatsSource == nil || *trino.StatsSource != "query_info" {
		t.Fatalf("trino row = %+v", trino)
	}
	if fallback.EngineMS != nil || fallback.PhysicalInputRows != nil || fallback.CPUMS == nil || *fallback.CPUMS != 1520 {
		t.Fatalf("fallback row = %+v, want blank engine time and physical rows", fallback)
	}
}

func TestLoadArtifactsAcceptsLegacyOrMissingServiceMetrics(t *testing.T) {
	runDir := writeFixtureRunDir(t)
	loaded, err := loadArtifacts(runDir)
	if err != nil || len(loaded.ServiceMetrics) != 0 {
		t.Fatalf("without sidecar: rows=%d err=%v", len(loaded.ServiceMetrics), err)
	}

	legacy := strings.Join(core.ServiceMetricsColumns[:core.LegacyServiceMetricsColumnCount], ",") + "\n" +
		"q1__athena_external,i1,1,athena,100.000000,200.000000,2000.000000,2500.000000,4096,4,false,Athena engine version 3,,athena\n"
	writeServiceMetricsFile(t, runDir, legacy)
	loaded, err = loadArtifacts(runDir)
	if err != nil {
		t.Fatalf("legacy sidecar: %v", err)
	}
	if len(loaded.ServiceMetrics) != 1 || loaded.ServiceMetrics[0].TotalSplits != nil {
		t.Fatalf("legacy rows = %+v", loaded.ServiceMetrics)
	}

	writeServiceMetricsFile(t, runDir, "query_id,protocol,total_splits\nq1,trino,7\n")
	if _, err := loadArtifacts(runDir); err == nil || !strings.Contains(err.Error(), "unexpected query_service_metrics.csv header") {
		t.Fatalf("expected header validation error, got %v", err)
	}
}

func TestPublishArtifactsReplacesServiceMetricsForRun(t *testing.T) {
	runDir := writeFixtureRunDir(t)
	writeServiceMetricsFile(t, runDir, serviceMetricsFixture)
	loaded, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatal(err)
	}
	db := &fakeDB{tx: &fakeTx{}}
	if err := publishArtifacts(context.Background(), Config{Schema: "duckgres_scenario_perf", BootstrapSchema: true}, db, loaded); err != nil {
		t.Fatalf("publishArtifacts returned error: %v", err)
	}
	var deletes, inserts []execCall
	for _, exec := range db.tx.execs {
		switch {
		case strings.HasPrefix(exec.query, "DELETE FROM duckgres_scenario_perf.query_service_metrics"):
			deletes = append(deletes, exec)
		case strings.HasPrefix(exec.query, "INSERT INTO duckgres_scenario_perf.query_service_metrics"):
			inserts = append(inserts, exec)
		}
	}
	if len(deletes) != 1 || deletes[0].args[0] != loaded.Summary.RunID {
		t.Fatalf("service metrics deletes = %+v, want one scoped to the run", deletes)
	}
	if len(inserts) != 3 {
		t.Fatalf("service metrics inserts = %d, want 3", len(inserts))
	}
	trino := inserts[1]
	for _, column := range []string{"total_splits", "completed_splits", "physical_input_rows", "cpu_ms", "peak_memory_bytes", "engine_query_id", "stats_source", "suite", "nightly_run_id"} {
		if !strings.Contains(trino.query, column) {
			t.Fatalf("service metrics insert missing %s: %s", column, trino.query)
		}
	}
	if got, ok := trino.args[15].(*int64); !ok || got == nil || *got != 97 {
		t.Fatalf("total_splits arg = %#v, want 97", trino.args[15])
	}
	if got, ok := trino.args[10].(*float64); !ok || got != nil {
		t.Fatalf("Trino dpu_count arg = %#v, want NULL", trino.args[10])
	}
	if got := trino.args[24]; got != core.SuiteTables {
		t.Fatalf("suite arg = %#v", got)
	}
	athena := inserts[0]
	if got, ok := athena.args[15].(*int64); !ok || got != nil {
		t.Fatalf("Athena total_splits arg = %#v, want NULL", athena.args[15])
	}
	if !db.tx.committed {
		t.Fatal("expected commit")
	}
}

func TestPublishArtifactsSkipsServiceMetricsTableWithoutRows(t *testing.T) {
	// A header-only sidecar (PGWire-only runs) must not touch the table, so
	// deployments that never bootstrapped it keep publishing.
	runDir := writeFixtureRunDir(t)
	writeServiceMetricsFile(t, runDir, strings.Join(core.ServiceMetricsColumns, ",")+"\n")
	loaded, err := loadArtifacts(runDir)
	if err != nil {
		t.Fatal(err)
	}
	db := &fakeDB{tx: &fakeTx{}}
	if err := publishArtifacts(context.Background(), Config{Schema: "duckgres_perf"}, db, loaded); err != nil {
		t.Fatal(err)
	}
	for _, exec := range db.tx.execs {
		if strings.Contains(exec.query, "query_service_metrics") {
			t.Fatalf("unexpected service metrics statement: %s", exec.query)
		}
	}
}
