package core

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// ServiceMetricsColumns is the query_service_metrics.csv header. Columns are
// only ever appended: the first LegacyServiceMetricsColumnCount columns are the
// original Athena-era contract, and the Trino query statistics follow. Athena
// rows leave the Trino columns blank; Trino rows leave dpu_count,
// result_reused, and engine_version blank.
var ServiceMetricsColumns = []string{
	"query_id",
	"intent_id",
	"measure_iteration",
	"protocol",
	"queue_ms",
	"planning_ms",
	"engine_ms",
	"service_ms",
	"bytes_scanned",
	"dpu_count",
	"result_reused",
	"engine_version",
	"representation",
	"run_label",
	"total_splits",
	"completed_splits",
	"physical_input_rows",
	"cpu_ms",
	"peak_memory_bytes",
	"engine_query_id",
	"stats_source",
}

// LegacyServiceMetricsColumnCount is the width of the header written before
// the Trino query statistics columns were appended.
const LegacyServiceMetricsColumnCount = 14

type ArtifactSink struct {
	dir                  string
	csvFile              *os.File
	csvWriter            *csv.Writer
	serviceMetricsFile   *os.File
	serviceMetricsWriter *csv.Writer
	closed               bool
}

func NewArtifactSink(dir string) (*ArtifactSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir %s: %w", dir, err)
	}
	csvPath := filepath.Join(dir, "query_results.csv")
	f, err := os.Create(csvPath)
	if err != nil {
		return nil, fmt.Errorf("create csv artifact: %w", err)
	}
	w := csv.NewWriter(f)
	header := []string{
		"query_id",
		"intent_id",
		"measure_iteration",
		"protocol",
		"status",
		"error",
		"error_class",
		"rows",
		"duration_ms",
		"started_at",
		"representation",
		"run_label",
	}
	if err := w.Write(header); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write csv header: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("flush csv header: %w", err)
	}
	serviceMetricsPath := filepath.Join(dir, "query_service_metrics.csv")
	serviceMetricsFile, err := os.Create(serviceMetricsPath)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("create service metrics artifact: %w", err)
	}
	serviceMetricsWriter := csv.NewWriter(serviceMetricsFile)
	if err := serviceMetricsWriter.Write(ServiceMetricsColumns); err != nil {
		_ = serviceMetricsFile.Close()
		_ = f.Close()
		return nil, fmt.Errorf("write service metrics header: %w", err)
	}
	serviceMetricsWriter.Flush()
	if err := serviceMetricsWriter.Error(); err != nil {
		_ = serviceMetricsFile.Close()
		_ = f.Close()
		return nil, fmt.Errorf("flush service metrics header: %w", err)
	}
	return &ArtifactSink{
		dir:                  dir,
		csvFile:              f,
		csvWriter:            w,
		serviceMetricsFile:   serviceMetricsFile,
		serviceMetricsWriter: serviceMetricsWriter,
	}, nil
}

func (s *ArtifactSink) Record(result QueryResult) error {
	if s.closed {
		return fmt.Errorf("artifact sink is already closed")
	}
	row := []string{
		result.QueryID,
		result.IntentID,
		strconv.Itoa(result.MeasureIteration),
		string(result.Protocol),
		result.Status,
		result.Error,
		result.ErrorClass,
		strconv.FormatInt(result.Rows, 10),
		strconv.FormatFloat(float64(result.Duration)/float64(time.Millisecond), 'f', 6, 64),
		result.StartedAt.UTC().Format(time.RFC3339Nano),
		result.Representation,
		result.Protocol.RunLabel(result.Representation),
	}
	if err := s.csvWriter.Write(row); err != nil {
		return fmt.Errorf("write csv row: %w", err)
	}
	s.csvWriter.Flush()
	if err := s.csvWriter.Error(); err != nil {
		return fmt.Errorf("flush csv row: %w", err)
	}
	if result.ServiceMetrics != nil {
		serviceMetricsRow := serviceMetricsRecord(result)
		if err := s.serviceMetricsWriter.Write(serviceMetricsRow); err != nil {
			return fmt.Errorf("write service metrics row: %w", err)
		}
		s.serviceMetricsWriter.Flush()
		if err := s.serviceMetricsWriter.Error(); err != nil {
			return fmt.Errorf("flush service metrics row: %w", err)
		}
	}
	return nil
}

func (s *ArtifactSink) Close(summary RunSummary, serverMetrics string) error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.csvWriter != nil {
		s.csvWriter.Flush()
		if err := s.csvWriter.Error(); err != nil {
			return fmt.Errorf("flush csv close: %w", err)
		}
	}
	if s.csvFile != nil {
		if err := s.csvFile.Close(); err != nil {
			return fmt.Errorf("close csv file: %w", err)
		}
	}
	if s.serviceMetricsWriter != nil {
		s.serviceMetricsWriter.Flush()
		if err := s.serviceMetricsWriter.Error(); err != nil {
			return fmt.Errorf("flush service metrics close: %w", err)
		}
	}
	if s.serviceMetricsFile != nil {
		if err := s.serviceMetricsFile.Close(); err != nil {
			return fmt.Errorf("close service metrics file: %w", err)
		}
	}

	summaryPath := filepath.Join(s.dir, "summary.json")
	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		return fmt.Errorf("create summary file: %w", err)
	}
	enc := json.NewEncoder(summaryFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		_ = summaryFile.Close()
		return fmt.Errorf("encode summary: %w", err)
	}
	if err := summaryFile.Close(); err != nil {
		return fmt.Errorf("close summary file: %w", err)
	}

	metricsPath := filepath.Join(s.dir, "server_metrics.prom")
	if err := os.WriteFile(metricsPath, []byte(serverMetrics), 0o644); err != nil {
		return fmt.Errorf("write server metrics: %w", err)
	}
	return nil
}

func serviceMetricsRecord(result QueryResult) []string {
	metrics := result.ServiceMetrics
	engineMS := ""
	if metrics.HasEngineDuration() {
		engineMS = formatMilliseconds(metrics.EngineDuration)
	}
	dpuCount := strconv.FormatFloat(metrics.DPUCount, 'f', -1, 64)
	resultReused := strconv.FormatBool(metrics.ResultReused)
	trinoColumns := make([]string, len(ServiceMetricsColumns)-LegacyServiceMetricsColumnCount)
	if trino := metrics.Trino; trino != nil {
		// DPUs and result reuse are Athena concepts; a zero would mislead.
		dpuCount, resultReused = "", ""
		physicalInputRows := ""
		if trino.PhysicalInputRows != nil {
			physicalInputRows = strconv.FormatInt(*trino.PhysicalInputRows, 10)
		}
		trinoColumns = []string{
			strconv.FormatInt(trino.TotalSplits, 10),
			strconv.FormatInt(trino.CompletedSplits, 10),
			physicalInputRows,
			formatMilliseconds(trino.CPUDuration),
			strconv.FormatInt(trino.PeakMemoryBytes, 10),
			trino.QueryID,
			trino.Source,
		}
	}
	return append([]string{
		result.QueryID,
		result.IntentID,
		strconv.Itoa(result.MeasureIteration),
		string(result.Protocol),
		formatMilliseconds(metrics.QueueDuration),
		formatMilliseconds(metrics.PlanningDuration),
		engineMS,
		formatMilliseconds(metrics.ServiceDuration),
		strconv.FormatInt(metrics.BytesScanned, 10),
		dpuCount,
		resultReused,
		metrics.EngineVersion,
		result.Representation,
		result.Protocol.RunLabel(result.Representation),
	}, trinoColumns...)
}

func formatMilliseconds(duration time.Duration) string {
	return strconv.FormatFloat(float64(duration)/float64(time.Millisecond), 'f', 6, 64)
}
