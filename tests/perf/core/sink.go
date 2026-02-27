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

type ArtifactSink struct {
	dir       string
	csvFile   *os.File
	csvWriter *csv.Writer
	closed    bool
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
		"protocol",
		"status",
		"error",
		"error_class",
		"rows",
		"duration_ms",
		"started_at",
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
	return &ArtifactSink{
		dir:       dir,
		csvFile:   f,
		csvWriter: w,
	}, nil
}

func (s *ArtifactSink) Record(result QueryResult) error {
	if s.closed {
		return fmt.Errorf("artifact sink is already closed")
	}
	row := []string{
		result.QueryID,
		result.IntentID,
		string(result.Protocol),
		result.Status,
		result.Error,
		result.ErrorClass,
		strconv.FormatInt(result.Rows, 10),
		strconv.FormatFloat(float64(result.Duration)/float64(time.Millisecond), 'f', 3, 64),
		result.StartedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := s.csvWriter.Write(row); err != nil {
		return fmt.Errorf("write csv row: %w", err)
	}
	s.csvWriter.Flush()
	if err := s.csvWriter.Error(); err != nil {
		return fmt.Errorf("flush csv row: %w", err)
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
