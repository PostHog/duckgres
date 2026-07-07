package server

import (
	"context"
	"sync/atomic"
	"testing"
)

type captureQueryLogSink struct {
	stops atomic.Int32
}

func (s *captureQueryLogSink) Log(QueryLogEntry) {}

func (s *captureQueryLogSink) StopContext(context.Context) error {
	s.stops.Add(1)
	return nil
}

func TestServerShutdownStopsQueryLogSink(t *testing.T) {
	srv := &Server{}
	sink := &captureQueryLogSink{}
	SetQueryLogSink(srv, sink)

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if got := sink.stops.Load(); got != 1 {
		t.Fatalf("expected query log sink to stop once, got %d", got)
	}
}

func TestNewQueryLogSinkRequiresDuckLakeMetadataStore(t *testing.T) {
	cfg := Config{
		QueryLog: QueryLogConfig{
			Enabled: true,
		},
	}
	sink, err := NewQueryLogSink(cfg)
	if err != nil {
		t.Fatalf("NewQueryLogSink: %v", err)
	}
	if sink != nil {
		t.Fatalf("expected nil sink without metadata store, got %T", sink)
	}
}
