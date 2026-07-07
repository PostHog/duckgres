package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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

func TestNewQueryLogSinkUsesInitializationTimeout(t *testing.T) {
	oldNewPostgresQueryLogSink := newPostgresQueryLogSink
	newPostgresQueryLogSink = func(ctx context.Context, cfg Config) (QueryLogSink, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected initialization context deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > queryLogInitTimeout {
			t.Fatalf("deadline in %s, want within %s", remaining, queryLogInitTimeout)
		}
		return &captureQueryLogSink{}, nil
	}
	t.Cleanup(func() {
		newPostgresQueryLogSink = oldNewPostgresQueryLogSink
	})

	sink, err := NewQueryLogSink(Config{
		DuckLake: DuckLakeConfig{
			MetadataStore: "postgres:host=metadata.internal dbname=ducklake",
		},
		QueryLog: QueryLogConfig{
			Enabled: true,
		},
	})
	if err != nil {
		t.Fatalf("NewQueryLogSink: %v", err)
	}
	if sink == nil {
		t.Fatal("expected query-log sink")
	}
}
