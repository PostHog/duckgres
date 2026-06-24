package server

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type captureQueryLogKafkaProducer struct {
	messages []queryLogKafkaMessage
	closed   bool
}

func (p *captureQueryLogKafkaProducer) WriteMessages(_ context.Context, messages ...queryLogKafkaMessage) error {
	p.messages = append(p.messages, messages...)
	return nil
}

func (p *captureQueryLogKafkaProducer) Close() error {
	p.closed = true
	return nil
}

type blockingQueryLogKafkaProducer struct {
	writeStarted chan struct{}
	closeStarted chan struct{}
	closeOnce    sync.Once
	writeOnce    sync.Once
}

func (p *blockingQueryLogKafkaProducer) WriteMessages(ctx context.Context, _ ...queryLogKafkaMessage) error {
	p.writeOnce.Do(func() {
		close(p.writeStarted)
	})
	<-ctx.Done()
	return ctx.Err()
}

func (p *blockingQueryLogKafkaProducer) Close() error {
	p.closeOnce.Do(func() {
		close(p.closeStarted)
	})
	return nil
}

type captureQueryLogSink struct {
	stops atomic.Int32
}

func (s *captureQueryLogSink) Log(QueryLogEntry) {}

func (s *captureQueryLogSink) Stop() {
	s.stops.Add(1)
}

func (s *captureQueryLogSink) StopContext(context.Context) error {
	s.Stop()
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

func TestKafkaQueryLogSinkPublishesEnvelope(t *testing.T) {
	producer := &captureQueryLogKafkaProducer{}
	sink, err := newKafkaQueryLogSinkForProducer(QueryLogConfig{
		BatchSize:     10,
		FlushInterval: time.Hour,
		Kafka: QueryLogKafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "duckgres-query-log",
		},
	}, producer)
	if err != nil {
		t.Fatalf("newKafkaQueryLogSinkForProducer: %v", err)
	}
	sink.now = func() time.Time { return time.Unix(1710000000, 0).UTC() }
	sink.newEventID = func() string { return "evt-test" }

	eventTime := time.Unix(1700000000, 0).UTC()
	sink.Log(QueryLogEntry{
		EventTime:             eventTime,
		QueryDurationMs:       42,
		Type:                  "QueryFinish",
		Query:                 "SELECT 1",
		QueryKind:             "Select",
		NormalizedHash:        -17,
		ResultRows:            1,
		UserName:              "alice",
		OrgID:                 "org-a",
		CurrentDatabase:       "ducklake",
		ClientAddress:         "127.0.0.1",
		ClientPort:            54321,
		ApplicationName:       "psql",
		PID:                   123,
		WorkerID:              456,
		Protocol:              "simple",
		TraceID:               "trace-1",
		SpanID:                "span-1",
		PostgresScanMs:        7,
		CPUTimeSeconds:        1.25,
		PeakBufferMemoryBytes: 2048,
	})
	sink.Stop()

	if !producer.closed {
		t.Fatal("expected producer to be closed on Stop")
	}
	if len(producer.messages) != 1 {
		t.Fatalf("expected 1 Kafka message, got %d", len(producer.messages))
	}
	message := producer.messages[0]
	if got, want := message.Topic, "duckgres-query-log"; got != want {
		t.Fatalf("topic mismatch: got %q want %q", got, want)
	}
	if got, want := string(message.Key), "org-a"; got != want {
		t.Fatalf("key mismatch: got %q want %q", got, want)
	}

	var event QueryLogKafkaEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if event.SchemaVersion != 1 {
		t.Fatalf("schema version mismatch: got %d", event.SchemaVersion)
	}
	if event.EventID != "evt-test" {
		t.Fatalf("event id mismatch: got %q", event.EventID)
	}
	if !event.EmittedAt.Equal(time.Unix(1710000000, 0).UTC()) {
		t.Fatalf("emitted_at mismatch: got %s", event.EmittedAt)
	}
	if !event.EventTime.Equal(eventTime) {
		t.Fatalf("event_time mismatch: got %s", event.EventTime)
	}
	if event.OrgID != "org-a" || event.UserName != "alice" || event.Query != "SELECT 1" {
		t.Fatalf("unexpected event identity fields: %#v", event)
	}
	if event.CPUTimeSeconds != 1.25 {
		t.Fatalf("cpu_time_s mismatch: got %f", event.CPUTimeSeconds)
	}
	if event.PeakBufferMemoryBytes != 2048 {
		t.Fatalf("peak_buffer_memory_bytes mismatch: got %d", event.PeakBufferMemoryBytes)
	}
	if event.PostgresScanMs != 7 {
		t.Fatalf("postgres_scan_ms mismatch: got %d", event.PostgresScanMs)
	}
}

func TestKafkaQueryLogSinkStopBoundsBlockedPublish(t *testing.T) {
	producer := &blockingQueryLogKafkaProducer{
		writeStarted: make(chan struct{}),
		closeStarted: make(chan struct{}),
	}
	sink, err := newKafkaQueryLogSinkForProducer(QueryLogConfig{
		BatchSize:     1,
		FlushInterval: time.Hour,
		Kafka: QueryLogKafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "duckgres-query-log",
		},
	}, producer)
	if err != nil {
		t.Fatalf("newKafkaQueryLogSinkForProducer: %v", err)
	}
	sink.publishTimeout = 10 * time.Millisecond

	sink.Log(QueryLogEntry{
		EventTime: time.Unix(1700000000, 0).UTC(),
		Type:      "QueryFinish",
		Query:     "SELECT 1",
		OrgID:     "org-a",
	})

	select {
	case <-producer.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("expected Kafka publish to start")
	}

	done := make(chan struct{})
	go func() {
		sink.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected Stop to return after publish timeout")
	}

	select {
	case <-producer.closeStarted:
	default:
		t.Fatal("expected producer to close")
	}
}

func TestKafkaQueryLogSinkStopContextBoundsTotalDrain(t *testing.T) {
	producer := &blockingQueryLogKafkaProducer{
		writeStarted: make(chan struct{}),
		closeStarted: make(chan struct{}),
	}
	sink, err := newKafkaQueryLogSinkForProducer(QueryLogConfig{
		BatchSize:     1,
		FlushInterval: time.Hour,
		Kafka: QueryLogKafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "duckgres-query-log",
		},
	}, producer)
	if err != nil {
		t.Fatalf("newKafkaQueryLogSinkForProducer: %v", err)
	}
	sink.publishTimeout = time.Second

	sink.Log(QueryLogEntry{
		EventTime: time.Unix(1700000000, 0).UTC(),
		Type:      "QueryFinish",
		Query:     "SELECT 1",
		OrgID:     "org-a",
	})

	select {
	case <-producer.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("expected Kafka publish to start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = sink.StopContext(ctx)
	if err == nil {
		t.Fatal("expected StopContext to return deadline error")
	}

	select {
	case <-producer.closeStarted:
	default:
		t.Fatal("expected producer to close after stop deadline")
	}
}

func TestNewQueryLogSinkDefaultsToDuckLake(t *testing.T) {
	cfg := Config{
		QueryLog: QueryLogConfig{
			Enabled: true,
			Sink:    "",
		},
	}
	sink, err := NewQueryLogSink(cfg)
	if err != nil {
		t.Fatalf("NewQueryLogSink: %v", err)
	}
	if sink != nil {
		t.Fatalf("expected nil sink without DuckLake metadata store, got %T", sink)
	}
}

func TestNewQueryLogSinkRejectsKafkaWithoutTopic(t *testing.T) {
	_, err := NewQueryLogSink(Config{
		QueryLog: QueryLogConfig{
			Enabled: true,
			Sink:    QueryLogSinkKafka,
			Kafka: QueryLogKafkaConfig{
				Brokers: []string{"localhost:9092"},
			},
		},
	})
	if err == nil {
		t.Fatal("expected missing Kafka topic error")
	}
}
