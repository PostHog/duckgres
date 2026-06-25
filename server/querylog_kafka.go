package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const (
	queryLogKafkaSchemaVersion         = 1
	defaultQueryLogKafkaPublishTimeout = 30 * time.Second
)

// QueryLogKafkaEvent is the stable JSON payload written to Kafka. It mirrors
// system.query_log columns and adds an event envelope for distributed delivery.
type QueryLogKafkaEvent struct {
	SchemaVersion         int       `json:"schema_version"`
	EventID               string    `json:"event_id"`
	EmittedAt             time.Time `json:"emitted_at"`
	EventTime             time.Time `json:"event_time"`
	QueryDurationMs       int64     `json:"query_duration_ms"`
	Type                  string    `json:"type"`
	Query                 string    `json:"query"`
	TranspiledQuery       *string   `json:"transpiled_query"`
	QueryKind             string    `json:"query_kind"`
	NormalizedHash        int64     `json:"normalized_query_hash"`
	ResultRows            int64     `json:"result_rows"`
	WrittenRows           int64     `json:"written_rows"`
	ExceptionCode         string    `json:"exception_code"`
	Exception             string    `json:"exception"`
	UserName              string    `json:"user_name"`
	OrgID                 string    `json:"org_id"`
	CurrentDatabase       string    `json:"current_database"`
	ClientAddress         string    `json:"client_address"`
	ClientPort            int       `json:"client_port"`
	ApplicationName       string    `json:"application_name"`
	PID                   int32     `json:"pid"`
	WorkerID              int       `json:"worker_id"`
	IsTranspiled          bool      `json:"is_transpiled"`
	Protocol              string    `json:"protocol"`
	TraceID               string    `json:"trace_id"`
	SpanID                string    `json:"span_id"`
	PostgresScanMs        int64     `json:"postgres_scan_ms"`
	CPUTimeSeconds        float64   `json:"cpu_time_s"`
	PeakBufferMemoryBytes int64     `json:"peak_buffer_memory_bytes"`
}

type queryLogKafkaMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

type queryLogKafkaProducer interface {
	WriteMessages(context.Context, ...queryLogKafkaMessage) error
	Close() error
}

// KafkaQueryLogSink batches query-log entries and publishes them to Kafka.
type KafkaQueryLogSink struct {
	cfg            QueryLogConfig
	producer       queryLogKafkaProducer
	ch             chan QueryLogEntry
	done           chan struct{}
	stopOnce       sync.Once
	stopMu         sync.RWMutex
	stopCtx        context.Context
	publishTimeout time.Duration
	now            func() time.Time
	newEventID     func() string
}

func NewKafkaQueryLogSink(cfg QueryLogConfig) (*KafkaQueryLogSink, error) {
	producer, err := newKafkaGoQueryLogProducer(cfg)
	if err != nil {
		return nil, err
	}
	return newKafkaQueryLogSinkForProducer(cfg, producer)
}

func newKafkaQueryLogSinkForProducer(cfg QueryLogConfig, producer queryLogKafkaProducer) (*KafkaQueryLogSink, error) {
	if err := validateQueryLogKafkaConfig(cfg); err != nil {
		if producer != nil {
			_ = producer.Close()
		}
		return nil, err
	}
	if producer == nil {
		return nil, errors.New("querylog: kafka producer is nil")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	sink := &KafkaQueryLogSink{
		cfg:            cfg,
		producer:       producer,
		ch:             make(chan QueryLogEntry, queryLogChannelSize),
		done:           make(chan struct{}),
		publishTimeout: defaultQueryLogKafkaPublishTimeout,
		now:            func() time.Time { return time.Now().UTC() },
		newEventID:     func() string { return uuid.NewString() },
	}
	go sink.flushLoop()
	slog.Info("Query log Kafka sink enabled.", "topic", cfg.Kafka.Topic, "brokers", len(cfg.Kafka.Brokers), "flush_interval", cfg.FlushInterval, "batch_size", cfg.BatchSize)
	return sink, nil
}

func validateQueryLogKafkaConfig(cfg QueryLogConfig) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("querylog: kafka brokers are required")
	}
	if strings.TrimSpace(cfg.Kafka.Topic) == "" {
		return errors.New("querylog: kafka topic is required")
	}
	return nil
}

func (s *KafkaQueryLogSink) Log(entry QueryLogEntry) {
	if s == nil || s.ch == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("querylog: kafka sink stopped while writing entry; dropping entry.")
		}
	}()
	select {
	case s.ch <- entry:
	default:
		slog.Warn("querylog: kafka channel full, dropping entry.")
	}
}

func (s *KafkaQueryLogSink) Stop() {
	_ = s.StopContext(context.Background())
}

func (s *KafkaQueryLogSink) StopContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ctx, cancel := queryLogStopContext(ctx, defaultQueryLogKafkaPublishTimeout)
	defer cancel()

	var stopErr error
	s.stopOnce.Do(func() {
		s.setStopContext(ctx)
		if s.ch != nil {
			close(s.ch)
		}
		if s.done != nil {
			select {
			case <-s.done:
			case <-ctx.Done():
				stopErr = ctx.Err()
			}
		}
		if s.producer != nil {
			if err := s.producer.Close(); err != nil {
				slog.Warn("querylog: kafka producer close failed.", "error", err)
				if stopErr == nil {
					stopErr = err
				}
			}
		}
	})
	return stopErr
}

func (s *KafkaQueryLogSink) flushLoop() {
	defer close(s.done)

	batch := make([]QueryLogEntry, 0, s.cfg.BatchSize)
	flushTicker := time.NewTicker(s.cfg.FlushInterval)
	defer flushTicker.Stop()

	for {
		if s.dropRemainingIfStopExpired(len(batch)) {
			return
		}
		select {
		case entry, ok := <-s.ch:
			if !ok {
				if len(batch) > 0 {
					if err := s.flushBatchWithContext(s.currentStopContext(), batch); err != nil && s.stopContextDone() {
						s.logDroppedOnStop(len(batch))
					}
				}
				return
			}
			batch = append(batch, entry)
			if len(batch) >= s.cfg.BatchSize {
				if err := s.flushBatchWithContext(s.currentStopContext(), batch); err != nil && s.stopContextDone() {
					s.logDroppedOnStop(len(batch))
					return
				}
				batch = batch[:0]
			}
		case <-flushTicker.C:
			if len(batch) > 0 {
				flushCtx := s.currentStopContext()
				if flushCtx == nil {
					flushCtx = context.Background()
				}
				if err := s.flushBatchWithContext(flushCtx, batch); err != nil && s.stopContextDone() {
					s.logDroppedOnStop(len(batch))
					return
				}
				batch = batch[:0]
			}
		}
	}
}

func (s *KafkaQueryLogSink) flushBatchWithContext(ctx context.Context, batch []QueryLogEntry) error {
	messages := make([]queryLogKafkaMessage, 0, len(batch))
	for _, entry := range batch {
		message, err := s.kafkaMessage(entry)
		if err != nil {
			slog.Error("querylog: encode kafka event failed.", "error", err)
			continue
		}
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	publishTimeout := s.publishTimeout
	if publishTimeout <= 0 {
		publishTimeout = defaultQueryLogKafkaPublishTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	if err := s.producer.WriteMessages(ctx, messages...); err != nil {
		slog.Error("querylog: kafka publish failed.", "error", err, "batch_size", len(messages))
		return err
	}
	return nil
}

func queryLogStopContext(ctx context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultTimeout)
}

func (s *KafkaQueryLogSink) setStopContext(ctx context.Context) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.stopCtx = ctx
}

func (s *KafkaQueryLogSink) currentStopContext() context.Context {
	s.stopMu.RLock()
	defer s.stopMu.RUnlock()
	return s.stopCtx
}

func (s *KafkaQueryLogSink) stopContextDone() bool {
	ctx := s.currentStopContext()
	return ctx != nil && ctx.Err() != nil
}

func (s *KafkaQueryLogSink) dropRemainingIfStopExpired(batchLen int) bool {
	ctx := s.currentStopContext()
	if ctx == nil || ctx.Err() == nil {
		return false
	}
	s.logDroppedOnStop(batchLen)
	return true
}

func (s *KafkaQueryLogSink) logDroppedOnStop(batchLen int) {
	dropped := batchLen
	if s.ch != nil {
		dropped += len(s.ch)
	}
	slog.Warn("querylog: kafka shutdown deadline exceeded; dropping queued entries.", "dropped", dropped)
}

func (s *KafkaQueryLogSink) kafkaMessage(entry QueryLogEntry) (queryLogKafkaMessage, error) {
	event := newQueryLogKafkaEvent(entry, s.now(), s.newEventID())
	value, err := json.Marshal(event)
	if err != nil {
		return queryLogKafkaMessage{}, err
	}
	return queryLogKafkaMessage{
		Topic: s.cfg.Kafka.Topic,
		Key:   []byte(entry.OrgID),
		Value: value,
	}, nil
}

func newQueryLogKafkaEvent(entry QueryLogEntry, emittedAt time.Time, eventID string) QueryLogKafkaEvent {
	return QueryLogKafkaEvent{
		SchemaVersion:         queryLogKafkaSchemaVersion,
		EventID:               eventID,
		EmittedAt:             emittedAt.UTC(),
		EventTime:             entry.EventTime.UTC(),
		QueryDurationMs:       entry.QueryDurationMs,
		Type:                  entry.Type,
		Query:                 truncateQuery(entry.Query),
		TranspiledQuery:       truncateNullableQuery(entry.TranspiledQuery),
		QueryKind:             entry.QueryKind,
		NormalizedHash:        entry.NormalizedHash,
		ResultRows:            entry.ResultRows,
		WrittenRows:           entry.WrittenRows,
		ExceptionCode:         entry.ExceptionCode,
		Exception:             entry.Exception,
		UserName:              entry.UserName,
		OrgID:                 entry.OrgID,
		CurrentDatabase:       entry.CurrentDatabase,
		ClientAddress:         entry.ClientAddress,
		ClientPort:            entry.ClientPort,
		ApplicationName:       entry.ApplicationName,
		PID:                   entry.PID,
		WorkerID:              entry.WorkerID,
		IsTranspiled:          entry.IsTranspiled,
		Protocol:              entry.Protocol,
		TraceID:               entry.TraceID,
		SpanID:                entry.SpanID,
		PostgresScanMs:        entry.PostgresScanMs,
		CPUTimeSeconds:        entry.CPUTimeSeconds,
		PeakBufferMemoryBytes: entry.PeakBufferMemoryBytes,
	}
}

type kafkaGoQueryLogProducer struct {
	writer *kafka.Writer
}

func newKafkaGoQueryLogProducer(cfg QueryLogConfig) (*kafkaGoQueryLogProducer, error) {
	if err := validateQueryLogKafkaConfig(cfg); err != nil {
		return nil, err
	}
	clientID := strings.TrimSpace(cfg.Kafka.ClientID)
	if clientID == "" {
		clientID = "duckgres-query-log"
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	batchTimeout := cfg.FlushInterval
	if batchTimeout <= 0 {
		batchTimeout = 5 * time.Second
	}
	return &kafkaGoQueryLogProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Kafka.Brokers...),
			Topic:        cfg.Kafka.Topic,
			Balancer:     &kafka.Hash{},
			BatchSize:    batchSize,
			BatchTimeout: batchTimeout,
			RequiredAcks: kafka.RequireOne,
			Transport: &kafka.Transport{
				ClientID: clientID,
			},
		},
	}, nil
}

func (p *kafkaGoQueryLogProducer) WriteMessages(ctx context.Context, messages ...queryLogKafkaMessage) error {
	if p == nil || p.writer == nil {
		return errors.New("querylog: kafka writer is nil")
	}
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for _, message := range messages {
		if message.Topic == "" {
			return errors.New("querylog: kafka message topic is empty")
		}
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   message.Key,
			Value: message.Value,
		})
	}
	if err := p.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		return fmt.Errorf("write kafka messages: %w", err)
	}
	return nil
}

func (p *kafkaGoQueryLogProducer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
