package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const defaultQueryLogKafkaWriterGroupID = "duckgres-query-log-writer"
const queryLogWriterCredentialExpiryMargin = 5 * time.Minute
const defaultQueryLogWriterIdleTTL = 15 * time.Minute
const defaultQueryLogWriterPruneInterval = time.Minute
const defaultQueryLogWriterMaxCachedWriters = 128
const defaultQueryLogKafkaWriterRetryDelay = time.Second

// ErrQueryLogNoDuckLakeTarget means an org has no DuckLake table that can store
// query-log events.
var ErrQueryLogNoDuckLakeTarget = errors.New("querylog: no DuckLake metadata store for query log target")

// QueryLogKafkaConsumedMessage is the minimal Kafka message shape needed by
// the query-log writer. The concrete kafka-go reader keeps offsets internally;
// tests use this value to prove commits happen only after durable writes.
type QueryLogKafkaConsumedMessage struct {
	Key   []byte
	Value []byte
	raw   kafka.Message
}

type QueryLogKafkaConsumer interface {
	FetchMessage(context.Context) (QueryLogKafkaConsumedMessage, error)
	CommitMessages(context.Context, ...QueryLogKafkaConsumedMessage) error
	Close() error
}

type QueryLogEntryWriter interface {
	WriteQueryLogEntries(context.Context, []QueryLogEntry) error
	Close() error
}

type QueryLogKafkaEventWriter interface {
	QueryLogEntryWriter
	WriteQueryLogKafkaEvent(context.Context, QueryLogKafkaEvent) error
}

type QueryLogEntryWriterResolver interface {
	ResolveQueryLogEntryWriter(context.Context, string) (QueryLogEntryWriter, error)
}

// QueryLogEntryWriterInvalidator lets the Kafka writer evict a cached writer
// after a failed write, so the next retry rebuilds the DuckLake connection.
type QueryLogEntryWriterInvalidator interface {
	InvalidateQueryLogEntryWriter(string, QueryLogEntryWriter)
}

// QueryLogDuckLakeResolvedConfig is the per-org DuckLake runtime used by the
// Kafka query-log writer. CredentialsExpiresAt is set for STS-backed object
// store credentials so cached writers can be refreshed before expiry.
type QueryLogDuckLakeResolvedConfig struct {
	Config               Config
	CredentialsExpiresAt *time.Time
}

// QueryLogDuckLakeConfigResolver resolves the per-org DuckLake runtime.
// Production wires this to the config store and the same tenant activation path
// used for workers.
type QueryLogDuckLakeConfigResolver interface {
	ResolveQueryLogDuckLakeConfig(context.Context, string) (QueryLogDuckLakeResolvedConfig, error)
}

type QueryLogKafkaWriter struct {
	consumer   QueryLogKafkaConsumer
	resolver   QueryLogEntryWriterResolver
	retryDelay time.Duration
}

func NewQueryLogKafkaWriter(consumer QueryLogKafkaConsumer, resolver QueryLogEntryWriterResolver) (*QueryLogKafkaWriter, error) {
	if consumer == nil {
		return nil, errors.New("querylog: kafka consumer is nil")
	}
	if resolver == nil {
		return nil, errors.New("querylog: entry writer resolver is nil")
	}
	return &QueryLogKafkaWriter{consumer: consumer, resolver: resolver, retryDelay: defaultQueryLogKafkaWriterRetryDelay}, nil
}

func (w *QueryLogKafkaWriter) Run(ctx context.Context) error {
	if w == nil {
		return nil
	}
	for {
		msg, err := w.fetchMessage(ctx)
		if err != nil {
			if isQueryLogKafkaWriterStopError(err) {
				return err
			}
			w.recordRetryableError(err)
			if err := w.waitBeforeRetry(ctx); err != nil {
				return err
			}
			continue
		}

		for {
			if err := w.processMessage(ctx, msg); err != nil {
				if isQueryLogKafkaWriterStopError(err) {
					return err
				}
				w.recordRetryableError(err)
				if err := w.waitBeforeRetry(ctx); err != nil {
					return err
				}
				continue
			}
			break
		}
	}
}

func (w *QueryLogKafkaWriter) ProcessOne(ctx context.Context) error {
	if w == nil {
		return nil
	}
	msg, err := w.fetchMessage(ctx)
	if err != nil {
		return err
	}
	return w.processMessage(ctx, msg)
}

func (w *QueryLogKafkaWriter) fetchMessage(ctx context.Context) (QueryLogKafkaConsumedMessage, error) {
	msg, err := w.consumer.FetchMessage(ctx)
	if err != nil {
		return QueryLogKafkaConsumedMessage{}, err
	}
	queryLogKafkaWriterEvents.WithLabelValues("consumed").Inc()
	return msg, nil
}

func (w *QueryLogKafkaWriter) processMessage(ctx context.Context, msg QueryLogKafkaConsumedMessage) error {
	var event QueryLogKafkaEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		queryLogKafkaWriterEvents.WithLabelValues("dropped").Inc()
		return w.dropAndCommit(ctx, msg, fmt.Errorf("decode query log kafka event: %w", err))
	}
	if err := validateQueryLogKafkaEvent(event); err != nil {
		queryLogKafkaWriterEvents.WithLabelValues("dropped").Inc()
		return w.dropAndCommit(ctx, msg, err)
	}

	entryWriter, err := w.resolver.ResolveQueryLogEntryWriter(ctx, event.OrgID)
	if err != nil {
		if errors.Is(err, ErrQueryLogNoDuckLakeTarget) {
			queryLogKafkaWriterEvents.WithLabelValues("dropped").Inc()
			return w.dropAndCommit(ctx, msg, err)
		}
		return fmt.Errorf("resolve query log writer for org %q: %w", event.OrgID, err)
	}
	if eventWriter, ok := entryWriter.(QueryLogKafkaEventWriter); ok {
		err = eventWriter.WriteQueryLogKafkaEvent(ctx, event)
	} else {
		err = entryWriter.WriteQueryLogEntries(ctx, []QueryLogEntry{queryLogEntryFromKafkaEvent(event)})
	}
	if err != nil {
		if invalidator, ok := w.resolver.(QueryLogEntryWriterInvalidator); ok {
			invalidator.InvalidateQueryLogEntryWriter(event.OrgID, entryWriter)
		}
		return fmt.Errorf("write query log event %q for org %q: %w", event.EventID, event.OrgID, err)
	}
	queryLogKafkaWriterEvents.WithLabelValues("inserted").Inc()

	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit query log kafka event %q: %w", event.EventID, err)
	}
	queryLogKafkaWriterEvents.WithLabelValues("committed").Inc()
	return nil
}

func (w *QueryLogKafkaWriter) recordRetryableError(err error) {
	queryLogKafkaWriterEvents.WithLabelValues("failed").Inc()
	slog.Error("querylog: kafka writer failed to process event.", "error", err)
	queryLogKafkaWriterEvents.WithLabelValues("retried").Inc()
}

func (w *QueryLogKafkaWriter) waitBeforeRetry(ctx context.Context) error {
	delay := w.retryDelay
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isQueryLogKafkaWriterStopError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (w *QueryLogKafkaWriter) dropAndCommit(ctx context.Context, msg QueryLogKafkaConsumedMessage, dropErr error) error {
	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("%w; commit dropped kafka message: %v", dropErr, err)
	}
	slog.Warn("querylog: dropped invalid kafka event.", "error", dropErr)
	queryLogKafkaWriterEvents.WithLabelValues("committed").Inc()
	return nil
}

func (w *QueryLogKafkaWriter) Close() error {
	if w == nil || w.consumer == nil {
		return nil
	}
	return w.consumer.Close()
}

func validateQueryLogKafkaEvent(event QueryLogKafkaEvent) error {
	if event.SchemaVersion != queryLogKafkaSchemaVersion {
		return fmt.Errorf("querylog: unsupported kafka event schema version %d", event.SchemaVersion)
	}
	if strings.TrimSpace(event.EventID) == "" {
		return errors.New("querylog: kafka event_id is required")
	}
	if strings.TrimSpace(event.OrgID) == "" {
		return errors.New("querylog: kafka org_id is required")
	}
	if strings.TrimSpace(event.Type) == "" {
		return errors.New("querylog: kafka event type is required")
	}
	if strings.TrimSpace(event.UserName) == "" {
		return errors.New("querylog: kafka user_name is required")
	}
	return nil
}

func queryLogEntryFromKafkaEvent(event QueryLogKafkaEvent) QueryLogEntry {
	return QueryLogEntry{
		EventID:               event.EventID,
		EventTime:             event.EventTime,
		QueryDurationMs:       event.QueryDurationMs,
		Type:                  event.Type,
		Query:                 event.Query,
		TranspiledQuery:       event.TranspiledQuery,
		QueryKind:             event.QueryKind,
		NormalizedHash:        event.NormalizedHash,
		ResultRows:            event.ResultRows,
		WrittenRows:           event.WrittenRows,
		ExceptionCode:         event.ExceptionCode,
		Exception:             event.Exception,
		UserName:              event.UserName,
		OrgID:                 event.OrgID,
		CurrentDatabase:       event.CurrentDatabase,
		ClientAddress:         event.ClientAddress,
		ClientPort:            event.ClientPort,
		ApplicationName:       event.ApplicationName,
		PID:                   event.PID,
		WorkerID:              event.WorkerID,
		IsTranspiled:          event.IsTranspiled,
		Protocol:              event.Protocol,
		TraceID:               event.TraceID,
		SpanID:                event.SpanID,
		PostgresScanMs:        event.PostgresScanMs,
		CPUTimeSeconds:        event.CPUTimeSeconds,
		PeakBufferMemoryBytes: event.PeakBufferMemoryBytes,
	}
}

type QueryLogDuckLakeEntryWriter struct {
	db    *sql.DB
	table string
}

func NewQueryLogDuckLakeEntryWriter(cfg Config) (*QueryLogDuckLakeEntryWriter, error) {
	db, err := openQueryLogDuckLakeDB(cfg)
	if err != nil {
		return nil, err
	}
	return &QueryLogDuckLakeEntryWriter{db: db, table: "ducklake.system.query_log"}, nil
}

func (w *QueryLogDuckLakeEntryWriter) WriteQueryLogEntries(ctx context.Context, entries []QueryLogEntry) error {
	if w == nil || w.db == nil {
		return errors.New("querylog: ducklake entry writer is nil")
	}
	return insertQueryLogEntries(ctx, w.db, w.table, entries)
}

func (w *QueryLogDuckLakeEntryWriter) WriteQueryLogKafkaEvent(ctx context.Context, event QueryLogKafkaEvent) error {
	if w == nil || w.db == nil {
		return errors.New("querylog: ducklake entry writer is nil")
	}
	entry := queryLogEntryFromKafkaEvent(event)
	return insertQueryLogKafkaEvent(ctx, w.db, w.table, entry)
}

func (w *QueryLogDuckLakeEntryWriter) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	return w.db.Close()
}

// insertQueryLogKafkaEvent skips an event_id that is already present. This makes
// normal single-consumer Kafka replays idempotent, but it is not a storage-level
// unique constraint across multiple independent writer groups.
func insertQueryLogKafkaEvent(ctx context.Context, db *sql.DB, table string, entry QueryLogEntry) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if table == "" {
		table = "query_log"
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (event_id, event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, org_id, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, trace_id, span_id, postgres_scan_ms, cpu_time_s, peak_buffer_memory_bytes) ")
	sb.WriteString("SELECT ")
	for i := 1; i <= 27; i++ {
		if i > 1 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "$%d", i)
	}
	sb.WriteString(" WHERE NOT EXISTS (SELECT 1 FROM ")
	sb.WriteString(table)
	sb.WriteString(" WHERE event_id = $1)")

	args := append([]any{entry.EventID}, queryLogEntryInsertArgs(entry)...)
	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert query_log kafka event: %w", err)
	}
	return nil
}

type QueryLogDuckLakeEntryWriterResolver struct {
	configResolver   QueryLogDuckLakeConfigResolver
	writers          map[string]cachedQueryLogEntryWriter
	newWriter        func(Config) (QueryLogEntryWriter, error)
	now              func() time.Time
	idleTTL          time.Duration
	pruneInterval    time.Duration
	maxCachedWriters int
	mu               sync.Mutex
}

type cachedQueryLogEntryWriter struct {
	writer               QueryLogEntryWriter
	credentialsExpiresAt *time.Time
	lastUsed             time.Time
}

func NewQueryLogDuckLakeEntryWriterResolver(configResolver QueryLogDuckLakeConfigResolver) (*QueryLogDuckLakeEntryWriterResolver, error) {
	if configResolver == nil {
		return nil, errors.New("querylog: ducklake config resolver is nil")
	}
	return &QueryLogDuckLakeEntryWriterResolver{
		configResolver: configResolver,
		writers:        make(map[string]cachedQueryLogEntryWriter),
		newWriter: func(cfg Config) (QueryLogEntryWriter, error) {
			return NewQueryLogDuckLakeEntryWriter(cfg)
		},
		now:              func() time.Time { return time.Now().UTC() },
		idleTTL:          defaultQueryLogWriterIdleTTL,
		pruneInterval:    defaultQueryLogWriterPruneInterval,
		maxCachedWriters: defaultQueryLogWriterMaxCachedWriters,
	}, nil
}

// StartIdlePruner closes cached DuckLake writers that have been idle beyond the
// resolver's idle TTL, even when no new Kafka events arrive to trigger pruning.
func (r *QueryLogDuckLakeEntryWriterResolver) StartIdlePruner(ctx context.Context) {
	if r == nil || r.idleTTL <= 0 || r.pruneInterval <= 0 {
		return
	}
	ticker := time.NewTicker(r.pruneInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.pruneIdleWriters(r.now())
			}
		}
	}()
}

func (r *QueryLogDuckLakeEntryWriterResolver) ResolveQueryLogEntryWriter(ctx context.Context, orgID string) (QueryLogEntryWriter, error) {
	orgID = strings.TrimSpace(orgID)
	if strings.TrimSpace(orgID) == "" {
		return nil, errors.New("querylog: org_id is required")
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneIdleWritersLocked(now)
	if cached, ok := r.writers[orgID]; ok {
		if cached.valid(now, r.idleTTL) {
			cached.lastUsed = now
			r.writers[orgID] = cached
			return cached.writer, nil
		}
		_ = cached.writer.Close()
		delete(r.writers, orgID)
	}
	resolved, err := r.configResolver.ResolveQueryLogDuckLakeConfig(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(resolved.Config.DuckLake.MetadataStore) == "" {
		return nil, ErrQueryLogNoDuckLakeTarget
	}
	writer, err := r.newWriter(resolved.Config)
	if err != nil {
		return nil, err
	}
	r.writers[orgID] = cachedQueryLogEntryWriter{
		writer:               writer,
		credentialsExpiresAt: resolved.CredentialsExpiresAt,
		lastUsed:             now,
	}
	r.evictLeastRecentlyUsedWritersLocked()
	return writer, nil
}

func (r *QueryLogDuckLakeEntryWriterResolver) InvalidateQueryLogEntryWriter(orgID string, writer QueryLogEntryWriter) {
	if r == nil || writer == nil {
		return
	}
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	cached, ok := r.writers[orgID]
	if !ok || cached.writer != writer {
		return
	}
	_ = cached.writer.Close()
	delete(r.writers, orgID)
}

func (c cachedQueryLogEntryWriter) valid(now time.Time, idleTTL time.Duration) bool {
	if c.writer == nil {
		return false
	}
	if c.idleExpired(now, idleTTL) {
		return false
	}
	if c.credentialsExpiresAt == nil {
		return true
	}
	return now.Before(c.credentialsExpiresAt.Add(-queryLogWriterCredentialExpiryMargin))
}

func (c cachedQueryLogEntryWriter) idleExpired(now time.Time, idleTTL time.Duration) bool {
	return idleTTL > 0 && !c.lastUsed.IsZero() && !now.Before(c.lastUsed.Add(idleTTL))
}

func (r *QueryLogDuckLakeEntryWriterResolver) pruneIdleWriters(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneIdleWritersLocked(now)
}

func (r *QueryLogDuckLakeEntryWriterResolver) pruneIdleWritersLocked(now time.Time) {
	if r.idleTTL <= 0 {
		return
	}
	for orgID, cached := range r.writers {
		if cached.idleExpired(now, r.idleTTL) {
			_ = cached.writer.Close()
			delete(r.writers, orgID)
		}
	}
}

func (r *QueryLogDuckLakeEntryWriterResolver) evictLeastRecentlyUsedWritersLocked() {
	if r.maxCachedWriters <= 0 {
		return
	}
	for len(r.writers) > r.maxCachedWriters {
		var oldestOrgID string
		var oldestLastUsed time.Time
		for orgID, cached := range r.writers {
			if oldestOrgID == "" || cached.lastUsed.Before(oldestLastUsed) {
				oldestOrgID = orgID
				oldestLastUsed = cached.lastUsed
			}
		}
		if oldestOrgID == "" {
			return
		}
		_ = r.writers[oldestOrgID].writer.Close()
		delete(r.writers, oldestOrgID)
	}
}

func (r *QueryLogDuckLakeEntryWriterResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var closeErr error
	for orgID, cached := range r.writers {
		if err := cached.writer.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close query log writer for org %q: %w", orgID, err)
		}
		delete(r.writers, orgID)
	}
	return closeErr
}

type kafkaGoQueryLogConsumer struct {
	reader *kafka.Reader
}

func NewKafkaQueryLogConsumer(cfg QueryLogConfig) (*kafkaGoQueryLogConsumer, error) {
	if err := validateQueryLogKafkaWriterConfig(cfg); err != nil {
		return nil, err
	}
	groupID := strings.TrimSpace(cfg.Kafka.GroupID)
	if groupID == "" {
		groupID = defaultQueryLogKafkaWriterGroupID
	}
	clientID := strings.TrimSpace(cfg.Kafka.ClientID)
	if clientID == "" {
		clientID = "duckgres-query-log"
	}
	return &kafkaGoQueryLogConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Kafka.Brokers,
			Topic:       cfg.Kafka.Topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     time.Second,
			Dialer: &kafka.Dialer{
				ClientID: clientID,
			},
		}),
	}, nil
}

func validateQueryLogKafkaWriterConfig(cfg QueryLogConfig) error {
	if err := validateQueryLogKafkaConfig(cfg); err != nil {
		return err
	}
	return nil
}

func (c *kafkaGoQueryLogConsumer) FetchMessage(ctx context.Context) (QueryLogKafkaConsumedMessage, error) {
	if c == nil || c.reader == nil {
		return QueryLogKafkaConsumedMessage{}, errors.New("querylog: kafka reader is nil")
	}
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return QueryLogKafkaConsumedMessage{}, err
	}
	return QueryLogKafkaConsumedMessage{Key: msg.Key, Value: msg.Value, raw: msg}, nil
}

func (c *kafkaGoQueryLogConsumer) CommitMessages(ctx context.Context, messages ...QueryLogKafkaConsumedMessage) error {
	if c == nil || c.reader == nil {
		return errors.New("querylog: kafka reader is nil")
	}
	raw := make([]kafka.Message, 0, len(messages))
	for _, msg := range messages {
		raw = append(raw, msg.raw)
	}
	if err := c.reader.CommitMessages(ctx, raw...); err != nil {
		return fmt.Errorf("commit kafka messages: %w", err)
	}
	return nil
}

func (c *kafkaGoQueryLogConsumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
