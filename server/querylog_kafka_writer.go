package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
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
const defaultQueryLogKafkaWriterBatchSize = 1000
const defaultQueryLogKafkaWriterFlushInterval = 5 * time.Second

const (
	queryLogKafkaWriterOutcomeConsumed  = "consumed"
	queryLogKafkaWriterOutcomeInserted  = "inserted"
	queryLogKafkaWriterOutcomeCommitted = "committed"
	queryLogKafkaWriterOutcomeDropped   = "dropped"
	queryLogKafkaWriterOutcomeFailed    = "failed"
	queryLogKafkaWriterOutcomeRetried   = "retried"

	queryLogKafkaWriterReasonOK            = "ok"
	queryLogKafkaWriterReasonInvalidEvent  = "invalid_event"
	queryLogKafkaWriterReasonNoTarget      = "no_target"
	queryLogKafkaWriterReasonResolveFailed = "resolve_failed"
	queryLogKafkaWriterReasonWriteFailed   = "write_failed"
	queryLogKafkaWriterReasonCommitFailed  = "commit_failed"
	queryLogKafkaWriterReasonFetchFailed   = "fetch_failed"
	queryLogKafkaWriterReasonBufferFailed  = "buffer_failed"
)

// ErrQueryLogNoDuckLakeTarget means an org has no DuckLake table that can store
// query-log events.
var ErrQueryLogNoDuckLakeTarget = errors.New("querylog: no DuckLake metadata store for query log target")

// QueryLogKafkaConsumedMessage is the minimal Kafka message shape needed by
// the query-log writer. The concrete kafka-go reader keeps offsets internally;
// tests use this value to prove commits happen only after durable writes or
// explicit drops.
type QueryLogKafkaConsumedMessage struct {
	Key       []byte
	Value     []byte
	Partition int
	Offset    int64
	raw       kafka.Message
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

type QueryLogKafkaEventBatchWriter interface {
	QueryLogEntryWriter
	WriteQueryLogKafkaEvents(context.Context, []QueryLogKafkaEvent) error
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

func recordQueryLogKafkaWriterEvent(outcome, reason string) {
	recordQueryLogKafkaWriterEvents(outcome, reason, 1)
}

func recordQueryLogKafkaWriterEvents(outcome, reason string, count float64) {
	if count <= 0 {
		return
	}
	queryLogKafkaWriterEvents.WithLabelValues(outcome, reason).Add(count)
}

type QueryLogKafkaWriter struct {
	consumer      QueryLogKafkaConsumer
	resolver      QueryLogEntryWriterResolver
	retryDelay    time.Duration
	batchSize     int
	flushInterval time.Duration
	now           func() time.Time
}

func NewQueryLogKafkaWriter(consumer QueryLogKafkaConsumer, resolver QueryLogEntryWriterResolver) (*QueryLogKafkaWriter, error) {
	if consumer == nil {
		return nil, errors.New("querylog: kafka consumer is nil")
	}
	if resolver == nil {
		return nil, errors.New("querylog: entry writer resolver is nil")
	}
	return &QueryLogKafkaWriter{
		consumer:      consumer,
		resolver:      resolver,
		retryDelay:    defaultQueryLogKafkaWriterRetryDelay,
		batchSize:     defaultQueryLogKafkaWriterBatchSize,
		flushInterval: defaultQueryLogKafkaWriterFlushInterval,
		now:           func() time.Time { return time.Now().UTC() },
	}, nil
}

func (w *QueryLogKafkaWriter) ConfigureBatching(batchSize int, flushInterval time.Duration) {
	if w == nil {
		return
	}
	if batchSize <= 0 {
		batchSize = defaultQueryLogKafkaWriterBatchSize
	}
	if flushInterval <= 0 {
		flushInterval = defaultQueryLogKafkaWriterFlushInterval
	}
	w.batchSize = batchSize
	w.flushInterval = flushInterval
}

func (w *QueryLogKafkaWriter) Run(ctx context.Context) error {
	if w == nil {
		return nil
	}
	batches := newQueryLogKafkaPartitionBatches()
	for {
		msg, err := w.fetchMessageBeforeNextFlush(ctx, batches)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil && batches.hasPending() {
				if err := w.flushDuePartitionsWithRetry(ctx, batches); err != nil {
					return err
				}
				continue
			}
			if isQueryLogKafkaWriterStopError(err) && ctx.Err() == nil && batches.hasPending() {
				if flushErr := w.flushAllPartitionsWithRetry(ctx, batches); flushErr != nil {
					return flushErr
				}
			}
			if isQueryLogKafkaWriterStopError(err) {
				return err
			}
			w.recordRetryableError(queryLogKafkaWriterReasonFetchFailed, err)
			if err := w.waitBeforeRetry(ctx); err != nil {
				return err
			}
			continue
		}

		for {
			partition, ready, err := w.bufferMessage(batches, msg)
			if err != nil {
				if isQueryLogKafkaWriterStopError(err) {
					return err
				}
				w.recordRetryableError(queryLogKafkaWriterReasonBufferFailed, err)
				if err := w.waitBeforeRetry(ctx); err != nil {
					return err
				}
				continue
			}
			if ready {
				if err := w.flushPartitionWithRetry(ctx, batches, partition); err != nil {
					return err
				}
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
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeConsumed, queryLogKafkaWriterReasonOK)
	return msg, nil
}

func (w *QueryLogKafkaWriter) fetchMessageBeforeNextFlush(ctx context.Context, batches *queryLogKafkaPartitionBatches) (QueryLogKafkaConsumedMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if batches == nil || !batches.hasPending() {
		return w.fetchMessage(ctx)
	}

	now := w.currentTime()
	nextFlush := batches.nextFlushAt(w.effectiveFlushInterval())
	wait := nextFlush.Sub(now)
	if wait <= 0 {
		return QueryLogKafkaConsumedMessage{}, context.DeadlineExceeded
	}
	fetchCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	return w.fetchMessage(fetchCtx)
}

func (w *QueryLogKafkaWriter) bufferMessage(batches *queryLogKafkaPartitionBatches, msg QueryLogKafkaConsumedMessage) (int, bool, error) {
	var event QueryLogKafkaEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeDropped, queryLogKafkaWriterReasonInvalidEvent)
		batches.addDropped(msg, fmt.Errorf("decode query log kafka event: %w", err), w.currentTime())
		return msg.Partition, true, nil
	}
	if err := validateQueryLogKafkaEvent(event); err != nil {
		recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeDropped, queryLogKafkaWriterReasonInvalidEvent)
		batches.addDropped(msg, err, w.currentTime())
		return msg.Partition, true, nil
	}

	orgID := strings.TrimSpace(event.OrgID)
	batches.addEvent(msg.Partition, orgID, queryLogKafkaBufferedEvent{
		event:   event,
		entry:   queryLogEntryFromKafkaEvent(event),
		message: msg,
	}, w.currentTime())
	return msg.Partition, batches.orgLen(msg.Partition, orgID) >= w.effectiveBatchSize(), nil
}

func (w *QueryLogKafkaWriter) processMessage(ctx context.Context, msg QueryLogKafkaConsumedMessage) error {
	var event QueryLogKafkaEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeDropped, queryLogKafkaWriterReasonInvalidEvent)
		return w.dropAndCommit(ctx, msg, fmt.Errorf("decode query log kafka event: %w", err))
	}
	if err := validateQueryLogKafkaEvent(event); err != nil {
		recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeDropped, queryLogKafkaWriterReasonInvalidEvent)
		return w.dropAndCommit(ctx, msg, err)
	}

	orgID := strings.TrimSpace(event.OrgID)
	if err := w.writeOrgBatchOrDrop(ctx, orgID, []queryLogKafkaBufferedEvent{{
		event:   event,
		entry:   queryLogEntryFromKafkaEvent(event),
		message: msg,
	}}); err != nil {
		return err
	}

	if err := w.consumer.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit query log kafka event %q: %w", event.EventID, err)
	}
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeCommitted, queryLogKafkaWriterReasonOK)
	return nil
}

func (w *QueryLogKafkaWriter) flushDuePartitionsWithRetry(ctx context.Context, batches *queryLogKafkaPartitionBatches) error {
	for _, partition := range batches.duePartitions(w.currentTime(), w.effectiveFlushInterval()) {
		if err := w.flushPartitionWithRetry(ctx, batches, partition); err != nil {
			return err
		}
	}
	return nil
}

func (w *QueryLogKafkaWriter) flushAllPartitionsWithRetry(ctx context.Context, batches *queryLogKafkaPartitionBatches) error {
	for _, partition := range batches.partitions() {
		if err := w.flushPartitionWithRetry(ctx, batches, partition); err != nil {
			return err
		}
	}
	return nil
}

func (w *QueryLogKafkaWriter) flushPartitionWithRetry(ctx context.Context, batches *queryLogKafkaPartitionBatches, partition int) error {
	for {
		if err := w.flushPartition(ctx, batches, partition); err != nil {
			if isQueryLogKafkaWriterStopError(err) {
				return err
			}
			w.recordRetryableError(queryLogKafkaWriterReasonCommitFailed, err)
			if err := w.waitBeforeRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (w *QueryLogKafkaWriter) flushPartition(ctx context.Context, batches *queryLogKafkaPartitionBatches, partition int) error {
	batch := batches.get(partition)
	if batch == nil || len(batch.messages) == 0 {
		return nil
	}

	for _, dropped := range batch.dropped {
		slog.Warn("querylog: dropped invalid kafka event.", "error", dropped.err, "partition", partition, "offset", dropped.message.Offset)
	}
	for _, orgID := range batch.orgIDs() {
		if err := w.writeOrgBatchOrDrop(ctx, orgID, batch.byOrg[orgID]); err != nil {
			return err
		}
	}

	if err := w.consumer.CommitMessages(ctx, batch.messages...); err != nil {
		return fmt.Errorf("commit query log kafka partition %d batch: %w", partition, err)
	}
	recordQueryLogKafkaWriterEvents(queryLogKafkaWriterOutcomeCommitted, queryLogKafkaWriterReasonOK, float64(len(batch.messages)))
	batches.remove(partition)
	return nil
}

func (w *QueryLogKafkaWriter) writeOrgBatchOrDrop(ctx context.Context, orgID string, batch []queryLogKafkaBufferedEvent) error {
	if len(batch) == 0 {
		return nil
	}

	entryWriter, err := w.resolver.ResolveQueryLogEntryWriter(ctx, orgID)
	if err != nil {
		if errors.Is(err, ErrQueryLogNoDuckLakeTarget) {
			recordQueryLogKafkaWriterEvents(queryLogKafkaWriterOutcomeDropped, queryLogKafkaWriterReasonNoTarget, float64(len(batch)))
			slog.Warn("querylog: dropped kafka events for org without DuckLake query-log target.", "org_id", orgID, "batch_size", len(batch))
			return nil
		}
		if isQueryLogKafkaWriterStopError(err) {
			return err
		}
		w.recordDroppedOrgBatchError(queryLogKafkaWriterReasonResolveFailed, orgID, len(batch), fmt.Errorf("resolve query log writer for org %q: %w", orgID, err))
		return nil
	}

	if eventBatchWriter, ok := entryWriter.(QueryLogKafkaEventBatchWriter); ok {
		err = eventBatchWriter.WriteQueryLogKafkaEvents(ctx, queryLogKafkaEvents(batch))
	} else if eventWriter, ok := entryWriter.(QueryLogKafkaEventWriter); ok {
		for _, item := range batch {
			if err = eventWriter.WriteQueryLogKafkaEvent(ctx, item.event); err != nil {
				break
			}
		}
	} else {
		err = entryWriter.WriteQueryLogEntries(ctx, queryLogKafkaEntries(batch))
	}
	if err != nil {
		if invalidator, ok := w.resolver.(QueryLogEntryWriterInvalidator); ok {
			invalidator.InvalidateQueryLogEntryWriter(orgID, entryWriter)
		}
		if isQueryLogKafkaWriterStopError(err) {
			return err
		}
		w.recordDroppedOrgBatchError(queryLogKafkaWriterReasonWriteFailed, orgID, len(batch), fmt.Errorf("write query log batch for org %q: %w", orgID, err))
		return nil
	}
	recordQueryLogKafkaWriterEvents(queryLogKafkaWriterOutcomeInserted, queryLogKafkaWriterReasonOK, float64(len(batch)))
	return nil
}

func (w *QueryLogKafkaWriter) recordDroppedOrgBatchError(reason, orgID string, batchSize int, err error) {
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeFailed, reason)
	recordQueryLogKafkaWriterEvents(queryLogKafkaWriterOutcomeDropped, reason, float64(batchSize))
	slog.Error("querylog: dropped query-log batch after write failure.", "org_id", orgID, "batch_size", batchSize, "error", RedactQueryLogWriterError(err))
}

func (w *QueryLogKafkaWriter) recordRetryableError(reason string, err error) {
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeFailed, reason)
	slog.Error("querylog: kafka writer failed to process event.", "error", RedactQueryLogWriterError(err))
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeRetried, reason)
}

var (
	queryLogWriterDuckLakeAttachRE = regexp.MustCompile(`(?is)ATTACH\s+'ducklake:(?:''|[^'])*'`)
	queryLogWriterSecretLiteralRE  = regexp.MustCompile(`(?i)\b(KEY_ID|SECRET|SESSION_TOKEN|CLIENT_SECRET)\s+'(?:''|[^'])*'`)
	queryLogWriterSecretKVRE       = regexp.MustCompile(`(?i)\b(PASSWORD|TOKEN|ACCESS_KEY_ID|SECRET_ACCESS_KEY|AWS_SECRET_ACCESS_KEY|KEY_ID|SECRET|SESSION_TOKEN|CLIENT_SECRET)(\s*(?:=|:|=>)\s*)("[^"]*"|'(?:''|[^'])*'|[^\s,;)]+)`)
)

// RedactQueryLogWriterError returns a log-safe error string for the privileged
// query-log writer. Writer failures can wrap DuckDB/DuckLake errors that echo
// generated ATTACH or CREATE SECRET SQL, so redact at the logging boundary
// without changing the original error used for retry/control-flow decisions.
func RedactQueryLogWriterError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = queryLogWriterDuckLakeAttachRE.ReplaceAllString(msg, "ATTACH 'ducklake:[REDACTED]'")
	msg = queryLogWriterSecretLiteralRE.ReplaceAllString(msg, "${1} '[REDACTED]'")
	msg = queryLogWriterSecretKVRE.ReplaceAllString(msg, "${1}${2}[REDACTED]")
	msg = RedactSecrets(msg)
	return msg
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
	recordQueryLogKafkaWriterEvent(queryLogKafkaWriterOutcomeCommitted, queryLogKafkaWriterReasonOK)
	return nil
}

func (w *QueryLogKafkaWriter) effectiveBatchSize() int {
	if w == nil || w.batchSize <= 0 {
		return defaultQueryLogKafkaWriterBatchSize
	}
	return w.batchSize
}

func (w *QueryLogKafkaWriter) effectiveFlushInterval() time.Duration {
	if w == nil || w.flushInterval <= 0 {
		return defaultQueryLogKafkaWriterFlushInterval
	}
	return w.flushInterval
}

func (w *QueryLogKafkaWriter) currentTime() time.Time {
	if w == nil || w.now == nil {
		return time.Now().UTC()
	}
	return w.now()
}

type queryLogKafkaBufferedEvent struct {
	event   QueryLogKafkaEvent
	entry   QueryLogEntry
	message QueryLogKafkaConsumedMessage
}

type queryLogKafkaDroppedMessage struct {
	message QueryLogKafkaConsumedMessage
	err     error
}

type queryLogKafkaPartitionBatch struct {
	firstBufferedAt time.Time
	messages        []QueryLogKafkaConsumedMessage
	byOrg           map[string][]queryLogKafkaBufferedEvent
	dropped         []queryLogKafkaDroppedMessage
}

func (b *queryLogKafkaPartitionBatch) orgIDs() []string {
	orgIDs := make([]string, 0, len(b.byOrg))
	for orgID, events := range b.byOrg {
		if len(events) > 0 {
			orgIDs = append(orgIDs, orgID)
		}
	}
	sort.Strings(orgIDs)
	return orgIDs
}

type queryLogKafkaPartitionBatches struct {
	byPartition map[int]*queryLogKafkaPartitionBatch
}

func newQueryLogKafkaPartitionBatches() *queryLogKafkaPartitionBatches {
	return &queryLogKafkaPartitionBatches{byPartition: make(map[int]*queryLogKafkaPartitionBatch)}
}

func (b *queryLogKafkaPartitionBatches) partitionBatch(partition int, now time.Time) *queryLogKafkaPartitionBatch {
	batch := b.byPartition[partition]
	if batch == nil {
		batch = &queryLogKafkaPartitionBatch{
			firstBufferedAt: now,
			byOrg:           make(map[string][]queryLogKafkaBufferedEvent),
		}
		b.byPartition[partition] = batch
	}
	return batch
}

func (b *queryLogKafkaPartitionBatches) addEvent(partition int, orgID string, item queryLogKafkaBufferedEvent, now time.Time) {
	batch := b.partitionBatch(partition, now)
	batch.messages = append(batch.messages, item.message)
	batch.byOrg[orgID] = append(batch.byOrg[orgID], item)
}

func (b *queryLogKafkaPartitionBatches) addDropped(msg QueryLogKafkaConsumedMessage, err error, now time.Time) {
	batch := b.partitionBatch(msg.Partition, now)
	batch.messages = append(batch.messages, msg)
	batch.dropped = append(batch.dropped, queryLogKafkaDroppedMessage{message: msg, err: err})
}

func (b *queryLogKafkaPartitionBatches) orgLen(partition int, orgID string) int {
	batch := b.byPartition[partition]
	if batch == nil {
		return 0
	}
	return len(batch.byOrg[orgID])
}

func (b *queryLogKafkaPartitionBatches) hasPending() bool {
	for _, batch := range b.byPartition {
		if len(batch.messages) > 0 {
			return true
		}
	}
	return false
}

func (b *queryLogKafkaPartitionBatches) get(partition int) *queryLogKafkaPartitionBatch {
	return b.byPartition[partition]
}

func (b *queryLogKafkaPartitionBatches) remove(partition int) {
	delete(b.byPartition, partition)
}

func (b *queryLogKafkaPartitionBatches) nextFlushAt(flushInterval time.Duration) time.Time {
	var next time.Time
	for _, batch := range b.byPartition {
		if len(batch.messages) == 0 {
			continue
		}
		flushAt := batch.firstBufferedAt.Add(flushInterval)
		if next.IsZero() || flushAt.Before(next) {
			next = flushAt
		}
	}
	return next
}

func (b *queryLogKafkaPartitionBatches) duePartitions(now time.Time, flushInterval time.Duration) []int {
	partitions := make([]int, 0, len(b.byPartition))
	for partition, batch := range b.byPartition {
		if len(batch.messages) == 0 {
			continue
		}
		if !now.Before(batch.firstBufferedAt.Add(flushInterval)) {
			partitions = append(partitions, partition)
		}
	}
	sort.Ints(partitions)
	return partitions
}

func (b *queryLogKafkaPartitionBatches) partitions() []int {
	partitions := make([]int, 0, len(b.byPartition))
	for partition, batch := range b.byPartition {
		if len(batch.messages) > 0 {
			partitions = append(partitions, partition)
		}
	}
	sort.Ints(partitions)
	return partitions
}

func queryLogKafkaEvents(batch []queryLogKafkaBufferedEvent) []QueryLogKafkaEvent {
	events := make([]QueryLogKafkaEvent, 0, len(batch))
	for _, item := range batch {
		events = append(events, item.event)
	}
	return events
}

func queryLogKafkaEntries(batch []queryLogKafkaBufferedEvent) []QueryLogEntry {
	entries := make([]QueryLogEntry, 0, len(batch))
	for _, item := range batch {
		entries = append(entries, item.entry)
	}
	return entries
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

func (w *QueryLogDuckLakeEntryWriter) WriteQueryLogKafkaEvents(ctx context.Context, events []QueryLogKafkaEvent) error {
	if w == nil || w.db == nil {
		return errors.New("querylog: ducklake entry writer is nil")
	}
	entries := make([]QueryLogEntry, 0, len(events))
	for _, event := range events {
		entries = append(entries, queryLogEntryFromKafkaEvent(event))
	}
	return insertQueryLogKafkaEvents(ctx, w.db, w.table, entries)
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
	if err := insertQueryLogKafkaEvents(ctx, db, table, []QueryLogEntry{entry}); err != nil {
		return fmt.Errorf("insert query_log kafka event: %w", err)
	}
	return nil
}

func insertQueryLogKafkaEvents(ctx context.Context, db *sql.DB, table string, entries []QueryLogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	entries = dedupeQueryLogEntriesByEventID(entries)
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
	sb.WriteString("SELECT incoming.event_id, incoming.event_time, incoming.query_duration_ms, incoming.type, incoming.query, incoming.transpiled_query, incoming.query_kind, incoming.normalized_query_hash, incoming.result_rows, incoming.written_rows, incoming.exception_code, incoming.exception, incoming.user_name, incoming.org_id, incoming.current_database, incoming.client_address, incoming.client_port, incoming.application_name, incoming.pid, incoming.worker_id, incoming.is_transpiled, incoming.protocol, incoming.trace_id, incoming.span_id, incoming.postgres_scan_ms, incoming.cpu_time_s, incoming.peak_buffer_memory_bytes FROM (VALUES ")

	const colsPerKafkaRow = 27
	args := make([]any, 0, len(entries)*colsPerKafkaRow)
	for i, entry := range entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * colsPerKafkaRow
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20,
			base+21, base+22, base+23, base+24, base+25, base+26, base+27)
		args = append(args, append([]any{entry.EventID}, queryLogEntryInsertArgs(entry)...)...)
	}
	sb.WriteString(") AS incoming(event_id, event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, org_id, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, trace_id, span_id, postgres_scan_ms, cpu_time_s, peak_buffer_memory_bytes)")
	sb.WriteString(" WHERE NOT EXISTS (SELECT 1 FROM ")
	sb.WriteString(table)
	sb.WriteString(" AS existing WHERE existing.event_id = incoming.event_id)")

	if _, err := db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("insert query_log kafka events: %w", err)
	}
	return nil
}

func dedupeQueryLogEntriesByEventID(entries []QueryLogEntry) []QueryLogEntry {
	if len(entries) < 2 {
		return entries
	}
	deduped := make([]QueryLogEntry, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if _, ok := seen[entry.EventID]; ok {
			continue
		}
		seen[entry.EventID] = struct{}{}
		deduped = append(deduped, entry)
	}
	return deduped
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
	return QueryLogKafkaConsumedMessage{Key: msg.Key, Value: msg.Value, Partition: msg.Partition, Offset: msg.Offset, raw: msg}, nil
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
