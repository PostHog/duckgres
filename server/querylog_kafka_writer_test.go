package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type fakeQueryLogKafkaConsumer struct {
	messages      []QueryLogKafkaConsumedMessage
	commits       []QueryLogKafkaConsumedMessage
	commitBatches [][]QueryLogKafkaConsumedMessage
	commitErrs    []error
	onCommit      func()
	closed        bool
}

func queryLogKafkaWriterEventMetricValue(t *testing.T, outcome, reason string) float64 {
	t.Helper()
	var metric dto.Metric
	if err := queryLogKafkaWriterEvents.WithLabelValues(outcome, reason).Write(&metric); err != nil {
		t.Fatalf("read query-log writer metric %s/%s: %v", outcome, reason, err)
	}
	if metric.Counter == nil {
		t.Fatalf("query-log writer metric %s/%s is not a counter", outcome, reason)
	}
	return metric.Counter.GetValue()
}

func (c *fakeQueryLogKafkaConsumer) FetchMessage(context.Context) (QueryLogKafkaConsumedMessage, error) {
	if len(c.messages) == 0 {
		return QueryLogKafkaConsumedMessage{}, context.Canceled
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

func (c *fakeQueryLogKafkaConsumer) CommitMessages(_ context.Context, messages ...QueryLogKafkaConsumedMessage) error {
	if len(c.commitErrs) > 0 {
		err := c.commitErrs[0]
		c.commitErrs = c.commitErrs[1:]
		if err != nil {
			return err
		}
	}
	batch := append([]QueryLogKafkaConsumedMessage(nil), messages...)
	c.commitBatches = append(c.commitBatches, batch)
	c.commits = append(c.commits, messages...)
	if c.onCommit != nil {
		c.onCommit()
	}
	return nil
}

func (c *fakeQueryLogKafkaConsumer) Close() error {
	c.closed = true
	return nil
}

type blockingAfterMessagesQueryLogKafkaConsumer struct {
	fakeQueryLogKafkaConsumer
}

func (c *blockingAfterMessagesQueryLogKafkaConsumer) FetchMessage(ctx context.Context) (QueryLogKafkaConsumedMessage, error) {
	if len(c.messages) > 0 {
		msg := c.messages[0]
		c.messages = c.messages[1:]
		return msg, nil
	}
	<-ctx.Done()
	return QueryLogKafkaConsumedMessage{}, ctx.Err()
}

type fakeQueryLogEntryWriterResolver struct {
	writer      *fakeQueryLogEntryWriter
	writers     map[string]*fakeQueryLogEntryWriter
	orgs        []string
	invalidated []struct {
		orgID  string
		writer QueryLogEntryWriter
	}
}

func (r *fakeQueryLogEntryWriterResolver) ResolveQueryLogEntryWriter(_ context.Context, orgID string) (QueryLogEntryWriter, error) {
	r.orgs = append(r.orgs, orgID)
	if r.writers != nil {
		if writer, ok := r.writers[orgID]; ok {
			return writer, nil
		}
	}
	return r.writer, nil
}

func (r *fakeQueryLogEntryWriterResolver) InvalidateQueryLogEntryWriter(orgID string, writer QueryLogEntryWriter) {
	r.invalidated = append(r.invalidated, struct {
		orgID  string
		writer QueryLogEntryWriter
	}{orgID: orgID, writer: writer})
}

type fakeQueryLogEntryWriter struct {
	entries []QueryLogEntry
	batches [][]QueryLogEntry
	err     error
	errs    []error
	closed  int
}

func (w *fakeQueryLogEntryWriter) WriteQueryLogEntries(_ context.Context, entries []QueryLogEntry) error {
	if len(w.errs) > 0 {
		err := w.errs[0]
		w.errs = w.errs[1:]
		if err != nil {
			return err
		}
	}
	if w.err != nil {
		return w.err
	}
	batch := append([]QueryLogEntry(nil), entries...)
	w.batches = append(w.batches, batch)
	w.entries = append(w.entries, entries...)
	return nil
}

func (w *fakeQueryLogEntryWriter) Close() error {
	w.closed++
	return nil
}

type fakeDuckLakeConfigResolver struct {
	resolved QueryLogDuckLakeResolvedConfig
	calls    int
}

func (r *fakeDuckLakeConfigResolver) ResolveQueryLogDuckLakeConfig(context.Context, string) (QueryLogDuckLakeResolvedConfig, error) {
	r.calls++
	return r.resolved, nil
}

func TestQueryLogKafkaWriterWritesAndCommitsAfterSuccess(t *testing.T) {
	event := QueryLogKafkaEvent{
		SchemaVersion:         queryLogKafkaSchemaVersion,
		EventID:               "evt-1",
		EmittedAt:             time.Unix(1700000001, 0).UTC(),
		EventTime:             time.Unix(1700000000, 0).UTC(),
		QueryDurationMs:       42,
		Type:                  "QueryFinish",
		Query:                 "SELECT 1",
		QueryKind:             "Select",
		NormalizedHash:        99,
		ResultRows:            1,
		UserName:              "alice",
		OrgID:                 "org-a",
		CurrentDatabase:       "ducklake",
		PostgresScanMs:        7,
		CPUTimeSeconds:        1.25,
		PeakBufferMemoryBytes: 2048,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
	}
	entryWriter := &fakeQueryLogEntryWriter{}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	if len(entryWriter.entries) != 1 {
		t.Fatalf("expected one written query log entry, got %d", len(entryWriter.entries))
	}
	got := entryWriter.entries[0]
	if got.Query != "SELECT 1" || got.OrgID != "org-a" || got.UserName != "alice" {
		t.Fatalf("unexpected written entry: %#v", got)
	}
	if got.CPUTimeSeconds != 1.25 || got.PeakBufferMemoryBytes != 2048 || got.PostgresScanMs != 7 {
		t.Fatalf("resource fields did not round-trip: %#v", got)
	}
	if len(consumer.commits) != 1 {
		t.Fatalf("expected one committed Kafka message, got %d", len(consumer.commits))
	}
}

func TestQueryLogKafkaWriterCommitsDroppedWriteFailure(t *testing.T) {
	payload, err := json.Marshal(QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-1",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
	}
	entryWriter := &fakeQueryLogEntryWriter{err: errors.New("write failed")}
	resolver := &fakeQueryLogEntryWriterResolver{writer: entryWriter}
	writer, err := NewQueryLogKafkaWriter(consumer, resolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("expected ProcessOne to drop and commit write failure, got %v", err)
	}
	if len(entryWriter.entries) != 0 {
		t.Fatalf("expected failed event to be dropped, got %d written entries", len(entryWriter.entries))
	}
	if len(consumer.commits) != 1 {
		t.Fatalf("expected dropped write failure to commit, got %d commits", len(consumer.commits))
	}
	if len(resolver.invalidated) != 1 {
		t.Fatalf("expected failed writer to be invalidated, got %d invalidations", len(resolver.invalidated))
	}
	if resolver.invalidated[0].orgID != "org-a" || resolver.invalidated[0].writer != entryWriter {
		t.Fatalf("unexpected invalidation: %#v", resolver.invalidated[0])
	}
}

func TestQueryLogKafkaWriterDroppedWriteFailureMetricHasReason(t *testing.T) {
	beforeDroppedWriteFailed := queryLogKafkaWriterEventMetricValue(t, "dropped", "write_failed")
	beforeFailedWriteFailed := queryLogKafkaWriterEventMetricValue(t, "failed", "write_failed")
	beforeDroppedInvalid := queryLogKafkaWriterEventMetricValue(t, "dropped", "invalid_event")

	payload, err := json.Marshal(QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-1",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
	}
	entryWriter := &fakeQueryLogEntryWriter{err: errors.New("write failed")}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	if got := queryLogKafkaWriterEventMetricValue(t, "dropped", "write_failed"); got != beforeDroppedWriteFailed+1 {
		t.Fatalf("dropped/write_failed = %v, want %v", got, beforeDroppedWriteFailed+1)
	}
	if got := queryLogKafkaWriterEventMetricValue(t, "failed", "write_failed"); got != beforeFailedWriteFailed+1 {
		t.Fatalf("failed/write_failed = %v, want %v", got, beforeFailedWriteFailed+1)
	}
	if got := queryLogKafkaWriterEventMetricValue(t, "dropped", "invalid_event"); got != beforeDroppedInvalid {
		t.Fatalf("dropped/invalid_event = %v, want unchanged %v", got, beforeDroppedInvalid)
	}
}

func TestRedactQueryLogWriterErrorScrubsAttachAndSecretMaterial(t *testing.T) {
	err := errors.New("querylog: attach ducklake: Parser Error: ATTACH 'ducklake:postgres:host=db user=metadata password=metadata-secret dbname=ducklake' AS ducklake; CREATE OR REPLACE SECRET ducklake_s3 (TYPE s3, KEY_ID 'access-key', SECRET 's3-secret', SESSION_TOKEN 'session-token'); CLIENT_SECRET 'oauth-secret'")

	got := RedactQueryLogWriterError(err)
	for _, secret := range []string{
		"metadata-secret",
		"access-key",
		"s3-secret",
		"session-token",
		"oauth-secret",
		"ducklake:postgres:host=db",
	} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted error still contains %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("redacted error should contain redaction marker, got %s", got)
	}
}

func TestQueryLogKafkaWriterRetryLogRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	writer := &QueryLogKafkaWriter{}
	writer.recordRetryableError(queryLogKafkaWriterReasonWriteFailed, errors.New("write failed: ATTACH 'ducklake:postgres:host=db user=metadata password=metadata-secret dbname=ducklake' AS ducklake; CREATE SECRET ducklake_s3 (TYPE s3, SECRET 's3-secret', SESSION_TOKEN 'session-token')"))

	got := buf.String()
	for _, secret := range []string{"metadata-secret", "s3-secret", "session-token", "ducklake:postgres:host=db"} {
		if strings.Contains(got, secret) {
			t.Fatalf("retry log still contains %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("retry log should contain redaction marker, got %s", got)
	}
}

func TestQueryLogKafkaWriterRunDropsFailedWriteBeforeFetchingNext(t *testing.T) {
	first := QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-first",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	}
	second := QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-second",
		EventTime:     time.Unix(1700000001, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 2",
		UserName:      "alice",
		OrgID:         "org-a",
	}
	firstPayload, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first event: %v", err)
	}
	secondPayload, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{
			{Value: firstPayload},
			{Value: secondPayload},
		},
	}
	entryWriter := &fakeQueryLogEntryWriter{
		errs: []error{errors.New("transient write failure"), nil, nil},
	}
	resolver := &fakeQueryLogEntryWriterResolver{writer: entryWriter}
	writer, err := NewQueryLogKafkaWriter(consumer, resolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 1
	writer.retryDelay = 0

	err = writer.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after fake consumer drains", err)
	}
	if len(entryWriter.entries) != 1 {
		t.Fatalf("expected failed event to be dropped and second event to be written, got %d entries", len(entryWriter.entries))
	}
	if entryWriter.entries[0].EventID != "evt-second" {
		t.Fatalf("unexpected write order: %#v", entryWriter.entries)
	}
	if len(resolver.invalidated) != 1 || resolver.invalidated[0].orgID != "org-a" {
		t.Fatalf("expected failed writer to be invalidated once, got %#v", resolver.invalidated)
	}
	if len(consumer.commits) != 2 {
		t.Fatalf("expected dropped and written messages to be committed, got %d commits", len(consumer.commits))
	}
}

func TestQueryLogKafkaWriterRunRetriesCommitFailureBeforeFetchingNext(t *testing.T) {
	first := QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-first",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	}
	second := QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-second",
		EventTime:     time.Unix(1700000001, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 2",
		UserName:      "alice",
		OrgID:         "org-a",
	}
	firstPayload, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first event: %v", err)
	}
	secondPayload, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{
			{Value: firstPayload},
			{Value: secondPayload},
		},
		commitErrs: []error{errors.New("transient commit failure"), nil, nil},
	}
	entryWriter := &fakeQueryLogEntryWriter{}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 1
	writer.retryDelay = 0

	err = writer.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after fake consumer drains", err)
	}
	if len(entryWriter.entries) != 3 {
		t.Fatalf("expected failed commit to retry the same event before the next fetch, got %d entries", len(entryWriter.entries))
	}
	if entryWriter.entries[0].EventID != "evt-first" || entryWriter.entries[1].EventID != "evt-first" || entryWriter.entries[2].EventID != "evt-second" {
		t.Fatalf("unexpected write order: %#v", entryWriter.entries)
	}
	if len(consumer.commits) != 2 {
		t.Fatalf("expected two successful commits after retry, got %d commits", len(consumer.commits))
	}
}

func TestQueryLogKafkaWriterRunBatchesMessagesByOrg(t *testing.T) {
	events := []QueryLogKafkaEvent{
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-a-1",
			EventTime:     time.Unix(1700000000, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 1",
			UserName:      "alice",
			OrgID:         "org-a",
		},
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-b-1",
			EventTime:     time.Unix(1700000001, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 2",
			UserName:      "bob",
			OrgID:         "org-b",
		},
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-a-2",
			EventTime:     time.Unix(1700000002, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 3",
			UserName:      "alice",
			OrgID:         "org-a",
		},
	}

	messages := make([]QueryLogKafkaConsumedMessage, 0, len(events))
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal event %q: %v", event.EventID, err)
		}
		messages = append(messages, QueryLogKafkaConsumedMessage{Value: payload, Partition: 0, Offset: int64(len(messages))})
	}
	consumer := &fakeQueryLogKafkaConsumer{messages: messages}
	writerA := &fakeQueryLogEntryWriter{}
	writerB := &fakeQueryLogEntryWriter{}
	resolver := &fakeQueryLogEntryWriterResolver{
		writers: map[string]*fakeQueryLogEntryWriter{
			"org-a": writerA,
			"org-b": writerB,
		},
	}
	writer, err := NewQueryLogKafkaWriter(consumer, resolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 2
	writer.flushInterval = time.Hour
	writer.retryDelay = 0

	err = writer.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after fake consumer drains", err)
	}

	if len(writerA.batches) != 1 {
		t.Fatalf("expected org-a to be written in one batch, got %d batches: %#v", len(writerA.batches), writerA.batches)
	}
	if got := []string{writerA.batches[0][0].EventID, writerA.batches[0][1].EventID}; got[0] != "evt-a-1" || got[1] != "evt-a-2" {
		t.Fatalf("unexpected org-a batch event IDs: %#v", got)
	}
	if len(writerB.batches) != 1 || len(writerB.batches[0]) != 1 || writerB.batches[0][0].EventID != "evt-b-1" {
		t.Fatalf("unexpected org-b batches: %#v", writerB.batches)
	}
	if len(consumer.commitBatches) != 1 {
		t.Fatalf("expected one partition-safe Kafka commit call, got %d", len(consumer.commitBatches))
	}
	if len(consumer.commitBatches[0]) != 3 {
		t.Fatalf("expected all buffered partition messages to commit together, got %d messages", len(consumer.commitBatches[0]))
	}
}

func TestQueryLogKafkaWriterRunKeepsKafkaPartitionsIndependent(t *testing.T) {
	events := []struct {
		event     QueryLogKafkaEvent
		partition int
		offset    int64
	}{
		{
			event: QueryLogKafkaEvent{
				SchemaVersion: queryLogKafkaSchemaVersion,
				EventID:       "evt-a-1",
				EventTime:     time.Unix(1700000000, 0).UTC(),
				Type:          "QueryFinish",
				Query:         "SELECT 1",
				UserName:      "alice",
				OrgID:         "org-a",
			},
			partition: 0,
			offset:    0,
		},
		{
			event: QueryLogKafkaEvent{
				SchemaVersion: queryLogKafkaSchemaVersion,
				EventID:       "evt-b-1",
				EventTime:     time.Unix(1700000001, 0).UTC(),
				Type:          "QueryFinish",
				Query:         "SELECT 2",
				UserName:      "bob",
				OrgID:         "org-b",
			},
			partition: 1,
			offset:    0,
		},
		{
			event: QueryLogKafkaEvent{
				SchemaVersion: queryLogKafkaSchemaVersion,
				EventID:       "evt-a-2",
				EventTime:     time.Unix(1700000002, 0).UTC(),
				Type:          "QueryFinish",
				Query:         "SELECT 3",
				UserName:      "alice",
				OrgID:         "org-a",
			},
			partition: 0,
			offset:    1,
		},
	}

	messages := make([]QueryLogKafkaConsumedMessage, 0, len(events))
	for _, item := range events {
		payload, err := json.Marshal(item.event)
		if err != nil {
			t.Fatalf("marshal event %q: %v", item.event.EventID, err)
		}
		messages = append(messages, QueryLogKafkaConsumedMessage{Value: payload, Partition: item.partition, Offset: item.offset})
	}
	consumer := &fakeQueryLogKafkaConsumer{messages: messages}
	writerA := &fakeQueryLogEntryWriter{}
	writerB := &fakeQueryLogEntryWriter{}
	resolver := &fakeQueryLogEntryWriterResolver{
		writers: map[string]*fakeQueryLogEntryWriter{
			"org-a": writerA,
			"org-b": writerB,
		},
	}
	writer, err := NewQueryLogKafkaWriter(consumer, resolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 2
	writer.flushInterval = time.Hour
	writer.retryDelay = 0

	err = writer.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after fake consumer drains", err)
	}

	if len(writerA.batches) != 1 || len(writerA.batches[0]) != 2 {
		t.Fatalf("expected org-a partition 0 batch to write first, got %#v", writerA.batches)
	}
	if len(writerB.batches) != 1 || len(writerB.batches[0]) != 1 {
		t.Fatalf("expected org-b partition 1 batch to write on drain, got %#v", writerB.batches)
	}
	if len(consumer.commitBatches) != 2 {
		t.Fatalf("expected one commit per partition, got %#v", consumer.commitBatches)
	}
	if got := consumer.commitBatches[0]; len(got) != 2 || got[0].Partition != 0 || got[1].Partition != 0 {
		t.Fatalf("expected first commit to contain only partition 0 messages, got %#v", got)
	}
	if got := consumer.commitBatches[1]; len(got) != 1 || got[0].Partition != 1 {
		t.Fatalf("expected second commit to contain only partition 1 message, got %#v", got)
	}
}

func TestQueryLogKafkaWriterRunDropsFailedOrgBatchAndCommitsPartition(t *testing.T) {
	events := []QueryLogKafkaEvent{
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-a-1",
			EventTime:     time.Unix(1700000000, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 1",
			UserName:      "alice",
			OrgID:         "org-a",
		},
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-b-1",
			EventTime:     time.Unix(1700000001, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 2",
			UserName:      "bob",
			OrgID:         "org-b",
		},
		{
			SchemaVersion: queryLogKafkaSchemaVersion,
			EventID:       "evt-a-2",
			EventTime:     time.Unix(1700000002, 0).UTC(),
			Type:          "QueryFinish",
			Query:         "SELECT 3",
			UserName:      "alice",
			OrgID:         "org-a",
		},
	}

	messages := make([]QueryLogKafkaConsumedMessage, 0, len(events))
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal event %q: %v", event.EventID, err)
		}
		messages = append(messages, QueryLogKafkaConsumedMessage{Value: payload, Partition: 0, Offset: int64(len(messages))})
	}
	consumer := &fakeQueryLogKafkaConsumer{messages: messages}
	writerA := &fakeQueryLogEntryWriter{err: errors.New("ducklake write failed")}
	writerB := &fakeQueryLogEntryWriter{}
	resolver := &fakeQueryLogEntryWriterResolver{
		writers: map[string]*fakeQueryLogEntryWriter{
			"org-a": writerA,
			"org-b": writerB,
		},
	}
	writer, err := NewQueryLogKafkaWriter(consumer, resolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 2
	writer.flushInterval = time.Hour
	writer.retryDelay = 0

	err = writer.Run(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after fake consumer drains", err)
	}

	if len(writerA.entries) != 0 {
		t.Fatalf("expected failed org-a batch to be dropped, got %d written entries", len(writerA.entries))
	}
	if len(writerB.batches) != 1 || len(writerB.batches[0]) != 1 || writerB.batches[0][0].EventID != "evt-b-1" {
		t.Fatalf("expected org-b batch to write despite org-a failure, got %#v", writerB.batches)
	}
	if len(resolver.invalidated) != 1 || resolver.invalidated[0].orgID != "org-a" {
		t.Fatalf("expected failed org-a writer to be invalidated, got %#v", resolver.invalidated)
	}
	if len(consumer.commitBatches) != 1 || len(consumer.commitBatches[0]) != 3 {
		t.Fatalf("expected whole partition to commit after write/drop handling, got %#v", consumer.commitBatches)
	}
}

func TestQueryLogKafkaWriterRunFlushesOrgBatchOnInterval(t *testing.T) {
	event := QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-a-1",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := &blockingAfterMessagesQueryLogKafkaConsumer{
		fakeQueryLogKafkaConsumer: fakeQueryLogKafkaConsumer{
			messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
			onCommit: cancel,
		},
	}
	entryWriter := &fakeQueryLogEntryWriter{}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}
	writer.batchSize = 1000
	writer.flushInterval = time.Millisecond
	writer.retryDelay = 0

	err = writer.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled after test cancel", err)
	}
	if len(entryWriter.batches) != 1 || len(entryWriter.batches[0]) != 1 {
		t.Fatalf("expected one time-flushed entry batch, got %#v", entryWriter.batches)
	}
	if len(consumer.commitBatches) != 1 || len(consumer.commitBatches[0]) != 1 {
		t.Fatalf("expected one committed Kafka batch, got %#v", consumer.commitBatches)
	}
}

func TestQueryLogKafkaWriterCommitsDroppedInvalidEvent(t *testing.T) {
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: []byte(`{"schema_version":999}`)}},
	}
	entryWriter := &fakeQueryLogEntryWriter{}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("expected dropped invalid event to be committed without retry, got %v", err)
	}
	if len(entryWriter.entries) != 0 {
		t.Fatalf("expected invalid event not to write entries, got %d", len(entryWriter.entries))
	}
	if len(consumer.commits) != 1 {
		t.Fatalf("expected invalid event to be committed after drop, got %d commits", len(consumer.commits))
	}
}

func TestQueryLogKafkaWriterAllowsEmptyQueryText(t *testing.T) {
	payload, err := json.Marshal(QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-empty-query",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "",
		UserName:      "alice",
		OrgID:         "org-a",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
	}
	entryWriter := &fakeQueryLogEntryWriter{}
	writer, err := NewQueryLogKafkaWriter(consumer, &fakeQueryLogEntryWriterResolver{writer: entryWriter})
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if len(entryWriter.entries) != 1 {
		t.Fatalf("expected one written query log entry, got %d", len(entryWriter.entries))
	}
	if entryWriter.entries[0].Query != "" {
		t.Fatalf("expected empty query text to round-trip, got %q", entryWriter.entries[0].Query)
	}
	if len(consumer.commits) != 1 {
		t.Fatalf("expected empty-query event to commit, got %d commits", len(consumer.commits))
	}
}

func TestQueryLogKafkaWriterCommitsWhenOrgHasNoDuckLakeTarget(t *testing.T) {
	payload, err := json.Marshal(QueryLogKafkaEvent{
		SchemaVersion: queryLogKafkaSchemaVersion,
		EventID:       "evt-no-ducklake",
		EventTime:     time.Unix(1700000000, 0).UTC(),
		Type:          "QueryFinish",
		Query:         "SELECT 1",
		UserName:      "alice",
		OrgID:         "org-a",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	consumer := &fakeQueryLogKafkaConsumer{
		messages: []QueryLogKafkaConsumedMessage{{Value: payload}},
	}
	configResolver := &fakeDuckLakeConfigResolver{
		resolved: QueryLogDuckLakeResolvedConfig{Config: Config{}},
	}
	entryWriterResolver, err := NewQueryLogDuckLakeEntryWriterResolver(configResolver)
	if err != nil {
		t.Fatalf("NewQueryLogDuckLakeEntryWriterResolver: %v", err)
	}
	writer, err := NewQueryLogKafkaWriter(consumer, entryWriterResolver)
	if err != nil {
		t.Fatalf("NewQueryLogKafkaWriter: %v", err)
	}

	if err := writer.ProcessOne(context.Background()); err != nil {
		t.Fatalf("expected missing DuckLake target to be committed without retry, got %v", err)
	}
	if len(consumer.commits) != 1 {
		t.Fatalf("expected no-target event to commit, got %d commits", len(consumer.commits))
	}
}

func TestQueryLogDuckLakeDBDSNAllowsUnsignedExtensions(t *testing.T) {
	if got, want := queryLogDuckLakeDBDSN(), ":memory:?allow_unsigned_extensions=true"; got != want {
		t.Fatalf("queryLogDuckLakeDBDSN() = %q, want %q", got, want)
	}
}

func TestQueryLogKafkaEventInsertIsIdempotentByEventID(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureQueryLogTable(db, "", "query_log", "query_log"); err != nil {
		t.Fatalf("ensureQueryLogTable: %v", err)
	}
	writer := &QueryLogDuckLakeEntryWriter{db: db, table: "query_log"}
	event := QueryLogKafkaEvent{
		SchemaVersion:         queryLogKafkaSchemaVersion,
		EventID:               "evt-duplicate",
		EventTime:             time.Unix(1700000000, 0).UTC(),
		QueryDurationMs:       42,
		Type:                  "QueryFinish",
		Query:                 "SELECT 1",
		QueryKind:             "Select",
		UserName:              "alice",
		OrgID:                 "org-a",
		CPUTimeSeconds:        1.25,
		PeakBufferMemoryBytes: 2048,
	}

	if err := writer.WriteQueryLogKafkaEvent(context.Background(), event); err != nil {
		t.Fatalf("first WriteQueryLogKafkaEvent: %v", err)
	}
	if err := writer.WriteQueryLogKafkaEvent(context.Background(), event); err != nil {
		t.Fatalf("second WriteQueryLogKafkaEvent: %v", err)
	}

	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM query_log").Scan(&rows); err != nil {
		t.Fatalf("count query_log: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected duplicate event_id to insert one row, got %d", rows)
	}
	var eventID string
	if err := db.QueryRow("SELECT event_id FROM query_log").Scan(&eventID); err != nil {
		t.Fatalf("query event_id: %v", err)
	}
	if eventID != "evt-duplicate" {
		t.Fatalf("expected event_id to persist, got %q", eventID)
	}
}

func TestQueryLogKafkaEventBatchInsertIsIdempotentByEventID(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureQueryLogTable(db, "", "query_log", "query_log"); err != nil {
		t.Fatalf("ensureQueryLogTable: %v", err)
	}
	writer := &QueryLogDuckLakeEntryWriter{db: db, table: "query_log"}
	events := []QueryLogKafkaEvent{
		{
			SchemaVersion:         queryLogKafkaSchemaVersion,
			EventID:               "evt-batch-1",
			EventTime:             time.Unix(1700000000, 0).UTC(),
			QueryDurationMs:       42,
			Type:                  "QueryFinish",
			Query:                 "SELECT 1",
			QueryKind:             "Select",
			UserName:              "alice",
			OrgID:                 "org-a",
			CPUTimeSeconds:        1.25,
			PeakBufferMemoryBytes: 2048,
		},
		{
			SchemaVersion:         queryLogKafkaSchemaVersion,
			EventID:               "evt-batch-2",
			EventTime:             time.Unix(1700000001, 0).UTC(),
			QueryDurationMs:       84,
			Type:                  "QueryFinish",
			Query:                 "SELECT 2",
			QueryKind:             "Select",
			UserName:              "alice",
			OrgID:                 "org-a",
			CPUTimeSeconds:        2.5,
			PeakBufferMemoryBytes: 4096,
		},
	}

	if err := writer.WriteQueryLogKafkaEvents(context.Background(), events); err != nil {
		t.Fatalf("first WriteQueryLogKafkaEvents: %v", err)
	}
	if err := writer.WriteQueryLogKafkaEvents(context.Background(), events); err != nil {
		t.Fatalf("second WriteQueryLogKafkaEvents: %v", err)
	}

	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM query_log").Scan(&rows); err != nil {
		t.Fatalf("count query_log: %v", err)
	}
	if rows != 2 {
		t.Fatalf("expected duplicate event_ids to insert two rows, got %d", rows)
	}
}

func TestQueryLogKafkaEventBatchInsertDeduplicatesEventIDsWithinBatch(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureQueryLogTable(db, "", "query_log", "query_log"); err != nil {
		t.Fatalf("ensureQueryLogTable: %v", err)
	}
	writer := &QueryLogDuckLakeEntryWriter{db: db, table: "query_log"}
	events := []QueryLogKafkaEvent{
		{
			SchemaVersion:         queryLogKafkaSchemaVersion,
			EventID:               "evt-duplicate-in-batch",
			EventTime:             time.Unix(1700000000, 0).UTC(),
			QueryDurationMs:       42,
			Type:                  "QueryFinish",
			Query:                 "SELECT 1",
			QueryKind:             "Select",
			UserName:              "alice",
			OrgID:                 "org-a",
			CPUTimeSeconds:        1.25,
			PeakBufferMemoryBytes: 2048,
		},
		{
			SchemaVersion:         queryLogKafkaSchemaVersion,
			EventID:               "evt-duplicate-in-batch",
			EventTime:             time.Unix(1700000001, 0).UTC(),
			QueryDurationMs:       84,
			Type:                  "QueryFinish",
			Query:                 "SELECT 2",
			QueryKind:             "Select",
			UserName:              "alice",
			OrgID:                 "org-a",
			CPUTimeSeconds:        2.5,
			PeakBufferMemoryBytes: 4096,
		},
	}

	if err := writer.WriteQueryLogKafkaEvents(context.Background(), events); err != nil {
		t.Fatalf("WriteQueryLogKafkaEvents: %v", err)
	}

	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM query_log").Scan(&rows); err != nil {
		t.Fatalf("count query_log: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected duplicate event_ids within a batch to insert one row, got %d", rows)
	}

	var query string
	if err := db.QueryRow("SELECT query FROM query_log").Scan(&query); err != nil {
		t.Fatalf("query inserted row: %v", err)
	}
	if query != "SELECT 1" {
		t.Fatalf("expected first duplicate event to win, got query %q", query)
	}
}

func TestQueryLogDuckLakeEntryWriterResolverRefreshesIdleWriters(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	configResolver := &fakeDuckLakeConfigResolver{
		resolved: QueryLogDuckLakeResolvedConfig{
			Config: Config{DuckLake: DuckLakeConfig{MetadataStore: "postgres:metadata"}},
		},
	}
	resolver, err := NewQueryLogDuckLakeEntryWriterResolver(configResolver)
	if err != nil {
		t.Fatalf("NewQueryLogDuckLakeEntryWriterResolver: %v", err)
	}
	resolver.now = func() time.Time { return now }
	resolver.idleTTL = time.Minute
	var created []*fakeQueryLogEntryWriter
	resolver.newWriter = func(Config) (QueryLogEntryWriter, error) {
		w := &fakeQueryLogEntryWriter{}
		created = append(created, w)
		return w, nil
	}

	first, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a")
	if err != nil {
		t.Fatalf("first ResolveQueryLogEntryWriter: %v", err)
	}
	now = now.Add(time.Minute + time.Second)
	second, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a")
	if err != nil {
		t.Fatalf("second ResolveQueryLogEntryWriter: %v", err)
	}

	if first == second {
		t.Fatal("expected idle writer to be refreshed")
	}
	if len(created) != 2 {
		t.Fatalf("expected two writers to be created, got %d", len(created))
	}
	if created[0].closed != 1 {
		t.Fatalf("expected idle writer to be closed on refresh, got %d closes", created[0].closed)
	}
}

func TestQueryLogDuckLakeEntryWriterResolverEvictsLeastRecentlyUsedWriter(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	configResolver := &fakeDuckLakeConfigResolver{
		resolved: QueryLogDuckLakeResolvedConfig{
			Config: Config{DuckLake: DuckLakeConfig{MetadataStore: "postgres:metadata"}},
		},
	}
	resolver, err := NewQueryLogDuckLakeEntryWriterResolver(configResolver)
	if err != nil {
		t.Fatalf("NewQueryLogDuckLakeEntryWriterResolver: %v", err)
	}
	resolver.now = func() time.Time { return now }
	resolver.maxCachedWriters = 2
	resolver.newWriter = func(Config) (QueryLogEntryWriter, error) {
		return &fakeQueryLogEntryWriter{}, nil
	}

	writerA, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a")
	if err != nil {
		t.Fatalf("resolve org-a: %v", err)
	}
	now = now.Add(time.Second)
	writerB, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-b")
	if err != nil {
		t.Fatalf("resolve org-b: %v", err)
	}
	now = now.Add(time.Second)
	if _, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a"); err != nil {
		t.Fatalf("refresh org-a recency: %v", err)
	}
	now = now.Add(time.Second)
	if _, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-c"); err != nil {
		t.Fatalf("resolve org-c: %v", err)
	}

	if _, ok := resolver.writers["org-a"]; !ok {
		t.Fatal("expected recently used org-a writer to remain cached")
	}
	if _, ok := resolver.writers["org-b"]; ok {
		t.Fatal("expected least recently used org-b writer to be evicted")
	}
	if _, ok := resolver.writers["org-c"]; !ok {
		t.Fatal("expected new org-c writer to be cached")
	}
	if writerB.(*fakeQueryLogEntryWriter).closed != 1 {
		t.Fatalf("expected evicted org-b writer to be closed, got %d closes", writerB.(*fakeQueryLogEntryWriter).closed)
	}
	if writerA.(*fakeQueryLogEntryWriter).closed != 0 {
		t.Fatalf("expected org-a writer to remain open, got %d closes", writerA.(*fakeQueryLogEntryWriter).closed)
	}
}

func TestQueryLogDuckLakeEntryWriterResolverPrunesIdleWritersWithoutResolve(t *testing.T) {
	current := time.Unix(1700000000, 0).UTC()
	var nowMu sync.Mutex
	configResolver := &fakeDuckLakeConfigResolver{
		resolved: QueryLogDuckLakeResolvedConfig{
			Config: Config{DuckLake: DuckLakeConfig{MetadataStore: "postgres:metadata"}},
		},
	}
	resolver, err := NewQueryLogDuckLakeEntryWriterResolver(configResolver)
	if err != nil {
		t.Fatalf("NewQueryLogDuckLakeEntryWriterResolver: %v", err)
	}
	resolver.now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return current
	}
	resolver.idleTTL = time.Millisecond
	resolver.pruneInterval = 10 * time.Millisecond
	var created []*fakeQueryLogEntryWriter
	resolver.newWriter = func(Config) (QueryLogEntryWriter, error) {
		w := &fakeQueryLogEntryWriter{}
		created = append(created, w)
		return w, nil
	}

	if _, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a"); err != nil {
		t.Fatalf("ResolveQueryLogEntryWriter: %v", err)
	}
	nowMu.Lock()
	current = current.Add(time.Second)
	nowMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.StartIdlePruner(ctx)

	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("idle writer was not pruned")
		case <-ticker.C:
			resolver.mu.Lock()
			closed := created[0].closed
			cached := len(resolver.writers)
			resolver.mu.Unlock()
			if closed == 1 && cached == 0 {
				return
			}
		}
	}
}

func TestQueryLogDuckLakeEntryWriterResolverRefreshesExpiringCredentials(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	expiresAt := now.Add(time.Minute)
	configResolver := &fakeDuckLakeConfigResolver{
		resolved: QueryLogDuckLakeResolvedConfig{
			Config:               Config{DuckLake: DuckLakeConfig{MetadataStore: "postgres:metadata"}},
			CredentialsExpiresAt: &expiresAt,
		},
	}
	resolver, err := NewQueryLogDuckLakeEntryWriterResolver(configResolver)
	if err != nil {
		t.Fatalf("NewQueryLogDuckLakeEntryWriterResolver: %v", err)
	}
	resolver.now = func() time.Time { return now }
	var created []*fakeQueryLogEntryWriter
	resolver.newWriter = func(Config) (QueryLogEntryWriter, error) {
		w := &fakeQueryLogEntryWriter{}
		created = append(created, w)
		return w, nil
	}

	first, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a")
	if err != nil {
		t.Fatalf("first ResolveQueryLogEntryWriter: %v", err)
	}
	second, err := resolver.ResolveQueryLogEntryWriter(context.Background(), "org-a")
	if err != nil {
		t.Fatalf("second ResolveQueryLogEntryWriter: %v", err)
	}

	if first == second {
		t.Fatal("expected expiring writer to be refreshed")
	}
	if len(created) != 2 {
		t.Fatalf("expected two writers to be created, got %d", len(created))
	}
	if created[0].closed != 1 {
		t.Fatalf("expected first writer to be closed on refresh, got %d closes", created[0].closed)
	}
	if configResolver.calls != 2 {
		t.Fatalf("expected config resolver to be called twice, got %d", configResolver.calls)
	}
}
