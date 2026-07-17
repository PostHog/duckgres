package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestSlackWebhookSinkRedactsSensitiveIdentifiers(t *testing.T) {
	var payload map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	sink := NewSlackWebhookSink(srv.URL, srv.Client())
	err := sink.Notify(t.Context(), Event{
		Name:  "org_created",
		OrgID: "example-org-123",
		Props: map[string]any{
			"database_name":    "example_database",
			"metadata_store":   "cnpg-shard",
			"ducklake_enabled": true,
			"source":           "provisioning",
		},
	})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	text := payload["text"]
	if text == "" {
		t.Fatal("expected Slack text payload")
	}
	for _, leaked := range []string{"example-org-123", "example_database", "database_name"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("Slack payload leaked %q: %s", leaked, text)
		}
	}
	if strings.Contains(text, "org_ref=") {
		t.Fatalf("Slack payload included org reference without a hash key: %s", text)
	}
	for _, want := range []string{"org_created", "metadata_store=cnpg-shard", "ducklake_enabled=true", "source=provisioning"} {
		if !strings.Contains(text, want) {
			t.Fatalf("Slack payload missing %q: %s", want, text)
		}
	}
}

func TestSlackWebhookSinkUsesHMACOrgReferenceWhenConfigured(t *testing.T) {
	var payload map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	sink := NewSlackWebhookSinkWithOptions(srv.URL, srv.Client(), SlackWebhookOptions{
		OrgRefKey: "local-test-key",
	})
	err := sink.Notify(t.Context(), Event{Name: "org_created", OrgID: "example-org-123"})
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	text := payload["text"]
	if strings.Contains(text, "example-org-123") {
		t.Fatalf("Slack payload leaked raw org ID: %s", text)
	}
	if !strings.Contains(text, "org_ref=hmac-sha256:") {
		t.Fatalf("Slack payload missing HMAC org reference: %s", text)
	}
	if strings.Contains(text, "org_ref=sha256:") {
		t.Fatalf("Slack payload used unsalted org reference: %s", text)
	}
}

func TestAsyncNotifierDropsWhenQueueFull(t *testing.T) {
	before := notificationDropsTotalValue(t)
	sink := &blockingSink{release: make(chan struct{})}
	notifier := NewAsyncNotifier(sink, AsyncOptions{QueueSize: 1})
	t.Cleanup(func() {
		close(sink.release)
		notifier.Close()
	})

	notifier.Notify(Event{Name: "first", OrgID: "org-a"})
	notifier.Notify(Event{Name: "second", OrgID: "org-b"})
	notifier.Notify(Event{Name: "third", OrgID: "org-c"})

	if got := notifier.Dropped(); got == 0 {
		t.Fatal("expected at least one dropped notification")
	}
	if got := notificationDropsTotalValue(t); got <= before {
		t.Fatalf("expected notification drop counter to increase, before=%v after=%v", before, got)
	}
}

func TestAsyncNotifierCountsDeliveryFailures(t *testing.T) {
	before := notificationDeliveryFailuresTotalValue(t)
	notifier := NewAsyncNotifier(failingSink{}, AsyncOptions{QueueSize: 1})
	t.Cleanup(notifier.Close)

	notifier.Notify(Event{Name: "warehouse_provision_failed", OrgID: "example-org"})
	waitForMetricIncrease(t, notificationDeliveryFailuresTotalValue, before)
}

type blockingSink struct {
	release chan struct{}
}

func (s *blockingSink) Notify(_ context.Context, _ Event) error {
	<-s.release
	return nil
}

type failingSink struct{}

func (failingSink) Notify(context.Context, Event) error {
	return errors.New("webhook failed")
}

func notificationDropsTotalValue(t *testing.T) float64 {
	t.Helper()
	return counterValue(t, notificationDropsTotal)
}

func notificationDeliveryFailuresTotalValue(t *testing.T) float64 {
	t.Helper()
	return counterValue(t, notificationDeliveryFailuresTotal)
}

func counterValue(t *testing.T, counter interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func waitForMetricIncrease(t *testing.T, value func(*testing.T) float64, before float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if value(t) > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("metric did not increase before timeout: before=%v after=%v", before, value(t))
}
