package notifications

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	DefaultQueueSize = 100
	DefaultTimeout   = 2 * time.Second
)

type Event struct {
	Name  string
	OrgID string
	Props map[string]any
}

type Notifier interface {
	Notify(Event)
	Close()
}

type Sink interface {
	Notify(context.Context, Event) error
}

type noopNotifier struct{}

func (noopNotifier) Notify(Event) {}
func (noopNotifier) Close()       {}

type AsyncOptions struct {
	QueueSize int
	Timeout   time.Duration
}

type AsyncNotifier struct {
	sink    Sink
	timeout time.Duration
	ch      chan Event
	done    chan struct{}
	once    sync.Once
	dropped atomic.Uint64
}

func NewAsyncNotifier(sink Sink, opts AsyncOptions) *AsyncNotifier {
	queueSize := opts.QueueSize
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	n := &AsyncNotifier{
		sink:    sink,
		timeout: timeout,
		ch:      make(chan Event, queueSize),
		done:    make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *AsyncNotifier) Notify(event Event) {
	select {
	case n.ch <- event:
	default:
		n.dropped.Add(1)
		notificationDropsTotal.Inc()
		slog.Warn("Notification queue full; dropping event.", "event", event.Name)
	}
}

func (n *AsyncNotifier) Dropped() uint64 {
	return n.dropped.Load()
}

// Close stops the background sender without draining queued events. Shutdown
// preserves the same best-effort contract as request-time delivery: never block
// process exit on Slack or another external notification sink.
func (n *AsyncNotifier) Close() {
	n.once.Do(func() {
		close(n.done)
	})
}

func (n *AsyncNotifier) run() {
	for {
		select {
		case event := <-n.ch:
			ctx, cancel := context.WithTimeout(context.Background(), n.timeout)
			if err := n.sink.Notify(ctx, event); err != nil {
				notificationDeliveryFailuresTotal.Inc()
				slog.Debug("Notification delivery failed.", "event", event.Name, "error", err)
			}
			cancel()
		case <-n.done:
			return
		}
	}
}

var notificationDropsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_notifications_dropped_total",
	Help: "Total number of operational notifications dropped because the in-process queue was full.",
})

var notificationDeliveryFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_notification_delivery_failures_total",
	Help: "Total number of operational notification delivery failures returned by the configured sink.",
})

type SlackWebhookSink struct {
	webhookURL string
	client     *http.Client
	orgRefKey  []byte
}

type SlackWebhookOptions struct {
	OrgRefKey string
}

func NewSlackWebhookSink(webhookURL string, client *http.Client) *SlackWebhookSink {
	return NewSlackWebhookSinkWithOptions(webhookURL, client, SlackWebhookOptions{})
}

func NewSlackWebhookSinkWithOptions(webhookURL string, client *http.Client, opts SlackWebhookOptions) *SlackWebhookSink {
	if client == nil {
		client = http.DefaultClient
	}
	return &SlackWebhookSink{webhookURL: webhookURL, client: client, orgRefKey: []byte(opts.OrgRefKey)}
}

func (s *SlackWebhookSink) Notify(ctx context.Context, event Event) error {
	if s.webhookURL == "" {
		return nil
	}
	body, err := json.Marshal(map[string]string{"text": s.slackText(event)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Debug("Failed to close Slack webhook response body.", "error", err)
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("slack webhook returned %s", resp.Status)
	}
	return nil
}

func (s *SlackWebhookSink) slackText(event Event) string {
	parts := []string{
		"Duckgres notification",
		"event=" + event.Name,
	}
	if ref := orgRef(event.OrgID, s.orgRefKey); ref != "" {
		parts = append(parts, "org_ref=hmac-sha256:"+ref)
	}
	for _, key := range safePropKeys(event.Props) {
		parts = append(parts, key+"="+fmt.Sprint(event.Props[key]))
	}
	return strings.Join(parts, "\n")
}

func orgRef(orgID string, key []byte) string {
	if orgID == "" || len(key) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(orgID))
	sum := mac.Sum(nil)
	return hex.EncodeToString(sum[:])[:12]
}

func safePropKeys(props map[string]any) []string {
	allow := map[string]bool{
		"ducklake_enabled": true,
		"metadata_store":   true,
		"reason":           true,
		"source":           true,
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		if allow[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

var (
	defaultMu sync.RWMutex
	current   Notifier = noopNotifier{}
)

func Default() Notifier {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return current
}

func SetDefault(n Notifier) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if n == nil {
		n = noopNotifier{}
	}
	current = n
}
