package cliboot

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/posthog/duckgres/internal/notifications"
)

// InitNotifications installs optional fire-and-forget operational notifications.
// Slack delivery is enabled only when DUCKGRES_SLACK_WEBHOOK_URL is set.
func InitNotifications() func() {
	webhookURL := os.Getenv("DUCKGRES_SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		return func() {}
	}

	queueSize := notifications.DefaultQueueSize
	if v := os.Getenv("DUCKGRES_NOTIFICATION_QUEUE_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			fmt.Fprintf(os.Stderr, "Invalid DUCKGRES_NOTIFICATION_QUEUE_SIZE %q; using default %d\n", v, queueSize)
		} else {
			queueSize = n
		}
	}

	timeout := notifications.DefaultTimeout
	if v := os.Getenv("DUCKGRES_NOTIFICATION_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			fmt.Fprintf(os.Stderr, "Invalid DUCKGRES_NOTIFICATION_TIMEOUT %q; using default %s\n", v, timeout)
		} else {
			timeout = d
		}
	}

	sink := notifications.NewSlackWebhookSinkWithOptions(webhookURL, http.DefaultClient, notifications.SlackWebhookOptions{
		OrgRefKey: os.Getenv("DUCKGRES_NOTIFICATION_HASH_KEY"),
	})
	notifier := notifications.NewAsyncNotifier(sink, notifications.AsyncOptions{
		QueueSize: queueSize,
		Timeout:   timeout,
	})
	notifications.SetDefault(notifier)
	fmt.Fprintln(os.Stderr, "Slack notifications enabled.")

	return func() {
		notifier.Close()
		notifications.SetDefault(nil)
	}
}
