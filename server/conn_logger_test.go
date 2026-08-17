package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestClientConnLoggerIncludesPID(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	c := &clientConn{
		username: "alice",
		orgID:    "acme",
		workerID: 7,
		pid:      42,
	}
	c.logger().Info("probe")
	out := buf.String()
	for _, want := range []string{"user=alice", "org=acme", "worker=7", "pid=42"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %s", want, out)
		}
	}
}
