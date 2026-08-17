package duckdbservice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNeverSetDefaultWithUser(t *testing.T) {
	attrs := workerIdentityAttrs("org-a", 17)
	for i := 0; i+1 < len(attrs); i += 2 {
		key, _ := attrs[i].(string)
		if key == "user" {
			t.Fatal("worker default identity must not include user")
		}
	}
	if len(attrs) != 4 || attrs[0] != "org" || attrs[2] != "worker" {
		t.Fatalf("worker identity attrs = %#v", attrs)
	}
}

func TestSessionLoggerClearedOnDestroy(t *testing.T) {
	s := &Session{Username: "alice"}
	attachSessionLog(s, "alice", 9)
	if s.Logger() == slog.Default() {
		t.Fatal("expected session logger after attach")
	}
	clearSessionLog(s)
	if s.Logger() != slog.Default() {
		t.Fatal("session logger still set after clear")
	}
}

func TestCreateSessionPIDOnLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	s := &Session{}
	attachSessionLog(s, "root", 1001)
	s.Logger().Info("probe")
	out := buf.String()
	if !strings.Contains(out, "user=root") {
		t.Fatalf("missing user: %s", out)
	}
	if !strings.Contains(out, "pid=1001") {
		t.Fatalf("missing pid: %s", out)
	}
}

func TestStuckQueryWarnCarriesSessionIdentity(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	s := &Session{Username: "alice"}
	attachSessionLog(s, "alice", 42)
	logStuckQuery(s, "session", "abc", "rows_processed", 0)
	out := buf.String()
	if !strings.Contains(out, "Query appears stuck — no progress detected.") {
		t.Fatalf("missing stuck-query message: %s", out)
	}
	if !strings.Contains(out, "user=alice") || !strings.Contains(out, "pid=42") {
		t.Fatalf("stuck-query WARN missing session identity: %s", out)
	}
}

func TestStampWorkerLogIdentityDoesNotIncludeUser(t *testing.T) {
	attrs := workerIdentityAttrs("acme", 3)
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "user" {
			t.Fatalf("user leaked onto worker identity: %#v", attrs)
		}
	}
}
