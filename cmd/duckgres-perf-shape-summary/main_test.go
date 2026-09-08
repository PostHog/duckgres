package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestRunRequiresArtifactsDirectory(t *testing.T) {
	for _, args := range [][]string{nil, {"--artifacts-dir", "test", "extra"}} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Fatal("expected usage error")
		}
	}
}

func TestRunPublishesIncompleteReportBeforeFailing(t *testing.T) {
	var out bytes.Buffer
	err := run([]string{"--artifacts-dir", t.TempDir()}, &out)
	if err == nil || !strings.Contains(out.String(), "Comparison incomplete") || !strings.Contains(out.String(), "missing artifact") {
		t.Fatalf("report=%s, error=%v", out.String(), err)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestRunReturnsWriteFailure(t *testing.T) {
	if err := run([]string{"--artifacts-dir", t.TempDir()}, failedWriter{}); err == nil || err.Error() != "write failed" {
		t.Fatalf("got %v", err)
	}
}
