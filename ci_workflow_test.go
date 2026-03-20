package main

import (
	"os"
	"regexp"
	"testing"
)

func TestCIWorkflowPinsSetupJustToFullSHA(t *testing.T) {
	t.Parallel()

	const (
		workflowPath = ".github/workflows/ci.yml"
		wantSHA      = "f8a3cce218d9f83db3a2ecd90e41ac3de6cdfd9b"
	)

	data, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}

	matches := regexp.MustCompile(`(?m)^\s*uses:\s*extractions/setup-just@([^\s#]+)`).FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		t.Fatalf("expected at least one extractions/setup-just use in %s", workflowPath)
	}

	for _, match := range matches {
		got := string(match[1])
		if got != wantSHA {
			t.Fatalf("expected extractions/setup-just to be pinned to %s, got %s", wantSHA, got)
		}
	}
}
