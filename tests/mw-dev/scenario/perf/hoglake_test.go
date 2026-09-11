package perf

import (
	"context"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestHoglakeSetupRequiresInputs(t *testing.T) {
	e := NewExecutor(ExecutorConfig{})
	err := e.ExecuteStep(context.Background(), core.Step{Type: "setup_hoglake", With: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "org_id") {
		t.Fatalf("expected missing org_id, got %v", err)
	}
}
