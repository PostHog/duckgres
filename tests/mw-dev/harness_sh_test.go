package e2emwdev_test

import (
	"os"
	"strings"
	"testing"
)

func TestHarnessTemporarilySkipsAllReshardOperations(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e/harness.sh: %v", err)
	}
	script := string(raw)

	const start = "  # ---- reshard operations"
	const end = "  # ---- lifecycle:"
	startAt := strings.Index(script, start)
	endAt := strings.Index(script, end)
	if startAt < 0 || endAt < 0 || endAt <= startAt {
		t.Fatal("could not locate the top-level reshard block")
	}
	block := script[startAt:endAt]

	if !strings.Contains(block, `log "SKIP reshard E2E (temporarily disabled; covered by unit tests)"`) {
		t.Fatalf("reshard block does not record its explicit E2E skip:\n%s", block)
	}
	for _, call := range []string{
		"\n  reshard_targets\n",
		"\n  reshard_validation ",
		"\n  reshard_cancel_during_drain ",
		"\n  reshard_bogus_shard_rollback ",
		"\n  reshard_ext_to_cnpg ",
	} {
		if strings.Contains(block, call) {
			t.Fatalf("reshard E2E call still enabled: %q", strings.TrimSpace(call))
		}
	}

}
