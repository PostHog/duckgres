package datasets

import "testing"

func TestGenerateDeterministicStable(t *testing.T) {
	a, err := GenerateDeterministic(42, 1)
	if err != nil {
		t.Fatalf("GenerateDeterministic returned error: %v", err)
	}
	b, err := GenerateDeterministic(42, 1)
	if err != nil {
		t.Fatalf("GenerateDeterministic returned error: %v", err)
	}
	if len(a) != len(b) || len(a) == 0 {
		t.Fatalf("unexpected output length: %d / %d", len(a), len(b))
	}
	if a[0] != b[0] || a[len(a)-1] != b[len(b)-1] {
		t.Fatalf("expected deterministic output, got %v vs %v", a[0], b[0])
	}
}
