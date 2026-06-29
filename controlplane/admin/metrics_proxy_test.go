//go:build kubernetes

package admin

import (
	"strings"
	"testing"
)

// Every panel must render to valid PromQL for both an empty and a non-empty org
// — in particular with NO leftover fmt "%!(EXTRA ...)" corruption (the bug that
// broke worker_states / queue_depth).
func TestRenderPanelNoCorruption(t *testing.T) {
	for _, org := range []struct{ sel, errSel string }{
		{"", `{outcome="error"}`},
		{`{org="acme"}`, `{org="acme",outcome="error"}`},
	} {
		for key, tmpl := range rangePanels {
			got := renderPanel(tmpl, org.sel, org.errSel, "5m")
			if strings.Contains(got, "%!") {
				t.Errorf("panel %q (org=%q) rendered with fmt corruption: %s", key, org.sel, got)
			}
			if strings.Contains(got, "$ORG") || strings.Contains(got, "$WIN") {
				t.Errorf("panel %q (org=%q) has unsubstituted token: %s", key, org.sel, got)
			}
		}
	}
}

// error_ratio's numerator must filter outcome="error" and differ from the
// denominator base (regression: it previously used the same org-only selector
// for both, always reporting ~100%).
func TestErrorRatioFiltersErrors(t *testing.T) {
	got := renderPanel(rangePanels["error_ratio"], `{org="acme"}`, `{org="acme",outcome="error"}`, "5m")
	if !strings.Contains(got, `outcome="error"`) {
		t.Fatalf("error_ratio numerator does not filter errors: %s", got)
	}
	// Denominator (after the division) must NOT carry outcome="error".
	parts := strings.SplitN(got, "/", 2)
	if len(parts) != 2 || strings.Contains(parts[1], `outcome="error"`) {
		t.Fatalf("error_ratio denominator unexpectedly scoped to errors: %s", got)
	}
}
