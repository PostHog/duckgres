package observe

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func connDurationSample(t *testing.T, org string) (count uint64, sum float64) {
	t.Helper()
	observer, err := connectionDurationHistogram.GetMetricWithLabelValues(org)
	if err != nil {
		t.Fatalf("histogram labels %q: %v", org, err)
	}
	m := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(m); err != nil {
		t.Fatalf("histogram write labels %q: %v", org, err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// ObserveConnectionDuration must record exactly one sample per call and add the
// observed seconds to the per-org sum — _sum is the load-bearing series for
// exact total connection-seconds (no scrape-integral bias). Verifies org
// labelling is honoured so per-org slicing works.
func TestObserveConnectionDurationRecordsPerOrgSample(t *testing.T) {
	const org = "conn-duration-test-org"
	countBefore, sumBefore := connDurationSample(t, org)

	ObserveConnectionDuration(org, 12.5)
	ObserveConnectionDuration(org, 7.5)

	countAfter, sumAfter := connDurationSample(t, org)
	if countAfter != countBefore+2 {
		t.Fatalf("sample count = %d, want %d", countAfter, countBefore+2)
	}
	if got := sumAfter - sumBefore; got != 20.0 {
		t.Fatalf("sum delta = %v, want 20", got)
	}

	// A different org must not see those samples (label isolation).
	if c, _ := connDurationSample(t, "conn-duration-other-org"); c != 0 {
		t.Fatalf("unrelated org sample count = %d, want 0", c)
	}
}
