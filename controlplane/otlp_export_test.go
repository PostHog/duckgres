package controlplane

import (
	"testing"
)

func TestOTLPExportFailureRollupFirstSeeAddsN(t *testing.T) {
	r := newOTLPExportFailureRollup()
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	n := int64(3)
	r.observe(7, &n)
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	if delta := after - before; delta != 3 {
		t.Fatalf("first see Add(n): delta=%v want 3", delta)
	}
}

func TestOTLPExportFailureRollupAddsDeltaOnly(t *testing.T) {
	r := newOTLPExportFailureRollup()
	n := int64(2)
	r.observe(1, &n)
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	n = 5
	r.observe(1, &n)
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	if delta := after - before; delta != 3 {
		t.Fatalf("n>=last Add(n-last): delta=%v want 3", delta)
	}
}

func TestOTLPExportFailureRollupRestartTreatsLastAsZero(t *testing.T) {
	r := newOTLPExportFailureRollup()
	n := int64(9)
	r.observe(2, &n)
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	n = 2
	r.observe(2, &n)
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	if delta := after - before; delta != 2 {
		t.Fatalf("restart n<last must Add(n) not Add(n-last): delta=%v want 2", delta)
	}
}

func TestOTLPExportFailureRollupOmitsMissingField(t *testing.T) {
	r := newOTLPExportFailureRollup()
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	r.observe(3, nil)
	r.observeFromHealth(3, &healthCheckResult{})
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	if after != before {
		t.Fatalf("omitted otlp_export_failures must not invent 0 then spike: before=%v after=%v", before, after)
	}
}

func TestOTLPExportFailureRollupForgetThenFirstSee(t *testing.T) {
	r := newOTLPExportFailureRollup()
	n := int64(4)
	r.observe(9, &n)
	r.forget(9)
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	n = 4
	r.observe(9, &n)
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceWorker, otlpExportReasonWorker)
	if delta := after - before; delta != 4 {
		t.Fatalf("after forget, next see is first see Add(n): delta=%v want 4", delta)
	}
}

func TestObserveOTLPExportFailureHasNoOrgLabel(t *testing.T) {
	// Label set is {source,reason} only — constructing with those two must work
	// and there is no org dimension on the vec.
	before := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceCP, otlpExportReasonExport)
	observeOTLPExportFailures(otlpExportSourceCP, otlpExportReasonExport, 1)
	after := counterVecLabelValue(t, otlpLogExportFailuresTotal, otlpExportSourceCP, otlpExportReasonExport)
	if after-before != 1 {
		t.Fatalf("cp/export increment: before=%v after=%v", before, after)
	}
}
