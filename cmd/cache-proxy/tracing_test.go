package main

import (
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// withSpanRecorder installs an in-memory span recorder as proxyTracer for the
// test and returns it. Restores the previous tracer on cleanup. This is how we
// assert cache-proxy emits its standalone traces — the cache proxy is not
// deployed in the e2e-mw-dev environment (DUCKGRES_CACHE_ENABLED is off there),
// so a Tempo round-trip assertion isn't possible in-Job; this unit test is the
// gate for the tracing behavior instead.
func withSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := proxyTracer
	proxyTracer = tp.Tracer("duckgres/cache-proxy")
	t.Cleanup(func() { proxyTracer = prev })
	return sr
}

// findSpan returns the first recorded span with the given name, or fails.
func findSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no span named %q (got %v)", name, spanNames(sr))
	return nil
}

func spanNames(sr *tracetest.SpanRecorder) []string {
	var names []string
	for _, s := range sr.Ended() {
		names = append(names, s.Name())
	}
	return names
}

func attrValue(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func mustAttr(t *testing.T, s sdktrace.ReadOnlySpan, key string) attribute.Value {
	t.Helper()
	v, ok := attrValue(s, key)
	if !ok {
		t.Fatalf("span %q missing attribute %q", s.Name(), key)
	}
	return v
}

// TestTracingMissThenHit asserts the proxy emits a cache.get root span on a
// cacheable GET, a nested cache.origin_fetch child on a miss (same trace, child
// of the get span), and that source/hit attributes flip miss→hit on the
// second request.
func TestTracingMissThenHit(t *testing.T) {
	sr := withSpanRecorder(t)
	proxy := newTestProxy(t)

	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("payload-bytes"))
	})

	// First request: cache miss → origin fetch.
	rec := doForwardProxyRequest(proxy, http.MethodGet, originURL+"/obj", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("miss: status = %d", rec.Code)
	}

	getSpan := findSpan(t, sr, "cache.get")
	if v := mustAttr(t, getSpan, "duckgres.cache.source"); v.AsString() != "miss" {
		t.Errorf("miss: duckgres.cache.source = %q, want miss", v.AsString())
	}
	if v := mustAttr(t, getSpan, "duckgres.cache.hit"); v.AsBool() {
		t.Errorf("miss: duckgres.cache.hit = true, want false")
	}
	// client.address is the cross-reference anchor back to the worker pod.
	if _, ok := attrValue(getSpan, "client.address"); !ok {
		t.Errorf("miss: cache.get span missing client.address")
	}

	originSpan := findSpan(t, sr, "cache.origin_fetch")
	if originSpan.Parent().SpanID() != getSpan.SpanContext().SpanID() {
		t.Errorf("cache.origin_fetch parent = %v, want cache.get span %v",
			originSpan.Parent().SpanID(), getSpan.SpanContext().SpanID())
	}
	if originSpan.SpanContext().TraceID() != getSpan.SpanContext().TraceID() {
		t.Errorf("cache.origin_fetch trace id differs from cache.get — not nested")
	}

	// Second request (same key): served from local cache → hit, no new fetch.
	sr2 := withSpanRecorder(t)
	rec = doForwardProxyRequest(proxy, http.MethodGet, originURL+"/obj", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("hit: status = %d", rec.Code)
	}
	hitSpan := findSpan(t, sr2, "cache.get")
	if v := mustAttr(t, hitSpan, "duckgres.cache.source"); v.AsString() != "hit" {
		t.Errorf("hit: duckgres.cache.source = %q, want hit", v.AsString())
	}
	if v := mustAttr(t, hitSpan, "duckgres.cache.hit"); !v.AsBool() {
		t.Errorf("hit: duckgres.cache.hit = false, want true")
	}
	for _, s := range sr2.Ended() {
		if s.Name() == "cache.origin_fetch" {
			t.Errorf("hit: unexpected cache.origin_fetch span (should serve from cache)")
		}
	}
}

// TestTracingForwardUncached asserts non-GET (uncached) requests get a
// cache.forward span carrying the response status.
func TestTracingForwardUncached(t *testing.T) {
	sr := withSpanRecorder(t)
	proxy := newTestProxy(t)

	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	rec := doForwardProxyRequest(proxy, http.MethodHead, originURL+"/obj", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("forward: status = %d", rec.Code)
	}

	fwd := findSpan(t, sr, "cache.forward")
	if v := mustAttr(t, fwd, "http.response.status_code"); v.AsInt64() != int64(http.StatusNoContent) {
		t.Errorf("forward: status_code attr = %d, want %d", v.AsInt64(), http.StatusNoContent)
	}
	if v := mustAttr(t, fwd, "http.request.method"); v.AsString() != http.MethodHead {
		t.Errorf("forward: method attr = %q, want HEAD", v.AsString())
	}
}
