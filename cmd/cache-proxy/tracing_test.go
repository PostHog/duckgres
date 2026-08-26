package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

func TestTracingExtractsRemoteParentAtIngress(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previousPropagator) })

	sr := withSpanRecorder(t)
	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("payload-bytes"))
	})

	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	rec := doForwardProxyRequest(proxy, http.MethodGet, originURL+"/obj", http.Header{
		"traceparent": {"00-" + parent.TraceID().String() + "-" + parent.SpanID().String() + "-01"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	getSpan := findSpan(t, sr, "cache.get")
	if getSpan.SpanContext().TraceID() != parent.TraceID() {
		t.Fatalf("cache.get trace = %s, want remote parent trace %s", getSpan.SpanContext().TraceID(), parent.TraceID())
	}
	if getSpan.Parent().SpanID() != parent.SpanID() {
		t.Fatalf("cache.get parent = %s, want remote parent %s", getSpan.Parent().SpanID(), parent.SpanID())
	}
}

func TestPeerTransferPropagatesContextAndRecordsLookup(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previousPropagator) })

	sr := withSpanRecorder(t)
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewCacheProxy(store, nil, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", peer.HandlePeerHas)
	mux.HandleFunc("/cache/get", peer.HandlePeerGet)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	if _, err := store.PutStream(key, strings.NewReader("peer-data")); err != nil {
		t.Fatal(err)
	}

	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2},
		TraceFlags: trace.FlagsSampled, Remote: true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	pm := peerManagerWith([]string{strings.TrimPrefix(server.URL, "http://")})
	holder, flight, ok := pm.LocateKey(ctx, key)
	if !ok || flight {
		t.Fatalf("LocateKey = (%q, flight=%v, ok=%v), want present peer", holder, flight, ok)
	}
	if _, ok := pm.FetchFromPeer(ctx, holder, key, false, func(r io.Reader) (int64, error) {
		return io.Copy(io.Discard, r)
	}); !ok {
		t.Fatal("FetchFromPeer failed")
	}

	lookup := findSpan(t, sr, "cache.peer_lookup")
	if lookup.SpanContext().TraceID() != parent.TraceID() || lookup.Parent().SpanID() != parent.SpanID() {
		t.Fatalf("lookup is not parented to the original query context")
	}
	if len(lookup.Events()) != 1 || lookup.Events()[0].Name != "cache.peer_probe" {
		t.Fatalf("lookup events = %#v, want one cache.peer_probe event", lookup.Events())
	}
	probe := lookup.Events()[0]
	var outcome string
	var hasDuration bool
	for _, attr := range probe.Attributes {
		switch string(attr.Key) {
		case "duckgres.cache.peer.outcome":
			outcome = attr.Value.AsString()
		case "duckgres.cache.peer.duration_ms":
			hasDuration = true
		}
	}
	if outcome != "present" || !hasDuration {
		t.Fatalf("lookup probe attributes = %v, want present outcome and duration", probe.Attributes)
	}
	get := findSpan(t, sr, "cache.peer_get")
	serve := findSpan(t, sr, "cache.peer_serve")
	if get.SpanContext().TraceID() != parent.TraceID() {
		t.Fatalf("peer get trace = %s, want %s", get.SpanContext().TraceID(), parent.TraceID())
	}
	if serve.Parent().SpanID() != get.SpanContext().SpanID() {
		t.Fatalf("peer serve parent = %s, want peer get %s", serve.Parent().SpanID(), get.SpanContext().SpanID())
	}
}

func TestPeerTransferCancellationStopsPeerWork(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm := peerManagerWith(nil)
	done := make(chan bool, 1)
	go func() {
		_, ok := pm.FetchFromPeer(ctx, strings.TrimPrefix(server.URL, "http://"), strings.Repeat("a", 64), false, func(io.Reader) (int64, error) {
			return 0, nil
		})
		done <- ok
	}()
	<-started
	cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("peer handler did not receive request cancellation")
	}
	if ok := <-done; ok {
		t.Fatal("canceled peer transfer succeeded")
	}
}
