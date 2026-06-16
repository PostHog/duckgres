package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "network timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type retryLogSignalHandler struct {
	ch   chan struct{}
	once sync.Once
}

func (h *retryLogSignalHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *retryLogSignalHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "Origin fetch failed with retriable error, retrying." {
		h.once.Do(func() { close(h.ch) })
	}
	return nil
}

func (h *retryLogSignalHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *retryLogSignalHandler) WithGroup(string) slog.Handler {
	return h
}

func waitForSignal(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

func waitForRecorder(t *testing.T, ch <-chan *httptest.ResponseRecorder, msg string) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case rec := <-ch:
		return rec
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
		return nil
	}
}

// captureSlog redirects slog.Default to a buffer for the duration of a test
// and returns the buffer + a restore function. Used by the forward-uncached
// logging tests to assert presence of the request/response log lines that
// were missing pre-PR (every PUT/POST through the proxy was a black hole).
func captureSlog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	prev := slog.Default()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf, func() { slog.SetDefault(prev) }
}

// newTestProxy wires a CacheProxy with no peers and a tempdir-backed store.
func newTestProxy(t *testing.T) *CacheProxy {
	t.Helper()
	proxy := NewCacheProxy(newTestCache(t), nil, nil)
	proxy.originRetryInitialBackoff = 0
	proxy.originRetryMaxBackoff = 0
	return proxy
}

// newTestServer returns an httptest origin plus a proxy that rewrites inbound
// forward-proxy-style requests to target that origin. DuckDB sends absolute-form
// URIs (scheme + host + path); we give the test the origin's URL so the proxy's
// fetchOrigin hits httptest.Server directly.
func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	// httptest URL looks like http://127.0.0.1:PORT
	return srv, srv.URL
}

// doForwardProxyRequest sends a request through the proxy in forward-proxy
// form (absolute URI in the request line). httptest's serve path preserves
// the URL.Host and URL.Scheme when we construct the request this way.
func doForwardProxyRequest(proxy *CacheProxy, method, absoluteURL string, headers http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, absoluteURL, nil)
	req.Host = req.URL.Host
	for k, vv := range headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	proxy.HandleProxy(rec, req)
	return rec
}

func TestHandleProxyGETMissThenHit(t *testing.T) {
	proxy := newTestProxy(t)

	var originCalls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello-parquet-bytes"))
	})

	// First request: cache miss → origin fetch → cached.
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/file.parquet", http.Header{"Range": []string{"bytes=0-18"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("miss: status = %d, want 206", rec.Code)
	}
	if body := rec.Body.String(); body != "hello-parquet-bytes" {
		t.Errorf("miss body = %q, want hello-parquet-bytes", body)
	}
	if atomic.LoadInt32(&originCalls) != 1 {
		t.Errorf("origin calls = %d, want 1 on miss", originCalls)
	}

	// Second request for the same URL + Range: cache hit, no extra origin call.
	rec = doForwardProxyRequest(proxy, "GET", originURL+"/bucket/file.parquet", http.Header{"Range": []string{"bytes=0-18"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("hit: status = %d, want 206", rec.Code)
	}
	if atomic.LoadInt32(&originCalls) != 1 {
		t.Errorf("origin calls = %d after hit, want 1", originCalls)
	}
}

func TestHandleProxyHEADForwardedUncached(t *testing.T) {
	proxy := newTestProxy(t)

	var calls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if r.Method != "HEAD" {
			t.Errorf("origin got method=%s, want HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "42")
		w.WriteHeader(http.StatusOK)
	})

	rec := doForwardProxyRequest(proxy, "HEAD", originURL+"/bucket/meta", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD status = %d, want 200", rec.Code)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("origin calls = %d, want 1", calls)
	}

	// A repeat HEAD should ALSO hit the origin (HEAD is never cached).
	_ = doForwardProxyRequest(proxy, "HEAD", originURL+"/bucket/meta", nil)
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("origin calls after repeat HEAD = %d, want 2", calls)
	}
}

func TestHandleProxyRejectsNonAbsoluteURL(t *testing.T) {
	proxy := newTestProxy(t)
	req := httptest.NewRequest("GET", "/relative/only", nil)
	// Clear scheme/host to simulate origin-form.
	req.URL.Scheme = ""
	req.URL.Host = ""
	rec := httptest.NewRecorder()
	proxy.HandleProxy(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for non-absolute URL", rec.Code)
	}
}

// TestHandleProxyForwardsOrigin5xxVerbatim: a transient 5xx that keeps failing
// must eventually be passed back to DuckDB unchanged. Translating a 500 into a
// 502 (the old behaviour) made DuckDB's httpfs retry transient-class errors
// that were really terminal, and hid the real status from logs and the client.
func TestHandleProxyForwardsOrigin5xxVerbatim(t *testing.T) {
	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/broken", http.Header{"Range": []string{"bytes=0-1"}})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 forwarded verbatim", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "boom") {
		t.Errorf("body = %q, want it to contain origin body 'boom'", rec.Body.String())
	}
}

func TestHandleProxyRetriesOrigin503ThenCachesSuccess(t *testing.T) {
	proxy := newTestProxy(t)

	var calls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.Header().Set("X-Amz-Request-Id", "retry-me")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-after-retry"))
	})

	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/flaky-503.parquet", http.Header{"Range": []string{"bytes=0-13"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206 after retry", rec.Code)
	}
	if got := rec.Body.String(); got != "ok-after-retry" {
		t.Fatalf("body = %q, want successful retry body", got)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("origin calls = %d, want 2 (initial 503 + retry success)", calls)
	}

	rec = doForwardProxyRequest(proxy, "GET", originURL+"/bucket/flaky-503.parquet", http.Header{"Range": []string{"bytes=0-13"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("cache hit status = %d, want 206", rec.Code)
	}
	if got := rec.Body.String(); got != "ok-after-retry" {
		t.Fatalf("cache hit body = %q, want successful retry body", got)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("origin calls after cache hit = %d, want still 2", calls)
	}
}

func TestHandleProxyRetriesOrigin503AndForwardsFinalFailure(t *testing.T) {
	proxy := newTestProxy(t)

	var calls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("X-Amz-Request-Id", fmt.Sprintf("attempt-%d", n))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `<Error><Code>SlowDown</Code><Attempt>%d</Attempt></Error>`, n)
	})

	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/still-503.parquet", http.Header{"Range": []string{"bytes=0-7"}})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want final 503 forwarded", rec.Code)
	}
	totalCalls := atomic.LoadInt32(&calls)
	if totalCalls != int32(defaultOriginRetryMaxAttempts) {
		t.Fatalf("origin calls = %d, want %d attempts before forwarding final failure", totalCalls, defaultOriginRetryMaxAttempts)
	}
	if rid := rec.Header().Get("X-Amz-Request-Id"); rid != fmt.Sprintf("attempt-%d", totalCalls) {
		t.Fatalf("X-Amz-Request-Id = %q, want final attempt header", rid)
	}
	wantBody := fmt.Sprintf(`<Error><Code>SlowDown</Code><Attempt>%d</Attempt></Error>`, totalCalls)
	if got := rec.Body.String(); got != wantBody {
		t.Fatalf("body = %q, want final attempt body %q", got, wantBody)
	}
}

func TestHandleProxyOriginFetchMetrics(t *testing.T) {
	proxy := newTestProxy(t)

	successBefore := counterValue(t, originFetchesTotal.WithLabelValues("success"))
	retryBefore := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503"))
	inFlightBefore := gaugeValue(t, originFetchInFlight)

	var calls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("retry later"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-after-metric-retry"))
	})

	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/metrics.parquet", http.Header{"Range": []string{"bytes=0-20"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206 after retry", rec.Code)
	}
	if got := counterValue(t, originFetchesTotal.WithLabelValues("success")); got != successBefore+1 {
		t.Fatalf("origin success metric = %v, want %v", got, successBefore+1)
	}
	if got := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503")); got != retryBefore+1 {
		t.Fatalf("origin retry metric = %v, want %v", got, retryBefore+1)
	}
	if got := gaugeValue(t, originFetchInFlight); got != inFlightBefore {
		t.Fatalf("in-flight origin fetches = %v, want %v after request completes", got, inFlightBefore)
	}
}

func TestHandleProxyOriginRetryMetricDoesNotCountCanceledBackoff(t *testing.T) {
	proxy := newTestProxy(t)
	proxy.originRetryInitialBackoff = 10 * time.Second
	proxy.originRetryMaxBackoff = 10 * time.Second

	retryBefore := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503"))
	canceledBefore := counterValue(t, originFetchesTotal.WithLabelValues("canceled"))
	firstAttemptReturned := make(chan struct{})
	var originCalls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&originCalls, 1) == 1 {
			close(firstAttemptReturned)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("retry later"))
	})

	retryLogSeen := make(chan struct{})
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(&retryLogSignalHandler{ch: retryLogSeen}))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest("GET", originURL+"/bucket/cancel-retry.parquet", nil).WithContext(ctx)
	req.Host = req.URL.Host
	req.Header.Set("Range", "bytes=0-20")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		proxy.HandleProxy(rec, req)
	}()

	waitForSignal(t, firstAttemptReturned, "timed out waiting for first origin attempt")
	waitForSignal(t, retryLogSeen, "timed out waiting for retry backoff path")
	cancel()
	waitForSignal(t, done, "timed out waiting for canceled proxy request")

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 after context cancellation", rec.Code)
	}
	if got := atomic.LoadInt32(&originCalls); got != 1 {
		t.Fatalf("origin calls = %d, want only the initial failed attempt", got)
	}
	if got := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503")); got != retryBefore {
		t.Fatalf("origin retry metric = %v, want unchanged %v when backoff is canceled", got, retryBefore)
	}
	if got := counterValue(t, originFetchesTotal.WithLabelValues("canceled")); got != canceledBefore+1 {
		t.Fatalf("origin canceled metric = %v, want %v", got, canceledBefore+1)
	}
}

func TestHandleProxyOriginRetryMetricCountsPreBackoffCancellationAsCanceled(t *testing.T) {
	proxy := newTestProxy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("retry later")),
		}, nil
	})}

	canceledBefore := counterValue(t, originFetchesTotal.WithLabelValues("canceled"))
	httpErrorBefore := counterValue(t, originFetchesTotal.WithLabelValues("http_error"))
	retryBefore := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503"))

	req := httptest.NewRequest("GET", "http://origin.test/bucket/pre-backoff-cancel.parquet", nil).WithContext(ctx)
	req.Host = req.URL.Host
	req.Header.Set("Range", "bytes=0-20")
	rec := httptest.NewRecorder()
	proxy.HandleProxy(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 after context cancellation", rec.Code)
	}
	if got := counterValue(t, originFetchesTotal.WithLabelValues("canceled")); got != canceledBefore+1 {
		t.Fatalf("origin canceled metric = %v, want %v", got, canceledBefore+1)
	}
	if got := counterValue(t, originFetchesTotal.WithLabelValues("http_error")); got != httpErrorBefore {
		t.Fatalf("origin http_error metric = %v, want unchanged %v", got, httpErrorBefore)
	}
	if got := counterValue(t, originFetchRetriesTotal.WithLabelValues("http_503")); got != retryBefore {
		t.Fatalf("origin retry metric = %v, want unchanged %v", got, retryBefore)
	}
}

func TestHandleProxyOriginFetchInFlightMetricDuringRequest(t *testing.T) {
	proxy := newTestProxy(t)
	inFlightBefore := gaugeValue(t, originFetchInFlight)
	originStarted := make(chan struct{})
	releaseOrigin := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseOrigin) })
	}
	defer release()

	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		close(originStarted)
		<-releaseOrigin
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	results := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		results <- doForwardProxyRequest(proxy, "GET", originURL+"/bucket/in-flight.parquet", http.Header{"Range": []string{"bytes=0-1"}})
	}()
	waitForSignal(t, originStarted, "timed out waiting for origin request to start")

	if got := gaugeValue(t, originFetchInFlight); got != inFlightBefore+1 {
		t.Fatalf("in-flight origin fetches during request = %v, want %v", got, inFlightBefore+1)
	}
	release()

	rec := waitForRecorder(t, results, "timed out waiting for proxy response")
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	if got := gaugeValue(t, originFetchInFlight); got != inFlightBefore {
		t.Fatalf("in-flight origin fetches after request = %v, want %v", got, inFlightBefore)
	}
}

func TestHandleProxyOriginFetchFailureOutcomeMetrics(t *testing.T) {
	tests := []struct {
		name       string
		label      string
		response   *http.Response
		err        error
		wantStatus int
	}{
		{
			name:  "http error",
			label: "http_error",
			response: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("retry later")),
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "timeout",
			label:      "timeout",
			err:        context.DeadlineExceeded,
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "generic error",
			label:      "error",
			err:        fmt.Errorf("transport boom"),
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy := newTestProxy(t)
			proxy.originRetryMaxAttempts = 1
			proxy.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return tt.response, tt.err
			})}
			before := counterValue(t, originFetchesTotal.WithLabelValues(tt.label))

			rec := doForwardProxyRequest(proxy, "GET", "http://origin.test/bucket/failure.parquet", http.Header{"Range": []string{"bytes=0-20"}})
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := counterValue(t, originFetchesTotal.WithLabelValues(tt.label)); got != before+1 {
				t.Fatalf("origin %s metric = %v, want %v", tt.label, got, before+1)
			}
		})
	}
}

func TestOriginFetchMetricLabels(t *testing.T) {
	outcomeTests := []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, "success"},
		{"http error", &originStatusError{status: http.StatusServiceUnavailable}, "http_error"},
		{"canceled", context.Canceled, "canceled"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"net timeout", timeoutNetError{}, "timeout"},
		{"generic", fmt.Errorf("boom"), "error"},
	}
	for _, tt := range outcomeTests {
		t.Run("outcome "+tt.name, func(t *testing.T) {
			if got := originFetchOutcome(tt.err); got != tt.want {
				t.Fatalf("originFetchOutcome(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}

	reasonTests := []struct {
		name string
		err  error
		want string
	}{
		{"http", &originStatusError{status: http.StatusTooManyRequests}, "http_429"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"net timeout", timeoutNetError{}, "timeout"},
		{"connection reset", fmt.Errorf("read: connection reset by peer"), "connection_reset"},
		{"connection refused", fmt.Errorf("dial tcp: connection refused"), "connection_refused"},
		{"unexpected eof", fmt.Errorf("unexpected EOF"), "unexpected_eof"},
		{"generic", fmt.Errorf("boom"), "transport_error"},
	}
	for _, tt := range reasonTests {
		t.Run("reason "+tt.name, func(t *testing.T) {
			if got := originRetryReason(tt.err); got != tt.want {
				t.Fatalf("originRetryReason(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestHandleProxyMetricsOnlyDoesNotRejectConcurrentOriginMisses(t *testing.T) {
	proxy := newTestProxy(t)

	const requests = 65
	var originCalls int32
	var activeOriginCalls int32
	var maxActiveOriginCalls int32
	releaseOrigin := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseOrigin) })
	}
	defer release()

	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&activeOriginCalls, 1)
		defer atomic.AddInt32(&activeOriginCalls, -1)
		for {
			maxActive := atomic.LoadInt32(&maxActiveOriginCalls)
			if current <= maxActive || atomic.CompareAndSwapInt32(&maxActiveOriginCalls, maxActive, current) {
				break
			}
		}
		atomic.AddInt32(&originCalls, 1)
		<-releaseOrigin
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	results := make(chan *httptest.ResponseRecorder, requests)
	for i := 0; i < requests; i++ {
		i := i
		go func() {
			url := fmt.Sprintf("%s/bucket/concurrent-%d.parquet", originURL, i)
			headers := http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", i, i+1)}}
			results <- doForwardProxyRequest(proxy, "GET", url, headers)
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&originCalls) < requests && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&originCalls); got != requests {
		release()
		t.Fatalf("origin calls before release = %d, want %d; metrics-only PR must not limit origin concurrency", got, requests)
	}
	if got := atomic.LoadInt32(&maxActiveOriginCalls); got != requests {
		release()
		t.Fatalf("simultaneous origin calls before release = %d, want %d; metrics-only PR must not queue origin concurrency", got, requests)
	}
	release()

	var failed []string
	for i := 0; i < requests; i++ {
		rec := waitForRecorder(t, results, "timed out waiting for concurrent proxy response")
		if rec.Code != http.StatusPartialContent {
			failed = append(failed, fmt.Sprintf("response %d status = %d", i+1, rec.Code))
		}
	}
	if len(failed) > 0 {
		t.Fatalf("metrics-only PR must not add local rejection behavior: %s", strings.Join(failed, ", "))
	}
	if got := atomic.LoadInt32(&originCalls); got != requests {
		t.Fatalf("origin calls = %d, want %d; metrics-only PR must not limit origin concurrency", got, requests)
	}
}

func TestHandleProxyDoesNotRetryTerminalOriginStatuses(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{"bad-request", http.StatusBadRequest},
		{"forbidden", http.StatusForbidden},
		{"not-found", http.StatusNotFound},
		{"range-not-satisfiable", http.StatusRequestedRangeNotSatisfiable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy := newTestProxy(t)
			var calls int32
			_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&calls, 1)
				w.WriteHeader(tt.status)
				_, _ = fmt.Fprintf(w, "status=%d", tt.status)
			})

			rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/terminal.parquet", http.Header{"Range": []string{"bytes=0-1"}})
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			if atomic.LoadInt32(&calls) != 1 {
				t.Fatalf("origin calls = %d, want 1 for terminal status %d", calls, tt.status)
			}
		})
	}
}

// TestHandleProxyForwardsOrigin400Verbatim is the case the user actually
// hit: S3 returns 400 with an XML envelope (<Code>ExpiredToken</Code>) and
// DuckDB needs to see *that* body and *that* status, not a generic 502 with
// a Go-formatted error string. Without verbatim passthrough the error class
// (4xx terminal vs 5xx retriable) is lost and httpfs retries non-retriable
// auth failures.
func TestHandleProxyForwardsOrigin400Verbatim(t *testing.T) {
	proxy := newTestProxy(t)
	const errBody = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>`
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("X-Amz-Request-Id", "TESTREQID123")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(errBody))
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/expired.parquet", http.Header{"Range": []string{"bytes=0-1023"}})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 forwarded verbatim", rec.Code)
	}
	if got := rec.Body.String(); got != errBody {
		t.Errorf("body mismatch:\n got = %q\nwant = %q", got, errBody)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/xml" {
		t.Errorf("Content-Type = %q, want application/xml so DuckLake parses it as an S3 error envelope", ct)
	}
	if rid := rec.Header().Get("X-Amz-Request-Id"); rid != "TESTREQID123" {
		t.Errorf("X-Amz-Request-Id = %q, want TESTREQID123 (preserved for debugging)", rid)
	}
}

// TestHandleProxyForwardsOrigin404Verbatim: a 404 must stay a 404 so
// DuckDB / DuckLake can distinguish "object missing" (terminal) from
// "transient gateway error" (retriable).
func TestHandleProxyForwardsOrigin404Verbatim(t *testing.T) {
	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/gone.parquet", http.Header{"Range": []string{"bytes=0-1"}})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 forwarded verbatim", rec.Code)
	}
}

// TestHandleProxyForwardsOrigin416Verbatim: Range Not Satisfiable carries
// semantically important metadata for DuckLake; collapsing to 502 made it
// look like a network error.
func TestHandleProxyForwardsOrigin416Verbatim(t *testing.T) {
	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes */1024")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/short.parquet", http.Header{"Range": []string{"bytes=999999-1000000"}})
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416 forwarded verbatim", rec.Code)
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes */1024" {
		t.Errorf("Content-Range = %q, want 'bytes */1024' (DuckDB uses this to learn the actual file size)", cr)
	}
}

// TestHandleProxyDoesNotCacheErrorResponses: a transient origin error
// must not poison the cache. The next request for the same key has to hit
// the origin again — otherwise an ExpiredToken error would persist past
// the credential refresh that fixes it.
func TestHandleProxyDoesNotCacheErrorResponses(t *testing.T) {
	proxy := newTestProxy(t)
	var calls int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("first call fails"))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-now"))
	})

	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/flaky.parquet", http.Header{"Range": []string{"bytes=0-5"}})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("first call: status = %d, want 400", rec.Code)
	}

	rec = doForwardProxyRequest(proxy, "GET", originURL+"/bucket/flaky.parquet", http.Header{"Range": []string{"bytes=0-5"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("retry: status = %d, want 206 (cache must not have stored the prior error)", rec.Code)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("origin calls = %d, want 2 (the cache must not serve a previously-failed request from cache)", calls)
	}
}

// TestHandleProxyNetworkErrorStill502: when the origin is fully
// unreachable (no HTTP response at all), 502 is still the right answer —
// there was no upstream status to forward, and httpfs's "retry on 5xx"
// behaviour is appropriate here.
func TestHandleProxyNetworkErrorStill502(t *testing.T) {
	proxy := newTestProxy(t)
	// Construct a URL that points at no listener: srv.Close before use.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	dead := srv.URL
	srv.Close()

	rec := doForwardProxyRequest(proxy, "GET", dead+"/bucket/anything", http.Header{"Range": []string{"bytes=0-1"}})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when origin is unreachable", rec.Code)
	}
}

func TestHandlePeerHasAndGet(t *testing.T) {
	proxy := newTestProxy(t)

	key := strings.Repeat("c", 64)
	body := []byte("peer-cached-payload")
	if err := proxy.store.Put(key, body); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// /cache/has → 200 for known key
	req := httptest.NewRequest("GET", "/cache/has?key="+key, nil)
	rec := httptest.NewRecorder()
	proxy.HandlePeerHas(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("HandlePeerHas known key: status = %d, want 200", rec.Code)
	}

	// /cache/has → 404 for unknown key
	missing := strings.Repeat("d", 64)
	req = httptest.NewRequest("GET", "/cache/has?key="+missing, nil)
	rec = httptest.NewRecorder()
	proxy.HandlePeerHas(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("HandlePeerHas missing key: status = %d, want 404", rec.Code)
	}

	// /cache/get → 200 + body for known key
	req = httptest.NewRequest("GET", "/cache/get?key="+key, nil)
	rec = httptest.NewRecorder()
	proxy.HandlePeerGet(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("HandlePeerGet: status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != string(body) {
		t.Errorf("HandlePeerGet body = %q, want %q", got, body)
	}
}

func TestHandlePeerRejectsInvalidKey(t *testing.T) {
	proxy := newTestProxy(t)
	for _, key := range []string{"", "../../etc/passwd", "deadbeef"} {
		req := httptest.NewRequest("GET", "/cache/has?key="+key, nil)
		rec := httptest.NewRecorder()
		proxy.HandlePeerHas(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("HandlePeerHas(%q): status = %d, want 400", key, rec.Code)
		}
		req = httptest.NewRequest("GET", "/cache/get?key="+key, nil)
		rec = httptest.NewRecorder()
		proxy.HandlePeerGet(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("HandlePeerGet(%q): status = %d, want 400", key, rec.Code)
		}
	}
}

func TestSingleFlightDedups(t *testing.T) {
	var sf singleFlight
	var calls int32
	fn := func() (fetchResult, error) {
		atomic.AddInt32(&calls, 1)
		return fetchResult{size: 7, source: "miss"}, nil
	}

	// Sequential calls with the same key should still invoke fn each time
	// (singleflight only coalesces concurrent in-flight callers). We verify
	// that the return values are correct rather than count.
	got1, err := sf.Do("k", fn)
	if err != nil || got1.size != 7 {
		t.Fatalf("sf.Do: got=%+v err=%v", got1, err)
	}
	got2, err := sf.Do("k", fn)
	if err != nil || got2.size != 7 {
		t.Fatalf("sf.Do repeat: got=%+v err=%v", got2, err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("sequential calls invoked fn %d times, want 2", calls)
	}
}

// TestServeBodyContentRange verifies that when a Range header is present,
// the cached body is returned with HTTP 206 + Content-Range.
func TestServeBodyContentRange(t *testing.T) {
	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123456789"))
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/r", http.Header{"Range": []string{"bytes=0-9"}})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	if cr := rec.Header().Get("Content-Range"); !strings.HasPrefix(cr, "bytes 0-9/") {
		t.Errorf("Content-Range = %q, want prefix 'bytes 0-9/'", cr)
	}
}

// TestFetchOriginPreservesSignedHeaders confirms the proxy forwards SigV4
// headers verbatim (we can't verify against real S3 here, but we can verify
// the wire representation reaches the origin).
func TestFetchOriginPreservesSignedHeaders(t *testing.T) {
	proxy := newTestProxy(t)

	var gotAuth, gotDate string
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotDate = r.Header.Get("X-Amz-Date")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	h := http.Header{
		"Range":                []string{"bytes=0-1"},
		"Authorization":        []string{"AWS4-HMAC-SHA256 Credential=AKIATEST/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcdef"},
		"X-Amz-Date":           []string{"20260101T000000Z"},
		"X-Amz-Content-Sha256": []string{"UNSIGNED-PAYLOAD"},
		// Hop-by-hop header must NOT be forwarded.
		"Proxy-Connection": []string{"Keep-Alive"},
	}
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/bucket/signed", h)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	if !strings.Contains(gotAuth, "Signature=abcdef") {
		t.Errorf("origin Authorization = %q, want SigV4 preserved", gotAuth)
	}
	if gotDate != "20260101T000000Z" {
		t.Errorf("origin X-Amz-Date = %q, want 20260101T000000Z", gotDate)
	}
}

// Sanity check that Content-Length set via serveBody matches body length.
func TestServeBodyContentLength(t *testing.T) {
	proxy := newTestProxy(t)
	payload := []byte("abcdef")
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	})
	rec := doForwardProxyRequest(proxy, "GET", originURL+"/b/k", http.Header{"Range": []string{"bytes=0-5"}})
	if rec.Header().Get("Content-Length") != fmt.Sprintf("%d", len(payload)) {
		t.Errorf("Content-Length = %q, want %d", rec.Header().Get("Content-Length"), len(payload))
	}
	got, _ := io.ReadAll(rec.Body)
	if string(got) != string(payload) {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

// TestForwardUncachedLogsSuccess locks in the invariant that a successful
// PUT/POST through the forward-proxy path produces a log line. Pre-PR this
// path was completely silent, leaving operators with no proxy-side
// breadcrumb to correlate against a downstream client error.
func TestForwardUncachedLogsSuccess(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	proxy := newTestProxy(t)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ack"))
	})

	rec := doForwardProxyRequest(proxy, http.MethodPut, originURL+"/b/k", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	out := buf.String()
	if !strings.Contains(out, `msg="Forward-proxy served."`) {
		t.Errorf("expected Forward-proxy served. log on success, got:\n%s", out)
	}
	if !strings.Contains(out, `method=PUT`) {
		t.Errorf("expected method=PUT in log, got:\n%s", out)
	}
	if !strings.Contains(out, `status=200`) {
		t.Errorf("expected status=200 in log, got:\n%s", out)
	}
}

// TestForwardUncachedLogsNon2xxWithBodyPreview is the symptomatic case
// the PR addresses: when an upstream rejects a PUT/POST (e.g. an S3
// endpoint returning 501 Not Implemented for a parquet write), the proxy
// must surface the status AND the response body preview so the
// client-side "HTTP code 501" error has a matching server-side
// breadcrumb. The previewBody helper truncates at 256 bytes to keep log
// volume bounded.
func TestForwardUncachedLogsNon2xxWithBodyPreview(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	proxy := newTestProxy(t)
	originBody := `<?xml version="1.0"?><Error><Code>NotImplemented</Code><Message>A header you provided implies functionality that is not implemented</Message></Error>`
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(originBody))
	})

	rec := doForwardProxyRequest(proxy, http.MethodPut, originURL+"/b/k.parquet", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
	if got := rec.Body.String(); got != originBody {
		t.Errorf("body forwarded as %q, want verbatim origin XML", got)
	}

	out := buf.String()
	if !strings.Contains(out, `msg="Forward-proxy origin returned non-2xx."`) {
		t.Errorf("expected non-2xx Warn, got:\n%s", out)
	}
	if !strings.Contains(out, `status=501`) {
		t.Errorf("expected status=501, got:\n%s", out)
	}
	if !strings.Contains(out, `<Code>NotImplemented</Code>`) {
		t.Errorf("expected origin XML <Code> prefix in body_preview, got:\n%s", out)
	}
}

// TestHandleConnectLogsOpenAndClose drives a real HTTPS CONNECT tunnel
// through the proxy and asserts that:
//
//   - "Forward-proxy CONNECT opened." fires when the tunnel is established;
//   - "Forward-proxy CONNECT closed." fires once both legs finish, with the
//     correct sent/recv byte counts.
//
// Without this, every HTTPS-routed write through the proxy (which is the
// path AWS S3 PUT/POST uses by default) was invisible in proxy-side logs.
// The client still got error codes from upstream, but operators couldn't
// confirm the request even reached the proxy.
func TestHandleConnectLogsOpenAndClose(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	// Origin: simple echo server that reflects exactly the bytes it reads.
	originDone := make(chan struct{})
	origin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("origin listen: %v", err)
	}
	defer func() { _ = origin.Close() }()
	go func() {
		defer close(originDone)
		c, err := origin.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 16)
		n, _ := c.Read(buf)
		if n > 0 {
			_, _ = c.Write(buf[:n])
		}
	}()

	// Proxy: wrap HandleProxy in an httptest server. CONNECT requests go
	// through Go's standard server hijack path, which is what the real
	// cache-proxy does in production.
	proxy := newTestProxy(t)
	proxySrv := httptest.NewServer(http.HandlerFunc(proxy.HandleProxy))
	defer proxySrv.Close()
	proxyURL, _ := url.Parse(proxySrv.URL)

	// Hand-craft the CONNECT exchange over a raw TCP connection — the
	// stdlib Transport hides CONNECT inside its HTTPS path, but for unit
	// testing we want the bytes-on-the-wire visibility.
	pconn, err := net.Dial("tcp", proxyURL.Host)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = pconn.Close() }()

	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", origin.Addr().String(), origin.Addr().String())
	if _, err := pconn.Write([]byte(connectReq)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	// Read the proxy's "200 Connection Established" response line + headers.
	resp := make([]byte, 256)
	n, err := pconn.Read(resp)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if !strings.Contains(string(resp[:n]), "200") {
		t.Fatalf("expected 200 from proxy, got %q", string(resp[:n]))
	}

	payload := []byte("ping-bytes")
	if _, err := pconn.Write(payload); err != nil {
		t.Fatalf("write tunnel payload: %v", err)
	}
	echo := make([]byte, len(payload))
	if _, err := io.ReadFull(pconn, echo); err != nil {
		t.Fatalf("read tunnel echo: %v", err)
	}
	if string(echo) != string(payload) {
		t.Fatalf("tunnel echo = %q, want %q", echo, payload)
	}

	// Close the client side; this should propagate to both io.Copy
	// goroutines and trigger the "closed" log emission.
	_ = pconn.Close()
	<-originDone

	// The "closed" log fires from a separate goroutine after both legs
	// finish; poll briefly so the test isn't flaky.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), `Forward-proxy CONNECT closed.`) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	out := buf.String()
	if !strings.Contains(out, `msg="Forward-proxy CONNECT opened."`) {
		t.Errorf("expected open log, got:\n%s", out)
	}
	if !strings.Contains(out, `msg="Forward-proxy CONNECT closed."`) {
		t.Errorf("expected close log, got:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf(`target=%s`, origin.Addr().String())) {
		t.Errorf("expected target= attr matching origin addr, got:\n%s", out)
	}
}

// TestHandleConnectLogsDialFailure: when the upstream is unreachable, the
// proxy must respond 502 to the client AND emit a Warn so the failure is
// visible in proxy-side logs.
func TestHandleConnectLogsDialFailure(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	proxy := newTestProxy(t)
	proxySrv := httptest.NewServer(http.HandlerFunc(proxy.HandleProxy))
	defer proxySrv.Close()
	proxyURL, _ := url.Parse(proxySrv.URL)

	pconn, err := net.Dial("tcp", proxyURL.Host)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = pconn.Close() }()

	// 127.0.0.1:1 is reserved/unbound — kernel rejects the dial fast.
	connectReq := "CONNECT 127.0.0.1:1 HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n"
	if _, err := pconn.Write([]byte(connectReq)); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	resp := make([]byte, 256)
	n, _ := pconn.Read(resp)
	if !strings.Contains(string(resp[:n]), "502") {
		t.Fatalf("expected 502 on dial failure, got %q", string(resp[:n]))
	}

	if !strings.Contains(buf.String(), `Forward-proxy CONNECT dial failed.`) {
		t.Errorf("expected dial-failed Warn, got:\n%s", buf.String())
	}
}

// TestForwardUncachedTruncatesLargeBodyInLog verifies previewBody is
// applied — a multi-KiB error envelope must not flood log storage.
func TestForwardUncachedTruncatesLargeBodyInLog(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	proxy := newTestProxy(t)
	huge := strings.Repeat("X", 4096)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(huge))
	})

	_ = doForwardProxyRequest(proxy, http.MethodPut, originURL+"/b/k", nil)

	out := buf.String()
	if !strings.Contains(out, `...(truncated)`) {
		t.Errorf("expected previewBody truncation marker in log, got:\n%s", out)
	}
}

// TestForwardUncachedPropagatesContentLength is the regression test for the
// AWS S3 "501 Not Implemented" error on parquet PUTs. The proxy used to
// build its outbound request via http.NewRequestWithContext with the
// inbound r.Body as the body — and because r.Body is a generic
// io.ReadCloser (not one of the *bytes.Buffer / *bytes.Reader /
// *strings.Reader types Go auto-detects), Go's Transport saw ContentLength=0
// and fell back to Transfer-Encoding: chunked. S3 rejects chunked PUT with
// 501 even though DuckDB sent a perfectly valid Content-Length-bearing
// request. After the fix, the outbound request must carry over the inbound's
// ContentLength so the wire shape is preserved.
func TestForwardUncachedPropagatesContentLength(t *testing.T) {
	proxy := newTestProxy(t)
	payload := []byte("parquet-bytes-of-known-length")

	var (
		gotMethod        string
		gotContentLength int64
		gotTE            string
		gotBody          []byte
	)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentLength = r.ContentLength
		gotTE = r.Header.Get("Transfer-Encoding")
		// httputil sometimes also strips Transfer-Encoding into r.TransferEncoding
		if gotTE == "" && len(r.TransferEncoding) > 0 {
			gotTE = strings.Join(r.TransferEncoding, ",")
		}
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPut, originURL+"/b/k.parquet", bytes.NewReader(payload))
	req.Host = req.URL.Host
	// httptest.NewRequest only auto-sets ContentLength for the special body
	// types; do it explicitly so we're modeling the inbound shape DuckDB
	// uses (Content-Length present, no Transfer-Encoding).
	req.ContentLength = int64(len(payload))
	rec := httptest.NewRecorder()

	proxy.HandleProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("origin saw method %q, want PUT", gotMethod)
	}
	if gotContentLength != int64(len(payload)) {
		t.Errorf("origin saw ContentLength=%d, want %d (proxy not propagating)", gotContentLength, len(payload))
	}
	if gotTE == "chunked" {
		t.Errorf("origin saw Transfer-Encoding: chunked — proxy is rewriting Content-Length-bearing PUTs as chunked, AWS S3 returns 501 for this")
	}
	if !bytes.Equal(gotBody, payload) {
		t.Errorf("body mismatch:\n  got:  %q\n  want: %q", gotBody, payload)
	}
}

// TestForwardUncachedPreservesRequestHeaders verifies that arbitrary
// non-hop-by-hop request headers (Authorization, x-amz-*, custom) round-trip
// to the origin unchanged. Critical for AWS Sigv4: any header in the signed
// list (host;x-amz-content-sha256;x-amz-date) being mutated would invalidate
// the signature; the rest still matter for content addressability.
func TestForwardUncachedPreservesRequestHeaders(t *testing.T) {
	proxy := newTestProxy(t)
	want := map[string]string{
		"Authorization":        "AWS4-HMAC-SHA256 Credential=AKIA/...",
		"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
		"X-Amz-Date":           "20260505T120000Z",
		"X-Amz-Security-Token": "FwoGZ...",
		"Content-Type":         "application/octet-stream",
		"X-Custom-Header":      "verbatim",
	}

	got := map[string]string{}
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		for k := range want {
			got[k] = r.Header.Get(k)
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPut, originURL+"/b/k", bytes.NewReader([]byte("x")))
	req.Host = req.URL.Host
	req.ContentLength = 1
	for k, v := range want {
		req.Header.Set(k, v)
	}

	proxy.HandleProxy(httptest.NewRecorder(), req)

	for k, w := range want {
		if got[k] != w {
			t.Errorf("header %s: got %q, want %q", k, got[k], w)
		}
	}
}

// TestForwardUncachedStripsHopByHopBothDirections verifies that
// hop-by-hop headers (per RFC 7230 §6.1) are NOT forwarded in either
// direction. Forwarding hop-by-hop headers can confuse origin / client
// parsers — Connection, Keep-Alive, TE, Trailers, Transfer-Encoding,
// Upgrade, Proxy-Authorization, Proxy-Authenticate.
func TestForwardUncachedStripsHopByHopBothDirections(t *testing.T) {
	proxy := newTestProxy(t)

	var originSawConnection, originSawKeepAlive string
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		originSawConnection = r.Header.Get("Connection")
		originSawKeepAlive = r.Header.Get("Keep-Alive")
		w.Header().Set("Connection", "should-not-leak")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("X-Allowed", "yes")
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPut, originURL+"/b/k", bytes.NewReader([]byte("x")))
	req.Host = req.URL.Host
	req.ContentLength = 1
	req.Header.Set("Connection", "close")
	req.Header.Set("Keep-Alive", "timeout=5")
	rec := httptest.NewRecorder()

	proxy.HandleProxy(rec, req)

	if originSawConnection != "" {
		t.Errorf("origin saw Connection header from inbound, hop-by-hop should be stripped: %q", originSawConnection)
	}
	if originSawKeepAlive != "" {
		t.Errorf("origin saw Keep-Alive header from inbound, hop-by-hop should be stripped: %q", originSawKeepAlive)
	}
	if got := rec.Header().Get("Connection"); got != "" {
		t.Errorf("client saw response Connection header from origin, hop-by-hop should be stripped: %q", got)
	}
	if got := rec.Header().Get("Keep-Alive"); got != "" {
		t.Errorf("client saw response Keep-Alive header, hop-by-hop should be stripped: %q", got)
	}
	if got := rec.Header().Get("X-Allowed"); got != "yes" {
		t.Errorf("client lost non-hop-by-hop response header X-Allowed: %q", got)
	}
}

// TestForwardUncachedPreservesQueryString covers AWS S3 multipart-upload
// query params (?uploads, ?partNumber=N&uploadId=...). Sigv4's canonical
// request includes the query string, so any mutation would 403.
func TestForwardUncachedPreservesQueryString(t *testing.T) {
	proxy := newTestProxy(t)

	var gotQuery string
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	})

	url := originURL + "/b/k.parquet?uploads=&partNumber=3&uploadId=ABC%3DDEF"
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader([]byte("x")))
	req.Host = req.URL.Host
	req.ContentLength = 1
	proxy.HandleProxy(httptest.NewRecorder(), req)

	if gotQuery != "uploads=&partNumber=3&uploadId=ABC%3DDEF" {
		t.Errorf("query mutated:\n  got:  %q\n  want: %q", gotQuery, "uploads=&partNumber=3&uploadId=ABC%3DDEF")
	}
}

// TestForwardUncachedPreservesResponseBodyBytewise locks in that 2xx
// response bodies are forwarded byte-for-byte (no compression / no
// transcoding). DuckDB downstream may parse this as XML / parquet and
// any mutation would corrupt it.
func TestForwardUncachedPreservesResponseBodyBytewise(t *testing.T) {
	proxy := newTestProxy(t)

	originBody := []byte("\x00\x01\x02\x03 binary <xml/> body \xff\xfe")
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(originBody)
	})

	req := httptest.NewRequest(http.MethodPut, originURL+"/b/k", bytes.NewReader([]byte("x")))
	req.Host = req.URL.Host
	req.ContentLength = 1
	rec := httptest.NewRecorder()
	proxy.HandleProxy(rec, req)

	if !bytes.Equal(rec.Body.Bytes(), originBody) {
		t.Errorf("response body mutated:\n  got:  %q\n  want: %q", rec.Body.Bytes(), originBody)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type lost: got %q", got)
	}
}

// TestForwardUncachedPreservesNon2xxResponseBodyBytewise covers the
// log-preview path on non-2xx — the proxy reads the full body for the
// body_preview log attr, but must still forward it verbatim to the client.
// Specifically the AWS S3 XML error envelope has to reach DuckDB so its
// own error parsing can extract the <Code>...</Code>.
func TestForwardUncachedPreservesNon2xxResponseBodyBytewise(t *testing.T) {
	proxy := newTestProxy(t)

	originBody := []byte(`<?xml version="1.0"?><Error><Code>NotImplemented</Code><Message>foo</Message></Error>`)
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write(originBody)
	})

	req := httptest.NewRequest(http.MethodPut, originURL+"/b/k", bytes.NewReader([]byte("x")))
	req.Host = req.URL.Host
	req.ContentLength = 1
	rec := httptest.NewRecorder()
	proxy.HandleProxy(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), originBody) {
		t.Errorf("non-2xx body mutated:\n  got:  %q\n  want: %q", rec.Body.Bytes(), originBody)
	}
}

// TestForwardUncachedPreservesMethod walks every method that hits the
// non-GET path and checks the origin saw the same method. Ensures we
// don't accidentally specialise on PUT/POST and break HEAD/DELETE/etc.
func TestForwardUncachedPreservesMethod(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			proxy := newTestProxy(t)
			var gotMethod string
			_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(method, originURL+"/b/k", bytes.NewReader([]byte("x")))
			req.Host = req.URL.Host
			req.ContentLength = 1
			proxy.HandleProxy(httptest.NewRecorder(), req)
			if gotMethod != method {
				t.Errorf("method mutated: got %q, want %q", gotMethod, method)
			}
		})
	}
}
