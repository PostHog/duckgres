package duckdbservice

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func waitForCacheProxyMode(t *testing.T, router *cacheProxyRouter, want cacheProxyMode) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if router.Mode() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("router mode = %q, want %q", router.Mode(), want)
}

func closedLocalAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve local address: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release local address: %v", err)
	}
	return addr
}

// A dead node-local cache must never prevent an otherwise valid S3 read. The
// router retries only the uncommitted GET through the authoritative endpoint.
func TestCacheProxyRouterFallsOpenToAuthoritativeSource(t *testing.T) {
	var sourceCalls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceCalls.Add(1)
		if r.URL.Path != "/warehouse/file.parquet" {
			t.Fatalf("source path = %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, "authoritative-data")
	}))
	defer source.Close()

	router := newCacheProxyRouter(closedLocalAddress(t), false)
	router.setMode(cacheProxyModeCached)
	proxy := httptest.NewServer(router)
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	resp, err := client.Get(source.URL + "/warehouse/file.parquet")
	if err != nil {
		t.Fatalf("GET through unavailable cache proxy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "authoritative-data" {
		t.Fatalf("fallback response = %d %q", resp.StatusCode, body)
	}
	if sourceCalls.Load() != 1 {
		t.Fatalf("authoritative source calls = %d, want 1", sourceCalls.Load())
	}
	if router.Mode() != cacheProxyModeBypassed {
		t.Fatalf("router mode = %q, want bypassed after cache dial failure", router.Mode())
	}
}

func TestCacheProxyRouterMarksPassthroughRequests(t *testing.T) {
	var marked atomic.Bool
	cache := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		marked.Store(r.Header.Get(cacheProxyPassthroughHeader) == "true")
		_, _ = io.WriteString(w, "ok")
	}))
	defer cache.Close()

	router := newCacheProxyRouter(cache.Listener.Addr().String(), false)
	router.setMode(cacheProxyModeCached)
	router.setPassthrough(true)
	proxy := httptest.NewServer(router)
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	resp, err := client.Get("http://example.com/warehouse/file.parquet")
	if err != nil {
		t.Fatalf("GET through router: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !marked.Load() {
		t.Fatal("cache-bound request was not marked as passthrough")
	}
}

// ResponseHeaderTimeout must bound an unavailable upstream without imposing a
// whole-response deadline: S3 range bodies can legitimately outlive it.
func TestCacheProxyRouterStreamsPastResponseHeaderTimeout(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("response writer does not support flush")
		}
		_, _ = io.WriteString(w, "first-")
		flusher.Flush()
		time.Sleep(40 * time.Millisecond)
		_, _ = io.WriteString(w, "last")
	}))
	defer source.Close()

	router := newCacheProxyRouterWithTransport(closedLocalAddress(t), false, cacheProxyTransportConfig{
		responseHeaderTimeout: 10 * time.Millisecond,
	})
	if router.directClient.Timeout != 0 || router.cacheClient.Timeout != 0 {
		t.Fatalf("router clients must not impose a whole-response timeout: direct=%s cache=%s", router.directClient.Timeout, router.cacheClient.Timeout)
	}
	router.setMode(cacheProxyModeBypassed)
	proxy := httptest.NewServer(router)
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	resp, err := client.Get(source.URL + "/warehouse/large.parquet")
	if err != nil {
		t.Fatalf("GET through direct fallback: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read streamed fallback body: %v", err)
	}
	if got := string(body); got != "first-last" {
		t.Fatalf("body = %q, want complete stream", got)
	}
}

// A response received from the cache is authoritative for that request. In
// particular, a 5xx must not be silently replaced with a direct source read:
// it can carry a source/data-integrity failure that the query must see.
func TestCacheProxyRouterDoesNotHideCacheResponses(t *testing.T) {
	var sourceCalls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sourceCalls.Add(1)
		_, _ = io.WriteString(w, "must not be read")
	}))
	defer source.Close()

	cache := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream checksum mismatch", http.StatusBadGateway)
	}))
	defer cache.Close()
	cacheURL, err := url.Parse(cache.URL)
	if err != nil {
		t.Fatalf("parse cache URL: %v", err)
	}

	router := newCacheProxyRouter(cacheURL.Host, false)
	router.setMode(cacheProxyModeCached)
	proxy := httptest.NewServer(router)
	defer proxy.Close()
	proxyURL, _ := url.Parse(proxy.URL)
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	resp, err := client.Get(source.URL + "/warehouse/file.parquet")
	if err != nil {
		t.Fatalf("GET through cache proxy: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if sourceCalls.Load() != 0 {
		t.Fatalf("source calls = %d, want 0", sourceCalls.Load())
	}
}

func TestWaitForCacheProxyIsBoundedAndFailsOpen(t *testing.T) {
	t.Setenv("DUCKGRES_CACHE_ENABLED", "true")
	t.Setenv("NODE_IP", "127.0.0.1")
	t.Setenv("DUCKGRES_CACHE_PROXY_CONNECT_TIMEOUT", "20ms")

	start := time.Now()
	if mode := waitForCacheProxy(); mode != cacheProxyModeBypassed {
		t.Fatalf("startup mode = %q, want bypassed", mode)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("cache-proxy startup wait = %s, want bounded wait", elapsed)
	}
}

func TestCacheProxySupervisorBypassesRuntimeLossAndRecovers(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	healthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer healthServer.Close()

	router := newCacheProxyRouter("127.0.0.1:8080", false)
	router.setMode(cacheProxyModeCached)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.superviseWith(healthServer.URL, stop, cacheProxySupervisorConfig{
			probeTimeout:          50 * time.Millisecond,
			healthyProbeInterval:  5 * time.Millisecond,
			initialReconnectDelay: 5 * time.Millisecond,
			maxReconnectDelay:     20 * time.Millisecond,
			jitter:                func(delay time.Duration) time.Duration { return delay },
		})
	}()

	healthy.Store(false)
	waitForCacheProxyMode(t, router, cacheProxyModeBypassed)
	healthy.Store(true)
	waitForCacheProxyMode(t, router, cacheProxyModeCached)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cache-proxy supervisor did not stop")
	}
}
