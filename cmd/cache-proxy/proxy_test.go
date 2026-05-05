package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

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
	return NewCacheProxy(newTestCache(t), nil, nil)
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

// TestHandleProxyForwardsOrigin5xxVerbatim: any non-2xx the origin returns
// must be passed back to DuckDB unchanged. Translating a 500 into a 502
// (the old behaviour) made DuckDB's httpfs retry transient-class errors that
// were really terminal, and hid the real status from logs and the client.
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
	fn := func() ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("payload"), nil
	}

	// Sequential calls with the same key should still invoke fn each time
	// (singleflight only coalesces concurrent in-flight callers). We verify
	// that the return values are correct rather than count.
	got1, err := sf.Do("k", fn)
	if err != nil || string(got1) != "payload" {
		t.Fatalf("sf.Do: got=%q err=%v", got1, err)
	}
	got2, err := sf.Do("k", fn)
	if err != nil || string(got2) != "payload" {
		t.Fatalf("sf.Do repeat: got=%q err=%v", got2, err)
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
