package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// requestSpanAttrs returns the attributes common to every proxied request,
// chosen to be the cross-reference anchors for correlating a cache-proxy trace
// back to a duckgres query trace by hand: the worker pod IP (client.address),
// the S3 object (server.address + url.path + range), and the HTTP method.
// org_id is deliberately absent — the proxy has no per-request tenant identity;
// map client.address → org via Kubernetes when correlating.
func requestSpanAttrs(r *http.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("http.request.method", r.Method),
		attribute.String("server.address", r.URL.Host),
		attribute.String("url.path", r.URL.Path),
		attribute.String("duckgres.s3.range", r.Header.Get("Range")),
		attribute.String("client.address", r.RemoteAddr),
	}
}

// CacheProxy is a forward HTTP proxy that caches responses on local NVMe.
// DuckDB httpfs sends each S3 request as a signed plain-HTTP request to the
// proxy; the proxy caches by URL+Range and forwards misses verbatim. The
// SigV4 signature stays valid because Host/URL are unchanged, so the proxy
// needs no AWS credentials of its own.
type CacheProxy struct {
	store   *DiskCache
	peers   *PeerManager
	client  *http.Client
	flights singleFlight

	originTimeout             time.Duration
	originRetryMaxAttempts    int
	originRetryInitialBackoff time.Duration
	originRetryMaxBackoff     time.Duration

	// cacheHostSuffixes are the Host substrings that identify DuckLake bucket
	// traffic worth caching. Requests whose Host doesn't contain any of these
	// are passed through without caching.
	cacheHostSuffixes []string

	// blockMode, blockSize, and maxSpanBlocks configure the block-aligned
	// serve path (serveBlockAligned): whether it's active, the fixed block
	// size in bytes, and the max number of blocks coalesced into one origin
	// fetch. Wired to HandleProxy in a later task.
	blockMode     bool
	blockSize     int64
	maxSpanBlocks int64

	// objectSizes remembers validated complete lengths learned from origin
	// Content-Range responses. Disk blocks remain the durable cache; this map
	// lets subsequent requests in the same process emit precise range headers
	// and reject starts beyond EOF without another origin request.
	objectSizes sync.Map // map[string]int64, keyed by full object URL
}

// fetchResult describes a body that fetchDedup has materialized onto local
// disk under its cache key. It deliberately carries no body bytes — the data
// lives on NVMe and is served by streaming straight from the file, so a flood
// of concurrent fetches no longer pins one buffer per request in memory.
type fetchResult struct {
	size        int64
	contentType string // origin Content-Type ("" for peer fetches and hits)
	source      string // "peer" or "miss"
}

const (
	defaultOriginTimeout             = 60 * time.Second
	defaultOriginRetryMaxAttempts    = 4
	defaultOriginRetryInitialBackoff = 100 * time.Millisecond
	defaultOriginRetryMaxBackoff     = 1 * time.Second
)

type singleFlight struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	wg    sync.WaitGroup
	start time.Time
	res   fetchResult
	err   error
}

func (sf *singleFlight) Do(key string, fn func() (fetchResult, error)) (fetchResult, error) {
	sf.mu.Lock()
	if sf.m == nil {
		sf.m = make(map[string]*call)
	}
	if c, ok := sf.m[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait()
		return c.res, c.err
	}
	c := &call{start: time.Now()}
	c.wg.Add(1)
	sf.m[key] = c
	sf.mu.Unlock()

	c.res, c.err = fn()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.m, key)
	sf.mu.Unlock()

	return c.res, c.err
}

// InFlight reports whether a fill for key is currently executing, and how
// long ago it started. HandlePeerHas consults it to answer 202, telling a
// peer that missed the index we are ALREADY fetching the key — so that peer
// waits for our fill instead of firing its own origin request for the same
// bytes.
func (sf *singleFlight) InFlight(key string) (time.Duration, bool) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	c, ok := sf.m[key]
	if !ok {
		return 0, false
	}
	return time.Since(c.start), true
}

func NewCacheProxy(store *DiskCache, peers *PeerManager, cacheHostSuffixes []string) *CacheProxy {
	return &CacheProxy{
		store:                     store,
		peers:                     peers,
		client:                    &http.Client{Timeout: defaultOriginTimeout},
		originTimeout:             defaultOriginTimeout,
		originRetryMaxAttempts:    defaultOriginRetryMaxAttempts,
		originRetryInitialBackoff: defaultOriginRetryInitialBackoff,
		originRetryMaxBackoff:     defaultOriginRetryMaxBackoff,
		cacheHostSuffixes:         cacheHostSuffixes,
	}
}

// How long a peer's /cache/get (or a local wait on a peer's in-flight fill)
// may block waiting for that peer's fill to land before giving up to origin.
// Long enough for a healthy multi-block fill, short enough that a wedged peer
// can't hold request latency hostage once the origin would have answered.
const peerFillWait = 10 * time.Second

// shouldCache returns true if the request targets a host we want to cache.
// When no suffixes are configured, all GETs are cached (legacy behavior).
func (p *CacheProxy) shouldCache(r *http.Request) bool {
	if len(p.cacheHostSuffixes) == 0 {
		return true
	}
	host := r.URL.Host
	if host == "" {
		host = r.Host
	}
	for _, s := range p.cacheHostSuffixes {
		if strings.Contains(host, s) {
			return true
		}
	}
	return false
}

// handleConnect tunnels an HTTPS CONNECT request. We don't cache — just copy
// bytes between client and origin. Required because SET GLOBAL http_proxy
// makes DuckDB tunnel all HTTPS through us, including external sources.
//
// What we CAN log: tunnel open (Info), dial / hijack errors (Warn / Error),
// final byte counts and duration on close (Info). What we CANNOT log: the
// actual HTTP request / response inside the tunnel — TLS terminates between
// the worker and the origin, so the encrypted bytes flowing past us are
// opaque. An S3 501 with an XML error envelope going through CONNECT is
// invisible to us at the body level; only "CONNECT to s3:443 → N bytes
// in / M bytes out / closed in T ms" is recoverable.
//
// We log this anyway because a black hole is worse than a partial trail —
// at least we can confirm a request was attempted, see the target host,
// and spot dial failures. For full request/response visibility on writes,
// DuckDB has to actually use plain HTTP via forwardUncached (s3_use_ssl =
// false), which httpfs has been observed ignoring for some PUT paths.
func (p *CacheProxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	connectStart := time.Now()
	target := r.Host
	_, span := proxyTracer.Start(r.Context(), "cache.connect", trace.WithAttributes(
		attribute.String("server.address", target),
		attribute.String("client.address", r.RemoteAddr),
	))
	upstream, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.End()
		slog.Warn("Forward-proxy CONNECT dial failed.", "target", target, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		span.SetStatus(codes.Error, "hijacking not supported")
		span.End()
		slog.Error("Forward-proxy CONNECT hijack unsupported.", "target", target)
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	client, _, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		span.SetStatus(codes.Error, err.Error())
		span.End()
		slog.Warn("Forward-proxy CONNECT hijack failed.", "target", target, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = upstream.Close()
		_ = client.Close()
		span.SetStatus(codes.Error, err.Error())
		span.End()
		slog.Warn("Forward-proxy CONNECT 200 write failed.", "target", target, "error", err)
		return
	}
	slog.Info("Forward-proxy CONNECT opened.", "target", target)

	// Track byte counts in both directions and emit a single closed log
	// once both legs finish, so a single CONNECT produces exactly one
	// opened + one closed pair with the bytes transferred. Wait on both
	// goroutines so the close-log fires once, not racy.
	var sentToUpstream, recvFromUpstream int64
	upstreamDone := make(chan struct{})
	clientDone := make(chan struct{})

	go func() {
		defer close(upstreamDone)
		n, _ := io.Copy(upstream, client)
		sentToUpstream = n
		_ = upstream.Close()
		_ = client.Close()
	}()
	go func() {
		defer close(clientDone)
		n, _ := io.Copy(client, upstream)
		recvFromUpstream = n
		_ = upstream.Close()
		_ = client.Close()
	}()

	go func() {
		<-upstreamDone
		<-clientDone
		span.SetAttributes(
			attribute.Int64("duckgres.connect.sent_bytes", sentToUpstream),
			attribute.Int64("duckgres.connect.recv_bytes", recvFromUpstream),
		)
		span.End()
		slog.Info("Forward-proxy CONNECT closed.",
			"target", target,
			"sent_bytes", sentToUpstream,
			"recv_bytes", recvFromUpstream,
			"duration_ms", time.Since(connectStart).Milliseconds())
	}()
}

// Hop-by-hop headers per RFC 7230 §6.1 — must not be forwarded.
var hopByHop = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailers":            true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

// cachePassthroughHeader is added only by duckgres's worker-local router.
// It selects the proxy's observable-but-uncached request path and is stripped
// before the origin request so it cannot affect S3 or a SigV4 request.
const cachePassthroughHeader = "X-Duckgres-Cache-Passthrough"

func isInternalPropagationHeader(header string) bool {
	return strings.EqualFold(header, "traceparent") || strings.EqualFold(header, "tracestate")
}

// HandleProxy handles forward HTTP proxy requests from DuckDB httpfs.
// Expects absolute-form URIs (scheme + host + path in the request-line).
func (p *CacheProxy) HandleProxy(w http.ResponseWriter, r *http.Request) {
	inflightRequests.Inc()
	defer inflightRequests.Dec()
	r = r.WithContext(otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)))

	// HTTPS via CONNECT tunnel — we can't cache encrypted traffic, but we must
	// still tunnel it so DuckDB can reach external HTTPS sources (e.g.
	// read_parquet('https://datasets.clickhouse.com/...')) while
	// http_proxy is set globally.
	if r.Method == http.MethodConnect {
		p.handleConnect(w, r)
		return
	}

	if r.URL.Scheme == "" || r.URL.Host == "" {
		http.Error(w, "expected forward-proxy absolute-form URL", http.StatusBadRequest)
		return
	}

	// Non-GET (HEAD, etc.) is never cached — forward and return.
	if r.Method != http.MethodGet {
		p.forwardUncached(w, r)
		return
	}
	if r.Header.Get(cachePassthroughHeader) == "true" {
		p.forwardUncached(w, r)
		return
	}

	// Only cache URLs that look like DuckLake bucket traffic. Anything else
	// (non-bucket HTTP) is a passthrough.
	if !p.shouldCache(r) {
		p.forwardUncached(w, r)
		return
	}

	rangeHeader := r.Header.Get("Range")

	if p.blockMode && p.serveBlockAligned(w, r, rangeHeader) {
		return
	}
	// Legacy exact-range path (also the fallback for non-absolute ranges).
	cacheKey := CacheKey(r.URL.String(), rangeHeader)

	// Requests without a propagated parent still start a standalone trace.
	// Thread the cache request span context into origin/peer work below.
	ctx, span := proxyTracer.Start(r.Context(), "cache.get", trace.WithAttributes(requestSpanAttrs(r)...))
	defer span.End()
	span.SetAttributes(attribute.String("duckgres.cache.key", cacheKey))
	r = r.WithContext(ctx)

	if reader, size, ok := p.store.Open(cacheKey); ok {
		cacheBytesServed.WithLabelValues("local").Add(float64(size))
		span.SetAttributes(
			attribute.String("duckgres.cache.source", "hit"),
			attribute.Bool("duckgres.cache.hit", true),
			attribute.Int64("duckgres.bytes", size),
		)
		slog.Info("Served.", "source", "hit", "url", r.URL.String(), "range", rangeHeader, "bytes", size)
		p.serveStream(w, reader, size, rangeHeader, "")
		_ = reader.Close()
		return
	}
	cacheMissesTotal.Inc()
	span.SetAttributes(attribute.Bool("duckgres.cache.hit", false))

	res, err := p.fetchDedup(cacheKey, r, rangeHeader)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		// An origin that responded with a non-2xx (e.g. S3 returning a 400 with
		// <Code>ExpiredToken</Code> in an XML envelope) is forwarded back to
		// DuckDB verbatim — same status code, same body, same headers minus
		// hop-by-hop. This preserves the error class so httpfs can distinguish
		// transient (5xx) from terminal (4xx) failures, and gives DuckLake the
		// raw S3 error body it knows how to parse.
		var oe *originStatusError
		if errors.As(err, &oe) {
			span.SetAttributes(attribute.Int("http.response.status_code", oe.status))
			slog.Warn("Origin returned non-2xx; forwarding verbatim.",
				"url", r.URL.String(), "range", rangeHeader, "status", oe.status, "body_preview", previewBody(oe.body))
			oe.writeTo(w)
			return
		}
		// True transport-level failure (DNS, connection refused, TLS, timeout):
		// no upstream status exists, so 502 Bad Gateway is the right answer
		// here — and it's also the one DuckDB's httpfs treats as transient.
		slog.Error("Failed to fetch.", "url", r.URL.String(), "range", rangeHeader, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// fetchDedup already committed the body to disk under cacheKey. Serve it by
	// streaming from the file (openFile, not Open — the miss was already counted,
	// so this must not also register a hit).
	reader, size, ok := p.store.openFile(cacheKey)
	if !ok {
		// The entry was evicted in the narrow window between commit and serve.
		// 502 so httpfs retries rather than receiving a truncated body.
		slog.Warn("Cached entry vanished before serve.", "url", r.URL.String(), "range", rangeHeader)
		http.Error(w, "cache entry vanished", http.StatusBadGateway)
		return
	}
	defer func() { _ = reader.Close() }()
	span.SetAttributes(
		attribute.String("duckgres.cache.source", res.source),
		attribute.Int64("duckgres.bytes", size),
	)
	slog.Info("Served.", "source", res.source, "url", r.URL.String(), "range", rangeHeader, "bytes", size)
	p.serveStream(w, reader, size, rangeHeader, res.contentType)
}

// fetchDedup resolves a local miss to on-disk bytes with as little origin
// traffic as the cluster state allows. On success the body has been committed
// to local disk under cacheKey; the caller serves it by streaming from the
// file. Nothing here holds the body in memory.
func (p *CacheProxy) fetchDedup(cacheKey string, r *http.Request, rangeHeader string) (fetchResult, error) {
	return p.flights.Do(cacheKey, func() (fetchResult, error) {
		if p.peers != nil {
			if p.peers.lookupMode == peerLookupSummary {
				// One logical lookup consists entirely of local Bloom tests and
				// at most two direct GETs under a shared deadline.
				peerFetchesTotal.Inc()
				peerCtx, cancel := context.WithTimeout(r.Context(), peerHasTimeout)
				defer cancel()
				for _, holder := range p.peers.SummaryCandidates(cacheKey, time.Now()) {
					res, ok := p.fetchFromPeer(holder, false, cacheKey, r.WithContext(peerCtx))
					if ok {
						peerDirectGetsTotal.WithLabelValues("success").Inc()
						return res, nil
					}
					peerDirectGetsTotal.WithLabelValues("miss_or_error").Inc()
					if peerCtx.Err() != nil {
						break
					}
				}
			} else if holder, flight, ok := p.peers.LocateKey(r.Context(), cacheKey); ok {
				res, ok := p.fetchFromPeer(holder, flight, cacheKey, r)
				if ok {
					return res, nil
				}
			}
		}
		originFetchInFlight.Inc()
		defer originFetchInFlight.Dec()
		size, ct, err := p.fetchOrigin(cacheKey, r)
		if err != nil {
			originFetchesTotal.WithLabelValues(originFetchOutcome(err)).Inc()
			return fetchResult{}, err
		}
		originFetchesTotal.WithLabelValues("success").Inc()
		cacheBytesServed.WithLabelValues("s3").Add(float64(size))
		return fetchResult{size: size, contentType: ct, source: "miss"}, nil
	})
}

// fetchFromPeer pulls cacheKey's body from a peer that has it (flight=false)
// or is mid-flight filling it (flight=true; the peer's /cache/get then blocks
// briefly on its fill instead of 404ing, so we read the bytes the peer just
// fetched rather than re-fetching them from the origin ourselves).
func (p *CacheProxy) fetchFromPeer(holder string, flight bool, cacheKey string, r *http.Request) (fetchResult, bool) {
	n, ok := p.peers.FetchFromPeer(r.Context(), holder, cacheKey, flight, func(body io.Reader) (int64, error) {
		return p.store.PutStream(cacheKey, body)
	})
	if !ok {
		return fetchResult{}, false
	}
	cacheBytesServed.WithLabelValues("peer").Add(float64(n))
	return fetchResult{size: n, source: "peer"}, true
}

// fetchOrigin forwards the request verbatim (headers, Host, signature) to the
// real origin and streams a successful body straight to the on-disk cache,
// returning the stored size and Content-Type. The SigV4 signature remains valid
// because the URL and Host header are unchanged. Streaming to disk (rather than
// io.ReadAll into a []byte) is what keeps proxy memory flat under concurrent
// large range reads. A non-2xx response is NOT cached — its (capped) error body
// is captured and returned as an originStatusError for verbatim forwarding.
func (p *CacheProxy) fetchOrigin(cacheKey string, r *http.Request) (int64, string, error) {
	_, originSpan := proxyTracer.Start(r.Context(), "cache.origin_fetch")
	defer originSpan.End()

	var size int64
	var contentType string
	err := p.retryOriginFetch(r, originSpan, func() error {
		var attemptErr error
		size, contentType, attemptErr = p.fetchOriginOnce(cacheKey, r)
		return attemptErr
	})
	if err != nil {
		return 0, "", err
	}
	originSpan.SetAttributes(attribute.Int64("duckgres.bytes", size))
	return size, contentType, nil
}

// retryOriginFetch runs attempt (one origin fetch) up to
// originRetryMaxAttempts times with jittered exponential backoff, while the
// error is retriable (isRetriableOriginFetchError) and the request context is
// alive. Shared by the legacy exact-range path (fetchOrigin) and the
// block-aligned path (fetchOriginSpan) so a transient origin 5xx or transport
// blip is absorbed by the cache layer instead of surfacing to DuckDB as a
// 502. Terminal origin statuses (4xx) and poisoned-response guards fail on
// the first attempt.
func (p *CacheProxy) retryOriginFetch(r *http.Request, originSpan trace.Span, attempt func() error) error {
	attempts := p.originRetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	backoff := p.originRetryInitialBackoff
	var lastErr error
	for i := 1; i <= attempts; i++ {
		err := attempt()
		originSpan.SetAttributes(attribute.Int("duckgres.origin.attempts", i))
		if err == nil {
			return nil
		}
		lastErr = err
		var oe *originStatusError
		if errors.As(err, &oe) {
			originSpan.SetAttributes(attribute.Int("http.response.status_code", oe.status))
		}
		if err := r.Context().Err(); err != nil {
			originSpan.SetStatus(codes.Error, err.Error())
			return err
		}
		if i == attempts || !isRetriableOriginFetchError(err) {
			originSpan.SetStatus(codes.Error, err.Error())
			return err
		}

		delay := jitteredOriginRetryDelay(backoff, p.originRetryMaxBackoff)
		slog.Warn("Origin fetch failed with retriable error, retrying.",
			"url", r.URL.String(),
			"range", r.Header.Get("Range"),
			"attempt", i,
			"max_attempts", attempts,
			"backoff", delay,
			"error", err)
		if !sleepContext(r.Context(), delay) {
			err := r.Context().Err()
			if err == nil {
				err = context.Canceled
			}
			originSpan.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := r.Context().Err(); err != nil {
			originSpan.SetStatus(codes.Error, err.Error())
			return err
		}
		originFetchRetriesTotal.WithLabelValues(originRetryReason(err)).Inc()
		if backoff > 0 {
			backoff *= 2
			if p.originRetryMaxBackoff > 0 && backoff > p.originRetryMaxBackoff {
				backoff = p.originRetryMaxBackoff
			}
		}
	}
	if lastErr != nil {
		originSpan.SetStatus(codes.Error, lastErr.Error())
	}
	return lastErr
}

func jitteredOriginRetryDelay(backoff, maxBackoff time.Duration) time.Duration {
	if backoff <= 0 {
		return 0
	}
	if maxBackoff > 0 && backoff > maxBackoff {
		backoff = maxBackoff
	}
	return time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5))
}

func (p *CacheProxy) fetchOriginOnce(cacheKey string, r *http.Request) (int64, string, error) {
	timeout := p.originTimeout
	if timeout <= 0 {
		timeout = defaultOriginTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL.String(), nil)
	if err != nil {
		return 0, "", err
	}
	for k, vv := range r.Header {
		if hopByHop[strings.ToLower(k)] || isInternalPropagationHeader(k) {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		// Capture the body up to a generous cap. S3 error envelopes are
		// typically <1 KiB; the cap is just a guard against a misbehaving
		// origin streaming forever. The 60s context above also protects us.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, originErrorBodyCap))
		return 0, "", &originStatusError{
			status:  resp.StatusCode,
			headers: resp.Header.Clone(),
			body:    body,
		}
	}

	size, err := p.store.PutStream(cacheKey, resp.Body)
	if err != nil {
		return 0, "", err
	}
	return size, resp.Header.Get("Content-Type"), nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isRetriableOriginFetchError(err error) bool {
	if err == nil {
		return false
	}
	var oe *originStatusError
	if errors.As(err, &oe) {
		switch oe.status {
		case http.StatusRequestTimeout,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "timeout")
}

func originFetchOutcome(err error) string {
	if err == nil {
		return "success"
	}
	var oe *originStatusError
	if errors.As(err, &oe) {
		return "http_error"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "error"
}

func originRetryReason(err error) string {
	var oe *originStatusError
	if errors.As(err, &oe) {
		return fmt.Sprintf("http_%d", oe.status)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection reset by peer"):
		return "connection_reset"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "unexpected eof"):
		return "unexpected_eof"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	default:
		return "transport_error"
	}
}

// originErrorBodyCap is the maximum number of bytes we'll buffer from a
// non-2xx origin response. S3 XML error envelopes are tiny; this is just a
// safety net.
const originErrorBodyCap = 1 << 20 // 1 MiB

// originStatusError captures a non-2xx response from the origin so the
// proxy can forward it back to the client verbatim. The status code, body,
// and response headers are all preserved (minus hop-by-hop) so DuckDB sees
// exactly what S3 said — including the XML error envelope DuckLake may
// inspect.
type originStatusError struct {
	status  int
	headers http.Header
	body    []byte
}

func (e *originStatusError) Error() string {
	return fmt.Sprintf("origin %d: %s", e.status, strings.TrimSpace(string(e.body)))
}

// writeTo replays the captured origin response onto w. Any header the origin
// set that isn't a hop-by-hop is forwarded — except Content-Length, which is
// always rewritten to the length of the body we ACTUALLY captured. The error
// body is read capped (originErrorBodyCap) precisely because we don't trust
// the origin to stop; forwarding the origin's declared length for bytes we
// never received would hang the client waiting for the remainder.
func (e *originStatusError) writeTo(w http.ResponseWriter) {
	for k, vv := range e.headers {
		if hopByHop[strings.ToLower(k)] || strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(e.body)))
	w.WriteHeader(e.status)
	_, _ = w.Write(e.body)
}

// previewBody returns up to 256 bytes of the body for log lines so we don't
// spam multi-KiB XML envelopes into structured logs while still keeping the
// useful prefix (S3 puts the <Code>...</Code> first).
func previewBody(body []byte) string {
	const max = 256
	if len(body) <= max {
		return string(body)
	}
	return string(body[:max]) + "...(truncated)"
}

// serveStream copies a cached body from r back to the client, reconstructing
// 206 Partial Content semantics when the original request had a Range header.
// size is the on-disk length (known up front), so Content-Length and the
// Content-Range total are set before any body bytes flow — same response shape
// the old buffered serveBody produced, without holding the body in memory.
func (p *CacheProxy) serveStream(w http.ResponseWriter, r io.Reader, size int64, rangeHeader, contentType string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	if rangeHeader != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %s/%d", strings.TrimPrefix(rangeHeader, "bytes="), size))
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = io.Copy(w, r)
}

// forwardUncached forwards a request to the origin without caching. Used for
// HEAD and other non-GET methods that shouldn't consume cache space.
//
// Logs symmetrically with HandleProxy's GET path so a non-2xx PUT/POST
// (e.g. an S3 501 on a parquet write) is visible in Loki — without this,
// the non-cached path was a black hole and an upstream-rejected write
// surfaced only as a DuckDB-side "HTTP code 501" with no proxy-side
// breadcrumb to correlate against.
//
// Transparency: the proxy must not silently mutate the request shape DuckDB
// chose, because AWS Sigv4 signs `host;x-amz-content-sha256;x-amz-date` (see
// duckdb-httpfs/src/s3fs.cpp:84) — which means Content-Length and
// Transfer-Encoding are NOT signed and we're free to set them, but we
// should match what the client sent so the wire shape is preserved.
//
// The bug we're fixing here: http.NewRequestWithContext only auto-populates
// req.ContentLength when the body is *bytes.Buffer / *bytes.Reader /
// *strings.Reader (per its docstring). For a generic io.ReadCloser like
// http.Request.Body, ContentLength stays 0 and Go's Transport falls back
// to Transfer-Encoding: chunked on the outbound. AWS S3 returns
// 501 NotImplemented for chunked PUT — so even though DuckDB sent a clean
// Content-Length-bearing PUT, the proxy was rewriting it as chunked and
// S3 rejected it. The fix is to mirror ContentLength + TransferEncoding +
// Trailer from the inbound request so the proxy is wire-shape-transparent.
func (p *CacheProxy) forwardUncached(w http.ResponseWriter, r *http.Request) {
	forwardRequestsTotal.WithLabelValues(r.Method).Inc()
	forwardStart := time.Now()
	ctx, span := proxyTracer.Start(r.Context(), "cache.forward", trace.WithAttributes(requestSpanAttrs(r)...))
	defer span.End()
	passthrough := r.Header.Get(cachePassthroughHeader) == "true"
	if passthrough {
		span.SetAttributes(attribute.Bool("duckgres.cache.passthrough", true))
	}
	r = r.WithContext(ctx)

	req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		slog.Warn("Forward-proxy request build failed.",
			"method", r.Method, "url", r.URL.String(), "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Mirror body-framing fields the inbound request had so Go's Transport
	// sends the same encoding (Content-Length vs chunked) as DuckDB chose.
	req.ContentLength = r.ContentLength
	req.TransferEncoding = r.TransferEncoding
	req.Trailer = r.Trailer
	for k, vv := range r.Header {
		if hopByHop[strings.ToLower(k)] || strings.EqualFold(k, cachePassthroughHeader) || isInternalPropagationHeader(k) {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host

	resp, err := p.client.Do(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		slog.Warn("Forward-proxy origin transport failed.",
			"method", r.Method, "url", r.URL.String(), "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))

	for k, vv := range resp.Header {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	// Capture the body for the log preview on non-2xx responses so the
	// origin's error envelope (e.g. S3's <Error><Code>...</Code>...) is
	// available without an additional debug round-trip. On 2xx we pass the
	// body straight through to the client without buffering — successful
	// writes can be large.
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		w.WriteHeader(resp.StatusCode)
		n, _ := io.Copy(w, resp.Body)
		span.SetAttributes(attribute.Int64("duckgres.bytes", n))
		dur := time.Since(forwardStart)
		requestDurationSeconds.WithLabelValues("forward", "origin").Observe(dur.Seconds())
		slog.Info("Forward-proxy served.",
			"method", r.Method, "url", r.URL.String(),
			"status", resp.StatusCode, "bytes", n, "dur_ms", dur.Milliseconds())
		return
	}

	span.SetStatus(codes.Error, fmt.Sprintf("origin %d", resp.StatusCode))
	body, _ := io.ReadAll(resp.Body)
	slog.Warn("Forward-proxy origin returned non-2xx.",
		"method", r.Method, "url", r.URL.String(),
		"status", resp.StatusCode, "body_preview", previewBody(body))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// HandlePeerHas answers "do you have this cache key?" from peers:
//
//	200 — tracked in the local index (probe counts as an access, so a block
//	      that is hot with peers is not evicted as least-recently-used while
//	      it is in fact one of the busiest entries on the node)
//	202 — not here yet, but a local fill is mid-flight; the requester should
//	      call /cache/get?flight=1 and wait for that fill instead of issuing
//	      its own origin fetch for the same bytes
//	404 — neither
func (p *CacheProxy) HandlePeerHas(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)))
	key := r.URL.Query().Get("key")
	if !IsValidCacheKey(key) {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}
	if p.store.Has(key) {
		p.store.Touch(key)
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, ok := p.flights.InFlight(key); ok {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// HandlePeerSummary receives best-effort Bloom hints over the existing peer
// transport. That transport is currently unauthenticated; membership checks
// merely avoid retaining unsolicited state and are not an auth boundary.
func (p *CacheProxy) HandlePeerSummary(w http.ResponseWriter, r *http.Request) {
	if p.peers == nil || p.peers.lookupMode != peerLookupSummary || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSummaryBodyBytes))
	if err != nil {
		summaryReceiptsTotal.WithLabelValues("rejected").Inc()
		http.Error(w, "summary too large", http.StatusRequestEntityTooLarge)
		return
	}
	peer, ok := p.peers.memberForRemoteAddr(r.RemoteAddr)
	if !ok || p.peers.ReceiveSummary(peer, body, time.Now()) != nil {
		http.Error(w, "invalid summary", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandlePeerGet returns cached data to a peer. With flight=1 and the key not
// yet on disk, it blocks (bounded by peerFillWait) for the local in-flight
// fill to land and then serves those bytes — the requester gets the fill it
// waited for instead of a 404 that would send it to the origin for the same
// bytes the peer is already fetching.
func (p *CacheProxy) HandlePeerGet(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)))
	ctx, span := proxyTracer.Start(r.Context(), "cache.peer_serve", trace.WithAttributes(
		attribute.String("http.request.method", r.Method),
		attribute.String("client.address", r.RemoteAddr),
	))
	defer span.End()
	r = r.WithContext(ctx)

	key := r.URL.Query().Get("key")
	if !IsValidCacheKey(key) {
		span.SetAttributes(attribute.Int("http.response.status_code", http.StatusBadRequest))
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	if r.URL.Query().Get("flight") == "1" && !p.store.Has(key) {
		p.waitForLocalFill(r, key)
	}

	// Peer traffic keeps the entry warm but must not inflate the worker-facing
	// local-hit counter. openFile touches LRU recency without recording a hit.
	reader, size, ok := p.store.openFile(key)
	if !ok {
		span.SetAttributes(attribute.Int("http.response.status_code", http.StatusNotFound))
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer func() { _ = reader.Close() }()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	n, _ := io.Copy(w, reader)
	span.SetAttributes(
		attribute.Int("http.response.status_code", http.StatusOK),
		attribute.Int64("duckgres.bytes", n),
	)
}

// waitForLocalFill blocks (bounded) until key lands on local disk or the
// in-flight fill for it finishes. Best-effort: a timeout just means the
// caller gets a 404 and falls back to the origin, same as before.
func (p *CacheProxy) waitForLocalFill(r *http.Request, key string) {
	deadline := time.Now().Add(peerFillWait)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if p.store.Has(key) {
			return
		}
		if _, ok := p.flights.InFlight(key); !ok || time.Now().After(deadline) {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
