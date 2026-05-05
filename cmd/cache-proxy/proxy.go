package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

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

	// cacheHostSuffixes are the Host substrings that identify DuckLake bucket
	// traffic worth caching. Requests whose Host doesn't contain any of these
	// are passed through without caching.
	cacheHostSuffixes []string
}

type singleFlight struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func (sf *singleFlight) Do(key string, fn func() ([]byte, error)) ([]byte, error) {
	sf.mu.Lock()
	if sf.m == nil {
		sf.m = make(map[string]*call)
	}
	if c, ok := sf.m[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call{}
	c.wg.Add(1)
	sf.m[key] = c
	sf.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.m, key)
	sf.mu.Unlock()

	return c.val, c.err
}

func NewCacheProxy(store *DiskCache, peers *PeerManager, cacheHostSuffixes []string) *CacheProxy {
	return &CacheProxy{
		store:             store,
		peers:             peers,
		client:            &http.Client{Timeout: 60 * time.Second},
		cacheHostSuffixes: cacheHostSuffixes,
	}
}

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
	upstream, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		slog.Warn("Forward-proxy CONNECT dial failed.", "target", target, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		slog.Error("Forward-proxy CONNECT hijack unsupported.", "target", target)
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	client, _, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		slog.Warn("Forward-proxy CONNECT hijack failed.", "target", target, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = upstream.Close()
		_ = client.Close()
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

// HandleProxy handles forward HTTP proxy requests from DuckDB httpfs.
// Expects absolute-form URIs (scheme + host + path in the request-line).
func (p *CacheProxy) HandleProxy(w http.ResponseWriter, r *http.Request) {
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

	// Only cache URLs that look like DuckLake bucket traffic. Anything else
	// (non-bucket HTTP) is a passthrough.
	if !p.shouldCache(r) {
		p.forwardUncached(w, r)
		return
	}

	rangeHeader := r.Header.Get("Range")
	cacheKey := CacheKey(r.URL.String(), rangeHeader)

	if data, ok := p.store.Get(cacheKey); ok {
		cacheBytesServed.WithLabelValues("local").Add(float64(len(data)))
		slog.Info("Served.", "source", "hit", "url", r.URL.String(), "range", rangeHeader, "bytes", len(data))
		p.serveBody(w, data, rangeHeader, "")
		return
	}
	cacheMissesTotal.Inc()

	data, contentType, source, err := p.fetchDedup(cacheKey, r, rangeHeader)
	if err != nil {
		// An origin that responded with a non-2xx (e.g. S3 returning a 400 with
		// <Code>ExpiredToken</Code> in an XML envelope) is forwarded back to
		// DuckDB verbatim — same status code, same body, same headers minus
		// hop-by-hop. This preserves the error class so httpfs can distinguish
		// transient (5xx) from terminal (4xx) failures, and gives DuckLake the
		// raw S3 error body it knows how to parse.
		var oe *originStatusError
		if errors.As(err, &oe) {
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

	if err := p.store.Put(cacheKey, data); err != nil {
		slog.Warn("Failed to cache.", "key", cacheKey[:16], "error", err)
	}
	slog.Info("Served.", "source", source, "url", r.URL.String(), "range", rangeHeader, "bytes", len(data))
	p.serveBody(w, data, rangeHeader, contentType)
}

// fetchDedup tries peers then origin, deduplicating concurrent fetches.
// contentType is reported only for origin fetches (peers strip it).
// source is "peer" or "miss" depending on where the data came from.
func (p *CacheProxy) fetchDedup(cacheKey string, r *http.Request, rangeHeader string) ([]byte, string, string, error) {
	var contentType, source string
	data, err := p.flights.Do(cacheKey, func() ([]byte, error) {
		if p.peers != nil {
			if peerData, peerAddr, ok := p.peers.FetchFromPeers(cacheKey); ok {
				cacheBytesServed.WithLabelValues("peer").Add(float64(len(peerData)))
				source = "peer"
				_ = peerAddr
				return peerData, nil
			}
		}
		data, ct, err := p.fetchOrigin(r)
		if err != nil {
			return nil, err
		}
		contentType = ct
		cacheBytesServed.WithLabelValues("s3").Add(float64(len(data)))
		source = "miss"
		return data, nil
	})
	return data, contentType, source, err
}

// fetchOrigin forwards the request verbatim (headers, Host, signature) to the
// real origin and returns the body. The SigV4 signature remains valid because
// the URL and Host header are unchanged.
func (p *CacheProxy) fetchOrigin(r *http.Request) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL.String(), nil)
	if err != nil {
		return nil, "", err
	}
	for k, vv := range r.Header {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		// Capture the body up to a generous cap. S3 error envelopes are
		// typically <1 KiB; the cap is just a guard against a misbehaving
		// origin streaming forever. The 60s context above also protects us.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, originErrorBodyCap))
		return nil, "", &originStatusError{
			status:  resp.StatusCode,
			headers: resp.Header.Clone(),
			body:    body,
		}
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return data, resp.Header.Get("Content-Type"), nil
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

// writeTo replays the captured origin response onto w. Any header the
// origin set that isn't a hop-by-hop is forwarded; status code and body
// follow.
func (e *originStatusError) writeTo(w http.ResponseWriter) {
	for k, vv := range e.headers {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
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

// serveBody writes cached data back to the client, reconstructing 206 Partial
// Content semantics when the original request had a Range header.
func (p *CacheProxy) serveBody(w http.ResponseWriter, data []byte, rangeHeader, contentType string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	if rangeHeader != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %s/%d", strings.TrimPrefix(rangeHeader, "bytes="), len(data)))
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(data)
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
	req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
	if err != nil {
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
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host

	resp, err := p.client.Do(req)
	if err != nil {
		slog.Warn("Forward-proxy origin transport failed.",
			"method", r.Method, "url", r.URL.String(), "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

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
		slog.Info("Forward-proxy served.",
			"method", r.Method, "url", r.URL.String(),
			"status", resp.StatusCode, "bytes", n)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	slog.Warn("Forward-proxy origin returned non-2xx.",
		"method", r.Method, "url", r.URL.String(),
		"status", resp.StatusCode, "body_preview", previewBody(body))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// HandlePeerHas responds to "do you have this cache key?" from peers.
func (p *CacheProxy) HandlePeerHas(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if !IsValidCacheKey(key) {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}
	if p.store.Has(key) {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// HandlePeerGet returns cached data to a peer.
func (p *CacheProxy) HandlePeerGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if !IsValidCacheKey(key) {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	reader, size, ok := p.store.Open(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer func() { _ = reader.Close() }()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	_, _ = io.Copy(w, reader)
}
