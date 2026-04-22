package main

import (
	"context"
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
func (p *CacheProxy) handleConnect(w http.ResponseWriter, r *http.Request) {
	upstream, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}
	client, _, err := hijacker.Hijack()
	if err != nil {
		_ = upstream.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = upstream.Close()
		_ = client.Close()
		return
	}
	go func() {
		defer func() { _ = upstream.Close() }()
		defer func() { _ = client.Close() }()
		_, _ = io.Copy(upstream, client)
	}()
	go func() {
		defer func() { _ = upstream.Close() }()
		defer func() { _ = client.Close() }()
		_, _ = io.Copy(client, upstream)
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, "", fmt.Errorf("origin %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return data, resp.Header.Get("Content-Type"), nil
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
func (p *CacheProxy) forwardUncached(w http.ResponseWriter, r *http.Request) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		if hopByHop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// HandlePeerHas responds to "do you have this cache key?" from peers.
func (p *CacheProxy) HandlePeerHas(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
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
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
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
