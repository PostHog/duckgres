package duckdbservice

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type cacheProxyMode string

const (
	cacheProxyModeDisabled cacheProxyMode = "disabled"
	cacheProxyModeCached   cacheProxyMode = "cached"
	cacheProxyModeBypassed cacheProxyMode = "bypassed"

	cacheProxyBypassReasonStartupUnavailable  = "startup_unavailable"
	cacheProxyBypassReasonRuntimeUnavailable  = "runtime_unavailable"
	cacheProxyBypassReasonUpstreamUnavailable = "upstream_unavailable"

	cacheProxyProbeTimeout         = 2 * time.Second
	cacheProxyReconnectInitial     = time.Second
	cacheProxyReconnectMaximum     = 30 * time.Second
	cacheProxyHealthyProbeInterval = 5 * time.Second

	cacheProxyDialTimeout         = 5 * time.Second
	cacheProxyTLSHandshakeTimeout = 5 * time.Second
	// The cache daemon may materialize an origin response for up to 60s before
	// sending headers, so leave a little coordination headroom here.
	cacheProxyResponseHeaderTimeout = 70 * time.Second
	cacheProxyIdleConnTimeout       = 90 * time.Second
)

// cacheProxyRouter is a stable, worker-local forward proxy. DuckDB always
// talks to this listener while caching is enabled. The router either forwards
// to the node daemon or directly to the signed S3 URL, so a daemon crash cannot
// strand an in-flight session behind a dead hostPort.
type cacheProxyRouter struct {
	cacheURL     *url.URL
	directUseTLS bool
	cacheClient  *http.Client
	directClient *http.Client

	mu   sync.RWMutex
	mode cacheProxyMode
}

type cacheProxySupervisorConfig struct {
	probeTimeout          time.Duration
	healthyProbeInterval  time.Duration
	initialReconnectDelay time.Duration
	maxReconnectDelay     time.Duration
	jitter                func(time.Duration) time.Duration
}

// cacheProxyTransportConfig intentionally excludes a whole-request timeout.
// The router may stream a large S3 range for longer than any sensible
// connection/header deadline; cancelling that body would corrupt the read.
type cacheProxyTransportConfig struct {
	dialTimeout           time.Duration
	tlsHandshakeTimeout   time.Duration
	responseHeaderTimeout time.Duration
	idleConnTimeout       time.Duration
}

func (r *cacheProxyRouter) setDirectUseTLS(useTLS bool) {
	r.mu.Lock()
	r.directUseTLS = useTLS
	r.mu.Unlock()
}

func (r *cacheProxyRouter) directUsesTLS() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.directUseTLS
}

func newCacheProxyRouter(cacheAddr string, directUseTLS bool) *cacheProxyRouter {
	return newCacheProxyRouterWithTransport(cacheAddr, directUseTLS, cacheProxyTransportConfig{})
}

func newCacheProxyRouterWithTransport(cacheAddr string, directUseTLS bool, cfg cacheProxyTransportConfig) *cacheProxyRouter {
	cfg = normalizeCacheProxyTransportConfig(cfg)
	cacheURL := &url.URL{Scheme: "http", Host: cacheAddr}
	router := &cacheProxyRouter{
		cacheURL:     cacheURL,
		directUseTLS: directUseTLS,
		cacheClient:  &http.Client{Transport: newCacheProxyTransport(cacheURL, cfg)},
		// Never inherit HTTP_PROXY here: bypass must not accidentally route back
		// through another proxy when the node-local cache daemon is unhealthy.
		directClient: &http.Client{Transport: newCacheProxyTransport(nil, cfg)},
		mode:         cacheProxyModeBypassed,
	}
	recordCacheProxyMode(router.mode)
	return router
}

func normalizeCacheProxyTransportConfig(cfg cacheProxyTransportConfig) cacheProxyTransportConfig {
	if cfg.dialTimeout == 0 {
		cfg.dialTimeout = cacheProxyDialTimeout
	}
	if cfg.tlsHandshakeTimeout == 0 {
		cfg.tlsHandshakeTimeout = cacheProxyTLSHandshakeTimeout
	}
	if cfg.responseHeaderTimeout == 0 {
		cfg.responseHeaderTimeout = cacheProxyResponseHeaderTimeout
	}
	if cfg.idleConnTimeout == 0 {
		cfg.idleConnTimeout = cacheProxyIdleConnTimeout
	}
	return cfg
}

func newCacheProxyTransport(proxyURL *url.URL, cfg cacheProxyTransportConfig) *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.dialTimeout}
	return &http.Transport{
		Proxy:                 http.ProxyURL(proxyURL),
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   cfg.tlsHandshakeTimeout,
		ResponseHeaderTimeout: cfg.responseHeaderTimeout,
		IdleConnTimeout:       cfg.idleConnTimeout,
	}
}

func (r *cacheProxyRouter) Mode() cacheProxyMode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mode
}

func (r *cacheProxyRouter) setMode(mode cacheProxyMode) {
	r.mu.Lock()
	previous := r.mode
	r.mode = mode
	r.mu.Unlock()
	if previous == mode {
		return
	}
	recordCacheProxyMode(mode)
	if mode == cacheProxyModeCached {
		cacheProxyRecoveriesTotal.Inc()
		slog.Info("Cache proxy recovered; re-enabled local NVMe cache.")
		return
	}
	if mode == cacheProxyModeBypassed {
		slog.Warn("Entering cache-proxy bypass mode; reads will fetch from peers or S3 through the authoritative source path.")
	}
}

func recordCacheProxyMode(mode cacheProxyMode) {
	for _, candidate := range []cacheProxyMode{cacheProxyModeDisabled, cacheProxyModeCached, cacheProxyModeBypassed} {
		value := 0.0
		if candidate == mode {
			value = 1
		}
		cacheProxyModeGauge.WithLabelValues(string(candidate)).Set(value)
	}
}

func (r *cacheProxyRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodConnect {
		r.handleConnect(w, req)
		return
	}
	if r.Mode() == cacheProxyModeCached {
		resp, err := r.cacheClient.Do(cloneProxyRequest(req, false))
		if err == nil {
			defer func() { _ = resp.Body.Close() }()
			copyProxyResponse(w, resp)
			return
		}
		// Only retry read-only operations after a failed dial before any response
		// exists. A cache HTTP response (including an S3 integrity error) is never
		// hidden, and writes are never replayed.
		if isCacheProxyUnavailable(err) && isReadOnlyRequest(req) {
			cacheProxyBypassedOperationsTotal.WithLabelValues(cacheProxyBypassReasonUpstreamUnavailable).Inc()
			cacheProxyBypassTransitionsTotal.WithLabelValues(cacheProxyBypassReasonUpstreamUnavailable).Inc()
			r.setMode(cacheProxyModeBypassed)
			resp, err = r.directClient.Do(cloneProxyRequest(req, r.directUsesTLS()))
			if err == nil {
				defer func() { _ = resp.Body.Close() }()
				copyProxyResponse(w, resp)
				return
			}
		}
		http.Error(w, "cache proxy unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}

	cacheProxyBypassedOperationsTotal.WithLabelValues(cacheProxyBypassReasonRuntimeUnavailable).Inc()
	resp, err := r.directClient.Do(cloneProxyRequest(req, r.directUsesTLS()))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	copyProxyResponse(w, resp)
}

func isReadOnlyRequest(req *http.Request) bool {
	return req.Method == http.MethodGet || req.Method == http.MethodHead
}

func cloneProxyRequest(req *http.Request, useTLS bool) *http.Request {
	copy := req.Clone(req.Context())
	copy.RequestURI = ""
	if useTLS && copy.URL.Scheme == "http" {
		copy.URL.Scheme = "https"
	}
	return copy
}

func copyProxyResponse(w http.ResponseWriter, resp *http.Response) {
	for key, values := range resp.Header {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func isCacheProxyUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// handleConnect preserves the existing generic httpfs behavior. In bypass
// mode it tunnels directly to the requested host; in cached mode it uses the
// daemon as the CONNECT proxy. S3 reads use plain HTTP and take ServeHTTP's
// direct source path above.
func (r *cacheProxyRouter) handleConnect(w http.ResponseWriter, req *http.Request) {
	target := req.Host
	if target == "" {
		target = req.URL.Host
	}
	if r.Mode() == cacheProxyModeCached {
		conn, err := net.DialTimeout("tcp", r.cacheURL.Host, cacheProxyProbeTimeout)
		if err == nil {
			defer func() { _ = conn.Close() }()
			if err := req.Write(conn); err == nil {
				if hijacker, ok := w.(http.Hijacker); ok {
					client, _, hijackErr := hijacker.Hijack()
					if hijackErr == nil {
						defer func() { _ = client.Close() }()
						go func() { _, _ = io.Copy(conn, client) }()
						_, _ = io.Copy(client, conn)
						return
					}
				}
			}
		}
	}
	conn, err := net.DialTimeout("tcp", target, cacheProxyProbeTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = conn.Close() }()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection hijacking unavailable", http.StatusInternalServerError)
		return
	}
	client, _, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer func() { _ = client.Close() }()
	_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	go func() { _, _ = io.Copy(conn, client) }()
	_, _ = io.Copy(client, conn)
}

func (r *cacheProxyRouter) supervise(healthURL string, stop <-chan struct{}) {
	r.superviseWith(healthURL, stop, cacheProxySupervisorConfig{
		probeTimeout:          cacheProxyProbeTimeout,
		healthyProbeInterval:  cacheProxyHealthyProbeInterval,
		initialReconnectDelay: cacheProxyReconnectInitial,
		maxReconnectDelay:     cacheProxyReconnectMaximum,
		jitter:                jitterCacheProxyBackoff,
	})
}

func (r *cacheProxyRouter) superviseWith(healthURL string, stop <-chan struct{}, cfg cacheProxySupervisorConfig) {
	backoff := cfg.initialReconnectDelay
	for {
		wait := cfg.healthyProbeInterval
		if r.Mode() == cacheProxyModeBypassed {
			wait = cfg.jitter(backoff)
		}
		timer := time.NewTimer(wait)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
		cacheProxyReconnectAttemptsTotal.Inc()
		if cacheProxyHealthy(healthURL, cfg.probeTimeout) {
			r.setMode(cacheProxyModeCached)
			backoff = cfg.initialReconnectDelay
			continue
		}
		if r.Mode() == cacheProxyModeCached {
			cacheProxyBypassTransitionsTotal.WithLabelValues(cacheProxyBypassReasonRuntimeUnavailable).Inc()
			r.setMode(cacheProxyModeBypassed)
		}
		backoff *= 2
		if backoff > cfg.maxReconnectDelay {
			backoff = cfg.maxReconnectDelay
		}
	}
}

func jitterCacheProxyBackoff(backoff time.Duration) time.Duration {
	return backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
}

func cacheProxyHealthy(healthURL string, timeout time.Duration) bool {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(healthURL)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}
