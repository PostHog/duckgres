package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	peerFetchesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_fetches_total",
		Help: "Total peer cache fetch attempts",
	})
	peerHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_hits_total",
		Help: "Total successful peer cache hits",
	})
)

// Peer timeouts. The /cache/has probe is a tiny HEAD-like check, so it gets a
// short WHOLE-request budget. The /cache/get transfer can move many MB of a
// Parquet range over the VPC at whatever the link currently allows, so it has
// NO whole-request timeout — a large healthy transfer must not be killed for
// being large (that silently downgraded hits into full S3 refetches). Its
// guard is a response-header deadline long enough to also cover the bounded
// wait a peer does when we ask about its in-flight fill.
const (
	peerHasTimeout             = 1 * time.Second
	peerGetResponseHeaderLimit = peerFillWait + 2*time.Second
)

// PeerManager discovers and communicates with cache proxy peers
// via a Kubernetes headless Service.
type PeerManager struct {
	serviceName  string
	peerPort     string       // port for peer API (e.g. ":8081")
	client       *http.Client // /cache/has probes (short whole-request timeout)
	streamClient *http.Client // /cache/get transfers (header deadline, no body timeout)

	mu    sync.RWMutex
	peers []string // peer addresses (ip:port)
}

func NewPeerManager(serviceName, peerPort string) *PeerManager {
	return &PeerManager{
		serviceName: serviceName,
		peerPort:    peerPort,
		client: &http.Client{
			Timeout: peerHasTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		streamClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       30 * time.Second,
				ResponseHeaderTimeout: peerGetResponseHeaderLimit,
			},
		},
	}
}

// WatchEndpoints periodically resolves the headless Service DNS to
// discover peer cache proxy pods.
func (pm *PeerManager) WatchEndpoints(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial resolve
	pm.resolve()

	for {
		select {
		case <-ticker.C:
			pm.resolve()
		case <-ctx.Done():
			return
		}
	}
}

func (pm *PeerManager) resolve() {
	ips, err := net.LookupHost(pm.serviceName)
	if err != nil {
		slog.Debug("Peer DNS resolve failed.", "service", pm.serviceName, "error", err)
		return
	}

	// Get our own IPs to exclude self
	myIPs := getLocalIPs()
	mySet := make(map[string]bool, len(myIPs))
	for _, ip := range myIPs {
		mySet[ip] = true
	}

	// Extract port number from peerPort (e.g. ":8081" → "8081")
	port := pm.peerPort
	if len(port) > 0 && port[0] == ':' {
		port = port[1:]
	}

	var peers []string
	for _, ip := range ips {
		if mySet[ip] {
			continue // skip self
		}
		peers = append(peers, fmt.Sprintf("%s:%s", ip, port))
	}

	pm.mu.Lock()
	pm.peers = peers
	pm.mu.Unlock()

	if len(peers) > 0 {
		slog.Debug("Discovered peers.", "count", len(peers), "peers", peers)
	}
}

// probeResult is one peer's answer to a /cache/has probe.
type probeResult struct {
	addr     string
	status   int // 200 has it · 202 mid-flight filling it · anything else: no
	err      error
	duration time.Duration
}

// LocateKey asks every peer in parallel whether it has cacheKey (200) or is
// mid-flight filling it (202), returning the first useful claim. A present
// entry wins over an in-flight one when both answer before the probes are
// cancelled; ok=false means nothing anywhere knows about the key and the
// caller should go to the origin. Cluster-wide this is what stops a cold key
// bursting into one duplicate origin fetch per node: the first node's
// in-flight fetch answers 202, so every other node waits for that fill.
func (pm *PeerManager) LocateKey(ctx context.Context, cacheKey string) (holder string, flight, ok bool) {
	pm.mu.RLock()
	peers := make([]string, len(pm.peers))
	copy(peers, pm.peers)
	pm.mu.RUnlock()

	if len(peers) == 0 {
		return "", false, false
	}

	peerFetchesTotal.Inc()
	ctx, span := proxyTracer.Start(ctx, "cache.peer_lookup", trace.WithAttributes(
		attribute.Int("duckgres.cache.peer_count", len(peers)),
	))

	ctx, cancel := context.WithTimeout(ctx, peerHasTimeout)
	defer cancel()

	resCh := make(chan probeResult, len(peers))
	for _, addr := range peers {
		go func(addr string) {
			startedAt := time.Now()
			res := probeResult{addr: addr, status: -1}
			hasURL := fmt.Sprintf("http://%s/cache/has?key=%s", addr, cacheKey)
			if req, err := http.NewRequestWithContext(ctx, "GET", hasURL, nil); err == nil {
				otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
				if resp, err := pm.client.Do(req); err == nil {
					res.status = resp.StatusCode
					_ = resp.Body.Close()
				} else {
					res.err = err
				}
			} else {
				res.err = err
			}
			res.duration = time.Since(startedAt)
			resCh <- res
		}(addr)
	}

	recordProbe := func(res probeResult) {
		span.AddEvent("cache.peer_probe", trace.WithAttributes(
			attribute.String("duckgres.cache.peer.address", res.addr),
			attribute.String("duckgres.cache.peer.outcome", probeOutcome(res)),
			attribute.Int64("duckgres.cache.peer.duration_ms", res.duration.Milliseconds()),
		))
	}
	firstFlight := ""
	for i := 0; i < len(peers); i++ {
		res := <-resCh
		recordProbe(res)
		switch res.status {
		case http.StatusOK:
			cancel() // release the losing probes
			// Preserve the first-present response latency while the lookup span
			// records the canceled peers' bounded outcomes in the background.
			go func(remaining int) {
				defer span.End()
				for range remaining {
					recordProbe(<-resCh)
				}
			}(len(peers) - i - 1)
			return res.addr, false, true
		case http.StatusAccepted:
			if firstFlight == "" {
				firstFlight = res.addr
			}
		}
	}
	span.End()
	if firstFlight != "" {
		return firstFlight, true, true
	}
	return "", false, false
}

func probeOutcome(res probeResult) string {
	if res.err != nil {
		if errors.Is(res.err, context.DeadlineExceeded) {
			return "timeout"
		}
		if errors.Is(res.err, context.Canceled) {
			return "canceled"
		}
		return "transport_error"
	}
	switch res.status {
	case http.StatusOK:
		return "present"
	case http.StatusAccepted:
		return "in_flight"
	case http.StatusNotFound:
		return "negative"
	default:
		return "negative"
	}
}

// FetchFromPeer streams cacheKey's body from one peer into sink. flight=true
// tells the peer the key is expected from its in-flight fill: it waits
// (bounded) for the fill instead of 404ing. ok is false if the peer couldn't
// deliver the body — the caller then falls back to the origin.
func (pm *PeerManager) FetchFromPeer(ctx context.Context, holder, cacheKey string, flight bool, sink func(io.Reader) (int64, error)) (int64, bool) {
	ctx, span := proxyTracer.Start(ctx, "cache.peer_get", trace.WithAttributes(
		attribute.String("server.address", holder),
		attribute.Bool("duckgres.cache.peer_flight", flight),
	))
	defer span.End()

	getURL := fmt.Sprintf("http://%s/cache/get?key=%s", holder, cacheKey)
	if flight {
		getURL += "&flight=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, false
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := pm.streamClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
			_ = resp.Body.Close()
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Error, fmt.Sprintf("peer status %d", resp.StatusCode))
		}
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	n, err := sink(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, false
	}
	span.SetAttributes(attribute.Int64("duckgres.bytes", n))
	peerHitsTotal.Inc()
	return n, true
}

func getLocalIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var ips []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			ips = append(ips, ipnet.IP.String())
		}
	}
	return ips
}
