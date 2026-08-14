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
	peerProbesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_peer_probes_total",
		Help: "Physical peer availability probe attempts, by outcome",
	}, []string{"outcome"}) // hit, miss, timeout, canceled, error
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
	addr   string
	status int // 200 has it · 202 mid-flight filling it · anything else: no
}

// LocateKey asks every peer in parallel whether it has cacheKey (200) or is
// mid-flight filling it (202), returning the first useful claim. A present
// entry wins over an in-flight one when both answer before the probes are
// cancelled; ok=false means nothing anywhere knows about the key and the
// caller should go to the origin. Cluster-wide this is what stops a cold key
// bursting into one duplicate origin fetch per node: the first node's
// in-flight fetch answers 202, so every other node waits for that fill.
func (pm *PeerManager) LocateKey(cacheKey string) (holder string, flight, ok bool) {
	pm.mu.RLock()
	peers := make([]string, len(pm.peers))
	copy(peers, pm.peers)
	pm.mu.RUnlock()

	if len(peers) == 0 {
		return "", false, false
	}

	peerFetchesTotal.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), peerHasTimeout)
	defer cancel()

	resCh := make(chan probeResult, len(peers))
	for _, addr := range peers {
		go func(addr string) {
			res := probeResult{addr: addr, status: -1}
			hasURL := fmt.Sprintf("http://%s/cache/has?key=%s", addr, cacheKey)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, hasURL, nil)
			if err != nil {
				peerProbesTotal.WithLabelValues("error").Inc()
				resCh <- res
				return
			}
			resp, err := pm.client.Do(req)
			if err != nil {
				peerProbesTotal.WithLabelValues(peerProbeErrorOutcome(err)).Inc()
				resCh <- res
				return
			}
			res.status = resp.StatusCode
			_ = resp.Body.Close()
			peerProbesTotal.WithLabelValues(peerProbeStatusOutcome(resp.StatusCode)).Inc()
			resCh <- res
		}(addr)
	}

	firstFlight := ""
	for range peers {
		res := <-resCh
		switch res.status {
		case http.StatusOK:
			cancel() // release the losing probes
			return res.addr, false, true
		case http.StatusAccepted:
			if firstFlight == "" {
				firstFlight = res.addr
			}
		}
	}
	if firstFlight != "" {
		return firstFlight, true, true
	}
	return "", false, false
}

func peerProbeStatusOutcome(status int) string {
	switch status {
	case http.StatusOK, http.StatusAccepted:
		return "hit"
	case http.StatusNotFound:
		return "miss"
	default:
		return "error"
	}
}

func peerProbeErrorOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return "error"
	}
}

// FetchFromPeer streams cacheKey's body from one peer into sink. flight=true
// tells the peer the key is expected from its in-flight fill: it waits
// (bounded) for the fill instead of 404ing. ok is false if the peer couldn't
// deliver the body — the caller then falls back to the origin.
func (pm *PeerManager) FetchFromPeer(holder, cacheKey string, flight bool, sink func(io.Reader) (int64, error)) (int64, bool) {
	getURL := fmt.Sprintf("http://%s/cache/get?key=%s", holder, cacheKey)
	if flight {
		getURL += "&flight=1"
	}
	req, err := http.NewRequest(http.MethodGet, getURL, nil)
	if err != nil {
		return 0, false
	}
	resp, err := pm.streamClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	n, err := sink(resp.Body)
	if err != nil {
		return 0, false
	}
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
