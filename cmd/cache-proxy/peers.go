package main

import (
	"context"
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
)

// PeerManager discovers and communicates with cache proxy peers
// via a Kubernetes headless Service.
type PeerManager struct {
	serviceName string
	peerPort    string // port for peer API (e.g. ":8081")
	client      *http.Client

	mu    sync.RWMutex
	peers []string // peer addresses (ip:port)
}

func NewPeerManager(serviceName, peerPort string) *PeerManager {
	return &PeerManager{
		serviceName: serviceName,
		peerPort:    peerPort,
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     30 * time.Second,
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

// FetchFromPeers locates a peer holding cacheKey and streams its body into
// sink, returning the serving peer's address and the number of bytes streamed.
// ok is false if no peer has the key (or the chosen peer's body couldn't be
// streamed). The body is never buffered here — sink (typically DiskCache.PutStream)
// consumes it as it arrives, so a peer hit costs only sink's copy buffer.
func (pm *PeerManager) FetchFromPeers(cacheKey string, sink func(io.Reader) (int64, error)) (string, int64, bool) {
	pm.mu.RLock()
	peers := make([]string, len(pm.peers))
	copy(peers, pm.peers)
	pm.mu.RUnlock()

	if len(peers) == 0 {
		return "", 0, false
	}

	peerFetchesTotal.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Phase 1: ask every peer "do you have this?" in parallel (cheap, no body)
	// and take the first that says yes.
	holderCh := make(chan string, len(peers))
	for _, addr := range peers {
		go func(addr string) {
			hasURL := fmt.Sprintf("http://%s/cache/has?key=%s", addr, cacheKey)
			req, err := http.NewRequestWithContext(ctx, "GET", hasURL, nil)
			if err != nil {
				holderCh <- ""
				return
			}
			resp, err := pm.client.Do(req)
			if err != nil {
				holderCh <- ""
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				holderCh <- addr
			} else {
				holderCh <- ""
			}
		}(addr)
	}

	var holder string
	for range peers {
		if a := <-holderCh; a != "" {
			holder = a
			break
		}
	}
	if holder == "" {
		return "", 0, false
	}

	// Phase 2: stream the body from the chosen holder straight into sink. Only
	// the winner is fetched, so we never pull a body we won't use.
	getURL := fmt.Sprintf("http://%s/cache/get?key=%s", holder, cacheKey)
	req, err := http.NewRequestWithContext(ctx, "GET", getURL, nil)
	if err != nil {
		return "", 0, false
	}
	resp, err := pm.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return "", 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	n, err := sink(resp.Body)
	if err != nil {
		return "", 0, false
	}
	peerHitsTotal.Inc()
	return holder, n, true
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
