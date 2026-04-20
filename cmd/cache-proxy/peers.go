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
				IdleConnTimeout:    30 * time.Second,
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

// FetchFromPeers asks all peers in parallel if they have the given cache key.
// Returns the data from the first peer that has it, or false if none do.
func (pm *PeerManager) FetchFromPeers(cacheKey string) ([]byte, string, bool) {
	pm.mu.RLock()
	peers := make([]string, len(pm.peers))
	copy(peers, pm.peers)
	pm.mu.RUnlock()

	if len(peers) == 0 {
		return nil, "", false
	}

	peerFetchesTotal.Inc()

	type result struct {
		data []byte
		addr string
		ok   bool
	}

	// Ask all peers in parallel
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results := make(chan result, len(peers))

	for _, addr := range peers {
		go func(addr string) {
			// First check if peer has the key (cheap HEAD-like check)
			hasURL := fmt.Sprintf("http://%s/cache/has?key=%s", addr, cacheKey)
			req, err := http.NewRequestWithContext(ctx, "GET", hasURL, nil)
			if err != nil {
				results <- result{ok: false}
				return
			}
			resp, err := pm.client.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				if resp != nil {
					resp.Body.Close()
				}
				results <- result{ok: false}
				return
			}
			resp.Body.Close()

			// Peer has it — fetch the data
			getURL := fmt.Sprintf("http://%s/cache/get?key=%s", addr, cacheKey)
			req2, err := http.NewRequestWithContext(ctx, "GET", getURL, nil)
			if err != nil {
				results <- result{ok: false}
				return
			}
			resp2, err := pm.client.Do(req2)
			if err != nil || resp2.StatusCode != http.StatusOK {
				if resp2 != nil {
					resp2.Body.Close()
				}
				results <- result{ok: false}
				return
			}
			defer resp2.Body.Close()

			data, err := io.ReadAll(resp2.Body)
			if err != nil {
				results <- result{ok: false}
				return
			}

			results <- result{data: data, addr: addr, ok: true}
		}(addr)
	}

	// Return first successful result
	for range peers {
		r := <-results
		if r.ok {
			peerHitsTotal.Inc()
			cancel() // cancel remaining peer requests
			return r.data, r.addr, true
		}
	}

	return nil, "", false
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
