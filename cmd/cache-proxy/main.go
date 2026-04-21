// cache-proxy is a caching S3 proxy for DuckDB workloads.
//
// It acts as an S3-compatible endpoint that caches HTTP range request
// responses to local NVMe storage. Worker nodes discover each other
// via a Kubernetes headless Service and share cached data over VPC.
//
// Request flow:
//  1. DuckDB sends S3 GET with Range header to localhost:8080
//  2. Proxy checks local NVMe cache → hit? serve
//  3. Cache miss → ask all peers in parallel → peer hit? serve + cache locally
//  4. All miss → fetch from real S3, cache locally, serve
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cacheDir := envOrDefault("CACHE_DIR", "/cache")
	maxPercent, _ := strconv.Atoi(envOrDefault("CACHE_MAX_PERCENT", "80"))
	listenAddr := envOrDefault("LISTEN_ADDR", ":8080")
	peerAddr := envOrDefault("PEER_ADDR", ":8081")
	healthAddr := envOrDefault("HEALTH_ADDR", ":8082")
	peerService := os.Getenv("PEER_SERVICE") // headless K8s service for peer discovery

	slog.Info("Starting cache-proxy.",
		"cache_dir", cacheDir,
		"max_percent", maxPercent,
		"listen", listenAddr,
		"peer_listen", peerAddr,
		"health", healthAddr,
		"peer_service", peerService,
	)

	// Initialize cache store
	store, err := NewDiskCache(cacheDir, maxPercent)
	if err != nil {
		slog.Error("Failed to initialize cache store.", "error", err)
		os.Exit(1)
	}

	// Initialize peer manager
	var peers *PeerManager
	if peerService != "" {
		peers = NewPeerManager(peerService, peerAddr)
		go peers.WatchEndpoints(context.Background())
	}

	proxy := NewCacheProxy(store, peers)

	// Forward HTTP proxy (DuckDB httpfs traffic). ServeMux can't match absolute
	// URLs in forward-proxy requests, so use the handler directly.
	s3Server := &http.Server{Addr: listenAddr, Handler: http.HandlerFunc(proxy.HandleProxy)}

	// Peer API (cache lookups from other nodes)
	peerMux := http.NewServeMux()
	peerMux.HandleFunc("/cache/has", proxy.HandlePeerHas)
	peerMux.HandleFunc("/cache/get", proxy.HandlePeerGet)
	peerServer := &http.Server{Addr: peerAddr, Handler: peerMux}

	// Health + metrics
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "OK")
	})
	healthMux.Handle("/metrics", promhttp.Handler())
	healthServer := &http.Server{Addr: healthAddr, Handler: healthMux}

	// Start servers
	go func() {
		slog.Info("S3 proxy listening.", "addr", listenAddr)
		if err := s3Server.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("S3 proxy error.", "error", err)
		}
	}()
	go func() {
		slog.Info("Peer API listening.", "addr", peerAddr)
		if err := peerServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("Peer API error.", "error", err)
		}
	}()
	go func() {
		slog.Info("Health/metrics listening.", "addr", healthAddr)
		if err := healthServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("Health server error.", "error", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	slog.Info("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s3Server.Shutdown(ctx)
	_ = peerServer.Shutdown(ctx)
	_ = healthServer.Shutdown(ctx)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
