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
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// stampedHandler emits log records in the order: time, level, pod, node,
// msg, attrs. Go's built-in TextHandler forces time/level/msg to the front
// and appends all other attrs after msg, which hides pod/node at the end
// of long lines. Having pod/node up front makes log triage scannable.
type stampedHandler struct {
	out   io.Writer
	level slog.Level
	stamp []slog.Attr
}

func (h *stampedHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

func (h *stampedHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	fmt.Fprintf(&b, "time=%s level=%s", r.Time.UTC().Format(time.RFC3339Nano), r.Level.String())
	for _, a := range h.stamp {
		fmt.Fprintf(&b, " %s=%s", a.Key, a.Value.String())
	}
	fmt.Fprintf(&b, " msg=%q", r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%s", a.Key, a.Value.String())
		return true
	})
	b.WriteByte('\n')
	_, err := io.WriteString(h.out, b.String())
	return err
}

func (h *stampedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.stamp = append(append([]slog.Attr{}, h.stamp...), attrs...)
	return &nh
}

func (h *stampedHandler) WithGroup(_ string) slog.Handler { return h }

// Build metadata, set at link time via -ldflags (see Dockerfile).
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	// Attach pod/node identifiers to every log line (Downward API) so they
	// appear right after level for quick triage.
	var stamp []slog.Attr
	if pod := os.Getenv("POD_NAME"); pod != "" {
		stamp = append(stamp, slog.String("pod", pod))
	}
	if node := os.Getenv("NODE_NAME"); node != "" {
		stamp = append(stamp, slog.String("node", node))
	}
	slog.SetDefault(slog.New(&stampedHandler{out: os.Stderr, level: slog.LevelInfo, stamp: stamp}))

	slog.Info("Cache-proxy build info.", "version", version, "commit", commit, "built", date)

	cacheDir := envOrDefault("CACHE_DIR", "/cache")
	maxPercent, _ := strconv.Atoi(envOrDefault("CACHE_MAX_PERCENT", "80"))
	listenAddr := envOrDefault("LISTEN_ADDR", ":8080")
	peerAddr := envOrDefault("PEER_ADDR", ":8081")
	healthAddr := envOrDefault("HEALTH_ADDR", ":8082")
	peerService := os.Getenv("PEER_SERVICE") // headless K8s service for peer discovery

	// Comma-separated Host substrings we should cache. Anything else is tunneled
	// or forwarded without caching. Empty means "cache everything" (legacy).
	var cacheHostSuffixes []string
	if raw := os.Getenv("CACHE_HOST_SUFFIXES"); raw != "" {
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s != "" {
				cacheHostSuffixes = append(cacheHostSuffixes, s)
			}
		}
	}

	slog.Info("Starting cache-proxy.",
		"cache_dir", cacheDir,
		"max_percent", maxPercent,
		"listen", listenAddr,
		"peer_listen", peerAddr,
		"health", healthAddr,
		"peer_service", peerService,
		"cache_host_suffixes", cacheHostSuffixes,
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

	proxy := NewCacheProxy(store, peers, cacheHostSuffixes)

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
		_, _ = fmt.Fprint(w, "OK")
	})
	healthMux.Handle("/metrics", promhttp.Handler())
	healthServer := &http.Server{Addr: healthAddr, Handler: healthMux}

	// Start servers
	go func() {
		slog.Info("Forward HTTP proxy listening.", "addr", listenAddr)
		if err := s3Server.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("Forward HTTP proxy error.", "error", err)
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
