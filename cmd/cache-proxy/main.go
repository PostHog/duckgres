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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
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

	// OTLP tracing (no-op unless an OTEL/DUCKGRES trace endpoint is set).
	// Emits standalone cache-proxy traces; flushed on shutdown.
	shutdownTracing := initTracing()
	defer shutdownTracing()

	cacheDir := envOrDefault("CACHE_DIR", "/cache")
	maxPercent, _ := strconv.Atoi(envOrDefault("CACHE_MAX_PERCENT", "80"))
	maxEntries, err := positiveEnvInt("CACHE_MAX_ENTRIES", defaultCacheMaxEntries)
	if err != nil {
		slog.Error("Invalid cache entry limit.", "error", err)
		return
	}
	summaryMemoryLimit := envInt64("CACHE_SUMMARY_MEMORY_LIMIT_BYTES", defaultSummaryMemoryLimitBytes)
	peerMaxProbes, usedDeprecatedPeerMaxProbes, err := positiveEnvIntWithDeprecatedAlias("CACHE_PEER_MAX_PROBES_PER_REQUEST", "CACHE_PEER_MAX_PROBES", defaultPeerMaxProbes)
	if err != nil {
		slog.Error("Invalid per-request peer probe limit.", "error", err)
		return
	}
	if usedDeprecatedPeerMaxProbes {
		slog.Warn("Deprecated cache-proxy setting in use; rename it before the next release.", "deprecated", "CACHE_PEER_MAX_PROBES", "replacement", "CACHE_PEER_MAX_PROBES_PER_REQUEST")
	}
	maxPeerProbesInFlight, usedDeprecatedMaxPeerProbesInFlight, err := positiveEnvIntWithDeprecatedAlias("CACHE_MAX_CONCURRENT_PEER_PROBES", "CACHE_MAX_PEER_PROBES_IN_FLIGHT", defaultMaxPeerProbesInFlight)
	if err != nil {
		slog.Error("Invalid concurrent peer probe limit.", "error", err)
		return
	}
	if usedDeprecatedMaxPeerProbesInFlight {
		slog.Warn("Deprecated cache-proxy setting in use; rename it before the next release.", "deprecated", "CACHE_MAX_PEER_PROBES_IN_FLIGHT", "replacement", "CACHE_MAX_CONCURRENT_PEER_PROBES")
	}
	listenAddr := envOrDefault("LISTEN_ADDR", ":8080")
	peerAddr := envOrDefault("PEER_ADDR", ":8081")
	healthAddr := envOrDefault("HEALTH_ADDR", ":8082")
	peerService := os.Getenv("PEER_SERVICE") // headless K8s service for peer discovery
	lookupMode, err := parsePeerLookupMode(os.Getenv("CACHE_PEER_LOOKUP_MODE"))
	if err != nil {
		slog.Error("Invalid cache peer lookup mode.", "error", err)
		return
	}
	if lookupMode == peerLookupSummary {
		summaryMemoryLimitBytes.Set(float64(summaryMemoryLimit))
		if err := validateSummaryMemoryLimit(summaryMemoryLimit); err != nil {
			slog.Error("Invalid summary memory limit.", "error", err)
			return
		}
	} else {
		summaryMemoryLimitBytes.Set(0)
	}
	hostname, _ := os.Hostname()
	identity := envOrDefault("CACHE_PROXY_ID", envOrDefault("POD_NAME", envOrDefault("NODE_NAME", hostname)))
	if identity == "" {
		identity = peerAddr
	}

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
		"max_entries", maxEntries,
		"summary_memory_limit", summaryMemoryLimit,
		"peer_max_probes", peerMaxProbes,
		"max_peer_probes_in_flight", maxPeerProbesInFlight,
		"listen", listenAddr,
		"peer_listen", peerAddr,
		"health", healthAddr,
		"peer_service", peerService,
		"peer_lookup_mode", lookupMode,
		"cache_host_suffixes", cacheHostSuffixes,
	)

	// Block-aligned cache mode: fixed-size, content-addressed blocks instead of
	// exact-range keys, so repeat reads over drifting ranges of the same object
	// still hit cache. See README.md "Block-aligned mode".
	blockMode := os.Getenv("CACHE_BLOCK_MODE") == "on"
	blockSize := envInt64("CACHE_BLOCK_SIZE_BYTES", 8<<20)
	maxSpanBlocks := envInt64("CACHE_BLOCK_MAX_SPAN_BLOCKS", 8)
	slog.Info("Block mode configured.", "enabled", blockMode, "block_size", blockSize, "max_span_blocks", maxSpanBlocks)

	// Install cancellation before the potentially long bounded startup scan so
	// Kubernetes termination never has to wait for enumeration or hard pruning.
	rootCtx, stopBackground := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopBackground()

	// Initialize cache store
	store, err := NewDiskCache(cacheDir, maxPercent, DiskCacheOptions{
		IncrementalSummary:    lookupMode == peerLookupSummary && peerService != "",
		DurableRecency:        true,
		BackgroundConvergence: true,
		MaxEntries:            maxEntries,
		BlockSizeBytes:        blockSize,
		startupContext:        rootCtx,
	})
	if err != nil {
		slog.Error("Failed to initialize cache store.", "error", err)
		if errors.Is(err, context.Canceled) {
			return
		}
		os.Exit(1)
	}
	// Track the disk's free space: when something outside the cache consumes
	// it, the cache's budget shrinks instead of evicting healthy entries for
	// room it never actually had and then ENOSPC-ing the fill.
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				store.refreshCapacity(maxPercent)
			}
		}
	}()

	// Initialize peer manager
	var peers *PeerManager
	if peerService != "" {
		peers = NewPeerManager(peerService, peerAddr)
		peers.ConfigureSummary(lookupMode, identity, SummaryConfig{
			PeerMaxProbes:         peerMaxProbes,
			MaxPeerProbesInFlight: maxPeerProbesInFlight,
			MemoryLimitBytes:      summaryMemoryLimit,
		})
		peers.StartSummarySynchronizer(rootCtx, store)
		go peers.WatchEndpoints(rootCtx)
	}

	proxy := NewCacheProxy(store, peers, cacheHostSuffixes)
	proxy.blockMode = blockMode
	proxy.blockSize = blockSize
	proxy.maxSpanBlocks = maxSpanBlocks

	// Forward HTTP proxy (DuckDB httpfs traffic). ServeMux can't match absolute
	// URLs in forward-proxy requests, so use the handler directly.
	s3Server := &http.Server{Addr: listenAddr, Handler: http.HandlerFunc(proxy.HandleProxy)}

	// Peer API (cache lookups from other nodes)
	peerMux := http.NewServeMux()
	peerMux.HandleFunc("/cache/has", proxy.HandlePeerHas)
	peerMux.HandleFunc("/cache/get", proxy.HandlePeerGet)
	peerMux.HandleFunc("/cache/summary", proxy.HandlePeerSummary)
	peerServer := &http.Server{Addr: peerAddr, Handler: peerMux}

	// Health + metrics
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "OK")
	})
	healthMux.Handle("/metrics", promhttp.Handler())
	// net/http/pprof's init() only registers on http.DefaultServeMux; since
	// this server uses its own mux, the handlers must be added explicitly.
	healthMux.HandleFunc("/debug/pprof/", pprof.Index)
	healthMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	healthMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	healthMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	healthMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
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

	// Wait for shutdown signal.
	<-rootCtx.Done()

	slog.Info("Shutting down...")
	stopBackground()
	if peers != nil {
		peers.StopSummarySynchronizer()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s3Server.Shutdown(ctx)
	_ = peerServer.Shutdown(ctx)
	_ = healthServer.Shutdown(ctx)
	if err := store.Close(ctx); err != nil {
		slog.Warn("Cache background work did not fully drain before shutdown.", "error", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func positiveEnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer: %w", key, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return n, nil
}

// positiveEnvIntWithDeprecatedAlias reads canonicalKey first and falls back to
// deprecatedKey only while the old setting remains supported. The canonical
// setting always wins when both are supplied.
func positiveEnvIntWithDeprecatedAlias(canonicalKey, deprecatedKey string, def int) (value int, usedDeprecated bool, err error) {
	if os.Getenv(canonicalKey) != "" {
		value, err = positiveEnvInt(canonicalKey, def)
		return value, false, err
	}
	if os.Getenv(deprecatedKey) != "" {
		value, err = positiveEnvInt(deprecatedKey, def)
		return value, true, err
	}
	return def, false, nil
}

// envInt64 parses an integer env var, falling back to def (with a warning)
// when the variable is unset or fails to parse.
func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		slog.Warn("Invalid integer env var; using default.", "key", key, "value", v, "default", def, "error", err)
		return def
	}
	return n
}
