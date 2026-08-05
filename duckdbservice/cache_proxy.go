package duckdbservice

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/posthog/duckgres/server"
)

// Cache proxy integration is controlled by a single env var:
//
//	DUCKGRES_CACHE_ENABLED=true
//
// When enabled, duckgres starts a stable worker-local router. The router uses
// the node-local DaemonSet when healthy and directly fetches from the signed
// authoritative S3 source when it is not, so cache availability never gates
// workers or PostgreSQL sessions.
//
// The proxy is reached via the node IP + fixed hostPorts. The NODE_IP env
// var is injected into worker pods via the Kubernetes Downward API; control
// plane pods use the same variable (set to the node they run on).
const (
	// Fixed hostPorts on the cache proxy DaemonSet. Must match the DaemonSet spec.
	cacheProxyHealthPort = "8082"
	cacheProxyS3Port     = "8080"

	// NODE_IP is populated via fieldRef: status.hostIP on each pod.
	nodeIPEnvVar = "NODE_IP"

	// defaultCacheProxyConnectTimeout bounds worker startup when the optional
	// node-local cache daemon is unavailable.
	defaultCacheProxyConnectTimeout = 5 * time.Second
)

// cacheEnabled returns true when the cache proxy integration should be used.
func cacheEnabled() bool {
	return os.Getenv("DUCKGRES_CACHE_ENABLED") == "true"
}

// cacheProxyHealthURL returns the health endpoint URL (empty if disabled).
func cacheProxyHealthURL() string {
	if !cacheEnabled() {
		return ""
	}
	nodeIP := os.Getenv(nodeIPEnvVar)
	if nodeIP == "" {
		nodeIP = "localhost"
	}
	return "http://" + nodeIP + ":" + cacheProxyHealthPort + "/health"
}

// cacheProxyS3Addr returns the S3 endpoint to use for DuckDB (empty if disabled).
func cacheProxyS3Addr() string {
	if !cacheEnabled() {
		return ""
	}
	nodeIP := os.Getenv(nodeIPEnvVar)
	if nodeIP == "" {
		nodeIP = "localhost"
	}
	return nodeIP + ":" + cacheProxyS3Port
}

// LogCacheProxyStatus logs whether the cache proxy integration is enabled.
// Called once from main on every duckgres process (control plane and workers)
// so the startup logs clearly show the cache state.
func LogCacheProxyStatus() {
	if !cacheEnabled() {
		slog.Info("Cache proxy integration disabled (DUCKGRES_CACHE_ENABLED not 'true').")
		return
	}
	slog.Info("Cache proxy integration enabled.",
		"node_ip", os.Getenv(nodeIPEnvVar),
		"health_url", cacheProxyHealthURL(),
		"s3_addr", cacheProxyS3Addr(),
	)
}

// overrideS3EndpointForCacheProxy routes DuckLake S3 traffic through the local
// cache proxy as a forward HTTP proxy. The proxy runs as a DaemonSet on worker
// nodes and caches S3 responses to local NVMe.
//
// The request keeps DuckDB's SigV4 signature for the real S3 hostname intact —
// the proxy just forwards the signed request verbatim. This requires plain HTTP
// (no TLS tunnel) so the proxy can see the URL to cache by. The proxy itself
// needs zero AWS credentials.
//
// DuckDB only honors USE_SSL=false when an explicit ENDPOINT is set on the S3
// secret (otherwise it defaults to HTTPS regardless), so we pin the real S3
// endpoint for the configured region.
func overrideS3EndpointForCacheProxy(cfg *server.DuckLakeConfig) {
	overrideS3EndpointForCacheProxyAddr(cfg, cacheProxyS3Addr())
}

func overrideS3EndpointForCacheProxyAddr(cfg *server.DuckLakeConfig, addr string) {
	if addr == "" {
		return
	}
	proxyURL := "http://" + addr
	if cfg.S3Endpoint == "" {
		region := cfg.S3Region
		if region == "" {
			region = "us-east-1"
		}
		cfg.S3Endpoint = "s3." + region + ".amazonaws.com"
	}
	cfg.HTTPProxy = proxyURL
	cfg.S3UseSSL = false
}

// cacheProxyConnectTimeout is deliberately configurable because a node may
// need a little time to mount NVMe after a restart. It is always bounded: the
// cache is an optimization, not a worker-startup dependency.
func cacheProxyConnectTimeout() time.Duration {
	value := os.Getenv("DUCKGRES_CACHE_PROXY_CONNECT_TIMEOUT")
	if value == "" {
		return defaultCacheProxyConnectTimeout
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		slog.Warn("Invalid DUCKGRES_CACHE_PROXY_CONNECT_TIMEOUT; using default.",
			"value", value, "default", defaultCacheProxyConnectTimeout, "error", err)
		return defaultCacheProxyConnectTimeout
	}
	return timeout
}

// waitForCacheProxy performs one bounded startup health check. It never gates
// readiness: a failure puts the worker-local router into direct-source bypass
// mode, where it fetches from the authoritative S3 path.
func waitForCacheProxy() cacheProxyMode {
	url := cacheProxyHealthURL()
	if url == "" {
		return cacheProxyModeDisabled
	}

	timeout := cacheProxyConnectTimeout()
	client := &http.Client{Timeout: timeout}
	start := time.Now()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err == nil {
		resp, requestErr := client.Do(req)
		if requestErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				slog.Info("Cache proxy is ready.", "wait_duration", time.Since(start))
				return cacheProxyModeCached
			}
			err = fmt.Errorf("health endpoint returned status %d", resp.StatusCode)
		} else {
			err = requestErr
		}
	}
	cacheProxyBypassTransitionsTotal.WithLabelValues(cacheProxyBypassReasonStartupUnavailable).Inc()
	slog.Warn("Cache proxy unavailable at worker startup; bypassing local NVMe cache.",
		"url", url, "timeout", timeout, "wait_duration", time.Since(start), "error", err)
	return cacheProxyModeBypassed
}
