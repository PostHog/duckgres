package duckdbservice

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/posthog/duckgres/server"
)

// Cache proxy integration is controlled by a single env var:
//
//   DUCKGRES_CACHE_ENABLED=true
//
// When enabled, duckgres assumes a cache proxy DaemonSet is running on the
// same node and:
//   - waits for its health endpoint before serving queries (prevents early
//     S3 traffic from bypassing the cache)
//   - routes DuckLake S3 requests through it (S3 endpoint override)
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
func overrideS3EndpointForCacheProxy(cfg *server.DuckLakeConfig) {
	addr := cacheProxyS3Addr()
	if addr == "" {
		return
	}
	proxyURL := "http://" + addr
	slog.Info("Routing S3 traffic through local cache proxy.",
		"from_use_ssl", cfg.S3UseSSL,
		"http_proxy", proxyURL,
	)
	cfg.HTTPProxy = proxyURL
	cfg.S3UseSSL = false
}

// waitForCacheProxy blocks until the local cache proxy responds healthy.
// When the cache is disabled, returns immediately (no-op).
//
// If the worker starts before the proxy is ready, DuckDB's first S3 requests
// would fail. Block startup until the proxy is up.
func waitForCacheProxy() {
	url := cacheProxyHealthURL()
	if url == "" {
		return
	}

	client := &http.Client{Timeout: 2 * time.Second}

	start := time.Now()
	slog.Info("Waiting for cache proxy to be ready.", "url", url)
	for {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				slog.Info("Cache proxy is ready.", "wait_duration", time.Since(start))
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
}
