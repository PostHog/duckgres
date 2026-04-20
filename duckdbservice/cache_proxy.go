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

// overrideS3EndpointForCacheProxy rewrites the DuckLake S3 configuration to
// route traffic through the local cache proxy. The proxy runs as a DaemonSet
// on worker nodes and caches S3 responses to local NVMe.
//
// The proxy handles authentication itself via AWS credentials on its Pod
// Identity, so we use path-style URLs and disable SSL on the local hop.
func overrideS3EndpointForCacheProxy(cfg *server.DuckLakeConfig) {
	addr := cacheProxyS3Addr()
	if addr == "" {
		return
	}
	slog.Info("Routing S3 traffic through local cache proxy.",
		"from_endpoint", cfg.S3Endpoint,
		"from_use_ssl", cfg.S3UseSSL,
		"to_endpoint", addr,
	)
	cfg.S3Endpoint = addr
	cfg.S3UseSSL = false
	cfg.S3URLStyle = "path"
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
