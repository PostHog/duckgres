package duckdbservice

import (
	"fmt"
	"net"
	"strings"

	"github.com/posthog/duckgres/server"
)

// ServiceConfig configures the standalone DuckDB Arrow Flight SQL service.
type ServiceConfig struct {
	// ListenAddr is the address to listen on.
	// Formats: "unix:///var/run/duckgres/duckdb.sock" or ":8816" or "0.0.0.0:8816"
	ListenAddr string

	// ListenFD is an inherited file descriptor for a pre-bound listener.
	// When > 0, the service uses this FD instead of creating a new socket.
	// This is used by the control plane to pass a pre-bound Unix socket to
	// worker processes, avoiding EROFS errors under ProtectSystem=strict.
	ListenFD int

	// ServerConfig is reused for CreateDBConnection (data_dir, extensions, DuckLake).
	ServerConfig server.Config

	// BearerToken is the authentication token for gRPC requests.
	// If empty, no authentication is required.
	BearerToken string

	// MaxSessions limits the number of concurrent sessions. 0 means unlimited.
	MaxSessions int
}

// ParseListenAddr parses ListenAddr into a network and address for net.Listen.
// Supports "unix:///path/to/sock" and TCP addresses like ":8816" or "host:port".
func ParseListenAddr(addr string) (network, listenAddr string, err error) {
	if strings.HasPrefix(addr, "unix://") {
		path := strings.TrimPrefix(addr, "unix://")
		if path == "" {
			return "", "", fmt.Errorf("unix socket path is empty")
		}
		return "unix", path, nil
	}

	// TCP address
	if addr == "" {
		return "", "", fmt.Errorf("listen address is empty")
	}

	// Validate it looks like a TCP address
	_, _, err = net.SplitHostPort(addr)
	if err != nil {
		return "", "", fmt.Errorf("invalid TCP address %q: %w", addr, err)
	}
	return "tcp", addr, nil
}
