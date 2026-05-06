//go:build !linux

package netkeepalive

import (
	"net"
	"time"
)

// setUserTimeout is a no-op outside Linux. macOS / BSD kernels do not expose
// TCP_USER_TIMEOUT; the keepalive budget remains the only detection path on
// developer machines, which is fine — production runs on Linux.
func setUserTimeout(_ *net.TCPConn, _ time.Duration) error {
	return nil
}
