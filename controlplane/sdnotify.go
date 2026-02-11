package controlplane

import (
	"fmt"
	"net"
	"os"
)

// sdNotify sends a notification to systemd via the NOTIFY_SOCKET.
// If NOTIFY_SOCKET is not set (i.e. not running under systemd), this is a no-op.
func sdNotify(state string) error {
	socketPath := os.Getenv("NOTIFY_SOCKET")
	if socketPath == "" {
		return nil
	}

	conn, err := net.Dial("unixgram", socketPath)
	if err != nil {
		return fmt.Errorf("dial NOTIFY_SOCKET: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte(state)); err != nil {
		return fmt.Errorf("write to NOTIFY_SOCKET: %w", err)
	}
	return nil
}
