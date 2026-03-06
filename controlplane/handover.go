package controlplane

import (
	"fmt"
	"net"
	"os"
)

// checkSocketDirWritable verifies that the socket directory is writable by
// creating and removing both a temporary file and a Unix socket. This catches
// cases where ProtectSystem=strict's ReadWritePaths bind mount has been lost
// or remounted RO (e.g., after a handover race with systemd's RuntimeDirectory cleanup).
func checkSocketDirWritable(dir string) error {
	// Test regular file creation
	f, err := os.CreateTemp(dir, ".writable-check-*")
	if err != nil {
		return fmt.Errorf("cannot write to socket directory %s: %w", dir, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)

	// Test Unix socket binding specifically (catches different EROFS/EPERM cases)
	socketPath := fmt.Sprintf("%s/.socket-check-%d.sock", dir, os.Getpid())
	_ = os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("cannot bind unix socket in %s: %w", dir, err)
	}
	_ = l.Close()
	_ = os.Remove(socketPath)

	return nil
}
