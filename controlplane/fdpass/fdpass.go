// Package fdpass provides Unix socket file descriptor passing via SCM_RIGHTS.
//
// This is used by the control plane to pass raw TCP file descriptors to worker
// processes at runtime, enabling workers to handle client connections that were
// accepted by the control plane.
package fdpass

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

// SendFD sends a file descriptor over a Unix socket using SCM_RIGHTS.
// The fd is the file descriptor to pass; conn is the Unix socket to send it over.
func SendFD(conn *net.UnixConn, fd int) error {
	rights := syscall.UnixRights(fd)
	// Send a single byte as the message body (required by sendmsg)
	_, _, err := conn.WriteMsgUnix([]byte{0}, rights, nil)
	if err != nil {
		return fmt.Errorf("sendmsg: %w", err)
	}
	return nil
}

// SendFile sends an *os.File's descriptor over a Unix socket using SCM_RIGHTS.
func SendFile(conn *net.UnixConn, f *os.File) error {
	return SendFD(conn, int(f.Fd()))
}

// RecvFD receives a file descriptor from a Unix socket using SCM_RIGHTS.
// Returns the received file descriptor. The caller is responsible for closing it.
func RecvFD(conn *net.UnixConn) (int, error) {
	buf := make([]byte, 1)
	oob := make([]byte, syscall.CmsgLen(4)) // space for one int32 fd
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, fmt.Errorf("recvmsg: %w", err)
	}

	cmsgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, fmt.Errorf("parse control message: %w", err)
	}

	for _, cmsg := range cmsgs {
		fds, err := syscall.ParseUnixRights(&cmsg)
		if err != nil {
			continue
		}
		if len(fds) > 0 {
			return fds[0], nil
		}
	}

	return -1, fmt.Errorf("no file descriptor received")
}

// RecvFile receives a file descriptor from a Unix socket and wraps it as *os.File.
// The caller is responsible for closing the returned file.
func RecvFile(conn *net.UnixConn, name string) (*os.File, error) {
	fd, err := RecvFD(conn)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// SocketPair creates a connected pair of Unix sockets for FD passing.
// Returns (sender, receiver) connections. Both must be closed by the caller.
func SocketPair() (*net.UnixConn, *net.UnixConn, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("socketpair: %w", err)
	}

	sender, err := fdToUnixConn(fds[0], "sender")
	if err != nil {
		_ = syscall.Close(fds[0])
		_ = syscall.Close(fds[1])
		return nil, nil, err
	}

	receiver, err := fdToUnixConn(fds[1], "receiver")
	if err != nil {
		_ = sender.Close()
		_ = syscall.Close(fds[1])
		return nil, nil, err
	}

	return sender, receiver, nil
}

func fdToUnixConn(fd int, name string) (*net.UnixConn, error) {
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		return nil, fmt.Errorf("invalid fd %d", fd)
	}
	defer func() { _ = f.Close() }()

	fc, err := net.FileConn(f)
	if err != nil {
		return nil, fmt.Errorf("FileConn: %w", err)
	}

	uc, ok := fc.(*net.UnixConn)
	if !ok {
		_ = fc.Close()
		return nil, fmt.Errorf("not a UnixConn")
	}
	return uc, nil
}
