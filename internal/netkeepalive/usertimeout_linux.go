//go:build linux

package netkeepalive

import (
	"net"
	"syscall"
	"time"
)

// tcpUserTimeout is the Linux setsockopt level=IPPROTO_TCP option for
// TCP_USER_TIMEOUT. Defined in <netinet/tcp.h> as 18; not exposed by Go's
// stdlib syscall package, so we declare the constant locally rather than
// pull in golang.org/x/sys/unix as a direct dependency for one integer.
const tcpUserTimeout = 18

func setUserTimeout(conn *net.TCPConn, d time.Duration) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	ms := int(d / time.Millisecond)
	var setErr error
	ctrlErr := raw.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout, ms)
	})
	if ctrlErr != nil {
		return ctrlErr
	}
	return setErr
}
