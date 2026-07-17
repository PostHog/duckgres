// Package netkeepalive tunes accepted TCP connections so dead peers and
// stalled writes are surfaced quickly.
//
// The defaults net.TCPConn ships with — and even SetKeepAlivePeriod alone —
// are not enough behind an AWS NLB. The NLB's idle timeout is 350s, and a
// stuck downstream (e.g. a pod whose host kernel stopped acking) can leave a
// CP write blocked on TCP retransmits for ~15 minutes before the connection
// finally errors. That window swallows the symptom: the CP keeps the query
// "open" with no error log, the pgwire client has long since given up, and
// the operator only sees a Client query finished. line at Info severity once the
// kernel finally collapses the socket.
//
// TuneAcceptedConn does two things to bound the window to ~60s:
//   - Tighter SO_KEEPALIVE: probe every 10s after 30s idle, give up after 3
//     missed acks. ~60s detection on read-idle sockets.
//   - TCP_USER_TIMEOUT (Linux only): if data is sitting unacked in the send
//     buffer for 60s, the kernel marks the socket dead instead of waiting on
//     the default ~15min retransmit ladder. Critical for the NLB-stall case
//     because a hung write does not benefit from KEEPALIVE — keepalive only
//     fires on idle sockets.
package netkeepalive

import (
	"net"
	"time"
)

// KeepAliveConfig parameters tuned for ~60s detection of dead peers behind
// an AWS NLB (350s idle timeout). Probes start after 30s idle, repeat every
// 10s, and after 3 missed acks the kernel marks the connection broken.
const (
	keepAliveIdle     = 30 * time.Second
	keepAliveInterval = 10 * time.Second
	keepAliveCount    = 3

	// userTimeout bounds the time a write can sit unacked before the kernel
	// declares the socket dead. Matches the keepalive budget so the two
	// detection paths agree on a ~60s ceiling.
	userTimeout = 60 * time.Second
)

// TuneAcceptedConn applies keepalive + user-timeout settings to a freshly
// accepted connection. Non-TCP connections and best-effort syscall failures
// are silently ignored — a misconfigured socket should not block the listener
// from serving traffic.
func TuneAcceptedConn(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     keepAliveIdle,
		Interval: keepAliveInterval,
		Count:    keepAliveCount,
	})
	_ = setUserTimeout(tcpConn, userTimeout)
}
