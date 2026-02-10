package controlplane

import (
	"net"
	"sync"
)

// SingleConnListener adapts a single accepted connection into a net.Listener.
type SingleConnListener struct {
	mu       sync.Mutex
	conn     net.Conn
	accepted bool
	closed   bool
	done     chan struct{}
	doneOnce sync.Once
}

func NewSingleConnListener(conn net.Conn) *SingleConnListener {
	return &SingleConnListener{
		conn: conn,
		done: make(chan struct{}),
	}
}

func (l *SingleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()

	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}

	if !l.accepted && l.conn != nil {
		l.accepted = true
		conn := &singleConn{
			Conn: l.conn,
			onClose: func() {
				l.markDone()
			},
		}
		l.mu.Unlock()
		return conn, nil
	}

	done := l.done
	l.mu.Unlock()

	<-done
	return nil, net.ErrClosed
}

func (l *SingleConnListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	l.markDone()
	return nil
}

func (l *SingleConnListener) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return &net.TCPAddr{}
	}
	return l.conn.LocalAddr()
}

func (l *SingleConnListener) markDone() {
	l.doneOnce.Do(func() {
		close(l.done)
	})
}

type singleConn struct {
	net.Conn
	onClose func()
	once    sync.Once
}

func (c *singleConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}
