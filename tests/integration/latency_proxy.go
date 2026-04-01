package integration

import (
	"io"
	"net"
	"sync"
	"time"
)

// LatencyProxy is a TCP proxy that adds configurable bidirectional latency
// between a client (DuckDB/DuckLake) and a target (metadata PostgreSQL).
// Each direction gets the configured delay, so total RTT overhead = 2 * Latency.
type LatencyProxy struct {
	listener net.Listener
	target   string
	latency  time.Duration

	mu     sync.Mutex
	closed bool
	conns  []net.Conn
}

// NewLatencyProxy creates a latency proxy listening on a random port,
// forwarding to the given target address with the specified one-way latency.
func NewLatencyProxy(target string, latency time.Duration) (*LatencyProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &LatencyProxy{
		listener: ln,
		target:   target,
		latency:  latency,
	}
	go p.acceptLoop()
	return p, nil
}

// Port returns the local port the proxy is listening on.
func (p *LatencyProxy) Port() int {
	return p.listener.Addr().(*net.TCPAddr).Port
}

// Close shuts down the proxy and all active connections.
func (p *LatencyProxy) Close() error {
	p.mu.Lock()
	p.closed = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
	return p.listener.Close()
}

func (p *LatencyProxy) acceptLoop() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = client.Close()
			return
		}
		p.conns = append(p.conns, client)
		p.mu.Unlock()

		go p.handleConn(client)
	}
}

func (p *LatencyProxy) handleConn(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}

	p.mu.Lock()
	p.conns = append(p.conns, upstream)
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)

	// client → upstream (with latency)
	go func() {
		defer wg.Done()
		copyWithLatency(upstream, client, p.latency)
		_ = upstream.Close()
	}()

	// upstream → client (with latency)
	go func() {
		defer wg.Done()
		copyWithLatency(client, upstream, p.latency)
		_ = client.Close()
	}()

	wg.Wait()
}

// copyWithLatency copies data from src to dst, injecting a delay before each
// write. For zero latency this degrades to a plain io.Copy.
func copyWithLatency(dst, src net.Conn, latency time.Duration) {
	if latency <= 0 {
		_, _ = io.Copy(dst, src)
		return
	}
	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			time.Sleep(latency)
			_, writeErr := dst.Write(buf[:n])
			if writeErr != nil {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}
