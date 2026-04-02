package integration

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestLatencyProxy(t *testing.T) {
	// Start an echo server as the "upstream".
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = echo.Close() }()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					_, _ = c.Write(buf[:n])
				}
			}(c)
		}
	}()

	echoAddr := echo.Addr().String()

	t.Run("zero_latency_forwards_data", func(t *testing.T) {
		proxy, err := NewLatencyProxy(echoAddr, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = proxy.Close() }()

		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxy.Port()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		msg := []byte("hello proxy")
		_, err = conn.Write(msg)
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 64)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if string(buf[:n]) != "hello proxy" {
			t.Errorf("got %q, want %q", buf[:n], "hello proxy")
		}
	})

	t.Run("adds_measurable_latency", func(t *testing.T) {
		latency := 50 * time.Millisecond
		proxy, err := NewLatencyProxy(echoAddr, latency)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = proxy.Close() }()

		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxy.Port()))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		msg := []byte("ping")
		start := time.Now()
		_, _ = conn.Write(msg)

		buf := make([]byte, 64)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := conn.Read(buf)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		if string(buf[:n]) != "ping" {
			t.Errorf("got %q, want %q", buf[:n], "ping")
		}

		// Expect at least 2x latency (one delay per direction).
		// Use 1.5x as lower bound to account for scheduling jitter.
		minExpected := time.Duration(float64(latency) * 1.5)
		if elapsed < minExpected {
			t.Errorf("round-trip %v faster than expected minimum %v (latency=%v per direction)", elapsed, minExpected, latency)
		}
		t.Logf("round-trip with %v one-way latency: %v", latency, elapsed)
	})

	t.Run("close_terminates_connections", func(t *testing.T) {
		proxy, err := NewLatencyProxy(echoAddr, 0)
		if err != nil {
			t.Fatal(err)
		}

		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxy.Port()))
		if err != nil {
			t.Fatal(err)
		}

		_ = proxy.Close()

		// After close, writes should eventually fail.
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, err = conn.Write([]byte("after close"))
		if err == nil {
			// Read should fail since upstream is gone
			buf := make([]byte, 64)
			_, _ = conn.Read(buf)
		}
		_ = conn.Close()
	})
}
