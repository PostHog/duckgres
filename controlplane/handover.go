package controlplane

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/posthog/duckgres/controlplane/fdpass"
)

// Handover protocol messages
type handoverMsg struct {
	Type    string           `json:"type"`
	Workers []handoverWorker `json:"workers,omitempty"`
}

type handoverWorker struct {
	ID         int    `json:"id"`
	GRPCSocket string `json:"grpc_socket"`
	FDSocket   string `json:"fd_socket"`
}

// startHandoverListener starts listening for handover requests from a new control plane.
// When a new CP connects, the old CP will transfer its TCP listener FD and worker info.
func (cp *ControlPlane) startHandoverListener() {
	if cp.cfg.HandoverSocket == "" {
		return
	}

	// Clean up old socket
	os.Remove(cp.cfg.HandoverSocket)

	ln, err := net.Listen("unix", cp.cfg.HandoverSocket)
	if err != nil {
		slog.Error("Failed to start handover listener.", "error", err)
		return
	}

	slog.Info("Handover listener started.", "socket", cp.cfg.HandoverSocket)

	go func() {
		defer ln.Close()
		defer os.Remove(cp.cfg.HandoverSocket)

		for {
			conn, err := ln.Accept()
			if err != nil {
				cp.closeMu.Lock()
				closed := cp.closed
				cp.closeMu.Unlock()
				if closed {
					return
				}
				slog.Error("Handover accept error.", "error", err)
				continue
			}

			// Handle handover in a goroutine (only one at a time is expected)
			go cp.handleHandoverRequest(conn, ln)
			return // Only handle one handover
		}
	}()
}

// handleHandoverRequest processes an incoming handover request from a new control plane.
func (cp *ControlPlane) handleHandoverRequest(conn net.Conn, handoverLn net.Listener) {
	defer conn.Close()
	defer handoverLn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Read handover request
	var req handoverMsg
	if err := decoder.Decode(&req); err != nil {
		slog.Error("Failed to read handover request.", "error", err)
		return
	}

	if req.Type != "handover_request" {
		slog.Error("Unexpected handover message type.", "type", req.Type)
		return
	}

	slog.Info("Received handover request, preparing transfer...")

	// Build worker list
	workers := cp.pool.Workers()
	handoverWorkers := make([]handoverWorker, 0, len(workers))
	for _, w := range workers {
		handoverWorkers = append(handoverWorkers, handoverWorker{
			ID:         w.ID,
			GRPCSocket: w.GRPCSocket,
			FDSocket:   w.FDSocket,
		})
	}

	// Send ack with worker info
	if err := encoder.Encode(handoverMsg{
		Type:    "handover_ack",
		Workers: handoverWorkers,
	}); err != nil {
		slog.Error("Failed to send handover ack.", "error", err)
		return
	}

	// Pass TCP listener FD via SCM_RIGHTS
	tcpLn, ok := cp.listener.(*net.TCPListener)
	if !ok {
		slog.Error("Listener is not TCP, cannot handover.")
		return
	}

	file, err := tcpLn.File()
	if err != nil {
		slog.Error("Failed to get listener FD.", "error", err)
		return
	}
	defer file.Close()

	uc, ok := conn.(*net.UnixConn)
	if !ok {
		slog.Error("Handover connection is not Unix.")
		return
	}

	if err := fdpass.SendFile(uc, file); err != nil {
		slog.Error("Failed to send listener FD.", "error", err)
		return
	}

	slog.Info("Listener FD sent to new control plane.")

	// Wait for handover_complete
	var complete handoverMsg
	if err := decoder.Decode(&complete); err != nil {
		slog.Error("Failed to read handover complete.", "error", err)
		return
	}

	if complete.Type != "handover_complete" {
		slog.Error("Unexpected handover message type.", "type", complete.Type)
		return
	}

	slog.Info("Handover complete. Old control plane stopping accept loop...")

	// Stop accepting new connections
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()
	_ = cp.listener.Close()

	// Brief wait for in-flight FD passes to complete
	time.Sleep(2 * time.Second)

	// Wait for wg to drain
	cp.wg.Wait()

	slog.Info("Old control plane exiting after handover.")
	os.Exit(0)
}

// receiveHandover connects to an existing control plane's handover socket,
// receives the TCP listener FD and worker info, and takes over.
func receiveHandover(handoverSocket string) (*net.TCPListener, []handoverWorker, error) {
	conn, err := net.Dial("unix", handoverSocket)
	if err != nil {
		return nil, nil, fmt.Errorf("connect handover socket: %w", err)
	}
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Send handover request
	if err := encoder.Encode(handoverMsg{Type: "handover_request"}); err != nil {
		return nil, nil, fmt.Errorf("send handover request: %w", err)
	}

	// Read ack with worker info
	var ack handoverMsg
	if err := decoder.Decode(&ack); err != nil {
		return nil, nil, fmt.Errorf("read handover ack: %w", err)
	}

	if ack.Type != "handover_ack" {
		return nil, nil, fmt.Errorf("unexpected handover message: %s", ack.Type)
	}

	// Receive listener FD
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, nil, fmt.Errorf("handover connection is not Unix")
	}

	file, err := fdpass.RecvFile(uc, "tcp-listener")
	if err != nil {
		return nil, nil, fmt.Errorf("receive listener FD: %w", err)
	}
	defer file.Close()

	// Reconstruct listener from FD
	ln, err := net.FileListener(file)
	if err != nil {
		return nil, nil, fmt.Errorf("FileListener: %w", err)
	}

	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
		ln.Close()
		return nil, nil, fmt.Errorf("not a TCP listener")
	}

	// Send handover complete
	if err := encoder.Encode(handoverMsg{Type: "handover_complete"}); err != nil {
		tcpLn.Close()
		return nil, nil, fmt.Errorf("send handover complete: %w", err)
	}

	slog.Info("Handover received: got listener and worker info.",
		"workers", len(ack.Workers))

	return tcpLn, ack.Workers, nil
}
