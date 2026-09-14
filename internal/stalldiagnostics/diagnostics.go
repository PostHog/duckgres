// Package stalldiagnostics provides opt-in diagnostics for bounded stall reproductions.
package stalldiagnostics

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

// Enabled is false unless explicitly enabled for a diagnostic deployment.
func Enabled() bool { return os.Getenv("DUCKGRES_STALL_DIAGNOSTICS") == "1" }

// Install handles SIGUSR2 without terminating the process. Stacks stay in a
// private local file; they must never be uploaded by public workflow artifacts.
func Install() {
	if !Enabled() {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR2)
	go func() {
		for range signals {
			path := fmt.Sprintf("/tmp/duckgres-stall-%d-%d.txt", os.Getpid(), time.Now().UnixNano())
			err := writeDump(path)
			slog.Info("Stall diagnostic stack capture", "success", err == nil)
		}
	}()
}

func writeDump(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	err = pprof.Lookup("goroutine").WriteTo(file, 2)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// Begin logs fixed phase names and duration only. It deliberately excludes SQL,
// session identifiers, data, and error text from these public-safe markers.
func Begin(phase string) func() {
	if !Enabled() {
		return func() {}
	}
	started := time.Now()
	slog.Info("Stall diagnostic phase", "phase", phase, "event", "begin")
	return func() {
		slog.Info("Stall diagnostic phase", "phase", phase, "event", "end", "elapsed", time.Since(started))
	}
}
