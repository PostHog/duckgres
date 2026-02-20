package server

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/acme/autocert"
)

// ACMEManager wraps autocert.Manager to provide Let's Encrypt TLS certificates.
// It starts an HTTP listener on port 80 for HTTP-01 challenge validation.
type ACMEManager struct {
	manager  *autocert.Manager
	httpSrv  *http.Server
	domain   string
	cacheDir string
	closeOnce sync.Once
}

// NewACMEManager creates a new ACME manager for the given domain.
// It starts an HTTP listener on the specified address (default ":80") for HTTP-01 challenges.
// cacheDir is used to persist certificates across restarts.
func NewACMEManager(domain, email, cacheDir, httpAddr string) (*ACMEManager, error) {
	if cacheDir == "" {
		cacheDir = "./certs/acme"
	}
	if httpAddr == "" {
		httpAddr = ":80"
	}

	// Create cache directory
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}

	mgr := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(cacheDir),
		HostPolicy: autocert.HostWhitelist(domain),
		Email:      email,
	}

	// Start HTTP listener for ACME HTTP-01 challenges.
	// Use mgr.HTTPHandler with a 404 fallback instead of nil to avoid
	// creating an open HTTP-to-HTTPS redirector on port 80.
	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return nil, err
	}

	httpSrv := &http.Server{
		Handler:           mgr.HTTPHandler(http.NotFoundHandler()),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("ACME HTTP challenge server error.", "error", err)
		}
	}()

	slog.Info("ACME enabled.", "domain", domain, "cache_dir", cacheDir, "http_addr", httpAddr)

	return &ACMEManager{
		manager:  mgr,
		httpSrv:  httpSrv,
		domain:   domain,
		cacheDir: cacheDir,
	}, nil
}

// TLSConfig returns a tls.Config that uses ACME for certificate management.
// The GetCertificate callback dynamically obtains/renews certificates.
func (a *ACMEManager) TLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: a.manager.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}
}

// Close gracefully shuts down the HTTP challenge listener.
// Safe to call multiple times.
func (a *ACMEManager) Close() error {
	var err error
	a.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = a.httpSrv.Shutdown(ctx)
	})
	return err
}
