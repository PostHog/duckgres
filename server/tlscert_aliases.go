package server

import "github.com/posthog/duckgres/server/tlscert"

// Re-exports kept here so existing references to server.ACMEManager,
// server.ACMEDNSManager, server.NewACMEManager, server.NewACMEDNSManager,
// and server.EnsureCertificates continue to compile after the TLS / ACME
// helpers moved into server/tlscert. New code should import server/tlscert
// and use tlscert.X directly.

type (
	ACMEManager    = tlscert.ACMEManager
	ACMEDNSManager = tlscert.ACMEDNSManager
)

var (
	NewACMEManager        = tlscert.NewACMEManager
	NewACMEDNSManager     = tlscert.NewACMEDNSManager
	EnsureCertificates    = tlscert.EnsureCertificates
	generateSelfSignedCert = tlscert.GenerateSelfSignedCert // server_test.go calls this directly
)
