package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"golang.org/x/crypto/acme"
)

// ACMEDNSManager manages TLS certificates via ACME DNS-01 challenges using Route53.
// Unlike HTTP-01 (which requires port 80 reachable from the internet), DNS-01 works
// for private/internal interfaces since validation happens via DNS TXT records.
type ACMEDNSManager struct {
	client   *acme.Client
	domain   string
	zoneID   string
	cacheDir string
	r53      *route53.Client

	mu      sync.RWMutex
	cert    *tls.Certificate
	renewal *time.Timer

	closeOnce sync.Once
	done      chan struct{}
}

// acmeDirectoryURL is the ACME directory for certificate issuance.
// Uses Let's Encrypt production by default.
var acmeDirectoryURL = acme.LetsEncryptURL

// dnsChangeTimeout is the maximum time to wait for Route53 DNS changes to propagate.
var dnsChangeTimeout = 120 * time.Second

// dnsPropagationDelay is the fixed wait after Route53 returns INSYNC before
// asking the ACME server to validate. Route53 returns INSYNC when the change
// has propagated to all authoritative nameservers, but there can still be a
// brief delay before recursive resolvers see it.
var dnsPropagationDelay = 10 * time.Second

// renewBeforeExpiry is how far before expiry to renew certificates.
var renewBeforeExpiry = 30 * 24 * time.Hour

type acmeDNSCachedCert struct {
	CertPEM string `json:"cert_pem"`
	KeyPEM  string `json:"key_pem"`
}

// NewACMEDNSManager creates a new ACME DNS-01 manager for the given domain.
// It uses Route53 to create/delete TXT records for DNS-01 challenge validation.
// The zoneID is the Route53 hosted zone ID that contains the domain.
func NewACMEDNSManager(domain, email, zoneID, cacheDir string) (*ACMEDNSManager, error) {
	if domain == "" {
		return nil, errors.New("ACME DNS domain is required")
	}
	if zoneID == "" {
		return nil, errors.New("ACME DNS zone ID is required")
	}
	if cacheDir == "" {
		cacheDir = "./certs/acme-dns"
	}
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create ACME DNS cache directory: %w", err)
	}

	// Load AWS config (uses instance profile, env vars, or shared config)
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	r53Client := route53.NewFromConfig(awsCfg)

	// Create or load ACME account key
	accountKey, err := loadOrCreateAccountKey(filepath.Join(cacheDir, "account-key.pem"))
	if err != nil {
		return nil, fmt.Errorf("failed to load/create account key: %w", err)
	}

	client := &acme.Client{
		Key:          accountKey,
		DirectoryURL: acmeDirectoryURL,
	}

	// Register account (idempotent)
	acct := &acme.Account{Contact: nil}
	if email != "" {
		acct.Contact = []string{"mailto:" + email}
	}
	if _, err := client.Register(context.Background(), acct, acme.AcceptTOS); err != nil {
		// ErrAccountAlreadyExists is fine
		var regErr *acme.Error
		if !errors.As(err, &regErr) || regErr.StatusCode != 409 {
			// Try to continue anyway - some ACME servers handle this differently
			slog.Warn("ACME account registration note.", "error", err)
		}
	}

	m := &ACMEDNSManager{
		client:   client,
		domain:   domain,
		zoneID:   zoneID,
		cacheDir: cacheDir,
		r53:      r53Client,
		done:     make(chan struct{}),
	}

	// Try to load cached certificate
	if err := m.loadCachedCert(); err != nil {
		slog.Info("No cached ACME DNS certificate, will obtain a new one.", "reason", err)
	}

	// Obtain certificate if we don't have one or it's expired/expiring
	if m.cert == nil || m.needsRenewal() {
		if err := m.obtainCert(context.Background()); err != nil {
			// If we have a cached cert that's still valid, log warning but continue
			if m.cert != nil {
				slog.Warn("Failed to renew ACME DNS certificate, using cached cert.", "error", err)
			} else {
				return nil, fmt.Errorf("failed to obtain initial certificate: %w", err)
			}
		}
	}

	// Schedule renewal
	m.scheduleRenewal()

	slog.Info("ACME DNS-01 enabled.", "domain", domain, "zone_id", zoneID, "cache_dir", cacheDir)
	return m, nil
}

// TLSConfig returns a tls.Config that serves the ACME-obtained certificate.
func (m *ACMEDNSManager) TLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: m.getCertificate,
		MinVersion:     tls.VersionTLS12,
	}
}

// Close stops the renewal timer and releases resources.
func (m *ACMEDNSManager) Close() error {
	m.closeOnce.Do(func() {
		close(m.done)
		m.mu.Lock()
		if m.renewal != nil {
			m.renewal.Stop()
		}
		m.mu.Unlock()
	})
	return nil
}

func (m *ACMEDNSManager) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cert == nil {
		return nil, errors.New("no certificate available")
	}
	return m.cert, nil
}

func (m *ACMEDNSManager) needsRenewal() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.cert == nil || m.cert.Leaf == nil {
		return true
	}
	return time.Until(m.cert.Leaf.NotAfter) < renewBeforeExpiry
}

func (m *ACMEDNSManager) scheduleRenewal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cert == nil || m.cert.Leaf == nil {
		return
	}

	renewAt := m.cert.Leaf.NotAfter.Add(-renewBeforeExpiry)
	delay := time.Until(renewAt)
	if delay < time.Minute {
		delay = time.Minute
	}

	m.renewal = time.AfterFunc(delay, func() {
		select {
		case <-m.done:
			return
		default:
		}

		slog.Info("Renewing ACME DNS certificate.", "domain", m.domain)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := m.obtainCert(ctx); err != nil {
			slog.Error("Failed to renew ACME DNS certificate.", "error", err)
			// Retry in 1 hour
			m.mu.Lock()
			m.renewal = time.AfterFunc(time.Hour, func() { m.scheduleRenewal() })
			m.mu.Unlock()
			return
		}
		slog.Info("ACME DNS certificate renewed.", "domain", m.domain)
		m.scheduleRenewal()
	})

	slog.Info("ACME DNS certificate renewal scheduled.", "renew_at", renewAt)
}

func (m *ACMEDNSManager) obtainCert(ctx context.Context) error {
	// Create a new order
	order, err := m.client.AuthorizeOrder(ctx, acme.DomainIDs(m.domain))
	if err != nil {
		return fmt.Errorf("authorize order: %w", err)
	}

	// Process each authorization
	for _, authzURL := range order.AuthzURLs {
		authz, err := m.client.GetAuthorization(ctx, authzURL)
		if err != nil {
			return fmt.Errorf("get authorization: %w", err)
		}

		if authz.Status == acme.StatusValid {
			continue
		}

		// Find DNS-01 challenge
		var chal *acme.Challenge
		for _, c := range authz.Challenges {
			if c.Type == "dns-01" {
				chal = c
				break
			}
		}
		if chal == nil {
			return fmt.Errorf("no dns-01 challenge found for %s", authz.Identifier.Value)
		}

		// Compute the DNS TXT record value
		txtValue, err := m.client.DNS01ChallengeRecord(chal.Token)
		if err != nil {
			return fmt.Errorf("compute dns-01 record: %w", err)
		}

		recordName := "_acme-challenge." + authz.Identifier.Value

		// Create DNS TXT record
		if err := m.createTXTRecord(ctx, recordName, txtValue); err != nil {
			return fmt.Errorf("create TXT record: %w", err)
		}

		// Accept the challenge
		if _, err := m.client.Accept(ctx, chal); err != nil {
			_ = m.deleteTXTRecord(context.Background(), recordName, txtValue)
			return fmt.Errorf("accept challenge: %w", err)
		}

		// Wait for authorization to be valid
		if _, err := m.client.WaitAuthorization(ctx, authzURL); err != nil {
			_ = m.deleteTXTRecord(context.Background(), recordName, txtValue)
			return fmt.Errorf("wait authorization: %w", err)
		}

		// Clean up DNS record
		if err := m.deleteTXTRecord(ctx, recordName, txtValue); err != nil {
			slog.Warn("Failed to clean up ACME DNS TXT record.", "record", recordName, "error", err)
		}
	}

	// Generate certificate key
	certKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate cert key: %w", err)
	}

	// Create CSR
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		DNSNames: []string{m.domain},
	}, certKey)
	if err != nil {
		return fmt.Errorf("create CSR: %w", err)
	}

	// Finalize order
	derChain, _, err := m.client.CreateOrderCert(ctx, order.FinalizeURL, csr, true)
	if err != nil {
		return fmt.Errorf("finalize order: %w", err)
	}

	// Build certificate chain
	var certPEM []byte
	for _, der := range derChain {
		certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}

	keyBytes, err := x509.MarshalECPrivateKey(certKey)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	// Parse the certificate
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("parse certificate: %w", err)
	}
	tlsCert.Leaf, _ = x509.ParseCertificate(tlsCert.Certificate[0])

	// Store the certificate
	m.mu.Lock()
	m.cert = &tlsCert
	m.mu.Unlock()

	// Cache to disk
	if err := m.saveCachedCert(certPEM, keyPEM); err != nil {
		slog.Warn("Failed to cache ACME DNS certificate.", "error", err)
	}

	return nil
}

func (m *ACMEDNSManager) createTXTRecord(ctx context.Context, name, value string) error {
	// Ensure the name is fully qualified
	if !strings.HasSuffix(name, ".") {
		name = name + "."
	}

	input := &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(m.zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionUpsert,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name:            aws.String(name),
						Type:            r53types.RRTypeTxt,
						TTL:             aws.Int64(60),
						ResourceRecords: []r53types.ResourceRecord{{Value: aws.String(`"` + value + `"`)}},
					},
				},
			},
		},
	}

	result, err := m.r53.ChangeResourceRecordSets(ctx, input)
	if err != nil {
		return fmt.Errorf("route53 upsert TXT record: %w", err)
	}

	// Wait for the change to propagate
	changeID := result.ChangeInfo.Id
	slog.Info("Waiting for DNS change to propagate.", "change_id", aws.ToString(changeID), "record", name)

	deadline := time.Now().Add(dnsChangeTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		status, err := m.r53.GetChange(ctx, &route53.GetChangeInput{Id: changeID})
		if err != nil {
			return fmt.Errorf("route53 get change status: %w", err)
		}
		if status.ChangeInfo.Status == r53types.ChangeStatusInsync {
			slog.Info("DNS change propagated.", "change_id", aws.ToString(changeID))
			// Additional propagation delay for recursive resolvers
			select {
			case <-time.After(dnsPropagationDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("DNS change timed out after %v", dnsChangeTimeout)
}

func (m *ACMEDNSManager) deleteTXTRecord(ctx context.Context, name, value string) error {
	if !strings.HasSuffix(name, ".") {
		name = name + "."
	}

	input := &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(m.zoneID),
		ChangeBatch: &r53types.ChangeBatch{
			Changes: []r53types.Change{
				{
					Action: r53types.ChangeActionDelete,
					ResourceRecordSet: &r53types.ResourceRecordSet{
						Name:            aws.String(name),
						Type:            r53types.RRTypeTxt,
						TTL:             aws.Int64(60),
						ResourceRecords: []r53types.ResourceRecord{{Value: aws.String(`"` + value + `"`)}},
					},
				},
			},
		},
	}

	_, err := m.r53.ChangeResourceRecordSets(ctx, input)
	return err
}

func (m *ACMEDNSManager) loadCachedCert() error {
	data, err := os.ReadFile(filepath.Join(m.cacheDir, "cert.json"))
	if err != nil {
		return err
	}

	var cached acmeDNSCachedCert
	if err := json.Unmarshal(data, &cached); err != nil {
		return err
	}

	tlsCert, err := tls.X509KeyPair([]byte(cached.CertPEM), []byte(cached.KeyPEM))
	if err != nil {
		return err
	}
	tlsCert.Leaf, _ = x509.ParseCertificate(tlsCert.Certificate[0])

	// Check if the certificate is still valid
	if tlsCert.Leaf != nil && time.Now().After(tlsCert.Leaf.NotAfter) {
		return fmt.Errorf("cached certificate expired at %v", tlsCert.Leaf.NotAfter)
	}

	m.mu.Lock()
	m.cert = &tlsCert
	m.mu.Unlock()

	if tlsCert.Leaf != nil {
		slog.Info("Loaded cached ACME DNS certificate.", "expires", tlsCert.Leaf.NotAfter)
	}
	return nil
}

func (m *ACMEDNSManager) saveCachedCert(certPEM, keyPEM []byte) error {
	cached := acmeDNSCachedCert{
		CertPEM: string(certPEM),
		KeyPEM:  string(keyPEM),
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.cacheDir, "cert.json"), data, 0600)
}

func loadOrCreateAccountKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		block, _ := pem.Decode(data)
		if block != nil && block.Type == "EC PRIVATE KEY" {
			return x509.ParseECPrivateKey(block.Bytes)
		}
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}

	pemData := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	if err := os.WriteFile(path, pemData, 0600); err != nil {
		return nil, err
	}

	return key, nil
}
