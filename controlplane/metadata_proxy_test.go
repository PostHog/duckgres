//go:build kubernetes

package controlplane

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/posthog/duckgres/server"
)

type metadataProxyTestStore struct {
	*fakeConfigStore
	orgID         string
	enabled       bool
	authenticated bool
	resolveCalls  int
	gotPrefix     string
	gotUsername   string
	gotPassword   string
}

func (s *metadataProxyTestStore) ResolveMetadataProxyConnection(prefix, username, password string) (string, bool, bool) {
	s.resolveCalls++
	s.gotPrefix = prefix
	s.gotUsername = username
	s.gotPassword = password
	return s.orgID, s.enabled, s.authenticated
}

func (s *metadataProxyTestStore) MetadataProxySessionAllowed(string, string) bool {
	return s.enabled && s.authenticated
}

type fakeMetadataPostgresConn struct {
	hijacked   *pgconn.HijackedConn
	onSyncConn func(context.Context)
}

func (c *fakeMetadataPostgresConn) SyncConn(ctx context.Context) error {
	if c.onSyncConn != nil {
		c.onSyncConn(ctx)
	}
	return nil
}
func (c *fakeMetadataPostgresConn) Hijack() (*pgconn.HijackedConn, error) {
	return c.hijacked, nil
}
func (c *fakeMetadataPostgresConn) Close(context.Context) error {
	return c.hijacked.Conn.Close()
}

func TestMetadataProxyRequiresExactVirtualDatabase(t *testing.T) {
	for _, database := range []string{"", "ducklake", "other"} {
		t.Run(database, func(t *testing.T) {
			store := &metadataProxyTestStore{
				fakeConfigStore: &fakeConfigStore{},
				orgID:           "org-a",
				enabled:         true,
				authenticated:   true,
			}
			cp := &ControlPlane{
				cfg:         ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
				configStore: store,
				metadataPostgresURL: func(context.Context, string) (string, error) {
					return "", nil
				},
			}
			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			defer clientConn.Close()
			var output strings.Builder
			if !cp.tryMetadataProxy(context.Background(), serverConn, bufio.NewReader(serverConn),
				bufio.NewWriter(&output), "org-a.md.us.postwh.com", "root", database, "secret") {
				t.Fatal("metadata hostname must be handled, not fall through")
			}
			if !strings.Contains(output.String(), `connect with dbname="metadata"`) {
				t.Fatalf("response %q does not contain strict database hint", output.String())
			}
			if !strings.Contains(output.String(), "3D000") {
				t.Fatalf("response %q does not contain invalid-database SQLSTATE", output.String())
			}
		})
	}
}

func TestMetadataProxyDisabledOrgFailsClosed(t *testing.T) {
	store := &metadataProxyTestStore{
		fakeConfigStore: &fakeConfigStore{},
		orgID:           "",
		enabled:         false,
	}
	resolverCalled := false
	cp := &ControlPlane{
		cfg:         ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
		configStore: store,
		metadataPostgresURL: func(context.Context, string) (string, error) {
			resolverCalled = true
			return "", nil
		},
	}
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	var output strings.Builder
	if !cp.tryMetadataProxy(context.Background(), serverConn, bufio.NewReader(serverConn),
		bufio.NewWriter(&output), "org-a.md.us.postwh.com", "root", "metadata", "secret") {
		t.Fatal("disabled metadata hostname must be denied, not fall through")
	}
	if !strings.Contains(output.String(), "metadata endpoint is unavailable") {
		t.Fatalf("response %q does not contain generic denial", output.String())
	}
	if store.resolveCalls != 1 {
		t.Fatalf("expected one atomic metadata resolution, got %d", store.resolveCalls)
	}
	if resolverCalled {
		t.Fatal("disabled org must not reach the internal metadata resolver")
	}
}

func TestMetadataProxyAuthenticatesBeforeResolvingInternalCredentials(t *testing.T) {
	for _, tc := range []struct {
		name          string
		username      string
		authenticated bool
		draining      bool
		wantResolver  bool
		wantSQLState  string
		wantOutcome   string
	}{
		{name: "wrong password", username: "root", authenticated: false, wantSQLState: "28P01", wantOutcome: metadataProxyOutcomeAuthFailed},
		{name: "non-root user", username: "reader", authenticated: true, wantSQLState: "28P01", wantOutcome: metadataProxyOutcomeAuthFailed},
		{name: "draining", username: "root", authenticated: true, draining: true, wantSQLState: "57P03", wantOutcome: metadataProxyOutcomeDraining},
		{name: "valid root", username: "root", authenticated: true, wantResolver: true, wantOutcome: metadataProxyOutcomeTargetResolutionError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &metadataProxyTestStore{
				fakeConfigStore: &fakeConfigStore{},
				orgID:           "org-a",
				enabled:         true,
				authenticated:   tc.authenticated,
			}
			resolverCalled := false
			cp := &ControlPlane{
				cfg:              ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
				configStore:      store,
				metadataSessions: newMetadataProxySessionRegistry(2),
				metadataPostgresURL: func(_ context.Context, orgID string) (string, error) {
					resolverCalled = true
					if orgID != "org-a" {
						t.Fatalf("internal resolver org = %q, want org-a", orgID)
					}
					return "", errors.New("stop before dialing")
				},
			}
			cp.sessionDraining.Store(tc.draining)
			outcomeBefore := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, "org-a", tc.wantOutcome)
			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			defer clientConn.Close()
			var output strings.Builder
			if !cp.tryMetadataProxy(context.Background(), serverConn, bufio.NewReader(serverConn),
				bufio.NewWriter(&output), "alias.md.us.postwh.com", tc.username, "metadata", "secret") {
				t.Fatal("metadata hostname must be handled")
			}
			if resolverCalled != tc.wantResolver {
				t.Fatalf("internal resolver called = %v, want %v", resolverCalled, tc.wantResolver)
			}
			if store.gotPrefix != "alias" || store.gotUsername != tc.username || store.gotPassword != "secret" {
				t.Fatalf("atomic auth got (%q, %q, %q)", store.gotPrefix, store.gotUsername, store.gotPassword)
			}
			if tc.wantSQLState != "" && !strings.Contains(output.String(), tc.wantSQLState) {
				t.Fatalf("response %q does not contain SQLSTATE %s", output.String(), tc.wantSQLState)
			}
			if got := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, "org-a", tc.wantOutcome); got != outcomeBefore+1 {
				t.Fatalf("metadata proxy outcome %q = %v, want %v", tc.wantOutcome, got, outcomeBefore+1)
			}
		})
	}
}

func TestMetadataProxyConnectionLimitRecordsCapacityOutcome(t *testing.T) {
	const orgID = "org-a"
	store := &metadataProxyTestStore{
		fakeConfigStore: &fakeConfigStore{},
		orgID:           orgID,
		enabled:         true,
		authenticated:   true,
	}
	registry := newMetadataProxySessionRegistry(1)
	occupiedServer, occupiedClient := net.Pipe()
	defer occupiedServer.Close()
	defer occupiedClient.Close()
	releaseOccupied, admitted := registry.Register(orgID, "root", occupiedServer)
	if !admitted {
		t.Fatal("failed to reserve the test org's only metadata proxy slot")
	}
	defer releaseOccupied()

	resolverCalled := false
	cp := &ControlPlane{
		cfg:              ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
		configStore:      store,
		metadataSessions: registry,
		metadataPostgresURL: func(context.Context, string) (string, error) {
			resolverCalled = true
			return "", nil
		},
	}
	outcomeBefore := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, orgID, metadataProxyOutcomeCapacity)
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	var output strings.Builder
	if !cp.tryMetadataProxy(context.Background(), serverConn, bufio.NewReader(serverConn),
		bufio.NewWriter(&output), "alias.md.us.postwh.com", "root", "metadata", "secret") {
		t.Fatal("metadata hostname must be handled")
	}
	if resolverCalled {
		t.Fatal("capacity rejection must happen before resolving internal credentials")
	}
	if !strings.Contains(output.String(), "53300") {
		t.Fatalf("response %q does not contain too-many-connections SQLSTATE", output.String())
	}
	if got := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, orgID, metadataProxyOutcomeCapacity); got != outcomeBefore+1 {
		t.Fatalf("metadata proxy capacity outcome = %v, want %v", got, outcomeBefore+1)
	}
}

func TestMetadataProxyRelaysAuthenticatedSession(t *testing.T) {
	const orgID = "org-a"
	openBefore := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID)
	successBefore := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, orgID, metadataProxyOutcomeSuccess)
	durationBefore := histogramVecLabelSampleCount(t, metadataProxyConnectionDurationHistogram, orgID)
	connectBefore := histogramVecLabelSampleCount(t, metadataProxyUpstreamConnectDurationHistogram, orgID, metadataProxyUpstreamOutcomeSuccess)
	clientBytesBefore := counterVecLabelValue(t, metadataProxyBytesCounter, orgID, metadataProxyDirectionClientToUpstream)
	upstreamBytesBefore := counterVecLabelValue(t, metadataProxyBytesCounter, orgID, metadataProxyDirectionUpstreamToClient)

	store := &metadataProxyTestStore{
		fakeConfigStore: &fakeConfigStore{},
		orgID:           orgID,
		enabled:         true,
		authenticated:   true,
	}
	upstreamProxy, upstreamServer := net.Pipe()
	defer upstreamServer.Close()
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i + 1)
	}
	fakeUpstream := &fakeMetadataPostgresConn{
		hijacked: &pgconn.HijackedConn{
			Conn:              upstreamProxy,
			PID:               42,
			SecretKey:         secret,
			ParameterStatuses: map[string]string{"server_version": "18.3"},
			TxStatus:          'I',
			Config: &pgconn.Config{DialFunc: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("cancel dial is not used in this test")
			}},
		},
		onSyncConn: func(ctx context.Context) {
			assertMetadataProxyBootstrapDeadline(t, ctx)
		},
	}
	var upstreamConnectURL string
	cp := &ControlPlane{
		cfg:              ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
		configStore:      store,
		metadataSessions: newMetadataProxySessionRegistry(2),
		metadataPostgresURL: func(ctx context.Context, orgID string) (string, error) {
			assertMetadataProxyBootstrapDeadline(t, ctx)
			if orgID != "org-a" {
				t.Fatalf("internal resolver org = %q, want org-a", orgID)
			}
			return "postgres://metadata_user:secret@internal/warehouse?sslmode=disable", nil
		},
		metadataPostgresConnect: func(ctx context.Context, upstreamURL string) (metadataPostgresConn, error) {
			assertMetadataProxyBootstrapDeadline(t, ctx)
			upstreamConnectURL = upstreamURL
			return fakeUpstream, nil
		},
	}

	proxySide, clientSide := net.Pipe()
	defer clientSide.Close()
	_ = clientSide.SetDeadline(time.Now().Add(3 * time.Second))
	done := make(chan bool, 1)
	go func() {
		defer proxySide.Close()
		done <- cp.tryMetadataProxy(context.Background(), proxySide, bufio.NewReader(proxySide),
			bufio.NewWriter(proxySide), "alias.md.us.postwh.com", "root", "metadata", "secret")
	}()

	frontend := pgproto3.NewFrontend(clientSide, clientSide)
	var sawAuthOK, sawParameter, sawLegacyKey bool
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive proxy handshake: %v", err)
		}
		switch msg := msg.(type) {
		case *pgproto3.AuthenticationOk:
			sawAuthOK = true
		case *pgproto3.ParameterStatus:
			if msg.Name == "server_version" && msg.Value == "18.3" {
				sawParameter = true
			}
		case *pgproto3.BackendKeyData:
			sawLegacyKey = len(msg.SecretKey) == 4 &&
				msg.ProcessID >= 0x40000000 && msg.ProcessID < 0x80000000
		case *pgproto3.ReadyForQuery:
			if !sawAuthOK || !sawParameter || !sawLegacyKey || msg.TxStatus != 'I' {
				t.Fatalf("incomplete proxy handshake: auth=%v parameter=%v legacy_key=%v tx=%q",
					sawAuthOK, sawParameter, sawLegacyKey, msg.TxStatus)
			}
			goto relay
		}
	}

relay:
	if got := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID); got != openBefore+1 {
		t.Fatalf("metadata proxy open connections = %v, want %v", got, openBefore+1)
	}
	upstreamConfig, err := pgconn.ParseConfig(upstreamConnectURL)
	if err != nil {
		t.Fatalf("parse tagged upstream URL: %v", err)
	}
	if got := upstreamConfig.RuntimeParams["application_name"]; got != metadataProxyUpstreamApplicationName {
		t.Fatalf("upstream application_name = %q, want %q", got, metadataProxyUpstreamApplicationName)
	}

	clientPayload := []byte("frontend-to-upstream")
	clientWrite := make(chan error, 1)
	go func() {
		_, err := clientSide.Write(clientPayload)
		clientWrite <- err
	}()
	gotClientPayload := make([]byte, len(clientPayload))
	if _, err := io.ReadFull(upstreamServer, gotClientPayload); err != nil {
		t.Fatalf("read client payload upstream: %v", err)
	}
	if err := <-clientWrite; err != nil {
		t.Fatalf("write client payload: %v", err)
	}
	if string(gotClientPayload) != string(clientPayload) {
		t.Fatalf("upstream payload = %q, want %q", gotClientPayload, clientPayload)
	}

	upstreamPayload := []byte("upstream-to-frontend")
	upstreamWrite := make(chan error, 1)
	go func() {
		_, err := upstreamServer.Write(upstreamPayload)
		upstreamWrite <- err
	}()
	gotUpstreamPayload := make([]byte, len(upstreamPayload))
	if _, err := io.ReadFull(clientSide, gotUpstreamPayload); err != nil {
		t.Fatalf("read upstream payload from proxy: %v", err)
	}
	if err := <-upstreamWrite; err != nil {
		t.Fatalf("write upstream payload: %v", err)
	}
	if string(gotUpstreamPayload) != string(upstreamPayload) {
		t.Fatalf("frontend payload = %q, want %q", gotUpstreamPayload, upstreamPayload)
	}

	_ = clientSide.Close()
	select {
	case handled := <-done:
		if !handled {
			t.Fatal("metadata session unexpectedly fell through")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("metadata proxy did not stop after the client closed")
	}

	if got := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID); got != openBefore {
		t.Fatalf("metadata proxy open connections after close = %v, want %v", got, openBefore)
	}
	if got := counterVecLabelValue(t, metadataProxyConnectionAttemptsCounter, orgID, metadataProxyOutcomeSuccess); got != successBefore+1 {
		t.Fatalf("successful metadata proxy attempts = %v, want %v", got, successBefore+1)
	}
	if got := histogramVecLabelSampleCount(t, metadataProxyConnectionDurationHistogram, orgID); got != durationBefore+1 {
		t.Fatalf("metadata proxy duration samples = %v, want %v", got, durationBefore+1)
	}
	if got := histogramVecLabelSampleCount(t, metadataProxyUpstreamConnectDurationHistogram, orgID, metadataProxyUpstreamOutcomeSuccess); got != connectBefore+1 {
		t.Fatalf("metadata proxy upstream connect samples = %v, want %v", got, connectBefore+1)
	}
	if got := counterVecLabelValue(t, metadataProxyBytesCounter, orgID, metadataProxyDirectionClientToUpstream); got != clientBytesBefore+float64(len(clientPayload)) {
		t.Fatalf("metadata proxy client bytes = %v, want %v", got, clientBytesBefore+float64(len(clientPayload)))
	}
	if got := counterVecLabelValue(t, metadataProxyBytesCounter, orgID, metadataProxyDirectionUpstreamToClient); got != upstreamBytesBefore+float64(len(upstreamPayload)) {
		t.Fatalf("metadata proxy upstream bytes = %v, want %v", got, upstreamBytesBefore+float64(len(upstreamPayload)))
	}
}

func TestHandleConnectionMetadataProxyKeepsGlobalAndWorkerMetricsDistinct(t *testing.T) {
	const orgID = "metadata-metrics-org"
	globalBefore := metricGaugeValue(t, "duckgres_connections_open")
	proxyBefore := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID)
	workerAcceptedBefore := counterVecLabelValue(t, orgPgSessionsAcceptedCounter, orgID, "false")

	store := &metadataProxyTestStore{
		fakeConfigStore: &fakeConfigStore{},
		orgID:           orgID,
		enabled:         true,
		authenticated:   true,
	}
	upstreamProxy, upstreamServer := net.Pipe()
	fakeUpstream := &fakeMetadataPostgresConn{hijacked: &pgconn.HijackedConn{
		Conn:              upstreamProxy,
		PID:               42,
		SecretKey:         []byte{1, 2, 3, 4},
		ParameterStatuses: map[string]string{"server_version": "18.3"},
		TxStatus:          'I',
		Config: &pgconn.Config{DialFunc: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("cancel dial is not used in this test")
		}},
	}}
	var upstreamConnectURL string
	cp := &ControlPlane{
		cfg:                 ControlPlaneConfig{MetadataHostnameSuffixes: []string{".md.us.postwh.com"}},
		tlsConfig:           testControlPlaneTLSConfig(t),
		configStore:         store,
		metadataSessions:    newMetadataProxySessionRegistry(2),
		metadataPostgresURL: func(context.Context, string) (string, error) { return "postgres://internal/metadata", nil },
		metadataPostgresConnect: func(_ context.Context, upstreamURL string) (metadataPostgresConn, error) {
			upstreamConnectURL = upstreamURL
			return fakeUpstream, nil
		},
	}

	cfg, err := pgconn.ParseConfig("postgres://root:secret@127.0.0.1/metadata?sslmode=require&application_name=customer-controlled")
	if err != nil {
		t.Fatalf("parse frontend config: %v", err)
	}
	cfg.TLSConfig = testMetadataProxyClientTLSConfig()
	clientClosed := make(chan struct{})
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, serverConn := net.Pipe()
		go func() {
			cp.handleConnection(serverConn)
			close(clientClosed)
		}()
		return client, nil
	}

	conn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect through metadata proxy: %v", err)
	}
	if got := metricGaugeValue(t, "duckgres_connections_open"); got != globalBefore+1 {
		t.Fatalf("global open connections = %v, want %v", got, globalBefore+1)
	}
	if got := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID); got != proxyBefore+1 {
		t.Fatalf("metadata proxy open connections = %v, want %v", got, proxyBefore+1)
	}
	if got := counterVecLabelValue(t, orgPgSessionsAcceptedCounter, orgID, "false"); got != workerAcceptedBefore {
		t.Fatalf("worker-backed accepted sessions changed for metadata proxy: got %v, want %v", got, workerAcceptedBefore)
	}
	upstreamConfig, err := pgconn.ParseConfig(upstreamConnectURL)
	if err != nil {
		t.Fatalf("parse internally tagged upstream URL: %v", err)
	}
	if got := upstreamConfig.RuntimeParams["application_name"]; got != metadataProxyUpstreamApplicationName {
		t.Fatalf("client application_name reached upstream: got %q, want fixed %q", got, metadataProxyUpstreamApplicationName)
	}

	frontendHijacked, err := conn.Hijack()
	if err != nil {
		t.Fatalf("hijack frontend test connection: %v", err)
	}
	_ = frontendHijacked.Conn.Close()
	_ = upstreamServer.Close()
	select {
	case <-clientClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("control-plane metadata connection did not close")
	}
	if got := metricGaugeValue(t, "duckgres_connections_open"); got != globalBefore {
		t.Fatalf("global open connections after close = %v, want %v", got, globalBefore)
	}
	if got := gaugeVecLabelValue(t, metadataProxyConnectionsOpenGauge, orgID); got != proxyBefore {
		t.Fatalf("metadata proxy open connections after close = %v, want %v", got, proxyBefore)
	}
}

func testMetadataProxyClientTLSConfig() *tls.Config {
	return &tls.Config{
		ServerName:         "alias.md.us.postwh.com",
		InsecureSkipVerify: true,
	}
}

func assertMetadataProxyBootstrapDeadline(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("metadata proxy bootstrap context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > metadataProxyBootstrapTimeout {
		t.Fatalf("metadata proxy bootstrap deadline remaining = %s, want (0, %s]", remaining, metadataProxyBootstrapTimeout)
	}
}

func TestMetadataProxySafeConnectErrorNeverLogsInjectedDSN(t *testing.T) {
	const secret = "metadata-password-must-not-appear"
	got := metadataProxySafeConnectError(errors.New("postgres://internal_user:" + secret + "@metadata.internal/db"))
	if strings.Contains(got, secret) || strings.Contains(got, "postgres://") {
		t.Fatalf("safe connect error leaked the injected DSN: %q", got)
	}

	parseErr := pgconn.NewParseConfigError(
		"postgres://internal_user:"+secret+"@metadata.internal/db",
		"test parse failure",
		nil,
	)
	if got := metadataProxySafeConnectError(parseErr); strings.Contains(got, secret) {
		t.Fatalf("safe pgconn parse error leaked password: %q", got)
	}
}

func TestMetadataProxySessionRegistryCapsAndKillsByUser(t *testing.T) {
	registry := newMetadataProxySessionRegistry(1)
	serverA, clientA := net.Pipe()
	defer clientA.Close()
	releaseA, ok := registry.Register("org-a", "root", serverA)
	if !ok {
		t.Fatal("first org session should be admitted")
	}
	defer releaseA()

	serverB, clientB := net.Pipe()
	defer serverB.Close()
	defer clientB.Close()
	if _, ok := registry.Register("org-a", "root", serverB); ok {
		t.Fatal("second session for capped org should be rejected")
	}

	serverOther, clientOther := net.Pipe()
	defer serverOther.Close()
	defer clientOther.Close()
	releaseOther, ok := registry.Register("org-b", "root", serverOther)
	if !ok {
		t.Fatal("cap must be scoped per org")
	}
	defer releaseOther()

	if killed := registry.KillUser("org-a", "root"); killed != 1 {
		t.Fatalf("killed = %d, want 1", killed)
	}
	_ = clientA.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := clientA.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("killed connection read error = %v, want EOF", err)
	}
	serverAfterKill, clientAfterKill := net.Pipe()
	defer clientAfterKill.Close()
	releaseAfterKill, ok := registry.Register("org-a", "root", serverAfterKill)
	if !ok {
		t.Fatal("per-user kill must not close future metadata admission")
	}
	defer releaseAfterKill()

	if killed := registry.KillAll(); killed != 2 {
		t.Fatalf("drain killed = %d, want org-a and org-b sessions", killed)
	}
	serverAfterDrain, clientAfterDrain := net.Pipe()
	defer serverAfterDrain.Close()
	defer clientAfterDrain.Close()
	if _, ok := registry.Register("org-a", "root", serverAfterDrain); ok {
		t.Fatal("drained metadata registry must reject new sessions")
	}
}

func TestForwardMetadataCancelClosesExactSessionConnections(t *testing.T) {
	cp := &ControlPlane{}
	frontendKey := server.BackendKey{Pid: 0x40000123, SecretKey: 456}
	terminatedBefore := counterVecLabelValue(
		t,
		metadataProxyCancelRequestsCounter,
		metadataProxyCancelOutcomeSessionTerminated,
	)
	notLocalBefore := counterVecLabelValue(
		t,
		metadataProxyCancelRequestsCounter,
		metadataProxyCancelOutcomeNotLocal,
	)
	frontend, frontendPeer := net.Pipe()
	defer frontendPeer.Close()
	upstream, upstreamPeer := net.Pipe()
	defer upstreamPeer.Close()
	cp.proxyCancels.Store(frontendKey, metadataProxyCancelTarget{
		frontend: frontend,
		upstream: upstream,
	})
	defer cp.proxyCancels.Delete(frontendKey)

	if !cp.forwardMetadataCancel(frontendKey) {
		t.Fatal("known proxy key should be handled")
	}
	for name, peer := range map[string]net.Conn{
		"frontend": frontendPeer,
		"upstream": upstreamPeer,
	} {
		var oneByte [1]byte
		if _, err := peer.Read(oneByte[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("%s peer read error = %v, want EOF from exact connection close", name, err)
		}
	}
	if got := counterVecLabelValue(t, metadataProxyCancelRequestsCounter, metadataProxyCancelOutcomeSessionTerminated); got != terminatedBefore+1 {
		t.Fatalf("metadata proxy terminated cancel outcomes = %v, want %v", got, terminatedBefore+1)
	}
	nonLocalKey := server.BackendKey{Pid: 0x40000001, SecretKey: 789}
	if !cp.forwardMetadataCancel(nonLocalKey) {
		t.Fatal("unknown synthetic proxy key must be handled as non-local")
	}
	if got := counterVecLabelValue(t, metadataProxyCancelRequestsCounter, metadataProxyCancelOutcomeNotLocal); got != notLocalBefore+1 {
		t.Fatalf("metadata proxy non-local cancel outcomes = %v, want %v", got, notLocalBefore+1)
	}
	if cp.forwardMetadataCancel(server.BackendKey{Pid: 1, SecretKey: 2}) {
		t.Fatal("unknown key must fall through to the local cancel handler")
	}
}
