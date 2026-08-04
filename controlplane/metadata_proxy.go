package controlplane

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/posthog/duckgres/server"
)

const (
	metadataProxyDatabase                = "metadata"
	metadataProxyUpstreamApplicationName = "duckgres-metadata-proxy"
	metadataProxyBootstrapTimeout        = 10 * time.Second
)

type metadataProxyCancelTarget struct {
	frontend net.Conn
	upstream net.Conn
}

type metadataPostgresConn interface {
	SyncConn(context.Context) error
	Hijack() (*pgconn.HijackedConn, error)
	Close(context.Context) error
}

type metadataProxySessionRegistry struct {
	mu        sync.Mutex
	maxPerOrg int
	byOrg     map[string]map[net.Conn]string
	closed    bool
}

func (r *metadataProxySessionRegistry) Register(orgID, username string, conn net.Conn) (func(), bool) {
	if r == nil || conn == nil {
		return nil, false
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, false
	}
	sessions := r.byOrg[orgID]
	if len(sessions) >= r.maxPerOrg {
		r.mu.Unlock()
		return nil, false
	}
	if sessions == nil {
		sessions = make(map[net.Conn]string)
		r.byOrg[orgID] = sessions
	}
	sessions[conn] = username
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		if sessions := r.byOrg[orgID]; sessions != nil {
			delete(sessions, conn)
			if len(sessions) == 0 {
				delete(r.byOrg, orgID)
			}
		}
		r.mu.Unlock()
	}, true
}

func (r *metadataProxySessionRegistry) KillUser(orgID, username string) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	var conns []net.Conn
	for conn, sessionUser := range r.byOrg[orgID] {
		if sessionUser == username {
			conns = append(conns, conn)
			delete(r.byOrg[orgID], conn)
		}
	}
	if len(r.byOrg[orgID]) == 0 {
		delete(r.byOrg, orgID)
	}
	r.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return len(conns)
}

func (r *metadataProxySessionRegistry) KillAll() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	r.closed = true
	var conns []net.Conn
	for orgID, sessions := range r.byOrg {
		for conn := range sessions {
			conns = append(conns, conn)
		}
		delete(r.byOrg, orgID)
	}
	r.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return len(conns)
}

// tryMetadataProxy handles every connection whose SNI matches the metadata
// suffix, including denials. Such connections must never fall through to the
// DuckDB worker path.
func (cp *ControlPlane) tryMetadataProxy(
	ctx context.Context,
	tlsConn net.Conn,
	reader *bufio.Reader,
	writer *bufio.Writer,
	sni, username, database, password string,
) bool {
	prefix, matched := cp.extractMetadataOrgFromSNI(sni)
	if !matched {
		return false
	}

	attemptMetrics := newMetadataProxyAttemptMetrics()
	defer attemptMetrics.Finish(metadataProxyOutcomeUnavailable)
	finish := func(outcome string) bool {
		attemptMetrics.Finish(outcome)
		return true
	}
	fail := func(outcome, code, message string) bool {
		attemptMetrics.Finish(outcome)
		_ = server.WriteErrorResponse(writer, "FATAL", code, message)
		_ = writer.Flush()
		return true
	}
	if cp.configStore == nil || cp.metadataPostgresURL == nil {
		return fail(metadataProxyOutcomeUnavailable, "08006", "metadata endpoint is unavailable")
	}
	metadataStore, supportsMetadataProxy := cp.configStore.(interface {
		ResolveMetadataProxyConnection(prefix, username, password string) (orgID string, enabled, authenticated bool)
	})
	if !supportsMetadataProxy {
		return fail(metadataProxyOutcomeUnavailable, "08006", "metadata endpoint is unavailable")
	}
	orgID, enabled, authenticated := metadataStore.ResolveMetadataProxyConnection(prefix, username, password)
	attemptMetrics.SetOrg(orgID)
	if orgID == "" || !enabled {
		return fail(metadataProxyOutcomeUnavailable, "08006", "metadata endpoint is unavailable")
	}
	if database != metadataProxyDatabase {
		return fail(metadataProxyOutcomeInvalidDatabase, "3D000", `database does not exist; connect with dbname="metadata"`)
	}
	if username != "root" || !authenticated {
		server.RecordFailedAuthAttempt(cp.rateLimiter, tlsConn.RemoteAddr())
		return fail(metadataProxyOutcomeAuthFailed, "28P01", "password authentication failed")
	}
	if cp.isDraining() {
		return fail(metadataProxyOutcomeDraining, "57P03", "control plane is draining, retry shortly")
	}
	if cp.metadataSessions == nil {
		return fail(metadataProxyOutcomeUnavailable, "08006", "metadata endpoint is unavailable")
	}
	releaseSession, admitted := cp.metadataSessions.Register(orgID, username, tlsConn)
	if !admitted {
		return fail(metadataProxyOutcomeCapacity, "53300", "too many metadata connections for this organization")
	}
	defer releaseSession()
	sessionStarted := time.Now()
	releaseConnectionMetrics := beginMetadataProxyConnection(orgID)
	established := false
	defer func() {
		releaseConnectionMetrics()
		if established {
			slog.Info("Metadata proxy session closed.",
				"org", orgID,
				"user", username,
				"duration_ms", time.Since(sessionStarted).Milliseconds())
		}
	}()

	// Target resolution may read Kubernetes resources and the upstream connect
	// includes DNS, TCP, TLS, and Postgres authentication. Bound the whole
	// bootstrap; established relay traffic deliberately does not inherit this
	// deadline.
	bootstrapCtx, cancelBootstrap := context.WithTimeout(ctx, metadataProxyBootstrapTimeout)
	defer cancelBootstrap()

	upstreamURL, err := cp.metadataPostgresURL(bootstrapCtx, orgID)
	if err != nil {
		// Resolver implementations can wrap secret or connection data. The
		// bounded outcome identifies the failure without serializing that
		// untrusted error into logs.
		slog.Error("Metadata proxy target resolution failed.",
			"org", orgID,
			"error_type", fmt.Sprintf("%T", err))
		return fail(metadataProxyOutcomeTargetResolutionError, "08006", "metadata endpoint is unavailable")
	}
	upstreamURL, err = metadataProxyTaggedUpstreamURL(upstreamURL)
	if err != nil {
		// net/url parse errors can contain the full input URL, including its
		// password. Never attach that error (or the URL) to the log record.
		slog.Error("Metadata proxy target URL was invalid.", "org", orgID)
		return fail(metadataProxyOutcomeTargetResolutionError, "08006", "metadata endpoint is unavailable")
	}
	var pgConn metadataPostgresConn
	upstreamConnectStarted := time.Now()
	if cp.metadataPostgresConnect != nil {
		pgConn, err = cp.metadataPostgresConnect(bootstrapCtx, upstreamURL)
	} else {
		pgConn, err = pgconn.Connect(bootstrapCtx, upstreamURL)
	}
	if err != nil {
		observeMetadataProxyUpstreamConnect(orgID, metadataProxyUpstreamOutcomeError, upstreamConnectStarted)
		// pgconn's ConnectError omits the password and ParseConfigError redacts
		// it. The type-gated helper degrades injected/custom errors to their
		// type so a connector can never smuggle the DSN into logs.
		slog.Error("Metadata proxy upstream connection failed.",
			"org", orgID,
			"error", metadataProxySafeConnectError(err))
		return fail(metadataProxyOutcomeUpstreamConnectError, "08006", "metadata endpoint is unavailable")
	}
	observeMetadataProxyUpstreamConnect(orgID, metadataProxyUpstreamOutcomeSuccess, upstreamConnectStarted)
	if err := pgConn.SyncConn(bootstrapCtx); err != nil {
		slog.Error("Metadata proxy upstream synchronization failed.",
			"org", orgID,
			"error", metadataProxySafeConnectError(err))
		_ = pgConn.Close(bootstrapCtx)
		return fail(metadataProxyOutcomeUpstreamSyncError, "08006", "metadata endpoint is unavailable")
	}
	hijacked, err := pgConn.Hijack()
	if err != nil {
		slog.Error("Metadata proxy upstream hijack failed.",
			"org", orgID,
			"error", metadataProxySafeConnectError(err))
		_ = pgConn.Close(bootstrapCtx)
		return fail(metadataProxyOutcomeUpstreamHijackError, "08006", "metadata endpoint is unavailable")
	}
	cancelBootstrap()
	defer func() { _ = hijacked.Conn.Close() }()

	target := metadataProxyCancelTarget{
		frontend: tlsConn,
		upstream: hijacked.Conn,
	}
	key, err := cp.registerMetadataCancel(target)
	if err != nil {
		slog.Error("Metadata proxy could not allocate a cancel key.", "org", orgID, "error", err)
		return fail(metadataProxyOutcomeCancelKeyError, "08006", "metadata endpoint is unavailable")
	}
	defer cp.proxyCancels.Delete(key)

	if err := server.WriteAuthOK(writer); err != nil {
		slog.Info("Metadata proxy frontend handshake ended before authentication response.", "org", orgID, "error", err)
		return finish(metadataProxyOutcomeHandshakeError)
	}
	for name, value := range hijacked.ParameterStatuses {
		if err := server.WriteParameterStatus(writer, name, value); err != nil {
			slog.Info("Metadata proxy frontend handshake ended while writing parameter status.", "org", orgID, "error", err)
			return finish(metadataProxyOutcomeHandshakeError)
		}
	}
	if err := server.WriteBackendKeyData(writer, key.Pid, key.SecretKey); err != nil {
		slog.Info("Metadata proxy frontend handshake ended while writing cancel key.", "org", orgID, "error", err)
		return finish(metadataProxyOutcomeHandshakeError)
	}
	if err := server.WriteReadyForQuery(writer, hijacked.TxStatus); err != nil {
		slog.Info("Metadata proxy frontend handshake ended before readiness.", "org", orgID, "error", err)
		return finish(metadataProxyOutcomeHandshakeError)
	}
	if err := writer.Flush(); err != nil {
		slog.Info("Metadata proxy frontend handshake flush failed.", "org", orgID, "error", err)
		return finish(metadataProxyOutcomeHandshakeError)
	}
	server.RecordSuccessfulAuthAttempt(cp.rateLimiter, tlsConn.RemoteAddr())
	attemptMetrics.Finish(metadataProxyOutcomeSuccess)
	established = true

	slog.Info("Metadata proxy session established.", "org", orgID, "user", username)
	type relayResult struct {
		direction string
		err       error
	}
	relays := make(chan relayResult, 2)
	go func() {
		_, err := io.Copy(
			metadataProxyTrafficWriter(hijacked.Conn, orgID, metadataProxyDirectionClientToUpstream),
			reader,
		)
		relays <- relayResult{direction: metadataProxyDirectionClientToUpstream, err: err}
	}()
	go func() {
		_, err := io.Copy(
			metadataProxyTrafficWriter(tlsConn, orgID, metadataProxyDirectionUpstreamToClient),
			hijacked.Conn,
		)
		relays <- relayResult{direction: metadataProxyDirectionUpstreamToClient, err: err}
	}()
	recheck := time.NewTicker(5 * time.Second)
	defer recheck.Stop()
	for {
		select {
		case relay := <-relays:
			slog.Info("Metadata proxy relay ended.",
				"org", orgID,
				"user", username,
				"direction", relay.direction,
				"error", relay.err)
			return true
		case <-recheck.C:
			enabledStore, ok := cp.configStore.(interface {
				MetadataProxySessionAllowed(orgID, username string) bool
			})
			if !ok || !enabledStore.MetadataProxySessionAllowed(orgID, username) {
				slog.Info("Metadata proxy session revoked.", "org", orgID, "user", username)
				return true
			}
		}
	}
}

func metadataProxyTaggedUpstreamURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", errors.New("metadata proxy upstream URL must use postgres")
	}
	query := parsed.Query()
	// This fixed internal tag intentionally ignores the client startup
	// application_name. It distinguishes proxy sessions from DuckDB
	// postgres_scanner sessions in pg_stat_activity without forwarding any
	// client-controlled value to the privileged upstream connection.
	query.Set("application_name", metadataProxyUpstreamApplicationName)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func metadataProxySafeConnectError(err error) string {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return connectErr.Error()
	}
	var parseErr *pgconn.ParseConfigError
	if errors.As(err, &parseErr) {
		return parseErr.Error()
	}
	return fmt.Sprintf("%T", err)
}

func (cp *ControlPlane) registerMetadataCancel(target metadataProxyCancelTarget) (server.BackendKey, error) {
	var raw [8]byte
	for range 16 {
		if _, err := rand.Read(raw[:]); err != nil {
			return server.BackendKey{}, err
		}
		// Use the high positive PID range for broad client compatibility while
		// staying away from normal OS and Duckgres session PIDs. The random
		// secret still makes the full key collision-resistant.
		key := server.BackendKey{
			Pid:       int32(binary.BigEndian.Uint32(raw[:4])&0x3fffffff | 0x40000000),
			SecretKey: int32(binary.BigEndian.Uint32(raw[4:])),
		}
		if _, loaded := cp.proxyCancels.LoadOrStore(key, target); !loaded {
			return key, nil
		}
	}
	return server.BackendKey{}, errors.New("could not allocate unique metadata proxy cancel key")
}

func (cp *ControlPlane) forwardMetadataCancel(key server.BackendKey) bool {
	raw, ok := cp.proxyCancels.LoadAndDelete(key)
	if !ok {
		// CancelRequest is a fresh raw TCP connection and the NLB may route it
		// to a different control-plane replica than the established session.
		// Synthetic high-range PIDs belong to this proxy, so absorb a miss and
		// expose it instead of falling through to the normal local query map.
		if key.Pid >= 0x40000000 {
			metadataProxyCancelRequestsCounter.WithLabelValues(metadataProxyCancelOutcomeNotLocal).Inc()
			return true
		}
		return false
	}
	target := raw.(metadataProxyCancelTarget)
	// PgBouncer cancel keys are instance-local. Re-dialing the pooler Service
	// can reach the wrong pod, so terminate the exact established frontend and
	// upstream sockets instead. This intentionally cancels the whole metadata
	// session, not just its current statement.
	_ = target.frontend.Close()
	_ = target.upstream.Close()
	metadataProxyCancelRequestsCounter.WithLabelValues(metadataProxyCancelOutcomeSessionTerminated).Inc()
	return true
}
