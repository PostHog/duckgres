package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	peerFetchesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_fetches_total",
		Help: "Total peer cache fetch attempts",
	})
	peerHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_hits_total",
		Help: "Total successful peer cache hits",
	})
	peerProbesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_peer_probes_total",
		Help: "Physical peer availability probe attempts, by outcome",
	}, []string{"outcome"}) // hit, miss, timeout, canceled, error
)

// Peer timeouts. The /cache/has probe is a tiny HEAD-like check, so it gets a
// short WHOLE-request budget. The /cache/get transfer can move many MB of a
// Parquet range over the VPC at whatever the link currently allows, so it has
// NO whole-request timeout — a large healthy transfer must not be killed for
// being large (that silently downgraded hits into full S3 refetches). Its
// guard is a response-header deadline long enough to also cover the bounded
// wait a peer does when we ask about its in-flight fill.
const (
	peerHasTimeout             = 1 * time.Second
	peerGetResponseHeaderLimit = peerFillWait + 2*time.Second
)

// PeerManager discovers and communicates with cache proxy peers
// via a Kubernetes headless Service.
type PeerManager struct {
	serviceName  string
	peerPort     string       // port for peer API (e.g. ":8081")
	client       *http.Client // /cache/has probes (short whole-request timeout)
	streamClient *http.Client // /cache/get transfers (header deadline, no body timeout)
	lookupMode   peerLookupMode
	identity     string

	summaries       summaryStore
	summaryMu       sync.Mutex // protects current local summary and generation
	localSummary    []byte
	generation      uint64
	publisherCancel context.CancelFunc
	pushPermits     chan struct{}
	receivePermits  chan struct{}
	backgroundCtx   context.Context

	mu    sync.RWMutex
	peers []string // peer addresses (ip:port)
}

func NewPeerManager(serviceName, peerPort string) *PeerManager {
	return &PeerManager{
		serviceName: serviceName,
		peerPort:    peerPort,
		client: &http.Client{
			Timeout: peerHasTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		streamClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       30 * time.Second,
				ResponseHeaderTimeout: peerGetResponseHeaderLimit,
			},
		},
		lookupMode:     peerLookupProbe,
		summaries:      summaryStore{records: make(map[string]summaryRecord)},
		pushPermits:    make(chan struct{}, maxSummaryPushes),
		receivePermits: make(chan struct{}, maxSummaryReceives),
	}
}

func (pm *PeerManager) ConfigureSummary(mode peerLookupMode, identity string) {
	pm.lookupMode = mode
	pm.identity = identity
}

// WatchEndpoints periodically resolves the headless Service DNS to
// discover peer cache proxy pods.
func (pm *PeerManager) WatchEndpoints(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial resolve
	pm.resolve()

	for {
		select {
		case <-ticker.C:
			pm.resolve()
		case <-ctx.Done():
			return
		}
	}
}

func (pm *PeerManager) resolve() {
	ips, err := net.LookupHost(pm.serviceName)
	if err != nil {
		slog.Debug("Peer DNS resolve failed.", "service", pm.serviceName, "error", err)
		return
	}

	// Get our own IPs to exclude self
	myIPs := getLocalIPs()
	mySet := make(map[string]bool, len(myIPs))
	for _, ip := range myIPs {
		mySet[ip] = true
	}

	// Extract port number from peerPort (e.g. ":8081" → "8081")
	port := pm.peerPort
	if len(port) > 0 && port[0] == ':' {
		port = port[1:]
	}

	var peers []string
	for _, ip := range ips {
		if mySet[ip] {
			continue // skip self
		}
		peers = append(peers, fmt.Sprintf("%s:%s", ip, port))
	}

	pm.mu.Lock()
	old := make(map[string]struct{}, len(pm.peers))
	for _, peer := range pm.peers {
		old[peer] = struct{}{}
	}
	pm.peers = peers
	pm.mu.Unlock()
	pm.summaries.removeNonMembers(pm.isMember, time.Now())
	pm.updateSummaryGauges()
	if pm.lookupMode == peerLookupSummary && len(pm.localSummaryCopy()) > 0 {
		for _, peer := range peers {
			if _, existed := old[peer]; !existed {
				pm.schedulePush(peer, pm.localSummaryCopy())
			}
		}
	}

	if len(peers) > 0 {
		slog.Debug("Discovered peers.", "count", len(peers), "peers", peers)
	}
}

func (pm *PeerManager) isMember(peer string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, p := range pm.peers {
		if p == peer {
			return true
		}
	}
	return false
}

func (pm *PeerManager) memberForRemoteAddr(remote string) (string, bool) {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return "", false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, peer := range pm.peers {
		peerHost, _, err := net.SplitHostPort(peer)
		if err == nil && peerHost == host {
			return peer, true
		}
	}
	return "", false
}

func (pm *PeerManager) localSummaryCopy() []byte {
	pm.summaryMu.Lock()
	defer pm.summaryMu.Unlock()
	return append([]byte(nil), pm.localSummary...)
}

// ReceiveSummary validates an untrusted peer payload before atomically
// replacing that peer's prior hint. It intentionally logs neither body nor
// cache keys.
func (pm *PeerManager) ReceiveSummary(sender string, body []byte, now time.Time) error {
	err := pm.summaries.receive(sender, body, now, pm.isMember)
	if err != nil {
		summaryReceiptsTotal.WithLabelValues("rejected").Inc()
		return err
	}
	summaryReceiptsTotal.WithLabelValues("accepted").Inc()
	pm.updateSummaryGauges()
	return nil
}

func (pm *PeerManager) summaryCount() int {
	pm.summaries.mu.RLock()
	defer pm.summaries.mu.RUnlock()
	return len(pm.summaries.records)
}
func (pm *PeerManager) updateSummaryGauges() {
	pm.summaries.mu.RLock()
	n, b := len(pm.summaries.records), pm.summaries.bytes
	pm.summaries.mu.RUnlock()
	summaryResidentCount.Set(float64(n))
	summaryResidentBytes.Set(float64(b))
}

func (pm *PeerManager) peerSnapshot() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return append([]string(nil), pm.peers...)
}

// SummaryLookup is local-only. Positive peers have valid summaries; uncovered
// peers have not yet supplied one and retain legacy probe behavior during
// convergence. A fully covered negative lookup never issues /cache/has.
func (pm *PeerManager) SummaryLookup(cacheKey string, now time.Time) (positive, uncovered []string) {
	members := pm.peerSnapshot()
	positive, uncovered = pm.summaries.candidates(cacheKey, members, now)
	if len(positive) == 0 && len(uncovered) == len(members) {
		summaryLookupTotal.WithLabelValues("no_valid_summary").Inc()
	} else if len(positive) == 0 {
		summaryLookupTotal.WithLabelValues("no_positive").Inc()
	} else {
		summaryLookupTotal.WithLabelValues("positive_candidate").Inc()
		if len(positive) > 2 {
			positive = positive[:2]
		}
	}
	return positive, uncovered
}

func (pm *PeerManager) SummaryCandidates(cacheKey string, now time.Time) []string {
	positive, _ := pm.SummaryLookup(cacheKey, now)
	return positive
}

// StartSummaryPublisher owns a cancellable background publisher. Publication
// work is bounded and never runs on a request goroutine.
func (pm *PeerManager) StartSummaryPublisher(ctx context.Context, store *DiskCache) {
	if pm.lookupMode != peerLookupSummary {
		return
	}
	ctx, pm.publisherCancel = context.WithCancel(ctx)
	pm.backgroundCtx = ctx
	go func() {
		pm.publish(ctx, store)
		for {
			// Deterministic jitter avoids a fleet-wide synchronized burst.
			delay := defaultSummaryInterval + time.Duration(time.Now().UnixNano()%int64(defaultSummaryInterval/5))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				pm.publish(ctx, store)
			}
		}
	}()
}

func (pm *PeerManager) StopSummaryPublisher() {
	if pm.publisherCancel != nil {
		pm.publisherCancel()
	}
}

func (pm *PeerManager) publish(ctx context.Context, store *DiskCache) {
	pm.summaries.removeNonMembers(pm.isMember, time.Now())
	pm.updateSummaryGauges()
	keys, ok := store.SnapshotKeys(maxSummaryItems)
	if !ok {
		summaryPushesTotal.WithLabelValues("snapshot_too_large").Inc()
		return
	}
	pm.summaryMu.Lock()
	pm.generation++
	generation := pm.generation
	pm.summaryMu.Unlock()
	s, err := newCacheSummary(pm.identity, generation, keys, time.Now(), defaultSummaryTTL)
	if err != nil {
		summaryPushesTotal.WithLabelValues("build_error").Inc()
		return
	}
	body, err := s.MarshalBinary()
	if err != nil {
		summaryPushesTotal.WithLabelValues("build_error").Inc()
		return
	}
	pm.summaryMu.Lock()
	pm.localSummary = append(pm.localSummary[:0], body...)
	pm.summaryMu.Unlock()
	pm.mu.RLock()
	peers := append([]string(nil), pm.peers...)
	pm.mu.RUnlock()
	jobs := make(chan string)
	var workers sync.WaitGroup
	for range maxSummaryPushes {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for peer := range jobs {
				select {
				case pm.pushPermits <- struct{}{}:
				case <-ctx.Done():
					return
				}
				pm.pushSummary(ctx, peer, body)
				<-pm.pushPermits
			}
		}()
	}
	for _, peer := range peers {
		select {
		case jobs <- peer:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		}
	}
	close(jobs)
	workers.Wait()
}

func (pm *PeerManager) pushSummary(ctx context.Context, peer string, body []byte) {
	if len(body) == 0 || len(body) > maxSummaryBodyBytes {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, summaryPushTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+peer+"/cache/summary", bytes.NewReader(body))
	if err != nil {
		summaryPushesTotal.WithLabelValues("error").Inc()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cache-Proxy-ID", pm.identity)
	resp, err := pm.client.Do(req)
	if err != nil {
		summaryPushesTotal.WithLabelValues(peerProbeErrorOutcome(err)).Inc()
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		summaryPushesTotal.WithLabelValues("success").Inc()
	} else {
		summaryPushesTotal.WithLabelValues("rejected").Inc()
	}
}

// schedulePush is deliberately lossy when all permits are busy. There is no
// retry queue; the next periodic generation retries delivery.
func (pm *PeerManager) schedulePush(peer string, body []byte) {
	select {
	case pm.pushPermits <- struct{}{}:
		ctx := pm.backgroundCtx
		if ctx == nil {
			ctx = context.Background()
		}
		go func() { defer func() { <-pm.pushPermits }(); pm.pushSummary(ctx, peer, body) }()
	default:
		summaryPushesTotal.WithLabelValues("concurrency_limited").Inc()
	}
}

// probeResult is one peer's answer to a /cache/has probe.
type probeResult struct {
	addr     string
	status   int // 200 has it · 202 mid-flight filling it · anything else: no
	err      error
	duration time.Duration
}

// LocateKey asks every peer in parallel whether it has cacheKey (200) or is
// mid-flight filling it (202), returning the first useful claim. A present
// entry wins over an in-flight one when both answer before the probes are
// cancelled; ok=false means nothing anywhere knows about the key and the
// caller should go to the origin. Cluster-wide this is what stops a cold key
// bursting into one duplicate origin fetch per node: the first node's
// in-flight fetch answers 202, so every other node waits for that fill.
func (pm *PeerManager) LocateKey(ctx context.Context, cacheKey string) (holder string, flight, ok bool) {
	return pm.locateKey(ctx, cacheKey, pm.peerSnapshot(), true)
}

// LocateKeyAmong retains probe-mode behavior for only the peers that have not
// delivered a valid summary. The caller already counted its logical lookup.
func (pm *PeerManager) LocateKeyAmong(ctx context.Context, cacheKey string, peers []string) (holder string, flight, ok bool) {
	return pm.locateKey(ctx, cacheKey, peers, false)
}

func (pm *PeerManager) locateKey(ctx context.Context, cacheKey string, peers []string, countLogical bool) (holder string, flight, ok bool) {
	if len(peers) == 0 {
		return "", false, false
	}
	if countLogical {
		peerFetchesTotal.Inc()
	}
	ctx, span := proxyTracer.Start(ctx, "cache.peer_lookup", trace.WithAttributes(
		attribute.Int("duckgres.cache.peer_count", len(peers)),
	))

	ctx, cancel := context.WithTimeout(ctx, peerHasTimeout)
	defer cancel()

	resCh := make(chan probeResult, len(peers))
	for _, addr := range peers {
		go func(addr string) {
			startedAt := time.Now()
			res := probeResult{addr: addr, status: -1}
			hasURL := fmt.Sprintf("http://%s/cache/has?key=%s", addr, cacheKey)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, hasURL, nil)
			if err != nil {
				res.err = err
				peerProbesTotal.WithLabelValues("error").Inc()
			} else {
				otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
				resp, err := pm.client.Do(req)
				if err != nil {
					res.err = err
					peerProbesTotal.WithLabelValues(peerProbeErrorOutcome(err)).Inc()
				} else {
					res.status = resp.StatusCode
					_ = resp.Body.Close()
					peerProbesTotal.WithLabelValues(peerProbeStatusOutcome(resp.StatusCode)).Inc()
				}
			}
			res.duration = time.Since(startedAt)
			resCh <- res
		}(addr)
	}

	recordProbe := func(res probeResult) {
		span.AddEvent("cache.peer_probe", trace.WithAttributes(
			attribute.String("duckgres.cache.peer.address", res.addr),
			attribute.String("duckgres.cache.peer.outcome", probeOutcome(res)),
			attribute.Int64("duckgres.cache.peer.duration_ms", res.duration.Milliseconds()),
		))
	}
	firstFlight := ""
	for i := 0; i < len(peers); i++ {
		res := <-resCh
		recordProbe(res)
		switch res.status {
		case http.StatusOK:
			cancel() // release the losing probes
			// Preserve the first-present response latency while the lookup span
			// records the canceled peers' bounded outcomes in the background.
			go func(remaining int) {
				defer span.End()
				for range remaining {
					recordProbe(<-resCh)
				}
			}(len(peers) - i - 1)
			return res.addr, false, true
		case http.StatusAccepted:
			if firstFlight == "" {
				firstFlight = res.addr
			}
		}
	}
	span.End()
	if firstFlight != "" {
		return firstFlight, true, true
	}
	return "", false, false
}

func probeOutcome(res probeResult) string {
	if res.err != nil {
		switch peerProbeErrorOutcome(res.err) {
		case "timeout":
			return "timeout"
		case "canceled":
			return "canceled"
		default:
			return "transport_error"
		}
	}
	switch res.status {
	case http.StatusOK:
		return "present"
	case http.StatusAccepted:
		return "in_flight"
	case http.StatusNotFound:
		return "negative"
	default:
		return "negative"
	}
}

func peerProbeStatusOutcome(status int) string {
	switch status {
	case http.StatusOK, http.StatusAccepted:
		return "hit"
	case http.StatusNotFound:
		return "miss"
	default:
		return "error"
	}
}

func peerProbeErrorOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return "error"
	}
}

// FetchFromPeer streams cacheKey's body from one peer into sink. flight=true
// tells the peer the key is expected from its in-flight fill: it waits
// (bounded) for the fill instead of 404ing. ok is false if the peer couldn't
// deliver the body — the caller then falls back to the origin.
func (pm *PeerManager) FetchFromPeer(ctx context.Context, holder, cacheKey string, flight bool, sink func(io.Reader) (int64, error)) (int64, bool) {
	ctx, span := proxyTracer.Start(ctx, "cache.peer_get", trace.WithAttributes(
		attribute.String("server.address", holder),
		attribute.Bool("duckgres.cache.peer_flight", flight),
	))
	defer span.End()

	getURL := fmt.Sprintf("http://%s/cache/get?key=%s", holder, cacheKey)
	if flight {
		getURL += "&flight=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, false
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := pm.streamClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
			_ = resp.Body.Close()
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Error, fmt.Sprintf("peer status %d", resp.StatusCode))
		}
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	n, err := sink(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, false
	}
	span.SetAttributes(attribute.Int64("duckgres.bytes", n))
	peerHitsTotal.Inc()
	return n, true
}

func getLocalIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var ips []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			ips = append(ips, ipnet.IP.String())
		}
	}
	return ips
}
