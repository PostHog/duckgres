package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
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
	peerProbeSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_probes_skipped_total",
		Help: "Summary-mode confirmations skipped because the per-pod HTTP probe budget is exhausted",
	})
)

// Peer timeouts. The /cache/has probe is a tiny HEAD-like check, so it gets a
// short WHOLE-request budget. The /cache/get transfer can move many MB of a
// Parquet range over the VPC at whatever the link currently allows, so it has
// NO whole-request timeout — a large healthy transfer must not be killed for
// being large (that silently downgraded hits into full S3 refetches). Its
// guard is a response-header deadline long enough to also cover the bounded
// wait a peer does when we ask about its in-flight fill.
const (
	peerHasTimeout               = 1 * time.Second
	peerGetResponseHeaderLimit   = peerFillWait + 2*time.Second
	defaultPeerMaxProbes         = 5
	defaultMaxPeerProbesInFlight = 64
)

type SummaryConfig struct {
	PeerMaxProbes         int
	MaxPeerProbesInFlight int
	MemoryLimitBytes      int64
}

// PeerManager discovers and communicates with cache proxy peers
// via a Kubernetes headless Service.
type PeerManager struct {
	serviceName               string
	peerPort                  string       // port for peer API (e.g. ":8081")
	client                    *http.Client // /cache/has probes (short whole-request timeout)
	streamClient              *http.Client // /cache/get transfers (header deadline, no body timeout)
	summaryClient             *http.Client // bounded /cache/summary pulls
	summaryPullCycleTimeout   time.Duration
	lookupMode                peerLookupMode
	identity                  string
	peerMaxProbes             int
	probePermits              chan struct{}
	summaryMemoryLimitBytes   int64
	summaryRemoteMemoryBudget int

	summaries         summaryStore
	summaryMu         sync.Mutex // protects the current local summary and ETag
	localSummary      []byte
	localSummaryETag  string
	syncCancel        context.CancelFunc
	syncDone          chan struct{}
	membershipChanged chan struct{}
	pullOrderMu       sync.Mutex
	nextPullOffset    int

	mu           sync.RWMutex
	peers        []string // all discovered peer addresses (ip:port)
	summaryPeers []string // deterministic receiver-selected subset to pull
	// pendingSummaryPeers are newly selected peers awaiting one priority pull.
	// Membership signals are coalesced, so the queue itself retains every peer.
	pendingSummaryPeers []string
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
		summaryClient: &http.Client{
			Timeout: summaryPullTimeout,
			Transport: &http.Transport{
				DialContext:            (&net.Dialer{Timeout: 200 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConnsPerHost:    maxSummaryPulls,
				IdleConnTimeout:        30 * time.Second,
				ResponseHeaderTimeout:  500 * time.Millisecond,
				MaxResponseHeaderBytes: maxSummaryResponseHeaderBytes,
			},
		},
		summaryPullCycleTimeout:   defaultSummaryPullCycleTimeout,
		lookupMode:                peerLookupProbe,
		peerMaxProbes:             defaultPeerMaxProbes,
		probePermits:              make(chan struct{}, defaultMaxPeerProbesInFlight),
		summaryMemoryLimitBytes:   defaultSummaryMemoryLimitBytes,
		summaryRemoteMemoryBudget: summaryRemoteMemoryBudget(defaultSummaryMemoryLimitBytes),
		summaries:                 summaryStore{records: make(map[string]summaryRecord)},
		membershipChanged:         make(chan struct{}, 1),
	}
}

func (pm *PeerManager) ConfigureSummary(mode peerLookupMode, identity string, config SummaryConfig) {
	pm.lookupMode = mode
	pm.identity = identity
	if config.PeerMaxProbes > 0 {
		pm.peerMaxProbes = config.PeerMaxProbes
	}
	if config.MaxPeerProbesInFlight > 0 {
		pm.probePermits = make(chan struct{}, config.MaxPeerProbesInFlight)
	}
	if config.MemoryLimitBytes > 0 {
		pm.summaryMemoryLimitBytes = config.MemoryLimitBytes
		pm.summaryRemoteMemoryBudget = summaryRemoteMemoryBudget(config.MemoryLimitBytes)
	}
	pm.updateSummaryGauges()
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
	seen := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if mySet[ip] {
			continue // skip self
		}
		peer := net.JoinHostPort(ip, port)
		if _, exists := seen[peer]; exists {
			continue
		}
		seen[peer] = struct{}{}
		peers = append(peers, peer)
	}
	sort.Strings(peers)
	pm.updateResolvedPeers(peers, time.Now())

	if len(peers) > 0 {
		slog.Debug("Discovered peers.", "count", len(peers), "peers", peers)
	}
}

func (pm *PeerManager) updateResolvedPeers(peers []string, now time.Time) {
	selected := pm.selectSummaryPeers(peers)
	selectedSet := stringSet(selected)

	pm.mu.Lock()
	oldSelected := stringSet(pm.summaryPeers)
	pendingSet := stringSet(pm.pendingSummaryPeers)
	pending := make([]string, 0, len(pm.pendingSummaryPeers)+len(selected))
	for _, peer := range pm.pendingSummaryPeers {
		if _, stillSelected := selectedSet[peer]; stillSelected {
			pending = append(pending, peer)
		}
	}
	added := false
	for _, peer := range selected {
		if _, existed := oldSelected[peer]; existed {
			continue
		}
		if _, queued := pendingSet[peer]; !queued {
			pending = append(pending, peer)
			pendingSet[peer] = struct{}{}
		}
		added = true
	}
	pm.peers = append([]string(nil), peers...)
	pm.summaryPeers = selected
	pm.pendingSummaryPeers = pending
	pm.mu.Unlock()
	pm.summaries.retainPeers(selectedSet, now)
	pm.updateSummaryGauges()
	if pm.lookupMode == peerLookupSummary && added {
		pm.signalMembershipChanged()
	}
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func (pm *PeerManager) selectSummaryPeers(peers []string) []string {
	if pm.lookupMode != peerLookupSummary || pm.summaryRemoteMemoryBudget <= 0 {
		return nil
	}
	maxPeers := pm.summaryRemoteMemoryBudget / int(summaryBloomBits/8)
	if maxPeers <= 0 {
		return nil
	}
	type rankedPeer struct {
		addr string
		rank [sha256.Size]byte
	}
	ranked := make([]rankedPeer, 0, len(peers))
	for _, peer := range peers {
		ranked = append(ranked, rankedPeer{addr: peer, rank: sha256.Sum256([]byte(pm.identity + "\x00" + peer))})
	}
	sort.Slice(ranked, func(i, j int) bool { return bytes.Compare(ranked[i].rank[:], ranked[j].rank[:]) < 0 })
	if maxPeers > len(ranked) {
		maxPeers = len(ranked)
	}
	selected := make([]string, maxPeers)
	for i := range selected {
		selected[i] = ranked[i].addr
	}
	return selected
}

func (pm *PeerManager) refreshSummarySelection() {
	pm.mu.RLock()
	peers := append([]string(nil), pm.peers...)
	pm.mu.RUnlock()
	selected := pm.selectSummaryPeers(peers)
	pm.mu.Lock()
	pm.summaryPeers = selected
	pm.mu.Unlock()
	pm.summaries.retainPeers(stringSet(selected), time.Now())
	pm.updateSummaryGauges()
}

func (pm *PeerManager) signalMembershipChanged() {
	select {
	case pm.membershipChanged <- struct{}{}:
	default:
	}
}

func (pm *PeerManager) isSummaryPeer(peer string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, selected := range pm.summaryPeers {
		if selected == peer {
			return true
		}
	}
	return false
}

func (pm *PeerManager) localSummarySnapshot() ([]byte, string) {
	pm.summaryMu.Lock()
	defer pm.summaryMu.Unlock()
	return pm.localSummary, pm.localSummaryETag
}

func (pm *PeerManager) receivePulledSummary(sender string, body []byte, etag string, now time.Time) error {
	err := pm.summaries.receive(sender, body, etag, now, pm.isSummaryPeer, pm.summaryRemoteMemoryBudget)
	if err != nil {
		return err
	}
	// Membership may change after the initial admission check but before the
	// store replacement. Revalidate and undo that replacement immediately.
	if !pm.isSummaryPeer(sender) {
		pm.summaries.removePeer(sender)
		pm.updateSummaryGauges()
		return errors.New("summary sender no longer selected")
	}
	pm.updateSummaryGauges()
	return nil
}

func (pm *PeerManager) updateSummaryGauges() {
	pm.summaries.mu.RLock()
	n, b := len(pm.summaries.records), pm.summaries.bytes
	pm.summaries.mu.RUnlock()
	accountedBytes := int64(0)
	effectiveLimit := int64(0)
	if pm.lookupMode == peerLookupSummary {
		// The ceiling reserves the fixed local counting Bloom plus the maximum
		// concurrent snapshot/pull working set before admitting remote summaries.
		// Report that same conservative accounting so resident/limit is alertable.
		accountedBytes = summaryMemoryReserveBytes() + int64(b)
		effectiveLimit = pm.summaryMemoryLimitBytes
	} else {
		n = 0
	}
	summaryResidentCount.Set(float64(n))
	summaryResidentBytes.Set(float64(accountedBytes))
	summaryMemoryLimitBytes.Set(float64(effectiveLimit))
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
	}
	return positive, uncovered
}

// StartSummarySynchronizer builds the local immutable summary and owns bounded,
// receiver-driven pulls of the peer summaries selected by this pod's memory
// budget. A buffered membership signal coalesces changes instead of queuing
// unbounded sync work.
func (pm *PeerManager) StartSummarySynchronizer(ctx context.Context, store *DiskCache) {
	if pm.lookupMode != peerLookupSummary {
		return
	}
	ctx, pm.syncCancel = context.WithCancel(ctx)
	pm.syncDone = make(chan struct{})
	pm.refreshSummarySelection()
	go func() {
		defer close(pm.syncDone)
		cycleStarted := time.Now()
		cycleInterval := jitteredSummaryInterval(cycleStarted)
		pm.buildLocalSummary(store)
		pm.pullSummaries(ctx, pm.summaryPeerSnapshot())
		for {
			delay := remainingSummaryCycleDelay(cycleStarted, time.Now(), cycleInterval)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-pm.membershipChanged:
				timer.Stop()
				pm.pullPendingSummaryPeers(ctx)
			case <-timer.C:
				cycleStarted = time.Now()
				cycleInterval = jitteredSummaryInterval(cycleStarted)
				pm.buildLocalSummary(store)
				pm.pullSummaries(ctx, pm.summaryPeerSnapshot())
			}
		}
	}()
}

func jitteredSummaryInterval(now time.Time) time.Duration {
	// Per-process jitter avoids a fleet-wide synchronized pull burst.
	return defaultSummaryInterval + time.Duration(now.UnixNano()%int64(defaultSummaryInterval/5))
}

func remainingSummaryCycleDelay(started, now time.Time, interval time.Duration) time.Duration {
	remaining := interval - now.Sub(started)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (pm *PeerManager) StopSummarySynchronizer() {
	if pm.syncCancel != nil {
		pm.syncCancel()
	}
	if pm.syncDone != nil {
		<-pm.syncDone
	}
}

func (pm *PeerManager) buildLocalSummary(store *DiskCache) {
	_, bits, ok := store.SummarySnapshot()
	if !ok {
		summaryServesTotal.WithLabelValues("summary_index_unavailable").Inc()
		return
	}
	s, err := newIncrementalCacheSummary(bits, time.Now(), defaultSummaryTTL)
	if err != nil {
		summaryServesTotal.WithLabelValues("build_error").Inc()
		return
	}
	body, err := s.MarshalBinary()
	if err != nil {
		summaryServesTotal.WithLabelValues("build_error").Inc()
		return
	}
	digest := sha256.Sum256(body)
	etag := fmt.Sprintf("\"%x\"", digest[:16])
	pm.summaryMu.Lock()
	// Assign a fresh immutable backing slice. GET handlers can safely retain the
	// old slice after releasing summaryMu while the next snapshot is built.
	pm.localSummary = body
	pm.localSummaryETag = etag
	pm.summaryMu.Unlock()
}

func (pm *PeerManager) summaryPeerSnapshot() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return append([]string(nil), pm.summaryPeers...)
}

func (pm *PeerManager) pullSummaries(ctx context.Context, peers []string) {
	if len(peers) == 0 {
		return
	}
	pm.pullOrderMu.Lock()
	start := pm.nextPullOffset % len(peers)
	pm.pullOrderMu.Unlock()
	ordered := make([]string, len(peers))
	for i := range peers {
		ordered[i] = peers[(start+i)%len(peers)]
	}
	started := pm.pullSummaryBatch(ctx, ordered)
	pm.advancePullOffset(start, len(started), len(peers))
}

type summaryPullJob struct {
	peer    string
	started chan bool
}

func (pm *PeerManager) pullSummaryBatch(ctx context.Context, ordered []string) []string {
	if len(ordered) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, pm.summaryPullCycleTimeout)
	defer cancel()
	jobs := make(chan summaryPullJob)
	var workers sync.WaitGroup
	workerCount := min(maxSummaryPulls, len(ordered))
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					job.started <- false
					continue
				}
				job.started <- true
				pm.pullSummary(ctx, job.peer)
			}
		}()
	}
	started := make([]string, 0, len(ordered))
	dispatching := true
	for _, peer := range ordered {
		ack := make(chan bool, 1)
		select {
		case jobs <- summaryPullJob{peer: peer, started: ack}:
			if <-ack {
				started = append(started, peer)
				continue
			}
			dispatching = false
		case <-ctx.Done():
			dispatching = false
		}
		if !dispatching {
			break
		}
	}
	close(jobs)
	workers.Wait()
	return started
}

func (pm *PeerManager) pendingSummaryPeerSnapshot() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return append([]string(nil), pm.pendingSummaryPeers...)
}

func (pm *PeerManager) pullPendingSummaryPeers(ctx context.Context) {
	pending := pm.pendingSummaryPeerSnapshot()
	if len(pending) == 0 {
		return
	}
	started := stringSet(pm.pullSummaryBatch(ctx, pending))
	pm.mu.Lock()
	remaining := pm.pendingSummaryPeers[:0]
	for _, peer := range pm.pendingSummaryPeers {
		if _, attempted := started[peer]; !attempted {
			remaining = append(remaining, peer)
		}
	}
	pm.pendingSummaryPeers = remaining
	hasRemaining := len(remaining) > 0
	pm.mu.Unlock()
	if hasRemaining && ctx.Err() == nil {
		pm.signalMembershipChanged()
	}
}

func (pm *PeerManager) advancePullOffset(start, submitted, peerCount int) {
	if peerCount == 0 {
		return
	}
	pm.pullOrderMu.Lock()
	pm.nextPullOffset = (start + submitted) % peerCount
	pm.pullOrderMu.Unlock()
}

func (pm *PeerManager) pullSummary(ctx context.Context, peer string) {
	ctx, cancel := context.WithTimeout(ctx, summaryPullTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+peer+"/cache/summary", nil)
	if err != nil {
		summaryPullsTotal.WithLabelValues("error").Inc()
		return
	}
	if etag := pm.summaries.etag(peer, time.Now()); etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := pm.summaryClient.Do(req)
	if err != nil {
		summaryPullsTotal.WithLabelValues(peerProbeErrorOutcome(err)).Inc()
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotModified {
		summaryPullsTotal.WithLabelValues("not_modified").Inc()
		return
	}
	if resp.StatusCode != http.StatusOK {
		summaryPullsTotal.WithLabelValues("status_error").Inc()
		return
	}
	if resp.ContentLength > maxSummaryBodyBytes {
		summaryPullsTotal.WithLabelValues("oversized").Inc()
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSummaryBodyBytes+1))
	if err != nil {
		summaryPullsTotal.WithLabelValues("read_error").Inc()
		return
	}
	if len(body) > maxSummaryBodyBytes {
		summaryPullsTotal.WithLabelValues("oversized").Inc()
		return
	}
	if err := pm.receivePulledSummary(peer, body, resp.Header.Get("ETag"), time.Now()); err != nil {
		summaryPullsTotal.WithLabelValues("rejected").Inc()
		return
	}
	summaryPullsTotal.WithLabelValues("success").Inc()
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
	return pm.locateKey(ctx, cacheKey, pm.peerSnapshot(), true, false, false)
}

// LocateSummaryKey uses Bloom filters only to eliminate definite negatives.
// It confirms a bounded mix of positive and uncovered peers in parallel, then
// returns the first exact 200/202 claim for one subsequent body GET.
func (pm *PeerManager) LocateSummaryKey(ctx context.Context, cacheKey string, positive, uncovered []string, maxProbes int) (holder string, flight, ok bool, selected int) {
	peers := selectSummaryProbePeers(cacheKey, positive, uncovered, maxProbes)
	holder, flight, ok = pm.locateKey(ctx, cacheKey, peers, false, true, true)
	return holder, flight, ok, len(peers)
}

func selectSummaryProbePeers(cacheKey string, positive, uncovered []string, maxProbes int) []string {
	if maxProbes <= 0 {
		return nil
	}
	if maxProbes == 1 && len(positive) > 0 && len(uncovered) > 0 {
		combined := make([]string, 0, len(positive)+len(uncovered))
		combined = append(combined, positive...)
		combined = append(combined, uncovered...)
		return selectProbePeers(cacheKey, combined, 1)
	}
	rankedPositive := selectProbePeers(cacheKey, positive, len(positive))
	rankedUncovered := selectProbePeers(cacheKey, uncovered, len(uncovered))
	positiveLimit := min(len(rankedPositive), maxProbes)
	if len(rankedPositive) > 0 && len(rankedUncovered) > 0 && maxProbes > 1 {
		positiveLimit = min(positiveLimit, maxProbes-1)
	}
	selected := append([]string(nil), rankedPositive[:positiveLimit]...)
	uncoveredLimit := min(len(rankedUncovered), maxProbes-len(selected))
	selected = append(selected, rankedUncovered[:uncoveredLimit]...)
	if len(selected) < maxProbes && positiveLimit < len(rankedPositive) {
		remaining := min(len(rankedPositive)-positiveLimit, maxProbes-len(selected))
		selected = append(selected, rankedPositive[positiveLimit:positiveLimit+remaining]...)
	}
	return selected
}

func selectProbePeers(cacheKey string, peers []string, maxProbes int) []string {
	if maxProbes <= 0 || len(peers) == 0 {
		return nil
	}
	type rankedPeer struct {
		addr string
		rank [sha256.Size]byte
	}
	ranked := make([]rankedPeer, 0, len(peers))
	for _, peer := range peers {
		ranked = append(ranked, rankedPeer{addr: peer, rank: sha256.Sum256([]byte(cacheKey + "\x00" + peer))})
	}
	sort.Slice(ranked, func(i, j int) bool { return bytes.Compare(ranked[i].rank[:], ranked[j].rank[:]) < 0 })
	if maxProbes > len(ranked) {
		maxProbes = len(ranked)
	}
	selected := make([]string, maxProbes)
	for i := range selected {
		selected[i] = ranked[i].addr
	}
	return selected
}

func (pm *PeerManager) locateKey(ctx context.Context, cacheKey string, peers []string, countLogical, useProbePermits, firstUseful bool) (holder string, flight, ok bool) {
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
			if useProbePermits {
				select {
				case pm.probePermits <- struct{}{}:
					defer func() { <-pm.probePermits }()
				default:
					peerProbeSkippedTotal.Inc()
					resCh <- probeResult{addr: addr, status: -1}
					return
				}
			}
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
			if firstUseful {
				cancel()
				go func(remaining int) {
					defer span.End()
					for range remaining {
						recordProbe(<-resCh)
					}
				}(len(peers) - i - 1)
				return res.addr, true, true
			}
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
