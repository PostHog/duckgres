package main

import (
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/semaphore"
)

var (
	peerHedgesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_peer_hedges_total",
		Help: "Origin span fetches started while every block in the span was still eligible to arrive from peers",
	})
	peerHedgeWinsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_peer_hedge_wins_total",
		Help: "Completed peer/origin races by winning side",
	}, []string{"winner"}) // peer, origin
	peerFetchCancellationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_fetch_cancellations_total",
		Help: "Hedged transfers canceled after the other side won",
	}, []string{"side"}) // peer, origin
	peerHedgeDuplicateBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_hedge_duplicate_bytes_total",
		Help: "Bytes received by a losing hedged transfer before cancellation",
	}, []string{"side"}) // peer, origin
	latePeerSuccessesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_late_peer_successes_total",
		Help: "Peer transfers that still completed successfully after origin won",
	})
	peerBreakerTransitionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_peer_breaker_transitions_total",
		Help: "Peer circuit-breaker state transitions",
	}, []string{"state"}) // open, closed
	peerBreakerState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_peer_breaker_state",
		Help: "Whether peer lookup is circuit-broken (1=open, 0=closed)",
	})
	peerFetchInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_peer_fetches_in_flight",
		Help: "Peer lookup/body transfers admitted by the process-wide limiter",
	})
	peerFetchBytesInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_peer_fetch_bytes_in_flight",
		Help: "Bytes reserved by admitted peer transfers",
	})
	peerFetchShedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_peer_fetch_shed_total",
		Help: "Peer fetches skipped before network I/O, by reason",
	}, []string{"reason"}) // deadline, capacity, canceled, breaker, unbounded, unconfigured
	peerFetchQueueDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cache_proxy_peer_fetch_queue_duration_seconds",
		Help:    "Time spent waiting for process-wide peer count and byte permits",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})
	peerFetchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_proxy_peer_fetch_duration_seconds",
		Help:    "Admitted peer lookup plus body-transfer duration",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"outcome"}) // hit, miss, canceled
	originSpanFetchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_proxy_origin_span_fetch_duration_seconds",
		Help:    "Block-aligned origin span fetch duration",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"outcome"}) // success, error, canceled
	peerHedgeHeadStartSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_peer_hedge_head_start_seconds",
		Help: "Current adaptive peer head-start before an origin hedge",
	})
	peerFetchLatencyEWMASeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_peer_fetch_latency_ewma_seconds",
		Help: "EWMA of successful peer block fetches and threshold-qualified canceled lower bounds",
	})
	originFetchLatencyEWMASeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_origin_fetch_latency_ewma_seconds",
		Help: "EWMA of time through a validated, atomically committed first origin block",
	})
)

const (
	defaultPeerFetchMaxConcurrent = int64(32)
	peerLatencyWindowSize         = 64
	defaultPeerHedgeMinDelay      = 25 * time.Millisecond
	defaultPeerHedgeMaxDelay      = 150 * time.Millisecond
	defaultPeerBreakerRatio       = 1.5
	defaultPeerBreakerOpenAfter   = 8
	defaultPeerBreakerCloseAfter  = 3
	defaultPeerBreakerProbeEvery  = 5 * time.Second
	defaultPeerBreakerMinSamples  = 8
	peerLatencyEWMAAlpha          = 0.2
)

// defaultPeerFetchMaxBytes keeps one complete block reservation available for
// every configured concurrent fetch. It follows CACHE_BLOCK_SIZE_BYTES so the
// deployed 1 MiB configuration at concurrency 32 reserves 32 MiB, while the
// source defaults reserve 256 MiB.
func defaultPeerFetchMaxBytes(maxConcurrent, blockSize int64) int64 {
	if maxConcurrent <= 0 {
		maxConcurrent = defaultPeerFetchMaxConcurrent
	}
	if blockSize <= 0 {
		blockSize = 8 << 20
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if blockSize > maxInt64/maxConcurrent {
		return maxInt64
	}
	return blockSize * maxConcurrent
}

// peerFetchLimiter enforces process-wide count and reserved-byte ceilings.
// Callers use a context whose deadline is the hedge start: a queued peer fetch
// that cannot get both permits before then is shed to origin and never starts
// later. The reservation is pessimistically one complete cache block.
type peerFetchLimiter struct {
	count    *semaphore.Weighted
	bytes    *semaphore.Weighted
	maxBytes int64

	inFlight      atomic.Int64
	bytesInFlight atomic.Int64
}

type peerFetchPermit struct {
	limiter *peerFetchLimiter
	bytes   int64
	once    sync.Once
}

type controlledPeerFetchResult struct {
	bytes    int64
	duration time.Duration
	ok       bool
	started  bool
}

type peerFetchDecision struct {
	allowed     bool
	nonBlocking bool
}

// fetchFromPeers applies the process-wide breaker and count/byte limits around
// the complete peer lookup plus body transfer. acquireDeadline is the absolute
// adaptive hedge start; a request still queued then is permanently shed.
func (p *CacheProxy) fetchFromPeers(
	ctx context.Context,
	acquireDeadline time.Time,
	reservedBytes int64,
	cacheKey string,
	sink func(io.Reader) (int64, error),
	decision peerFetchDecision,
	observeLatency bool,
	onStart func(),
) controlledPeerFetchResult {
	if p.peers == nil {
		return controlledPeerFetchResult{}
	}
	policy := p.peerPolicy
	if policy == nil {
		peerFetchShedTotal.WithLabelValues("unconfigured").Inc()
		return controlledPeerFetchResult{}
	}
	if !decision.allowed {
		peerFetchShedTotal.WithLabelValues("breaker").Inc()
		return controlledPeerFetchResult{}
	}
	if ctx.Err() != nil {
		peerFetchShedTotal.WithLabelValues("canceled").Inc()
		return controlledPeerFetchResult{}
	}

	var permit *peerFetchPermit
	var reason string
	if decision.nonBlocking {
		permit, reason = policy.limiter.tryAcquire(reservedBytes)
	} else {
		permit, reason = policy.limiter.acquireBefore(ctx, acquireDeadline, reservedBytes)
	}
	if permit == nil {
		peerFetchShedTotal.WithLabelValues(reason).Inc()
		return controlledPeerFetchResult{}
	}
	defer permit.release()
	if onStart != nil {
		onStart()
	}

	startedAt := time.Now()
	_, n, ok := p.peers.FetchFromPeers(ctx, cacheKey, sink)
	duration := time.Since(startedAt)
	if ctx.Err() != nil {
		peerFetchDuration.WithLabelValues("canceled").Observe(duration.Seconds())
	} else if observeLatency {
		policy.observePeer(duration, ok)
	} else {
		outcome := "miss"
		if ok {
			outcome = "hit"
		}
		peerFetchDuration.WithLabelValues(outcome).Observe(duration.Seconds())
	}
	return controlledPeerFetchResult{
		bytes:    n,
		duration: duration,
		ok:       ok,
		started:  true,
	}
}

func newPeerFetchLimiter(maxConcurrent, maxBytes int64) *peerFetchLimiter {
	if maxConcurrent <= 0 {
		maxConcurrent = defaultPeerFetchMaxConcurrent
	}
	if maxBytes <= 0 {
		maxBytes = 1
	}
	return &peerFetchLimiter{
		count:    semaphore.NewWeighted(maxConcurrent),
		bytes:    semaphore.NewWeighted(maxBytes),
		maxBytes: maxBytes,
	}
}

func (l *peerFetchLimiter) acquire(ctx context.Context, reservedBytes int64) (*peerFetchPermit, string) {
	queuedAt := time.Now()
	defer func() { peerFetchQueueDuration.Observe(time.Since(queuedAt).Seconds()) }()
	if reservedBytes <= 0 || reservedBytes > l.maxBytes {
		return nil, "capacity"
	}
	if err := l.count.Acquire(ctx, 1); err != nil {
		return nil, acquireFailureReason(ctx)
	}
	if err := l.bytes.Acquire(ctx, reservedBytes); err != nil {
		l.count.Release(1)
		return nil, acquireFailureReason(ctx)
	}
	return l.admit(reservedBytes), ""
}

// acquireBefore makes the absolute hedge deadline part of the limiter's own
// contract. The explicit checks prevent a late fetch even if semaphore
// cancellation semantics change or the deadline expires in the narrow gap
// after both permits are granted.
func (l *peerFetchLimiter) acquireBefore(
	ctx context.Context,
	deadline time.Time,
	reservedBytes int64,
) (*peerFetchPermit, string) {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return nil, "deadline"
	}
	queueCtx := ctx
	cancelQueue := func() {}
	if !deadline.IsZero() {
		queueCtx, cancelQueue = context.WithDeadline(ctx, deadline)
	}
	permit, reason := l.acquire(queueCtx, reservedBytes)
	cancelQueue()
	if permit == nil {
		return nil, reason
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		permit.release()
		return nil, "deadline"
	}
	return permit, ""
}

// tryAcquire is used by half-open recovery probes. A diagnostic probe must
// never queue in front of origin or delay a user request while the breaker is
// open, so it either takes both permits immediately or skips this interval.
func (l *peerFetchLimiter) tryAcquire(reservedBytes int64) (*peerFetchPermit, string) {
	if reservedBytes <= 0 || reservedBytes > l.maxBytes {
		return nil, "capacity"
	}
	if !l.count.TryAcquire(1) {
		return nil, "capacity"
	}
	if !l.bytes.TryAcquire(reservedBytes) {
		l.count.Release(1)
		return nil, "capacity"
	}
	return l.admit(reservedBytes), ""
}

func (l *peerFetchLimiter) admit(reservedBytes int64) *peerFetchPermit {
	l.inFlight.Add(1)
	l.bytesInFlight.Add(reservedBytes)
	peerFetchInFlight.Inc()
	peerFetchBytesInFlight.Add(float64(reservedBytes))
	return &peerFetchPermit{limiter: l, bytes: reservedBytes}
}

func acquireFailureReason(ctx context.Context) string {
	if ctx.Err() == context.DeadlineExceeded {
		return "deadline"
	}
	return "canceled"
}

func (p *peerFetchPermit) release() {
	if p == nil || p.limiter == nil {
		return
	}
	p.once.Do(func() {
		p.limiter.bytes.Release(p.bytes)
		p.limiter.count.Release(1)
		p.limiter.inFlight.Add(-1)
		p.limiter.bytesInFlight.Add(-p.bytes)
		peerFetchInFlight.Dec()
		peerFetchBytesInFlight.Sub(float64(p.bytes))
	})
}

func (l *peerFetchLimiter) snapshot() (int64, int64) {
	return l.inFlight.Load(), l.bytesInFlight.Load()
}

type peerFetchPolicyConfig struct {
	maxConcurrent        int64
	maxBytes             int64
	minHeadStart         time.Duration
	maxHeadStart         time.Duration
	breakerRatio         float64
	breakerOpenAfter     int
	breakerCloseAfter    int
	breakerProbeInterval time.Duration
	breakerMinSamples    int
	now                  func() time.Time
}

func defaultPeerFetchPolicyConfig(maxConcurrent, maxBytes int64) peerFetchPolicyConfig {
	if maxConcurrent <= 0 {
		maxConcurrent = defaultPeerFetchMaxConcurrent
	}
	if maxBytes <= 0 {
		maxBytes = defaultPeerFetchMaxBytes(maxConcurrent, 8<<20)
	}
	return peerFetchPolicyConfig{
		maxConcurrent:        maxConcurrent,
		maxBytes:             maxBytes,
		minHeadStart:         defaultPeerHedgeMinDelay,
		maxHeadStart:         defaultPeerHedgeMaxDelay,
		breakerRatio:         defaultPeerBreakerRatio,
		breakerOpenAfter:     defaultPeerBreakerOpenAfter,
		breakerCloseAfter:    defaultPeerBreakerCloseAfter,
		breakerProbeInterval: defaultPeerBreakerProbeEvery,
		breakerMinSamples:    defaultPeerBreakerMinSamples,
		now:                  time.Now,
	}
}

type latencyEWMA struct {
	value       float64
	initialized bool
	samples     int
}

func (e *latencyEWMA) add(d time.Duration) {
	v := d.Seconds()
	if !e.initialized {
		e.value = v
		e.initialized = true
	} else {
		e.value = peerLatencyEWMAAlpha*v + (1-peerLatencyEWMAAlpha)*e.value
	}
	e.samples++
}

// peerFetchPolicy owns the process-wide limiter, latency estimators, adaptive
// hedge delay, and circuit breaker. All mutable policy state is protected by
// mu so request goroutines observe one coherent breaker decision.
type peerFetchPolicy struct {
	limiter *peerFetchLimiter
	cfg     peerFetchPolicyConfig

	mu             sync.Mutex
	peerWindow     []time.Duration
	peerWindowNext int
	peerEWMA       latencyEWMA
	originEWMA     latencyEWMA

	open            bool
	badStreak       int
	directBadStreak int
	lastDirectBad   time.Time
	recoveryStreak  int
	probeInFlight   bool
	nextProbe       time.Time
	measureInFlight bool
	nextMeasurement time.Time
}

func newPeerFetchPolicy(cfg peerFetchPolicyConfig) *peerFetchPolicy {
	defaults := defaultPeerFetchPolicyConfig(cfg.maxConcurrent, cfg.maxBytes)
	if cfg.maxConcurrent <= 0 {
		cfg.maxConcurrent = defaults.maxConcurrent
	}
	if cfg.maxBytes <= 0 {
		cfg.maxBytes = defaults.maxBytes
	}
	if cfg.minHeadStart <= 0 {
		cfg.minHeadStart = defaults.minHeadStart
	}
	if cfg.maxHeadStart < cfg.minHeadStart {
		cfg.maxHeadStart = defaults.maxHeadStart
	}
	if cfg.breakerRatio <= 0 {
		cfg.breakerRatio = defaults.breakerRatio
	}
	if cfg.breakerOpenAfter <= 0 {
		cfg.breakerOpenAfter = defaults.breakerOpenAfter
	}
	if cfg.breakerCloseAfter <= 0 {
		cfg.breakerCloseAfter = defaults.breakerCloseAfter
	}
	if cfg.breakerProbeInterval <= 0 {
		cfg.breakerProbeInterval = defaults.breakerProbeInterval
	}
	if cfg.breakerMinSamples <= 0 {
		cfg.breakerMinSamples = defaults.breakerMinSamples
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &peerFetchPolicy{
		limiter: newPeerFetchLimiter(cfg.maxConcurrent, cfg.maxBytes),
		cfg:     cfg,
	}
}

func (p *peerFetchPolicy) headStart() time.Duration {
	p.mu.Lock()
	samples := append([]time.Duration(nil), p.peerWindow...)
	minDelay, maxDelay := p.cfg.minHeadStart, p.cfg.maxHeadStart
	p.mu.Unlock()

	delay := minDelay
	if len(samples) > 0 {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		delay = samples[len(samples)/2]
	}
	if delay < minDelay {
		delay = minDelay
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	peerHedgeHeadStartSeconds.Set(delay.Seconds())
	return delay
}

func (p *peerFetchPolicy) observePeer(d time.Duration, success bool) {
	outcome := "miss"
	if success {
		outcome = "hit"
	}
	peerFetchDuration.WithLabelValues(outcome).Observe(d.Seconds())
	if !success {
		return
	}
	p.mu.Lock()
	if len(p.peerWindow) < peerLatencyWindowSize {
		p.peerWindow = append(p.peerWindow, d)
	} else {
		p.peerWindow[p.peerWindowNext] = d
		p.peerWindowNext = (p.peerWindowNext + 1) % peerLatencyWindowSize
	}
	p.peerEWMA.add(d)
	ewma := p.peerEWMA.value
	p.mu.Unlock()
	peerFetchLatencyEWMASeconds.Set(ewma)
}

// observePeerLowerBound records only a threshold-qualified censored sample:
// origin won and cancellation proves the still-running peer had already taken
// at least d. Keeping it out of the p50 window avoids treating an incomplete
// transfer as a completed hedge-delay sample, while still letting sustained
// evidence update the breaker EWMA after an abrupt slowdown.
func (p *peerFetchPolicy) observePeerLowerBound(d time.Duration) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	p.peerEWMA.add(d)
	ewma := p.peerEWMA.value
	p.mu.Unlock()
	peerFetchLatencyEWMASeconds.Set(ewma)
}

func (p *peerFetchPolicy) observeOriginSpan(totalDuration time.Duration, success, canceled bool) {
	outcome := "error"
	if canceled {
		outcome = "canceled"
	} else if success {
		outcome = "success"
	}
	originSpanFetchDuration.WithLabelValues(outcome).Observe(totalDuration.Seconds())
}

// observeOriginFirstBlock records the peer-comparable unit from a coalesced
// origin span. Content-Range and Content-Length have already been validated,
// and exactLengthReader plus PutStream prove that the first block was committed
// atomically. A later block failing cannot invalidate this completed sample.
func (p *peerFetchPolicy) observeOriginFirstBlock(firstBlockDuration time.Duration) {
	if firstBlockDuration <= 0 {
		return
	}
	p.mu.Lock()
	p.originEWMA.add(firstBlockDuration)
	ewma := p.originEWMA.value
	p.mu.Unlock()
	originFetchLatencyEWMASeconds.Set(ewma)
}

func (p *peerFetchPolicy) comparableOriginLatency(current time.Duration) (time.Duration, bool) {
	if current > 0 {
		return current, true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.originEWMA.initialized {
		return 0, false
	}
	return time.Duration(p.originEWMA.value * float64(time.Second)), true
}

func (p *peerFetchPolicy) allowPeer(recoveryEligible bool) (allowed, recoverySample bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.open {
		return true, false
	}
	if !recoveryEligible {
		return false, false
	}
	now := p.cfg.now()
	if p.probeInFlight || now.Before(p.nextProbe) {
		return false, false
	}
	p.probeInFlight = true
	return true, true
}

func (p *peerFetchPolicy) finishProbe(recoverySample, success bool) {
	if !recoverySample {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probeInFlight = false
	p.nextProbe = p.cfg.now().Add(p.cfg.breakerProbeInterval)
	if !p.open {
		return
	}
	// The caller classifies this individual recovery sample from its actual
	// peer/origin durations. Do not re-apply the stale pre-open EWMAs here: the
	// whole purpose of the sampled probe is to discover that the regime changed.
	if success {
		p.recoveryStreak++
	} else {
		p.recoveryStreak = 0
	}
	if p.recoveryStreak >= p.cfg.breakerCloseAfter {
		p.open = false
		p.badStreak = 0
		p.directBadStreak = 0
		p.lastDirectBad = time.Time{}
		p.recoveryStreak = 0
		peerBreakerState.Set(0)
		peerBreakerTransitionsTotal.WithLabelValues("closed").Inc()
	}
}

// startAmbiguousMeasurement admits at most one closed-breaker diagnostic per
// sampling interval. It is used only when prompt loser cancellation leaves a
// peer latency sample censored below the 1.5x threshold.
func (p *peerFetchPolicy) startAmbiguousMeasurement() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.cfg.now()
	if p.open || p.measureInFlight || now.Before(p.nextMeasurement) {
		return false
	}
	p.measureInFlight = true
	return true
}

func (p *peerFetchPolicy) finishAmbiguousMeasurement() {
	p.mu.Lock()
	p.measureInFlight = false
	p.nextMeasurement = p.cfg.now().Add(p.cfg.breakerProbeInterval)
	p.mu.Unlock()
}

// recordLatencyRatioObservation evaluates the sustained ratio signal once per
// client request, regardless of how many coalesced origin spans it needed.
func (p *peerFetchPolicy) recordLatencyRatioObservation() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.open {
		return
	}
	ratioSlow := p.peerEWMA.samples >= p.cfg.breakerMinSamples &&
		p.originEWMA.samples >= p.cfg.breakerMinSamples &&
		p.peerEWMA.value > p.cfg.breakerRatio*p.originEWMA.value
	if ratioSlow {
		p.badStreak++
	} else {
		p.badStreak = 0
	}
	p.maybeOpenLocked()
}

// recordThresholdComparison tracks paired observations that prove whether a
// peer exceeded the configured origin-latency ratio. This evidence is kept
// separate from the EWMAs: a timeout exactly at the ratio boundary is censored
// proof of a slow peer, but averaging that lower bound into a formerly healthy
// EWMA could otherwise take minutes to cross the threshold. Unknown origin
// wins do not call this method and therefore do not reset the streak.
func (p *peerFetchPolicy) recordThresholdComparison(exceeded bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.open {
		return
	}
	if !exceeded {
		p.directBadStreak = 0
		p.lastDirectBad = time.Time{}
		return
	}
	now := p.cfg.now()
	if !p.lastDirectBad.IsZero() && now.Sub(p.lastDirectBad) > 2*p.cfg.breakerProbeInterval {
		p.directBadStreak = 0
	}
	p.lastDirectBad = now
	p.directBadStreak++
	p.maybeOpenLocked()
}

func (p *peerFetchPolicy) maybeOpenLocked() {
	if p.open || (p.badStreak < p.cfg.breakerOpenAfter && p.directBadStreak < p.cfg.breakerOpenAfter) {
		return
	}
	p.open = true
	p.recoveryStreak = 0
	p.nextProbe = p.cfg.now().Add(p.cfg.breakerProbeInterval)
	peerBreakerState.Set(1)
	peerBreakerTransitionsTotal.WithLabelValues("open").Inc()
}

func (p *peerFetchPolicy) breakerOpen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.open
}

func peerWithinBreakerRatio(peer, origin time.Duration, ratio float64) bool {
	if peer < 0 || origin <= 0 || ratio <= 0 {
		return false
	}
	return float64(peer) <= ratio*float64(origin)
}

type originSpanResult struct {
	bytesRead          int64
	duration           time.Duration
	firstBlockDuration time.Duration
}

// originSpanFlights coalesces identical block-origin spans while allowing
// each request to stop waiting independently. The producer has a service-owned
// context and is canceled only after its final interested waiter leaves.
type originSpanFlights struct {
	mu    sync.Mutex
	calls map[string]*originSpanCall
}

type originSpanCall struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	refs   int

	firstIdx           int64
	committedThrough   int64
	firstBlockDuration time.Duration
	progress           chan struct{}

	result           originSpanResult
	err              error
	complete         bool
	canceledAsLoser  bool
	originUsed       bool
	peerWon          bool
	duplicateCounted bool
}

type originSpanLease struct {
	group *originSpanFlights
	key   string
	call  *originSpanCall
	once  sync.Once
}

func (g *originSpanFlights) start(key string, fn func(context.Context) (originSpanResult, error)) *originSpanLease {
	return g.startWithProgress(key, 0, func(ctx context.Context, _ func(int64, time.Duration)) (originSpanResult, error) {
		return fn(ctx)
	})
}

// startWithProgress coalesces an origin span while publishing monotonic block
// commits to every lease. A close-and-replace progress channel avoids callback
// backpressure and lets late joiners replay the committed prefix from state.
func (g *originSpanFlights) startWithProgress(
	key string,
	firstIdx int64,
	fn func(context.Context, func(int64, time.Duration)) (originSpanResult, error),
) *originSpanLease {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*originSpanCall)
	}
	if call, ok := g.calls[key]; ok {
		call.refs++
		g.mu.Unlock()
		return &originSpanLease{group: g, key: key, call: call}
	}
	ctx, cancel := context.WithCancel(context.Background())
	call := &originSpanCall{
		ctx: ctx, cancel: cancel, done: make(chan struct{}), refs: 1,
		firstIdx: firstIdx, committedThrough: firstIdx - 1, progress: make(chan struct{}),
	}
	g.calls[key] = call
	g.mu.Unlock()

	reportCommit := func(idx int64, elapsed time.Duration) {
		g.mu.Lock()
		if !call.complete && idx > call.committedThrough {
			call.committedThrough = idx
			if idx == call.firstIdx {
				call.firstBlockDuration = elapsed
			}
			close(call.progress)
			call.progress = make(chan struct{})
		}
		g.mu.Unlock()
	}
	go func() {
		result, err := fn(ctx, reportCommit)
		g.mu.Lock()
		call.result = result
		call.err = err
		call.complete = true
		loser := call.canceledAsLoser
		close(call.done)
		close(call.progress)
		if g.calls[key] == call {
			delete(g.calls, key)
		}
		g.mu.Unlock()
		if loser && result.bytesRead > 0 {
			peerHedgeDuplicateBytesTotal.WithLabelValues("origin").Add(float64(result.bytesRead))
		}
	}()
	return &originSpanLease{group: g, key: key, call: call}
}

// progressSnapshot returns immutable commit state plus a notification channel
// that closes on the next commit or final completion. Callers act only after
// the flight mutex is released.
func (l *originSpanLease) progressSnapshot() (
	committedThrough int64,
	firstBlockDuration time.Duration,
	progress <-chan struct{},
	complete bool,
) {
	if l == nil || l.group == nil || l.call == nil {
		return -1, 0, nil, true
	}
	l.group.mu.Lock()
	committedThrough = l.call.committedThrough
	firstBlockDuration = l.call.firstBlockDuration
	progress = l.call.progress
	complete = l.call.complete
	l.group.mu.Unlock()
	return committedThrough, firstBlockDuration, progress, complete
}

func (l *originSpanLease) committedThroughIn(lo, hi int64) int64 {
	if l == nil || l.group == nil || l.call == nil {
		return lo - 1
	}
	l.group.mu.Lock()
	through := min(l.call.committedThrough, hi)
	if through < lo {
		through = lo - 1
	}
	l.group.mu.Unlock()
	return through
}

func (l *originSpanLease) wait(ctx context.Context) (originSpanResult, error) {
	select {
	case <-l.call.done:
		return l.call.result, l.call.err
	case <-ctx.Done():
		return originSpanResult{}, ctx.Err()
	}
}

type originLeaseOutcome uint8

const (
	originLeaseAbandoned originLeaseOutcome = iota
	originLeaseUsed
	originLeaseLostToPeer
)

// release drops a caller that no longer needs the origin result. Abandonment
// (for example a client disconnect) may cancel an unneeded producer, but only
// a peer winner is classified as a hedge cancellation or duplicate traffic.
func (l *originSpanLease) release() bool {
	return l.releaseWithOutcome(originLeaseAbandoned)
}

func (l *originSpanLease) releaseOriginUsed() bool {
	return l.releaseWithOutcome(originLeaseUsed)
}

func (l *originSpanLease) releasePeerWinner() bool {
	return l.releaseWithOutcome(originLeaseLostToPeer)
}

func (l *originSpanLease) releaseWithOutcome(outcome originLeaseOutcome) (canceled bool) {
	if l == nil || l.group == nil || l.call == nil {
		return false
	}
	var duplicateBytes int64
	var hedgeCancellation bool
	l.once.Do(func() {
		l.group.mu.Lock()
		switch outcome {
		case originLeaseUsed:
			l.call.originUsed = true
		case originLeaseLostToPeer:
			l.call.peerWon = true
		}
		l.call.refs--
		if l.call.refs == 0 {
			// Do not let a fresh request attach to a producer whose context is
			// about to be canceled. Producer cleanup is identity-checked, so it
			// cannot delete a newer call installed under the same key.
			if l.group.calls[l.key] == l.call {
				delete(l.group.calls, l.key)
			}
			losingHedge := !l.call.originUsed && l.call.peerWon
			if !l.call.complete {
				l.call.canceledAsLoser = losingHedge
				l.call.cancel()
				canceled = true
				hedgeCancellation = losingHedge
			} else if losingHedge && !l.call.duplicateCounted {
				l.call.duplicateCounted = true
				duplicateBytes = l.call.result.bytesRead
			}
		}
		l.group.mu.Unlock()
		if hedgeCancellation {
			peerFetchCancellationsTotal.WithLabelValues("origin").Inc()
		}
		if duplicateBytes > 0 {
			peerHedgeDuplicateBytesTotal.WithLabelValues("origin").Add(float64(duplicateBytes))
		}
	})
	return canceled
}
