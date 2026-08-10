package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cacheOriginBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_origin_bytes_total",
		Help: "Bytes fetched from S3 origin (block mode; the origin-offload SLI numerator)",
	})
	blockFallbackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_block_fallback_total",
		Help: "Requests that fell back to the legacy exact-range path, by reason",
	}, []string{"reason"}) // no_range, range_shape, entry_vanished, config, capacity
	blockReadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_block_reads_total",
		Help: "Blocks resolved while assembling responses, by source",
	}, []string{"source"}) // local, peer, s3
	// requestDurationSeconds is shared between the block-serve path (this
	// file) and the forward-proxy path (proxy.go); buckets start at 1ms
	// because a local cache hit can be sub-millisecond and top out around 8s
	// to cover multi-block cold-origin fetches.
	requestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cache_proxy_request_duration_seconds",
		Help:    "End-to-end proxy request duration by served path and byte source",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"path", "source"})
)

type exactLengthReader struct {
	r         io.Reader
	remaining int64
}

func (r *exactLengthReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF && r.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	if err == io.EOF && r.remaining == 0 {
		return n, nil
	}
	return n, err
}

func (p *CacheProxy) rememberObjectSize(url string, size int64) {
	if size >= 0 {
		p.objectSizes.Store(url, size)
	}
}

func (p *CacheProxy) knownObjectSize(url string) (int64, bool) {
	v, ok := p.objectSizes.Load(url)
	if !ok {
		return 0, false
	}
	size, ok := v.(int64)
	return size, ok
}

func writeRangeNotSatisfiable(w http.ResponseWriter, objectSize int64) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", objectSize))
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
}

// fetchOriginSpan fetches blocks [firstIdx, lastIdx] of r.URL in ONE origin
// range GET and commits each block to the store under its BlockKey. Rewriting
// the Range header is legal: DuckDB httpfs signs only
// host;x-amz-content-sha256;x-amz-date (see forwardUncached), so Range is not
// covered by the SigV4 signature. Content-Range is validated before any block
// is committed, and each selected block must contain exactly the advertised
// number of bytes.
func (p *CacheProxy) fetchOriginSpan(r *http.Request, blockSize, firstIdx, lastIdx int64) error {
	_, _, err := p.fetchOriginSpanContext(r.Context(), r, blockSize, firstIdx, lastIdx, nil)
	return err
}

// fetchOriginSpanContext is the cancelable implementation used by origin
// hedges. bytesRead includes partial network traffic when cancellation wins;
// successfully committed bytes remain accounted by cacheOriginBytesTotal.
// firstBlockDuration measures request setup, transfer, and durable cache commit
// for the first block, which is directly comparable to one peer block fetch.
func (p *CacheProxy) fetchOriginSpanContext(
	parent context.Context,
	r *http.Request,
	blockSize, firstIdx, lastIdx int64,
	onBlockCommit func(idx int64, elapsed time.Duration),
) (bytesRead int64, firstBlockDuration time.Duration, retErr error) {
	startedAt := time.Now()
	originFetchInFlight.Inc()
	defer func() {
		originFetchInFlight.Dec()
		originFetchesTotal.WithLabelValues(originFetchOutcome(retErr)).Inc()
	}()
	timeout := p.originTimeout
	if timeout <= 0 {
		timeout = defaultOriginTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL.String(), nil)
	if err != nil {
		return 0, 0, err
	}
	for k, vv := range r.Header {
		if hopByHop[strings.ToLower(k)] || strings.EqualFold(k, "Range") {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host
	wantStart := firstIdx * blockSize
	wantEnd := (lastIdx+1)*blockSize - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", wantStart, wantEnd))

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	counted := &countingReader{r: resp.Body}

	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			if objectSize, ok := parseUnsatisfiedContentRange(resp.Header.Get("Content-Range")); ok {
				p.rememberObjectSize(r.URL.String(), objectSize)
			}
		}
		body, _ := io.ReadAll(io.LimitReader(counted, originErrorBodyCap))
		return counted.n, 0, &originStatusError{status: resp.StatusCode, headers: resp.Header.Clone(), body: body}
	}

	// This function always sends a Range header, so anything other than 206
	// means the origin ignored it and is sending the full object from byte 0
	// (e.g. a proxy/CDN in front of origin stripping Range, or an origin that
	// doesn't support it). Storing that body under this span's block keys
	// would put object-offset-0 bytes into blocks tagged with firstIdx..lastIdx
	// — every read of those blocks would silently return the wrong bytes, and
	// since blocks are treated as immutable once cached, the corruption would
	// never self-heal. Fail closed instead.
	if resp.StatusCode != http.StatusPartialContent {
		return counted.n, 0, fmt.Errorf("origin ignored Range (status %d): refusing to cache misaligned blocks", resp.StatusCode)
	}
	gotStart, gotEnd, objectSize, ok := parsePartialContentRange(resp.Header.Get("Content-Range"))
	if !ok {
		return counted.n, 0, fmt.Errorf("origin returned invalid Content-Range %q", resp.Header.Get("Content-Range"))
	}
	wantResponseEnd := min(wantEnd, objectSize-1)
	if gotStart != wantStart || gotEnd != wantResponseEnd {
		return counted.n, 0, fmt.Errorf("origin returned Content-Range bytes %d-%d/%d for requested bytes %d-%d",
			gotStart, gotEnd, objectSize, wantStart, wantEnd)
	}
	expectedBodySize := gotEnd - gotStart + 1
	if resp.ContentLength >= 0 && resp.ContentLength != expectedBodySize {
		return counted.n, 0, fmt.Errorf("origin Content-Length %d does not match Content-Range length %d", resp.ContentLength, expectedBodySize)
	}

	remaining := expectedBodySize
	for idx := firstIdx; idx <= lastIdx && remaining > 0; idx++ {
		blockBytes := min(blockSize, remaining)
		size, err := p.store.PutStream(BlockKey(r.URL.String(), idx, blockSize), &exactLengthReader{
			r:         counted,
			remaining: blockBytes,
		})
		if err != nil {
			return counted.n, firstBlockDuration, fmt.Errorf("commit block %d: %w", idx, err)
		}
		if size != blockBytes {
			return counted.n, firstBlockDuration, fmt.Errorf("commit block %d: stored %d bytes, expected %d", idx, size, blockBytes)
		}
		cacheOriginBytesTotal.Add(float64(size))
		if idx == firstIdx {
			firstBlockDuration = time.Since(startedAt)
		}
		if onBlockCommit != nil {
			onBlockCommit(idx, time.Since(startedAt))
		}
		remaining -= size
	}
	if remaining != 0 {
		return counted.n, firstBlockDuration, fmt.Errorf("origin body ended with %d bytes still expected", remaining)
	}
	p.rememberObjectSize(r.URL.String(), objectSize)
	return counted.n, firstBlockDuration, nil
}

// peerFillConcurrency bounds how many of one request's blocks are fetched
// from peers at the same time. Requests typically span one or two blocks, so
// this only matters for wide spans, where it keeps a single request from
// monopolizing peer bandwidth.
const peerFillConcurrency = 8

// peerFill is one cancelable per-block peer attempt. At most eight workers are
// created per request; the process-wide policy adds count and byte ceilings.
type peerFill struct {
	idx    int64
	key    string
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	started           atomic.Bool
	loser             atomic.Bool
	lateEligible      atomic.Bool
	accounted         atomic.Bool
	recoveryDone      atomic.Bool
	recoveryDetached  atomic.Bool
	measurementQueued atomic.Bool
	recoveryMu        sync.Mutex
	recoveryUpdates   chan time.Duration
	startedAt         time.Time
	recovery          bool
	policy            *peerFetchPolicy
	result            controlledPeerFetchResult
}

func (f *peerFill) isDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func (f *peerFill) finish(result controlledPeerFetchResult) {
	f.result = result
	close(f.done)
	if f.recovery && !result.ok {
		f.completeRecovery(false)
	}
	if f.loser.Load() {
		f.accountLosingResult()
	}
}

func (f *peerFill) markStarted() {
	f.startedAt = time.Now()
	f.started.Store(true)
}

func (f *peerFill) completeRecovery(success bool) {
	if !f.recovery || f.policy == nil || !f.recoveryDone.CompareAndSwap(false, true) {
		return
	}
	f.policy.finishProbe(true, success)
}

func (f *peerFill) accountLosingResult() {
	if !f.accounted.CompareAndSwap(false, true) {
		return
	}
	if f.result.bytes > 0 {
		peerHedgeDuplicateBytesTotal.WithLabelValues("peer").Add(float64(f.result.bytes))
	}
	if f.lateEligible.Load() && f.result.ok {
		latePeerSuccessesTotal.Inc()
	}
}

// markOriginWinnerWithoutCancel classifies a diagnostic recovery transfer as
// duplicate work once the request has committed to origin, while allowing the
// one sampled transfer to finish inside the breaker's latency-ratio budget.
func (f *peerFill) markOriginWinnerWithoutCancel() {
	doneBefore := f.isDone()
	f.loser.Store(true)
	if !doneBefore {
		f.lateEligible.Store(true)
	}
	if f.isDone() {
		f.accountLosingResult()
	}
}

// cancelForOrigin marks this fill as losing work. Queued fills are canceled
// without consuming a global permit; admitted network work is counted as a
// peer-side cancellation and reports any bytes already transferred.
func (f *peerFill) cancelForOrigin() {
	doneBefore := f.isDone()
	f.markOriginWinnerWithoutCancel()
	if !doneBefore {
		if f.started.Load() {
			peerFetchCancellationsTotal.WithLabelValues("peer").Inc()
		}
		f.cancel()
	}
	if f.isDone() {
		f.accountLosingResult()
	}
}

func (p *CacheProxy) launchPeerFills(
	ctx context.Context,
	urlStr string,
	firstIdx, lastIdx int64,
) (map[int64]*peerFill, <-chan struct{}, time.Time) {
	fills := make(map[int64]*peerFill)
	allDone := make(chan struct{})
	if p.peers == nil {
		close(allDone)
		return fills, allDone, time.Now()
	}
	policy := p.peerPolicy
	if policy == nil {
		close(allDone)
		return fills, allDone, time.Now()
	}
	startedAt := time.Now()
	deadline := startedAt.Add(policy.headStart())
	breakerWasOpen := policy.breakerOpen()
	jobs := make(chan *peerFill, int(lastIdx-firstIdx+1))
	pendingJobs := 0
	for idx := firstIdx; idx <= lastIdx; idx++ {
		key := BlockKey(urlStr, idx, p.blockSize)
		if p.store.Has(key) {
			continue
		}
		allowed, recovery := policy.allowPeer(true)
		fillParent := ctx
		if recovery {
			// A sampled recovery fetch is bounded by the measured 1.5x ratio
			// after origin completes, not by the client request returning.
			fillParent = context.Background()
		}
		fillCtx, cancel := context.WithCancel(fillParent)
		fill := &peerFill{
			idx: idx, key: key, ctx: fillCtx, cancel: cancel, done: make(chan struct{}),
			recovery: recovery, policy: policy,
		}
		fills[idx] = fill
		if !allowed {
			peerFetchShedTotal.WithLabelValues("breaker").Inc()
			fill.finish(controlledPeerFetchResult{})
			continue
		}
		if recovery {
			// Half-open probes are diagnostic. Origin starts immediately and the
			// probe gets only an instantaneous limiter acquisition.
			breakerWasOpen = true
		}
		jobs <- fill
		pendingJobs++
	}
	close(jobs)
	if breakerWasOpen {
		deadline = startedAt
	}
	if len(fills) == 0 {
		close(allDone)
		return fills, allDone, deadline
	}

	if pendingJobs == 0 {
		close(allDone)
		return fills, allDone, deadline
	}
	workers := min(peerFillConcurrency, pendingJobs)
	var wg sync.WaitGroup
	wg.Add(pendingJobs)
	for range workers {
		go func() {
			for fill := range jobs {
				result := p.fetchFromPeers(fill.ctx, deadline, p.blockSize, fill.key, func(rd io.Reader) (int64, error) {
					return p.store.PutStream(fill.key, io.LimitReader(rd, p.blockSize))
				}, peerFetchDecision{allowed: true, nonBlocking: fill.recovery}, true,
					fill.markStarted)
				fill.finish(result)
				wg.Done()
			}
		}()
	}
	go func() {
		wg.Wait()
		close(allDone)
	}()
	return fills, allDone, deadline
}

func waitForPeerHeadStart(ctx context.Context, allDone <-chan struct{}, deadline time.Time) error {
	wait := time.Until(deadline)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-allDone:
		return nil
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type blockSpanRaceResult struct {
	source                    string // source for blocks not in originCommittedThrough
	originCommittedThrough    int64
	hedged                    bool
	breakerRatioEvidence      bool
	breakerComparison         peerThresholdComparison
	breakerComparisonObserved bool
	originWait                time.Duration
	peerWait                  time.Duration
	err                       error
}

func spanPeerState(store *DiskCache, fills map[int64]*peerFill, lo, hi int64) (canWin, ready bool) {
	ready = true
	for idx := lo; idx <= hi; idx++ {
		fill := fills[idx]
		if fill != nil && store.Has(fill.key) {
			continue
		}
		if fill == nil {
			return false, false
		}
		if fill.isDone() {
			if !fill.result.ok || !store.Has(fill.key) {
				return false, false
			}
			continue
		}
		if !fill.started.Load() {
			// Still queued when the adaptive head-start expired. It is no
			// longer eligible to start, so this span must shed directly.
			return false, false
		}
		ready = false
	}
	return true, ready
}

type spanReadyResult struct {
	ready                  bool
	originCommittedThrough int64
}

func spanPeerOutcome(
	ctx context.Context,
	store *DiskCache,
	lease *originSpanLease,
	fills map[int64]*peerFill,
	lo, hi int64,
) <-chan spanReadyResult {
	outcome := make(chan spanReadyResult, 1)
	go func() {
		for idx := lo; idx <= hi; idx++ {
			fill := fills[idx]
			if fill != nil && store.Has(fill.key) {
				continue
			}
			if fill == nil {
				outcome <- spanReadyResult{}
				return
			}
			select {
			case <-fill.done:
				// An origin commit can cancel this peer fill. Re-check the
				// shared cache before treating its canceled result as a gap.
				if !store.Has(fill.key) {
					outcome <- spanReadyResult{}
					return
				}
			case <-ctx.Done():
				return
			}
		}
		outcome <- spanReadyResult{
			ready:                  true,
			originCommittedThrough: lease.committedThroughIn(lo, hi),
		}
	}()
	return outcome
}

// abandonSpanPeerFills stops request-owned work after origin failure or client
// cancellation. It is not a hedge win, so it deliberately avoids loser and
// duplicate-byte accounting. A detached recovery sample keeps its hard bound.
func abandonSpanPeerFills(fills map[int64]*peerFill, lo, hi int64) {
	for idx := lo; idx <= hi; idx++ {
		if fill := fills[idx]; fill != nil {
			if fill.recovery && fill.recoveryDetached.Load() {
				continue
			}
			fill.cancel()
		}
	}
}

// finishSpanRecovery resolves only recovery fills that never transferred a
// bounded timer from the commit watcher. Detached fills are classified by
// assessRecoveryAtOriginCommit at the exact first-block ratio boundary.
func finishSpanRecovery(store *DiskCache, fills map[int64]*peerFill, lo, hi int64) {
	for idx := lo; idx <= hi; idx++ {
		fill := fills[idx]
		if fill == nil || !fill.recovery || fill.recoveryDetached.Load() {
			continue
		}
		success := fill.isDone() && fill.result.ok && store.Has(fill.key)
		fill.completeRecovery(success)
	}
}

func failSpanRecovery(fills map[int64]*peerFill, lo, hi int64) {
	for idx := lo; idx <= hi; idx++ {
		if fill := fills[idx]; fill != nil {
			if fill.recoveryDetached.Load() {
				continue
			}
			fill.completeRecovery(false)
		}
	}
}

func spanHasRecovery(fills map[int64]*peerFill, lo, hi int64) bool {
	for idx := lo; idx <= hi; idx++ {
		if fill := fills[idx]; fill != nil && fill.recovery {
			return true
		}
	}
	return false
}

// armRecoveryAssessment starts one service-owned bounded controller or updates
// its comparison when a fresher first-block sample arrives. The initial bound
// comes from the rolling origin EWMA, so a peer cannot run to its 30-second GET
// timeout even when the current origin stalls before returning a first byte.
func (p *CacheProxy) armRecoveryAssessment(fill *peerFill, originComparison time.Duration) {
	if p.peerPolicy == nil || fill == nil || !fill.recovery || fill.recoveryDone.Load() || originComparison <= 0 {
		return
	}

	fill.recoveryMu.Lock()
	updates := fill.recoveryUpdates
	start := updates == nil
	if start {
		updates = make(chan time.Duration, 1)
		fill.recoveryUpdates = updates
		fill.recoveryDetached.Store(true)
	}
	fill.recoveryMu.Unlock()

	if start {
		go p.runRecoveryAssessment(fill, originComparison, updates)
		return
	}
	select {
	case updates <- originComparison:
	default:
		// Only the latest (first-commit) comparison matters. Replace a stale
		// queued update without blocking the origin commit callback.
		select {
		case <-updates:
		default:
		}
		select {
		case updates <- originComparison:
		default:
		}
	}
}

func (p *CacheProxy) runRecoveryAssessment(
	fill *peerFill,
	originComparison time.Duration,
	updates <-chan time.Duration,
) {
	peerDone := false
	hasCurrentComparison := false
	fillDone := (<-chan struct{})(fill.done)
	completeFromPeer := func() {
		healthy := fill.result.ok && peerWithinBreakerRatio(
			fill.result.duration,
			originComparison,
			p.peerPolicy.cfg.breakerRatio,
		)
		fill.completeRecovery(healthy)
	}
	for {
		budget := time.Duration(p.peerPolicy.cfg.breakerRatio * float64(originComparison))
		remaining := budget
		if fill.started.Load() {
			remaining -= time.Since(fill.startedAt)
		}
		if remaining <= 0 {
			fill.cancelForOrigin()
			fill.completeRecovery(false)
			return
		}

		timer := time.NewTimer(remaining)
		select {
		case <-fillDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if !fill.result.ok {
				fill.completeRecovery(false)
				return
			}
			peerDone = true
			fillDone = nil
			if hasCurrentComparison {
				completeFromPeer()
				return
			}
			// A successful peer may finish before the current origin supplies
			// its first block. Keep only this small controller alive: classify
			// against a fresher commit if it arrives, otherwise against the
			// rolling fallback when the existing hard deadline expires.
			continue
		case next := <-updates:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			originComparison = next
			hasCurrentComparison = true
			if peerDone {
				completeFromPeer()
				return
			}
			continue
		case <-timer.C:
			if peerDone {
				completeFromPeer()
			} else {
				fill.cancelForOrigin()
				fill.completeRecovery(false)
			}
			return
		}
	}
}

// assessRecoveryAtOriginCommit marks the current origin block as the winner
// for duplicate-byte accounting and tightens/classifies the controller with
// the contemporaneous first-block duration.
func (p *CacheProxy) assessRecoveryAtOriginCommit(fill *peerFill, originComparison time.Duration) {
	if p.peerPolicy == nil || fill == nil || !fill.recovery {
		return
	}
	if !fill.started.Load() || originComparison <= 0 {
		fill.cancelForOrigin()
		fill.completeRecovery(false)
		return
	}
	fill.markOriginWinnerWithoutCancel()
	p.armRecoveryAssessment(fill, originComparison)
}

func (p *CacheProxy) armSpanRecoveryFallback(fills map[int64]*peerFill, lo, hi int64) {
	if p.peerPolicy == nil {
		return
	}
	originComparison, ok := p.peerPolicy.comparableOriginLatency(0)
	if !ok {
		// A naturally opened breaker always has an origin baseline. Keep a
		// defensive hard bound for restored/test state that was forced open.
		originComparison = p.peerPolicy.cfg.maxHeadStart
	}
	for idx := lo; idx <= hi; idx++ {
		if fill := fills[idx]; fill != nil && fill.recovery {
			p.armRecoveryAssessment(fill, originComparison)
		}
	}
}

// watchOriginCommits applies producer progress to this request's own peer
// fills. Progress is replayable for leases that join an existing flight, and
// all cancellation/classification happens outside the flight mutex.
func (p *CacheProxy) watchOriginCommits(
	ctx context.Context,
	lease *originSpanLease,
	fills map[int64]*peerFill,
	lo, hi int64,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		processedThrough := lo - 1
		for {
			committedThrough, firstBlockDuration, progress, complete := lease.progressSnapshot()
			for idx := processedThrough + 1; idx <= min(committedThrough, hi); idx++ {
				fill := fills[idx]
				if fill == nil {
					continue
				}
				if fill.recovery {
					p.assessRecoveryAtOriginCommit(fill, firstBlockDuration)
					continue
				}

				doneBefore := fill.isDone()
				peerElapsed := time.Duration(0)
				if fill.started.Load() {
					peerElapsed = time.Since(fill.startedAt)
				}
				fill.cancelForOrigin()
				if !doneBefore && peerElapsed > 0 &&
					peerWithinBreakerRatio(peerElapsed, firstBlockDuration, p.peerPolicy.cfg.breakerRatio) &&
					fill.measurementQueued.CompareAndSwap(false, true) {
					// Wait only for prompt context cancellation to release the
					// original permit, then run the globally sampled diagnostic.
					go func(fill *peerFill) {
						<-fill.done
						if !p.startAmbiguousPeerMeasurement(fill.key, firstBlockDuration) {
							fill.measurementQueued.Store(false)
						}
					}(fill)
				}
			}
			processedThrough = max(processedThrough, min(committedThrough, hi))
			if complete || processedThrough >= hi {
				if complete {
					for idx := lo; idx <= hi; idx++ {
						fill := fills[idx]
						if fill != nil && fill.recovery && idx > committedThrough &&
							!fill.recoveryDone.Load() {
							fill.cancel()
							fill.completeRecovery(false)
						}
					}
				}
				return
			}
			select {
			case <-progress:
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

type peerThresholdComparison uint8

const (
	peerThresholdUnknown peerThresholdComparison = iota
	peerThresholdHealthy
	peerThresholdExceeded
)

// observeCanceledPeers records threshold-qualified lower bounds and returns a
// tri-state paired comparison. Healthy requires every observed peer fill in
// this span to have completed successfully within the ratio; a canceled fill,
// fast miss, or still-running fill is unknown rather than evidence of health.
// At most one slow lower bound is added per span.
func (p *CacheProxy) observeCanceledPeers(
	fills map[int64]*peerFill,
	lo, hi int64,
	originFirstBlockDuration time.Duration,
) (ratioEvidence bool, comparison peerThresholdComparison, ambiguousKey string, originComparison time.Duration) {
	if p.peerPolicy == nil || originFirstBlockDuration <= 0 {
		return false, peerThresholdUnknown, "", 0
	}
	originComparison, comparable := p.peerPolicy.comparableOriginLatency(originFirstBlockDuration)
	if !comparable {
		return false, peerThresholdUnknown, "", 0
	}
	var slowestLowerBound time.Duration
	observed := false
	allHealthy := true
	thresholdExceeded := false
	for idx := lo; idx <= hi; idx++ {
		fill := fills[idx]
		if fill == nil || fill.recovery || !fill.started.Load() {
			continue
		}
		observed = true
		peerLowerBound := time.Since(fill.startedAt)
		if fill.isDone() {
			if fill.result.ok {
				// Successful completions were already added to the peer EWMA.
				ratioEvidence = true
				if !peerWithinBreakerRatio(
					fill.result.duration,
					originComparison,
					p.peerPolicy.cfg.breakerRatio,
				) {
					thresholdExceeded = true
					allHealthy = false
				}
				continue
			}
			allHealthy = false
			peerLowerBound = fill.result.duration
			if fill.loser.Load() && !fill.measurementQueued.Load() && ambiguousKey == "" {
				ambiguousKey = fill.key
			}
		} else {
			allHealthy = false
		}
		if !fill.isDone() && !fill.measurementQueued.Load() && ambiguousKey == "" {
			ambiguousKey = fill.key
		}
		if !peerWithinBreakerRatio(peerLowerBound, originComparison, p.peerPolicy.cfg.breakerRatio) &&
			peerLowerBound > slowestLowerBound {
			thresholdExceeded = true
			slowestLowerBound = peerLowerBound
		}
	}
	if slowestLowerBound > 0 {
		p.peerPolicy.observePeerLowerBound(slowestLowerBound)
	}
	if thresholdExceeded {
		return true, peerThresholdExceeded, "", originComparison
	}
	if observed && allHealthy {
		return ratioEvidence, peerThresholdHealthy, "", originComparison
	}
	return ratioEvidence, peerThresholdUnknown, ambiguousKey, originComparison
}

// startAmbiguousPeerMeasurement restarts one promptly canceled loser as a
// non-blocking diagnostic. The normal transfer is still canceled immediately;
// this sampled fetch gets an instantaneous global permit and at most the 1.5x
// origin budget, which provides the otherwise-missing evidence after an abrupt
// slowdown without reviving the 30-second zombie path.
func (p *CacheProxy) startAmbiguousPeerMeasurement(cacheKey string, originComparison time.Duration) bool {
	policy := p.peerPolicy
	if policy == nil || cacheKey == "" || originComparison <= 0 || !policy.startAmbiguousMeasurement() {
		return false
	}
	go func() {
		defer policy.finishAmbiguousMeasurement()
		budget := time.Duration(policy.cfg.breakerRatio * float64(originComparison))
		if budget <= 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		result := p.fetchFromPeers(ctx, time.Time{}, p.blockSize, cacheKey, func(rd io.Reader) (int64, error) {
			return io.Copy(io.Discard, io.LimitReader(rd, p.blockSize))
		}, peerFetchDecision{allowed: true, nonBlocking: true}, true, nil)
		timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
		cancel()
		if !result.started {
			return
		}
		if result.bytes > 0 {
			peerHedgeDuplicateBytesTotal.WithLabelValues("peer").Add(float64(result.bytes))
		}
		if result.ok {
			latePeerSuccessesTotal.Inc()
		}
		if timedOut {
			peerFetchCancellationsTotal.WithLabelValues("peer").Inc()
			// The transfer was still incomplete at the boundary, so its true
			// latency is strictly greater than the configured ratio.
			policy.observePeerLowerBound(budget + time.Nanosecond)
			policy.recordThresholdComparison(true)
			return
		}
		if !result.ok && !peerWithinBreakerRatio(result.duration, originComparison, policy.cfg.breakerRatio) {
			policy.observePeerLowerBound(result.duration)
			policy.recordThresholdComparison(true)
			return
		}
		if result.ok {
			policy.recordThresholdComparison(!peerWithinBreakerRatio(
				result.duration,
				originComparison,
				policy.cfg.breakerRatio,
			))
			policy.recordLatencyRatioObservation()
		}
	}()
	return true
}

func (p *CacheProxy) raceOriginSpan(
	r *http.Request,
	urlStr string,
	lo, hi int64,
	fills map[int64]*peerFill,
) blockSpanRaceResult {
	noOriginCommit := lo - 1
	hasRecovery := spanHasRecovery(fills, lo, hi)
	canWin, ready := spanPeerState(p.store, fills, lo, hi)
	if ready && !hasRecovery {
		finishSpanRecovery(p.store, fills, lo, hi)
		return blockSpanRaceResult{source: "peer", originCommittedThrough: noOriginCommit}
	}
	// An open breaker always starts origin immediately. Its one periodic peer
	// sample is diagnostic and never delays or supplies the user request.
	hedged := canWin && !hasRecovery
	if hasRecovery {
		p.armSpanRecoveryFallback(fills, lo, hi)
	}

	flightKey := fmt.Sprintf("%s|%d", BlockKey(urlStr, lo, p.blockSize), hi)
	originStarted := time.Now()
	lease := p.blockFlights.startWithProgress(flightKey, lo, func(
		ctx context.Context,
		reportCommit func(int64, time.Duration),
	) (originSpanResult, error) {
		fetchStarted := time.Now()
		bytesRead, firstBlockDuration, err := p.fetchOriginSpanContext(
			ctx,
			r,
			p.blockSize,
			lo,
			hi,
			func(idx int64, elapsed time.Duration) {
				if idx == lo && p.peerPolicy != nil {
					p.peerPolicy.observeOriginFirstBlock(elapsed)
				}
				reportCommit(idx, elapsed)
			},
		)
		duration := time.Since(fetchStarted)
		if p.peerPolicy != nil {
			p.peerPolicy.observeOriginSpan(duration, err == nil, errors.Is(err, context.Canceled))
		}
		return originSpanResult{
			bytesRead: bytesRead, duration: duration, firstBlockDuration: firstBlockDuration,
		}, err
	})
	commitWatchCtx, cancelCommitWatch := context.WithCancel(r.Context())
	commitWatchDone := p.watchOriginCommits(commitWatchCtx, lease, fills, lo, hi)
	stopCommitWatch := func() {
		cancelCommitWatch()
		<-commitWatchDone
	}
	if hedged {
		peerHedgesTotal.Inc()
	}

	if !hedged {
		result, err := lease.wait(r.Context())
		stopCommitWatch()
		breakerRatioEvidence := false
		breakerComparison := peerThresholdUnknown
		breakerComparisonObserved := false
		if err == nil {
			if !hasRecovery {
				var ambiguousKey string
				var originComparison time.Duration
				breakerRatioEvidence, breakerComparison, ambiguousKey, originComparison =
					p.observeCanceledPeers(fills, lo, hi, result.firstBlockDuration)
				breakerComparisonObserved = true
				finishSpanRecovery(p.store, fills, lo, hi)
				p.startAmbiguousPeerMeasurement(ambiguousKey, originComparison)
			}
		} else {
			failSpanRecovery(fills, lo, hi)
			abandonSpanPeerFills(fills, lo, hi)
		}
		if err == nil {
			lease.releaseOriginUsed()
		} else {
			lease.release()
		}
		return blockSpanRaceResult{
			source:                    "s3",
			originCommittedThrough:    lease.committedThroughIn(lo, hi),
			breakerRatioEvidence:      breakerRatioEvidence,
			breakerComparison:         breakerComparison,
			breakerComparisonObserved: breakerComparisonObserved,
			originWait:                time.Since(originStarted),
			err:                       err,
		}
	}

	peerWaitStarted := time.Now()
	peerCtx, cancelPeerWait := context.WithCancel(r.Context())
	defer cancelPeerWait()
	defer cancelCommitWatch()
	peerOutcome := spanPeerOutcome(peerCtx, p.store, lease, fills, lo, hi)
	originDone := lease.call.done
	var originErr error
	for {
		select {
		case spanReady := <-peerOutcome:
			peerOutcome = nil
			if spanReady.ready {
				finishSpanRecovery(p.store, fills, lo, hi)
				cancelPeerWait()
				stopCommitWatch()
				raceResult := blockSpanRaceResult{
					source:                 "peer",
					originCommittedThrough: spanReady.originCommittedThrough,
					hedged:                 true,
					originWait:             time.Since(originStarted),
					peerWait:               time.Since(peerWaitStarted),
				}
				if spanReady.originCommittedThrough >= lo {
					_, firstBlockDuration, _, _ := lease.progressSnapshot()
					var ambiguousKey string
					var originComparison time.Duration
					raceResult.breakerRatioEvidence, raceResult.breakerComparison,
						ambiguousKey, originComparison =
						p.observeCanceledPeers(fills, lo, hi, firstBlockDuration)
					raceResult.breakerComparisonObserved = true
					p.startAmbiguousPeerMeasurement(ambiguousKey, originComparison)
					lease.releaseOriginUsed()
					peerHedgeWinsTotal.WithLabelValues("origin").Inc()
				} else {
					lease.releasePeerWinner()
					peerHedgeWinsTotal.WithLabelValues("peer").Inc()
				}
				return raceResult
			}
			if originDone == nil {
				failSpanRecovery(fills, lo, hi)
				lease.release()
				return blockSpanRaceResult{
					source: "s3", originCommittedThrough: lease.committedThroughIn(lo, hi),
					hedged: true, originWait: time.Since(originStarted), err: originErr,
				}
			}
		case <-originDone:
			result, err := lease.wait(context.Background())
			stopCommitWatch()
			originDone = nil
			originErr = err
			if err == nil {
				breakerRatioEvidence, breakerComparison, ambiguousKey, originComparison :=
					p.observeCanceledPeers(fills, lo, hi, result.firstBlockDuration)
				lease.releaseOriginUsed()
				cancelPeerWait()
				finishSpanRecovery(p.store, fills, lo, hi)
				p.startAmbiguousPeerMeasurement(ambiguousKey, originComparison)
				peerHedgeWinsTotal.WithLabelValues("origin").Inc()
				return blockSpanRaceResult{
					source:                    "s3",
					originCommittedThrough:    lease.committedThroughIn(lo, hi),
					hedged:                    true,
					breakerRatioEvidence:      breakerRatioEvidence,
					breakerComparison:         breakerComparison,
					breakerComparisonObserved: true,
					originWait:                time.Since(originStarted),
				}
			}
			// Origin failed while peers were still viable. Let them finish as
			// the redundant path; if any peer fails too, return origin's error.
			if peerOutcome == nil {
				lease.release()
				return blockSpanRaceResult{
					source: "s3", originCommittedThrough: lease.committedThroughIn(lo, hi),
					hedged: true, originWait: time.Since(originStarted), err: err,
				}
			}
		case <-r.Context().Done():
			lease.release()
			cancelPeerWait()
			stopCommitWatch()
			failSpanRecovery(fills, lo, hi)
			abandonSpanPeerFills(fills, lo, hi)
			return blockSpanRaceResult{
				source: "s3", originCommittedThrough: lease.committedThroughIn(lo, hi),
				hedged: hedged, originWait: time.Since(originStarted), err: r.Context().Err(),
			}
		}
	}
}

// serveBlockAligned serves a cacheable GET whose Range is an absolute
// bytes=start-end pair from block-aligned cache entries: local disk, then
// peers, then coalesced origin fetches for contiguous missing runs (chunked
// at maxSpanBlocks per origin request). Returns false when the request shape
// is not block-servable; the caller then runs the legacy exact-range path.
func (p *CacheProxy) serveBlockAligned(w http.ResponseWriter, r *http.Request, rangeHeader string) bool {
	requestStart := time.Now()
	var peerDur, s3Dur, writeDur time.Duration

	// A misconfigured or not-yet-wired proxy (blockSize/maxSpanBlocks left at
	// their zero value) must fall back rather than divide by zero in
	// blockSpan or loop forever in flushRun's `lo += p.maxSpanBlocks`.
	if p.blockSize <= 0 || p.maxSpanBlocks <= 0 {
		blockFallbackTotal.WithLabelValues("config").Inc()
		return false
	}
	if rangeHeader == "" {
		blockFallbackTotal.WithLabelValues("no_range").Inc()
		return false
	}
	start, end, ok := parseAbsoluteRange(rangeHeader)
	if !ok {
		blockFallbackTotal.WithLabelValues("range_shape").Inc()
		return false
	}
	urlStr := r.URL.String()
	if objectSize, known := p.knownObjectSize(urlStr); known {
		if start >= objectSize {
			writeRangeNotSatisfiable(w, objectSize)
			return true
		}
		end = min(end, objectSize-1)
	}
	firstIdx, lastIdx := blockSpan(start, end, p.blockSize)
	blockCount := lastIdx - firstIdx + 1
	if p.store.maxBytes <= 0 || blockCount > p.store.maxBytes/p.blockSize {
		blockFallbackTotal.WithLabelValues("capacity").Inc()
		return false
	}

	// Launch at most eight peer workers for this request. Every worker must
	// also acquire the process-wide count and byte permits before the adaptive
	// head-start expires; queued work that misses it never starts later.
	peerPhaseStart := time.Now()
	fills, allPeerFillsDone, fillDeadline := p.launchPeerFills(r.Context(), urlStr, firstIdx, lastIdx)
	defer func() {
		for _, fill := range fills {
			if fill.recovery && fill.recoveryDetached.Load() {
				continue
			}
			fill.completeRecovery(false)
			fill.cancel()
		}
	}()
	if err := waitForPeerHeadStart(r.Context(), allPeerFillsDone, fillDeadline); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	peerDur += time.Since(peerPhaseStart)

	// Phase 1: ensure every block is present locally. Track sources for the
	// hit/miss accounting and the log line.
	sources := make(map[int64]string, blockCount)
	var nHedged int64
	var peerWonHedge, originWonHedge, breakerRatioEvidence bool
	var breakerComparisonSeen, breakerComparisonUnknown, breakerComparisonHealthy, breakerComparisonExceeded bool
	var missRunStart int64 = -1
	flushRun := func(runEnd int64) bool {
		if missRunStart < 0 {
			return true
		}
		runStart := missRunStart
		missRunStart = -1
		for lo := runStart; lo <= runEnd; {
			hi := min(lo+p.maxSpanBlocks-1, runEnd)
			race := p.raceOriginSpan(r, urlStr, lo, hi, fills)
			breakerRatioEvidence = breakerRatioEvidence || race.breakerRatioEvidence
			if race.breakerComparisonObserved {
				breakerComparisonSeen = true
				switch race.breakerComparison {
				case peerThresholdExceeded:
					breakerComparisonExceeded = true
				case peerThresholdHealthy:
					breakerComparisonHealthy = true
				default:
					breakerComparisonUnknown = true
				}
			}
			s3Dur += race.originWait
			peerDur += race.peerWait
			if race.hedged {
				nHedged += hi - lo + 1
				if race.originCommittedThrough < lo {
					peerWonHedge = true
				} else if race.err == nil {
					originWonHedge = true
				}
			}
			if race.err != nil {
				var oe *originStatusError
				if errors.As(race.err, &oe) {
					if oe.status == http.StatusRequestedRangeNotSatisfiable {
						if objectSize, known := p.knownObjectSize(urlStr); known && start < objectSize {
							end = min(end, objectSize-1)
							lastIdx = end / p.blockSize
							return true
						}
					}
					oe.writeTo(w)
					return false
				}
				slog.Error("Block span fetch failed.", "url", urlStr, "blocks", fmt.Sprintf("%d-%d", lo, hi), "error", race.err)
				http.Error(w, race.err.Error(), http.StatusBadGateway)
				return false
			}
			if objectSize, known := p.knownObjectSize(urlStr); known {
				if start >= objectSize {
					writeRangeNotSatisfiable(w, objectSize)
					return false
				}
				end = min(end, objectSize-1)
				lastIdx = end / p.blockSize
			}
			actualHi := min(hi, lastIdx)
			for idx := lo; idx <= actualHi; idx++ {
				if idx <= race.originCommittedThrough {
					sources[idx] = "s3"
				} else {
					sources[idx] = race.source
				}
			}
			lo = hi + 1
		}
		return true
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		key := BlockKey(urlStr, idx, p.blockSize)
		fill := fills[idx]
		if p.store.Has(key) && (fill == nil || !fill.recovery) {
			if !flushRun(idx - 1) {
				return true
			}
			if idx > lastIdx {
				break
			}
			if fill != nil && fill.isDone() && fill.result.ok {
				fill.completeRecovery(true)
				sources[idx] = "peer"
			} else {
				sources[idx] = "local"
			}
			continue
		}
		if missRunStart < 0 {
			missRunStart = idx
		}
	}
	if !flushRun(lastIdx) {
		return true
	}

	// Phase 1.5: verify every block phase 1 believes is present is actually
	// on disk before any response header is written. This is the backstop for
	// the single-flight race above (and any other residual gap): one direct
	// re-fetch of each residual missing run, bypassing the single-flight so it
	// always runs. A validated short object tail shrinks lastIdx during phase 1
	// and is therefore not considered a gap. If blocks are still missing after
	// the re-fetch we fail closed with a retryable 502 rather than risk
	// assembling a corrupt short body.
	var reverifyStart int64 = -1
	reverify := func(runEnd int64) {
		if reverifyStart < 0 {
			return
		}
		lo := reverifyStart
		reverifyStart = -1
		fetchStart := time.Now()
		err := p.fetchOriginSpan(r, p.blockSize, lo, runEnd)
		s3Dur += time.Since(fetchStart)
		if err != nil {
			slog.Warn("Presence re-fetch failed; failing closed below if blocks are still missing.",
				"url", urlStr, "blocks", fmt.Sprintf("%d-%d", lo, runEnd), "error", err)
			return
		}
		for idx := lo; idx <= runEnd; idx++ {
			sources[idx] = "s3"
		}
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		if p.store.Has(BlockKey(urlStr, idx, p.blockSize)) {
			reverify(idx - 1)
			continue
		}
		if reverifyStart < 0 {
			reverifyStart = idx
		}
	}
	reverify(lastIdx)
	for idx := firstIdx; idx <= lastIdx; idx++ {
		if !p.store.Has(BlockKey(urlStr, idx, p.blockSize)) {
			slog.Error("Block still missing after presence re-fetch; failing closed.", "url", urlStr, "block", idx)
			http.Error(w, "block cache entry missing after re-fetch", http.StatusBadGateway)
			return true
		}
	}

	var nLocal, nPeer, nOrigin int64
	for idx := firstIdx; idx <= lastIdx; idx++ {
		switch sources[idx] {
		case "peer":
			nPeer++
		case "s3":
			nOrigin++
		default:
			// A concurrent request may have populated a residual block between
			// phase checks. It is local from this request's perspective.
			nLocal++
		}
	}

	// Phase 2: open every block before committing response headers. Open file
	// descriptors keep their contents readable even if the LRU removes the
	// directory entries while the response is being assembled. If an entry
	// vanished before it could be opened, return false so HandleProxy can use
	// the legacy exact-range path while the response is still untouched.
	type openedBlock struct {
		idx    int64
		reader io.ReadCloser
		size   int64
		skip   int64
		want   int64
	}
	opened := make([]openedBlock, 0, lastIdx-firstIdx+1)
	closeOpened := func() {
		for i := range opened {
			_ = opened[i].reader.Close()
		}
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		reader, size, ok := p.store.openFile(BlockKey(urlStr, idx, p.blockSize))
		if !ok {
			closeOpened()
			blockFallbackTotal.WithLabelValues("entry_vanished").Inc()
			slog.Warn("Block vanished before assembly; falling back.", "url", urlStr, "block", idx)
			return false
		}
		if size <= 0 || size > p.blockSize {
			_ = reader.Close()
			closeOpened()
			slog.Error("Cached block has invalid size; falling back.",
				"url", urlStr, "block", idx, "size", size, "block_size", p.blockSize)
			return false
		}
		if size < p.blockSize {
			// A validated short block is the object's tail. Remembering its
			// boundary also recovers exact range semantics after a process
			// restart, when the in-memory object-size map starts empty.
			objectSize := idx*p.blockSize + size
			p.rememberObjectSize(urlStr, objectSize)
			if start >= objectSize {
				_ = reader.Close()
				closeOpened()
				writeRangeNotSatisfiable(w, objectSize)
				return true
			}
			end = min(end, objectSize-1)
			lastIdx = idx
		}
		opened = append(opened, openedBlock{idx: idx, reader: reader, size: size})
	}

	total := end - start + 1
	planned := int64(0)
	for i := range opened {
		blockStart := opened[i].idx * p.blockSize
		opened[i].skip = max(0, start-blockStart)
		opened[i].want = min(opened[i].size-opened[i].skip, end-blockStart+1-opened[i].skip, total-planned)
		if opened[i].want <= 0 {
			closeOpened()
			slog.Error("Cached block cannot satisfy requested range; falling back.",
				"url", urlStr, "block", opened[i].idx, "start", start,
				"block_start", blockStart, "size", opened[i].size)
			return false
		}
		planned += opened[i].want
	}
	if planned != total {
		closeOpened()
		slog.Error("Opened blocks do not cover requested range; falling back.",
			"url", urlStr, "planned", planned, "total", total)
		return false
	}
	defer closeOpened()

	blockReadsTotal.WithLabelValues("local").Add(float64(nLocal))
	blockReadsTotal.WithLabelValues("peer").Add(float64(nPeer))
	blockReadsTotal.WithLabelValues("s3").Add(float64(nOrigin))

	// Request-level hit/miss accounting mirrors the legacy meaning: a hit is
	// "no origin traffic needed".
	if nOrigin == 0 {
		cacheHitsTotal.Inc()
	} else {
		cacheMissesTotal.Inc()
	}
	if p.peerPolicy != nil {
		switch {
		case breakerComparisonExceeded:
			p.peerPolicy.recordThresholdComparison(true)
		case !breakerComparisonUnknown && ((breakerComparisonSeen && breakerComparisonHealthy) ||
			peerWonHedge || (nOrigin == 0 && nPeer > 0)):
			p.peerPolicy.recordThresholdComparison(false)
		}
		if originWonHedge || breakerRatioEvidence || peerWonHedge || (nOrigin == 0 && nPeer > 0) {
			p.peerPolicy.recordLatencyRatioObservation()
		}
	}

	representationSize := "*"
	if objectSize, known := p.knownObjectSize(urlStr); known {
		representationSize = strconv.FormatInt(objectSize, 10)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%s", start, end, representationSize))
	w.WriteHeader(http.StatusPartialContent)

	served := int64(0)
	for i := range opened {
		writeStart := time.Now()
		if opened[i].skip > 0 {
			// Block readers are disk files, so jump to the slice instead of
			// reading and discarding the prefix — the discard costs up to a
			// full block of disk reads and copies per request.
			if seeker, ok := opened[i].reader.(io.Seeker); ok {
				if _, err := seeker.Seek(opened[i].skip, io.SeekStart); err != nil {
					return true
				}
			} else if _, err := io.CopyN(io.Discard, opened[i].reader, opened[i].skip); err != nil {
				return true
			}
		}
		n, _ := io.CopyN(w, opened[i].reader, opened[i].want)
		writeDur += time.Since(writeStart)
		served += n
		if n < opened[i].want {
			return true
		}
	}
	source := sourceLabel(nPeer, nOrigin)
	cacheBytesServed.WithLabelValues(source).Add(float64(served))
	totalDur := time.Since(requestStart)
	requestDurationSeconds.WithLabelValues("block", source).Observe(totalDur.Seconds())
	slog.Info("Served.", "source", "blocks", "client", clientAddress(r.RemoteAddr),
		"url", urlStr, "range", rangeHeader,
		"bytes", served, "blocks_local", nLocal, "blocks_peer", nPeer, "blocks_s3", nOrigin,
		"blocks_hedged", nHedged,
		"dur_ms", totalDur.Milliseconds(), "peer_ms", peerDur.Milliseconds(),
		"s3_ms", s3Dur.Milliseconds(), "write_ms", writeDur.Milliseconds())
	return true
}

// sourceLabel picks the legacy bytes_served source label for an assembled
// response: s3 if any origin fetch happened, else peer if any peer fill, else
// local — so the existing "Bytes served by source" dashboard keeps meaning
// "where did the slowest byte come from".
func sourceLabel(nPeer, nOrigin int64) string {
	switch {
	case nOrigin > 0:
		return "s3"
	case nPeer > 0:
		return "peer"
	default:
		return "local"
	}
}
