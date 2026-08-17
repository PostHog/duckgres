package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	timeout := p.originTimeout
	if timeout <= 0 {
		timeout = defaultOriginTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL.String(), nil)
	if err != nil {
		return err
	}
	for k, vv := range r.Header {
		if hopByHop[strings.ToLower(k)] || strings.EqualFold(k, "Range") || isInternalPropagationHeader(k) {
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
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			if objectSize, ok := parseUnsatisfiedContentRange(resp.Header.Get("Content-Range")); ok {
				p.rememberObjectSize(r.URL.String(), objectSize)
			}
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, originErrorBodyCap))
		return &originStatusError{status: resp.StatusCode, headers: resp.Header.Clone(), body: body}
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
		return fmt.Errorf("origin ignored Range (status %d): refusing to cache misaligned blocks", resp.StatusCode)
	}
	gotStart, gotEnd, objectSize, ok := parsePartialContentRange(resp.Header.Get("Content-Range"))
	if !ok {
		return fmt.Errorf("origin returned invalid Content-Range %q", resp.Header.Get("Content-Range"))
	}
	wantResponseEnd := min(wantEnd, objectSize-1)
	if gotStart != wantStart || gotEnd != wantResponseEnd {
		return fmt.Errorf("origin returned Content-Range bytes %d-%d/%d for requested bytes %d-%d",
			gotStart, gotEnd, objectSize, wantStart, wantEnd)
	}
	expectedBodySize := gotEnd - gotStart + 1
	if resp.ContentLength >= 0 && resp.ContentLength != expectedBodySize {
		return fmt.Errorf("origin Content-Length %d does not match Content-Range length %d", resp.ContentLength, expectedBodySize)
	}

	remaining := expectedBodySize
	for idx := firstIdx; idx <= lastIdx && remaining > 0; idx++ {
		blockBytes := min(blockSize, remaining)
		size, err := p.store.PutStream(BlockKey(r.URL.String(), idx, blockSize), &exactLengthReader{
			r:         resp.Body,
			remaining: blockBytes,
		})
		if err != nil {
			return fmt.Errorf("commit block %d: %w", idx, err)
		}
		if size != blockBytes {
			return fmt.Errorf("commit block %d: stored %d bytes, expected %d", idx, size, blockBytes)
		}
		cacheOriginBytesTotal.Add(float64(size))
		remaining -= size
	}
	if remaining != 0 {
		return fmt.Errorf("origin body ended with %d bytes still expected", remaining)
	}
	p.rememberObjectSize(r.URL.String(), objectSize)
	return nil
}

// blockPresent reports whether a block's bytes are on local disk. It checks
// the tracked index first (the common case, and the one that drives LRU
// recency) but also accepts a tracked-file-still-syncing entry: a concurrent
// PutStream lands its file (rename) a moment before it lands the LRU
// accounting (addLocked), so an index-only Has can race a just-filled block
// and report it missing while its bytes are already servable. Neither peek
// counts as an access — openFile (phase 2) does the touching.
func (p *CacheProxy) blockPresent(key string) bool {
	if p.store.Has(key) {
		return true
	}
	_, err := os.Stat(filepath.Join(p.store.dir, key))
	return err == nil
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

	// Phase 1: ensure every block is present locally. Track sources for the
	// hit/miss accounting and the log line.
	var nLocal, nPeer, nOrigin int64
	var missRunStart int64 = -1
	// Summary mode bounds direct peer I/O across the entire block request, not
	// merely per block. Remaining block misses safely coalesce to origin.
	summaryGetsLeft := 2
	summaryLookupRecorded := false
	summaryCtx, cancelSummary := context.WithTimeout(r.Context(), peerHasTimeout)
	defer cancelSummary()
	flushRun := func(runEnd int64) bool {
		if missRunStart < 0 {
			return true
		}
		for lo := missRunStart; lo <= runEnd; lo += p.maxSpanBlocks {
			hi := min(lo+p.maxSpanBlocks-1, runEnd)
			// hi must be part of the single-flight key, not just lo: two
			// concurrent requests can both start a missing run at the same lo
			// but need different-length spans (their own runEnd differs), and
			// if only lo keyed the call, the loser would adopt the winner's
			// (shorter) fetch result while believing its own longer span was
			// covered — silently leaving trailing blocks unfetched.
			flightKey := fmt.Sprintf("%s|%d", BlockKey(urlStr, lo, p.blockSize), hi)
			_, err := p.flights.Do(flightKey, func() (fetchResult, error) {
				fetchStart := time.Now()
				// Retry transient origin failures inside the flight so every
				// waiter on this key benefits, and a brief origin blip is
				// absorbed here instead of reaching DuckDB as a 502.
				_, fetchSpan := proxyTracer.Start(r.Context(), "cache.origin_span_fetch")
				fetchErr := p.retryOriginFetch(r, fetchSpan, func() error {
					return p.fetchOriginSpan(r, p.blockSize, lo, hi)
				})
				fetchSpan.End()
				s3Dur += time.Since(fetchStart)
				return fetchResult{}, fetchErr
			})
			if err != nil {
				var oe *originStatusError
				if errors.As(err, &oe) {
					if oe.status == http.StatusRequestedRangeNotSatisfiable {
						if objectSize, known := p.knownObjectSize(urlStr); known && start < objectSize {
							end = min(end, objectSize-1)
							lastIdx = end / p.blockSize
							missRunStart = -1
							return true
						}
					}
					oe.writeTo(w)
					return false
				}
				slog.Error("Block span fetch failed.", "url", urlStr, "blocks", fmt.Sprintf("%d-%d", lo, hi), "error", err)
				http.Error(w, err.Error(), http.StatusBadGateway)
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
			if actualHi >= lo {
				nOrigin += actualHi - lo + 1
			}
		}
		missRunStart = -1
		return true
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		key := BlockKey(urlStr, idx, p.blockSize)
		if p.blockPresent(key) {
			if !flushRun(idx - 1) {
				return true // error already written
			}
			if idx > lastIdx {
				break
			}
			nLocal++
			continue
		}
		if p.peers != nil {
			peerStart := time.Now()
			ok := false
			if p.peers.lookupMode == peerLookupSummary {
				if !summaryLookupRecorded {
					peerFetchesTotal.Inc()
					summaryLookupRecorded = true
				}
				for _, holder := range p.peers.SummaryCandidates(key, time.Now()) {
					if summaryGetsLeft == 0 || summaryCtx.Err() != nil {
						break
					}
					summaryGetsLeft--
					_, ok = p.peers.FetchFromPeer(summaryCtx, holder, key, false, func(rd io.Reader) (int64, error) {
						return p.store.PutStream(key, rd)
					})
					if ok {
						peerDirectGetsTotal.WithLabelValues("success").Inc()
						break
					}
					peerDirectGetsTotal.WithLabelValues("miss_or_error").Inc()
				}
			} else if holder, flight, found := p.peers.LocateKey(r.Context(), key); found {
				_, ok = p.peers.FetchFromPeer(r.Context(), holder, key, flight, func(rd io.Reader) (int64, error) {
					return p.store.PutStream(key, rd)
				})
			}
			peerDur += time.Since(peerStart)
			if ok {
				if !flushRun(idx - 1) {
					return true
				}
				if idx > lastIdx {
					break
				}
				nPeer++
				continue
			}
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
		_, fetchSpan := proxyTracer.Start(r.Context(), "cache.origin_span_refetch")
		err := p.retryOriginFetch(r, fetchSpan, func() error {
			return p.fetchOriginSpan(r, p.blockSize, lo, runEnd)
		})
		fetchSpan.End()
		s3Dur += time.Since(fetchStart)
		if err != nil {
			slog.Warn("Presence re-fetch failed; failing closed below if blocks are still missing.",
				"url", urlStr, "blocks", fmt.Sprintf("%d-%d", lo, runEnd), "error", err)
			return
		}
		nOrigin += runEnd - lo + 1
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		if p.blockPresent(BlockKey(urlStr, idx, p.blockSize)) {
			reverify(idx - 1)
			continue
		}
		if reverifyStart < 0 {
			reverifyStart = idx
		}
	}
	reverify(lastIdx)
	for idx := firstIdx; idx <= lastIdx; idx++ {
		if !p.blockPresent(BlockKey(urlStr, idx, p.blockSize)) {
			slog.Error("Block still missing after presence re-fetch; failing closed.",
				"url", urlStr, "block", idx)
			http.Error(w, "block cache entry missing after re-fetch", http.StatusBadGateway)
			return true
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

	// Request-level hit/miss accounting mirrors the metric's documented
	// meaning: a hit means every requested block was already on local NVMe.
	// Fetching any block from a peer or origin is a local miss.
	if nPeer == 0 && nOrigin == 0 {
		cacheHitsTotal.Inc()
	} else {
		cacheMissesTotal.Inc()
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
	slog.Info("Served.", "source", "blocks", "url", urlStr, "range", rangeHeader,
		"bytes", served, "blocks_local", nLocal, "blocks_peer", nPeer, "blocks_s3", nOrigin,
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
