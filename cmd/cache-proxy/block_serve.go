package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

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
	}, []string{"reason"}) // range_shape, origin_error, entry_vanished
	blockReadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_block_reads_total",
		Help: "Blocks resolved while assembling responses, by source",
	}, []string{"source"}) // local, peer, s3
)

// fetchOriginSpan fetches blocks [firstIdx, lastIdx] of r.URL in ONE origin
// range GET and commits each block to the store under its BlockKey. Rewriting
// the Range header is legal: DuckDB httpfs signs only
// host;x-amz-content-sha256;x-amz-date (see forwardUncached), so Range is not
// covered by the SigV4 signature. The final block of an object is naturally
// short — a clean EOF mid-span is success, not an error.
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
		if hopByHop[strings.ToLower(k)] || strings.EqualFold(k, "Range") {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Host = r.Host
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", firstIdx*blockSize, (lastIdx+1)*blockSize-1))

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, originErrorBodyCap))
		return &originStatusError{status: resp.StatusCode, headers: resp.Header.Clone(), body: body}
	}

	for idx := firstIdx; idx <= lastIdx; idx++ {
		size, err := p.store.PutStream(BlockKey(r.URL.String(), idx, blockSize), io.LimitReader(resp.Body, blockSize))
		if err != nil {
			return fmt.Errorf("commit block %d: %w", idx, err)
		}
		cacheOriginBytesTotal.Add(float64(size))
		if size < blockSize {
			// Object ended inside this block — it is the tail. Done.
			break
		}
	}
	return nil
}

// serveBlockAligned serves a cacheable GET whose Range is an absolute
// bytes=start-end pair from block-aligned cache entries: local disk, then
// peers, then coalesced origin fetches for contiguous missing runs (chunked
// at maxSpanBlocks per origin request). Returns false when the request shape
// is not block-servable; the caller then runs the legacy exact-range path.
func (p *CacheProxy) serveBlockAligned(w http.ResponseWriter, r *http.Request, rangeHeader string) bool {
	start, end, ok := parseAbsoluteRange(rangeHeader)
	if !ok {
		blockFallbackTotal.WithLabelValues("range_shape").Inc()
		return false
	}
	firstIdx, lastIdx := blockSpan(start, end, p.blockSize)
	urlStr := r.URL.String()

	// Phase 1: ensure every block is present locally. Track sources for the
	// hit/miss accounting and the log line.
	var nLocal, nPeer, nOrigin int64
	var missRunStart int64 = -1
	flushRun := func(runEnd int64) bool {
		if missRunStart < 0 {
			return true
		}
		for lo := missRunStart; lo <= runEnd; lo += p.maxSpanBlocks {
			hi := min(lo+p.maxSpanBlocks-1, runEnd)
			_, err := p.flights.Do(BlockKey(urlStr, lo, p.blockSize), func() (fetchResult, error) {
				return fetchResult{}, p.fetchOriginSpan(r, p.blockSize, lo, hi)
			})
			if err != nil {
				var oe *originStatusError
				if errors.As(err, &oe) {
					oe.writeTo(w)
					return false
				}
				slog.Error("Block span fetch failed.", "url", urlStr, "blocks", fmt.Sprintf("%d-%d", lo, hi), "error", err)
				http.Error(w, err.Error(), http.StatusBadGateway)
				return false
			}
			nOrigin += hi - lo + 1
		}
		missRunStart = -1
		return true
	}
	for idx := firstIdx; idx <= lastIdx; idx++ {
		key := BlockKey(urlStr, idx, p.blockSize)
		if p.store.Has(key) {
			if !flushRun(idx - 1) {
				return true // error already written
			}
			nLocal++
			continue
		}
		if p.peers != nil {
			if _, _, ok := p.peers.FetchFromPeers(key, func(rd io.Reader) (int64, error) {
				return p.store.PutStream(key, rd)
			}); ok {
				if !flushRun(idx - 1) {
					return true
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

	// Phase 2: stream the assembled range. Mirrors serveStream's legacy
	// response shape: 206 + Content-Range with served size after the slash.
	total := end - start + 1
	w.Header().Set("Content-Length", fmt.Sprintf("%d", total))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.WriteHeader(http.StatusPartialContent)

	served := int64(0)
	for idx := firstIdx; idx <= lastIdx; idx++ {
		reader, size, ok := p.store.openFile(BlockKey(urlStr, idx, p.blockSize))
		if !ok {
			// Evicted in the window between phase 1 and here (or object
			// shorter than the requested range). Nothing sane to send after
			// headers are out; abort so httpfs sees a short body and retries.
			blockFallbackTotal.WithLabelValues("entry_vanished").Inc()
			slog.Warn("Block vanished during assembly.", "url", urlStr, "block", idx)
			return true
		}
		blockStart := idx * p.blockSize
		skip := max(0, start-blockStart)
		want := min(size-skip, end-blockStart+1-skip, total-served)
		if skip > 0 {
			_, _ = io.CopyN(io.Discard, reader, skip)
		}
		n, _ := io.CopyN(w, reader, want)
		served += n
		_ = reader.Close()
		if n < want {
			return true
		}
	}
	cacheBytesServed.WithLabelValues(sourceLabel(nPeer, nOrigin)).Add(float64(served))
	slog.Info("Served.", "source", "blocks", "url", urlStr, "range", rangeHeader,
		"bytes", served, "blocks_local", nLocal, "blocks_peer", nPeer, "blocks_s3", nOrigin)
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
