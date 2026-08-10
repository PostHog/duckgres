package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// originServer serves a synthetic object of objSize bytes where byte i has
// value byte(i % 251), honoring absolute Range headers like S3.
func originServer(t *testing.T, objSize int64) *httptest.Server {
	t.Helper()
	body := make([]byte, objSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, ok := parseAbsoluteRange(r.Header.Get("Range"))
		if !ok {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			return
		}
		if start >= objSize {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", objSize))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= objSize {
			end = objSize - 1 // S3 clamps to object end
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
}

func TestFetchOriginSpan(t *testing.T) {
	const blockSize = 1024
	const objSize = int64(3*blockSize + 100) // 4 blocks, last one short

	origin := originServer(t, objSize)
	defer origin.Close()

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}

	// Fetch blocks 1..3 in one span (block 3 is the short tail).
	if err := p.fetchOriginSpan(req, blockSize, 1, 3); err != nil {
		t.Fatalf("fetchOriginSpan: %v", err)
	}

	// Every block in the span must now be a complete, correct cache entry.
	for idx := int64(1); idx <= 3; idx++ {
		key := BlockKey(u.String(), idx, blockSize)
		reader, size, ok := store.Open(key)
		if !ok {
			t.Fatalf("block %d not committed to store", idx)
		}
		data, _ := io.ReadAll(reader)
		_ = reader.Close()
		wantSize := int64(blockSize)
		if idx == 3 {
			wantSize = 100 // tail block truncated at object end
		}
		if size != wantSize || int64(len(data)) != wantSize {
			t.Fatalf("block %d: size %d, want %d", idx, size, wantSize)
		}
		for i, b := range data {
			if want := byte((idx*blockSize + int64(i)) % 251); b != want {
				t.Fatalf("block %d byte %d: got %d, want %d", idx, i, b, want)
			}
		}
	}

	// Block 0 was outside the span and must not exist.
	if store.Has(BlockKey(u.String(), 0, blockSize)) {
		t.Fatal("block 0 should not have been fetched")
	}
}

func TestFetchOriginSpanReportsFirstBlockCommitBeforeSpanCompletion(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, 2*blockSize)
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	defer release()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:blockSize])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-releaseSecond
		_, _ = w.Write(body[blockSize:])
	}))
	t.Cleanup(origin.Close)
	t.Cleanup(release)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, nil)
	p.client = origin.Client()
	u, _ := url.Parse(origin.URL + "/bucket/coalesced.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}

	type fetchResult struct {
		bytesRead          int64
		firstBlockDuration time.Duration
		err                error
	}
	done := make(chan fetchResult, 1)
	started := time.Now()
	go func() {
		bytesRead, firstBlockDuration, fetchErr := p.fetchOriginSpanContext(
			context.Background(), req, blockSize, 0, 1, nil,
		)
		done <- fetchResult{bytesRead: bytesRead, firstBlockDuration: firstBlockDuration, err: fetchErr}
	}()

	deadline := time.Now().Add(time.Second)
	for !store.Has(BlockKey(u.String(), 0, blockSize)) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !store.Has(BlockKey(u.String(), 0, blockSize)) {
		t.Fatal("first origin block was not committed while the span remained open")
	}
	select {
	case result := <-done:
		t.Fatalf("span returned before the second block was released: %+v", result)
	default:
	}
	time.Sleep(50 * time.Millisecond)
	release()
	result := <-done
	totalDuration := time.Since(started)
	if result.err != nil {
		t.Fatalf("fetchOriginSpanContext: %v", result.err)
	}
	if result.bytesRead != int64(len(body)) {
		t.Fatalf("bytes read = %d, want %d", result.bytesRead, len(body))
	}
	if result.firstBlockDuration <= 0 || totalDuration-result.firstBlockDuration < 40*time.Millisecond {
		t.Fatalf("first-block duration = %v, total = %v; want the committed first block, not full span latency",
			result.firstBlockDuration, totalDuration)
	}
}

// TestFetchOriginSpanRejects200 guards the immutable-block-cache poisoning
// hazard: fetchOriginSpan always sends a Range header, so an origin that
// ignores it and returns 200 + the full body would otherwise be split from
// byte 0 into blocks tagged with the requested (non-zero) indices — every
// future read of those blocks would silently return the wrong bytes forever,
// since cached blocks are never revalidated. The fetch must fail instead of
// committing anything.
func TestFetchOriginSpanRejects200(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 4*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ignore the Range header entirely — respond 200 with the full body,
		// as a Range-blind proxy/CDN or misbehaving origin might.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer origin.Close()

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}

	err = p.fetchOriginSpan(req, blockSize, 1, 2)
	if err == nil {
		t.Fatal("expected fetchOriginSpan to fail closed on a 200 response to a ranged request")
	}
	if !strings.Contains(err.Error(), "200") {
		t.Fatalf("error %q should mention the unexpected status code", err.Error())
	}
	for idx := int64(0); idx <= 3; idx++ {
		if store.Has(BlockKey(u.String(), idx, blockSize)) {
			t.Fatalf("block %d must not be committed when the origin ignored Range", idx)
		}
	}
}

func TestFetchOriginSpanRejectsMismatchedContentRange(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 2*blockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The proxy asks for blocks 1..2, but a broken intermediary returns a
		// different 206 range starting at byte zero.
		w.Header().Set("Content-Range", "bytes 0-2047/4096")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	defer origin.Close()

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()
	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}

	if err := p.fetchOriginSpan(req, blockSize, 1, 2); err == nil {
		t.Fatal("expected mismatched Content-Range to be rejected")
	}
	for idx := int64(1); idx <= 2; idx++ {
		if store.Has(BlockKey(u.String(), idx, blockSize)) {
			t.Fatalf("block %d committed from a mismatched Content-Range", idx)
		}
	}
}

func TestFetchOriginSpanRejectsCleanShortBody(t *testing.T) {
	const blockSize = 1024
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 1024-3071/4096")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, blockSize)) // clean EOF one block too soon
	}))
	defer origin.Close()

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()
	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}

	if err := p.fetchOriginSpan(req, blockSize, 1, 2); err == nil {
		t.Fatal("expected clean short 206 body to be rejected")
	}
	if store.Has(BlockKey(u.String(), 2, blockSize)) {
		t.Fatal("truncated block committed from a short 206 body")
	}
}

// TestServeBlockAlignedFailsClosedOn200Origin exercises the same hazard at
// the serveBlockAligned level: a cold request against an origin that returns
// 200 (Range-blind) must fail the whole request with a retryable 502 rather
// than serve a 206 assembled from misaligned blocks.
func TestServeBlockAlignedFailsClosedOn200Origin(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 4*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer origin.Close()

	p, store := newBlockProxy(t, origin, blockSize)
	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{"bytes=1500-2500"}}}
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()

	if !p.serveBlockAligned(w, req, "bytes=1500-2500") {
		t.Fatal("expected serveBlockAligned to handle the request (not fall back)")
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", w.Code)
	}
	if got := w.Header().Get("Content-Range"); got != "" {
		t.Fatalf("Content-Range = %q, want unset: headers must not be committed before the 502", got)
	}
	for idx := int64(0); idx <= 3; idx++ {
		if store.Has(BlockKey(u.String(), idx, blockSize)) {
			t.Fatalf("block %d must not be committed when the origin ignored Range", idx)
		}
	}
}

func TestFetchOriginSpanSendsBlockAlignedRange(t *testing.T) {
	const blockSize = 1024
	var gotRange string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Range", "bytes 1024-3071/4096")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 2*blockSize))
	}))
	defer origin.Close()

	store, _ := NewDiskCache(t.TempDir(), 80)
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{
		"Range": []string{"bytes=1500-2500"}, // client's original, must be ignored
	}}
	if err := p.fetchOriginSpan(req, blockSize, 1, 2); err != nil {
		t.Fatal(err)
	}
	want := "bytes=" + strconv.Itoa(1*blockSize) + "-" + strconv.Itoa(3*blockSize-1)
	if gotRange != want {
		t.Fatalf("origin saw Range %q, want block-aligned %q", gotRange, want)
	}
	if strings.Contains(gotRange, "1500") {
		t.Fatal("client range leaked to origin")
	}
}

func newBlockProxy(t *testing.T, origin *httptest.Server, blockSize int64) (*CacheProxy, *DiskCache) {
	t.Helper()
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	return p, store
}

func setTestPeerPolicy(p *CacheProxy, maxConcurrent, maxBytes int64, headStart time.Duration) {
	cfg := defaultPeerFetchPolicyConfig(maxConcurrent, maxBytes)
	cfg.minHeadStart = headStart
	cfg.maxHeadStart = headStart
	p.peerPolicy = newPeerFetchPolicy(cfg)
}

func doBlockRequest(t *testing.T, p *CacheProxy, rawURL, rangeHeader string) *httptest.ResponseRecorder {
	t.Helper()
	u, _ := url.Parse(rawURL)
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{rangeHeader}}}
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()
	if !p.serveBlockAligned(w, req, rangeHeader) {
		t.Fatalf("serveBlockAligned returned false for %q", rangeHeader)
	}
	return w
}

func TestServeBlockAlignedColdThenWarm(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 10*blockSize)
	defer origin.Close()
	p, store := newBlockProxy(t, origin, blockSize)
	target := origin.URL + "/bucket/f.parquet"

	// Cold: range crossing blocks 1-3.
	w := doBlockRequest(t, p, target, "bytes=1500-3500")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status %d, want 206", w.Code)
	}
	body := w.Body.Bytes()
	if int64(len(body)) != 2001 {
		t.Fatalf("body length %d, want 2001", len(body))
	}
	for i, b := range body {
		if want := byte((1500 + i) % 251); b != want {
			t.Fatalf("byte %d: got %d, want %d", i, b, want)
		}
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 1500-3500/10240" {
		t.Fatalf("Content-Range %q; want representation size %q", got, "bytes 1500-3500/10240")
	}

	// Warm with a DIFFERENT range over the same bytes (the drift scenario):
	// must be served entirely from stored blocks — origin must not be touched.
	origin.Close() // any origin fetch now fails the request
	w2 := doBlockRequest(t, p, target, "bytes=1400-3400")
	if w2.Code != http.StatusPartialContent || w2.Body.Len() != 2001 {
		t.Fatalf("drifted warm read failed: status %d len %d", w2.Code, w2.Body.Len())
	}
	_ = store
}

func TestServeBlockAlignedClampsColdRangeAtObjectEOF(t *testing.T) {
	const blockSize = 1024
	const objSize = int64(2*blockSize + 100)
	origin := originServer(t, objSize)
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, blockSize)

	w := doBlockRequest(t, p, origin.URL+"/bucket/f.parquet", "bytes=2000-4095")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if got, want := w.Body.Len(), int(objSize-2000); got != want {
		t.Fatalf("body length = %d, want %d", got, want)
	}
	if got := w.Header().Get("Content-Length"); got != "148" {
		t.Fatalf("Content-Length = %q, want 148", got)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 2000-2147/2148" {
		t.Fatalf("Content-Range = %q, want bytes 2000-2147/2148", got)
	}
}

func TestServeBlockAlignedReturns416ForKnownPastEOFStart(t *testing.T) {
	const blockSize = 1024
	const objSize = int64(2*blockSize + 100)
	origin := originServer(t, objSize)
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, blockSize)
	target := origin.URL + "/bucket/f.parquet"

	// Learn and cache the representation size from a valid tail response.
	first := doBlockRequest(t, p, target, "bytes=2000-2147")
	if first.Code != http.StatusPartialContent {
		t.Fatalf("setup status = %d, want 206", first.Code)
	}

	w := doBlockRequest(t, p, target, "bytes=2200-2500")
	if w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", w.Code)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes */2148" {
		t.Fatalf("Content-Range = %q, want bytes */2148", got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("416 body length = %d, want 0", w.Body.Len())
	}
}

// TestServeBlockAlignedFallsBackOnRangeShape covers both non-block-servable
// Range shapes: no Range header at all (reason "no_range") and a shape
// parseAbsoluteRange rejects, e.g. a suffix range (reason "range_shape").
// These get distinct fallback reasons so the dashboard can tell "client sent
// no Range" apart from "client sent a Range shape we don't handle" — a
// sustained no_range rate would mean something upstream of DuckDB httpfs is
// stripping Range, which range_shape wouldn't surface.
func TestServeBlockAlignedFallsBackOnRangeShape(t *testing.T) {
	tests := []struct {
		name        string
		rangeHeader string
		reason      string
	}{
		{"no range header", "", "no_range"},
		{"suffix range", "bytes=-500", "range_shape"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := originServer(t, 4096)
			defer origin.Close()
			p, _ := newBlockProxy(t, origin, 1024)
			u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
			req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
				Header: http.Header{"Range": []string{tt.rangeHeader}}}
			req = req.WithContext(context.Background())

			before := counterValue(t, blockFallbackTotal.WithLabelValues(tt.reason))
			if p.serveBlockAligned(httptest.NewRecorder(), req, tt.rangeHeader) {
				t.Fatalf("range %q must return false (legacy fallback)", tt.rangeHeader)
			}
			if got := counterValue(t, blockFallbackTotal.WithLabelValues(tt.reason)); got != before+1 {
				t.Fatalf("blockFallbackTotal{reason=%q} delta = %v, want 1", tt.reason, got-before)
			}
		})
	}
}

func TestServeBlockAlignedSpansChunkedByMaxSpan(t *testing.T) {
	const blockSize = 1024
	var originRanges []string
	body := make([]byte, 32*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originRanges = append(originRanges, r.Header.Get("Range"))
		start, end, _ := parseAbsoluteRange(r.Header.Get("Range"))
		if end >= int64(len(body)) {
			end = int64(len(body)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, blockSize)
	p.maxSpanBlocks = 4

	// 20 cold blocks with maxSpanBlocks=4 → 5 origin fetches, never more than
	// 4 blocks per request.
	doBlockRequest(t, p, origin.URL+"/bucket/f.parquet", "bytes=0-20479")
	if len(originRanges) != 5 {
		t.Fatalf("origin fetches: %d, want 5 (got %v)", len(originRanges), originRanges)
	}
}

// TestServeBlockAlignedPeerFillCountsAsHit exercises the previously-untested
// peer branch of Phase 1: a block resolvable from a peer must never touch
// origin, must land under blockReadsTotal{peer} / cacheBytesServed{peer}, and
// (having triggered zero origin fetches) must count as a cache hit, not a
// miss — the same "hit" meaning the legacy path uses.
func TestServeBlockAlignedPeerFillCountsAsHit(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	target := origin.URL + "/bucket/f.parquet"

	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(i % 251)
	}
	key := BlockKey(target, 0, blockSize)
	var hasCalls, getCalls int32
	peerAddr := newPeerServer(t, key, blockData, &hasCalls, &getCalls)

	origin.Close() // the block is fully resolvable from the peer; origin must never be touched

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{peerAddr}), []string{})
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	p.peerPolicy.mu.Lock()
	p.peerPolicy.directBadStreak = p.peerPolicy.cfg.breakerOpenAfter - 1
	p.peerPolicy.lastDirectBad = p.peerPolicy.cfg.now()
	p.peerPolicy.mu.Unlock()

	hitsBefore := counterValue(t, cacheHitsTotal)
	missesBefore := counterValue(t, cacheMissesTotal)
	peerReadsBefore := counterValue(t, blockReadsTotal.WithLabelValues("peer"))
	peerBytesBefore := counterValue(t, cacheBytesServed.WithLabelValues("peer"))

	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status %d, want 206", w.Code)
	}
	if got := w.Body.Bytes(); string(got) != string(blockData[:100]) {
		t.Fatalf("body mismatch: got %d bytes", len(got))
	}
	if atomic.LoadInt32(&getCalls) != 1 {
		t.Fatalf("peer /cache/get calls = %d, want 1", getCalls)
	}

	if got := counterValue(t, cacheHitsTotal); got != hitsBefore+1 {
		t.Fatalf("cacheHitsTotal delta = %v, want 1 (peer fill with zero origin fetches must count as a hit)", got-hitsBefore)
	}
	if got := counterValue(t, cacheMissesTotal); got != missesBefore {
		t.Fatalf("cacheMissesTotal delta = %v, want 0", got-missesBefore)
	}
	if got := counterValue(t, blockReadsTotal.WithLabelValues("peer")); got != peerReadsBefore+1 {
		t.Fatalf("blockReadsTotal{peer} delta = %v, want 1", got-peerReadsBefore)
	}
	if got := counterValue(t, cacheBytesServed.WithLabelValues("peer")); got != peerBytesBefore+100 {
		t.Fatalf("cacheBytesServed{peer} delta = %v, want 100 (sourceLabel must resolve to peer when no origin fetch happened)", got-peerBytesBefore)
	}
	p.peerPolicy.mu.Lock()
	directBadStreak := p.peerPolicy.directBadStreak
	p.peerPolicy.mu.Unlock()
	if directBadStreak != 0 {
		t.Fatalf("direct bad streak = %d after a pure peer win, want reset", directBadStreak)
	}
}

// TestServeBlockAlignedPeerFillsRunConcurrently locks in the parallel peer
// fill behavior: the peer's /cache/get handlers gate on all three of the
// request's blocks being fetched at once, so a regression to one-at-a-time
// fills can never open the gate — the request would hedge to a closed origin
// and fail instead of assembling the response.
func TestServeBlockAlignedPeerFillsRunConcurrently(t *testing.T) {
	const blockSize = 1024
	const nBlocks = 3
	origin := originServer(t, nBlocks*blockSize)
	target := origin.URL + "/bucket/f.parquet"

	body := make([]byte, nBlocks*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	keys := make(map[string]int64, nBlocks)
	for idx := int64(0); idx < nBlocks; idx++ {
		keys[BlockKey(target, idx, blockSize)] = idx
	}

	var arrivals int32
	gate := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := keys[r.URL.Query().Get("key")]; ok {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		idx, ok := keys[r.URL.Query().Get("key")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if atomic.AddInt32(&arrivals, 1) == nBlocks {
			close(gate)
		}
		<-gate
		block := body[idx*blockSize : (idx+1)*blockSize]
		w.Header().Set("Content-Length", strconv.Itoa(len(block)))
		_, _ = w.Write(block)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	origin.Close() // all blocks must come from the peer; a hedge to origin fails loudly

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(srv.URL, "http://")}), []string{})
	p.blockSize = blockSize
	p.maxSpanBlocks = 8

	w := doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", nBlocks*blockSize-1))
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status %d, want 206 (sequential fills would starve the gate and hedge into the closed origin)", w.Code)
	}
	if got := w.Body.Bytes(); string(got) != string(body) {
		t.Fatalf("body mismatch: got %d bytes, want %d", len(got), len(body))
	}
	if got := atomic.LoadInt32(&arrivals); got != nBlocks {
		t.Fatalf("peer /cache/get arrivals = %d, want %d", got, nBlocks)
	}
}

// TestServeBlockAlignedHedgesSlowPeerToOrigin covers the adaptive head start: a
// peer that claims the block but stalls the body transfer must not pin the
// request for the full peer get timeout. Origin starts while peer work is still
// running, and the first successful side completes the response.
func TestServeBlockAlignedHedgesSlowPeerToOrigin(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/f.parquet"
	key := BlockKey(target, 0, blockSize)

	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") == key {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		<-release // stall the body transfer past the wait budget
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // LIFO: unblock the handler before srv.Close waits on it

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(srv.URL, "http://")}), []string{})
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 50*time.Millisecond)
	p.peerPolicy.cfg.breakerMinSamples = 1
	p.peerPolicy.cfg.breakerOpenAfter = 1

	hedgedBefore := counterValue(t, peerHedgesTotal)
	originWinsBefore := counterValue(t, peerHedgeWinsTotal.WithLabelValues("origin"))
	s3ReadsBefore := counterValue(t, blockReadsTotal.WithLabelValues("s3"))

	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status %d, want 206", w.Code)
	}
	want := make([]byte, 100)
	for i := range want {
		want[i] = byte(i % 251)
	}
	if got := w.Body.Bytes(); string(got) != string(want) {
		t.Fatalf("body mismatch: got %d bytes", len(got))
	}
	if got := counterValue(t, peerHedgesTotal); got != hedgedBefore+1 {
		t.Fatalf("peerHedgesTotal delta = %v, want 1", got-hedgedBefore)
	}
	if got := counterValue(t, peerHedgeWinsTotal.WithLabelValues("origin")); got != originWinsBefore+1 {
		t.Fatalf("origin hedge-win delta = %v, want 1", got-originWinsBefore)
	}
	if got := counterValue(t, blockReadsTotal.WithLabelValues("s3")); got != s3ReadsBefore+1 {
		t.Fatalf("blockReadsTotal{s3} delta = %v, want 1 (hedged block must be served from origin)", got-s3ReadsBefore)
	}
	if !p.peerPolicy.breakerOpen() {
		t.Fatal("threshold-qualified canceled peer lower bound did not open the breaker")
	}
}

func TestAmbiguousOriginWinnerRunsBoundedBreakerMeasurement(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, blockSize)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", blockSize-1, blockSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/abrupt-slowdown.parquet"
	key := BlockKey(target, 0, blockSize)

	var peerGets, peerCancels atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		peerGets.Add(1)
		<-r.Context().Done()
		peerCancels.Add(1)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.minHeadStart = 25 * time.Millisecond
	cfg.maxHeadStart = 25 * time.Millisecond
	cfg.breakerMinSamples = 1
	cfg.breakerOpenAfter = 1
	p.peerPolicy = newPeerFetchPolicy(cfg)

	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	deadline := time.Now().Add(time.Second)
	for !p.peerPolicy.breakerOpen() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !p.peerPolicy.breakerOpen() {
		t.Fatal("breaker did not learn from a bounded measurement after an ambiguous canceled peer")
	}
	if got := peerGets.Load(); got != 2 {
		t.Fatalf("peer GETs = %d, want normal hedge plus one sampled measurement", got)
	}
	deadline = time.Now().Add(time.Second)
	for peerCancels.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := peerCancels.Load(); got != 2 {
		t.Fatalf("peer cancellations = %d, want both losing transfers canceled", got)
	}
}

func TestServeBlockAlignedOriginWinnerCancelsPeerAndCountsDuplicateBytes(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/cancel-peer.parquet"
	key := BlockKey(target, 0, blockSize)

	peerCanceled := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") == key {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write(make([]byte, 128))
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			close(peerCanceled)
		case <-time.After(time.Second):
		}
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), []string{})
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 10*time.Millisecond)

	cancelsBefore := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("peer"))
	duplicatesBefore := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("peer"))
	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	select {
	case <-peerCanceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("origin won but the losing peer transfer was not canceled")
	}
	deadline := time.Now().Add(time.Second)
	for counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("peer")) == duplicatesBefore && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("peer")); got != cancelsBefore+1 {
		t.Fatalf("peer cancellation delta = %v, want 1", got-cancelsBefore)
	}
	if got := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("peer")); got <= duplicatesBefore {
		t.Fatal("partial losing peer bytes were not recorded as duplicate traffic")
	}
}

func TestServeBlockAlignedPeerWinnerCancelsOrigin(t *testing.T) {
	const blockSize = 1024
	originStarted := make(chan struct{})
	originCanceled := make(chan struct{})
	var startedOnce, canceledOnce sync.Once
	origin := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(originStarted) })
		<-r.Context().Done()
		canceledOnce.Do(func() { close(originCanceled) })
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/cancel-origin.parquet"
	key := BlockKey(target, 0, blockSize)
	data := make([]byte, blockSize)
	for i := range data {
		data[i] = byte(i % 251)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		time.Sleep(40 * time.Millisecond)
		_, _ = w.Write(data)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), []string{})
	p.client = origin.Client()
	p.originTimeout = 200 * time.Millisecond
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 10*time.Millisecond)

	cancelsBefore := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin"))
	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	select {
	case <-originStarted:
	default:
		t.Fatal("adaptive head start never launched the origin hedge")
	}
	select {
	case <-originCanceled:
	case <-time.After(time.Second):
		t.Fatal("peer won but the losing origin transfer was not canceled")
	}
	if got := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin")); got != cancelsBefore+1 {
		t.Fatalf("origin cancellation delta = %v, want 1", got-cancelsBefore)
	}
}

func TestServeBlockAlignedQueuedPeerShedsToOriginWithoutStartingLater(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/queued.parquet"
	key := BlockKey(target, 0, blockSize)
	var peerCalls int32
	peerAddr := newPeerServer(t, key, make([]byte, blockSize), &peerCalls, &peerCalls)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{peerAddr}), []string{})
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 1, blockSize, 20*time.Millisecond)

	blocker, reason := p.peerPolicy.limiter.acquire(context.Background(), blockSize)
	if blocker == nil || reason != "" {
		t.Fatalf("occupy limiter = (%v, %q), want permit", blocker, reason)
	}
	w := doBlockRequest(t, p, target, "bytes=0-99")
	blocker.release()
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	time.Sleep(40 * time.Millisecond)
	if got := atomic.LoadInt32(&peerCalls); got != 0 {
		t.Fatalf("queued peer issued %d network calls after its head-start expired", got)
	}
}

func TestLaunchPeerFillsDoesNotStartLaterWorkerBatchesAfterHeadStart(t *testing.T) {
	const (
		blockSize  = int64(1024)
		blockCount = int64(peerFillConcurrency + 4)
	)
	target := "http://origin.invalid/bucket/wide.parquet"

	releaseFirstBatch := make(chan struct{})
	var peerCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, _ *http.Request) {
		call := peerCalls.Add(1)
		if call <= peerFillConcurrency {
			<-releaseFirstBatch
		}
		w.Header().Set("Content-Length", strconv.FormatInt(blockSize, 10))
		_, _ = w.Write(make([]byte, blockSize))
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), []string{})
	p.blockSize = blockSize
	setTestPeerPolicy(p, peerFillConcurrency, peerFillConcurrency*blockSize, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	_, allDone, deadline := p.launchPeerFills(ctx, target, 0, blockCount-1)
	waitUntil := time.Now().Add(time.Second)
	for peerCalls.Load() < peerFillConcurrency && time.Now().Before(waitUntil) {
		time.Sleep(time.Millisecond)
	}
	if got := peerCalls.Load(); got != peerFillConcurrency {
		close(releaseFirstBatch)
		t.Fatalf("first peer batch calls = %d, want %d", got, peerFillConcurrency)
	}
	if wait := time.Until(deadline) + 10*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	close(releaseFirstBatch)

	select {
	case <-allDone:
	case <-time.After(time.Second):
		t.Fatal("peer fill workers did not finish")
	}
	if got := peerCalls.Load(); got != peerFillConcurrency {
		t.Fatalf("peer calls = %d, want only the first %d; later batches started after the head-start deadline", got, peerFillConcurrency)
	}
}

func TestServeBlockAlignedGlobalPeerCapAcrossRequests(t *testing.T) {
	const blockSize = 1024
	const requests = 4
	origin := originServer(t, 4*blockSize)
	t.Cleanup(origin.Close)

	release := make(chan struct{})
	var active, maxActive atomic.Int32
	data := make([]byte, blockSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, _ *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maxActive.Load()
			if current <= prior || maxActive.CompareAndSwap(prior, current) {
				break
			}
		}
		<-release
		_, _ = w.Write(data)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), []string{})
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 2, 2*blockSize, 150*time.Millisecond)

	statuses := make(chan int, requests)
	for i := 0; i < requests; i++ {
		go func(i int) {
			target := fmt.Sprintf("%s/bucket/file-%d.parquet", origin.URL, i)
			u, _ := url.Parse(target)
			req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
				Header: http.Header{"Range": []string{"bytes=0-99"}}}
			w := httptest.NewRecorder()
			if !p.serveBlockAligned(w, req.WithContext(context.Background()), "bytes=0-99") {
				statuses <- 0
				return
			}
			statuses <- w.Code
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	for i := 0; i < requests; i++ {
		if status := <-statuses; status != http.StatusPartialContent {
			t.Fatalf("request status = %d, want 206", status)
		}
	}
	if got := maxActive.Load(); got > 2 {
		t.Fatalf("global concurrent peer GETs = %d, want <= 2", got)
	}
}

func TestServeBlockAlignedKeepsPerRequestPeerFairnessBound(t *testing.T) {
	const blockSize = 128
	const nBlocks = 20
	body := make([]byte, nBlocks*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	origin := originServer(t, int64(len(body)))
	target := origin.URL + "/bucket/wide.parquet"

	var active, maxActive atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maxActive.Load()
			if current <= prior || maxActive.CompareAndSwap(prior, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		for idx := int64(0); idx < nBlocks; idx++ {
			if r.URL.Query().Get("key") == BlockKey(target, idx, blockSize) {
				_, _ = w.Write(body[idx*blockSize : (idx+1)*blockSize])
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)
	origin.Close()

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.blockSize = blockSize
	p.maxSpanBlocks = nBlocks
	setTestPeerPolicy(p, 32, 32*blockSize, 150*time.Millisecond)

	w := doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", len(body)-1))
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if got := maxActive.Load(); got > peerFillConcurrency {
		t.Fatalf("one request used %d concurrent peer GETs, want <= %d", got, peerFillConcurrency)
	}
}

func TestOnePeerBlockDoesNotCancelOriginNeededForRestOfSpan(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 2*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	var originCalls atomic.Int32
	var originCanceled atomic.Bool
	var originRange atomic.Value
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		originRange.Store(r.Header.Get("Range"))
		select {
		case <-time.After(70 * time.Millisecond):
		case <-r.Context().Done():
			originCanceled.Store(true)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/span-race.parquet"

	secondCanceled := make(chan struct{})
	var secondCanceledOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("key") {
		case BlockKey(target, 0, blockSize):
			time.Sleep(35 * time.Millisecond)
			_, _ = w.Write(body[:blockSize])
		case BlockKey(target, 1, blockSize):
			<-r.Context().Done()
			secondCanceledOnce.Do(func() { close(secondCanceled) })
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 10*time.Millisecond)

	w := doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", len(body)-1))
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if originCanceled.Load() {
		t.Fatal("one completed peer block canceled the origin span still needed by another block")
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want one coalesced span", got)
	}
	if got, _ := originRange.Load().(string); got != "bytes=0-2047" {
		t.Fatalf("origin Range = %q, want bytes=0-2047", got)
	}
	select {
	case <-secondCanceled:
	case <-time.After(time.Second):
		t.Fatal("origin won without canceling the remaining peer block")
	}
}

func TestOriginCommitCancelsMatchingPeerBeforeWideSpanCompletes(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, 2*blockSize)
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	defer release()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:blockSize])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-releaseSecond
		_, _ = w.Write(body[blockSize:])
	}))
	t.Cleanup(origin.Close)
	t.Cleanup(release)
	target := origin.URL + "/bucket/per-block-cancel.parquet"

	peerCanceled := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var canceledOnce [2]sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		for idx := range int64(2) {
			if r.URL.Query().Get("key") != BlockKey(target, idx, blockSize) {
				continue
			}
			<-r.Context().Done()
			canceledOnce[idx].Do(func() { close(peerCanceled[idx]) })
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 10*time.Millisecond)

	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response <- doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", len(body)-1))
	}()
	select {
	case <-peerCanceled[0]:
	case <-time.After(time.Second):
		t.Fatal("first peer was not canceled when origin committed its block")
	}
	select {
	case <-peerCanceled[1]:
		t.Fatal("second peer was canceled before origin committed the second block")
	default:
	}
	select {
	case <-response:
		t.Fatal("request completed while the second origin block was still withheld")
	default:
	}
	release()
	if w := <-response; w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	select {
	case <-peerCanceled[1]:
	case <-time.After(time.Second):
		t.Fatal("second peer was not canceled when origin committed its block")
	}
}

func TestWideSpanRecoveryIsBoundedFromFirstOriginBlockCommit(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, 2*blockSize)
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	defer release()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:blockSize])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-releaseSecond
		_, _ = w.Write(body[blockSize:])
	}))
	t.Cleanup(origin.Close)
	t.Cleanup(release)
	target := origin.URL + "/bucket/wide-recovery.parquet"
	key := BlockKey(target, 0, blockSize)

	peerCanceled := make(chan struct{})
	var peerCanceledOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		<-r.Context().Done()
		peerCanceledOnce.Do(func() { close(peerCanceled) })
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.now = func() time.Time { return now }
	p.peerPolicy = newPeerFetchPolicy(cfg)
	p.peerPolicy.mu.Lock()
	p.peerPolicy.open = true
	p.peerPolicy.nextProbe = now
	p.peerPolicy.mu.Unlock()

	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response <- doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", len(body)-1))
	}()
	select {
	case <-peerCanceled:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("recovery peer outlived 1.5x first-block latency while the wider origin span was stalled")
	}
	select {
	case <-response:
		t.Fatal("request completed while the second origin block was still withheld")
	default:
	}
	release()
	if w := <-response; w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if !p.peerPolicy.breakerOpen() {
		t.Fatal("unhealthy wide-span recovery sample closed the breaker")
	}
}

func TestRecoveryIsBoundedWhenOriginStallsBeforeFirstBlock(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, blockSize)
	releaseOrigin := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOrigin) }) }
	defer release()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-releaseOrigin
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", blockSize-1, blockSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/pre-first-block-stall.parquet"
	key := BlockKey(target, 0, blockSize)

	peerCanceled := make(chan struct{})
	var canceledOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		<-r.Context().Done()
		canceledOnce.Do(func() { close(peerCanceled) })
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.now = func() time.Time { return now }
	p.peerPolicy = newPeerFetchPolicy(cfg)
	p.peerPolicy.observeOriginFirstBlock(50 * time.Millisecond)
	p.peerPolicy.mu.Lock()
	p.peerPolicy.open = true
	p.peerPolicy.nextProbe = now
	p.peerPolicy.mu.Unlock()

	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response <- doBlockRequest(t, p, target, "bytes=0-99")
	}()
	select {
	case <-peerCanceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("recovery peer outlived the rolling-origin fallback while origin had no first byte")
	}
	select {
	case <-response:
		t.Fatal("request completed while origin was still withheld")
	default:
	}
	release()
	if w := <-response; w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if !p.peerPolicy.breakerOpen() {
		t.Fatal("timed-out recovery sample closed the breaker")
	}
}

func TestConcurrentIdenticalHedgesShareOriginSpan(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, blockSize)
	var originCalls atomic.Int32
	bothPeersStarted := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls.Add(1)
		<-bothPeersStarted
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", blockSize-1, blockSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/shared-span.parquet"
	key := BlockKey(target, 0, blockSize)

	var peerArrivals atomic.Int32
	normalPeerCanceled := []chan struct{}{make(chan struct{}), make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		arrival := peerArrivals.Add(1)
		if arrival == 2 {
			close(bothPeersStarted)
		}
		<-r.Context().Done()
		if arrival <= int32(len(normalPeerCanceled)) {
			close(normalPeerCanceled[arrival-1])
		}
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	setTestPeerPolicy(p, 32, 32*blockSize, 10*time.Millisecond)

	start := make(chan struct{})
	statuses := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			u, _ := url.Parse(target)
			req := (&http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
				Header: http.Header{"Range": []string{"bytes=0-99"}}}).WithContext(context.Background())
			w := httptest.NewRecorder()
			if !p.serveBlockAligned(w, req, "bytes=0-99") {
				statuses <- 0
				return
			}
			statuses <- w.Code
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if status := <-statuses; status != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", status)
		}
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("identical concurrent hedges made %d origin calls, want 1", got)
	}
	for i, canceled := range normalPeerCanceled {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatalf("shared origin commit did not cancel peer fill for waiter %d", i+1)
		}
	}
}

func TestOpenBreakerRecoveryProbeDoesNotWaitOrGetCanceledByAdjacentBlocks(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 2*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	originStarted := make(chan struct{})
	var originOnce sync.Once
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		originOnce.Do(func() { close(originStarted) })
		time.Sleep(80 * time.Millisecond)
		start, end, ok := parseAbsoluteRange(r.Header.Get("Range"))
		if !ok {
			t.Fatalf("origin Range = %q, want absolute range", r.Header.Get("Range"))
		}
		end = min(end, int64(len(body))-1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start : end+1])
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/recovery.parquet"

	var peerGets atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		peerGets.Add(1)
		select {
		case <-originStarted:
		case <-time.After(20 * time.Millisecond):
			t.Error("open-breaker request delayed origin for its recovery probe")
		}
		time.Sleep(20 * time.Millisecond)
		for idx := int64(0); idx < 2; idx++ {
			if r.URL.Query().Get("key") == BlockKey(target, idx, blockSize) {
				_, _ = w.Write(body[idx*blockSize : (idx+1)*blockSize])
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.now = func() time.Time { return now }
	cfg.breakerCloseAfter = 1
	p.peerPolicy = newPeerFetchPolicy(cfg)
	p.peerPolicy.mu.Lock()
	p.peerPolicy.open = true
	p.peerPolicy.nextProbe = now
	p.peerPolicy.mu.Unlock()

	w := doBlockRequest(t, p, target, fmt.Sprintf("bytes=0-%d", len(body)-1))
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if got := peerGets.Load(); got != 1 {
		t.Fatalf("recovery peer GETs = %d, want exactly one sampled block", got)
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want one coalesced span; recovery sampling must not serialize origin", got)
	}
	if p.peerPolicy.breakerOpen() {
		t.Fatal("recovered peer beat origin but the breaker remained open")
	}
}

func TestOpenBreakerRecoveryAcceptsPeerWithinLatencyRatioAfterOriginWins(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, blockSize)
	originStarted := make(chan struct{})
	var originOnce sync.Once
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls.Add(1)
		originOnce.Do(func() { close(originStarted) })
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", blockSize-1, blockSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/marginal-recovery.parquet"
	key := BlockKey(target, 0, blockSize)

	peerFinished := make(chan struct{})
	var peerFinishOnce sync.Once
	var peerGets atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		peerGets.Add(1)
		<-originStarted
		time.Sleep(130 * time.Millisecond) // 1.3x origin: slower, but below the 1.5x breaker threshold.
		w.Header().Set("Content-Length", strconv.FormatInt(blockSize, 10))
		_, _ = w.Write(body)
		peerFinishOnce.Do(func() { close(peerFinished) })
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.now = func() time.Time { return now }
	cfg.breakerCloseAfter = 1
	p.peerPolicy = newPeerFetchPolicy(cfg)
	p.peerPolicy.mu.Lock()
	p.peerPolicy.open = true
	p.peerPolicy.nextProbe = now
	p.peerPolicy.mu.Unlock()

	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	select {
	case <-peerFinished:
		t.Fatal("open-breaker request waited for a slower diagnostic peer instead of returning origin")
	default:
	}
	select {
	case <-peerFinished:
	case <-time.After(time.Second):
		t.Fatal("recovery peer was canceled when origin won")
	}
	deadline := time.Now().Add(time.Second)
	for p.peerPolicy.breakerOpen() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.peerPolicy.breakerOpen() {
		t.Fatal("breaker stayed open even though sampled peer latency was within 1.5x origin")
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want 1", got)
	}
	if got := peerGets.Load(); got != 1 {
		t.Fatalf("recovery peer GETs = %d, want 1", got)
	}
}

func TestOpenBreakerRecoveryCancelsPeerAtLatencyRatioBoundary(t *testing.T) {
	const blockSize = int64(1024)
	body := make([]byte, blockSize)
	originStarted := make(chan struct{})
	var originOnce sync.Once
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originOnce.Do(func() { close(originStarted) })
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", blockSize-1, blockSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	t.Cleanup(origin.Close)
	target := origin.URL + "/bucket/slow-recovery.parquet"
	key := BlockKey(target, 0, blockSize)

	peerCanceled := make(chan struct{})
	var canceledOnce sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		<-originStarted
		<-r.Context().Done()
		canceledOnce.Do(func() { close(peerCanceled) })
	})
	peer := httptest.NewServer(mux)
	t.Cleanup(peer.Close)

	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, peerManagerWith([]string{strings.TrimPrefix(peer.URL, "http://")}), nil)
	p.client = origin.Client()
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32*blockSize)
	cfg.now = func() time.Time { return now }
	p.peerPolicy = newPeerFetchPolicy(cfg)
	p.peerPolicy.mu.Lock()
	p.peerPolicy.open = true
	p.peerPolicy.nextProbe = now
	p.peerPolicy.mu.Unlock()

	cancellationsBefore := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("peer"))
	w := doBlockRequest(t, p, target, "bytes=0-99")
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	select {
	case <-peerCanceled:
	case <-time.After(time.Second):
		t.Fatal("recovery peer exceeded 1.5x origin without cancellation")
	}
	if !p.peerPolicy.breakerOpen() {
		t.Fatal("unhealthy recovery sample closed the breaker")
	}
	if got := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("peer")); got != cancellationsBefore+1 {
		t.Fatalf("peer cancellation delta = %v, want 1", got-cancellationsBefore)
	}
}

// TestServeBlockAlignedDoesNotReverifyPastObjectEOF covers the cold-request
// case where the requested end lies past the true object size. The validated
// Content-Range must clamp the response and shrink the block span immediately,
// rather than treating the nonexistent trailing block as a residual miss and
// issuing a phase 1.5 re-fetch that can never fill it.
func TestServeBlockAlignedDoesNotReverifyPastObjectEOF(t *testing.T) {
	const blockSize = 1024
	const objSize = int64(2*blockSize + 100) // true tail is block 2; block 3 doesn't exist
	body := make([]byte, objSize)
	for i := range body {
		body[i] = byte(i % 251)
	}

	var callCount int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&callCount, 1) == 1 {
			// Phase 1's single coalesced fetch: origin honestly reports its
			// true (short) length, like S3 clamping a range past EOF.
			// fetchOriginSpan stores the short tail and returns success —
			// leaving block 3 uncommitted even though this call "succeeded".
			start, end, _ := parseAbsoluteRange(r.Header.Get("Range"))
			if end >= objSize {
				end = objSize - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objSize))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[start : end+1])
			return
		}
		// Any second request indicates that the proxy still believes the
		// nonexistent block after EOF must be fetched.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer origin.Close()

	p, _ := newBlockProxy(t, origin, blockSize)
	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{"bytes=0-4095"}}}
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()

	if !p.serveBlockAligned(w, req, "bytes=0-4095") {
		t.Fatal("expected serveBlockAligned to handle the request (not fall back)")
	}
	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes 0-2147/2148" {
		t.Fatalf("Content-Range = %q, want bytes 0-2147/2148", got)
	}
	if got := w.Body.Len(); got != int(objSize) {
		t.Fatalf("body length = %d, want %d", got, objSize)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("origin calls = %d, want 1 (no impossible past-EOF re-fetch)", callCount)
	}
}

// TestServeBlockAlignedReturns416ForColdPastEOFStart covers a past-EOF start
// before the object size has been learned. The aligned origin fetch starts at
// the beginning of the tail block, whose validated short Content-Range teaches
// the proxy the true size. The client still receives a standards-compliant 416
// instead of a zero-byte 206.
func TestServeBlockAlignedReturns416ForColdPastEOFStart(t *testing.T) {
	const blockSize = 1024
	const objSize = int64(2*blockSize + 100) // tail block 2 has only 100 real bytes
	origin := originServer(t, objSize)
	defer origin.Close()
	p, store := newBlockProxy(t, origin, blockSize)
	target := origin.URL + "/bucket/f.parquet"

	// Request entirely within block 2, but starting 52 bytes past the tail's
	// real end (2048+100=2148): start=2200, well inside the object's nominal
	// [2048, 3072) block range yet past its actual short content.
	u, _ := url.Parse(target)
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{"bytes=2200-3000"}}}
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()

	if !p.serveBlockAligned(w, req, "bytes=2200-3000") {
		t.Fatal("expected serveBlockAligned to handle the request (not fall back)")
	}
	if w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", w.Code)
	}
	if got := w.Header().Get("Content-Range"); got != "bytes */2148" {
		t.Fatalf("Content-Range = %q, want bytes */2148", got)
	}
	if got := w.Body.Len(); got != 0 {
		t.Fatalf("body length = %d, want 0", got)
	}
	if !store.Has(BlockKey(target, 2, blockSize)) {
		t.Fatal("validated tail block was not cached while learning object size")
	}
}

func TestHandleProxyRoutesToBlockMode(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	defer origin.Close()
	p, store := newBlockProxy(t, origin, blockSize)
	p.blockMode = true
	originURL, _ := url.Parse(origin.URL)
	p.cacheHostSuffixes = []string{originURL.Host} // make shouldCache match the test origin

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	req.URL = u
	req.Header.Set("Range", "bytes=100-2100")
	w := httptest.NewRecorder()
	p.HandleProxy(w, req)

	if w.Code != http.StatusPartialContent || w.Body.Len() != 2001 {
		t.Fatalf("block-mode HandleProxy: status %d len %d", w.Code, w.Body.Len())
	}
	// Blocks 0-2 stored under block keys; the legacy exact-range key must NOT exist.
	if store.Has(CacheKey(u.String(), "bytes=100-2100")) {
		t.Fatal("legacy key written in block mode")
	}
	if !store.Has(BlockKey(u.String(), 0, blockSize)) {
		t.Fatal("block 0 missing after block-mode request")
	}
}

func TestHandleProxyBlockModeOffUsesLegacyPath(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	defer origin.Close()
	p, store := newBlockProxy(t, origin, blockSize)
	p.blockMode = false
	originURL, _ := url.Parse(origin.URL)
	p.cacheHostSuffixes = []string{originURL.Host}

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	req.URL = u
	req.Header.Set("Range", "bytes=100-2100")
	p.HandleProxy(httptest.NewRecorder(), req)

	if !store.Has(CacheKey(u.String(), "bytes=100-2100")) {
		t.Fatal("legacy key missing with block mode off")
	}
	if store.Has(BlockKey(u.String(), 0, blockSize)) {
		t.Fatal("block key written with block mode off")
	}
}

// TestServeBlockAlignedConcurrentDriftedRanges is a -race regression test for
// the single-flight-keyed-by-run-end fix described on flushRun: many
// goroutines issue overlapping but differently-shaped ("drifted") cold ranges
// against the same object at once, through one shared DiskCache and one
// shared CacheProxy. Each response body must be byte-correct for exactly the
// range that goroutine asked for — a race in the missing-run coalescing or
// single-flight keying would surface here as wrong bytes, not just a crash.
func TestServeBlockAlignedConcurrentDriftedRanges(t *testing.T) {
	const blockSize = 1024
	const numBlocks = 32
	const objSize = int64(numBlocks * blockSize)
	origin := originServer(t, objSize)
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, blockSize)
	target := origin.URL + "/bucket/f.parquet"

	const numGoroutines = 32
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			// Drift the start by a non-block-aligned offset per goroutine so
			// ranges overlap but don't share block boundaries, and vary the
			// length so missing runs differ in size across concurrent callers.
			start := int64(i * 733 % int(objSize-200))
			length := int64(200 + (i%5)*300)
			end := start + length - 1
			if end >= objSize {
				end = objSize - 1
			}
			rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
			u, _ := url.Parse(target)
			req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
				Header: http.Header{"Range": []string{rangeHeader}}}
			req = req.WithContext(context.Background())
			w := httptest.NewRecorder()
			// t.Errorf (not Fatalf) below: FailNow must only be called from
			// the goroutine running the test function, not spawned ones.
			if !p.serveBlockAligned(w, req, rangeHeader) {
				t.Errorf("goroutine %d: serveBlockAligned returned false (legacy fallback) for %q", i, rangeHeader)
				return
			}
			if w.Code != http.StatusPartialContent {
				t.Errorf("goroutine %d: status %d, want 206 for range %s", i, w.Code, rangeHeader)
				return
			}
			body := w.Body.Bytes()
			wantLen := end - start + 1
			if int64(len(body)) != wantLen {
				t.Errorf("goroutine %d: body length %d, want %d for range %s", i, len(body), wantLen, rangeHeader)
				return
			}
			for j, b := range body {
				if want := byte((start + int64(j)) % 251); b != want {
					t.Errorf("goroutine %d: byte %d (abs %d) = %d, want %d for range %s",
						i, j, start+int64(j), b, want, rangeHeader)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestServeBlockAlignedRejectsDegenerateConfig guards the infinite-loop /
// divide-by-zero hazard: if blockSize or maxSpanBlocks is left at its zero
// value (e.g. Task 4's wiring is skipped), serveBlockAligned must fall back
// to the legacy path rather than hang or panic.
func TestServeBlockAlignedRejectsDegenerateConfig(t *testing.T) {
	tests := []struct {
		name          string
		blockSize     int64
		maxSpanBlocks int64
	}{
		{"zero block size", 0, 8},
		{"negative block size", -1, 8},
		{"zero max span blocks", 1024, 0},
		{"negative max span blocks", 1024, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := originServer(t, 4096)
			defer origin.Close()
			store, err := NewDiskCache(t.TempDir(), 80)
			if err != nil {
				t.Fatal(err)
			}
			p := NewCacheProxy(store, nil, []string{})
			p.client = origin.Client()
			p.blockSize = tt.blockSize
			p.maxSpanBlocks = tt.maxSpanBlocks

			u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
			req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
				Header: http.Header{"Range": []string{"bytes=0-100"}}}
			req = req.WithContext(context.Background())
			if p.serveBlockAligned(httptest.NewRecorder(), req, "bytes=0-100") {
				t.Fatal("expected false (legacy fallback) for degenerate config")
			}
		})
	}
}

type writeHeaderHookRecorder struct {
	*httptest.ResponseRecorder
	hook func()
}

func (w *writeHeaderHookRecorder) WriteHeader(statusCode int) {
	if w.hook != nil {
		hook := w.hook
		w.hook = nil
		hook()
	}
	w.ResponseRecorder.WriteHeader(statusCode)
}

func TestServeBlockAlignedPinsBlocksBeforeHeaders(t *testing.T) {
	const blockSize = int64(1024)
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	store.maxBytes = 2 * blockSize
	target := "http://origin.test/bucket/f.parquet"

	for idx := int64(0); idx < 2; idx++ {
		data := make([]byte, blockSize)
		for i := range data {
			data[i] = byte((idx*blockSize + int64(i)) % 251)
		}
		if _, err := store.PutStream(BlockKey(target, idx, blockSize), strings.NewReader(string(data))); err != nil {
			t.Fatalf("seed block %d: %v", idx, err)
		}
	}

	p := NewCacheProxy(store, nil, []string{})
	p.blockSize = blockSize
	p.maxSpanBlocks = 8
	u, _ := url.Parse(target)
	req := (&http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{"bytes=0-2047"}}}).WithContext(context.Background())
	w := &writeHeaderHookRecorder{ResponseRecorder: httptest.NewRecorder()}
	w.hook = func() {
		// Force eviction precisely after headers are committed. Readers opened
		// before this point remain valid on Unix even though their paths vanish.
		if _, err := store.PutStream(strings.Repeat("f", 64), strings.NewReader(string(make([]byte, blockSize)))); err != nil {
			t.Fatalf("trigger eviction: %v", err)
		}
	}

	if !p.serveBlockAligned(w, req, "bytes=0-2047") {
		t.Fatal("serveBlockAligned unexpectedly fell back")
	}
	if w.Code != http.StatusPartialContent || w.Body.Len() != 2*int(blockSize) {
		t.Fatalf("response status=%d len=%d, want 206 and %d bytes", w.Code, w.Body.Len(), 2*blockSize)
	}
	for i, b := range w.Body.Bytes() {
		if want := byte(int64(i) % 251); b != want {
			t.Fatalf("byte %d = %d, want %d", i, b, want)
		}
	}
}

// TestHandleProxyBlockModeRecordsInflightAndDuration guards the two
// signals the phase-timing instrumentation exists to provide during an
// investigation: the in-flight gauge must return to its pre-request value
// once HandleProxy returns (so it reflects live queue depth, not a leak),
// and a served block-mode request must land exactly one sample in the
// request-duration histogram under its resolved source label.
func TestHandleProxyBlockModeRecordsInflightAndDuration(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, blockSize)
	p.blockMode = true
	originURL, _ := url.Parse(origin.URL)
	p.cacheHostSuffixes = []string{originURL.Host}

	inflightBefore := gaugeValue(t, inflightRequests)
	countBefore := histogramSampleCount(t, requestDurationSeconds.WithLabelValues("block", "s3").(prometheus.Histogram))

	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := httptest.NewRequest(http.MethodGet, u.String(), nil)
	req.URL = u
	req.Header.Set("Range", "bytes=100-2100")
	w := httptest.NewRecorder()
	p.HandleProxy(w, req)

	if w.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", w.Code)
	}
	if got := gaugeValue(t, inflightRequests); got != inflightBefore {
		t.Fatalf("inflightRequests after request = %v, want back to pre-request value %v", got, inflightBefore)
	}
	if got := histogramSampleCount(t, requestDurationSeconds.WithLabelValues("block", "s3").(prometheus.Histogram)); got != countBefore+1 {
		t.Fatalf("requestDurationSeconds{block,s3} sample count delta = %v, want 1", got-countBefore)
	}
}

func TestBlockOriginFetchUpdatesOriginInflightGauge(t *testing.T) {
	const blockSize = 1024
	started := make(chan struct{})
	release := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set("Content-Range", "bytes 0-1023/1024")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, blockSize))
	}))
	t.Cleanup(origin.Close)
	p, _ := newBlockProxy(t, origin, blockSize)
	p.blockMode = true
	originURL, _ := url.Parse(origin.URL)
	p.cacheHostSuffixes = []string{originURL.Host}

	before := gaugeValue(t, originFetchInFlight)
	done := make(chan int, 1)
	go func() {
		u, _ := url.Parse(origin.URL + "/bucket/inflight.parquet")
		req := httptest.NewRequest(http.MethodGet, u.String(), nil)
		req.URL = u
		req.Header.Set("Range", "bytes=0-99")
		w := httptest.NewRecorder()
		p.HandleProxy(w, req)
		done <- w.Code
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("block origin request did not start")
	}
	during := gaugeValue(t, originFetchInFlight)
	close(release)
	if status := <-done; status != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", status)
	}
	if during != before+1 {
		t.Fatalf("originFetchInFlight during block fetch = %v, want %v", during, before+1)
	}
	if got := gaugeValue(t, originFetchInFlight); got != before {
		t.Fatalf("originFetchInFlight after block fetch = %v, want %v", got, before)
	}
}

func TestHandleProxyOversizedBlockSpanFallsBackToLegacy(t *testing.T) {
	const blockSize = int64(1024)
	origin := originServer(t, 4*blockSize)
	defer origin.Close()
	p, store := newBlockProxy(t, origin, blockSize)
	store.maxBytes = 2 * blockSize
	p.blockMode = true
	originURL, _ := url.Parse(origin.URL)
	p.cacheHostSuffixes = []string{originURL.Host}
	target := origin.URL + "/bucket/f.parquet"
	u, _ := url.Parse(target)
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.URL = u
	req.Header.Set("Range", "bytes=0-3071")
	w := httptest.NewRecorder()

	p.HandleProxy(w, req)

	if w.Code != http.StatusPartialContent || w.Body.Len() != 3*int(blockSize) {
		t.Fatalf("response status=%d len=%d, want legacy 206 and %d bytes", w.Code, w.Body.Len(), 3*blockSize)
	}
	if !store.Has(CacheKey(target, "bytes=0-3071")) {
		t.Fatal("legacy exact-range entry was not written")
	}
	for idx := int64(0); idx < 3; idx++ {
		if store.Has(BlockKey(target, idx, blockSize)) {
			t.Fatalf("block %d was written before oversized-span fallback", idx)
		}
	}
}
