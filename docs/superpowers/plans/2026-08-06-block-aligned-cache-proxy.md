# Block-Aligned Cache Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-key the node-local NVMe cache on fixed-size blocks per S3 object (instead of exact request ranges) so cache hits are independent of DuckDB's read-boundary drift, then disable parquet prefetching on workers to eliminate cold-scan read amplification without losing warm performance.

**Architecture:** The cache proxy (`cmd/cache-proxy/`, a per-node DaemonSet) currently keys entries by `sha256(url|Range)`. DuckDB's lazy (non-prefetch) reads produce differently-aligned ranges on every execution, so identical repeat workloads miss 100% of the time (measured: 0 of 7,370 repeat ranges matched) and re-fetch from S3. This plan decomposes every absolute-range GET into fixed 8 MiB blocks keyed `sha256(url|blk|idx|blockSize)`, serves any sub-range from stored blocks (local → peer → one coalesced origin fetch for missing runs), and keeps the legacy exact-range path as fallback for non-standard range shapes. Once block mode is validated, a worker-side env flag sets `disable_parquet_prefetching=true`, which is a measured 2.6–3.5× cold-scan speedup (57.8s vs 179.9s narrow; 141.7s vs 363.2s wide; 34× fewer bytes) that is only safe to ship once the cache serves drifted ranges.

**Tech Stack:** Go (stdlib `net/http`, `httptest`), Prometheus client, existing `DiskCache`/`PeerManager`/`singleFlight` machinery.

## Global Constraints

- **Range rewriting is legal:** DuckDB httpfs SigV4 signs only `host;x-amz-content-sha256;x-amz-date` (`duckdb-httpfs/src/s3fs.cpp:84`, documented in `proxy.go` `forwardUncached` comment). The `Range` header is NOT signed; the proxy may fetch different ranges than the client requested. Task 2 adds a runtime guard anyway.
- **Feature-flagged, default off:** block mode ships behind `CACHE_BLOCK_MODE=on` (default `off`). The legacy exact-range path is preserved untouched and remains the fallback for non-`bytes=start-end` ranges even when block mode is on.
- **Peer HTTP API unchanged:** `HandlePeerHas`/`HandlePeerGet` take opaque 64-hex keys; block keys are the same shape and pass `IsValidCacheKey` unchanged. Old-format and block-format entries coexist (different keys, no collision); old entries age out naturally (cache is at ~7% fill, zero evictions).
- **Response-shape compatibility:** mirror the legacy `serveStream` behavior exactly — `206 Partial Content` with `Content-Range: bytes <start>-<end>/<served-size>` when the client sent a Range header. Do not "fix" the Content-Range total to the object size; DuckDB tolerates the current shape and changing two variables at once masks regressions.
- **DuckLake parquet files are immutable** — no etag validation, no TTL, no content invalidation (matches existing design).
- Go style: table tests, `slog` structured logging, `promauto` metrics, conventional commits.

## Out of Scope (follow-up plans)

- Write-through cache warming from the sink's upload path.
- Worker→node soft affinity per org.
- Per-org cache accounting/quotas.
- Upstream DuckDB fix (coalesce prefetch only within projected columns) — file the issue, but don't block on it.

## File Structure

- Create: `cmd/cache-proxy/blocks.go` — pure functions: range parsing, block-span math, `BlockKey`.
- Create: `cmd/cache-proxy/blocks_test.go`
- Create: `cmd/cache-proxy/block_serve.go` — `serveBlockAligned` (assembly: local → peer → origin), `fetchOriginSpan` (one coalesced origin GET split into per-block cache entries).
- Create: `cmd/cache-proxy/block_serve_test.go`
- Modify: `cmd/cache-proxy/proxy.go` — branch in `HandleProxy` (~line 284), new metrics.
- Modify: `cmd/cache-proxy/main.go` — env config plumbing.
- Modify: `tests/mw-dev/manifests.tmpl.yaml` — mw-dev env vars for rollout.
- Modify: `server/server.go` — `applyParquetPrefetchPolicy` next to `applyHTTPFSRetryBudget` (line 1071).
- Modify: `cmd/cache-proxy/README.md` — document block mode.

---

### Task 1: Block math and keys (`blocks.go`)

**Files:**
- Create: `cmd/cache-proxy/blocks.go`
- Test: `cmd/cache-proxy/blocks_test.go`

**Interfaces:**
- Produces: `parseAbsoluteRange(rangeHeader string) (start, end int64, ok bool)`; `blockSpan(start, end, blockSize int64) (firstIdx, lastIdx int64)`; `BlockKey(url string, blockIdx, blockSize int64) string` (64-hex, passes `IsValidCacheKey`). Task 3 consumes all three.

- [ ] **Step 1: Write the failing tests**

```go
package main

import "testing"

func TestParseAbsoluteRange(t *testing.T) {
	tests := []struct {
		name   string
		header string
		start  int64
		end    int64
		ok     bool
	}{
		{"absolute", "bytes=100-199", 100, 199, true},
		{"zero start", "bytes=0-0", 0, 0, true},
		{"large offsets", "bytes=43730341-82843457", 43730341, 82843457, true},
		{"suffix form", "bytes=-500", 0, 0, false},
		{"open ended", "bytes=100-", 0, 0, false},
		{"multi range", "bytes=0-1,5-9", 0, 0, false},
		{"empty", "", 0, 0, false},
		{"garbage", "bytes=a-b", 0, 0, false},
		{"inverted", "bytes=200-100", 0, 0, false},
		{"no prefix", "100-199", 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseAbsoluteRange(tt.header)
			if ok != tt.ok || start != tt.start || end != tt.end {
				t.Fatalf("parseAbsoluteRange(%q) = (%d, %d, %v); want (%d, %d, %v)",
					tt.header, start, end, ok, tt.start, tt.end, tt.ok)
			}
		})
	}
}

func TestBlockSpan(t *testing.T) {
	const bs = 8 << 20 // 8 MiB
	tests := []struct {
		name     string
		start    int64
		end      int64
		firstIdx int64
		lastIdx  int64
	}{
		{"within first block", 0, 100, 0, 0},
		{"exact block", 0, bs - 1, 0, 0},
		{"crosses boundary", bs - 1, bs, 0, 1},
		{"multi block", 0, 3*bs - 1, 0, 2},
		{"interior", 2*bs + 5, 4*bs + 5, 2, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, last := blockSpan(tt.start, tt.end, bs)
			if first != tt.firstIdx || last != tt.lastIdx {
				t.Fatalf("blockSpan(%d, %d) = (%d, %d); want (%d, %d)",
					tt.start, tt.end, first, last, tt.firstIdx, tt.lastIdx)
			}
		})
	}
}

func TestBlockKey(t *testing.T) {
	k1 := BlockKey("http://s3/bucket/f.parquet", 0, 8<<20)
	k2 := BlockKey("http://s3/bucket/f.parquet", 1, 8<<20)
	k3 := BlockKey("http://s3/bucket/f.parquet", 0, 16<<20)
	if !IsValidCacheKey(k1) {
		t.Fatalf("BlockKey output %q is not a valid cache key", k1)
	}
	if k1 == k2 {
		t.Fatal("different block indexes must produce different keys")
	}
	if k1 == k3 {
		t.Fatal("different block sizes must produce different keys")
	}
	if k1 != BlockKey("http://s3/bucket/f.parquet", 0, 8<<20) {
		t.Fatal("BlockKey must be deterministic")
	}
	if k1 == CacheKey("http://s3/bucket/f.parquet", "bytes=0-100") {
		t.Fatal("block keys must not collide with legacy keys")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/cache-proxy && go test -run 'TestParseAbsoluteRange|TestBlockSpan|TestBlockKey' -v`
Expected: FAIL — `undefined: parseAbsoluteRange` etc.

- [ ] **Step 3: Implement `blocks.go`**

```go
package main

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

// parseAbsoluteRange parses a Range header of the exact form "bytes=start-end"
// (both bounds present, start <= end). Every other shape — suffix ("bytes=-N"),
// open-ended ("bytes=N-"), multi-range, empty — returns ok=false and the
// caller falls back to the legacy exact-range cache path. DuckDB httpfs knows
// file sizes up front (it HEADs first) and only issues absolute ranges, so the
// fallback is expected to be cold-path only.
func parseAbsoluteRange(rangeHeader string) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(rangeHeader, "bytes=")
	if !found {
		return 0, 0, false
	}
	lo, hi, found := strings.Cut(spec, "-")
	if !found || lo == "" || hi == "" || strings.Contains(hi, ",") {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	end, err = strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

// blockSpan returns the inclusive index range of blocks covering [start, end].
func blockSpan(start, end, blockSize int64) (firstIdx, lastIdx int64) {
	return start / blockSize, end / blockSize
}

// BlockKey computes the cache key for one block of an object. blockSize is
// part of the key so a block-size config change can never serve a
// wrong-sized entry — old-size entries simply become unreachable and age out.
func BlockKey(url string, blockIdx, blockSize int64) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|blk|%d|%d", url, blockIdx, blockSize)
	return fmt.Sprintf("%x", h.Sum(nil))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/cache-proxy && go test -run 'TestParseAbsoluteRange|TestBlockSpan|TestBlockKey' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/cache-proxy/blocks.go cmd/cache-proxy/blocks_test.go
git commit -m "feat(cache-proxy): block-aligned range math and cache keys"
```

---

### Task 2: Coalesced origin span fetch (`fetchOriginSpan`)

**Files:**
- Create: `cmd/cache-proxy/block_serve.go`
- Test: `cmd/cache-proxy/block_serve_test.go`

**Interfaces:**
- Consumes: `BlockKey` (Task 1); `DiskCache.PutStream(key string, r io.Reader) (int64, error)`, `p.client`, `p.originTimeout`, `hopByHop` (existing).
- Produces: `(p *CacheProxy) fetchOriginSpan(r *http.Request, blockSize, firstIdx, lastIdx int64) error` — fetches blocks `[firstIdx, lastIdx]` of `r.URL` in ONE origin range GET and commits each block under its `BlockKey`. Task 3 consumes it.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/cache-proxy && go test -run TestFetchOriginSpan -v`
Expected: FAIL — `undefined: (*CacheProxy).fetchOriginSpan`

- [ ] **Step 3: Implement `fetchOriginSpan` in `block_serve.go`**

```go
package main

import (
	"context"
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
```

Note: if `PutStream` rejects zero-byte writes (check its implementation at `cache.go:229` while implementing — if a `size == 0` entry would be committed for an `idx` past the object end, drop it with `store.dropLocked`-style cleanup or skip commit by checking `size == 0` before treating the block as stored; the span math in Task 3 never requests blocks past `end`, so this only matters when the client's `end` exceeds the object size, which S3 clamps).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/cache-proxy && go test -run TestFetchOriginSpan -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/cache-proxy/block_serve.go cmd/cache-proxy/block_serve_test.go
git commit -m "feat(cache-proxy): coalesced block-aligned origin span fetch"
```

---

### Task 3: Block-aligned serve path (`serveBlockAligned`)

**Files:**
- Modify: `cmd/cache-proxy/block_serve.go`
- Test: `cmd/cache-proxy/block_serve_test.go`

**Interfaces:**
- Consumes: `parseAbsoluteRange`, `blockSpan`, `BlockKey` (Task 1); `fetchOriginSpan` (Task 2); `DiskCache.Open/openFile/Has`, `PeerManager.FetchFromPeers`, `p.flights.Do`, `cacheHitsTotal`, `cacheMissesTotal`, `cacheBytesServed` (existing).
- Produces: `(p *CacheProxy) serveBlockAligned(w http.ResponseWriter, r *http.Request, rangeHeader string) bool` — returns `false` if the request is not block-servable (caller runs the legacy path). Task 4 consumes it. `p.blockSize int64` and `p.maxSpanBlocks int64` fields on `CacheProxy`.

- [ ] **Step 1: Write the failing tests**

```go
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
	if got := w.Header().Get("Content-Range"); got != "bytes 1500-3500/2001" {
		t.Fatalf("Content-Range %q; want legacy served-size shape %q", got, "bytes 1500-3500/2001")
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

func TestServeBlockAlignedFallsBackOnRangeShape(t *testing.T) {
	origin := originServer(t, 4096)
	defer origin.Close()
	p, _ := newBlockProxy(t, origin, 1024)
	u, _ := url.Parse(origin.URL + "/bucket/f.parquet")
	req := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host,
		Header: http.Header{"Range": []string{"bytes=-500"}}}
	req = req.WithContext(context.Background())
	if p.serveBlockAligned(httptest.NewRecorder(), req, "bytes=-500") {
		t.Fatal("suffix range must return false (legacy fallback)")
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/cache-proxy && go test -run TestServeBlockAligned -v`
Expected: FAIL — `undefined: (*CacheProxy).serveBlockAligned`, missing fields.

- [ ] **Step 3: Implement `serveBlockAligned`**

Add fields to `CacheProxy` in `proxy.go` (`blockMode bool`, `blockSize int64`, `maxSpanBlocks int64` — wired in Task 4), then in `block_serve.go`:

```go
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
```

Add `"errors"` to imports. Note the `fetchResult{}` sentinel through `p.flights.Do`: the single-flight keyed on the run's first block dedups concurrent identical span fetches; duplicate fetches of overlapping spans from different starting blocks are benign (atomic rename, idempotent immutable content).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/cache-proxy && go test -run TestServeBlockAligned -v`
Expected: PASS

- [ ] **Step 5: Run the full package suite for regressions**

Run: `cd cmd/cache-proxy && go test ./... -count=1`
Expected: PASS (legacy path untouched)

- [ ] **Step 6: Commit**

```bash
git add cmd/cache-proxy/block_serve.go cmd/cache-proxy/block_serve_test.go cmd/cache-proxy/proxy.go
git commit -m "feat(cache-proxy): block-aligned serve path with peer and coalesced origin fill"
```

---

### Task 4: Wire block mode into `HandleProxy` + env config

**Files:**
- Modify: `cmd/cache-proxy/proxy.go` (`HandleProxy`, ~line 284, right after `rangeHeader := r.Header.Get("Range")`)
- Modify: `cmd/cache-proxy/main.go` (env parsing where `CACHE_MAX_PERCENT` is read)
- Test: `cmd/cache-proxy/block_serve_test.go`

**Interfaces:**
- Consumes: `serveBlockAligned` (Task 3).
- Produces: env contract `CACHE_BLOCK_MODE` (`on`/`off`, default `off`), `CACHE_BLOCK_SIZE_BYTES` (default `8388608`), `CACHE_BLOCK_MAX_SPAN_BLOCKS` (default `8`). Tasks 6/8 (rollout) consume the env contract.

- [ ] **Step 1: Write the failing test**

```go
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
```

(Adapt the `shouldCache` wiring to how `cacheHostSuffixes` is actually stored on `CacheProxy` — check `NewCacheProxy` at `proxy.go:112` when implementing; if it's a private field of a different name, set it the way `proxy_test.go` already does.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/cache-proxy && go test -run TestHandleProxy -v`
Expected: new tests FAIL (block mode never routes).

- [ ] **Step 3: Implement the branch and env plumbing**

In `HandleProxy`, immediately after `rangeHeader := r.Header.Get("Range")` and before the legacy `cacheKey := CacheKey(...)` line:

```go
	if p.blockMode && p.serveBlockAligned(w, r, rangeHeader) {
		return
	}
	// Legacy exact-range path (also the fallback for non-absolute ranges).
```

In `main.go`, next to the existing env parsing:

```go
	blockMode := os.Getenv("CACHE_BLOCK_MODE") == "on"
	blockSize := envInt64("CACHE_BLOCK_SIZE_BYTES", 8<<20)
	maxSpanBlocks := envInt64("CACHE_BLOCK_MAX_SPAN_BLOCKS", 8)
```

with an `envInt64(name string, def int64) int64` helper (parse with `strconv.ParseInt`, warn + default on error), assigned onto the `CacheProxy` after `NewCacheProxy`, and logged at startup:

```go
	slog.Info("Block mode configured.", "enabled", blockMode, "block_size", blockSize, "max_span_blocks", maxSpanBlocks)
```

- [ ] **Step 4: Run the full package suite**

Run: `cd cmd/cache-proxy && go test ./... -count=1`
Expected: PASS

- [ ] **Step 5: Update `cmd/cache-proxy/README.md`**

Add a "Block-aligned mode" section: the three env vars, the key format `sha256(url|blk|idx|blockSize)`, the drift problem it solves (identical repeat workloads previously missed 100% — 0/7,370 range keys matched between two runs of the same query), first-touch amplification tradeoff (a 40 KB footer read on an uncached object fetches one full block; bounded by block size, amortized by immutability), and the fallback matrix (suffix/open/multi ranges, non-GET, non-bucket → legacy path).

- [ ] **Step 6: Commit**

```bash
git add cmd/cache-proxy/proxy.go cmd/cache-proxy/main.go cmd/cache-proxy/README.md cmd/cache-proxy/block_serve_test.go
git commit -m "feat(cache-proxy): CACHE_BLOCK_MODE flag wiring and docs"
```

---

### Task 5: Peer round-trip for block keys

**Files:**
- Test: `cmd/cache-proxy/peers_test.go`

**Interfaces:**
- Consumes: `BlockKey` (Task 1), `HandlePeerHas`/`HandlePeerGet` (`proxy.go:775/789`, unchanged).

- [ ] **Step 1: Write the test** (follow the existing peer test pattern in `peers_test.go` — two proxies with real `DiskCache`s, the second's `PeerManager` pointed at the first's test server):

```go
func TestPeerServesBlockKeys(t *testing.T) {
	store, _ := NewDiskCache(t.TempDir(), 80)
	key := BlockKey("http://s3/bucket/f.parquet", 3, 8<<20)
	if _, err := store.PutStream(key, strings.NewReader("block-content")); err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(store, nil, []string{})

	// HandlePeerHas must recognize the block key.
	hasReq := httptest.NewRequest(http.MethodGet, "/peer/has?key="+key, nil)
	hasW := httptest.NewRecorder()
	p.HandlePeerHas(hasW, hasReq)
	if hasW.Code != http.StatusOK {
		t.Fatalf("HandlePeerHas(%s) = %d, want 200", key, hasW.Code)
	}

	// HandlePeerGet must stream it.
	getReq := httptest.NewRequest(http.MethodGet, "/peer/get?key="+key, nil)
	getW := httptest.NewRecorder()
	p.HandlePeerGet(getW, getReq)
	if getW.Code != http.StatusOK || getW.Body.String() != "block-content" {
		t.Fatalf("HandlePeerGet: %d %q", getW.Code, getW.Body.String())
	}
}
```

(Adjust the paths/params to match how `HandlePeerHas`/`HandlePeerGet` actually read the key — check the handlers at `proxy.go:775/789` and mirror the existing tests.)

- [ ] **Step 2: Run it**

Run: `cd cmd/cache-proxy && go test -run TestPeerServesBlockKeys -v`
Expected: PASS immediately (block keys are ordinary 64-hex keys). If it fails, the peer API made an assumption about key provenance — fix the handler, not the test.

- [ ] **Step 3: Commit**

```bash
git add cmd/cache-proxy/peers_test.go
git commit -m "test(cache-proxy): peer round-trip covers block-format keys"
```

---

### Task 6: mw-dev rollout + drift validation

**Correction (found during execution):** the cache-proxy DaemonSet is NOT defined in this repo — `tests/mw-dev/manifests.tmpl.yaml` only holds the per-PR e2e stack, and the DaemonSet deploys from the charts/cloud-infra repo for mw-dev and prod alike. Step 1's env change happens there; this repo ships only the image (`.github/workflows/container-image-cache-proxy-cd.yml`).

**Files:** none here — Step 1 lands in the charts/cloud-infra repo.

- [ ] **Step 1: Add env to the mw-dev cache-proxy DaemonSet spec**

```yaml
            - name: CACHE_BLOCK_MODE
              value: "on"
            - name: CACHE_BLOCK_SIZE_BYTES
              value: "8388608"
```

(Match the indentation/structure of the existing env entries in that DaemonSet block.)

- [ ] **Step 2: Deploy to mw-dev and confirm startup**

Deploy the way duckgres normally ships to mw-dev (see repo `CLAUDE.md` / `tests/mw-dev/` docs). Then:

```bash
kubectl --context mw-dev logs -n duckgres -l app=duckgres-cache-proxy --tail=50 | grep "Block mode configured"
```

Expected: `enabled=true block_size=8388608`.

- [ ] **Step 3: Run the drift validation — the experiment that motivated this plan**

Against an mw-dev org endpoint with a multi-GB ducklake table (psql, standard worker via `options='-c duckgres.worker_cpu=4'`, keepalives on):

1. Run a narrow aggregation twice in one session, e.g. `SELECT count(*) FROM <schema>.<table> WHERE "timestamp" >= '...' AND "timestamp" < '...';`
2. Between runs, snapshot `cache_proxy_origin_bytes_total` from the proxy metrics (or sum `source=miss` bytes from proxy logs for the table's URL path).

Pass criteria:
- Run 2 origin bytes ≈ 0 (< 1% of run 1) — with legacy keying this was 100% re-fetch.
- Run 2 wall-clock ≤ run 1 warm time under legacy mode (no regression from assembly overhead).
- Zero `cache_proxy_block_fallback_total{reason="range_shape"}` growth during the runs (DuckDB only sends absolute ranges; growth means the parser is too strict — investigate before prod).

- [ ] **Step 4: Soak with the sink**

Leave block mode on in mw-dev for at least one full data-import cycle; confirm no `block_fallback_total{reason="origin_error"}` growth and no sink pipeline errors (the sink's `read_parquet` verification queries also flow through the proxy).

- [ ] **Step 5: Commit**

```bash
git add tests/mw-dev/manifests.tmpl.yaml
git commit -m "chore(mw-dev): enable cache-proxy block mode"
```

---

### Task 7: Worker prefetch policy flag

**Files:**
- Modify: `server/server.go` (next to `applyHTTPFSRetryBudget`, line 1071, and its call site)
- Test: wherever `applyHTTPFSRetryBudget` is covered (check `server/server_test.go`; if it has no direct test, assert statement generation only)

**Interfaces:**
- Consumes: the `applyHTTPFSRetryBudget` pattern — post-attach, `SET GLOBAL`, warn-only on failure.
- Produces: env contract `DUCKGRES_DISABLE_PARQUET_PREFETCHING` (`true`/unset, default unset=off). Task 8 consumes it.

- [ ] **Step 1: Write the failing test**

```go
func TestParquetPrefetchPolicyStatements(t *testing.T) {
	if got := parquetPrefetchPolicyStatements(false); len(got) != 0 {
		t.Fatalf("disabled policy must produce no statements, got %v", got)
	}
	got := parquetPrefetchPolicyStatements(true)
	want := []string{"SET GLOBAL disable_parquet_prefetching = true"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("got %v, want %v", got, want)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./server/ -run TestParquetPrefetchPolicy -v`
Expected: FAIL — undefined function.

- [ ] **Step 3: Implement**

```go
// parquetPrefetchPolicyStatements returns the SET statements for the
// deployment's parquet prefetch policy. Prefetch coalesces reads across
// non-projected columns on remote files — measured at up to ~50x byte
// amplification on narrow scans of wide tables — so deployments whose
// cache proxy runs in block-aligned mode (which serves drifted lazy-read
// ranges from cache) turn it off. SET GLOBAL for the same reason as
// applyHTTPFSRetryBudget: workers recycle connections between sessions.
func parquetPrefetchPolicyStatements(disablePrefetch bool) []string {
	if !disablePrefetch {
		return nil
	}
	return []string{"SET GLOBAL disable_parquet_prefetching = true"}
}
```

At `applyHTTPFSRetryBudget`'s call site, apply the statements the same warn-only way, gated on the config value; thread the env through however `applyHTTPFSRetryBudget`'s caller gets its config (follow the existing pattern — likely a field on the server/duckdbservice config populated from `DUCKGRES_DISABLE_PARQUET_PREFETCHING` where other `DUCKGRES_*` envs are read). Grep `DUCKGRES_EXPLORATORY_TIER_ENABLED` for the canonical env-to-config path and mirror it.

- [ ] **Step 4: Run tests**

Run: `go test ./server/ -run TestParquetPrefetchPolicy -v` — PASS.
Run the package suite: `go test ./server/ -count=1` — PASS.

- [ ] **Step 5: Enable in mw-dev and re-run the Task 6 Step 3 validation**

Set `DUCKGRES_DISABLE_PARQUET_PREFETCHING=true` on the mw-dev worker deployment env (same manifest file as Task 6). Re-run the two-run drift test plus one wide-projection query (`SELECT max(length(<large-text-column>)) FROM ...`). Pass criteria:
- Cold narrow scan: bytes through proxy within ~2× of the column's actual compressed size (vs ~50× with prefetch on).
- Warm repeat: served from blocks, origin bytes ≈ 0, wall-clock at or better than prefetch-on warm.
- Wide query cold and warm: no regression vs prefetch-on block mode (measured baseline expectation: prefetch-off is *faster* cold even for wide reads).

- [ ] **Step 6: Commit**

```bash
git add server/server.go server/server_test.go tests/mw-dev/manifests.tmpl.yaml
git commit -m "feat(server): env-gated disable_parquet_prefetching worker policy"
```

---

### Task 8: Production rollout

**Files:** none in this repo — prod env values live in the charts/cloud-infra deploy repo (same place mw-prod worker/proxy env is set today). This task is the runbook.

- [ ] **Step 1: Enable `CACHE_BLOCK_MODE=on` on the prod cache-proxy DaemonSet (proxy only — prefetch stays on).** Watch for 24h:
  - `Cache hit ratio` on the duckgres-cache-proxy Grafana dashboard: expect climb from ~60% baseline toward 85%+ as block entries populate (mixed traffic during transition; old exact-range entries still serve legacy-path requests).
  - `cache_proxy_block_fallback_total` by reason: `range_shape` should be ~0; any sustained growth means real traffic uses range forms the parser rejects — capture samples from proxy logs before proceeding.
  - No growth in worker-side query failures (`ducklake.system.query_log` exception rates per org).
  - Revert = flip the env back; legacy keys were never removed.
- [ ] **Step 2: Enable `DUCKGRES_DISABLE_PARQUET_PREFETCHING=true` on prod workers.** Watch for 48h:
  - `cache_proxy_origin_bytes_total` rate: expect a large drop (amplified fetches gone) — this is the S3 egress cost lever.
  - p50/p95 of `duckgres_query_duration_seconds` per org: cold-heavy orgs should improve markedly; no org should regress >10% warm.
  - `Bytes served by source`: total volume should drop ~an order of magnitude during heavy scans (amplification removed), with the `s3` share shrinking toward first-touch-only.
  - Revert = flip this env back first (prefetch returns, block mode still on — that combination is strictly safe: deterministic prefetch ranges hit block cache fine).
- [ ] **Step 3: Success snapshot.** Re-run the original benchmark on a real org table and record in the PR: month-bucket full-table aggregation, fresh standard worker — target cold ≤ ~4 min (from 13.7–65+ min) and warm repeat ≤ legacy warm, with run-2 origin bytes ≈ 0. Update `docs/metrics.md` if it enumerates proxy metrics, adding `cache_proxy_origin_bytes_total`, `cache_proxy_block_reads_total`, `cache_proxy_block_fallback_total`.

#### Pre-flag-flip checklist (from final review)

- [ ] `fetchOriginSpan` has no internal retry loop, unlike legacy `fetchOrigin`'s 4-attempt backoff. Accept as-is (httpfs itself retries up to 10x on the client side, and each retry makes progress because blocks already committed by an earlier attempt stay cached) or add retry to `fetchOriginSpan` before flipping the flag.
- [ ] Origin fetch metrics (the `cache_proxy_origin_fetches_total` family) are not incremented on the block-mode path. Existing dashboard panels built on that metric will go dark as block-mode traffic comes to dominate — wire block-mode origin fetches into the same metric family, or annotate the affected dashboard panels, before flipping the flag.
- [ ] Per-block sequential peer probing in Phase 1 can stall roughly 1 second per missing block when a peer is unresponsive (no per-peer timeout tuning yet). Consider probing peers per contiguous missing run instead of per block before flipping the flag, to bound worst-case stall time.
- [ ] The block-aligned path emits no trace spans — the legacy `cache.get` span (and its child spans) simply vanish for block-mode traffic. Anyone correlating a duckgres query trace to proxy behavior loses that path once block mode is on; either add spans to `serveBlockAligned`/`fetchOriginSpan` or document the tracing gap for on-call before flipping the flag.
- [ ] Reverting `DUCKGRES_DISABLE_PARQUET_PREFETCHING` only takes effect on newly-recycled workers: the setting is applied via `SET GLOBAL`, which persists on already-running workers until they recycle. A rollback of this env var does not immediately restore prefetching fleet-wide — factor that lag into rollback runbooks.

---

## Self-Review Notes

- **Spec coverage:** drift fix (Tasks 1–5), rollout gating (4, 6, 8), prefetch flip only after block mode validated (7 ordered after 6, prod ordering enforced in 8), origin-offload SLI (`cache_proxy_origin_bytes_total`, Tasks 2/8), legacy compatibility (Global Constraints + Task 4 off-mode test).
- **Known simplifications, deliberate:** no per-URL object-size cache (S3 clamps over-long spans; short tail handled by `PutStream` size); request-level hit/miss redefined as "no origin traffic" (documented in Task 3 code); suffix/open ranges fall back to legacy rather than being block-served (DuckDB doesn't emit them; counter watches for surprises).
- **Type consistency check:** `parseAbsoluteRange`/`blockSpan`/`BlockKey` signatures match across Tasks 1→3; `fetchOriginSpan(r, blockSize, firstIdx, lastIdx)` matches 2→3; env names match 4→6→8; `parquetPrefetchPolicyStatements(bool) []string` self-contained in 7.
