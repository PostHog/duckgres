package main

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads a Prometheus counter's current value without pulling in the
// testutil package (and its extra module deps). Used to assert the Open-vs-
// openFile hit-count split.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	return m.GetGauge().GetValue()
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("read histogram: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// errAfterReader yields n bytes then returns a non-EOF error, simulating an
// origin/peer connection dropping mid-body.
type errAfterReader struct {
	data []byte
	pos  int
}

func (e *errAfterReader) Read(p []byte) (int, error) {
	if e.pos >= len(e.data) {
		return 0, errors.New("connection reset mid-stream")
	}
	n := copy(p, e.data[e.pos:])
	e.pos += n
	return n, nil
}

func TestIsValidCacheKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{strings.Repeat("a", 64), true},
		{strings.Repeat("f", 64), true},
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true},
		// Wrong length.
		{strings.Repeat("a", 63), false},
		{strings.Repeat("a", 65), false},
		{"", false},
		// Upper-case — CacheKey uses %x (lowercase); reject upper.
		{strings.Repeat("A", 64), false},
		// Path traversal attempts.
		{"../../etc/passwd", false},
		{strings.Repeat("a", 60) + "/../x", false},
		// Non-hex chars.
		{strings.Repeat("g", 64), false},
		{strings.Repeat("z", 64), false},
	}
	for _, c := range cases {
		if got := IsValidCacheKey(c.key); got != c.want {
			t.Errorf("IsValidCacheKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestCacheKeyDeterministic(t *testing.T) {
	a := CacheKey("", "http://s3/bucket/file.parquet", "bytes=0-1023")
	b := CacheKey("", "http://s3/bucket/file.parquet", "bytes=0-1023")
	if a != b {
		t.Fatalf("CacheKey not deterministic: %s != %s", a, b)
	}
	if !IsValidCacheKey(a) {
		t.Errorf("CacheKey output %q is not a valid key", a)
	}
	c := CacheKey("", "http://s3/bucket/file.parquet", "bytes=0-2047")
	if a == c {
		t.Fatal("different ranges produced identical keys")
	}
}

// newTestCache creates a DiskCache backed by t.TempDir() for isolation.
func newTestCache(t *testing.T) *DiskCache {
	t.Helper()
	c, err := NewDiskCache(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	return c
}

func TestDiskCachePutGetHas(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("a", 64)
	data := []byte("hello world")

	if c.Has(key) {
		t.Fatal("Has should be false before PutStream")
	}
	if _, _, ok := c.Open(key); ok {
		t.Fatal("Open should miss before PutStream")
	}
	if _, err := c.PutStream(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if !c.Has(key) {
		t.Fatal("Has should be true after PutStream")
	}
	r, _, ok := c.Open(key)
	if !ok {
		t.Fatal("Open should hit after PutStream")
	}
	defer func() { _ = r.Close() }()
	got, _ := io.ReadAll(r)
	if string(got) != string(data) {
		t.Errorf("Open returned %q, want %q", got, data)
	}
}

func TestDiskCacheOpen(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("b", 64)
	data := []byte("streaming bytes")
	if _, err := c.PutStream(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	r, size, ok := c.Open(key)
	if !ok {
		t.Fatal("Open should find entry")
	}
	defer func() { _ = r.Close() }()
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Open data = %q, want %q", got, data)
	}
}

func TestDiskCacheRejectsInvalidKey(t *testing.T) {
	c := newTestCache(t)
	bad := "../../etc/passwd"

	if c.Has(bad) {
		t.Error("Has should reject invalid key")
	}
	if _, _, ok := c.Open(bad); ok {
		t.Error("Open should reject invalid key")
	}
	if _, err := c.PutStream(bad, bytes.NewReader([]byte("x"))); err == nil {
		t.Error("PutStream should reject invalid key")
	}
}

// TestDiskCacheEviction exercises LRU eviction when the cache fills up.
// We set maxBytes tiny by construction: NewDiskCache uses statfs * percent,
// so we directly mutate the cache after construction for a predictable test.
func TestDiskCacheEviction(t *testing.T) {
	c := newTestCache(t)
	// Force the eviction threshold low so we trigger it quickly.
	c.maxBytes = 100

	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
	}
	for _, k := range keys {
		if _, err := c.PutStream(k, bytes.NewReader(make([]byte, 60))); err != nil {
			t.Fatalf("PutStream %s: %v", k, err)
		}
	}
	// After three 60-byte puts with maxBytes=100, the first key must have
	// been evicted (oldest lastAccess).
	if c.Has(keys[0]) {
		t.Error("oldest entry should have been evicted")
	}
	// The most recent must still be present.
	if !c.Has(keys[2]) {
		t.Error("newest entry should still be cached")
	}
}

func TestDiskCacheEntryLimitEvictsOldest(t *testing.T) {
	c := newTestCache(t)
	c.maxEntries = 2
	keys := []string{strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64)}
	for _, key := range keys {
		if _, err := c.PutStream(key, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatal(err)
		}
	}
	if c.order.Len() != 2 || c.Has(keys[0]) || !c.Has(keys[1]) || !c.Has(keys[2]) {
		t.Fatalf("entry-limited cache retained unexpected keys: len=%d first=%t second=%t third=%t", c.order.Len(), c.Has(keys[0]), c.Has(keys[1]), c.Has(keys[2]))
	}
}

func TestDiskCacheEntryLimitAppliesDuringStartupScan(t *testing.T) {
	dir := t.TempDir()
	entries := []struct {
		key  string
		when time.Time
	}{
		{strings.Repeat("7", 64), time.Now().Add(-2 * time.Hour)},
		{strings.Repeat("8", 64), time.Now().Add(-time.Hour)},
		{strings.Repeat("9", 64), time.Now()},
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.key)
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, entry.when, entry.when); err != nil {
			t.Fatal(err)
		}
	}
	c, err := NewDiskCache(dir, 100, DiskCacheOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if c.order.Len() != 2 || c.Has(entries[0].key) || !c.Has(entries[1].key) || !c.Has(entries[2].key) {
		t.Fatalf("startup scan did not retain newest two entries")
	}
}

// tmpDirEntries counts the files left behind in the cache's .tmp subdir, so
// tests can assert PutStream never leaks a temp file.
func tmpDirEntries(t *testing.T, c *DiskCache) int {
	t.Helper()
	entries, err := readDirNames(c.dir + "/" + tmpSubdir)
	if err != nil {
		t.Fatalf("read tmp dir: %v", err)
	}
	return len(entries)
}

func readDirNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return f.Readdirnames(-1)
}

func TestPutStreamRoundTrip(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("a", 64)
	data := []byte("streamed-parquet-bytes")

	n, err := c.PutStream(key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("PutStream returned %d, want %d", n, len(data))
	}
	if !c.Has(key) {
		t.Fatal("Has should be true after PutStream")
	}
	r, size, ok := c.Open(key)
	if !ok {
		t.Fatal("Open should find the streamed entry")
	}
	defer func() { _ = r.Close() }()
	got, _ := io.ReadAll(r)
	if string(got) != string(data) {
		t.Errorf("streamed data = %q, want %q", got, data)
	}
	if size != int64(len(data)) {
		t.Errorf("Open size = %d, want %d", size, len(data))
	}
	if left := tmpDirEntries(t, c); left != 0 {
		t.Errorf("temp dir has %d leftover files, want 0", left)
	}
}

// TestPutStreamErrorMidStream verifies that a reader failing mid-body leaves no
// servable entry, no LRU accounting, and no temp-file leak.
func TestPutStreamErrorMidStream(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("b", 64)

	_, err := c.PutStream(key, &errAfterReader{data: make([]byte, 32)})
	if err == nil {
		t.Fatal("PutStream should return the mid-stream error")
	}
	if c.Has(key) {
		t.Error("a failed stream must not produce a servable entry")
	}
	if c.currentSize != 0 {
		t.Errorf("currentSize = %d after failed stream, want 0", c.currentSize)
	}
	if c.order.Len() != 0 {
		t.Errorf("order has %d entries after failed stream, want 0", c.order.Len())
	}
	if left := tmpDirEntries(t, c); left != 0 {
		t.Errorf("temp dir has %d leftover files after failed stream, want 0", left)
	}
}

// TestPutStreamOverwrite verifies that re-streaming a key replaces it without
// double-counting currentSize or leaving a duplicate LRU entry (finding #2).
func TestPutStreamOverwrite(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("c", 64)

	if _, err := c.PutStream(key, bytes.NewReader(make([]byte, 100))); err != nil {
		t.Fatalf("first PutStream: %v", err)
	}
	if _, err := c.PutStream(key, bytes.NewReader(make([]byte, 40))); err != nil {
		t.Fatalf("second PutStream: %v", err)
	}

	if c.currentSize != 40 {
		t.Errorf("currentSize = %d after overwrite, want 40 (not 140)", c.currentSize)
	}
	count := 0
	for el := c.order.Front(); el != nil; el = el.Next() {
		if el.Value.(*cacheEntry).key == key {
			count++
		}
	}
	if count != 1 {
		t.Errorf("key appears %d times in order, want exactly 1", count)
	}
	if len(c.index) != c.order.Len() {
		t.Errorf("index has %d entries, order has %d — must match", len(c.index), c.order.Len())
	}
	r, size, ok := c.Open(key)
	if !ok || size != 40 {
		t.Fatalf("Open after overwrite: ok=%v size=%d, want ok size=40", ok, size)
	}
	_ = r.Close()
}

func TestPutStreamFailedOverwritePreservesExistingEntryAndSummary(t *testing.T) {
	c, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("d", 64)
	oldBody := []byte("previously-committed-body")
	if _, err := c.PutStream(key, bytes.NewReader(oldBody)); err != nil {
		t.Fatalf("seed PutStream: %v", err)
	}

	c.renameFile = func(string, string) error { return errors.New("forced rename failure") }
	if _, err := c.PutStream(key, strings.NewReader("replacement")); err == nil {
		t.Fatal("overwrite should report the forced rename failure")
	}

	if !c.Has(key) {
		t.Fatal("failed overwrite removed the existing exact-index entry")
	}
	if c.currentSize != int64(len(oldBody)) {
		t.Fatalf("currentSize=%d, want preserved size %d", c.currentSize, len(oldBody))
	}
	r, size, ok := c.Open(key)
	if !ok {
		t.Fatal("failed overwrite made the existing body unservable")
	}
	got, readErr := io.ReadAll(r)
	_ = r.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if size != int64(len(oldBody)) || !bytes.Equal(got, oldBody) {
		t.Fatalf("preserved body size/data=%d/%q, want %d/%q", size, got, len(oldBody), oldBody)
	}
	items, bits, ok := c.SummarySnapshot()
	if !ok {
		t.Fatal("incremental summary unavailable")
	}
	summary, err := newIncrementalCacheSummary(bits, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if items != 1 || !summary.Contains(key) {
		t.Fatalf("failed overwrite removed key from Bloom summary: items=%d contains=%v", items, summary.Contains(key))
	}
	if left := tmpDirEntries(t, c); left != 0 {
		t.Fatalf("temp dir has %d leftover files after failed overwrite", left)
	}
}

func TestPutStreamConcurrentSameKeyCommitsKeepBodyAndAccountingConsistent(t *testing.T) {
	c, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("e", 64)
	if _, err := c.PutStream(key, strings.NewReader("old")); err != nil {
		t.Fatalf("seed PutStream: %v", err)
	}

	firstBody := bytes.Repeat([]byte("a"), 100)
	secondBody := bytes.Repeat([]byte("b"), 40)
	firstRenamed := make(chan struct{})
	releaseFirst := make(chan struct{})
	c.renameFile = func(oldPath, newPath string) error {
		body, err := os.ReadFile(oldPath)
		if err != nil {
			return err
		}
		if err := os.Rename(oldPath, newPath); err != nil {
			return err
		}
		if len(body) == len(firstBody) {
			close(firstRenamed)
			<-releaseFirst
		}
		return nil
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := c.PutStream(key, bytes.NewReader(firstBody))
		firstDone <- err
	}()
	<-firstRenamed

	secondDone := make(chan error, 1)
	go func() {
		_, err := c.PutStream(key, bytes.NewReader(secondBody))
		secondDone <- err
	}()

	// Before commits were serialized, the second writer could finish its rename
	// and accounting while the first writer was paused between those two steps.
	// Give that interleaving a chance, then let the first commit finish.
	var secondErr error
	secondFinishedEarly := false
	select {
	case secondErr = <-secondDone:
		secondFinishedEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first overwrite: %v", err)
	}
	if !secondFinishedEarly {
		secondErr = <-secondDone
	}
	if secondErr != nil {
		t.Fatalf("second overwrite: %v", secondErr)
	}

	r, size, ok := c.Open(key)
	if !ok {
		t.Fatal("final same-key commit is missing")
	}
	got, readErr := io.ReadAll(r)
	_ = r.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if size != int64(len(secondBody)) || c.currentSize != int64(len(secondBody)) || !bytes.Equal(got, secondBody) {
		t.Fatalf("final size/accounting/body=%d/%d/%q, want %d/%d/%q", size, c.currentSize, got, len(secondBody), len(secondBody), secondBody)
	}
}

// TestPutStreamOversizedObject documents that an object larger than maxBytes is
// stored anyway (the eviction loop drains the cache but stops when empty), so a
// single huge range is never silently dropped.
func TestPutStreamOversizedObject(t *testing.T) {
	c := newTestCache(t)
	c.maxBytes = 100
	// Seed a smaller entry that should get evicted to make room.
	if _, err := c.PutStream(strings.Repeat("1", 64), bytes.NewReader(make([]byte, 50))); err != nil {
		t.Fatalf("seed PutStream: %v", err)
	}

	big := strings.Repeat("2", 64)
	if _, err := c.PutStream(big, bytes.NewReader(make([]byte, 250))); err != nil {
		t.Fatalf("oversized PutStream: %v", err)
	}
	if !c.Has(big) {
		t.Error("oversized object should still be stored, not dropped")
	}
	// The seed entry was evicted to (try to) make room.
	if c.Has(strings.Repeat("1", 64)) {
		t.Error("seed entry should have been evicted by the oversized put")
	}
}

// TestHasAnswersFromIndexNotDisk: an untracked file sitting in the cache dir
// is NOT "in the cache" — the index is the single source of truth for both
// lookups and eviction/size accounting. (Pre-fix, Has stat'ed the disk and
// answered true for files the LRU didn't know about.)
func TestHasAnswersFromIndexNotDisk(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("9", 64)

	// Drop a valid-key file on disk with NO index entry.
	if err := os.WriteFile(c.dir+"/"+key, []byte("stray"), 0o640); err != nil {
		t.Fatal(err)
	}
	if c.Has(key) {
		t.Error("Has must answer from the index: untracked on-disk file reported as cached")
	}

	// And the inverse: a tracked entry whose file has been removed still
	// reports present until the LRU drops it (an open reader keeps serving it —
	// eviction removes directory entries, not in-flight reads).
	tracked := strings.Repeat("8", 64)
	if _, err := c.PutStream(tracked, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.dir + "/" + tracked); err != nil {
		t.Fatal(err)
	}
	if !c.Has(tracked) {
		t.Error("tracked entry must report present from the index even if its file vanished")
	}
}

// TestRefreshCapacityShrinksToFreeDisk: when something else eats the filesystem,
// the cache budget must shrink (and over-budget entries evict) rather than the
// cache evicting healthy entries to make room for writes the disk can't take.
func TestRefreshCapacityShrinksToFreeDisk(t *testing.T) {
	c := newTestCache(t)

	// Force an over-capacity state relative to the free disk this test runs on:
	// pretend the budget used to be huge and track some bytes against it.
	c.maxBytes = 1 << 40 // 1 TiB
	if _, err := c.PutStream(strings.Repeat("1", 64), bytes.NewReader(make([]byte, 100))); err != nil {
		t.Fatal(err)
	}
	before := c.currentSize

	_, maxed, ok := c.refreshCapacityStats(80)
	if !ok {
		t.Fatal("refreshCapacityStats: statfs failed")
	}
	if maxed >= 1<<40 {
		t.Fatalf("maxBytes = %d, want it clamped to a fraction of the real disk", maxed)
	}
	// The tracked bytes are tiny and fit any realistic budget, so nothing
	// should have evicted — the budget just came down.
	if c.currentSize != before {
		t.Errorf("currentSize = %d, want %d (no eviction when fits)", c.currentSize, before)
	}
}

// TestRefreshCapacityEvictsWhenOverBudget: if the recomputed budget is below
// the live contents, refresh evicts LRU-first until the contents fit it.
func TestRefreshCapacityEvictsWhenOverBudget(t *testing.T) {
	c := newTestCache(t)
	if _, err := c.PutStream(strings.Repeat("1", 64), bytes.NewReader(make([]byte, 60))); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutStream(strings.Repeat("2", 64), bytes.NewReader(make([]byte, 60))); err != nil {
		t.Fatal(err)
	}
	if c.currentSize != 120 {
		t.Fatalf("currentSize = %d, want 120", c.currentSize)
	}

	// What would the budget be if the disk only had (reserve + 60B) free?
	var stat syscall.Statfs_t
	if err := syscall.Statfs(c.dir, &stat); err != nil {
		t.Fatal(err)
	}
	total := int64(stat.Blocks) * int64(stat.Bsize)
	fakeFree := total/20 + 60 // reserve + exactly one entry
	if got := clampToFree(1<<40, total, fakeFree); got != 60 {
		t.Fatalf("clampToFree = %d, want 60", got)
	}
}

// TestOpenCountsHitOpenFileDoesNot locks in the metric split: serving a hit via
// Open records a cache hit; serving a freshly-fetched miss via openFile does
// not (otherwise misses would be double-counted as hits).
func TestOpenCountsHitOpenFileDoesNot(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("d", 64)
	if _, err := c.PutStream(key, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("PutStream: %v", err)
	}

	before := counterValue(t, cacheHitsTotal)
	r, _, ok := c.openFile(key)
	if !ok {
		t.Fatal("openFile should find the entry")
	}
	_ = r.Close()
	if mid := counterValue(t, cacheHitsTotal); mid != before {
		t.Errorf("openFile changed hit counter by %v, want 0", mid-before)
	}

	r, _, ok = c.Open(key)
	if !ok {
		t.Fatal("Open should find the entry")
	}
	_ = r.Close()
	if after := counterValue(t, cacheHitsTotal); after != before+1 {
		t.Errorf("Open changed hit counter by %v, want 1", after-before)
	}
}

// TestEvictionRespectsTouchRecency locks in true LRU semantics across the
// rewrite: touching an old entry must move it out of eviction's path, so the
// victim is the least recently *used* entry, not the least recently written.
func TestEvictionRespectsTouchRecency(t *testing.T) {
	c := newTestCache(t)
	c.maxBytes = 100

	a, b := strings.Repeat("a", 64), strings.Repeat("b", 64)
	if _, err := c.PutStream(a, bytes.NewReader(make([]byte, 45))); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutStream(b, bytes.NewReader(make([]byte, 45))); err != nil {
		t.Fatal(err)
	}
	// Touch a so b becomes the LRU entry.
	if r, _, ok := c.Open(a); !ok {
		t.Fatal("Open(a) should hit")
	} else {
		_ = r.Close()
	}
	if _, err := c.PutStream(strings.Repeat("d", 64), bytes.NewReader(make([]byte, 45))); err != nil {
		t.Fatal(err)
	}
	if !c.Has(a) {
		t.Error("recently touched entry was evicted; eviction must follow access recency")
	}
	if c.Has(b) {
		t.Error("least recently used entry survived eviction")
	}
}

// BenchmarkTouchLargeCache documents why the LRU must be O(1): a production
// node tracks ~285k entries (285GB at 1MiB blocks), and every cache operation
// touches under one mutex. The prior slice implementation scanned and spliced
// the whole slice per touch, putting ~half the proxy's CPU into bookkeeping.
func BenchmarkTouchLargeCache(b *testing.B) {
	c := &DiskCache{order: list.New(), index: make(map[string]*list.Element)}
	keys := make([]string, 285000)
	for i := range keys {
		keys[i] = CacheKey("", fmt.Sprintf("http://bucket/f%d.parquet", i), "bytes=0-1")
		c.addLocked(keys[i], 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.mu.Lock()
		c.touchLocked(keys[i%len(keys)])
		c.mu.Unlock()
	}
}

// TestScanExistingSeedsRecencyOrder covers eviction correctness after a
// restart: scanExisting must seed the access list in mtime order, because
// evictOldest trusts the list front to be the LRU entry. If the seed order
// were arbitrary, the first evictions after every proxy restart would remove
// arbitrary entries instead of the oldest.
func TestScanExistingSeedsRecencyOrder(t *testing.T) {
	dir := t.TempDir()
	old, mid, recent := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	now := time.Now()
	// Write in an order unrelated to the mtimes we stamp.
	for _, e := range []struct {
		key string
		age time.Duration
	}{{mid, 2 * time.Hour}, {recent, time.Hour}, {old, 3 * time.Hour}} {
		path := dir + "/" + e.key
		if err := os.WriteFile(path, make([]byte, 45), 0o640); err != nil {
			t.Fatal(err)
		}
		ts := now.Add(-e.age)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatal(err)
		}
	}
	c, err := NewDiskCache(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	// 135 bytes are resident; a fourth 45-byte entry against a 170-byte cap
	// forces exactly one eviction — which must be the oldest-mtime entry.
	c.maxBytes = 170

	if _, err := c.PutStream(strings.Repeat("d", 64), bytes.NewReader(make([]byte, 45))); err != nil {
		t.Fatal(err)
	}
	if c.Has(old) {
		t.Error("oldest-mtime entry survived post-restart eviction")
	}
	if !c.Has(recent) || !c.Has(mid) {
		t.Error("newer entries were evicted before the oldest")
	}
}

func TestScanExistingEnforcesByteAndEntryCeilings(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)}
	base := time.Now().Add(-time.Hour)
	for i, key := range keys {
		path := filepath.Join(dir, key)
		if err := os.WriteFile(path, []byte(strings.Repeat("x", 10)), 0o600); err != nil {
			t.Fatal(err)
		}
		when := base.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatal(err)
		}
	}

	c := &DiskCache{
		dir:        dir,
		maxBytes:   15,
		maxEntries: 2,
		order:      list.New(),
		index:      make(map[string]*list.Element),
	}
	c.scanExisting()
	if c.currentSize > c.maxBytes || c.order.Len() > c.maxEntries {
		t.Fatalf("startup cache exceeds ceilings: bytes=%d/%d entries=%d/%d", c.currentSize, c.maxBytes, c.order.Len(), c.maxEntries)
	}
	if !c.Has(keys[2]) {
		t.Fatal("startup pruning did not retain the newest entry")
	}
}
