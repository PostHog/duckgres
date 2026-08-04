package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

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
	a := CacheKey("http://s3/bucket/file.parquet", "bytes=0-1023")
	b := CacheKey("http://s3/bucket/file.parquet", "bytes=0-1023")
	if a != b {
		t.Fatalf("CacheKey not deterministic: %s != %s", a, b)
	}
	if !IsValidCacheKey(a) {
		t.Errorf("CacheKey output %q is not a valid key", a)
	}
	c := CacheKey("http://s3/bucket/file.parquet", "bytes=0-2047")
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
		t.Fatal("Has should be false before Put")
	}
	if _, ok := c.Get(key); ok {
		t.Fatal("Get should miss before Put")
	}
	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !c.Has(key) {
		t.Fatal("Has should be true after Put")
	}
	got, ok := c.Get(key)
	if !ok {
		t.Fatal("Get should hit after Put")
	}
	if string(got) != string(data) {
		t.Errorf("Get returned %q, want %q", got, data)
	}
}

func TestDiskCacheOpen(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("b", 64)
	data := []byte("streaming bytes")
	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
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
	if _, ok := c.Get(bad); ok {
		t.Error("Get should reject invalid key")
	}
	if _, _, ok := c.Open(bad); ok {
		t.Error("Open should reject invalid key")
	}
	if err := c.Put(bad, []byte("x")); err == nil {
		t.Error("Put should reject invalid key")
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
		if err := c.Put(k, make([]byte, 60)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
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
	if len(c.order) != 0 {
		t.Errorf("order has %d entries after failed stream, want 0", len(c.order))
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
	for _, e := range c.order {
		if e.key == key {
			count++
		}
	}
	if count != 1 {
		t.Errorf("key appears %d times in order, want exactly 1", count)
	}
	r, size, ok := c.Open(key)
	if !ok || size != 40 {
		t.Fatalf("Open after overwrite: ok=%v size=%d, want ok size=40", ok, size)
	}
	_ = r.Close()
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
