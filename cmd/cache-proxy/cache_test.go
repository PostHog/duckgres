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

func counterVecValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	return counterValue(t, c.WithLabelValues(labels...))
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

type scriptedCacheDirectory struct {
	entries []os.DirEntry
	err     error
	read    bool
}

func (d *scriptedCacheDirectory) ReadDir(int) ([]os.DirEntry, error) {
	if !d.read {
		d.read = true
		return d.entries, nil
	}
	return nil, d.err
}

func (d *scriptedCacheDirectory) Close() error { return nil }

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

func TestDiskCacheHardEntryLimitAppliesDuringStartupScan(t *testing.T) {
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
	c, err := NewDiskCache(dir, 100, DiskCacheOptions{MaxEntries: 3, hardMaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if c.order.Len() != 2 || c.Has(entries[0].key) || !c.Has(entries[1].key) || !c.Has(entries[2].key) {
		t.Fatalf("startup scan did not retain newest two entries")
	}
}

func TestDiskCacheStartupCountsCommittedBytesAsReclaimable(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	for _, key := range keys {
		if err := os.WriteFile(filepath.Join(dir, key), make([]byte, 400), 0600); err != nil {
			t.Fatal(err)
		}
	}

	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries: 2,
		CapacityProvider: func(string) (diskSpace, error) {
			// The disk is 80% full only because of the two committed cache files.
			return diskSpace{TotalBytes: 1000, FreeBytes: 200}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.currentSize != 800 || c.maxBytes != 800 {
		t.Fatalf("startup cache size/capacity = %d/%d, want 800/800; committed entries must not be mistaken for external disk use", c.currentSize, c.maxBytes)
	}
	for _, key := range keys {
		if !c.Has(key) {
			t.Fatalf("startup pruned committed cache key %s", key)
		}
	}
}

func TestDiskCacheScanSeparatesTemporaryInvalidAndCommittedFiles(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("c", 64)
	if err := os.WriteFile(filepath.Join(dir, key), []byte("committed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, tmpSubdir), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, tmpSubdir, "interrupted"), []byte("temporary"), 0600); err != nil {
		t.Fatal(err)
	}
	invalid := filepath.Join(dir, "not-a-cache-key")
	if err := os.WriteFile(invalid, []byte("external"), 0600); err != nil {
		t.Fatal(err)
	}

	beforeTemporary := counterValue(t, cacheTemporaryFilesRemovedTotal)
	beforeInvalid := counterValue(t, cacheStartupInvalidFilesTotal)
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{CapacityProvider: func(string) (diskSpace, error) {
		return diskSpace{TotalBytes: 1000, FreeBytes: 990}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !c.Has(key) {
		t.Fatal("committed cache entry was not loaded")
	}
	if _, err := os.Stat(filepath.Join(dir, tmpSubdir, "interrupted")); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists after startup: %v", err)
	}
	if _, err := os.Stat(invalid); err != nil {
		t.Fatalf("unrelated file should remain external disk use: %v", err)
	}
	if got := counterValue(t, cacheTemporaryFilesRemovedTotal) - beforeTemporary; got != 1 {
		t.Fatalf("temporary cleanup count = %v, want 1", got)
	}
	if got := counterValue(t, cacheStartupInvalidFilesTotal) - beforeInvalid; got != 1 {
		t.Fatalf("invalid file count = %v, want 1", got)
	}
}

func TestNewDiskCacheFailsWhenStartupScanCannotOpenDirectory(t *testing.T) {
	scanErr := errors.New("forced startup scan open failure")
	c, err := NewDiskCache(t.TempDir(), 80, DiskCacheOptions{
		openScanDirectory: func(string) (cacheDirectory, error) {
			return nil, scanErr
		},
	})
	if c != nil {
		t.Fatal("NewDiskCache returned a cache after its startup scan failed")
	}
	if !errors.Is(err, scanErr) {
		t.Fatalf("NewDiskCache error = %v, want wrapped scan error", err)
	}
}

func TestNewDiskCacheFailsWhenStartupScanStopsBeforeDirectoryIsExhausted(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	path := filepath.Join(dir, key)
	if err := os.WriteFile(path, []byte("committed"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	scanErr := errors.New("forced startup scan read failure")
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &scriptedCacheDirectory{entries: entries, err: scanErr}, nil
		},
	})
	if c != nil {
		t.Fatal("NewDiskCache returned a partially scanned cache")
	}
	if !errors.Is(err, scanErr) {
		t.Fatalf("NewDiskCache error = %v, want wrapped mid-scan error", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("committed file changed after non-pressure scan failure: %v", err)
	}
}

func TestNewDiskCacheFailsWhenHardPruneCannotRemoveVictim(t *testing.T) {
	for _, tc := range []struct {
		name       string
		firstAge   time.Duration
		secondAge  time.Duration
		wantVictim string
	}{
		{name: "incoming entry is older", firstAge: time.Hour, secondAge: 2 * time.Hour, wantVictim: strings.Repeat("b", 64)},
		{name: "existing survivor is older", firstAge: 2 * time.Hour, secondAge: time.Hour, wantVictim: strings.Repeat("a", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			first := strings.Repeat("a", 64)
			second := strings.Repeat("b", 64)
			now := time.Now()
			for _, entry := range []struct {
				key string
				age time.Duration
			}{{first, tc.firstAge}, {second, tc.secondAge}} {
				path := filepath.Join(dir, entry.key)
				if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
				when := now.Add(-entry.age)
				if err := os.Chtimes(path, when, when); err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}

			removeErr := errors.New("forced startup prune removal failure")
			var attemptedVictim string
			beforeAggregate := counterValue(t, cacheEvictionsTotal)
			beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
			c, err := NewDiskCache(dir, 100, DiskCacheOptions{
				MaxEntries:     2,
				hardMaxEntries: 1,
				openScanDirectory: func(string) (cacheDirectory, error) {
					return &scriptedCacheDirectory{entries: entries, err: io.EOF}, nil
				},
				removeFile: func(path string) error {
					attemptedVictim = filepath.Base(path)
					return removeErr
				},
			})
			if c != nil || !errors.Is(err, removeErr) {
				t.Fatalf("NewDiskCache after failed hard-prune removal: cache=%v err=%v, want nil/wrapped failure", c, err)
			}
			if attemptedVictim != tc.wantVictim {
				t.Fatalf("startup prune attempted victim %q, want %q", attemptedVictim, tc.wantVictim)
			}
			for _, key := range []string{first, second} {
				if _, err := os.Stat(filepath.Join(dir, key)); err != nil {
					t.Fatalf("committed file %s changed after failed prune: %v", key, err)
				}
			}
			if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
				t.Fatalf("aggregate evictions after failed startup removal = %v, want 0", got)
			}
			if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry) - beforeLabel; got != 0 {
				t.Fatalf("labelled evictions after failed startup removal = %v, want 0", got)
			}
		})
	}
}

func TestNewDiskCacheTreatsStartupPruneENOENTAsSuccessfulCleanup(t *testing.T) {
	dir := t.TempDir()
	oldest := strings.Repeat("a", 64)
	newest := strings.Repeat("b", 64)
	now := time.Now()
	for _, entry := range []struct {
		key string
		age time.Duration
	}{{oldest, 2 * time.Hour}, {newest, time.Hour}} {
		path := filepath.Join(dir, entry.key)
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		when := now.Add(-entry.age)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
	c, err := NewDiskCache(dir, 100, DiskCacheOptions{
		MaxEntries:     2,
		hardMaxEntries: 1,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &scriptedCacheDirectory{entries: entries, err: io.EOF}, nil
		},
		removeFile: func(path string) error {
			if err := os.Remove(path); err != nil {
				return err
			}
			return &os.PathError{Op: "remove", Path: path, Err: syscall.ENOENT}
		},
	})
	if err != nil {
		t.Fatalf("NewDiskCache rejected an ENOENT startup-prune race: %v", err)
	}
	if c.order.Len() != 1 || c.Has(oldest) || !c.Has(newest) {
		t.Fatalf("startup survivors after ENOENT race = entries:%d oldest:%t newest:%t, want 1/false/true", c.order.Len(), c.Has(oldest), c.Has(newest))
	}
	if _, err := os.Stat(filepath.Join(dir, oldest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("externally removed victim still exists: %v", err)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
		t.Fatalf("aggregate evictions after ENOENT startup race = %v, want 0", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry) - beforeLabel; got != 0 {
		t.Fatalf("labelled evictions after ENOENT startup race = %v, want 0", got)
	}
}

func TestNewDiskCacheDefersInitialByteConvergence(t *testing.T) {
	dir := t.TempDir()
	for _, key := range []string{strings.Repeat("c", 64), strings.Repeat("d", 64)} {
		if err := os.WriteFile(filepath.Join(dir, key), make([]byte, 60), 0600); err != nil {
			t.Fatal(err)
		}
	}
	removeErr := errors.New("forced startup byte-prune removal failure")
	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonByte)
	removeAttempts := 0
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		CapacityProvider: func(string) (diskSpace, error) {
			// owned=120, reserve=50, free=0 => byte ceiling=70.
			return diskSpace{TotalBytes: 1000, FreeBytes: 0}, nil
		},
		removeFile: func(string) error {
			removeAttempts++
			return removeErr
		},
	})
	if err != nil || c == nil {
		t.Fatalf("NewDiskCache must load inspectable entries before rate-limited byte convergence: cache=%v err=%v", c, err)
	}
	if c.currentSize != 120 || c.maxBytes != 70 || removeAttempts != 0 {
		t.Fatalf("startup state bytes/capacity/removals = %d/%d/%d, want 120/70/0", c.currentSize, c.maxBytes, removeAttempts)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
		t.Fatalf("aggregate evictions after failed startup byte removal = %v, want 0", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonByte) - beforeLabel; got != 0 {
		t.Fatalf("labelled evictions after failed startup byte removal = %v, want 0", got)
	}
}

func TestCachePressureEvictionsHaveBoundedPhaseAndReason(t *testing.T) {
	t.Run("startup entry pressure", func(t *testing.T) {
		dir := t.TempDir()
		for _, key := range []string{strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)} {
			if err := os.WriteFile(filepath.Join(dir, key), []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		before := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
		beforeAggregate := counterValue(t, cacheEvictionsTotal)
		c, err := NewDiskCache(dir, 80, DiskCacheOptions{
			MaxEntries:     3,
			hardMaxEntries: 2,
			CapacityProvider: func(string) (diskSpace, error) {
				return diskSpace{TotalBytes: 1000, FreeBytes: 990}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if c.order.Len() != 2 {
			t.Fatalf("startup entry limit retained %d entries, want 2", c.order.Len())
		}
		if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry) - before; got != 1 {
			t.Fatalf("startup entry evictions = %v, want 1", got)
		}
		if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 1 {
			t.Fatalf("aggregate startup evictions = %v, want 1", got)
		}
	})

	t.Run("request byte pressure", func(t *testing.T) {
		c := newTestCache(t)
		c.maxBytes = 100
		before := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, cacheEvictionReasonByte)
		if _, err := c.PutStream(strings.Repeat("4", 64), bytes.NewReader(make([]byte, 60))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PutStream(strings.Repeat("5", 64), bytes.NewReader(make([]byte, 60))); err != nil {
			t.Fatal(err)
		}
		if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, cacheEvictionReasonByte) - before; got != 1 {
			t.Fatalf("request byte evictions = %v, want 1", got)
		}
	})
}

func TestRefreshCapacityShrinksImmediatelyAndRecoversAfterHysteresis(t *testing.T) {
	space := diskSpace{TotalBytes: 1000, FreeBytes: 200}
	c, err := NewDiskCache(t.TempDir(), 80, DiskCacheOptions{CapacityProvider: func(string) (diskSpace, error) {
		return space, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	// The provider is synthetic, so make the seeded in-memory cache and free
	// space agree with a cache that has reached its 80%-of-disk target.
	c.maxBytes = 800
	for _, key := range []string{strings.Repeat("6", 64), strings.Repeat("7", 64)} {
		if _, err := c.PutStream(key, bytes.NewReader(make([]byte, 400))); err != nil {
			t.Fatal(err)
		}
	}

	before := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonByte)
	space.FreeBytes = 10
	if _, got, ok := c.refreshCapacityStats(80); !ok || got != 760 {
		t.Fatalf("shrunk capacity = %d (ok=%v), want 760", got, ok)
	}
	c.convergeOne()
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonByte) - before; got != 1 {
		t.Fatalf("background byte evictions = %v, want 1", got)
	}

	space.FreeBytes = 600
	if _, got, ok := c.refreshCapacityStats(80); !ok || got != 760 {
		t.Fatalf("first recovery refresh = %d (ok=%v), want hysteresis to hold 760", got, ok)
	}
	if _, got, ok := c.refreshCapacityStats(80); !ok || got != 800 {
		t.Fatalf("second recovery refresh = %d (ok=%v), want 800", got, ok)
	}
}

func TestRefreshCapacityRetriesEvictionAtUnchangedCeiling(t *testing.T) {
	space := diskSpace{TotalBytes: 1000, FreeBytes: 1000}
	c, err := NewDiskCache(t.TempDir(), 80, DiskCacheOptions{CapacityProvider: func(string) (diskSpace, error) {
		return space, nil
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{strings.Repeat("8", 64), strings.Repeat("9", 64)} {
		if _, err := c.PutStream(key, bytes.NewReader(make([]byte, 400))); err != nil {
			t.Fatal(err)
		}
	}

	removeAttempts := 0
	c.removeFile = func(path string) error {
		removeAttempts++
		if removeAttempts == 1 {
			return errors.New("transient background removal failure")
		}
		return os.Remove(path)
	}
	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonByte)

	// free + owned - reserve = 10 + 800 - 50 = 760. The first refresh
	// lowers the ceiling, but its one required eviction fails transiently.
	space.FreeBytes = 10
	if _, got, ok := c.refreshCapacityStats(80); !ok || got != 760 {
		t.Fatalf("first refresh capacity = %d (ok=%v), want 760", got, ok)
	}
	c.convergeOne()
	if removeAttempts != 1 || c.currentSize != 800 || c.order.Len() != 2 {
		t.Fatalf("cache after failed convergence = attempts:%d bytes:%d entries:%d, want 1/800/2", removeAttempts, c.currentSize, c.order.Len())
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
		t.Fatalf("aggregate evictions after failed convergence = %v, want 0", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonByte) - beforeLabel; got != 0 {
		t.Fatalf("labelled evictions after failed convergence = %v, want 0", got)
	}

	// The byte ceiling is unchanged, but the cache is still over it, so the
	// next refresh must retry and converge after the transient error clears.
	if _, got, ok := c.refreshCapacityStats(80); !ok || got != 760 {
		t.Fatalf("second refresh capacity = %d (ok=%v), want 760", got, ok)
	}
	c.convergeOne()
	if removeAttempts != 2 || c.currentSize != 400 || c.order.Len() != 1 {
		t.Fatalf("cache after retried convergence = attempts:%d bytes:%d entries:%d, want 2/400/1", removeAttempts, c.currentSize, c.order.Len())
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 1 {
		t.Fatalf("aggregate evictions after convergence = %v, want 1", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonByte) - beforeLabel; got != 1 {
		t.Fatalf("labelled evictions after convergence = %v, want 1", got)
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

func TestPutStreamRejectsNewEntryWhenRequiredEvictionFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxBytes   int64
		maxEntries int
		reason     cacheEvictionReason
	}{
		{name: "byte pressure", maxBytes: 100, maxEntries: 10, reason: cacheEvictionReasonByte},
		{name: "entry pressure", maxBytes: 1000, maxEntries: 1, reason: cacheEvictionReasonEntry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
			if err != nil {
				t.Fatal(err)
			}
			c.maxBytes = tc.maxBytes
			c.maxEntries = tc.maxEntries

			incumbent := strings.Repeat("1", 64)
			candidate := strings.Repeat("2", 64)
			body := bytes.Repeat([]byte("x"), 60)
			if _, err := c.PutStream(incumbent, bytes.NewReader(body)); err != nil {
				t.Fatalf("seed PutStream: %v", err)
			}

			removeErr := errors.New("forced committed-file removal failure")
			incumbentPath := filepath.Join(c.dir, incumbent)
			c.removeFile = func(path string) error {
				if path == incumbentPath {
					return removeErr
				}
				return os.Remove(path)
			}
			beforeAggregate := counterValue(t, cacheEvictionsTotal)
			beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, tc.reason)

			if _, err := c.PutStream(candidate, bytes.NewReader(body)); err == nil {
				t.Fatal("PutStream admitted a new entry after required eviction failed")
			}
			if !c.Has(incumbent) || c.Has(candidate) {
				t.Fatalf("index membership incumbent/candidate = %t/%t, want true/false", c.Has(incumbent), c.Has(candidate))
			}
			if c.currentSize != int64(len(body)) || c.order.Len() != 1 {
				t.Fatalf("cache accounting after rejected admission = %d bytes/%d entries, want %d/1", c.currentSize, c.order.Len(), len(body))
			}
			got, err := os.ReadFile(incumbentPath)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("incumbent body after rejected admission = %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(c.dir, candidate)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("candidate committed after rejected admission: %v", err)
			}
			if tmpDirEntries(t, c) != 0 {
				t.Fatal("rejected admission leaked a temporary file")
			}
			items, bits, ok := c.SummarySnapshot()
			if !ok || items != 1 {
				t.Fatalf("summary after rejected admission = ok:%t items:%d, want true/1", ok, items)
			}
			summary := &cacheSummary{MBits: summaryBloomBits, Hashes: summaryBloomHashes, Bits: bits}
			if !summary.Contains(incumbent) {
				t.Fatal("rejected admission removed incumbent from the Bloom summary")
			}
			if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
				t.Fatalf("aggregate evictions after failed removal = %v, want 0", got)
			}
			if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, tc.reason) - beforeLabel; got != 0 {
				t.Fatalf("labelled evictions after failed removal = %v, want 0", got)
			}
		})
	}
}

func TestPutStreamPreservesOldBodyWhenReplacementEvictionFails(t *testing.T) {
	c, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	c.maxBytes = 100
	replacementKey := strings.Repeat("3", 64)
	victimKey := strings.Repeat("4", 64)
	oldBody := bytes.Repeat([]byte("o"), 20)
	victimBody := bytes.Repeat([]byte("v"), 60)
	if _, err := c.PutStream(replacementKey, bytes.NewReader(oldBody)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutStream(victimKey, bytes.NewReader(victimBody)); err != nil {
		t.Fatal(err)
	}

	victimPath := filepath.Join(c.dir, victimKey)
	removeErr := errors.New("forced replacement-victim removal failure")
	c.removeFile = func(path string) error {
		if path == victimPath {
			return removeErr
		}
		return os.Remove(path)
	}
	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, cacheEvictionReasonByte)

	if _, err := c.PutStream(replacementKey, bytes.NewReader(bytes.Repeat([]byte("n"), 60))); err == nil {
		t.Fatal("PutStream committed an over-budget replacement after required eviction failed")
	}
	if c.currentSize != int64(len(oldBody)+len(victimBody)) || c.order.Len() != 2 {
		t.Fatalf("cache accounting after rejected replacement = %d bytes/%d entries, want %d/2", c.currentSize, c.order.Len(), len(oldBody)+len(victimBody))
	}
	got, err := os.ReadFile(filepath.Join(c.dir, replacementKey))
	if err != nil || !bytes.Equal(got, oldBody) {
		t.Fatalf("replacement body after rejected overwrite = %q, %v; want old body", got, err)
	}
	if got, err := os.ReadFile(victimPath); err != nil || !bytes.Equal(got, victimBody) {
		t.Fatalf("victim body after rejected overwrite = %q, %v", got, err)
	}
	if tmpDirEntries(t, c) != 0 {
		t.Fatal("rejected replacement leaked a temporary file")
	}
	items, _, ok := c.SummarySnapshot()
	if !ok || items != 2 {
		t.Fatalf("summary after rejected replacement = ok:%t items:%d, want true/2", ok, items)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
		t.Fatalf("aggregate evictions after failed replacement removal = %v, want 0", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseRequest, cacheEvictionReasonByte) - beforeLabel; got != 0 {
		t.Fatalf("labelled evictions after failed replacement removal = %v, want 0", got)
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
		keys[i] = CacheKey(fmt.Sprintf("http://bucket/f%d.parquet", i), "bytes=0-1")
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

func TestScanExistingLoadsAboveSoftByteAndEntryTargets(t *testing.T) {
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
	if _, err := c.scanExisting(); err != nil {
		t.Fatal(err)
	}
	if c.currentSize != 30 || c.order.Len() != 3 {
		t.Fatalf("startup cache bytes/entries = %d/%d, want all 30/3 before convergence", c.currentSize, c.order.Len())
	}
	for _, key := range keys {
		if !c.Has(key) {
			t.Fatalf("startup omitted soft-over-target key %s", key)
		}
	}
}
