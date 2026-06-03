package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// validCacheKey matches the 64-hex-digit output of CacheKey (sha256 hex).
// Cache keys arrive from untrusted peers via HTTP query params; anything
// that isn't a clean hex digest is rejected to prevent filepath traversal
// when composing the on-disk path.
var validCacheKey = regexp.MustCompile(`^[0-9a-f]{64}$`)

// IsValidCacheKey returns true if key is a 64-char lowercase hex string.
func IsValidCacheKey(key string) bool {
	return validCacheKey.MatchString(key)
}

// tmpSubdir holds in-progress streamed writes. It lives under the cache dir so
// the temp file and its final destination share a filesystem and os.Rename is
// atomic. Entries here are never servable and are excluded from scan/accounting.
const tmpSubdir = ".tmp"

var (
	cacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_hits_total",
		Help: "Cache hits served from local NVMe",
	})
	cacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_misses_total",
		Help: "Cache misses (not in local cache)",
	})
	cacheBytesServed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_bytes_served_total",
		Help: "Bytes served by source",
	}, []string{"source"}) // local, peer, s3
	cacheSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_size_bytes",
		Help: "Current cache size on disk",
	})
	cacheCapacityBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_capacity_bytes",
		Help: "Maximum cache capacity",
	})
	cacheEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_evictions_total",
		Help: "Number of LRU cache evictions",
	})
)

// CacheKey computes a deterministic cache key from a full URL and byte range.
// The URL includes scheme, host, path, and query — so different buckets, regions,
// or query-signed URLs naturally produce different keys.
func CacheKey(url, rangeHeader string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s", url, rangeHeader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// DiskCache manages cached S3 responses on local NVMe storage with LRU eviction.
type DiskCache struct {
	dir         string
	maxBytes    int64
	currentSize int64

	mu sync.Mutex
	// access order: most recently used at the end
	order []cacheEntry
}

type cacheEntry struct {
	key        string
	size       int64
	lastAccess time.Time
}

// NewDiskCache creates a cache backed by the given directory.
// maxPercent is the percentage of filesystem capacity to use (e.g. 80).
func NewDiskCache(dir string, maxPercent int) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	// Recreate the temp dir from scratch so any half-written streams left by a
	// crash don't linger and leak disk.
	tmpDir := filepath.Join(dir, tmpSubdir)
	_ = os.RemoveAll(tmpDir)
	if err := os.MkdirAll(tmpDir, 0750); err != nil {
		return nil, fmt.Errorf("create cache temp dir: %w", err)
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return nil, fmt.Errorf("statfs %s: %w", dir, err)
	}

	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	maxBytes := totalBytes * int64(maxPercent) / 100

	slog.Info("Cache initialized.",
		"dir", dir,
		"total_disk", totalBytes,
		"max_cache", maxBytes,
		"max_percent", maxPercent,
	)

	cacheCapacityBytes.Set(float64(maxBytes))

	dc := &DiskCache{
		dir:      dir,
		maxBytes: maxBytes,
	}

	// Scan existing cache entries
	dc.scanExisting()

	return dc, nil
}

func (c *DiskCache) scanExisting() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Only count real cache entries — the .tmp dir's contents and any
		// stray non-key files must not enter the LRU accounting.
		if !IsValidCacheKey(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		c.order = append(c.order, cacheEntry{
			key:        e.Name(),
			size:       info.Size(),
			lastAccess: info.ModTime(),
		})
		c.currentSize += info.Size()
	}
	cacheSizeBytes.Set(float64(c.currentSize))
}

// Has returns true if the cache contains the given key.
func (c *DiskCache) Has(key string) bool {
	if !IsValidCacheKey(key) {
		return false
	}
	path := filepath.Join(c.dir, key)
	_, err := os.Stat(path)
	return err == nil
}

// Get returns the cached data for the given key, or nil if not found.
func (c *DiskCache) Get(key string) ([]byte, bool) {
	if !IsValidCacheKey(key) {
		return nil, false
	}
	path := filepath.Join(c.dir, key)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	c.mu.Lock()
	c.touchLocked(key)
	c.mu.Unlock()

	cacheHitsTotal.Inc()
	return data, true
}

// Put stores data in the cache, evicting old entries if needed.
func (c *DiskCache) Put(key string, data []byte) error {
	if !IsValidCacheKey(key) {
		return fmt.Errorf("invalid cache key")
	}
	size := int64(len(data))

	c.mu.Lock()
	// Evict until we have space
	for c.currentSize+size > c.maxBytes && len(c.order) > 0 {
		c.evictOldest()
	}
	c.mu.Unlock()

	path := filepath.Join(c.dir, key)
	if err := os.WriteFile(path, data, 0640); err != nil {
		return fmt.Errorf("write cache entry: %w", err)
	}

	c.mu.Lock()
	c.order = append(c.order, cacheEntry{
		key:        key,
		size:       size,
		lastAccess: time.Now(),
	})
	c.currentSize += size
	cacheSizeBytes.Set(float64(c.currentSize))
	c.mu.Unlock()

	return nil
}

// PutStream stores data from r under key without buffering the whole body in
// memory. It writes to a temp file and atomically renames it into place, so a
// truncated or failed copy never becomes a servable entry. Returns the number
// of bytes stored. This is the streaming counterpart to Put and is what keeps
// the proxy's memory flat under a flood of concurrent large range reads.
func (c *DiskCache) PutStream(key string, r io.Reader) (int64, error) {
	if !IsValidCacheKey(key) {
		return 0, fmt.Errorf("invalid cache key")
	}

	tmp, err := os.CreateTemp(filepath.Join(c.dir, tmpSubdir), key+"-*")
	if err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	size, copyErr := io.Copy(tmp, r)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return 0, closeErr
	}

	// Now that the actual size is known, drop any prior accounting for this key
	// (the rename below overwrites it) and evict to make room.
	c.mu.Lock()
	c.dropLocked(key)
	for c.currentSize+size > c.maxBytes && len(c.order) > 0 {
		c.evictOldest()
	}
	c.mu.Unlock()

	path := filepath.Join(c.dir, key)
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("commit cache entry: %w", err)
	}

	c.mu.Lock()
	c.order = append(c.order, cacheEntry{
		key:        key,
		size:       size,
		lastAccess: time.Now(),
	})
	c.currentSize += size
	cacheSizeBytes.Set(float64(c.currentSize))
	c.mu.Unlock()

	return size, nil
}

// Open returns a reader for the cached data and counts a cache hit. Caller must
// close the reader.
func (c *DiskCache) Open(key string) (io.ReadCloser, int64, bool) {
	f, size, ok := c.openFile(key)
	if !ok {
		return nil, 0, false
	}
	cacheHitsTotal.Inc()
	return f, size, true
}

// openFile opens a cached entry WITHOUT recording a hit. Used to serve a body
// that was just fetched on a miss — that path already counted a miss, so
// counting it as a hit too would double-count.
func (c *DiskCache) openFile(key string) (io.ReadCloser, int64, bool) {
	if !IsValidCacheKey(key) {
		return nil, 0, false
	}
	path := filepath.Join(c.dir, key)
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, false
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, false
	}

	c.mu.Lock()
	c.touchLocked(key)
	c.mu.Unlock()

	return f, info.Size(), true
}

func (c *DiskCache) touchLocked(key string) {
	for i := range c.order {
		if c.order[i].key == key {
			c.order[i].lastAccess = time.Now()
			// Move to end (most recent)
			entry := c.order[i]
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, entry)
			return
		}
	}
}

// dropLocked removes any accounting for key (used when an overwrite is about to
// replace the file under it) so currentSize doesn't double-count. No-op if the
// key isn't tracked. Caller holds c.mu.
func (c *DiskCache) dropLocked(key string) {
	for i := range c.order {
		if c.order[i].key == key {
			c.currentSize -= c.order[i].size
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func (c *DiskCache) evictOldest() {
	if len(c.order) == 0 {
		return
	}

	// Sort by last access, evict the oldest
	sort.Slice(c.order, func(i, j int) bool {
		return c.order[i].lastAccess.Before(c.order[j].lastAccess)
	})

	oldest := c.order[0]
	path := filepath.Join(c.dir, oldest.key)
	_ = os.Remove(path)
	c.currentSize -= oldest.size
	c.order = c.order[1:]
	cacheEvictionsTotal.Inc()
	cacheSizeBytes.Set(float64(c.currentSize))
}
