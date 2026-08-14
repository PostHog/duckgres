package main

import (
	"container/list"
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
//
// Every operation below holds the one mutex, and a production node tracks
// hundreds of thousands of entries — so each critical section must be O(1).
// The previous slice-based order ([]cacheEntry with linear scans and splices)
// put ~half the proxy's CPU into LRU bookkeeping under this lock, serializing
// all cache traffic behind it.
type DiskCache struct {
	dir string
	// maxBytes is the eviction threshold. The constructor sets it to
	// maxPercent of the filesystem's TOTAL bytes, and the background refresh
	// loop (refreshCapacity) lowers it whenever FREE space shrinks so the
	// cache can never grow into disk it doesn't own. currentSize is the sum
	// of tracked entry sizes.
	maxBytes    int64
	currentSize int64

	mu sync.Mutex
	// order is the access list: least recently used at the front, most recent
	// at the back. index maps a key to its element for O(1) touch/drop.
	order *list.List
	index map[string]*list.Element
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
	freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
	// Start at the percent-of-total ceiling, immediately clamped to what is
	// actually free (well, free minus a reserve): the cache must never treat
	// space already consumed by anything else (container layers, other pods
	// sharing the filesystem, the rootfs on a tiny test disk) as available.
	maxBytes := clampToFree(totalBytes*int64(maxPercent)/100, totalBytes, freeBytes)

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
		order:    list.New(),
		index:    make(map[string]*list.Element),
	}

	// Scan existing cache entries
	dc.scanExisting()

	return dc, nil
}

// clampToFree bounds a capacity target by the space actually available on the
// filesystem: the cache may not grow into bytes it doesn't own (free space
// shrinks over time as other writers consume the disk), and it always leaves
// a small reserve so a full cache doesn't take the filesystem to 100%.
func clampToFree(target, totalBytes, freeBytes int64) int64 {
	reserve := totalBytes / 20 // 5% of total, kept for everyone else
	if avail := freeBytes - reserve; target > avail {
		target = avail
	}
	if target < 0 {
		target = 0
	}
	return target
}

// refreshCapacityStats recomputes maxBytes from the current statfs free space
// and the percent-of-total ceiling. Returns (total, clamped max, ok). Only
// ever LOWERS maxBytes: once another writer has eaten into the disk the cache
// commits to the smaller budget, so later frees don't balloon it back up.
func (c *DiskCache) refreshCapacityStats(maxPercent int) (int64, int64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(c.dir, &stat); err != nil {
		return 0, 0, false
	}
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
	target := clampToFree(totalBytes*int64(maxPercent)/100, totalBytes, freeBytes)

	c.mu.Lock()
	if target < c.maxBytes {
		evicted := 0
		for c.currentSize > target && c.order.Len() > 0 {
			c.evictOldest()
			evicted++
		}
		if target != c.maxBytes {
			slog.Warn("Cache capacity lowered to fit free disk.",
				"old_max", c.maxBytes, "new_max", target, "free", freeBytes, "evicted", evicted)
			c.maxBytes = target
			cacheCapacityBytes.Set(float64(target))
		}
	}
	max := c.maxBytes
	c.mu.Unlock()
	return totalBytes, max, true
}

// refreshCapacity periodically re-derives maxBytes from live statfs data so a
// disk that fills up from outside the cache shrinks the cache instead of the
// cache ENOSPC-ing after evicting healthy entries for room it never had.
func (c *DiskCache) refreshCapacity(maxPercent int) {
	c.refreshCapacityStats(maxPercent)
}

func (c *DiskCache) scanExisting() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	// Only count real cache entries — the .tmp dir's contents and any
	// stray non-key files must not enter the LRU accounting.
	var found []cacheEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !IsValidCacheKey(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		found = append(found, cacheEntry{
			key:        e.Name(),
			size:       info.Size(),
			lastAccess: info.ModTime(),
		})
	}
	// The access list must start in recency order (front = oldest) or the
	// first evictions after a restart would remove arbitrary entries.
	sort.Slice(found, func(i, j int) bool {
		return found[i].lastAccess.Before(found[j].lastAccess)
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range found {
		entry := found[i]
		c.index[entry.key] = c.order.PushBack(&entry)
		c.currentSize += entry.size
	}
	cacheSizeBytes.Set(float64(c.currentSize))
}

// Has returns true if the key is a tracked cache entry. It answers from the
// in-memory index under the mutex — not the filesystem — so "has it" always
// agrees with what eviction and size accounting believe, and costs no syscall.
func (c *DiskCache) Has(key string) bool {
	c.mu.Lock()
	_, ok := c.index[key]
	c.mu.Unlock()
	return ok
}

// Touch marks a key as most recently used without serving it. HandlePeerHas
// uses it: a peer /cache/has probe counts as an access, so an entry that is
// popular with peers is not evicted as "least recently used" while it is in
// fact one of the busiest blocks on the node.
func (c *DiskCache) Touch(key string) {
	c.mu.Lock()
	c.touchLocked(key)
	c.mu.Unlock()
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
	for c.currentSize+size > c.maxBytes && c.order.Len() > 0 {
		c.evictOldest()
	}
	c.mu.Unlock()

	path := filepath.Join(c.dir, key)
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("commit cache entry: %w", err)
	}

	c.mu.Lock()
	// addLocked drops any existing entry first: in production singleFlight
	// serializes writes per key so the earlier drop is enough, but the guard
	// keeps the "one entry per key" invariant (and currentSize) correct even
	// if some future caller writes the same key concurrently — a duplicate
	// entry would otherwise permanently inflate currentSize.
	c.addLocked(key, size)
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

// openFile opens and touches a cached entry WITHOUT recording a worker-facing
// hit. It serves bodies just fetched on a miss and peer API reads; neither is a
// local hit for the requesting worker.
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

// touchLocked marks key as most recently used. No-op if the key isn't
// tracked. Caller holds c.mu.
func (c *DiskCache) touchLocked(key string) {
	el, ok := c.index[key]
	if !ok {
		return
	}
	el.Value.(*cacheEntry).lastAccess = time.Now()
	c.order.MoveToBack(el)
}

// dropLocked removes any accounting for key (used when an overwrite is about to
// replace the file under it) so currentSize doesn't double-count. No-op if the
// key isn't tracked. Caller holds c.mu.
func (c *DiskCache) dropLocked(key string) {
	el, ok := c.index[key]
	if !ok {
		return
	}
	c.currentSize -= el.Value.(*cacheEntry).size
	c.order.Remove(el)
	delete(c.index, key)
}

// addLocked records a freshly written entry as most recently used, replacing
// any prior entry for the key so "one entry per key" (and currentSize) holds.
// Caller holds c.mu.
func (c *DiskCache) addLocked(key string, size int64) {
	c.dropLocked(key)
	c.index[key] = c.order.PushBack(&cacheEntry{
		key:        key,
		size:       size,
		lastAccess: time.Now(),
	})
	c.currentSize += size
	cacheSizeBytes.Set(float64(c.currentSize))
}

// evictOldest removes the least recently used entry. The list front is the
// LRU entry by construction: adds and touches always move entries to the
// back, and scanExisting seeds the list in recency order.
func (c *DiskCache) evictOldest() {
	front := c.order.Front()
	if front == nil {
		return
	}
	oldest := front.Value.(*cacheEntry)
	path := filepath.Join(c.dir, oldest.key)
	_ = os.Remove(path)
	c.currentSize -= oldest.size
	c.order.Remove(front)
	delete(c.index, oldest.key)
	cacheEvictionsTotal.Inc()
	cacheSizeBytes.Set(float64(c.currentSize))
}
