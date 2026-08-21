package main

import (
	"container/heap"
	"container/list"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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
	cacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_entries",
		Help: "Current committed cache entries represented by the exact index",
	})
	cacheEntryLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_entry_limit",
		Help: "Active committed cache entry limit",
	})
	cacheOwnedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_owned_bytes",
		Help: "Committed cache bytes owned and reclaimable by the cache",
	})
	cacheCapacityBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_capacity_bytes",
		Help: "Maximum cache capacity",
	})
	cacheDiskTargetBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_disk_target_bytes",
		Help: "Configured percentage-of-disk cache target before free-space clamping",
	})
	cacheDiskReserveBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_disk_reserve_bytes",
		Help: "Whole-filesystem reserve excluded from cache capacity",
	})
	cacheEntryLimitReason = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_entry_limit_reason",
		Help: "Future derived entry-limit bottleneck; exactly one of disk or metadata is 1",
	}, []string{"reason"})
	cacheEvictionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_evictions_total",
		Help: "Number of LRU cache evictions",
	})
	cacheEvictionsByPhaseReasonTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_proxy_evictions_by_phase_reason_total",
		Help: "Committed cache-entry evictions by bounded phase and pressure reason",
	}, []string{"phase", "reason"})
	cacheStartupScanDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "cache_proxy_cache_startup_scan_duration_seconds",
		Help: "Duration of cache startup scans",
	})
	cacheStartupScanFilesInspectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_scan_files_inspected_total",
		Help: "Directory entries inspected during cache startup scans",
	})
	cacheStartupInvalidFilesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_invalid_files_total",
		Help: "Invalid or unrelated root-directory entries excluded from cache ownership during startup scans",
	})
	cacheTemporaryFilesRemovedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_temporary_files_removed_total",
		Help: "Incomplete temporary cache files removed during startup cleanup",
	})
)

type cacheEvictionPhase = string

const (
	cacheEvictionPhaseStartup    cacheEvictionPhase = "startup"
	cacheEvictionPhaseBackground cacheEvictionPhase = "background"
	cacheEvictionPhaseRequest    cacheEvictionPhase = "request"
)

type cacheEvictionReason = string

const (
	cacheEvictionReasonEntry cacheEvictionReason = "entry"
	cacheEvictionReasonByte  cacheEvictionReason = "byte"
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
	maxEntries  int
	currentSize int64
	maxPercent  int
	blockSize   int64
	space       capacityProvider

	// Consecutive above-current samples are required before raising capacity.
	// Reductions apply immediately to restore the filesystem reserve.
	recoveryCandidate int64
	recoverySamples   int

	mu sync.Mutex
	// order is the access list: least recently used at the front, most recent
	// at the back. index maps a key to its element for O(1) touch/drop.
	order *list.List
	index map[string]*list.Element
	// summary is enabled only in summary lookup mode. It mirrors index
	// mutations incrementally, so snapshot serving never scans the cache index.
	summary *summaryIndex
	// renameFile is os.Rename in production and a per-cache failure seam in tests.
	renameFile func(string, string) error
	// removeFile is os.Remove in production and a per-cache failure seam in tests.
	removeFile func(string) error
	// openScanDirectory is os.Open in production and lets constructor tests
	// inject directory-open and mid-scan failures deterministically.
	openScanDirectory openCacheDirectory
}

const defaultCacheMaxEntries = 1_000_000

const (
	defaultCacheBlockSizeBytes      int64 = 8 << 20
	capacityRecoveryRequiredSamples       = 2
)

type diskSpace struct {
	TotalBytes int64
	FreeBytes  int64
}

type capacityProvider func(string) (diskSpace, error)

type cacheDirectory interface {
	ReadDir(int) ([]os.DirEntry, error)
	Close() error
}

type openCacheDirectory func(string) (cacheDirectory, error)

func openDirectory(path string) (cacheDirectory, error) {
	return os.Open(path)
}

type cacheEntry struct {
	key        string
	size       int64
	lastAccess time.Time
}

type oldestEntries []cacheEntry

func (h oldestEntries) Len() int           { return len(h) }
func (h oldestEntries) Less(i, j int) bool { return h[i].lastAccess.Before(h[j].lastAccess) }
func (h oldestEntries) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *oldestEntries) Push(x any)        { *h = append(*h, x.(cacheEntry)) }
func (h *oldestEntries) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewDiskCache creates a cache backed by the given directory.
// maxPercent is the percentage of filesystem capacity to use (e.g. 80).
func NewDiskCache(dir string, maxPercent int, options ...DiskCacheOptions) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	dc := &DiskCache{
		dir: dir,
		// Startup must account for committed files before applying a byte
		// ceiling, so scans initially load without byte pruning.
		maxBytes:          math.MaxInt64,
		maxEntries:        defaultCacheMaxEntries,
		maxPercent:        maxPercent,
		blockSize:         defaultCacheBlockSizeBytes,
		space:             statfsDiskSpace,
		order:             list.New(),
		index:             make(map[string]*list.Element),
		renameFile:        os.Rename,
		removeFile:        os.Remove,
		openScanDirectory: openDirectory,
	}
	if len(options) > 0 {
		if options[0].MaxEntries > 0 {
			dc.maxEntries = options[0].MaxEntries
		}
		if options[0].IncrementalSummary {
			dc.summary = newSummaryIndex()
		}
		if options[0].BlockSizeBytes > 0 {
			dc.blockSize = options[0].BlockSizeBytes
		}
		if options[0].CapacityProvider != nil {
			dc.space = options[0].CapacityProvider
		}
		if options[0].openScanDirectory != nil {
			dc.openScanDirectory = options[0].openScanDirectory
		}
		if options[0].removeFile != nil {
			dc.removeFile = options[0].removeFile
		}
	}

	removedTemporaryFiles, err := resetTemporaryDirectory(filepath.Join(dir, tmpSubdir))
	if err != nil {
		return nil, fmt.Errorf("reset cache temp dir: %w", err)
	}
	if removedTemporaryFiles > 0 {
		cacheTemporaryFilesRemovedTotal.Add(float64(removedTemporaryFiles))
	}

	// Sample free space before the scan can apply the legacy entry ceiling.
	// Together with the scan's owned-byte total, this is a consistent restart
	// view: if the scan deletes entry-limit victims before statfs runs, adding
	// their former bytes to the newer free sample would double count them.
	startupSpace, err := dc.diskSpace()
	if err != nil {
		return nil, fmt.Errorf("statfs %s: %w", dir, err)
	}

	// Scan existing cache entries before capacity is derived. The scan's owned
	// byte total includes every valid committed file, even ones later discarded
	// by the legacy entry ceiling, so a restart never mistakes the cache itself
	// for external disk pressure.
	ownedBytes, err := dc.scanExisting()
	if err != nil {
		return nil, fmt.Errorf("scan existing cache entries: %w", err)
	}
	if err := dc.initializeCapacity(ownedBytes, startupSpace); err != nil {
		return nil, fmt.Errorf("apply startup cache limits: %w", err)
	}

	slog.Info("Cache initialized.",
		"dir", dir,
		"max_cache", dc.maxBytes,
		"max_percent", maxPercent,
	)

	return dc, nil
}

type DiskCacheOptions struct {
	IncrementalSummary bool
	MaxEntries         int
	BlockSizeBytes     int64
	CapacityProvider   capacityProvider
	openScanDirectory  openCacheDirectory
	removeFile         func(string) error
}

// clampToFree remains a narrow compatibility helper for callers that have no
// owned-cache total. DiskCache itself always uses deriveDiskCapacity instead.
func clampToFree(target, totalBytes, freeBytes int64) int64 {
	reserve := percentageOf(nonNegative(totalBytes), cacheDiskReservePercent)
	if avail := freeBytes - reserve; target > avail {
		target = avail
	}
	if target < 0 {
		target = 0
	}
	return target
}

// refreshCapacityStats recomputes maxBytes from live disk state. Valid
// committed files are reclaimable cache-owned bytes, unlike invalid and
// unrelated files. Capacity reductions are immediate to restore the 5%
// reserve; capacity recovery requires two consecutive healthy samples to keep
// short-lived external writers from flapping the advertised ceiling.
func (c *DiskCache) refreshCapacityStats(maxPercent int) (int64, int64, bool) {
	space, err := c.diskSpace()
	if err != nil {
		return 0, 0, false
	}

	c.mu.Lock()
	capacity := deriveDiskCapacity(space.TotalBytes, space.FreeBytes, c.currentSize, maxPercent)
	c.applyCapacityLocked(capacity, cacheEvictionPhaseBackground)
	max := c.maxBytes
	c.mu.Unlock()
	return space.TotalBytes, max, true
}

// refreshCapacity periodically re-derives maxBytes from live statfs data so a
// disk that fills up from outside the cache shrinks the cache instead of the
// cache ENOSPC-ing after evicting healthy entries for room it never had.
func (c *DiskCache) refreshCapacity(maxPercent int) {
	c.refreshCapacityStats(maxPercent)
}

func (c *DiskCache) initializeCapacity(ownedBytes int64, space diskSpace) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	capacity := deriveDiskCapacity(space.TotalBytes, space.FreeBytes, ownedBytes, c.maxPercent)
	c.maxBytes = capacity.ByteCeiling
	c.recoveryCandidate = 0
	c.recoverySamples = 0
	c.updateCapacityMetricsLocked(capacity)
	if err := c.evictToLimitsLocked(cacheEvictionPhaseStartup); err != nil {
		c.updateStateMetricsLocked()
		return err
	}
	c.updateStateMetricsLocked()
	return nil
}

func (c *DiskCache) applyCapacityLocked(capacity diskCapacity, phase cacheEvictionPhase) {
	oldMax := c.maxBytes
	newMax := capacity.ByteCeiling
	switch {
	case newMax < oldMax:
		c.maxBytes = newMax
		c.recoveryCandidate = 0
		c.recoverySamples = 0
		slog.Warn("Cache capacity lowered to fit live disk pressure.",
			"old_max", oldMax, "new_max", newMax, "owned_bytes", c.currentSize)
	case newMax > oldMax:
		if newMax >= c.recoveryCandidate {
			c.recoveryCandidate = newMax
			c.recoverySamples++
		} else {
			c.recoveryCandidate = newMax
			c.recoverySamples = 1
		}
		if c.recoverySamples >= capacityRecoveryRequiredSamples {
			c.maxBytes = newMax
			c.recoveryCandidate = 0
			c.recoverySamples = 0
			slog.Info("Cache capacity recovered after stable disk samples.", "old_max", oldMax, "new_max", newMax)
		}
	default:
		c.recoveryCandidate = 0
		c.recoverySamples = 0
	}
	// Background convergence remains best effort: a hard deletion failure is
	// logged by removeCommittedFile and leaves the entry indexed rather than
	// taking the running proxy down. Retry on every refresh while over either
	// active limit, including when the derived byte ceiling is unchanged.
	if c.currentSize > c.maxBytes || c.order.Len() > c.maxEntries {
		_ = c.evictToLimitsLocked(phase)
	}
	c.updateCapacityMetricsLocked(capacity)
	c.updateStateMetricsLocked()
}

func (c *DiskCache) updateCapacityMetricsLocked(capacity diskCapacity) {
	cacheCapacityBytes.Set(float64(c.maxBytes))
	cacheDiskTargetBytes.Set(float64(capacity.DiskTargetBytes))
	cacheDiskReserveBytes.Set(float64(capacity.ReserveBytes))

	// The active admission ceiling remains the compatibility 1M value in PR 1.
	// These two reason series describe which guardrail would limit the future
	// disk-derived ceiling, keeping the rollout observable before enabling it.
	if deriveCacheEntryCeiling(capacity.ByteCeiling, c.blockSize) >= cacheMetadataEntryLimit {
		cacheEntryLimitReason.WithLabelValues("disk").Set(0)
		cacheEntryLimitReason.WithLabelValues("metadata").Set(1)
	} else {
		cacheEntryLimitReason.WithLabelValues("disk").Set(1)
		cacheEntryLimitReason.WithLabelValues("metadata").Set(0)
	}
}

func (c *DiskCache) updateStateMetricsLocked() {
	cacheSizeBytes.Set(float64(c.currentSize))
	cacheOwnedBytes.Set(float64(c.currentSize))
	cacheEntries.Set(float64(c.order.Len()))
	cacheEntryLimit.Set(float64(c.maxEntries))
}

func (c *DiskCache) diskSpace() (diskSpace, error) {
	if c.space == nil {
		return diskSpace{}, errors.New("no disk capacity provider")
	}
	return c.space(c.dir)
}

func statfsDiskSpace(dir string) (diskSpace, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return diskSpace{}, err
	}
	blockSize := uint64(stat.Bsize)
	return diskSpace{
		TotalBytes: filesystemBytes(stat.Blocks, blockSize),
		FreeBytes:  filesystemBytes(stat.Bavail, blockSize),
	}, nil
}

func filesystemBytes(blocks, blockSize uint64) int64 {
	if blocks == 0 || blockSize == 0 {
		return 0
	}
	if blocks > uint64(math.MaxInt64)/blockSize {
		return math.MaxInt64
	}
	return int64(blocks * blockSize)
}

func resetTemporaryDirectory(tmpDir string) (int, error) {
	removed := 0
	if _, err := os.Lstat(tmpDir); err == nil {
		err = filepath.WalkDir(tmpDir, func(_ string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !entry.IsDir() {
				removed++
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if err := os.RemoveAll(tmpDir); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(tmpDir, 0750); err != nil {
		return 0, err
	}
	return removed, nil
}

func (c *DiskCache) scanExisting() (ownedBytes int64, scanErr error) {
	started := time.Now()
	defer func() { cacheStartupScanDuration.Observe(time.Since(started).Seconds()) }()

	openScanDirectory := c.openScanDirectory
	if openScanDirectory == nil {
		openScanDirectory = openDirectory
	}
	dir, err := openScanDirectory(c.dir)
	if err != nil {
		return 0, fmt.Errorf("open cache directory %s: %w", c.dir, err)
	}
	defer func() {
		if err := dir.Close(); err != nil && scanErr == nil {
			scanErr = fmt.Errorf("close cache directory %s: %w", c.dir, err)
		}
	}()
	// Only count real, committed cache entries. Temporary files were removed
	// before this scan; invalid and unrelated root entries stay on disk as
	// external usage and never enter the exact index or owned-byte accounting.
	// Do not reserve a million cacheEntry structs for an empty/new cache. The
	// heap remains bounded by maxEntries but grows with actual disk contents.
	found := make(oldestEntries, 0, min(c.maxEntries, 1024))
	for {
		entries, readErr := dir.ReadDir(1024)
		for _, e := range entries {
			cacheStartupScanFilesInspectedTotal.Inc()
			if e.Name() == tmpSubdir {
				continue
			}
			if !IsValidCacheKey(e.Name()) {
				cacheStartupInvalidFilesTotal.Inc()
				continue
			}
			info, err := e.Info()
			if err != nil {
				return 0, fmt.Errorf("inspect cache entry %s: %w", filepath.Join(c.dir, e.Name()), err)
			}
			if !info.Mode().IsRegular() {
				cacheStartupInvalidFilesTotal.Inc()
				continue
			}
			entry := cacheEntry{key: e.Name(), size: info.Size(), lastAccess: info.ModTime()}
			ownedBytes = saturatingAdd(ownedBytes, entry.size)
			if found.Len() < c.maxEntries {
				heap.Push(&found, entry)
				continue
			}
			if !entry.lastAccess.After(found[0].lastAccess) {
				path := filepath.Join(c.dir, entry.key)
				if err := c.removeCommittedFile(path, cacheEvictionPhaseStartup, cacheEvictionReasonEntry); err != nil {
					return 0, fmt.Errorf("prune startup cache entry %s: %w", path, err)
				}
				continue
			}
			dropped := heap.Pop(&found).(cacheEntry)
			path := filepath.Join(c.dir, dropped.key)
			if err := c.removeCommittedFile(path, cacheEvictionPhaseStartup, cacheEvictionReasonEntry); err != nil {
				return 0, fmt.Errorf("prune startup cache entry %s: %w", path, err)
			}
			heap.Push(&found, entry)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read cache directory %s: %w", c.dir, readErr)
		}
	}
	// The access list must start in recency order (front = oldest) or the
	// first evictions after a restart would remove arbitrary entries.
	sort.Slice(found, func(i, j int) bool {
		return found[i].lastAccess.Before(found[j].lastAccess)
	})

	c.mu.Lock()
	for i := range found {
		entry := found[i]
		c.index[entry.key] = c.order.PushBack(&entry)
		c.currentSize += entry.size
		if c.summary != nil {
			c.summary.Add(entry.key)
		}
	}
	// Direct callers that construct a DiskCache by hand retain the historical
	// scan behavior. NewDiskCache defers byte pruning until its owned-byte
	// capacity calculation is complete.
	if c.space == nil {
		if err := c.evictToLimitsLocked(cacheEvictionPhaseStartup); err != nil {
			c.updateStateMetricsLocked()
			c.mu.Unlock()
			return 0, fmt.Errorf("apply startup cache limits: %w", err)
		}
	}
	c.updateStateMetricsLocked()
	c.mu.Unlock()
	return ownedBytes, nil
}

// SummarySnapshot returns an immutable bounded copy of the locally maintained
// Bloom bits. It does not scan or lock the cache index.
func (c *DiskCache) SummarySnapshot() (items int, bits []byte, ok bool) {
	if c.summary == nil {
		return 0, nil, false
	}
	items, bits = c.summary.Snapshot()
	return items, bits, true
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

	path := filepath.Join(c.dir, key)
	renameFile := c.renameFile
	if renameFile == nil {
		renameFile = os.Rename
	}
	// Make the local metadata rename and its index/Bloom bookkeeping one atomic
	// commit. Streaming happened above without this lock; only the short rename
	// syscall is serialized so eviction cannot delete the replacement in between.
	c.mu.Lock()
	el, replacing := c.index[key]
	if replacing {
		if !c.makeRoomForReplacementLocked(el, size, cacheEvictionPhaseRequest) {
			c.mu.Unlock()
			_ = os.Remove(tmpPath)
			return 0, errors.New("admit cache replacement: required eviction failed")
		}
	} else {
		if !c.makeRoomForNewEntryLocked(size, cacheEvictionPhaseRequest) {
			c.mu.Unlock()
			_ = os.Remove(tmpPath)
			return 0, errors.New("admit cache entry: required eviction failed")
		}
	}
	if err := renameFile(tmpPath, path); err != nil {
		c.mu.Unlock()
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("commit cache entry: %w", err)
	}

	if replacing {
		// The key remained represented throughout the filesystem commit. Update
		// its accounting in place so a concurrent Bloom snapshot can never see a
		// transient negative for an already committed entry.
		entry := el.Value.(*cacheEntry)
		c.currentSize += size - entry.size
		entry.size = size
		entry.lastAccess = time.Now()
		c.order.MoveToBack(el)
		c.updateStateMetricsLocked()
	} else {
		c.addLocked(key, size)
	}
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
	if c.summary != nil {
		c.summary.Remove(key)
	}
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
	if c.summary != nil {
		c.summary.Add(key)
	}
	c.updateStateMetricsLocked()
}

// evictOldest removes the least recently used entry. The list front is the
// LRU entry by construction: adds and touches always move entries to the
// back, and scanExisting seeds the list in recency order.
func (c *DiskCache) evictOldest(phase cacheEvictionPhase, reason cacheEvictionReason) error {
	front := c.order.Front()
	if front == nil {
		return nil
	}
	oldest := front.Value.(*cacheEntry)
	path := filepath.Join(c.dir, oldest.key)
	if err := c.removeCommittedFile(path, phase, reason); err != nil {
		return err
	}
	c.currentSize -= oldest.size
	c.order.Remove(front)
	delete(c.index, oldest.key)
	if c.summary != nil {
		c.summary.Remove(oldest.key)
	}
	c.updateStateMetricsLocked()
	return nil
}

func (c *DiskCache) makeRoomForNewEntryLocked(size int64, phase cacheEvictionPhase) bool {
	for c.order.Len() >= c.maxEntries && c.order.Len() > 0 {
		if c.evictOldest(phase, cacheEvictionReasonEntry) != nil {
			return false
		}
	}
	for c.currentSize+size > c.maxBytes && c.order.Len() > 0 {
		if c.evictOldest(phase, cacheEvictionReasonByte) != nil {
			return false
		}
	}
	return true
}

func (c *DiskCache) makeRoomForReplacementLocked(replacement *list.Element, size int64, phase cacheEvictionPhase) bool {
	entry := replacement.Value.(*cacheEntry)
	// Protect the entry being replaced from eviction. If reservation or rename
	// fails, its old committed body and accounting remain usable.
	c.order.MoveToBack(replacement)
	for c.order.Len() > c.maxEntries && c.order.Len() > 1 {
		if c.evictOldest(phase, cacheEvictionReasonEntry) != nil {
			return false
		}
	}
	for c.currentSize-entry.size+size > c.maxBytes && c.order.Len() > 1 {
		if c.evictOldest(phase, cacheEvictionReasonByte) != nil {
			return false
		}
	}
	return true
}

func (c *DiskCache) evictToLimitsLocked(phase cacheEvictionPhase) error {
	for c.order.Len() > c.maxEntries && c.order.Len() > 0 {
		if err := c.evictOldest(phase, cacheEvictionReasonEntry); err != nil {
			return err
		}
	}
	for c.currentSize > c.maxBytes && c.order.Len() > 0 {
		if err := c.evictOldest(phase, cacheEvictionReasonByte); err != nil {
			return err
		}
	}
	return nil
}

// removeCommittedFile is the one filesystem deletion path for valid cache
// files under pressure. A successful unlink is always visible in both the
// aggregate eviction counter and its bounded phase/reason breakdown. A file
// already removed by an external actor is forgotten without claiming an
// eviction; a hard unlink failure preserves its index accounting.
func (c *DiskCache) removeCommittedFile(path string, phase cacheEvictionPhase, reason cacheEvictionReason) error {
	removeFile := c.removeFile
	if removeFile == nil {
		removeFile = os.Remove
	}
	err := removeFile(path)
	if err == nil {
		cacheEvictionsTotal.Inc()
		cacheEvictionsByPhaseReasonTotal.WithLabelValues(string(phase), string(reason)).Inc()
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	slog.Warn("Unable to evict committed cache file.", "path", path, "phase", phase, "reason", reason, "error", err)
	return err
}
