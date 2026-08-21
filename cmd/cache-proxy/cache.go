package main

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
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
	cacheHardEntryLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_hard_entry_limit",
		Help: "Non-configurable exact-index safety guardrail",
	})
	cacheExactIndexEstimatedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_exact_index_estimated_bytes",
		Help: "Conservative estimated bytes occupied by exact-index metadata and keys",
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
	maxBytes       int64
	maxEntries     int
	hardMaxEntries int
	currentSize    int64
	maxPercent     int
	blockSize      int64
	space          capacityProvider

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

	// Durable recency is optional for tests and always enabled by main. Exact
	// LRU updates remain synchronous; only coarse mtime persistence is queued.
	recency         *recencyWriter
	recencyNow      func() time.Time
	recencyInterval time.Duration
	// commitMu linearizes the short final rename/accounting/recency commit with
	// shutdown without coupling lifecycle progress to a potentially blocking
	// pressure-driven unlink under mu.
	commitMu sync.Mutex

	convergenceCancel context.CancelFunc
	convergenceDone   chan struct{}
	convergenceWake   chan struct{}
	closing           atomic.Bool
}

const defaultCacheMaxEntries = 1_000_000

const (
	defaultCacheBlockSizeBytes       int64 = 8 << 20
	capacityRecoveryRequiredSamples        = 2
	defaultConvergenceInterval             = time.Millisecond
	estimatedExactIndexBytesPerEntry       = 256
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
	key                  string
	size                 int64
	lastAccess           time.Time
	lastPersistedRecency time.Time
	evictionInFlight     bool
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
		hardMaxEntries:    int(cacheMetadataEntryLimit),
		maxPercent:        maxPercent,
		blockSize:         defaultCacheBlockSizeBytes,
		space:             statfsDiskSpace,
		order:             list.New(),
		index:             make(map[string]*list.Element),
		renameFile:        os.Rename,
		removeFile:        os.Remove,
		openScanDirectory: openDirectory,
		recencyNow:        time.Now,
		recencyInterval:   defaultRecencyGranularity,
	}
	var option DiskCacheOptions
	if len(options) > 0 {
		option = options[0]
		if option.MaxEntries > 0 {
			dc.maxEntries = option.MaxEntries
		}
		if option.IncrementalSummary {
			dc.summary = newSummaryIndex()
		}
		if option.BlockSizeBytes > 0 {
			dc.blockSize = option.BlockSizeBytes
		}
		if option.CapacityProvider != nil {
			dc.space = option.CapacityProvider
		}
		if option.openScanDirectory != nil {
			dc.openScanDirectory = option.openScanDirectory
		}
		if option.removeFile != nil {
			dc.removeFile = option.removeFile
		}
		if option.hardMaxEntries > 0 {
			dc.hardMaxEntries = option.hardMaxEntries
		}
		if option.recencyNow != nil {
			dc.recencyNow = option.recencyNow
		}
		if option.recencyInterval > 0 {
			dc.recencyInterval = option.recencyInterval
		}
	}
	if dc.maxEntries > dc.hardMaxEntries {
		dc.maxEntries = dc.hardMaxEntries
	}

	startupContext := option.startupContext
	if startupContext == nil {
		startupContext = context.Background()
	}
	removedTemporaryFiles, err := resetTemporaryDirectory(startupContext, filepath.Join(dir, tmpSubdir))
	if err != nil {
		return nil, fmt.Errorf("reset cache temp dir: %w", err)
	}
	if removedTemporaryFiles > 0 {
		cacheTemporaryFilesRemovedTotal.Add(float64(removedTemporaryFiles))
	}

	// Complete enumeration and the exceptional hard-cap survivor pass before
	// sampling free space. No soft-target deletion occurs during startup.
	ownedBytes, err := dc.scanExistingContext(startupContext)
	if err != nil {
		return nil, fmt.Errorf("scan existing cache entries: %w", err)
	}
	startupSpace, err := dc.diskSpace()
	if err != nil {
		return nil, fmt.Errorf("statfs %s: %w", dir, err)
	}
	if err := dc.initializeCapacity(ownedBytes, startupSpace); err != nil {
		return nil, fmt.Errorf("apply startup cache limits: %w", err)
	}

	if option.DurableRecency {
		dc.recency = newRecencyWriter(dir, option.recencyQueueCapacity, option.recencyWorkerCount, option.recencyChtimes)
	}
	if option.convergencePermits != nil || option.BackgroundConvergence {
		dc.startConvergence(startupContext, option.convergencePermits, option.convergenceInterval)
	}

	slog.Info("Cache initialized.",
		"dir", dir,
		"max_cache", dc.maxBytes,
		"max_percent", maxPercent,
	)

	return dc, nil
}

type DiskCacheOptions struct {
	IncrementalSummary    bool
	DurableRecency        bool
	BackgroundConvergence bool
	MaxEntries            int
	BlockSizeBytes        int64
	CapacityProvider      capacityProvider
	openScanDirectory     openCacheDirectory
	removeFile            func(string) error
	hardMaxEntries        int
	startupContext        context.Context
	recencyNow            func() time.Time
	recencyChtimes        recencyPersistence
	recencyInterval       time.Duration
	recencyQueueCapacity  int
	recencyWorkerCount    int
	convergencePermits    <-chan struct{}
	convergenceInterval   time.Duration
}

// Close stops background convergence and drains accepted durable-recency work
// until ctx expires. It is safe to call more than once.
func (c *DiskCache) Close(ctx context.Context) error {
	c.closing.Store(true)
	if c.convergenceCancel != nil {
		c.convergenceCancel()
	}
	// A commit that already passed its closing check publishes its recency intent
	// before writer admission closes. Eviction syscalls never hold this gate, so
	// shutdown is not trapped behind a stalled unlink.
	c.commitMu.Lock()
	var recencyDone <-chan struct{}
	if c.recency != nil {
		recencyDone = c.recency.beginClose()
	}
	c.commitMu.Unlock()
	if c.convergenceDone != nil {
		select {
		case <-c.convergenceDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if recencyDone != nil {
		select {
		case <-recencyDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *DiskCache) waitForRecencyIdle(ctx context.Context) error {
	if c.recency == nil {
		return nil
	}
	return c.recency.waitIdle(ctx)
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
	c.applyCapacityLocked(capacity)
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
	c.updateStateMetricsLocked()
	return nil
}

func (c *DiskCache) applyCapacityLocked(capacity diskCapacity) {
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
	cacheHardEntryLimit.Set(float64(c.hardMaxEntries))
	cacheExactIndexEstimatedBytes.Set(float64(c.order.Len()) * estimatedExactIndexBytesPerEntry)
	c.updateConvergenceMetricsLocked()
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

func resetTemporaryDirectory(ctx context.Context, tmpDir string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	removed, err := removeTemporaryTree(ctx, tmpDir)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(tmpDir, 0750); err != nil {
		return 0, err
	}
	return removed, nil
}

// removeTemporaryTree is the cancellation-aware equivalent of RemoveAll for
// the private staging directory. It never follows symlinks and checks the
// startup context before each filesystem operation, so a large interrupted
// fill tree cannot delay SIGTERM until the entire cleanup finishes.
func removeTemporaryTree(ctx context.Context, path string) (int, error) {
	return removeTemporaryTreeWith(ctx, path, openDirectory)
}

func removeTemporaryTreeWith(ctx context.Context, path string, openDir openCacheDirectory) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.IsDir() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		return 1, nil
	}

	removed := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		dir, err := openDir(path)
		if err != nil {
			return 0, err
		}
		entries, readErr := dir.ReadDir(startupScanChunkSize)
		closeErr := dir.Close()
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if closeErr != nil {
			return 0, closeErr
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return 0, readErr
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			count, err := removeTemporaryTreeWith(ctx, filepath.Join(path, entry.Name()), openDir)
			if err != nil {
				return 0, err
			}
			removed += count
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	return removed, nil
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
	if c.closing.Load() {
		return 0, errors.New("cache is closed for writes")
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
	if c.closing.Load() {
		c.mu.Unlock()
		_ = os.Remove(tmpPath)
		return 0, errors.New("cache is closed for writes")
	}
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
	c.commitMu.Lock()
	if c.closing.Load() {
		c.commitMu.Unlock()
		c.mu.Unlock()
		_ = os.Remove(tmpPath)
		return 0, errors.New("cache is closed for writes")
	}
	if err := renameFile(tmpPath, path); err != nil {
		c.commitMu.Unlock()
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
		now := c.now()
		entry.lastAccess = now
		c.order.MoveToBack(el)
		c.queueRecencyLocked(entry, now, true)
		c.updateStateMetricsLocked()
	} else {
		c.addLocked(key, size)
	}
	c.commitMu.Unlock()
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
	entry := el.Value.(*cacheEntry)
	now := c.now()
	entry.lastAccess = now
	c.order.MoveToBack(el)
	c.queueRecencyLocked(entry, now, false)
}

func (c *DiskCache) now() time.Time {
	if c.recencyNow == nil {
		return time.Now()
	}
	return c.recencyNow()
}

func (c *DiskCache) queueRecencyLocked(entry *cacheEntry, timestamp time.Time, force bool) {
	if c.recency == nil {
		return
	}
	interval := c.recencyInterval
	if interval <= 0 {
		interval = defaultRecencyGranularity
	}
	bucket := timestamp
	if !force {
		cacheRecencyTouchAttemptsTotal.Inc()
		bucket = timestamp.Truncate(interval)
	}
	if !force && !bucket.After(entry.lastPersistedRecency) {
		cacheRecencyTouchCoalescedTotal.Inc()
		return
	}
	_ = c.recency.submit(entry.key, bucket)
	// A dropped update intentionally degrades only this bucket's restart
	// accuracy. Recording the attempt prevents a hot overflowed key from
	// contending on the bounded writer for every hit; the next bucket retries.
	entry.lastPersistedRecency = bucket
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
	now := c.now()
	entry := &cacheEntry{
		key:        key,
		size:       size,
		lastAccess: now,
	}
	c.index[key] = c.order.PushBack(entry)
	c.currentSize += size
	if c.summary != nil {
		c.summary.Add(key)
	}
	c.queueRecencyLocked(entry, now, true)
	c.updateStateMetricsLocked()
}

// evictOldest removes the least recently used entry. The list front is the
// LRU entry by construction: adds and touches always move entries to the
// back, and scanExisting seeds the list in recency order.
func (c *DiskCache) evictOldest(phase cacheEvictionPhase, reason cacheEvictionReason) error {
	front := c.oldestEvictableLocked()
	if front == nil {
		return errors.New("no cache entry available for eviction")
	}
	oldest := front.Value.(*cacheEntry)
	path := filepath.Join(c.dir, oldest.key)
	if _, err := c.removeCommittedFile(path, phase, reason); err != nil {
		return err
	}
	c.forgetEntryLocked(front)
	return nil
}

func (c *DiskCache) oldestEvictableLocked() *list.Element {
	for candidate := c.order.Front(); candidate != nil; candidate = candidate.Next() {
		if !candidate.Value.(*cacheEntry).evictionInFlight {
			return candidate
		}
	}
	return nil
}

func (c *DiskCache) forgetEntryLocked(element *list.Element) {
	entry := element.Value.(*cacheEntry)
	c.currentSize -= entry.size
	c.order.Remove(element)
	delete(c.index, entry.key)
	if c.summary != nil {
		c.summary.Remove(entry.key)
	}
	c.updateStateMetricsLocked()
}

func (c *DiskCache) makeRoomForNewEntryLocked(size int64, phase cacheEvictionPhase) bool {
	if c.order.Len() == 0 {
		return true
	}
	// Byte capacity protects the filesystem reserve and remains strict on the
	// request path. In block mode this normally removes one equal-sized block;
	// the loop also handles short tail blocks without admitting above capacity.
	for c.currentSize+size > c.maxBytes && c.order.Len() > 0 {
		if c.evictOldest(phase, cacheEvictionReasonByte) != nil {
			return false
		}
	}
	// Entry count is a soft convergence target. At or above it, perform only a
	// net-neutral one-for-one swap rather than collapsing restart overage here.
	if c.order.Len() >= c.maxEntries && c.order.Len() > 0 {
		if c.evictOldest(phase, cacheEvictionReasonEntry) != nil {
			return false
		}
	}
	return true
}

func (c *DiskCache) makeRoomForReplacementLocked(replacement *list.Element, size int64, phase cacheEvictionPhase) bool {
	entry := replacement.Value.(*cacheEntry)
	if entry.evictionInFlight {
		return false
	}
	// Protect the entry being replaced from eviction. If reservation or rename
	// fails, its old committed body and accounting remain usable.
	c.order.MoveToBack(replacement)
	projectedSize := c.currentSize - entry.size + size
	// A replacement is count-neutral. Only evict one unrelated LRU entry when
	// the replacement itself would make byte pressure worse; the background
	// worker handles any pre-existing overage at its bounded rate.
	for projectedSize > c.maxBytes && c.order.Len() > 1 {
		if c.evictOldest(phase, cacheEvictionReasonByte) != nil {
			return false
		}
		projectedSize = c.currentSize - entry.size + size
	}
	return true
}

// removeCommittedFile is the one filesystem deletion path for valid cache
// files under pressure. A successful unlink is always visible in both the
// aggregate eviction counter and its bounded phase/reason breakdown. A file
// already removed by an external actor is forgotten without claiming an
// eviction; a hard unlink failure preserves its index accounting.
func (c *DiskCache) removeCommittedFile(path string, phase cacheEvictionPhase, reason cacheEvictionReason) (bool, error) {
	removeFile := c.removeFile
	if removeFile == nil {
		removeFile = os.Remove
	}
	err := removeFile(path)
	if err == nil {
		cacheEvictionsTotal.Inc()
		cacheEvictionsByPhaseReasonTotal.WithLabelValues(string(phase), string(reason)).Inc()
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	slog.Warn("Unable to evict committed cache file.", "path", path, "phase", phase, "reason", reason, "error", err)
	return false, err
}
