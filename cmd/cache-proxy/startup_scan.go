package main

import (
	"bufio"
	"container/heap"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	startupPruneRecordBytes = sha256.Size + 8 + 8
	startupScanChunkSize    = 1024
)

var (
	cacheStartupUninspectableFilesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_uninspectable_files_total",
		Help: "Valid-looking committed cache files preserved but excluded because metadata inspection failed",
	})
	cacheStartupDiscoveredOwnedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_startup_discovered_owned_bytes",
		Help: "Bytes in inspectable committed files discovered before hard-cap survivor pruning",
	})
	cacheStartupSelectedEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_startup_selected_entries",
		Help: "Inspectable committed entries selected for the bounded exact index during startup",
	})
	cacheStartupSelectedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_startup_selected_bytes",
		Help: "Bytes in committed entries selected for the bounded exact index during startup",
	})
	cacheStartupHardPruneCandidatesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_hard_prune_candidates_total",
		Help: "Committed files selected as non-survivors above the hard exact-index guardrail",
	})
	cacheStartupHardPruneCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_hard_prune_completed_total",
		Help: "Hard-guardrail non-survivors successfully removed during startup",
	})
	cacheStartupHardPruneFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_hard_prune_failures_total",
		Help: "Hard-guardrail non-survivors whose removal failed during startup",
	})
	cacheStartupHardPrunePreservedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_hard_prune_preserved_total",
		Help: "Hard-guardrail non-survivors preserved because they disappeared or changed after enumeration",
	})
	cacheStartupCancellationsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_proxy_cache_startup_cancellations_total",
		Help: "Startup scans or hard-prune passes canceled before completion",
	})
	cacheStartupPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cache_proxy_cache_startup_phase",
		Help: "Current startup cache phase; exactly one phase is 1 while startup is active",
	}, []string{"phase"})
)

type startupEntryHeap []*cacheEntry

func (h startupEntryHeap) Len() int      { return len(h) }
func (h startupEntryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h startupEntryHeap) Less(i, j int) bool {
	return startupEntryOlder(*h[i], *h[j])
}
func (h *startupEntryHeap) Push(value any) { *h = append(*h, value.(*cacheEntry)) }
func (h *startupEntryHeap) Pop() any {
	old := *h
	lastIndex := len(old) - 1
	last := old[lastIndex]
	old[lastIndex] = nil
	*h = old[:len(old)-1]
	return last
}

func startupEntryOlder(a, b cacheEntry) bool {
	if a.lastAccess.Equal(b.lastAccess) {
		return a.key < b.key
	}
	return a.lastAccess.Before(b.lastAccess)
}

func (c *DiskCache) scanExisting() (int64, error) {
	return c.scanExistingContext(context.Background())
}

func (c *DiskCache) scanExistingContext(ctx context.Context) (ownedBytes int64, scanErr error) {
	started := time.Now()
	defer func() {
		cacheStartupScanDuration.Observe(time.Since(started).Seconds())
		setStartupPhase("")
	}()
	setStartupPhase("enumerate")
	cacheStartupDiscoveredOwnedBytes.Set(0)
	cacheStartupSelectedEntries.Set(0)
	cacheStartupSelectedBytes.Set(0)

	hardLimit := c.hardMaxEntries
	if hardLimit <= 0 {
		hardLimit = int(cacheMetadataEntryLimit)
	}
	openScanDirectory := c.openScanDirectory
	if openScanDirectory == nil {
		openScanDirectory = openDirectory
	}
	dir, err := openScanDirectory(c.dir)
	if err != nil {
		return 0, fmt.Errorf("open cache directory %s: %w", c.dir, err)
	}
	dirClosed := false
	defer func() {
		if !dirClosed {
			if err := dir.Close(); err != nil && scanErr == nil {
				scanErr = fmt.Errorf("close cache directory %s: %w", c.dir, err)
			}
		}
	}()

	found := make(startupEntryHeap, 0, min(hardLimit, 1024))
	heapReady := false
	var spool *os.File
	var spoolWriter *bufio.Writer
	var spoolPath string
	defer func() {
		if spool != nil {
			_ = spool.Close()
		}
		if spoolPath != "" {
			_ = os.Remove(spoolPath)
		}
	}()

	writeLoser := func(entry *cacheEntry) error {
		if spool == nil {
			spool, err = os.CreateTemp(filepath.Join(c.dir, tmpSubdir), "hard-prune-*")
			if err != nil {
				return fmt.Errorf("create hard-prune spool: %w", err)
			}
			spoolPath = spool.Name()
			spoolWriter = bufio.NewWriterSize(spool, 256<<10)
		}
		var record [startupPruneRecordBytes]byte
		if _, err := hex.Decode(record[:sha256.Size], []byte(entry.key)); err != nil {
			return fmt.Errorf("decode cache key for hard-prune spool: %w", err)
		}
		binary.BigEndian.PutUint64(record[sha256.Size:sha256.Size+8], uint64(entry.size))
		binary.BigEndian.PutUint64(record[sha256.Size+8:], uint64(entry.lastAccess.UnixNano()))
		if _, err := spoolWriter.Write(record[:]); err != nil {
			return fmt.Errorf("write hard-prune spool: %w", err)
		}
		cacheStartupHardPruneCandidatesTotal.Inc()
		return nil
	}

	for {
		if err := startupContextError(ctx); err != nil {
			return 0, err
		}
		entries, readErr := dir.ReadDir(startupScanChunkSize)
		if err := startupContextError(ctx); err != nil {
			return 0, err
		}
		for i, directoryEntry := range entries {
			if i%256 == 0 {
				if err := startupContextError(ctx); err != nil {
					return 0, err
				}
			}
			cacheStartupScanFilesInspectedTotal.Inc()
			name := directoryEntry.Name()
			if name == tmpSubdir {
				continue
			}
			if !IsValidCacheKey(name) {
				cacheStartupInvalidFilesTotal.Inc()
				continue
			}
			info, infoErr := directoryEntry.Info()
			if infoErr != nil {
				cacheStartupUninspectableFilesTotal.Inc()
				continue
			}
			if !info.Mode().IsRegular() {
				cacheStartupInvalidFilesTotal.Inc()
				continue
			}
			entry := &cacheEntry{
				key:                  name,
				size:                 info.Size(),
				lastAccess:           info.ModTime(),
				lastPersistedRecency: info.ModTime().Truncate(c.recencyInterval),
			}
			ownedBytes = saturatingAdd(ownedBytes, entry.size)
			cacheStartupDiscoveredOwnedBytes.Set(float64(ownedBytes))
			if len(found) < hardLimit {
				found = append(found, entry)
				continue
			}
			if !heapReady {
				heap.Init(&found)
				heapReady = true
			}
			if !startupEntryOlder(*found[0], *entry) {
				if err := writeLoser(entry); err != nil {
					return 0, err
				}
				continue
			}
			loser := heap.Pop(&found).(*cacheEntry)
			if err := writeLoser(loser); err != nil {
				return 0, err
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
	if err := dir.Close(); err != nil {
		return 0, fmt.Errorf("close cache directory %s: %w", c.dir, err)
	}
	dirClosed = true
	if err := startupContextError(ctx); err != nil {
		return 0, err
	}

	if spool != nil {
		if err := spoolWriter.Flush(); err != nil {
			return 0, fmt.Errorf("flush hard-prune spool: %w", err)
		}
		if err := spool.Close(); err != nil {
			return 0, fmt.Errorf("close hard-prune spool: %w", err)
		}
		spool = nil
		if err := c.pruneStartupSpool(ctx, spoolPath); err != nil {
			return 0, err
		}
	}
	if err := startupContextError(ctx); err != nil {
		return 0, err
	}

	setStartupPhase("index")
	if !heapReady {
		heap.Init(&found)
	}
	selectedCount := len(found)
	selectedBytes := int64(0)
	c.mu.Lock()
	c.index = make(map[string]*list.Element, selectedCount)
	for i := 0; found.Len() > 0; i++ {
		if i%256 == 0 {
			if err := startupContextError(ctx); err != nil {
				c.mu.Unlock()
				return 0, err
			}
		}
		entry := heap.Pop(&found).(*cacheEntry)
		c.index[entry.key] = c.order.PushBack(entry)
		c.currentSize = saturatingAdd(c.currentSize, entry.size)
		selectedBytes = saturatingAdd(selectedBytes, entry.size)
		if c.summary != nil {
			c.summary.Add(entry.key)
		}
	}
	c.updateStateMetricsLocked()
	c.mu.Unlock()
	cacheStartupSelectedEntries.Set(float64(selectedCount))
	cacheStartupSelectedBytes.Set(float64(selectedBytes))
	return selectedBytes, nil
}

func (c *DiskCache) pruneStartupSpool(ctx context.Context, path string) error {
	setStartupPhase("hard_prune")
	spool, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open hard-prune spool: %w", err)
	}
	defer func() { _ = spool.Close() }()
	var record [startupPruneRecordBytes]byte
	for {
		if err := startupContextError(ctx); err != nil {
			return err
		}
		_, err := io.ReadFull(spool, record[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read hard-prune spool: %w", err)
		}
		key := hex.EncodeToString(record[:sha256.Size])
		size := int64(binary.BigEndian.Uint64(record[sha256.Size : sha256.Size+8]))
		mtime := int64(binary.BigEndian.Uint64(record[sha256.Size+8:]))
		entryPath := filepath.Join(c.dir, key)
		info, statErr := os.Lstat(entryPath)
		if statErr != nil {
			cacheStartupHardPrunePreservedTotal.Inc()
			continue
		}
		if !info.Mode().IsRegular() || info.Size() != size || info.ModTime().UnixNano() != mtime {
			cacheStartupHardPrunePreservedTotal.Inc()
			continue
		}
		deleted, err := c.removeCommittedFile(entryPath, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
		if err != nil {
			cacheStartupHardPruneFailuresTotal.Inc()
			return fmt.Errorf("remove hard-prune candidate %s: %w", key, err)
		}
		if !deleted {
			cacheStartupHardPrunePreservedTotal.Inc()
			continue
		}
		cacheStartupHardPruneCompletedTotal.Inc()
	}
}

func startupContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		cacheStartupCancellationsTotal.Inc()
		return err
	}
	return nil
}

func setStartupPhase(active string) {
	for _, phase := range []string{"enumerate", "hard_prune", "index"} {
		if phase == active {
			cacheStartupPhase.WithLabelValues(phase).Set(1)
		} else {
			cacheStartupPhase.WithLabelValues(phase).Set(0)
		}
	}
}
