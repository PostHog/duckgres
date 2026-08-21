package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// startupInfoErrorEntry models a valid cache-key directory entry whose
// metadata cannot be inspected. The startup scanner must preserve it on disk
// while treating its bytes as external/unowned for this process lifetime.
type startupInfoErrorEntry struct {
	os.DirEntry
	err error
}

func (e startupInfoErrorEntry) Info() (os.FileInfo, error) {
	return nil, e.err
}

// cancelAfterReadDirectory makes cancellation deterministic: the scan receives
// a real chunk, but its context is canceled before it may prune anything from
// that chunk.
type cancelAfterReadDirectory struct {
	entries []os.DirEntry
	cancel  context.CancelFunc
	read    bool
}

func (d *cancelAfterReadDirectory) ReadDir(int) ([]os.DirEntry, error) {
	if d.read {
		return nil, io.EOF
	}
	d.read = true
	d.cancel()
	return d.entries, nil
}

func (d *cancelAfterReadDirectory) Close() error { return nil }

type cancelOnCloseDirectory struct {
	entries []os.DirEntry
	cancel  context.CancelFunc
	read    bool
}

func (d *cancelOnCloseDirectory) ReadDir(int) ([]os.DirEntry, error) {
	if d.read {
		return nil, io.EOF
	}
	d.read = true
	return d.entries, io.EOF
}

func (d *cancelOnCloseDirectory) Close() error {
	d.cancel()
	return nil
}

func writeStartupScanEntry(t *testing.T, dir, key string, body []byte, mtime time.Time) {
	t.Helper()
	path := filepath.Join(dir, key)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write startup entry %s: %v", key, err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("set startup entry mtime %s: %v", key, err)
	}
}

func assertStartupFilesExist(t *testing.T, dir string, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := os.Stat(filepath.Join(dir, key)); err != nil {
			t.Errorf("startup entry %s was changed or removed: %v", key, err)
		}
	}
}

func testStartupCapacityProvider(string) (diskSpace, error) {
	return diskSpace{TotalBytes: 1 << 30, FreeBytes: 1 << 30}, nil
}

func TestStartupScanLoadsAllEntriesWithinHardLimitAboveSoftTarget(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)}
	for i, key := range keys {
		writeStartupScanEntry(t, dir, key, []byte{byte(i)}, now.Add(time.Duration(i)*time.Second))
	}

	removals := 0
	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       2, // PR2 compatibility soft target.
		hardMaxEntries:   4, // Test-only small form of the fixed 10M guardrail.
		CapacityProvider: testStartupCapacityProvider,
		removeFile: func(path string) error {
			removals++
			return os.Remove(path)
		},
	})
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	if got := c.order.Len(); got != len(keys) {
		t.Fatalf("startup loaded %d entries, want all %d entries below the hard guardrail", got, len(keys))
	}
	for _, key := range keys {
		if !c.Has(key) {
			t.Errorf("startup omitted within-hard-limit key %s", key)
		}
	}
	if removals != 0 {
		t.Fatalf("startup performed %d soft-target deletions, want 0", removals)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 0 {
		t.Fatalf("aggregate startup evictions = %v, want 0", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry) - beforeLabel; got != 0 {
		t.Fatalf("labeled startup entry evictions = %v, want 0", got)
	}
	assertStartupFilesExist(t, dir, keys...)
}

func TestStartupScanPreservesUninspectableValidEntryAndContinues(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	body := []byte("preserve-me")
	writeStartupScanEntry(t, dir, key, body, time.Now())
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("fixture entries = %d, want 1", len(entries))
	}
	infoErr := errors.New("forced entry metadata failure")
	entries[0] = startupInfoErrorEntry{DirEntry: entries[0], err: infoErr}

	removals := 0
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       1,
		hardMaxEntries:   4,
		CapacityProvider: testStartupCapacityProvider,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &scriptedCacheDirectory{entries: entries, err: io.EOF}, nil
		},
		removeFile: func(path string) error {
			removals++
			return os.Remove(path)
		},
	})
	if err != nil {
		t.Fatalf("NewDiskCache rejected one uninspectable entry: %v", err)
	}
	if c.Has(key) || c.order.Len() != 0 || c.currentSize != 0 {
		t.Fatalf("uninspectable entry entered cache ownership: has=%t entries=%d bytes=%d", c.Has(key), c.order.Len(), c.currentSize)
	}
	if removals != 0 {
		t.Fatalf("startup attempted %d removals for an uninspectable entry, want 0", removals)
	}
	got, err := os.ReadFile(filepath.Join(dir, key))
	if err != nil {
		t.Fatalf("uninspectable committed entry was not preserved: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("uninspectable entry body = %q, want %q", got, body)
	}
}

func TestStartupHardLimitKeepsDeterministicNewestEntriesAndMetersPruning(t *testing.T) {
	dir := t.TempDir()
	old := strings.Repeat("a", 64)
	tiedLow := strings.Repeat("b", 64)
	tiedMid := strings.Repeat("c", 64)
	tiedHigh := strings.Repeat("d", 64)
	base := time.Now().Add(-time.Hour)
	writeStartupScanEntry(t, dir, old, []byte("old"), base)
	for _, key := range []string{tiedLow, tiedMid, tiedHigh} {
		writeStartupScanEntry(t, dir, key, []byte(key[:1]), base.Add(time.Minute))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	removals := 0
	beforeAggregate := counterValue(t, cacheEvictionsTotal)
	beforeLabel := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry)
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       4,
		hardMaxEntries:   2,
		CapacityProvider: testStartupCapacityProvider,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &scriptedCacheDirectory{entries: entries, err: io.EOF}, nil
		},
		removeFile: func(path string) error {
			removals++
			return os.Remove(path)
		},
	})
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	// Equal persisted mtimes use the key as a stable secondary rank. The two
	// lexicographically greatest tied keys are therefore the exact survivors.
	if c.order.Len() != 2 || !c.Has(tiedMid) || !c.Has(tiedHigh) || c.Has(old) || c.Has(tiedLow) {
		t.Fatalf("hard-limit survivors old/low/mid/high = %t/%t/%t/%t, want false/false/true/true",
			c.Has(old), c.Has(tiedLow), c.Has(tiedMid), c.Has(tiedHigh))
	}
	if removals != 2 {
		t.Fatalf("hard-limit removal calls = %d, want 2", removals)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeAggregate; got != 2 {
		t.Fatalf("aggregate hard-limit evictions = %v, want 2", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseStartup, cacheEvictionReasonEntry) - beforeLabel; got != 2 {
		t.Fatalf("labeled hard-limit evictions = %v, want 2", got)
	}
	assertStartupFilesExist(t, dir, tiedMid, tiedHigh)
}

func TestStartupLateScanErrorDoesNotDeleteCommittedEntries(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)}
	for i, key := range keys {
		writeStartupScanEntry(t, dir, key, []byte("x"), time.Now().Add(time.Duration(i)*time.Second))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	lateErr := errors.New("forced late startup enumeration failure")
	removals := 0
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       1,
		hardMaxEntries:   2,
		CapacityProvider: testStartupCapacityProvider,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &scriptedCacheDirectory{entries: entries, err: lateErr}, nil
		},
		removeFile: func(path string) error {
			removals++
			return os.Remove(path)
		},
	})
	if c != nil {
		t.Fatal("NewDiskCache returned a partial cache after a late scan error")
	}
	if !errors.Is(err, lateErr) {
		t.Fatalf("NewDiskCache error = %v, want wrapped late scan error", err)
	}
	if removals != 0 {
		t.Fatalf("late scan error followed %d committed removals, want 0", removals)
	}
	assertStartupFilesExist(t, dir, keys...)
}

func TestStartupCancellationDoesNotDeleteCommittedEntries(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64)}
	for i, key := range keys {
		writeStartupScanEntry(t, dir, key, []byte("x"), time.Now().Add(time.Duration(i)*time.Second))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	removals := 0
	c, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       1,
		hardMaxEntries:   2,
		startupContext:   ctx,
		CapacityProvider: testStartupCapacityProvider,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &cancelAfterReadDirectory{entries: entries, cancel: cancel}, nil
		},
		removeFile: func(path string) error {
			removals++
			return os.Remove(path)
		},
	})
	if c != nil {
		t.Fatal("NewDiskCache returned a partial cache after startup cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewDiskCache error = %v, want context.Canceled", err)
	}
	if removals != 0 {
		t.Fatalf("startup cancellation followed %d committed removals, want 0", removals)
	}
	assertStartupFilesExist(t, dir, keys...)
}

func TestStartupCancellationAfterEnumerationStopsIndexBuild(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("7", 64), strings.Repeat("8", 64)}
	for i, key := range keys {
		writeStartupScanEntry(t, dir, key, []byte("x"), time.Now().Add(time.Duration(i)*time.Second))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       2,
		hardMaxEntries:   2,
		startupContext:   ctx,
		CapacityProvider: testStartupCapacityProvider,
		openScanDirectory: func(string) (cacheDirectory, error) {
			return &cancelOnCloseDirectory{entries: entries, cancel: cancel}, nil
		},
	})
	if cache != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("startup after post-enumeration cancellation: cache=%v err=%v, want nil/context.Canceled", cache, err)
	}
	assertStartupFilesExist(t, dir, keys...)
}

func TestStartupPreCanceledContextLeavesTemporarySentinelUntouched(t *testing.T) {
	dir := t.TempDir()
	tmpDir := filepath.Join(dir, tmpSubdir)
	if err := os.Mkdir(tmpDir, 0o750); err != nil {
		t.Fatalf("create cache temp directory: %v", err)
	}
	sentinel := filepath.Join(tmpDir, "interrupted-write")
	if err := os.WriteFile(sentinel, []byte("do not clean after cancellation"), 0o600); err != nil {
		t.Fatalf("write temporary sentinel: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache, err := NewDiskCache(dir, 80, DiskCacheOptions{
		startupContext: ctx,
	})
	if cache != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled startup: cache=%v err=%v, want nil/context.Canceled", cache, err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("pre-canceled startup cleaned temporary sentinel: %v", err)
	}
}

type cancelingTemporaryDirectory struct {
	cacheDirectory
	cancel context.CancelFunc
}

func (d *cancelingTemporaryDirectory) ReadDir(count int) ([]os.DirEntry, error) {
	entries, err := d.cacheDirectory.ReadDir(count)
	d.cancel()
	return entries, err
}

func TestTemporaryCleanupChecksCancellationBetweenDirectoryChunks(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), tmpSubdir)
	if err := os.Mkdir(tmpDir, 0o750); err != nil {
		t.Fatalf("create cache temp directory: %v", err)
	}
	sentinel := filepath.Join(tmpDir, "interrupted-write")
	if err := os.WriteFile(sentinel, []byte("keep after cancellation"), 0o600); err != nil {
		t.Fatalf("write temporary sentinel: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	open := func(path string) (cacheDirectory, error) {
		dir, err := openDirectory(path)
		if err != nil {
			return nil, err
		}
		return &cancelingTemporaryDirectory{cacheDirectory: dir, cancel: cancel}, nil
	}
	removed, err := removeTemporaryTreeWith(ctx, tmpDir, open)
	if removed != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled chunked cleanup = removed:%d err:%v, want 0/context.Canceled", removed, err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("chunk cancellation removed temporary sentinel: %v", err)
	}
}

func TestStartupLargeSparseDirectoryKeepsBoundedNewestSet(t *testing.T) {
	if testing.Short() {
		t.Skip("large sparse-directory restart coverage")
	}
	const (
		totalEntries = 5_000
		hardLimit    = 2_048
		logicalSize  = int64(1 << 20)
	)
	dir := t.TempDir()
	base := time.Now().Add(-24 * time.Hour)
	for i := range totalEntries {
		key := fmt.Sprintf("%064x", i)
		path := filepath.Join(dir, key)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatalf("create sparse entry %d: %v", i, err)
		}
		if err := file.Truncate(logicalSize); err != nil {
			_ = file.Close()
			t.Fatalf("truncate sparse entry %d: %v", i, err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close sparse entry %d: %v", i, err)
		}
		when := base.Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatalf("stamp sparse entry %d: %v", i, err)
		}
	}

	cache, err := NewDiskCache(dir, 80, DiskCacheOptions{
		MaxEntries:       hardLimit,
		hardMaxEntries:   hardLimit,
		CapacityProvider: testStartupCapacityProvider,
	})
	if err != nil {
		t.Fatalf("restart large sparse cache: %v", err)
	}
	if got := cacheEntryCount(cache); got != hardLimit {
		t.Fatalf("large sparse restart selected %d entries, want hard limit %d", got, hardLimit)
	}
	if cache.currentSize != int64(hardLimit)*logicalSize {
		t.Fatalf("large sparse selected bytes = %d, want %d", cache.currentSize, int64(hardLimit)*logicalSize)
	}
	oldestSurvivor := totalEntries - hardLimit
	if cache.Has(fmt.Sprintf("%064x", oldestSurvivor-1)) {
		t.Fatal("large sparse restart retained an entry below the newest hard-limit window")
	}
	if !cache.Has(fmt.Sprintf("%064x", oldestSurvivor)) || !cache.Has(fmt.Sprintf("%064x", totalEntries-1)) {
		t.Fatal("large sparse restart omitted expected newest survivors")
	}
}

func TestStartupTenMillionGuardrailEstimateFitsEightGiBEnvelope(t *testing.T) {
	// The final metric estimate includes the key, entry object, list node, and
	// map overhead. Startup adds only the bounded survivor pointer heap because
	// those same entry objects are transferred into the final LRU.
	peakEstimate := int64(cacheMetadataEntryLimit) * (int64(estimatedExactIndexBytesPerEntry) + int64(unsafe.Sizeof((*cacheEntry)(nil))))
	const memoryEnvelope = int64(8 << 30)
	if peakEstimate >= memoryEnvelope {
		t.Fatalf("10M startup metadata estimate = %d, must remain below 8 GiB", peakEstimate)
	}
	if size := unsafe.Sizeof(cacheEntry{}); size > 80 {
		t.Fatalf("cacheEntry grew to %d bytes; revisit the 10M startup memory envelope", size)
	}
}
