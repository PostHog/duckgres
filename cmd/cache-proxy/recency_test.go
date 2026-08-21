package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests deliberately describe the durable-recency contract before its
// implementation exists. The package-private DiskCacheOptions seams keep the
// asynchronous filesystem work deterministic: no test depends on wall-clock
// sleeps or the host filesystem's mtime precision.
type recencyTestHooks struct {
	now      func() time.Time
	chtimes  func(string, time.Time, time.Time) error
	interval time.Duration
	queueCap int
	workers  int
}

func newRecencyTestCache(t *testing.T, dir string, maxEntries int, hooks recencyTestHooks) *DiskCache {
	t.Helper()
	if hooks.now == nil {
		hooks.now = time.Now
	}
	if hooks.chtimes == nil {
		hooks.chtimes = os.Chtimes
	}
	if hooks.interval == 0 {
		hooks.interval = time.Minute
	}
	if hooks.queueCap == 0 {
		hooks.queueCap = 8
	}
	if hooks.workers == 0 {
		hooks.workers = 1
	}

	c, err := NewDiskCache(dir, 100, DiskCacheOptions{
		MaxEntries:           maxEntries,
		DurableRecency:       true,
		recencyNow:           hooks.now,
		recencyChtimes:       hooks.chtimes,
		recencyInterval:      hooks.interval,
		recencyQueueCapacity: hooks.queueCap,
		recencyWorkerCount:   hooks.workers,
	})
	if err != nil {
		t.Fatalf("NewDiskCache with durable recency: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c
}

func waitForRecencyIdle(t *testing.T, c *DiskCache) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.waitForRecencyIdle(ctx); err != nil {
		t.Fatalf("wait for durable recency writer: %v", err)
	}
}

func closeRecencyCache(t *testing.T, c *DiskCache) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.Close(ctx)
}

func seedRecencyFile(t *testing.T, dir, key string, body []byte, mtime time.Time) {
	t.Helper()
	path := filepath.Join(dir, key)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatalf("seed cache file %s: %v", key, err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("set seed cache mtime %s: %v", key, err)
	}
}

func openRecencyEntry(t *testing.T, c *DiskCache, key string) {
	t.Helper()
	r, _, ok := c.Open(key)
	if !ok {
		t.Fatalf("Open(%s) = miss", key)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close cached reader: %v", err)
	}
}

func TestDurableRecencyRestartPrefersTouchedOlderEntry(t *testing.T) {
	dir := t.TempDir()
	oldKey := strings.Repeat("a", 64)
	newKey := strings.Repeat("b", 64)
	base := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	seedRecencyFile(t, dir, oldKey, []byte("old"), base.Add(-2*time.Hour))
	seedRecencyFile(t, dir, newKey, []byte("new"), base.Add(-time.Hour))

	now := base
	c := newRecencyTestCache(t, dir, 2, recencyTestHooks{
		now:     func() time.Time { return now },
		chtimes: os.Chtimes,
	})
	// The initial scan must not itself race the test's deliberately ordered
	// seed mtimes. Drain any constructor/commit bookkeeping before touching.
	waitForRecencyIdle(t, c)

	// Move to a later coarse bucket. A real local read, not just a synthetic
	// index mutation, must be the event persisted across restart.
	now = base.Add(2 * time.Hour)
	beforeAttempts := counterValue(t, cacheRecencyTouchAttemptsTotal)
	beforeSuccesses := counterValue(t, cacheRecencyTouchSuccessesTotal)
	openRecencyEntry(t, c, oldKey)
	waitForRecencyIdle(t, c)
	if got := counterValue(t, cacheRecencyTouchAttemptsTotal) - beforeAttempts; got != 1 {
		t.Fatalf("durable recency attempts = %v, want 1", got)
	}
	if got := counterValue(t, cacheRecencyTouchSuccessesTotal) - beforeSuccesses; got != 1 {
		t.Fatalf("durable recency successes = %v, want 1", got)
	}
	if err := closeRecencyCache(t, c); err != nil {
		t.Fatalf("close first cache: %v", err)
	}

	// PR2's configured entry target is soft and must not itself force a startup
	// purge. Use the private hard safety seam to require survivor selection;
	// the formerly old file must win because its asynchronous read touch became
	// durable mtime.
	restarted, err := NewDiskCache(dir, 100, DiskCacheOptions{
		MaxEntries:     2,
		hardMaxEntries: 1,
	})
	if err != nil {
		t.Fatalf("restart cache: %v", err)
	}
	if !restarted.Has(oldKey) || restarted.Has(newKey) {
		t.Fatalf("restart survivors old/new = %t/%t, want true/false", restarted.Has(oldKey), restarted.Has(newKey))
	}
}

func TestDurableRecencyTouchUpdatesLRUAndDropsWhenQueueIsBounded(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)}
	for _, key := range keys {
		seedRecencyFile(t, dir, key, []byte(key[:1]), time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC))
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	calls := 0
	c := newRecencyTestCache(t, dir, len(keys), recencyTestHooks{
		now:      func() time.Time { return time.Date(2026, time.August, 21, 13, 0, 0, 0, time.UTC) },
		queueCap: 1,
		workers:  1,
		chtimes: func(path string, atime, mtime time.Time) error {
			calls++
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return os.Chtimes(path, atime, mtime)
		},
	})
	beforeDrops := counterValue(t, cacheRecencyTouchDroppedTotal)

	// The first work item is in flight; the second consumes the sole queue
	// slot. The third read must still update LRU immediately, but its durable
	// write is dropped rather than making a request wait behind metadata I/O.
	openRecencyEntry(t, c, keys[0])
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("durable recency writer did not start")
	}
	openRecencyEntry(t, c, keys[1])
	thirdReturned := make(chan error, 1)
	go func() {
		r, _, ok := c.Open(keys[2])
		if !ok {
			thirdReturned <- errors.New("third cache hit became a miss")
			return
		}
		thirdReturned <- r.Close()
	}()
	select {
	case err := <-thirdReturned:
		if err != nil {
			t.Fatalf("third cache hit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cache hit waited for a full durable-recency queue")
	}

	if got := c.order.Back().Value.(*cacheEntry).key; got != keys[2] {
		t.Fatalf("synchronous LRU tail = %s, want %s", got, keys[2])
	}
	if got := counterValue(t, cacheRecencyTouchDroppedTotal) - beforeDrops; got != 1 {
		t.Fatalf("recency queue drops = %v, want 1", got)
	}
	if got := gaugeValue(t, cacheRecencyQueueDepth); got > 2 {
		t.Fatalf("bounded recency outstanding depth = %v, want at most queue+worker (2)", got)
	}

	close(release)
	waitForRecencyIdle(t, c)
	if calls != 2 {
		t.Fatalf("metadata writes after one queue overflow = %d, want 2", calls)
	}
}

func TestDurableRecencyCoalescesRepeatedTouchesInOneCoarseBucket(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("c", 64)
	seedRecencyFile(t, dir, key, []byte("body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))

	now := time.Date(2026, time.August, 21, 12, 3, 0, 0, time.UTC)
	writes := 0
	c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
		now: func() time.Time { return now },
		chtimes: func(path string, atime, mtime time.Time) error {
			writes++
			return os.Chtimes(path, atime, mtime)
		},
	})
	beforeCoalesced := counterValue(t, cacheRecencyTouchCoalescedTotal)

	for range 5 {
		openRecencyEntry(t, c, key)
	}
	waitForRecencyIdle(t, c)
	if writes != 1 {
		t.Fatalf("same-bucket durable mtime writes = %d, want 1", writes)
	}
	if got := counterValue(t, cacheRecencyTouchCoalescedTotal) - beforeCoalesced; got < 4 {
		t.Fatalf("same-bucket coalesced touches = %v, want at least 4", got)
	}

	// The next coarse bucket schedules exactly one fresh write.
	now = now.Add(time.Minute)
	openRecencyEntry(t, c, key)
	waitForRecencyIdle(t, c)
	if writes != 2 {
		t.Fatalf("next-bucket durable mtime writes = %d, want 2", writes)
	}
}

func TestDurableRecencyDroppedKeyRetriesAtMostOncePerBucket(t *testing.T) {
	dir := t.TempDir()
	keys := []string{strings.Repeat("7", 64), strings.Repeat("8", 64), strings.Repeat("9", 64)}
	for _, key := range keys {
		seedRecencyFile(t, dir, key, []byte("x"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := newRecencyTestCache(t, dir, len(keys), recencyTestHooks{
		now:      func() time.Time { return time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC) },
		queueCap: 1,
		workers:  1,
		chtimes: func(path string, atime, mtime time.Time) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return os.Chtimes(path, atime, mtime)
		},
	})
	openRecencyEntry(t, c, keys[0])
	<-started
	openRecencyEntry(t, c, keys[1])
	beforeDrops := counterValue(t, cacheRecencyTouchDroppedTotal)
	for range 5 {
		openRecencyEntry(t, c, keys[2])
	}
	if got := counterValue(t, cacheRecencyTouchDroppedTotal) - beforeDrops; got != 1 {
		t.Fatalf("same-bucket drops for one overflowed key = %v, want 1", got)
	}
	close(release)
	waitForRecencyIdle(t, c)
}

func TestDurableRecencyPersistenceFailureDoesNotAffectServingOrIndex(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	body := []byte("still-readable")
	seedRecencyFile(t, dir, key, body, time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))
	failure := errors.New("forced chtimes failure")
	c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
		now:     func() time.Time { return time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC) },
		chtimes: func(string, time.Time, time.Time) error { return failure },
	})
	beforeFailures := counterValue(t, cacheRecencyTouchFailuresTotal)
	beforeEvictions := counterValue(t, cacheEvictionsTotal)

	r, size, ok := c.Open(key)
	if !ok || size != int64(len(body)) {
		t.Fatalf("Open after scheduled persistence failure = ok:%t size:%d", ok, size)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("cache body after persistence failure = %q, %v", got, err)
	}
	waitForRecencyIdle(t, c)

	if !c.Has(key) || c.order.Len() != 1 || c.currentSize != int64(len(body)) {
		t.Fatalf("cache state changed by persistence failure: has=%t entries=%d bytes=%d", c.Has(key), c.order.Len(), c.currentSize)
	}
	if got := counterValue(t, cacheRecencyTouchFailuresTotal) - beforeFailures; got != 1 {
		t.Fatalf("recency persistence failures = %v, want 1", got)
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeEvictions; got != 0 {
		t.Fatalf("evictions caused by recency persistence failure = %v, want 0", got)
	}
}

func TestDurableRecencyStaleQueuedTouchCannotRegressReplacementMtime(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("e", 64)
	seedRecencyFile(t, dir, key, []byte("old-body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))

	oldTouch := time.Date(2026, time.August, 21, 12, 1, 0, 0, time.UTC)
	newCommit := oldTouch.Add(2 * time.Minute)
	now := oldTouch
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	first := true
	c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
		now: func() time.Time { return now },
		chtimes: func(path string, atime, mtime time.Time) error {
			if first {
				first = false
				started <- struct{}{}
				<-release
			}
			return os.Chtimes(path, atime, mtime)
		},
	})

	openRecencyEntry(t, c, key)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old durable touch did not reach the writer")
	}

	// Commit a replacement while the old mtime write is blocked. The final
	// durable timestamp must describe the replacement/current access, not the
	// stale queued touch that runs after its rename.
	now = newCommit
	if _, err := c.PutStream(key, strings.NewReader("new-body")); err != nil {
		t.Fatalf("replace cached body: %v", err)
	}
	close(release)
	waitForRecencyIdle(t, c)

	info, err := os.Stat(filepath.Join(dir, key))
	if err != nil {
		t.Fatalf("stat replacement: %v", err)
	}
	if info.ModTime().Before(newCommit) {
		t.Fatalf("replacement mtime regressed to %s, want at least %s", info.ModTime(), newCommit)
	}
	r, _, ok := c.Open(key)
	if !ok {
		t.Fatal("replacement disappeared from index")
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(got) != "new-body" {
		t.Fatalf("replacement body = %q, %v", got, err)
	}
}

func TestDurableRecencyCloseRejectsReplacementBehindQueuedTouch(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("f", 64)
	seedRecencyFile(t, dir, key, []byte("old-body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))

	oldTouch := time.Date(2026, time.August, 21, 12, 1, 0, 0, time.UTC)
	replacementTime := oldTouch.Add(2 * time.Minute)
	now := oldTouch
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
		now: func() time.Time { return now },
		chtimes: func(path string, atime, mtime time.Time) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return os.Chtimes(path, atime, mtime)
		},
	})

	// Keep an accepted old-bucket touch in flight, then close writer admission
	// with a canceled deadline. Close is terminal for mutations, so a replacement
	// cannot race the draining old metadata write and inherit its timestamp.
	openRecencyEntry(t, c, key)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old durable touch did not reach the writer")
	}
	closeCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(closeCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close with canceled context = %v, want context.Canceled", err)
	}

	now = replacementTime
	if _, err := c.PutStream(key, strings.NewReader("new-body")); err == nil {
		t.Fatal("replacement succeeded after Close began")
	}
	close(release)
	if err := closeRecencyCache(t, c); err != nil {
		t.Fatalf("drain closed writer: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, key))
	if err != nil {
		t.Fatalf("stat replacement: %v", err)
	}
	if info.ModTime().Before(oldTouch.Truncate(time.Minute)) {
		t.Fatalf("drained touch mtime = %s, want at least %s", info.ModTime(), oldTouch.Truncate(time.Minute))
	}
	r, _, ok := c.Open(key)
	if !ok {
		t.Fatal("original entry disappeared after rejected replacement")
	}
	body, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(body) != "old-body" {
		t.Fatalf("body after rejected replacement = %q, %v", body, err)
	}
}

func TestDurableRecencyCloseDrainsAndHonorsCanceledDeadline(t *testing.T) {
	t.Run("drains accepted work and ignores later touches", func(t *testing.T) {
		dir := t.TempDir()
		key := strings.Repeat("f", 64)
		seedRecencyFile(t, dir, key, []byte("body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		writes := 0
		c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
			now: func() time.Time { return time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC) },
			chtimes: func(path string, atime, mtime time.Time) error {
				writes++
				started <- struct{}{}
				<-release
				return os.Chtimes(path, atime, mtime)
			},
		})
		openRecencyEntry(t, c, key)
		<-started

		closed := make(chan error, 1)
		go func() { closed <- closeRecencyCache(t, c) }()
		select {
		case err := <-closed:
			t.Fatalf("Close returned before accepted work drained: %v", err)
		default:
		}
		close(release)
		if err := <-closed; err != nil {
			t.Fatalf("Close after releasing durable write: %v", err)
		}
		if writes != 1 {
			t.Fatalf("drained durable writes = %d, want 1", writes)
		}

		// Closing the writer must only make durability best-effort; runtime LRU
		// touches stay safe and must never send to a closed work channel.
		openRecencyEntry(t, c, key)
		if writes != 1 {
			t.Fatalf("post-close touch scheduled a durable write: %d", writes)
		}
	})

	t.Run("honors a canceled close context", func(t *testing.T) {
		dir := t.TempDir()
		key := strings.Repeat("0", 64)
		seedRecencyFile(t, dir, key, []byte("body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
		writes := 0
		c := newRecencyTestCache(t, dir, 1, recencyTestHooks{
			now: func() time.Time { return now },
			chtimes: func(path string, atime, mtime time.Time) error {
				writes++
				started <- struct{}{}
				<-release
				return os.Chtimes(path, atime, mtime)
			},
		})
		openRecencyEntry(t, c, key)
		<-started

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close with canceled context = %v, want context.Canceled", err)
		}
		// A newer same-key touch after the close boundary still updates in-memory
		// LRU, but must not extend the set of durable work being drained.
		now = now.Add(time.Minute)
		openRecencyEntry(t, c, key)
		close(release)
		if err := closeRecencyCache(t, c); err != nil {
			t.Fatalf("second Close after worker release: %v", err)
		}
		if writes != 1 {
			t.Fatalf("durable writes admitted after Close = %d, want 1 pre-Close write", writes)
		}
	})

	t.Run("closes recency admission even when convergence wait is canceled", func(t *testing.T) {
		dir := t.TempDir()
		key := strings.Repeat("1", 64)
		seedRecencyFile(t, dir, key, []byte("body"), time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC))
		permits := make(chan struct{})
		c, err := NewDiskCache(dir, 100, DiskCacheOptions{
			MaxEntries:         1,
			DurableRecency:     true,
			convergencePermits: permits,
		})
		if err != nil {
			t.Fatalf("NewDiskCache: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close with canceled context = %v, want context.Canceled", err)
		}
		c.recency.mu.Lock()
		closed := c.recency.closed
		c.recency.mu.Unlock()
		if !closed {
			t.Fatal("recency writer still accepted work after Close canceled its convergence wait")
		}
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})
}
