package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newSoftConvergenceCache creates more on-disk entries than the configured
// soft target, but holds convergence behind an explicit permit. The
// convergencePermits option is deliberately a test-only seam: production uses
// its bounded rate limiter instead.
func newSoftConvergenceCache(t *testing.T, softTarget int, keys []string) (*DiskCache, chan struct{}) {
	t.Helper()
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	for i, key := range keys {
		path := filepath.Join(dir, key)
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		when := base.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatal(err)
		}
	}

	permits := make(chan struct{})
	cache, err := NewDiskCache(dir, 100, DiskCacheOptions{
		MaxEntries:         softTarget,
		convergencePermits: permits,
	})
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = cache.Close(ctx)
	})
	return cache, permits
}

func cacheEntryCount(c *DiskCache) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func allowConvergence(t *testing.T, permits chan<- struct{}) {
	t.Helper()
	select {
	case permits <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for convergence worker to accept permit")
	}
}

func waitForCacheEntryCount(t *testing.T, c *DiskCache, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := cacheEntryCount(c); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cache entry count = %d, want %d", cacheEntryCount(c), want)
}

func TestStartupAboveSoftTargetDoesNotSynchronouslyDelete(t *testing.T) {
	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
	}
	cache, _ := newSoftConvergenceCache(t, 2, keys)

	// The configured 2-entry target is a convergence target, not a startup
	// pruning limit. All entries are below the independent 10M hard guardrail.
	if got := cacheEntryCount(cache); got != len(keys) {
		t.Fatalf("startup synchronously pruned to %d entries, want all %d", got, len(keys))
	}
	for _, key := range keys {
		if !cache.Has(key) {
			t.Fatalf("startup discarded %q despite no convergence permit", key)
		}
		if _, err := os.Stat(filepath.Join(cache.dir, key)); err != nil {
			t.Fatalf("startup removed committed body %q: %v", key, err)
		}
	}
}

func TestSoftConvergenceDeletesExactlyOneLRUPerPermit(t *testing.T) {
	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
		strings.Repeat("4", 64),
	}
	cache, permits := newSoftConvergenceCache(t, 2, keys)
	if got := cacheEntryCount(cache); got != 4 {
		t.Fatalf("startup count = %d, want 4 before convergence", got)
	}

	allowConvergence(t, permits)
	waitForCacheEntryCount(t, cache, 3)
	if cache.Has(keys[0]) || !cache.Has(keys[1]) || !cache.Has(keys[2]) || !cache.Has(keys[3]) {
		t.Fatal("first convergence permit did not remove exactly the oldest entry")
	}

	allowConvergence(t, permits)
	waitForCacheEntryCount(t, cache, 2)
	if cache.Has(keys[1]) || !cache.Has(keys[2]) || !cache.Has(keys[3]) {
		t.Fatal("second convergence permit did not remove exactly the next LRU entry")
	}
}

func TestAdmissionAtOrAboveSoftTargetIsNetNeutral(t *testing.T) {
	t.Run("at target swaps only the LRU", func(t *testing.T) {
		oldest, recent, incoming := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)
		cache, _ := newSoftConvergenceCache(t, 2, []string{oldest, recent})

		if _, err := cache.PutStream(incoming, bytes.NewReader([]byte("new"))); err != nil {
			t.Fatalf("PutStream: %v", err)
		}
		if got := cacheEntryCount(cache); got != 2 {
			t.Fatalf("admission at soft target changed count to %d, want 2", got)
		}
		if cache.Has(oldest) || !cache.Has(recent) || !cache.Has(incoming) {
			t.Fatal("admission at target did not perform one LRU swap")
		}
	})

	t.Run("above target swaps at most one and replacement is count neutral", func(t *testing.T) {
		oldest, retained, replaced := strings.Repeat("4", 64), strings.Repeat("5", 64), strings.Repeat("6", 64)
		incoming := strings.Repeat("7", 64)
		cache, _ := newSoftConvergenceCache(t, 2, []string{oldest, retained, replaced})

		if _, err := cache.PutStream(incoming, bytes.NewReader([]byte("new"))); err != nil {
			t.Fatalf("PutStream new entry: %v", err)
		}
		if got := cacheEntryCount(cache); got != 3 {
			t.Fatalf("admission above soft target changed count to %d, want net-neutral 3", got)
		}
		if cache.Has(oldest) || !cache.Has(retained) || !cache.Has(replaced) || !cache.Has(incoming) {
			t.Fatal("admission above target removed more than the one LRU victim")
		}

		if _, err := cache.PutStream(replaced, bytes.NewReader([]byte("replacement"))); err != nil {
			t.Fatalf("PutStream replacement: %v", err)
		}
		if got := cacheEntryCount(cache); got != 3 {
			t.Fatalf("replacement above soft target changed count to %d, want 3", got)
		}
		if !cache.Has(retained) || !cache.Has(replaced) || !cache.Has(incoming) {
			t.Fatal("replacement above target evicted an unrelated entry")
		}
	})
}

func TestAdmissionStillEnforcesHardByteCapacityAcrossSmallVictims(t *testing.T) {
	cache := newTestCache(t)
	cache.maxEntries = 100
	cache.maxBytes = 100
	for _, key := range []string{strings.Repeat("8", 64), strings.Repeat("9", 64), strings.Repeat("a", 64)} {
		if _, err := cache.PutStream(key, bytes.NewReader(make([]byte, 5))); err != nil {
			t.Fatalf("seed small cache entry: %v", err)
		}
	}
	if _, err := cache.PutStream(strings.Repeat("b", 64), bytes.NewReader(make([]byte, 95))); err != nil {
		t.Fatalf("admit normal cache entry: %v", err)
	}
	if cache.currentSize > cache.maxBytes {
		t.Fatalf("accepted cache entry left bytes above capacity: %d > %d", cache.currentSize, cache.maxBytes)
	}
}
