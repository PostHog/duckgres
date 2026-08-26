package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestConvergenceBlockedUnlinkDoesNotBlockCacheOrCanceledClose(t *testing.T) {
	victim := strings.Repeat("1", 64)
	recent := strings.Repeat("2", 64)
	cache, permits := newSoftConvergenceCache(t, 1, []string{victim, recent})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDelete := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseDelete)
	cache.removeFile = func(path string) error {
		if filepath.Base(path) != victim {
			return os.Remove(path)
		}
		started <- struct{}{}
		<-release
		return os.Remove(path)
	}

	beforeEvictions := counterValue(t, cacheEvictionsTotal)
	beforeByReason := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonEntry)
	allowConvergence(t, permits)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("convergence did not reach the blocked removal")
	}

	hasReturned := make(chan bool, 1)
	go func() { hasReturned <- cache.Has(recent) }()
	closeCtx, cancel := context.WithCancel(context.Background())
	cancel()
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- cache.Close(closeCtx) }()

	select {
	case hasRecent := <-hasReturned:
		if !hasRecent {
			t.Error("unrelated cache operation lost its resident entry during a blocked unlink")
		}
	case <-time.After(time.Second):
		t.Error("cache operation blocked behind convergence unlink")
	}
	select {
	case err := <-closeReturned:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Close with canceled context = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Error("Close with canceled context blocked behind convergence unlink")
	}

	releaseDelete()
	select {
	case <-cache.convergenceDone:
	case <-time.After(time.Second):
		t.Error("convergence worker did not finish after unlink release")
	}
	if cache.Has(victim) {
		t.Error("successful in-flight convergence deletion left the victim indexed")
	}
	if got := counterValue(t, cacheEvictionsTotal) - beforeEvictions; got != 1 {
		t.Errorf("successful blocked convergence evictions = %v, want 1", got)
	}
	if got := counterVecValue(t, cacheEvictionsByPhaseReasonTotal, cacheEvictionPhaseBackground, cacheEvictionReasonEntry) - beforeByReason; got != 1 {
		t.Errorf("successful blocked convergence entry evictions = %v, want 1", got)
	}
}

func TestConvergenceInFlightVictimAdmissionDoesNotRaceUnlink(t *testing.T) {
	victim := strings.Repeat("3", 64)
	recent := strings.Repeat("4", 64)
	cache, permits := newSoftConvergenceCache(t, 1, []string{victim, recent})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	removed := make(chan error, 1)
	var releaseOnce sync.Once
	releaseDelete := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseDelete)
	cache.removeFile = func(path string) error {
		if filepath.Base(path) != victim {
			return os.Remove(path)
		}
		started <- struct{}{}
		<-release
		err := os.Remove(path)
		removed <- err
		return err
	}

	allowConvergence(t, permits)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("convergence did not reach the blocked removal")
	}

	admission := make(chan error, 1)
	go func() {
		_, err := cache.PutStream(victim, strings.NewReader("replacement"))
		admission <- err
	}()
	var admissionErr error
	admissionReturned := false
	select {
	case admissionErr = <-admission:
		admissionReturned = true
	case <-time.After(time.Second):
		t.Error("replacement/new admission blocked behind in-flight victim unlink")
	}
	if admissionReturned {
		body, err := os.ReadFile(filepath.Join(cache.dir, victim))
		if err != nil {
			t.Errorf("in-flight admission changed victim path before unlink release: %v", err)
		} else if admissionErr == nil && string(body) != "replacement" {
			t.Errorf("accepted in-flight replacement body = %q, want replacement", body)
		} else if admissionErr != nil && string(body) != "x" {
			t.Errorf("rejected in-flight replacement changed body to %q, want original", body)
		}
	}

	releaseDelete()
	select {
	case err := <-removed:
		if err != nil {
			t.Fatalf("release blocked convergence unlink: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked convergence unlink did not finish")
	}
	if !admissionReturned {
		admissionErr = <-admission
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cache.Close(ctx); err != nil {
		t.Fatalf("stop convergence after unlink release: %v", err)
	}
	if admissionErr == nil {
		body, err := os.ReadFile(filepath.Join(cache.dir, victim))
		if err != nil || string(body) != "replacement" || !cache.Has(victim) {
			t.Fatalf("accepted replacement raced with old unlink: body=%q err=%v indexed=%t", body, err, cache.Has(victim))
		}
	}
}

func TestConvergenceHardFailureKeepsVictimIndexedAndRetryable(t *testing.T) {
	victim := strings.Repeat("5", 64)
	recent := strings.Repeat("6", 64)
	cache, permits := newSoftConvergenceCache(t, 1, []string{victim, recent})

	firstStarted := make(chan struct{}, 1)
	firstRelease := make(chan struct{})
	firstReturned := make(chan struct{}, 1)
	secondStarted := make(chan struct{}, 1)
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(firstRelease) }) }
	t.Cleanup(releaseFirst)
	forcedFailure := errors.New("forced convergence removal failure")
	attempt := 0
	cache.removeFile = func(path string) error {
		if filepath.Base(path) != victim {
			return errors.New("convergence selected an unexpected victim")
		}
		attempt++
		if attempt == 1 {
			firstStarted <- struct{}{}
			<-firstRelease
			firstReturned <- struct{}{}
			return forcedFailure
		}
		secondStarted <- struct{}{}
		return os.Remove(path)
	}

	beforeFailures := counterValue(t, cacheConvergenceEvictionFailuresTotal)
	allowConvergence(t, permits)
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("convergence did not reach the first blocked removal")
	}
	releaseFirst()
	select {
	case <-firstReturned:
	case <-time.After(time.Second):
		t.Fatal("blocked convergence removal did not return its forced failure")
	}
	if !cache.Has(victim) {
		t.Fatal("failed in-flight deletion removed the victim from the index")
	}
	if _, err := os.Stat(filepath.Join(cache.dir, victim)); err != nil {
		t.Fatalf("failed in-flight deletion removed the original body: %v", err)
	}

	// The second permit can be received only once the first convergence attempt
	// has finalized. Its successful retry proves a failed reservation was cleared.
	allowConvergence(t, permits)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("hard failure left the victim permanently in flight")
	}
	waitForCacheEntryCount(t, cache, 1)
	if cache.Has(victim) {
		t.Fatal("successful retry did not remove the formerly failed victim")
	}
	if got := counterValue(t, cacheConvergenceEvictionFailuresTotal) - beforeFailures; got != 1 {
		t.Fatalf("convergence failures = %v, want 1", got)
	}
}
