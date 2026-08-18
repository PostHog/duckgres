package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheSummaryContainsSnapshotKeysButNotRawKeys(t *testing.T) {
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	s, err := newCacheSummary("peer-a", 1, keys, time.Now(), defaultSummaryTTL)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !s.Contains(key) {
			t.Fatalf("summary omitted source key %q", key)
		}
	}
	body, err := s.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range append(keys, "https://bucket.example/object?X-Amz-Signature=secret") {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatalf("wire summary leaked raw cache locator %q", secret)
		}
	}
}

func TestReceiveSummaryRejectsInvalidWithoutReplacingLastValid(t *testing.T) {
	pm := peerManagerWith([]string{"peer-a:8081"})
	now := time.Now()
	valid, err := newCacheSummary("peer-a:8081", 1, []string{strings.Repeat("a", 64)}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := valid.MarshalBinary()
	if err := pm.ReceiveSummary("peer-a:8081", body, now); err != nil {
		t.Fatalf("receive valid summary: %v", err)
	}
	bad := append([]byte(nil), body...)
	bad[0] ^= 0xff
	if err := pm.ReceiveSummary("peer-a:8081", bad, now); err == nil {
		t.Fatal("malformed summary accepted")
	}
	if got := pm.summaryCount(); got != 1 {
		t.Fatalf("invalid summary replaced last valid one; count=%d", got)
	}
}

func TestSummaryReplacementExpiryAndMembershipCleanup(t *testing.T) {
	peer := "peer-a:8081"
	pm := peerManagerWith([]string{peer})
	now := time.Now()
	old, _ := newCacheSummary("node-a", 1, []string{strings.Repeat("a", 64)}, now, time.Minute)
	oldBody, _ := old.MarshalBinary()
	if err := pm.ReceiveSummary(peer, oldBody, now); err != nil {
		t.Fatal(err)
	}
	newer, _ := newCacheSummary("node-a", 2, []string{strings.Repeat("b", 64)}, now, time.Minute)
	newerBody, _ := newer.MarshalBinary()
	if err := pm.ReceiveSummary(peer, newerBody, now); err != nil {
		t.Fatal(err)
	}
	if got := pm.SummaryCandidates(strings.Repeat("a", 64), now); len(got) != 0 {
		t.Fatalf("old generation remained selectable: %v", got)
	}
	if got := pm.SummaryCandidates(strings.Repeat("b", 64), now); len(got) != 1 {
		t.Fatalf("new generation was not installed: %v", got)
	}
	pm.summaries.removeNonMembers(pm.isMember, now.Add(2*time.Minute))
	if pm.summaryCount() != 0 {
		t.Fatal("expired summary remained resident")
	}
	if err := pm.ReceiveSummary(peer, newerBody, now); err != nil {
		t.Fatal(err)
	}
	pm.mu.Lock()
	pm.peers = nil
	pm.mu.Unlock()
	pm.summaries.removeNonMembers(pm.isMember, now)
	if pm.summaryCount() != 0 {
		t.Fatal("departed peer summary remained resident")
	}
}

func TestSummaryReceiptRevalidatesSelectionAfterCommit(t *testing.T) {
	peer := "peer-a:8081"
	s, err := newCacheSummary("peer-a", 1, nil, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, err := s.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	pm := NewPeerManager("test.svc", ":8081")
	pm.ConfigureSummary(peerLookupSummary, "receiver")
	checks := 0
	eligible := func(string) bool {
		checks++
		return checks == 1
	}
	if err := pm.receiveSummary(peer, body, "", time.Now(), eligible); err == nil {
		t.Fatal("receipt succeeded after the sender became deselected during commit")
	}
	if pm.summaryCount() != 0 {
		t.Fatal("deselected peer was reinserted after membership cleanup")
	}
}

func TestParsePeerLookupMode(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want peerLookupMode
		ok   bool
	}{
		{"", peerLookupProbe, true}, {"probe", peerLookupProbe, true}, {"summary", peerLookupSummary, true}, {"typo", "", false},
	} {
		got, err := parsePeerLookupMode(tt.in)
		if (err == nil) != tt.ok || got != tt.want {
			t.Fatalf("parse %q = %q, %v", tt.in, got, err)
		}
	}
}

func TestPositiveEnvIntRejectsInvalidAndOverflowingValues(t *testing.T) {
	t.Setenv("TEST_POSITIVE_INT", "")
	if got, err := positiveEnvInt("TEST_POSITIVE_INT", 7); err != nil || got != 7 {
		t.Fatalf("unset setting = %d, %v; want default 7", got, err)
	}

	for _, value := range []string{"0", "-1", "not-a-number", "999999999999999999999999999999"} {
		t.Setenv("TEST_POSITIVE_INT", value)
		if _, err := positiveEnvInt("TEST_POSITIVE_INT", 7); err == nil {
			t.Fatalf("value %q unexpectedly accepted", value)
		}
	}

	t.Setenv("TEST_POSITIVE_INT", "11")
	if got, err := positiveEnvInt("TEST_POSITIVE_INT", 7); err != nil || got != 11 {
		t.Fatalf("valid setting = %d, %v; want 11", got, err)
	}
}

func TestValidateSummaryMemoryLimit(t *testing.T) {
	minimum := summaryMemoryReserveBytes()
	if err := validateSummaryMemoryLimit(minimum); err == nil {
		t.Fatal("memory limit equal to the fixed reserve was accepted")
	}
	if err := validateSummaryMemoryLimit(minimum + int64(summaryBloomBits/8)); err != nil {
		t.Fatalf("memory limit with room for one peer summary rejected: %v", err)
	}
}

func TestSummaryPullCycleHasTotalDeadline(t *testing.T) {
	release := make(chan struct{})
	peers := make([]string, 0, 12)
	for range 12 {
		remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
				w.WriteHeader(http.StatusServiceUnavailable)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(remote.Close)
		peers = append(peers, strings.TrimPrefix(remote.URL, "http://"))
	}

	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver")
	pm.summaryPullCycleTimeout = 50 * time.Millisecond
	started := time.Now()
	pm.pullSummaries(context.Background(), peers)
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		close(release)
		t.Fatalf("bounded pull cycle took %s", elapsed)
	}
	close(release)
}

func TestSummaryCycleCadenceIncludesPullWork(t *testing.T) {
	started := time.Unix(100, 0)
	if got := remainingSummaryCycleDelay(started, started.Add(15*time.Second), 20*time.Second); got != 5*time.Second {
		t.Fatalf("remaining delay=%s, want 5s after 15s of a 20s cycle", got)
	}
	if got := remainingSummaryCycleDelay(started, started.Add(25*time.Second), 20*time.Second); got != 0 {
		t.Fatalf("overdue cycle delay=%s, want immediate", got)
	}
}

func TestSummaryCandidatesAreComputedLocallyWithoutPeerRPC(t *testing.T) {
	key := strings.Repeat("c", 64)
	var hasCalls, getCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cache/has":
			hasCalls++
			w.WriteHeader(http.StatusOK)
		case "/cache/get":
			getCalls++
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	pm := peerManagerWith([]string{addr})
	pm.lookupMode = peerLookupSummary
	s, err := newCacheSummary(addr, 1, []string{key}, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(addr, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	candidates := pm.SummaryCandidates(key, time.Now())
	if len(candidates) != 1 {
		t.Fatalf("candidates=%v, want one", candidates)
	}
	if hasCalls != 0 || getCalls != 0 {
		t.Fatalf("has=%d get=%d, want no RPC during local Bloom lookup", hasCalls, getCalls)
	}
}

func TestFetchDedupSummaryHitPreservesPeerSourceAfterConfirmation(t *testing.T) {
	key := strings.Repeat("d", 64)
	data := []byte("peer body")
	var hasCalls, getCalls, originCalls int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cache/has":
			atomic.AddInt32(&hasCalls, 1)
		case "/cache/get":
			atomic.AddInt32(&getCalls, 1)
			_, _ = w.Write(data)
		}
	}))
	defer peer.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	pm := peerManagerWith([]string{addr})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("node-a", 1, []string{key}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(addr, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(newTestCache(t), pm, nil)
	p.client = origin.Client()
	r := httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil)
	before := counterValue(t, peerFetchesTotal)
	got, err := p.fetchDedup(key, r, "")
	if err != nil || got.source != "peer" || got.size != int64(len(data)) {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
	if hasCalls != 1 || getCalls != 1 || originCalls != 0 {
		t.Fatalf("has=%d get=%d origin=%d", hasCalls, getCalls, originCalls)
	}
	if delta := counterValue(t, peerFetchesTotal) - before; delta != 1 {
		t.Fatalf("logical peer lookups=%v, want 1", delta)
	}
}

func TestSummaryLookupReportsOnlyUncoveredPeersForWarmupProbes(t *testing.T) {
	covered, uncovered := "covered:8081", "uncovered:8081"
	pm := peerManagerWith([]string{covered, uncovered})
	key := strings.Repeat("e", 64)
	s, err := newCacheSummary("node-covered", 1, []string{strings.Repeat("f", 64)}, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(covered, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	positive, missing := pm.SummaryLookup(key, time.Now())
	if len(positive) != 0 {
		t.Fatalf("positive peers = %v, want none", positive)
	}
	if len(missing) != 1 || missing[0] != uncovered {
		t.Fatalf("uncovered peers = %v, want %q", missing, uncovered)
	}
}

func TestSummaryRejectsBloomParametersNotDerivedFromItemCount(t *testing.T) {
	s, err := newCacheSummary("peer-a", 1, []string{strings.Repeat("a", 64)}, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	s.MBits += 8
	s.Bits = append(s.Bits, 0)
	body, _ := s.MarshalBinary()
	if _, err := parseCacheSummary(body, time.Now()); err == nil {
		t.Fatal("accepted a Bloom filter larger than the declared item count requires")
	}
}

func TestSummaryFetchDoesNotCancelHealthyBodyAfterProbeTimeout(t *testing.T) {
	key := strings.Repeat("9", 64)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cache/has" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/cache/get" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("first"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(peerHasTimeout + 100*time.Millisecond)
		_, _ = w.Write([]byte("second"))
	}))
	defer peer.Close()
	addr := strings.TrimPrefix(peer.URL, "http://")
	pm := peerManagerWith([]string{addr})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("node-a", 1, []string{key}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(addr, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(newTestCache(t), pm, nil)
	r := httptest.NewRequest(http.MethodGet, "http://origin.test/object", nil)
	got, err := p.fetchDedup(key, r, "")
	if err != nil || got.source != "peer" || got.size != int64(len("firstsecond")) {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
}

func TestSummaryWarmupProbesOnlyPeersWithoutSummaries(t *testing.T) {
	key, other := strings.Repeat("1", 64), strings.Repeat("2", 64)
	var coveredHas, coveredGet, uncoveredHas, uncoveredGet, originCalls int32
	covered := newPeerServer(t, other, []byte("other"), http.StatusOK, &coveredHas, &coveredGet)
	uncovered := newPeerServer(t, key, []byte("peer"), http.StatusOK, &uncoveredHas, &uncoveredGet)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	pm := peerManagerWith([]string{covered, uncovered})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("covered", 1, []string{other}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(covered, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(newTestCache(t), pm, nil)
	p.client = origin.Client()
	r := httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil)
	got, err := p.fetchDedup(key, r, "")
	if err != nil || got.source != "peer" {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
	if coveredHas != 0 || coveredGet != 0 || uncoveredHas != 1 || uncoveredGet != 1 || originCalls != 0 {
		t.Fatalf("covered has/get=%d/%d, uncovered has/get=%d/%d, origin=%d", coveredHas, coveredGet, uncoveredHas, uncoveredGet, originCalls)
	}
}

func TestFullyCoveredSummaryNegativeGoesDirectlyToOrigin(t *testing.T) {
	key, other := strings.Repeat("3", 64), strings.Repeat("4", 64)
	var hasCalls, getCalls, originCalls int32
	peer := newPeerServer(t, other, []byte("other"), http.StatusOK, &hasCalls, &getCalls)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	pm := peerManagerWith([]string{peer})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("peer", 1, []string{other}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(peer, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := NewCacheProxy(newTestCache(t), pm, nil)
	p.client = origin.Client()
	r := httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil)
	got, err := p.fetchDedup(key, r, "")
	if err != nil || got.source != "miss" {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
	if hasCalls != 0 || getCalls != 0 || originCalls != 1 {
		t.Fatalf("has=%d get=%d origin=%d", hasCalls, getCalls, originCalls)
	}
}

func TestJoiningPeerIsProbedUntilItsSummaryArrives(t *testing.T) {
	key, other := strings.Repeat("5", 64), strings.Repeat("6", 64)
	var coveredHas, coveredGet, joiningHas, joiningGet, originCalls int32
	covered := newPeerServer(t, other, []byte("other"), http.StatusOK, &coveredHas, &coveredGet)
	joining := newPeerServer(t, key, []byte("joining-peer"), http.StatusOK, &joiningHas, &joiningGet)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	pm := peerManagerWith([]string{covered})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("covered", 1, []string{other}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(covered, body, time.Now()); err != nil {
		t.Fatal(err)
	}

	// DNS membership changes before this proxy has received the new peer's
	// summary. That peer is intentionally the only one left on probe fallback.
	pm.mu.Lock()
	pm.peers = append(pm.peers, joining)
	pm.mu.Unlock()
	p := NewCacheProxy(newTestCache(t), pm, nil)
	p.client = origin.Client()
	got, err := p.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "peer" {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
	if coveredHas != 0 || coveredGet != 0 || joiningHas != 1 || joiningGet != 1 || originCalls != 0 {
		t.Fatalf("covered has/get=%d/%d joining has/get=%d/%d origin=%d", coveredHas, coveredGet, joiningHas, joiningGet, originCalls)
	}
}

func TestDepartedPeerIsNeverSelectedFromAStaleSummary(t *testing.T) {
	key := strings.Repeat("7", 64)
	var hasCalls, getCalls, originCalls int32
	peer := newPeerServer(t, key, []byte("peer"), http.StatusOK, &hasCalls, &getCalls)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	pm := peerManagerWith([]string{peer})
	pm.lookupMode = peerLookupSummary
	s, _ := newCacheSummary("departed", 1, []string{key}, time.Now(), time.Minute)
	body, _ := s.MarshalBinary()
	if err := pm.ReceiveSummary(peer, body, time.Now()); err != nil {
		t.Fatal(err)
	}
	pm.mu.Lock()
	pm.peers = nil
	pm.mu.Unlock()
	p := NewCacheProxy(newTestCache(t), pm, nil)
	p.client = origin.Client()
	got, err := p.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "miss" {
		t.Fatalf("fetch=%+v err=%v", got, err)
	}
	if hasCalls != 0 || getCalls != 0 || originCalls != 1 {
		t.Fatalf("has=%d get=%d origin=%d", hasCalls, getCalls, originCalls)
	}
}

func TestIncrementalSummaryIndexTracksInsertionsAndEvictions(t *testing.T) {
	index := newSummaryIndexWithParams(128, 3, 2)
	keys := []string{strings.Repeat("8", 64), strings.Repeat("9", 64), strings.Repeat("a", 64)}
	for _, key := range keys {
		index.Add(key)
	}
	count, bits := index.Snapshot()
	if count != len(keys) {
		t.Fatalf("entry count = %d, want %d", count, len(keys))
	}
	s := summaryFromIncrementalBits("peer-a", 1, count, 128, 3, bits, time.Now(), time.Minute)
	for _, key := range keys {
		if !s.Contains(key) {
			t.Fatalf("incremental filter omitted inserted key %q", key)
		}
	}
	index.Remove(keys[1])
	count, bits = index.Snapshot()
	if count != 2 {
		t.Fatalf("entry count after eviction = %d, want 2", count)
	}
	s = summaryFromIncrementalBits("peer-a", 2, count, 128, 3, bits, time.Now(), time.Minute)
	if !s.Contains(keys[0]) || !s.Contains(keys[2]) {
		t.Fatal("eviction cleared a bit shared by a remaining key")
	}
}

func TestIncrementalSummaryIndexContinuesPastCapacityAndReportsFPR(t *testing.T) {
	index := newSummaryIndexWithParams(256, 4, 2)
	for _, key := range []string{strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)} {
		index.Add(key)
	}
	count, _ := index.Snapshot()
	if count != 3 {
		t.Fatalf("entry count = %d, want entries beyond capacity retained", count)
	}
	if fpr := index.FalsePositiveRate(); fpr <= 0 {
		t.Fatalf("FPR = %v, want positive value", fpr)
	}
	if !index.Saturated() {
		t.Fatal("index should report saturation after capacity is exceeded")
	}
}

func TestDiskCacheIncrementallyUpdatesPublishedBloomOnPutAndEviction(t *testing.T) {
	cache, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	cache.maxBytes = 8
	first, second := strings.Repeat("e", 64), strings.Repeat("f", 64)
	if _, err := cache.PutStream(first, strings.NewReader("12345")); err != nil {
		t.Fatal(err)
	}
	items, bits, ok := cache.SummarySnapshot()
	if !ok || items != 1 {
		t.Fatalf("summary snapshot available=%t items=%d, want true/1", ok, items)
	}
	s := summaryFromIncrementalBits("peer-a", 1, items, summaryBloomBits, summaryBloomHashes, bits, time.Now(), time.Minute)
	if !s.Contains(first) {
		t.Fatal("served Bloom snapshot omitted committed cache entry")
	}
	if _, err := cache.PutStream(second, strings.NewReader("67890")); err != nil {
		t.Fatal(err)
	}
	items, _, ok = cache.SummarySnapshot()
	if !ok || items != 1 || cache.Has(first) || !cache.Has(second) {
		t.Fatalf("post-eviction summary/cache state: available=%t items=%d first=%t second=%t", ok, items, cache.Has(first), cache.Has(second))
	}
}

func TestSnapshotServingContinuesPastBloomTargetCapacity(t *testing.T) {
	cache, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	cache.summary.mu.Lock()
	cache.summary.itemCount = summaryBloomTargetItems + 1
	cache.summary.mu.Unlock()
	pm := peerManagerWith(nil)
	pm.ConfigureSummary(peerLookupSummary, "snapshot-server")
	pm.buildLocalSummary(cache)
	body := pm.localSummaryCopy()
	got, err := parseCacheSummary(body, time.Now())
	if err != nil {
		t.Fatalf("snapshot server skipped an over-target summary: %v", err)
	}
	if got.ItemCount != summaryBloomTargetItems+1 || got.MBits != summaryBloomBits || got.Hashes != summaryBloomHashes {
		t.Fatalf("served summary = items=%d bits=%d hashes=%d", got.ItemCount, got.MBits, got.Hashes)
	}
}

func TestSummaryFallbackProbesAreBounded(t *testing.T) {
	key := strings.Repeat("b", 64)
	var hasCalls int32
	peers := make([]string, 0, 8)
	for range 8 {
		peers = append(peers, newPeerServer(t, key, nil, http.StatusNotFound, &hasCalls, nil))
	}
	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "requester", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	if _, _, found, selected := pm.LocateKeyAmong(context.Background(), key, peers, pm.peerMaxProbes); found || selected != 5 {
		t.Fatalf("found=%t selected=%d, want false/5", found, selected)
	}
	if got := atomic.LoadInt32(&hasCalls); got != 5 {
		t.Fatalf("physical probes=%d, want 5", got)
	}
}

func TestSummaryFallbackSkipsWhenProbeCapacityIsExhausted(t *testing.T) {
	key := strings.Repeat("c", 64)
	var hasCalls int32
	peer := newPeerServer(t, key, nil, http.StatusNotFound, &hasCalls, nil)
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "requester", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 1})
	pm.probePermits <- struct{}{}
	defer func() { <-pm.probePermits }()
	if _, _, found, _ := pm.LocateKeyAmong(context.Background(), key, []string{peer}, pm.peerMaxProbes); found {
		t.Fatal("probe found a peer despite exhausted permit capacity")
	}
	if got := atomic.LoadInt32(&hasCalls); got != 0 {
		t.Fatalf("physical probes=%d, want 0 when capacity is exhausted", got)
	}
}

func TestSummaryMemoryBudgetRetainsOnlyPeersThatFit(t *testing.T) {
	peers := []string{"peer-a:8081", "peer-b:8081"}
	bits := make([]byte, summaryBloomBits/8)
	rawBits := int64(summaryBloomBits / 8)
	// During a rebuild, both the currently served and next encoded bodies are
	// live alongside the copied raw bits.
	reserve := rawBits + int64(summaryBloomBits)*2 + 2*int64(maxSummaryBodyBytes) + rawBits + int64(maxSummaryPulls)*(int64(maxSummaryBodyBytes)+rawBits+maxSummaryResponseHeaderBytes)
	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{MemoryLimitBytes: reserve + int64(len(bits))})
	for i, peer := range peers {
		s, err := newIncrementalCacheSummary(peer, uint64(i+1), 0, append([]byte(nil), bits...), time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := s.MarshalBinary()
		_ = pm.ReceiveSummary(peer, body, time.Now())
	}
	if got := pm.summaryCount(); got != 1 {
		t.Fatalf("retained summary count=%d, want one within the configured budget", got)
	}
	pm.summaries.mu.RLock()
	used := pm.summaries.bytes
	pm.summaries.mu.RUnlock()
	if used > len(bits) {
		t.Fatalf("retained bytes=%d exceed one-summary budget=%d", used, len(bits))
	}
}
