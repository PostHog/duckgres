package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixedCacheSummaryForKeys(t testing.TB, keys []string, now time.Time, ttl time.Duration) *cacheSummary {
	t.Helper()
	bits := make([]byte, summaryBloomBits/8)
	for _, key := range keys {
		if !IsValidCacheKey(key) {
			t.Fatalf("invalid test cache key %q", key)
		}
		bloomHashes(key, summaryBloomBits, summaryBloomHashes, func(bit uint64) {
			bits[bit/8] |= 1 << (bit % 8)
		})
	}
	summary, err := newIncrementalCacheSummary(bits, now, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return summary
}

func marshalCacheSummaryForTest(t testing.TB, summary *cacheSummary) []byte {
	t.Helper()
	body, err := summary.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func installPulledSummaryForTest(t testing.TB, pm *PeerManager, peer string, summary *cacheSummary, now time.Time) {
	t.Helper()
	pm.refreshSummarySelection()
	if err := pm.receivePulledSummary(peer, marshalCacheSummaryForTest(t, summary), "", now); err != nil {
		t.Fatal(err)
	}
}

func summaryPositivesForTest(pm *PeerManager, key string, now time.Time) []string {
	positive, _ := pm.SummaryLookup(key, now)
	return positive
}

func localSummaryBodyForTest(pm *PeerManager) []byte {
	body, _ := pm.localSummarySnapshot()
	return append([]byte(nil), body...)
}

func TestSummaryTTLCoversTwoWorstCaseSyncIntervals(t *testing.T) {
	maxSyncInterval := defaultSummaryInterval + defaultSummaryInterval/5
	minimumTTL := 2*maxSyncInterval + summaryPullTimeout
	if defaultSummaryTTL < minimumTTL {
		t.Fatalf("summary TTL %s is shorter than two worst-case sync intervals plus pull margin %s", defaultSummaryTTL, minimumTTL)
	}
}

func TestCacheSummaryContainsSnapshotKeysButNotRawKeys(t *testing.T) {
	keys := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	s := fixedCacheSummaryForKeys(t, keys, time.Now(), defaultSummaryTTL)
	for _, key := range keys {
		if !s.Contains(key) {
			t.Fatalf("summary omitted source key %q", key)
		}
	}
	body := marshalCacheSummaryForTest(t, s)
	for _, secret := range append(keys, "https://bucket.example/object?X-Amz-Signature=secret") {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatalf("wire summary leaked raw cache locator %q", secret)
		}
	}
}

func TestCacheSummaryWireOmitsPushEraMetadata(t *testing.T) {
	s := fixedCacheSummaryForKeys(t, nil, time.Now(), defaultSummaryTTL)
	body := marshalCacheSummaryForTest(t, s)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"s", "g", "n"} {
		if _, ok := fields[field]; ok {
			t.Fatalf("pull summary retained obsolete wire field %q", field)
		}
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, addr, fixedCacheSummaryForKeys(t, []string{key}, time.Now(), time.Minute), time.Now())
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	key := strings.Repeat("e", 64)
	installPulledSummaryForTest(t, pm, covered, fixedCacheSummaryForKeys(t, []string{strings.Repeat("f", 64)}, time.Now(), time.Minute), time.Now())
	positive, missing := pm.SummaryLookup(key, time.Now())
	if len(positive) != 0 {
		t.Fatalf("positive peers = %v, want none", positive)
	}
	if len(missing) != 1 || missing[0] != uncovered {
		t.Fatalf("uncovered peers = %v, want %q", missing, uncovered)
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, addr, fixedCacheSummaryForKeys(t, []string{key}, time.Now(), time.Minute), time.Now())
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, covered, fixedCacheSummaryForKeys(t, []string{other}, time.Now(), time.Minute), time.Now())
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, peer, fixedCacheSummaryForKeys(t, []string{other}, time.Now(), time.Minute), time.Now())
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, covered, fixedCacheSummaryForKeys(t, []string{other}, time.Now(), time.Minute), time.Now())

	// DNS membership changes before this proxy has received the new peer's
	// summary. That peer is intentionally the only one left on probe fallback.
	pm.updateResolvedPeers([]string{covered, joining}, time.Now())
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
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	installPulledSummaryForTest(t, pm, peer, fixedCacheSummaryForKeys(t, []string{key}, time.Now(), time.Minute), time.Now())
	pm.updateResolvedPeers(nil, time.Now())
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
	s := summaryFromIncrementalBits(128, 3, bits, time.Now(), time.Minute)
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
	s = summaryFromIncrementalBits(128, 3, bits, time.Now(), time.Minute)
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
	if fpr := bloomFalsePositiveRate(count, index.bitCount, index.hashes); fpr <= 0 {
		t.Fatalf("FPR = %v, want positive value", fpr)
	}
	if count <= index.targetItems {
		t.Fatal("index should report saturation after capacity is exceeded")
	}
}

func TestDiskCacheIncrementallyUpdatesSummaryBloomOnPutAndEviction(t *testing.T) {
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
	s := summaryFromIncrementalBits(summaryBloomBits, summaryBloomHashes, bits, time.Now(), time.Minute)
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

func TestSummaryMemoryMetricsIncludeLocalTransientAndRemoteState(t *testing.T) {
	peer := "peer-a:8081"
	bits := make([]byte, summaryBloomBits/8)
	limit := summaryMemoryReserveBytes() + int64(len(bits))
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{MemoryLimitBytes: limit})
	pm.refreshSummarySelection()

	if got := int64(gaugeValue(t, summaryMemoryLimitBytes)); got != limit {
		t.Fatalf("summary memory limit metric=%d, want %d", got, limit)
	}
	if got := int64(gaugeValue(t, summaryResidentBytes)); got != summaryMemoryReserveBytes() {
		t.Fatalf("summary resident bytes before remote receipt=%d, want fixed/transient reserve %d", got, summaryMemoryReserveBytes())
	}

	s, err := newIncrementalCacheSummary(bits, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := pm.receivePulledSummary(peer, marshalCacheSummaryForTest(t, s), "", time.Now()); err != nil {
		t.Fatal(err)
	}
	wantResident := summaryMemoryReserveBytes() + int64(len(bits))
	if got := int64(gaugeValue(t, summaryResidentBytes)); got != wantResident {
		t.Fatalf("summary resident bytes after remote receipt=%d, want %d", got, wantResident)
	}
}

func TestSummaryMemoryMetricsAreZeroOutsideSummaryMode(t *testing.T) {
	pm := NewPeerManager("test.svc", ":8081")
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	if got := int64(gaugeValue(t, summaryMemoryLimitBytes)); got != defaultSummaryMemoryLimitBytes {
		t.Fatalf("default summary memory limit metric=%d, want %d", got, defaultSummaryMemoryLimitBytes)
	}
	if got := int64(gaugeValue(t, summaryResidentBytes)); got != summaryMemoryReserveBytes() {
		t.Fatalf("default summary resident metric=%d, want reserve %d", got, summaryMemoryReserveBytes())
	}

	pm.ConfigureSummary(peerLookupProbe, "receiver", SummaryConfig{})
	if got := gaugeValue(t, summaryMemoryLimitBytes); got != 0 {
		t.Fatalf("probe-mode summary memory limit metric=%v, want 0", got)
	}
	if got := gaugeValue(t, summaryResidentBytes); got != 0 {
		t.Fatalf("probe-mode summary resident metric=%v, want 0", got)
	}
}
