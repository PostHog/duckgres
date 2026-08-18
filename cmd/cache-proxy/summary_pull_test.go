package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests describe the pull-based summary protocol and the request-time
// confirmation contract. Bloom summaries only eliminate definite negatives;
// they never authorize a body GET without an exact /cache/has confirmation.

func TestSummaryParserRejectsLegacyDynamicBloomLayout(t *testing.T) {
	key := strings.Repeat("a", 64)
	bits, hashes := bloomParams(1)
	payload := make([]byte, bits/8)
	bloomHashes(key, bits, hashes, func(bit uint64) {
		payload[bit/8] |= 1 << (bit % 8)
	})
	summary := summaryFromIncrementalBits(bits, hashes, payload, time.Now(), time.Minute)
	body, err := summary.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseCacheSummary(body, time.Now()); err == nil {
		t.Fatal("accepted legacy dynamically sized Bloom layout; want fixed incremental layout only")
	}
}

func TestBlockPresentRejectsUntrackedDiskFile(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 100)
	if err != nil {
		t.Fatal(err)
	}
	key := BlockKey("https://example.invalid/object", 0, 1024)
	if err := os.WriteFile(filepath.Join(store.dir, key), []byte("stray"), 0o640); err != nil {
		t.Fatal(err)
	}
	proxy := NewCacheProxy(store, peerManagerWith(nil), nil)
	if proxy.blockPresent(key) {
		t.Fatal("untracked on-disk file reported as a present block")
	}
}

func TestSummarySyncImmediatelyPullsNewPeerWithGET(t *testing.T) {
	key := strings.Repeat("a", 64)
	summary := fixedCacheSummaryForKeys(t, []string{key}, time.Now(), time.Minute)
	body := marshalCacheSummaryForTest(t, summary)

	var gets, posts int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			atomic.AddInt32(&gets, 1)
			w.Header().Set("ETag", `"snapshot-1"`)
			_, _ = w.Write(body)
		case http.MethodPost:
			atomic.AddInt32(&posts, 1)
			w.WriteHeader(http.StatusMethodNotAllowed)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer remote.Close()

	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	store, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm.StartSummarySynchronizer(ctx, store)
	defer pm.StopSummarySynchronizer()

	eventually(t, time.Second, func() bool {
		return len(summaryPositivesForTest(pm, key, time.Now())) == 1
	})
	if got := atomic.LoadInt32(&gets); got != 1 {
		t.Fatalf("initial summary GETs=%d, want 1", got)
	}
	if got := atomic.LoadInt32(&posts); got != 0 {
		t.Fatalf("summary POSTs=%d, want 0 in receiver-driven protocol", got)
	}
}

func TestPeerSummaryGETServesSnapshotAndSupportsETag(t *testing.T) {
	key := strings.Repeat("b", 64)
	store, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutStream(key, strings.NewReader("cached")); err != nil {
		t.Fatal(err)
	}
	pm := peerManagerWith(nil)
	pm.ConfigureSummary(peerLookupSummary, "snapshot-server", SummaryConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm.StartSummarySynchronizer(ctx, store)
	defer pm.StopSummarySynchronizer()
	eventually(t, time.Second, func() bool { return len(localSummaryBodyForTest(pm)) > 0 })

	proxy := NewCacheProxy(store, pm, nil)
	first := httptest.NewRecorder()
	proxy.HandlePeerSummary(first, httptest.NewRequest(http.MethodGet, "/cache/summary", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("GET /cache/summary status=%d, want 200", first.Code)
	}
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("GET /cache/summary omitted ETag")
	}
	if got, want := first.Header().Get("Content-Length"), strconv.Itoa(first.Body.Len()); got != want {
		t.Fatalf("GET /cache/summary Content-Length=%q, want %q", got, want)
	}
	parsed, err := parseCacheSummary(first.Body.Bytes(), time.Now())
	if err != nil || !parsed.Contains(key) {
		t.Fatalf("served snapshot contains key=%t, parse error=%v", err == nil && parsed.Contains(key), err)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/cache/summary", nil)
	secondReq.Header.Set("If-None-Match", etag)
	second := httptest.NewRecorder()
	proxy.HandlePeerSummary(second, secondReq)
	if second.Code != http.StatusNotModified || second.Body.Len() != 0 {
		t.Fatalf("conditional GET status/body=%d/%d, want 304/0", second.Code, second.Body.Len())
	}
	if got := second.Header().Get("ETag"); got != etag {
		t.Fatalf("conditional GET ETag=%q, want %q", got, etag)
	}
	if got, want := second.Header().Get("Content-Length"), strconv.Itoa(first.Body.Len()); got != want {
		t.Fatalf("conditional GET Content-Length=%q, want selected representation length %q", got, want)
	}
}

func TestPeerSummaryGETSetsHandlerLocalWriteDeadline(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	pm := peerManagerWith(nil)
	pm.ConfigureSummary(peerLookupSummary, "snapshot-server", SummaryConfig{})
	pm.buildLocalSummary(store)
	proxy := NewCacheProxy(store, pm, nil)
	w := &deadlineResponseWriter{header: make(http.Header)}
	started := time.Now()
	proxy.HandlePeerSummary(w, httptest.NewRequest(http.MethodGet, "/cache/summary", nil))
	if w.deadline.IsZero() {
		t.Fatal("summary GET did not set a handler-local write deadline")
	}
	if got := w.deadline.Sub(started); got <= 0 || got > summaryServeTimeout+time.Second {
		t.Fatalf("summary write deadline offset=%s, want within %s", got, summaryServeTimeout)
	}
}

type deadlineResponseWriter struct {
	header   http.Header
	status   int
	deadline time.Time
}

func (w *deadlineResponseWriter) Header() http.Header { return w.header }
func (w *deadlineResponseWriter) WriteHeader(status int) {
	w.status = status
}
func (w *deadlineResponseWriter) Write(body []byte) (int, error) { return len(body), nil }
func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestSummarySelectionIsDeterministicAndFitsRemoteMemoryBudget(t *testing.T) {
	peers := []string{"peer-e:8081", "peer-b:8081", "peer-d:8081", "peer-a:8081", "peer-c:8081"}
	filterBytes := int(summaryBloomBits / 8)
	memoryLimit := summaryMemoryLimitForRemoteBytes(int64(2*filterBytes + filterBytes/2))

	first := NewPeerManager("test.svc", ":8081")
	first.ConfigureSummary(peerLookupSummary, "receiver-a", SummaryConfig{MemoryLimitBytes: memoryLimit})
	firstSelection := first.selectSummaryPeers(peers)
	if len(firstSelection) != 2 {
		t.Fatalf("selected peers=%v, want exactly two summaries within budget", firstSelection)
	}

	reversed := append([]string(nil), peers...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	second := NewPeerManager("test.svc", ":8081")
	second.ConfigureSummary(peerLookupSummary, "receiver-a", SummaryConfig{MemoryLimitBytes: memoryLimit})
	secondSelection := second.selectSummaryPeers(reversed)
	if !slices.Equal(firstSelection, secondSelection) {
		t.Fatalf("selection depends on DNS order: first=%v reversed=%v", firstSelection, secondSelection)
	}
	if used := len(firstSelection) * filterBytes; used > first.summaryRemoteMemoryBudget || used+filterBytes <= first.summaryRemoteMemoryBudget {
		t.Fatalf("selected bytes=%d budget=%d does not form the maximal fitting subset", used, first.summaryRemoteMemoryBudget)
	}
}

func TestSummarySyncPullsOnlyReceiverSelectedSubset(t *testing.T) {
	filterBytes := int64(summaryBloomBits / 8)
	var totalGets int32
	peers := make([]string, 0, 6)
	peerGets := make(map[string]*int32, 6)
	for range 6 {
		calls := new(int32)
		remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				atomic.AddInt32(calls, 1)
				atomic.AddInt32(&totalGets, 1)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(remote.Close)
		peer := strings.TrimPrefix(remote.URL, "http://")
		peers = append(peers, peer)
		peerGets[peer] = calls
	}

	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{MemoryLimitBytes: summaryMemoryLimitForRemoteBytes(2 * filterBytes)})
	wantSelected := pm.selectSummaryPeers(peers)
	if len(wantSelected) != 2 {
		t.Fatalf("selected peers=%v, want two", wantSelected)
	}
	store, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm.StartSummarySynchronizer(ctx, store)
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&totalGets) == 2 })
	cancel()
	pm.StopSummarySynchronizer()
	for peer, calls := range peerGets {
		got := atomic.LoadInt32(calls)
		want := int32(0)
		for _, selected := range wantSelected {
			if peer == selected {
				want = 1
				break
			}
		}
		if got != want {
			t.Fatalf("peer %q pull count=%d, want %d for selected subset %v", peer, got, want, wantSelected)
		}
	}
}

func TestConditionalSummaryPullPreservesRecordOnNotModified(t *testing.T) {
	key := strings.Repeat("1", 64)
	now := time.Now()
	summary := fixedCacheSummaryForKeys(t, []string{key}, now, time.Minute)
	body := marshalCacheSummaryForTest(t, summary)
	const etag = `"remote-snapshot-1"`
	var calls int32
	var conditional atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body)
			return
		}
		if r.Header.Get("If-None-Match") == etag {
			conditional.Store(true)
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
	}))
	defer remote.Close()

	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.pullSummary(context.Background(), peer)
	pm.pullSummary(context.Background(), peer)
	if !conditional.Load() {
		t.Fatal("second summary pull omitted If-None-Match for the retained ETag")
	}
	if candidates := summaryPositivesForTest(pm, key, time.Now()); len(candidates) != 1 || candidates[0] != peer {
		t.Fatalf("304 discarded the last valid summary: candidates=%v", candidates)
	}
}

func TestFailedSummaryPullRetainsHintUntilTTLThenBecomesUncovered(t *testing.T) {
	key := strings.Repeat("2", 64)
	created := time.Now()
	summary := fixedCacheSummaryForKeys(t, []string{key}, created, 5*time.Second)
	body := marshalCacheSummaryForTest(t, summary)
	var fail atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", `"remote-snapshot-1"`)
		_, _ = w.Write(body)
	}))
	defer remote.Close()

	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.pullSummary(context.Background(), peer)
	fail.Store(true)
	pm.pullSummary(context.Background(), peer)
	positive, uncovered := pm.SummaryLookup(key, created.Add(time.Second))
	if len(positive) != 1 || positive[0] != peer || len(uncovered) != 0 {
		t.Fatalf("failed refresh discarded valid hint early: positive=%v uncovered=%v", positive, uncovered)
	}
	positive, uncovered = pm.SummaryLookup(key, created.Add(6*time.Second))
	if len(positive) != 0 || len(uncovered) != 1 || uncovered[0] != peer {
		t.Fatalf("expired hint remained authoritative: positive=%v uncovered=%v", positive, uncovered)
	}
}

func TestExpiredSummaryETagDoesNotPreventUnconditionalRecoveryPull(t *testing.T) {
	oldKey, newKey := strings.Repeat("5", 64), strings.Repeat("6", 64)
	first := fixedCacheSummaryForKeys(t, []string{oldKey}, time.Now(), 200*time.Millisecond)
	firstBody := marshalCacheSummaryForTest(t, first)
	second := fixedCacheSummaryForKeys(t, []string{newKey}, time.Now(), time.Minute)
	secondBody := marshalCacheSummaryForTest(t, second)
	const firstETag = `"remote-snapshot-1"`
	var calls int32
	var staleConditional atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("ETag", firstETag)
			_, _ = w.Write(firstBody)
			return
		}
		if r.Header.Get("If-None-Match") != "" {
			staleConditional.Store(true)
		}
		w.Header().Set("ETag", `"remote-snapshot-2"`)
		_, _ = w.Write(secondBody)
	}))
	defer remote.Close()

	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.pullSummary(context.Background(), peer)
	time.Sleep(250 * time.Millisecond)
	pm.pullSummary(context.Background(), peer)
	if staleConditional.Load() {
		t.Fatal("pull sent If-None-Match for an expired summary")
	}
	if candidates := summaryPositivesForTest(pm, newKey, time.Now()); len(candidates) != 1 || candidates[0] != peer {
		t.Fatalf("unconditional recovery pull did not install new summary: candidates=%v", candidates)
	}
}

func TestRejectedSummaryPullDoesNotReplaceLastValidHint(t *testing.T) {
	oldKey, newKey := strings.Repeat("3", 64), strings.Repeat("4", 64)
	now := time.Now()
	valid := fixedCacheSummaryForKeys(t, []string{oldKey}, now, time.Minute)
	validBody := marshalCacheSummaryForTest(t, valid)
	invalid := fixedCacheSummaryForKeys(t, []string{newKey}, now, time.Minute)
	invalidBody := marshalCacheSummaryForTest(t, invalid)
	invalidBody[0] ^= 0xff
	var serveInvalid atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if serveInvalid.Load() {
			w.Header().Set("ETag", `"remote-snapshot-2"`)
			_, _ = w.Write(invalidBody)
			return
		}
		w.Header().Set("ETag", `"remote-snapshot-1"`)
		_, _ = w.Write(validBody)
	}))
	defer remote.Close()

	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.pullSummary(context.Background(), peer)
	serveInvalid.Store(true)
	pm.pullSummary(context.Background(), peer)
	if candidates := summaryPositivesForTest(pm, oldKey, time.Now()); len(candidates) != 1 || candidates[0] != peer {
		t.Fatalf("invalid refresh replaced last valid hint: candidates=%v", candidates)
	}
	if candidates := summaryPositivesForTest(pm, newKey, time.Now()); len(candidates) != 0 {
		t.Fatalf("invalid refresh installed new key: candidates=%v", candidates)
	}
}

func TestOversizedPeerETagIsNotRetainedWithValidSummary(t *testing.T) {
	peer := "peer-a:8081"
	oldKey, newKey := strings.Repeat("7", 64), strings.Repeat("8", 64)
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()

	first := fixedCacheSummaryForKeys(t, []string{oldKey}, time.Now(), time.Minute)
	firstBody := marshalCacheSummaryForTest(t, first)
	const ordinaryETag = `"snapshot-1"`
	if err := pm.receivePulledSummary(peer, firstBody, ordinaryETag, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := pm.summaries.etag(peer, time.Now()); got != ordinaryETag {
		t.Fatalf("ordinary ETag=%q, want %q", got, ordinaryETag)
	}

	second := fixedCacheSummaryForKeys(t, []string{newKey}, time.Now(), time.Minute)
	secondBody := marshalCacheSummaryForTest(t, second)
	oversizedETag := `"` + strings.Repeat("x", 1<<20) + `"`
	if err := pm.receivePulledSummary(peer, secondBody, oversizedETag, time.Now()); err != nil {
		t.Fatalf("valid summary was rejected solely because its optional ETag was oversized: %v", err)
	}
	if candidates := summaryPositivesForTest(pm, newKey, time.Now()); len(candidates) != 1 || candidates[0] != peer {
		t.Fatalf("valid newer summary was not installed: candidates=%v", candidates)
	}
	if got := pm.summaries.etag(peer, time.Now()); got != "" {
		t.Fatalf("peer-controlled oversized ETag retained %d bytes, want it discarded", len(got))
	}
}

func TestSummaryClientBoundsResponseHeaders(t *testing.T) {
	key := strings.Repeat("9", 64)
	body := marshalCacheSummaryForTest(t, fixedCacheSummaryForKeys(t, []string{key}, time.Now(), time.Minute))
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Oversized", strings.Repeat("x", maxSummaryResponseHeaderBytes+1))
		_, _ = w.Write(body)
	}))
	defer remote.Close()
	peer := strings.TrimPrefix(remote.URL, "http://")
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.pullSummary(context.Background(), peer)
	if positive := summaryPositivesForTest(pm, key, time.Now()); len(positive) != 0 {
		t.Fatalf("summary behind oversized response headers was retained: %v", positive)
	}
}

func TestShortSummaryPullCyclesRotateAcrossSelectedPeers(t *testing.T) {
	peers := make([]string, 12)
	for i := range peers {
		peers[i] = "peer-" + strconv.Itoa(i) + ":8081"
	}
	var mu sync.Mutex
	attempted := make(map[string]int, len(peers))
	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.summaryPullCycleTimeout = 10 * time.Millisecond
	pm.summaryClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		attempted[req.URL.Host]++
		mu.Unlock()
		<-req.Context().Done()
		// Keep all workers occupied briefly after the cycle deadline. The job
		// producer therefore observes cancellation before a worker can accept a
		// fifth job, making each cycle's attempted window deterministic.
		time.Sleep(5 * time.Millisecond)
		return nil, req.Context().Err()
	})}

	for range 3 {
		pm.pullSummaries(context.Background(), peers)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, peer := range peers {
		if attempted[peer] == 0 {
			t.Fatalf("later selected peer %q was starved across repeated deadline-limited cycles; attempts=%v", peer, attempted)
		}
	}
}

func TestMembershipPullPrioritizesNewlySelectedPeer(t *testing.T) {
	oldPeers := []string{"peer-a:8081", "peer-b:8081", "peer-c:8081", "peer-d:8081"}
	newPeer := "peer-e:8081"
	pm := peerManagerWith(oldPeers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	pm.refreshSummarySelection()
	pm.updateResolvedPeers(append(append([]string(nil), oldPeers...), newPeer), time.Now())

	var mu sync.Mutex
	var attempted []string
	pm.summaryClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		attempted = append(attempted, req.URL.Host)
		mu.Unlock()
		return nil, errors.New("test pull failure")
	})}
	pm.pullPendingSummaryPeers(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(attempted) != 1 || attempted[0] != newPeer {
		t.Fatalf("membership pull attempts=%v, want only newly selected peer %q", attempted, newPeer)
	}
}

func TestMaxOneSummaryProbeCanSelectUncoveredPeer(t *testing.T) {
	positive := []string{"positive:8081"}
	uncovered := []string{"uncovered:8081"}
	var key string
	for i := 0; i < 10_000; i++ {
		candidate := strings.Repeat(strconv.FormatInt(int64(i%16), 16), 64)
		if selected := selectProbePeers(candidate, append(append([]string(nil), positive...), uncovered...), 1); len(selected) == 1 && selected[0] == uncovered[0] {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("test could not find a deterministic key ranking the uncovered peer first")
	}
	selected := selectSummaryProbePeers(key, positive, uncovered, 1)
	if len(selected) != 1 || selected[0] != uncovered[0] {
		t.Fatalf("maxProbes=1 selected %v, want uncovered peer %q for key whose combined rank prefers it", selected, uncovered[0])
	}
}

func TestSummaryPullConcurrencyIsBoundedAndCancellationStopsWork(t *testing.T) {
	var active, maximum, started int32
	release := make(chan struct{})
	peers := make([]string, 0, 12)
	for range 12 {
		remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			atomic.AddInt32(&started, 1)
			now := atomic.AddInt32(&active, 1)
			for {
				old := atomic.LoadInt32(&maximum)
				if now <= old || atomic.CompareAndSwapInt32(&maximum, old, now) {
					break
				}
			}
			defer atomic.AddInt32(&active, -1)
			select {
			case <-release:
				w.WriteHeader(http.StatusNotFound)
			case <-r.Context().Done():
			}
		}))
		t.Cleanup(remote.Close)
		peers = append(peers, strings.TrimPrefix(remote.URL, "http://"))
	}

	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{})
	store, err := NewDiskCache(t.TempDir(), 100, DiskCacheOptions{IncrementalSummary: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pm.StartSummarySynchronizer(ctx, store)
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&started) > 0 })
	const maxPullConcurrency = 4
	if got := atomic.LoadInt32(&maximum); got > maxPullConcurrency {
		t.Fatalf("simultaneous summary pulls=%d, want <=%d", got, maxPullConcurrency)
	}
	cancel()
	pm.StopSummarySynchronizer()
	eventually(t, time.Second, func() bool { return atomic.LoadInt32(&active) == 0 })
	close(release)
}

func TestSummaryPositivesAreConfirmedBeforeExactlyOnePeerGET(t *testing.T) {
	key := strings.Repeat("c", 64)
	var falseHas, falseGet, realHas, realGet, originCalls int32
	falsePeer := confirmationPeer(t, key, http.StatusNotFound, http.StatusNotFound, nil, 0, &falseHas, &falseGet, nil)
	realPeer := confirmationPeer(t, key, http.StatusOK, http.StatusOK, []byte("peer"), 25*time.Millisecond, &realHas, &realGet, nil)

	pm := peerManagerWith([]string{falsePeer, realPeer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	installPositiveSummary(t, pm, falsePeer, key)
	installPositiveSummary(t, pm, realPeer, key)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()

	proxy := NewCacheProxy(newTestCache(t), pm, nil)
	proxy.client = origin.Client()
	got, err := proxy.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "peer" {
		t.Fatalf("fetch=%+v err=%v, want peer", got, err)
	}
	if gotFalse, gotReal := atomic.LoadInt32(&falseHas), atomic.LoadInt32(&realHas); gotFalse != 1 || gotReal != 1 {
		t.Fatalf("confirmation calls false/real=%d/%d, want 1/1", gotFalse, gotReal)
	}
	if gotFalse, gotReal, gotOrigin := atomic.LoadInt32(&falseGet), atomic.LoadInt32(&realGet), atomic.LoadInt32(&originCalls); gotFalse != 0 || gotReal != 1 || gotOrigin != 0 {
		t.Fatalf("body GETs false/real=%d/%d origin=%d, want 0/1/0", gotFalse, gotReal, gotOrigin)
	}
}

func TestFalsePositiveDoesNotSuppressUncoveredPeerProbe(t *testing.T) {
	key := strings.Repeat("d", 64)
	var falseHas, falseGet, realHas, realGet, originCalls int32
	falsePeer := confirmationPeer(t, key, http.StatusNotFound, http.StatusNotFound, nil, 0, &falseHas, &falseGet, nil)
	realPeer := confirmationPeer(t, key, http.StatusOK, http.StatusOK, []byte("peer"), 20*time.Millisecond, &realHas, &realGet, nil)
	pm := peerManagerWith([]string{falsePeer, realPeer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	installPositiveSummary(t, pm, falsePeer, key) // realPeer is intentionally uncovered
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()

	proxy := NewCacheProxy(newTestCache(t), pm, nil)
	proxy.client = origin.Client()
	got, err := proxy.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "peer" {
		t.Fatalf("fetch=%+v err=%v, want uncovered peer", got, err)
	}
	gotFalseHas, gotRealHas := atomic.LoadInt32(&falseHas), atomic.LoadInt32(&realHas)
	gotFalseGet, gotRealGet, gotOrigin := atomic.LoadInt32(&falseGet), atomic.LoadInt32(&realGet), atomic.LoadInt32(&originCalls)
	if gotFalseHas != 1 || gotRealHas != 1 || gotFalseGet != 0 || gotRealGet != 1 || gotOrigin != 0 {
		t.Fatalf("false has/get=%d/%d real has/get=%d/%d origin=%d, want 1/0 1/1 0", gotFalseHas, gotFalseGet, gotRealHas, gotRealGet, gotOrigin)
	}
}

func TestFirstUsefulConfirmationTriggersExactlyOnePeerGET(t *testing.T) {
	key := strings.Repeat("f", 64)
	var flightHas, flightGet, presentHas, presentGet, originCalls int32
	var flightSeen atomic.Bool
	flightPeer := confirmationPeer(t, key, http.StatusAccepted, http.StatusOK, []byte("in-flight"), 0, &flightHas, &flightGet, &flightSeen)
	presentPeer := confirmationPeer(t, key, http.StatusOK, http.StatusOK, []byte("present"), 100*time.Millisecond, &presentHas, &presentGet, nil)
	pm := peerManagerWith([]string{flightPeer, presentPeer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	installPositiveSummary(t, pm, flightPeer, key)
	installPositiveSummary(t, pm, presentPeer, key)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()

	proxy := NewCacheProxy(newTestCache(t), pm, nil)
	proxy.client = origin.Client()
	got, err := proxy.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "peer" {
		t.Fatalf("fetch=%+v err=%v, want peer", got, err)
	}
	if gotFlight, gotPresent := atomic.LoadInt32(&flightGet), atomic.LoadInt32(&presentGet); gotFlight != 1 || gotPresent != 0 {
		t.Fatalf("peer GETs in-flight/present=%d/%d, want 1/0", gotFlight, gotPresent)
	}
	if !flightSeen.Load() {
		t.Fatal("GET selected by a 202 confirmation omitted flight=1")
	}
	if got := atomic.LoadInt32(&originCalls); got != 0 {
		t.Fatalf("origin calls=%d, want 0", got)
	}
}

func TestSummaryTotalConfirmationWorkIsCappedPerRequest(t *testing.T) {
	key := strings.Repeat("e", 64)
	var hasCalls, getCalls, originCalls int32
	peers := make([]string, 0, 11)
	for i := 0; i < 11; i++ {
		peer := confirmationPeer(t, key, http.StatusNotFound, http.StatusNotFound, nil, 0, &hasCalls, &getCalls, nil)
		peers = append(peers, peer)
	}
	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	for _, peer := range peers[:8] {
		installPositiveSummary(t, pm, peer, key)
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&originCalls, 1)
		_, _ = w.Write([]byte("origin"))
	}))
	defer origin.Close()
	proxy := NewCacheProxy(newTestCache(t), pm, nil)
	proxy.client = origin.Client()
	got, err := proxy.fetchDedup(key, httptest.NewRequest(http.MethodGet, origin.URL+"/object", nil), "")
	if err != nil || got.source != "miss" {
		t.Fatalf("fetch=%+v err=%v, want origin miss", got, err)
	}
	if got := atomic.LoadInt32(&hasCalls); got != 5 {
		t.Fatalf("confirmation probes=%d, want request cap 5", got)
	}
	if gotGET, gotOrigin := atomic.LoadInt32(&getCalls), atomic.LoadInt32(&originCalls); gotGET != 0 || gotOrigin != 1 {
		t.Fatalf("peer GETs/origin=%d/%d, want 0/1", gotGET, gotOrigin)
	}
}

func TestBlockRequestSharesOneConfirmationBudgetAcrossAllBlocks(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 8*blockSize)
	defer origin.Close()
	target := origin.URL + "/bucket/large.parquet"
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = BlockKey(target, int64(i), blockSize)
	}

	var hasCalls, getCalls int32
	peers := make([]string, 0, 8)
	for range 8 {
		peers = append(peers, confirmationPeer(t, keys[0], http.StatusNotFound, http.StatusNotFound, nil, 0, &hasCalls, &getCalls, nil))
	}
	pm := peerManagerWith(peers)
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	for _, peer := range peers {
		installPositiveSummaryForKeys(t, pm, peer, keys)
	}
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	proxy := NewCacheProxy(store, pm, nil)
	proxy.client = origin.Client()
	proxy.blockSize = blockSize
	proxy.maxSpanBlocks = 8

	lookupsBefore := counterValue(t, summaryLookupTotal.WithLabelValues("positive_candidate"))
	response := doBlockRequest(t, proxy, target, "bytes=0-8191")
	if response.Code != http.StatusPartialContent {
		t.Fatalf("status=%d, want 206", response.Code)
	}
	if got := atomic.LoadInt32(&hasCalls); got != 5 {
		t.Fatalf("confirmation probes across eight blocks=%d, want one request budget of 5", got)
	}
	if got := atomic.LoadInt32(&getCalls); got != 0 {
		t.Fatalf("peer GETs=%d, want 0 after confirmation misses", got)
	}
	if delta := counterValue(t, summaryLookupTotal.WithLabelValues("positive_candidate")) - lookupsBefore; delta != 1 {
		t.Fatalf("summary lookups after shared probe budget exhaustion=%v, want 1 total", delta)
	}
}

func TestBlockRequestStopsSummaryLookupsAfterGETBudgetExhaustion(t *testing.T) {
	const blockSize = 1024
	origin := originServer(t, 4*blockSize)
	defer origin.Close()
	target := origin.URL + "/bucket/get-budget.parquet"
	keys := make([]string, 4)
	for i := range keys {
		keys[i] = BlockKey(target, int64(i), blockSize)
	}

	var hasCalls, getCalls int32
	peer := confirmationPeer(t, keys[0], http.StatusOK, http.StatusNotFound, nil, 0, &hasCalls, &getCalls, nil)
	pm := peerManagerWith([]string{peer})
	pm.ConfigureSummary(peerLookupSummary, "receiver", SummaryConfig{PeerMaxProbes: 5, MaxPeerProbesInFlight: 5})
	installPositiveSummaryForKeys(t, pm, peer, keys)
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatal(err)
	}
	proxy := NewCacheProxy(store, pm, nil)
	proxy.client = origin.Client()
	proxy.blockSize = blockSize
	proxy.maxSpanBlocks = 4

	lookupsBefore := counterValue(t, summaryLookupTotal.WithLabelValues("positive_candidate"))
	response := doBlockRequest(t, proxy, target, "bytes=0-4095")
	if response.Code != http.StatusPartialContent {
		t.Fatalf("status=%d, want 206", response.Code)
	}
	if gotHas, gotGET := atomic.LoadInt32(&hasCalls), atomic.LoadInt32(&getCalls); gotHas != 2 || gotGET != 2 {
		t.Fatalf("peer has/get calls=%d/%d, want 2/2 before GET budget exhaustion", gotHas, gotGET)
	}
	if delta := counterValue(t, summaryLookupTotal.WithLabelValues("positive_candidate")) - lookupsBefore; delta != 2 {
		t.Fatalf("summary lookups after shared GET budget exhaustion=%v, want 2 total", delta)
	}
}

func installPositiveSummary(t *testing.T, pm *PeerManager, peer, key string) {
	installPositiveSummaryForKeys(t, pm, peer, []string{key})
}

func installPositiveSummaryForKeys(t *testing.T, pm *PeerManager, peer string, keys []string) {
	t.Helper()
	installPulledSummaryForTest(t, pm, peer, fixedCacheSummaryForKeys(t, keys, time.Now(), time.Minute), time.Now())
}

func confirmationPeer(t *testing.T, key string, hasStatus, getStatus int, data []byte, hasDelay time.Duration, hasCalls, getCalls *int32, flightSeen *atomic.Bool) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cache/has":
			atomic.AddInt32(hasCalls, 1)
			if hasDelay > 0 {
				select {
				case <-time.After(hasDelay):
				case <-r.Context().Done():
					return
				}
			}
			w.WriteHeader(hasStatus)
		case "/cache/get":
			atomic.AddInt32(getCalls, 1)
			if flightSeen != nil && r.URL.Query().Get("flight") == "1" {
				flightSeen.Store(true)
			}
			w.WriteHeader(getStatus)
			if getStatus == http.StatusOK {
				_, _ = w.Write(data)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition did not become true before timeout")
	}
}

func summaryMemoryLimitForRemoteBytes(remoteBytes int64) int64 {
	return summaryMemoryReserveBytes() + remoteBytes
}
