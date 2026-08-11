package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// collectSink returns a sink that streams the peer body into buf, mirroring how
// DiskCache.PutStream consumes it in production (without touching disk).
func collectSink(buf *bytes.Buffer) func(io.Reader) (int64, error) {
	return func(r io.Reader) (int64, error) {
		return io.Copy(buf, r)
	}
}

// newPeerServer returns an httptest server exposing /cache/has and /cache/get
// for the supplied key and data. hasStatus controls the /cache/has answer for
// the key (200 = has it, 202 = mid-flight, 404 = no); getCallback increments a
// counter on body requests.
func newPeerServer(t *testing.T, key string, data []byte, hasStatus int, hasCalls, getCalls *int32) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		if hasCalls != nil {
			atomic.AddInt32(hasCalls, 1)
		}
		if r.URL.Query().Get("key") == key {
			w.WriteHeader(hasStatus)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		if getCalls != nil {
			atomic.AddInt32(getCalls, 1)
		}
		if r.URL.Query().Get("key") != key {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// peerManagerWith returns a PeerManager with the given peer addresses hardcoded,
// skipping DNS resolution entirely.
func peerManagerWith(peers []string) *PeerManager {
	pm := NewPeerManager("test.svc", ":8081")
	pm.peers = peers
	return pm
}

func TestPeerRoundTripHit(t *testing.T) {
	key := strings.Repeat("f", 64)
	data := []byte("from-peer")
	var hasCalls, getCalls int32

	addr := newPeerServer(t, key, data, http.StatusOK, &hasCalls, &getCalls)
	pm := peerManagerWith([]string{addr})

	holder, flight, ok := pm.LocateKey(key)
	if !ok {
		t.Fatal("expected peer claim")
	}
	if flight {
		t.Error("flight should be false for a 200 peer")
	}
	if holder != addr {
		t.Errorf("holder = %q, want %q", holder, addr)
	}

	var buf bytes.Buffer
	n, ok := pm.FetchFromPeer(holder, key, flight, collectSink(&buf))
	if !ok {
		t.Fatal("expected peer fetch to succeed")
	}
	if buf.String() != string(data) {
		t.Errorf("peer data = %q, want %q", buf.String(), data)
	}
	if n != int64(len(data)) {
		t.Errorf("streamed bytes = %d, want %d", n, len(data))
	}
	if atomic.LoadInt32(&hasCalls) != 1 {
		t.Errorf("peer /cache/has calls = %d, want 1", hasCalls)
	}
	if atomic.LoadInt32(&getCalls) != 1 {
		t.Errorf("peer /cache/get calls = %d, want 1", getCalls)
	}
}

func TestLocateKeyMissFromAll(t *testing.T) {
	key := strings.Repeat("a", 64)
	other := strings.Repeat("b", 64)
	var hasCalls, getCalls int32

	// Seed peer with DIFFERENT key so our lookup misses.
	addr := newPeerServer(t, other, []byte("not-ours"), http.StatusOK, &hasCalls, &getCalls)
	pm := peerManagerWith([]string{addr})

	if _, _, ok := pm.LocateKey(key); ok {
		t.Fatal("expected miss from every peer")
	}
	if atomic.LoadInt32(&hasCalls) != 1 {
		t.Errorf("peer /cache/has calls = %d, want 1", hasCalls)
	}
	// On miss, /cache/get should NOT be called.
	if atomic.LoadInt32(&getCalls) != 0 {
		t.Errorf("peer /cache/get calls = %d, want 0 on miss", getCalls)
	}
}

func TestLocateKeyInFlightClaim(t *testing.T) {
	key := strings.Repeat("e", 64)
	var hasCalls, getCalls int32

	addr := newPeerServer(t, key, []byte("filling"), http.StatusAccepted, &hasCalls, &getCalls)
	pm := peerManagerWith([]string{addr})

	holder, flight, ok := pm.LocateKey(key)
	if !ok {
		t.Fatal("a 202 peer is still a claim")
	}
	if !flight {
		t.Error("flight should be true for a 202 peer")
	}
	if holder != addr {
		t.Errorf("holder = %q, want %q", holder, addr)
	}
}

func TestLocateKeyPrefersPresentEntryOverInFlight(t *testing.T) {
	key := strings.Repeat("d", 64)
	data := []byte("present")
	var h200, g200, h202, g202 int32

	// One peer has it (200), another is mid-flight (202). The 200 must win so
	// no one waits on a fill that already exists elsewhere.
	present := newPeerServer(t, key, data, http.StatusOK, &h200, &g200)
	filling := newPeerServer(t, key, []byte("filling"), http.StatusAccepted, &h202, &g202)
	pm := peerManagerWith([]string{filling, present})

	holder, flight, ok := pm.LocateKey(key)
	if !ok {
		t.Fatal("expected a claim")
	}
	if flight || holder != present {
		t.Errorf("claim = (%q, flight=%v), want the 200 peer %q with flight=false", holder, flight, present)
	}
}

func TestLocateKeyReturnsFirstHit(t *testing.T) {
	key := strings.Repeat("c", 64)
	data := []byte("winner")
	var has1, get1, has2, get2 int32

	// Two peers both have the data. We should get one successful claim and not
	// block waiting for both.
	addr1 := newPeerServer(t, key, data, http.StatusOK, &has1, &get1)
	addr2 := newPeerServer(t, key, data, http.StatusOK, &has2, &get2)
	pm := peerManagerWith([]string{addr1, addr2})

	holder, flight, ok := pm.LocateKey(key)
	if !ok {
		t.Fatal("expected peer claim from one of two peers")
	}
	if flight {
		t.Error("flight should be false for a 200 claim")
	}
	if holder != addr1 && holder != addr2 {
		t.Errorf("holder = %q, want one of the two peers", holder)
	}
	totalHas := atomic.LoadInt32(&has1) + atomic.LoadInt32(&has2)
	if totalHas < 1 {
		t.Errorf("no peer /cache/has calls at all")
	}
}

func TestFetchFromPeerEmptyPeerList(t *testing.T) {
	pm := peerManagerWith(nil)
	if _, _, ok := pm.LocateKey(strings.Repeat("a", 64)); ok {
		t.Error("expected miss when no peers are known")
	}
}

func TestFetchFromPeerLargeBodyHasNoClockTimeout(t *testing.T) {
	// The transfer must not be bounded by a whole-request wall clock: a big
	// body trickling under the old 30s budget used to be killed mid-stream and
	// downgraded to an origin refetch. Simulate a slow-but-steady body and
	// require it to complete.
	key := strings.Repeat("9", 64)
	chunk := bytes.Repeat([]byte("x"), 64*1024)
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/cache/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(chunk)*3))
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			_, _ = w.Write(chunk)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	pm := peerManagerWith([]string{strings.TrimPrefix(srv.URL, "http://")})

	var buf bytes.Buffer
	n, ok := pm.FetchFromPeer(strings.TrimPrefix(srv.URL, "http://"), key, false, collectSink(&buf))
	if !ok {
		t.Fatal("large slow body must complete — no whole-request timeout")
	}
	if n != int64(len(chunk)*3) {
		t.Errorf("streamed %d bytes, want %d", n, len(chunk)*3)
	}
}

func TestPeerServesBlockKeys(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	// Create a block key for a parquet block.
	key := BlockKey("http://s3/bucket/f.parquet", 3, 8<<20)
	blockContent := "block-content"

	// Store the block.
	if _, err := store.PutStream(key, strings.NewReader(blockContent)); err != nil {
		t.Fatalf("PutStream: %v", err)
	}

	// Create a proxy with the store and no peer manager.
	p := NewCacheProxy(store, nil, []string{})

	// HandlePeerHas must recognize the block key and return 200.
	hasReq := httptest.NewRequest(http.MethodGet, "/peer/has?key="+key, nil)
	hasW := httptest.NewRecorder()
	p.HandlePeerHas(hasW, hasReq)
	if hasW.Code != http.StatusOK {
		t.Fatalf("HandlePeerHas(%s) = %d, want 200", key, hasW.Code)
	}

	// HandlePeerGet must stream the content and return 200.
	getReq := httptest.NewRequest(http.MethodGet, "/peer/get?key="+key, nil)
	getW := httptest.NewRecorder()
	p.HandlePeerGet(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("HandlePeerGet: %d, want 200", getW.Code)
	}
	if getW.Body.String() != blockContent {
		t.Fatalf("HandlePeerGet body = %q, want %q", getW.Body.String(), blockContent)
	}
}

// TestHandlePeerHasAnswersAcceptedWhenInFlight locks in the cluster-wide
// single-flight contract: a peer asking about a key we are ALREADY fetching
// must hear 202, not 404, so it waits on our fill instead of issuing a
// duplicate origin request for the same bytes.
func TestHandlePeerHasAnswersAcceptedWhenInFlight(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	p := NewCacheProxy(store, nil, []string{})
	key := strings.Repeat("7", 64)

	release := make(chan struct{})
	fetched := make(chan struct{}, 1)
	go func() {
		_, _ = p.flights.Do(key, func() (fetchResult, error) {
			fetched <- struct{}{}
			<-release
			return fetchResult{}, nil
		})
	}()
	<-fetched // the flight is now registered

	req := httptest.NewRequest(http.MethodGet, "/cache/has?key="+key, nil)
	w := httptest.NewRecorder()
	p.HandlePeerHas(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("HandlePeerHas on an in-flight key = %d, want 202", w.Code)
	}
	close(release)

	// Once the fill completes with an entry on disk, the answer is 200.
	if _, err := store.PutStream(key, strings.NewReader("filled")); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	p.HandlePeerHas(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("HandlePeerHas on a stored key = %d, want 200", w.Code)
	}
}

// TestHandlePeerGetFlightWaitsForFill: flight=1 on a missing-but-in-flight
// key must block until the fill lands and then serve those bytes, not 404.
func TestHandlePeerGetFlightWaitsForFill(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	p := NewCacheProxy(store, nil, []string{})
	key := strings.Repeat("8", 64)
	content := "filled-by-peer"

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	go func() {
		_, _ = p.flights.Do(key, func() (fetchResult, error) {
			started <- struct{}{}
			<-release
			return fetchResult{}, nil
		})
	}()
	<-started

	done := make(chan struct{})
	var getW *httptest.ResponseRecorder
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodGet, "/cache/get?key="+key+"&flight=1", nil)
		getW = httptest.NewRecorder()
		p.HandlePeerGet(getW, req)
	}()

	// Let the waiter start blocking, then land the fill.
	time.Sleep(100 * time.Millisecond)
	if _, err := store.PutStream(key, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flight=1 /cache/get did not unblock after the fill landed")
	}
	if getW.Code != http.StatusOK {
		t.Fatalf("flight wait: status = %d, want 200", getW.Code)
	}
	if getW.Body.String() != content {
		t.Fatalf("flight wait: body = %q, want %q", getW.Body.String(), content)
	}
}

// TestHandlePeerGetFlightFailsFastWhenFillDies: flight=1 must NOT hang when
// the in-flight fill errors out without producing an entry — no fill, no
// claim, a prompt 404 so the requester can go to the origin.
func TestHandlePeerGetFlightFailsFastWhenFillDies(t *testing.T) {
	store, err := NewDiskCache(t.TempDir(), 80)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	p := NewCacheProxy(store, nil, []string{})
	key := strings.Repeat("6", 64)

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	go func() {
		_, _ = p.flights.Do(key, func() (fetchResult, error) {
			started <- struct{}{}
			<-release
			return fetchResult{}, fmt.Errorf("origin denied") // fill bombs, nothing stored
		})
	}()
	<-started

	waitStart := time.Now()
	req := httptest.NewRequest(http.MethodGet, "/cache/get?key="+key+"&flight=1", nil)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.HandlePeerGet(w, req)
	}()

	time.Sleep(100 * time.Millisecond)
	close(release) // fill dies now

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flight=1 /cache/get hung after the fill failed")
	}
	if elapsed := time.Since(waitStart); elapsed > peerFillWait {
		t.Fatalf("flight wait outlived the fill's death by %v", elapsed)
	}
	if w.Code != http.StatusNotFound {
		t.Fatalf("dead fill: status = %d, want 404", w.Code)
	}
}
