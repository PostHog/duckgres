package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// newPeerServer returns an httptest server exposing /cache/has and /cache/get
// for the supplied key and data. hasCallback/getCallback increment counters.
func newPeerServer(t *testing.T, key string, data []byte, hasCalls, getCalls *int32) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/cache/has", func(w http.ResponseWriter, r *http.Request) {
		if hasCalls != nil {
			atomic.AddInt32(hasCalls, 1)
		}
		if r.URL.Query().Get("key") == key {
			w.WriteHeader(http.StatusOK)
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

func TestFetchFromPeersHit(t *testing.T) {
	key := strings.Repeat("f", 64)
	data := []byte("from-peer")
	var hasCalls, getCalls int32

	addr := newPeerServer(t, key, data, &hasCalls, &getCalls)
	pm := peerManagerWith([]string{addr})

	got, from, ok := pm.FetchFromPeers(key)
	if !ok {
		t.Fatal("expected peer hit")
	}
	if string(got) != string(data) {
		t.Errorf("peer data = %q, want %q", got, data)
	}
	if from != addr {
		t.Errorf("peer addr = %q, want %q", from, addr)
	}
	if atomic.LoadInt32(&hasCalls) != 1 {
		t.Errorf("peer /cache/has calls = %d, want 1", hasCalls)
	}
	if atomic.LoadInt32(&getCalls) != 1 {
		t.Errorf("peer /cache/get calls = %d, want 1", getCalls)
	}
}

func TestFetchFromPeersMissFromAll(t *testing.T) {
	key := strings.Repeat("a", 64)
	other := strings.Repeat("b", 64)
	var hasCalls, getCalls int32

	// Seed peer with DIFFERENT key so our lookup misses.
	addr := newPeerServer(t, other, []byte("not-ours"), &hasCalls, &getCalls)
	pm := peerManagerWith([]string{addr})

	got, _, ok := pm.FetchFromPeers(key)
	if ok {
		t.Fatalf("expected miss, got %q", got)
	}
	if atomic.LoadInt32(&hasCalls) != 1 {
		t.Errorf("peer /cache/has calls = %d, want 1", hasCalls)
	}
	// On miss, /cache/get should NOT be called.
	if atomic.LoadInt32(&getCalls) != 0 {
		t.Errorf("peer /cache/get calls = %d, want 0 on miss", getCalls)
	}
}

func TestFetchFromPeersReturnsFirstHit(t *testing.T) {
	key := strings.Repeat("c", 64)
	data := []byte("winner")
	var has1, get1, has2, get2 int32

	// Two peers both have the data. We should get one successful result and
	// not block waiting for both.
	addr1 := newPeerServer(t, key, data, &has1, &get1)
	addr2 := newPeerServer(t, key, data, &has2, &get2)
	pm := peerManagerWith([]string{addr1, addr2})

	got, _, ok := pm.FetchFromPeers(key)
	if !ok {
		t.Fatal("expected peer hit from one of two peers")
	}
	if string(got) != string(data) {
		t.Errorf("peer data = %q, want %q", got, data)
	}
	// At least one peer must have been asked; both may have responded.
	totalHas := atomic.LoadInt32(&has1) + atomic.LoadInt32(&has2)
	if totalHas < 1 {
		t.Errorf("no peer /cache/has calls at all")
	}
}

func TestFetchFromPeersEmptyPeerList(t *testing.T) {
	pm := peerManagerWith(nil)
	if _, _, ok := pm.FetchFromPeers(strings.Repeat("a", 64)); ok {
		t.Error("expected miss when no peers are known")
	}
}
