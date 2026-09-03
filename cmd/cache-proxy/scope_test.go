package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestTenantScope(t *testing.T) {
	cases := []struct {
		name string
		auth string
		want string
	}{
		{
			name: "sigv4 header",
			auth: "AWS4-HMAC-SHA256 Credential=ASIAEXAMPLE123/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc",
			want: "ASIAEXAMPLE123",
		},
		{
			name: "static credentials",
			auth: "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260101/eu-west-1/s3/aws4_request, SignedHeaders=host, Signature=def",
			want: "AKIAEXAMPLE",
		},
		{name: "no header", auth: "", want: ""},
		{name: "not sigv4", auth: "Bearer token", want: ""},
		{name: "algorithm without credential", auth: "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc", want: ""},
		{name: "credential without scope path", auth: "AWS4-HMAC-SHA256 Credential=ASIAEXAMPLE123", want: ""},
		{name: "empty access key id", auth: "AWS4-HMAC-SHA256 Credential=/20260101/us-east-1/s3/aws4_request", want: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "http://s3/bucket/f", nil)
			if c.auth != "" {
				r.Header.Set("Authorization", c.auth)
			}
			if got := TenantScope(r); got != c.want {
				t.Errorf("TenantScope(%q) = %q, want %q", c.auth, got, c.want)
			}
		})
	}
}

// TestCacheKeyTenantScope: the tenant scope is part of the cache key. Two
// access key IDs reading the same URL+range must produce different keys, and
// unsigned requests (empty scope) must share one namespace.
func TestCacheKeyTenantScope(t *testing.T) {
	const url = "http://s3/bucket/org-a/data.parquet"
	const rng = "bytes=0-1023"
	a := CacheKey("ASIAAAAA", url, rng)
	b := CacheKey("ASIABBBB", url, rng)
	if a == b {
		t.Fatal("different access key IDs must produce different cache keys")
	}
	if a != CacheKey("ASIAAAAA", url, rng) {
		t.Fatal("CacheKey must be deterministic for a fixed scope")
	}
	if CacheKey("", url, rng) == a || CacheKey("", url, rng) == b {
		t.Fatal("unsigned requests must not collide with a signed tenant namespace")
	}
	if CacheKey("", url, rng) != CacheKey("", url, rng) {
		t.Fatal("unsigned requests must share one cache namespace")
	}

	if BlockKey("ASIAAAAA", url, 0, 8<<20) == BlockKey("ASIABBBB", url, 0, 8<<20) {
		t.Fatal("different access key IDs must produce different block keys")
	}
	if BlockKey("", url, 0, 8<<20) != BlockKey("", url, 0, 8<<20) {
		t.Fatal("unsigned requests must share one block namespace")
	}
}

func sigv4Header(accessKeyID string) string {
	return "AWS4-HMAC-SHA256 Credential=" + accessKeyID + "/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"
}

// TestHandleProxyTenantIsolation: a warm cache entry for tenant A must never
// be served to tenant B. Same URL, same Range, different SigV4 access key
// IDs: B's request must MISS and hit the origin, not receive A's bytes from
// cache.
func TestHandleProxyTenantIsolation(t *testing.T) {
	proxy := newTestProxy(t)

	var originCalls atomic.Int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tenant-shared-object-bytes"))
	})

	target := originURL + "/bucket/org-a/data.parquet"
	rangeHeader := "bytes=0-24"

	// Tenant A warms the cache.
	rec := doForwardProxyRequest(proxy, http.MethodGet, target, http.Header{
		"Range":         []string{rangeHeader},
		"Authorization": []string{sigv4Header("ASIATENANTAAAAA")},
	})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant A: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls after tenant A = %d, want 1", got)
	}

	// Tenant A again: cache hit, no origin call.
	rec = doForwardProxyRequest(proxy, http.MethodGet, target, http.Header{
		"Range":         []string{rangeHeader},
		"Authorization": []string{sigv4Header("ASIATENANTAAAAA")},
	})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant A repeat: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls after tenant A repeat = %d, want 1 (cache hit)", got)
	}

	// Tenant B requests the same URL+range. It must MISS and go to origin —
	// under no circumstances may it be served tenant A's cached entry.
	rec = doForwardProxyRequest(proxy, http.MethodGet, target, http.Header{
		"Range":         []string{rangeHeader},
		"Authorization": []string{sigv4Header("ASIATENANTBBBBB")},
	})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant B: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 2 {
		t.Fatalf("origin calls after tenant B = %d, want 2 (B must miss A's entry)", got)
	}

	// Both tenants now hold independent entries: a repeat from each hits its
	// own namespace and the origin sees no further calls.
	for _, akid := range []string{"ASIATENANTAAAAA", "ASIATENANTBBBBB"} {
		rec = doForwardProxyRequest(proxy, http.MethodGet, target, http.Header{
			"Range":         []string{rangeHeader},
			"Authorization": []string{sigv4Header(akid)},
		})
		if rec.Code != http.StatusPartialContent {
			t.Fatalf("%s repeat: status = %d, want 206", akid, rec.Code)
		}
	}
	if got := originCalls.Load(); got != 2 {
		t.Fatalf("origin calls after warm repeats = %d, want 2", got)
	}
}

// TestHandleProxyUnsignedRequestsShareCache: requests without a SigV4
// Authorization header share one cache namespace (correct for public
// objects).
func TestHandleProxyUnsignedRequestsShareCache(t *testing.T) {
	proxy := newTestProxy(t)

	var originCalls atomic.Int32
	_, originURL := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("public-object-bytes"))
	})

	target := originURL + "/bucket/public/data.parquet"
	headers := http.Header{"Range": []string{"bytes=0-18"}}

	if rec := doForwardProxyRequest(proxy, http.MethodGet, target, headers); rec.Code != http.StatusPartialContent {
		t.Fatalf("first unsigned: status = %d, want 206", rec.Code)
	}
	if rec := doForwardProxyRequest(proxy, http.MethodGet, target, headers); rec.Code != http.StatusPartialContent {
		t.Fatalf("second unsigned: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want 1 (unsigned requests share one entry)", got)
	}
}

// TestBlockModeTenantIsolation: the block-aligned path keys blocks by tenant
// scope too. Tenant B reading the same object blocks as a warm tenant A must
// miss and fetch its own span from the origin.
func TestBlockModeTenantIsolation(t *testing.T) {
	const blockSize = 1024
	body := make([]byte, 4*blockSize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		serveSyntheticRanged(w, r, body)
	}))
	defer origin.Close()

	p, _ := newBlockProxy(t, origin, blockSize)
	p.blockMode = true
	target := origin.URL + "/bucket/org-a/f.parquet"

	get := func(akid string) *httptest.ResponseRecorder {
		headers := http.Header{"Range": []string{"bytes=0-2047"}}
		if akid != "" {
			headers.Set("Authorization", sigv4Header(akid))
		}
		return doForwardProxyRequest(p, http.MethodGet, target, headers)
	}

	if rec := get("ASIATENANTAAAAA"); rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant A: status = %d, want 206 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls after tenant A = %d, want 1", got)
	}
	// Tenant A repeat: warm hit, no origin call.
	if rec := get("ASIATENANTAAAAA"); rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant A repeat: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 1 {
		t.Fatalf("origin calls after tenant A repeat = %d, want 1 (cache hit)", got)
	}
	// Tenant B: same URL, same range, same blocks — but a different scope, so
	// it must fetch from the origin rather than adopt A's cached blocks.
	if rec := get("ASIATENANTBBBBB"); rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant B: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 2 {
		t.Fatalf("origin calls after tenant B = %d, want 2 (B must miss A's blocks)", got)
	}
	if rec := get("ASIATENANTBBBBB"); rec.Code != http.StatusPartialContent {
		t.Fatalf("tenant B repeat: status = %d, want 206", rec.Code)
	}
	if got := originCalls.Load(); got != 2 {
		t.Fatalf("origin calls after tenant B repeat = %d, want 2", got)
	}
}
