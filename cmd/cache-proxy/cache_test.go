package main

import (
	"io"
	"strings"
	"testing"
)

func TestIsValidCacheKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{strings.Repeat("a", 64), true},
		{strings.Repeat("f", 64), true},
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true},
		// Wrong length.
		{strings.Repeat("a", 63), false},
		{strings.Repeat("a", 65), false},
		{"", false},
		// Upper-case — CacheKey uses %x (lowercase); reject upper.
		{strings.Repeat("A", 64), false},
		// Path traversal attempts.
		{"../../etc/passwd", false},
		{strings.Repeat("a", 60) + "/../x", false},
		// Non-hex chars.
		{strings.Repeat("g", 64), false},
		{strings.Repeat("z", 64), false},
	}
	for _, c := range cases {
		if got := IsValidCacheKey(c.key); got != c.want {
			t.Errorf("IsValidCacheKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestCacheKeyDeterministic(t *testing.T) {
	a := CacheKey("http://s3/bucket/file.parquet", "bytes=0-1023")
	b := CacheKey("http://s3/bucket/file.parquet", "bytes=0-1023")
	if a != b {
		t.Fatalf("CacheKey not deterministic: %s != %s", a, b)
	}
	if !IsValidCacheKey(a) {
		t.Errorf("CacheKey output %q is not a valid key", a)
	}
	c := CacheKey("http://s3/bucket/file.parquet", "bytes=0-2047")
	if a == c {
		t.Fatal("different ranges produced identical keys")
	}
}

// newTestCache creates a DiskCache backed by t.TempDir() for isolation.
func newTestCache(t *testing.T) *DiskCache {
	t.Helper()
	c, err := NewDiskCache(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}
	return c
}

func TestDiskCachePutGetHas(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("a", 64)
	data := []byte("hello world")

	if c.Has(key) {
		t.Fatal("Has should be false before Put")
	}
	if _, ok := c.Get(key); ok {
		t.Fatal("Get should miss before Put")
	}
	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !c.Has(key) {
		t.Fatal("Has should be true after Put")
	}
	got, ok := c.Get(key)
	if !ok {
		t.Fatal("Get should hit after Put")
	}
	if string(got) != string(data) {
		t.Errorf("Get returned %q, want %q", got, data)
	}
}

func TestDiskCacheOpen(t *testing.T) {
	c := newTestCache(t)
	key := strings.Repeat("b", 64)
	data := []byte("streaming bytes")
	if err := c.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, size, ok := c.Open(key)
	if !ok {
		t.Fatal("Open should find entry")
	}
	defer r.Close()
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Open data = %q, want %q", got, data)
	}
}

func TestDiskCacheRejectsInvalidKey(t *testing.T) {
	c := newTestCache(t)
	bad := "../../etc/passwd"

	if c.Has(bad) {
		t.Error("Has should reject invalid key")
	}
	if _, ok := c.Get(bad); ok {
		t.Error("Get should reject invalid key")
	}
	if _, _, ok := c.Open(bad); ok {
		t.Error("Open should reject invalid key")
	}
	if err := c.Put(bad, []byte("x")); err == nil {
		t.Error("Put should reject invalid key")
	}
}

// TestDiskCacheEviction exercises LRU eviction when the cache fills up.
// We set maxBytes tiny by construction: NewDiskCache uses statfs * percent,
// so we directly mutate the cache after construction for a predictable test.
func TestDiskCacheEviction(t *testing.T) {
	c := newTestCache(t)
	// Force the eviction threshold low so we trigger it quickly.
	c.maxBytes = 100

	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
	}
	for _, k := range keys {
		if err := c.Put(k, make([]byte, 60)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	// After three 60-byte puts with maxBytes=100, the first key must have
	// been evicted (oldest lastAccess).
	if c.Has(keys[0]) {
		t.Error("oldest entry should have been evicted")
	}
	// The most recent must still be present.
	if !c.Has(keys[2]) {
		t.Error("newest entry should still be cached")
	}
}
