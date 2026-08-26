package main

import "testing"

func TestParseAbsoluteRange(t *testing.T) {
	tests := []struct {
		name   string
		header string
		start  int64
		end    int64
		ok     bool
	}{
		{"absolute", "bytes=100-199", 100, 199, true},
		{"zero start", "bytes=0-0", 0, 0, true},
		{"large offsets", "bytes=43730341-82843457", 43730341, 82843457, true},
		{"suffix form", "bytes=-500", 0, 0, false},
		{"open ended", "bytes=100-", 0, 0, false},
		{"multi range", "bytes=0-1,5-9", 0, 0, false},
		{"empty", "", 0, 0, false},
		{"garbage", "bytes=a-b", 0, 0, false},
		{"inverted", "bytes=200-100", 0, 0, false},
		{"no prefix", "100-199", 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok := parseAbsoluteRange(tt.header)
			if ok != tt.ok || start != tt.start || end != tt.end {
				t.Fatalf("parseAbsoluteRange(%q) = (%d, %d, %v); want (%d, %d, %v)",
					tt.header, start, end, ok, tt.start, tt.end, tt.ok)
			}
		})
	}
}

func TestBlockSpan(t *testing.T) {
	const bs = 8 << 20 // 8 MiB
	tests := []struct {
		name     string
		start    int64
		end      int64
		firstIdx int64
		lastIdx  int64
	}{
		{"within first block", 0, 100, 0, 0},
		{"exact block", 0, bs - 1, 0, 0},
		{"crosses boundary", bs - 1, bs, 0, 1},
		{"multi block", 0, 3*bs - 1, 0, 2},
		{"interior", 2*bs + 5, 4*bs + 5, 2, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, last := blockSpan(tt.start, tt.end, bs)
			if first != tt.firstIdx || last != tt.lastIdx {
				t.Fatalf("blockSpan(%d, %d) = (%d, %d); want (%d, %d)",
					tt.start, tt.end, first, last, tt.firstIdx, tt.lastIdx)
			}
		})
	}
}

func TestBlockKey(t *testing.T) {
	k1 := BlockKey("http://s3/bucket/f.parquet", 0, 8<<20)
	k2 := BlockKey("http://s3/bucket/f.parquet", 1, 8<<20)
	k3 := BlockKey("http://s3/bucket/f.parquet", 0, 16<<20)
	if !IsValidCacheKey(k1) {
		t.Fatalf("BlockKey output %q is not a valid cache key", k1)
	}
	if k1 == k2 {
		t.Fatal("different block indexes must produce different keys")
	}
	if k1 == k3 {
		t.Fatal("different block sizes must produce different keys")
	}
	if k1 != BlockKey("http://s3/bucket/f.parquet", 0, 8<<20) {
		t.Fatal("BlockKey must be deterministic")
	}
	if k1 == CacheKey("http://s3/bucket/f.parquet", "bytes=0-100") {
		t.Fatal("block keys must not collide with legacy keys")
	}
}
