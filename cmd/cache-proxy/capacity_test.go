package main

import (
	"math"
	"testing"
)

func TestDeriveDiskCapacityAccountsForOwnedCacheBytes(t *testing.T) {
	capacity := deriveDiskCapacity(1000, 200, 800, 80)
	if capacity.DiskTargetBytes != 800 {
		t.Fatalf("disk target = %d, want 800", capacity.DiskTargetBytes)
	}
	if capacity.ReserveBytes != 50 {
		t.Fatalf("reserve = %d, want 50", capacity.ReserveBytes)
	}
	if capacity.ReclaimableBytes != 950 {
		t.Fatalf("reclaimable = %d, want 950", capacity.ReclaimableBytes)
	}
	if capacity.ByteCeiling != 800 {
		t.Fatalf("byte ceiling = %d, want 800; committed cache bytes must remain reclaimable after restart", capacity.ByteCeiling)
	}
}

func TestDeriveDiskCapacityRespectsReserveAndInvalidInputs(t *testing.T) {
	cases := []struct {
		name                          string
		total, free, owned            int64
		percent                       int
		wantTarget, wantReserve, want int64
	}{
		{name: "external use reduces ceiling", total: 1000, free: 10, owned: 800, percent: 80, wantTarget: 800, wantReserve: 50, want: 760},
		{name: "reserve consumes all reclaimable space", total: 1000, free: 20, owned: 20, percent: 80, wantTarget: 800, wantReserve: 50, want: 0},
		{name: "invalid inputs", total: -1, free: 20, owned: 20, percent: 80, wantTarget: 0, wantReserve: 0, want: 0},
		{name: "percent is bounded", total: 1000, free: 1000, owned: 0, percent: 101, wantTarget: 1000, wantReserve: 50, want: 950},
		{name: "negative percent", total: 1000, free: 1000, owned: 0, percent: -1, wantTarget: 0, wantReserve: 50, want: 0},
		{name: "addition does not overflow", total: math.MaxInt64, free: math.MaxInt64, owned: math.MaxInt64, percent: 80, wantTarget: 7_378_697_629_483_820_645, wantReserve: math.MaxInt64 / 100 * 5, want: 7_378_697_629_483_820_645},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveDiskCapacity(tt.total, tt.free, tt.owned, tt.percent)
			if got.DiskTargetBytes != tt.wantTarget || got.ReserveBytes != tt.wantReserve || got.ByteCeiling != tt.want {
				t.Fatalf("deriveDiskCapacity(%d, %d, %d, %d) = %+v, want target/reserve/ceiling %d/%d/%d", tt.total, tt.free, tt.owned, tt.percent, got, tt.wantTarget, tt.wantReserve, tt.want)
			}
		})
	}
}

func TestDerivedEntryAndBloomCapacity(t *testing.T) {
	const oneMiB = 1 << 20
	cases := []struct {
		name        string
		cacheBytes  int64
		wantEntries int64
	}{
		{name: "2.07 TiB", cacheBytes: 2_275_989_069_496, wantEntries: 2_411_726},
		{name: "2.76 TiB", cacheBytes: 3_034_652_092_662, wantEntries: 3_215_635},
		{name: "4.15 TiB", cacheBytes: 4_562_973_255_270, wantEntries: 4_835_103},
		{name: "8.29 TiB", cacheBytes: 9_114_951_394_263, wantEntries: 9_658_555},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := deriveCacheEntryCeiling(tt.cacheBytes, oneMiB); got != tt.wantEntries {
				t.Fatalf("entry ceiling = %d, want %d", got, tt.wantEntries)
			}
			bloom := deriveBloomCapacity(tt.cacheBytes, oneMiB)
			if bloom.DesignEntries != tt.wantEntries || bloom.BitCount == 0 || bloom.Hashes == 0 {
				t.Fatalf("bloom capacity = %+v, want design entries %d and non-zero layout", bloom, tt.wantEntries)
			}
		})
	}
}

func TestDerivedEntryAndBloomCapacityBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name               string
		blockSize          int64
		wantEffectiveBytes int64
	}{
		{name: "1 MiB blocks", blockSize: 1 << 20, wantEffectiveBytes: 943718},
		{name: "8 MiB blocks", blockSize: 8 << 20, wantEffectiveBytes: 7549747},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveCacheEntryBytes(tt.blockSize); got != tt.wantEffectiveBytes {
				t.Fatalf("effective entry bytes = %d, want %d", got, tt.wantEffectiveBytes)
			}
			if got := deriveCacheEntryCeiling(2*tt.wantEffectiveBytes, tt.blockSize); got != 2 {
				t.Fatalf("entry ceiling = %d, want 2", got)
			}
			bloom := deriveBloomCapacity(2*tt.wantEffectiveBytes, tt.blockSize)
			if bloom.DesignEntries != 2 || bloom.BitCount == 0 || bloom.Hashes == 0 {
				t.Fatalf("bloom capacity = %+v, want design entries 2 and non-zero layout", bloom)
			}
		})
	}

	// A deliberately small input makes the ceil behavior obvious: a ten-byte
	// ceiling needs two entries when a ten-byte block has nine effective bytes.
	if got := deriveCacheEntryCeiling(10, 10); got != 2 {
		t.Fatalf("small entry ceiling rounds up to %d, want 2", got)
	}
	if got := deriveCacheEntryCeiling(math.MaxInt64, 1<<20); got != cacheMetadataEntryLimit {
		t.Fatalf("entry ceiling = %d, want metadata guardrail %d", got, cacheMetadataEntryLimit)
	}
	if got := deriveCacheEntryCeiling(100, 0); got != 0 {
		t.Fatalf("entry ceiling with invalid block size = %d, want 0", got)
	}

	bloom := deriveBloomCapacity(80*(1<<20), 1<<20)
	if bloom.DesignEntries <= 0 || bloom.DesignEntries > cacheMetadataEntryLimit || bloom.BitCount == 0 || bloom.Hashes == 0 {
		t.Fatalf("invalid bloom capacity: %+v", bloom)
	}
}

func TestDerivedSummaryMemoryLimit(t *testing.T) {
	cases := []struct {
		goMemLimit int64
		want       int64
	}{
		{goMemLimit: 0, want: 0},
		{goMemLimit: 5 << 30, want: 1 << 30},
		{goMemLimit: 2 << 30, want: 429496729},
		{goMemLimit: math.MaxInt64, want: 1 << 30},
	}
	for _, tt := range cases {
		if got := deriveSummaryMemoryLimit(tt.goMemLimit); got != tt.want {
			t.Errorf("deriveSummaryMemoryLimit(%d) = %d, want %d", tt.goMemLimit, got, tt.want)
		}
	}
}
