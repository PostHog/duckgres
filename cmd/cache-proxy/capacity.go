package main

import "math"

const (
	cacheDiskReservePercent int64 = 5
	cacheEntryFillPercent   int64 = 90

	// cacheMetadataEntryLimit is the hard metadata guardrail for a future
	// derived entry limit. PR 1 intentionally continues to use the existing
	// one-million-entry compatibility limit for admission.
	cacheMetadataEntryLimit int64 = 10_000_000

	maxSummaryMemoryBytes int64 = 1 << 30
)

// diskCapacity is the cache capacity derived from a single filesystem sample.
// All values are bytes. ReclaimableBytes includes valid, committed cache files
// because the cache can evict those files to make room again after a restart.
type diskCapacity struct {
	DiskTargetBytes  int64
	ReserveBytes     int64
	ReclaimableBytes int64
	ByteCeiling      int64
}

// bloomCapacity describes the eventual dynamic Bloom-filter layout. The
// current fixed layout remains in use until the wire-format migration.
type bloomCapacity struct {
	DesignEntries int64
	BitCount      uint64
	Hashes        uint8
}

// deriveDiskCapacity computes the byte ceiling without looking at the
// filesystem. Inputs below zero are treated as zero and the percent is bounded
// to [0, 100], so callers can use the result safely even when an input source
// is malformed.
func deriveDiskCapacity(totalBytes, freeBytes, ownedCacheBytes int64, maxPercent int) diskCapacity {
	totalBytes = nonNegative(totalBytes)
	freeBytes = nonNegative(freeBytes)
	ownedCacheBytes = nonNegative(ownedCacheBytes)

	percent := int64(maxPercent)
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	target := percentageOf(totalBytes, percent)
	reserve := percentageOf(totalBytes, cacheDiskReservePercent)
	reclaimable := saturatingAdd(freeBytes, ownedCacheBytes)
	if reclaimable <= reserve {
		reclaimable = 0
	} else {
		reclaimable -= reserve
	}

	return diskCapacity{
		DiskTargetBytes:  target,
		ReserveBytes:     reserve,
		ReclaimableBytes: reclaimable,
		ByteCeiling:      min(target, reclaimable),
	}
}

// effectiveCacheEntryBytes is a conservative planning estimate. It is not a
// strict per-entry allocation guarantee; the metadata guardrail remains the
// hard safety bound for unexpectedly small files.
func effectiveCacheEntryBytes(blockSizeBytes int64) int64 {
	if blockSizeBytes <= 0 {
		return 0
	}
	effective := percentageOf(blockSizeBytes, cacheEntryFillPercent)
	if effective == 0 {
		return 1
	}
	return effective
}

// deriveCacheEntryCeiling returns the future disk-derived metadata target,
// capped by the hard guardrail. It does not change the active 1M admission cap
// in this compatibility release.
func deriveCacheEntryCeiling(cacheByteCeiling, blockSizeBytes int64) int64 {
	effectiveEntryBytes := effectiveCacheEntryBytes(blockSizeBytes)
	if cacheByteCeiling <= 0 || effectiveEntryBytes <= 0 {
		return 0
	}
	return min(ceilDiv(cacheByteCeiling, effectiveEntryBytes), cacheMetadataEntryLimit)
}

// deriveBloomCapacity sizes the future dynamic local Bloom filter from stable
// disk target capacity, not currently-free capacity, so temporary external
// disk pressure does not resize the filter.
func deriveBloomCapacity(diskTargetBytes, blockSizeBytes int64) bloomCapacity {
	designEntries := deriveCacheEntryCeiling(diskTargetBytes, blockSizeBytes)
	if designEntries == 0 {
		return bloomCapacity{}
	}
	bits, hashes := bloomParams(int(designEntries))
	return bloomCapacity{DesignEntries: designEntries, BitCount: bits, Hashes: hashes}
}

// deriveSummaryMemoryLimit is the fixed platform guardrail for future dynamic
// summary admission: no more than one GiB or twenty percent of GOMEMLIMIT.
func deriveSummaryMemoryLimit(goMemLimitBytes int64) int64 {
	if goMemLimitBytes <= 0 {
		return 0
	}
	return min(goMemLimitBytes/5, maxSummaryMemoryBytes)
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func percentageOf(value, percent int64) int64 {
	if value <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return value
	}
	return value/100*percent + value%100*percent/100
}

func saturatingAdd(left, right int64) int64 {
	if left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func ceilDiv(numerator, denominator int64) int64 {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	quotient := numerator / denominator
	if numerator%denominator != 0 {
		quotient++
	}
	return quotient
}
