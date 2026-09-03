package main

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

// parseAbsoluteRange parses a Range header of the exact form "bytes=start-end"
// (both bounds present, start <= end). Every other shape — suffix ("bytes=-N"),
// open-ended ("bytes=N-"), multi-range, empty — returns ok=false and the
// caller falls back to the legacy exact-range cache path. DuckDB httpfs knows
// file sizes up front (it HEADs first) and only issues absolute ranges, so the
// fallback is expected to be cold-path only.
func parseAbsoluteRange(rangeHeader string) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(rangeHeader, "bytes=")
	if !found {
		return 0, 0, false
	}
	lo, hi, found := strings.Cut(spec, "-")
	if !found || lo == "" || hi == "" || strings.Contains(hi, ",") {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	end, err = strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

// parsePartialContentRange parses the response form
// "bytes start-end/completeLength". Block mode requires a known complete
// length so it can distinguish a legitimate short tail from a truncated body.
func parsePartialContentRange(header string) (start, end, completeLength int64, ok bool) {
	spec, found := strings.CutPrefix(header, "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	selected, total, found := strings.Cut(spec, "/")
	if !found || total == "" || total == "*" {
		return 0, 0, 0, false
	}
	lo, hi, found := strings.Cut(selected, "-")
	if !found || lo == "" || hi == "" {
		return 0, 0, 0, false
	}
	start, err := strconv.ParseInt(lo, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, 0, false
	}
	end, err = strconv.ParseInt(hi, 10, 64)
	if err != nil || end < start {
		return 0, 0, 0, false
	}
	completeLength, err = strconv.ParseInt(total, 10, 64)
	if err != nil || completeLength <= end {
		return 0, 0, 0, false
	}
	return start, end, completeLength, true
}

// parseUnsatisfiedContentRange parses the 416 response form "bytes */N".
func parseUnsatisfiedContentRange(header string) (completeLength int64, ok bool) {
	total, found := strings.CutPrefix(header, "bytes */")
	if !found || total == "" {
		return 0, false
	}
	completeLength, err := strconv.ParseInt(total, 10, 64)
	if err != nil || completeLength < 0 {
		return 0, false
	}
	return completeLength, true
}

// blockSpan returns the inclusive index range of blocks covering [start, end].
func blockSpan(start, end, blockSize int64) (firstIdx, lastIdx int64) {
	return start / blockSize, end / blockSize
}

// BlockKey computes the cache key for one block of an object. blockSize is
// part of the key so a block-size config change can never serve a
// wrong-sized entry — old-size entries simply become unreachable and age out.
// scope is the SigV4 access key ID (see TenantScope), so two tenants reading
// the same object URL never share a block.
//
// Hash input format: scope + "\x00" + url + "|blk|" + idx + "|" + blockSize.
// The NUL separator makes the scope boundary unambiguous, same as CacheKey.
func BlockKey(scope, url string, blockIdx, blockSize int64) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s|blk|%d|%d", scope, url, blockIdx, blockSize)
	return fmt.Sprintf("%x", h.Sum(nil))
}
