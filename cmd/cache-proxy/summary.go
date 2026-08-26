package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Summary data deliberately contains only opaque cache-key digests. It is a
// best-effort hint: rejecting it or losing it must only reduce peer hits.
const (
	legacySummaryFormatVersion  = 2
	dynamicSummaryFormatVersion = 3
	// summaryFormatVersion remains the compatibility-publisher version. New
	// receivers accept both versions, while the explicit publish-format rollout
	// controls when a pod starts emitting v3.
	summaryFormatVersion    = legacySummaryFormatVersion
	cacheLayoutVersion      = 1
	defaultSummaryTTL       = 60 * time.Second
	defaultSummaryInterval  = 20 * time.Second
	summaryTargetFPR        = 0.01
	summaryBloomTargetItems = 1_000_000
	// A 10m-entry filter at 1% false-positive rate is about 11.43 MiB raw
	// and 15.24 MiB after JSON base64 encoding. Sixteen MiB is the protocol
	// cap for both fixed v2 and dynamic v3 summaries.
	maxSummaryBodyBytes            = 16 << 20
	maxSummaryResponseHeaderBytes  = 16 << 10
	maxSummaryPulls                = 4
	defaultSummaryMemoryLimitBytes = 512 << 20
	summaryPullTimeout             = 2 * time.Second
	summaryServeTimeout            = 2 * time.Second
	defaultSummaryPullCycleTimeout = 15 * time.Second
)

type summaryPublishFormat string

const (
	summaryPublishFixed   summaryPublishFormat = "fixed"
	summaryPublishDynamic summaryPublishFormat = "dynamic"
)

func parseSummaryPublishFormat(value string) (summaryPublishFormat, error) {
	switch value {
	case "", string(summaryPublishFixed):
		return summaryPublishFixed, nil
	case string(summaryPublishDynamic):
		return summaryPublishDynamic, nil
	default:
		return "", fmt.Errorf("invalid CACHE_SUMMARY_PUBLISH_FORMAT %q (want fixed or dynamic)", value)
	}
}

func fixedSummaryBloomCapacity() bloomCapacity {
	return bloomCapacity{DesignEntries: summaryBloomTargetItems, BitCount: summaryBloomBits, Hashes: summaryBloomHashes}
}

func maxAcceptedSummaryBloomCapacity() bloomCapacity {
	bits, hashes := bloomParams(int(cacheMetadataEntryLimit))
	return bloomCapacity{DesignEntries: cacheMetadataEntryLimit, BitCount: bits, Hashes: hashes}
}

func validSummaryBloomCapacity(capacity bloomCapacity) bool {
	if capacity.DesignEntries <= 0 || capacity.DesignEntries > cacheMetadataEntryLimit || capacity.BitCount < 8 || capacity.BitCount%8 != 0 || capacity.Hashes == 0 {
		return false
	}
	bits, hashes := bloomParams(int(capacity.DesignEntries))
	return capacity.BitCount == bits && capacity.Hashes == hashes
}

func summaryMemoryReserveBytes(localCapacity ...bloomCapacity) int64 {
	local := fixedSummaryBloomCapacity()
	if len(localCapacity) > 0 && validSummaryBloomCapacity(localCapacity[0]) {
		local = localCapacity[0]
	}
	localRawBytes := int64(local.BitCount / 8)
	localIndex := localRawBytes + int64(local.BitCount)*2
	maxRemoteRawBytes := int64(maxAcceptedSummaryBloomCapacity().BitCount / 8)
	// Keep room for the currently served body, the JSON encoder buffer and its
	// returned clone, plus the temporary raw-bit snapshot. Each pull similarly
	// retains the response body, the Decoder buffer and a RawMessage copy while
	// allocating the decoded bitset. Pull reserve is layout-independent because
	// a fixed-publishing receiver must already be safe to accept v3 peers during
	// a rolling deployment.
	transient := 3*int64(maxSummaryBodyBytes) + localRawBytes + int64(maxSummaryPulls)*(3*int64(maxSummaryBodyBytes)+maxRemoteRawBytes+maxSummaryResponseHeaderBytes)
	return localIndex + transient
}

func validateSummaryMemoryLimit(total int64, localCapacity ...bloomCapacity) error {
	minimum := summaryMemoryReserveBytes(localCapacity...) + int64(maxAcceptedSummaryBloomCapacity().BitCount/8)
	if total < minimum {
		return fmt.Errorf("CACHE_SUMMARY_MEMORY_LIMIT_BYTES must be at least %d", minimum)
	}
	return nil
}

func summaryRemoteMemoryBudget(total int64, localCapacity ...bloomCapacity) int {
	budget := total - summaryMemoryReserveBytes(localCapacity...)
	if budget < 0 {
		return 0
	}
	maxInt := int64(^uint(0) >> 1)
	if budget > maxInt {
		return int(maxInt)
	}
	return int(budget)
}

var (
	summaryBloomBits, summaryBloomHashes = bloomParams(summaryBloomTargetItems)
)

var (
	summaryPullsTotal                   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_pulls_total", Help: "Peer summary pulls by outcome"}, []string{"outcome"})
	summaryServesTotal                  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_serves_total", Help: "Local summary endpoint responses by outcome"}, []string{"outcome"})
	summarySelectedPeers                = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_selected_peers", Help: "Current deterministic peer subset selected for summary pulls"})
	summaryResidentCount                = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_resident_count", Help: "Current retained peer summary records, including records awaiting expiry pruning"})
	summaryValidResidentPeers           = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_valid_resident_peers", Help: "Current retained peer summaries whose advertised TTL has not expired"})
	summaryResidentBytes                = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_resident_bytes", Help: "Conservative Bloom-state memory accounting: current local index, maximum v3 transient reserve, and retained peer summaries"})
	summaryMemoryLimitBytes             = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_memory_limit_bytes", Help: "Effective derived and optionally lowered ceiling for total Bloom-state memory accounting"})
	summaryAgeSeconds                   = promauto.NewHistogram(prometheus.HistogramOpts{Name: "cache_proxy_summary_age_seconds", Help: "Age of a summary used during lookup", Buckets: prometheus.ExponentialBuckets(0.1, 2, 10)})
	summaryLookupTotal                  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_lookups_total", Help: "Summary lookup decisions"}, []string{"outcome"})
	summaryConfirmedGetsTotal           = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_confirmed_gets_total", Help: "Peer body GET attempts after exact summary-mode confirmation"}, []string{"outcome"})
	summaryBloomItems                   = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_items", Help: "Current local cache keys represented by the incremental Bloom filter"})
	summaryBloomDesignItems             = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_design_items", Help: "Entry count used to size the local Bloom filter at the target false-positive rate"})
	summaryBloomBitsGauge               = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_bits", Help: "Allocated bits in the local incremental Bloom filter"})
	summaryBloomHashesGauge             = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_hashes", Help: "Hash functions in the local incremental Bloom filter"})
	summaryBloomFPR                     = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_false_positive_ratio", Help: "Predicted local Bloom false-positive ratio"})
	summaryBloomSaturated               = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_saturated", Help: "Whether local Bloom entries exceed its 1 percent target capacity"})
	summaryBloomOccupancy               = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_bit_occupancy_ratio", Help: "Fraction of local Bloom bits currently set"})
	summaryBloomAddsTotal               = promauto.NewCounter(prometheus.CounterOpts{Name: "cache_proxy_summary_bloom_additions_total", Help: "Cache insertions applied to the incremental Bloom filter"})
	summaryBloomRemovalsTotal           = promauto.NewCounter(prometheus.CounterOpts{Name: "cache_proxy_summary_bloom_removals_total", Help: "Cache evictions applied to the incremental Bloom filter"})
	summaryBloomCounterSaturationsTotal = promauto.NewCounter(prometheus.CounterOpts{Name: "cache_proxy_summary_bloom_counter_saturations_total", Help: "Counting Bloom cells that reached uint16 saturation"})
	summaryBloomSnapshotsTotal          = promauto.NewCounter(prometheus.CounterOpts{Name: "cache_proxy_summary_bloom_snapshots_total", Help: "Immutable Bloom snapshots prepared for serving"})
	summaryBloomSnapshotBytes           = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_bloom_snapshot_bytes", Help: "Bytes in the most recent local Bloom snapshot"})
)

type peerLookupMode string

const (
	peerLookupProbe   peerLookupMode = "probe"
	peerLookupSummary peerLookupMode = "summary"
)

func parsePeerLookupMode(value string) (peerLookupMode, error) {
	if value == "" || value == string(peerLookupProbe) {
		return peerLookupProbe, nil
	}
	if value == string(peerLookupSummary) {
		return peerLookupSummary, nil
	}
	return "", fmt.Errorf("invalid CACHE_PEER_LOOKUP_MODE %q (want probe or summary)", value)
}

// cacheSummary is the complete versioned wire payload. []byte is base64 in
// JSON; no raw cache locator can be recovered from the Bloom bits.
type cacheSummary struct {
	Version     int
	Layout      int
	CreatedNS   int64
	ExpiresNS   int64
	ItemCount   int
	DesignItems int
	MBits       uint64
	Hashes      uint8
	Bits        []byte
}

type legacyCacheSummaryWire struct {
	Version   int             `json:"v"`
	Layout    int             `json:"l"`
	CreatedNS int64           `json:"c"`
	ExpiresNS int64           `json:"e"`
	MBits     uint64          `json:"m"`
	Hashes    uint8           `json:"k"`
	Bits      json.RawMessage `json:"b"`
}

type dynamicCacheSummaryWire struct {
	Version     int             `json:"v"`
	Layout      int             `json:"l"`
	CreatedNS   int64           `json:"c"`
	ExpiresNS   int64           `json:"e"`
	ItemCount   *int            `json:"n"`
	DesignItems *int            `json:"d"`
	MBits       uint64          `json:"m"`
	Hashes      uint8           `json:"k"`
	Bits        json.RawMessage `json:"b"`
}

type cacheSummaryMetadataWire struct {
	Version     int    `json:"v"`
	Layout      int    `json:"l"`
	CreatedNS   int64  `json:"c"`
	ExpiresNS   int64  `json:"e"`
	ItemCount   *int   `json:"n"`
	DesignItems *int   `json:"d"`
	MBits       uint64 `json:"m"`
	Hashes      uint8  `json:"k"`
}

type legacyCacheSummaryOutput struct {
	Version   int    `json:"v"`
	Layout    int    `json:"l"`
	CreatedNS int64  `json:"c"`
	ExpiresNS int64  `json:"e"`
	MBits     uint64 `json:"m"`
	Hashes    uint8  `json:"k"`
	Bits      []byte `json:"b"`
}

type dynamicCacheSummaryOutput struct {
	Version     int    `json:"v"`
	Layout      int    `json:"l"`
	CreatedNS   int64  `json:"c"`
	ExpiresNS   int64  `json:"e"`
	ItemCount   int    `json:"n"`
	DesignItems int    `json:"d"`
	MBits       uint64 `json:"m"`
	Hashes      uint8  `json:"k"`
	Bits        []byte `json:"b"`
}

// summaryIndex is a bounded counting Bloom filter. Counters make eviction
// safe: a bit is cleared only when no remaining key uses it. A saturated
// counter is intentionally never cleared, which can only add false positives.
type summaryIndex struct {
	mu          sync.RWMutex
	bits        []byte
	counters    []uint16
	bitCount    uint64
	hashes      uint8
	targetItems int
	itemCount   int
	setBits     uint64
}

func newSummaryIndexWithParams(bitCount uint64, hashes uint8, targetItems int) *summaryIndex {
	if bitCount < 8 || bitCount%8 != 0 || hashes == 0 || targetItems <= 0 {
		panic("invalid incremental Bloom parameters")
	}
	i := &summaryIndex{
		bits:        make([]byte, bitCount/8),
		counters:    make([]uint16, bitCount),
		bitCount:    bitCount,
		hashes:      hashes,
		targetItems: targetItems,
	}
	return i
}

func (i *summaryIndex) Add(key string) {
	if !IsValidCacheKey(key) {
		return
	}
	i.mu.Lock()
	bloomHashes(key, i.bitCount, i.hashes, func(bit uint64) {
		if i.counters[bit] < ^uint16(0) {
			if i.counters[bit] == ^uint16(0)-1 {
				summaryBloomCounterSaturationsTotal.Inc()
			}
			i.counters[bit]++
		}
		if i.bits[bit/8]&(1<<(bit%8)) == 0 {
			i.setBits++
		}
		i.bits[bit/8] |= 1 << (bit % 8)
	})
	i.itemCount++
	summaryBloomAddsTotal.Inc()
	i.mu.Unlock()
}

func (i *summaryIndex) Remove(key string) {
	if !IsValidCacheKey(key) {
		return
	}
	i.mu.Lock()
	bloomHashes(key, i.bitCount, i.hashes, func(bit uint64) {
		// Saturated counters stay set. Clearing them could cause a false
		// negative after more than MaxUint16 colliding additions.
		if i.counters[bit] > 0 && i.counters[bit] < ^uint16(0) {
			i.counters[bit]--
			if i.counters[bit] == 0 {
				i.bits[bit/8] &^= 1 << (bit % 8)
				i.setBits--
			}
		}
	})
	if i.itemCount > 0 {
		i.itemCount--
	}
	summaryBloomRemovalsTotal.Inc()
	i.mu.Unlock()
}

func (i *summaryIndex) Snapshot() (int, []byte) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	i.updateMetricsLocked()
	summaryBloomSnapshotsTotal.Inc()
	summaryBloomSnapshotBytes.Set(float64(len(i.bits)))
	return i.itemCount, append([]byte(nil), i.bits...)
}

func (i *summaryIndex) updateMetricsLocked() {
	summaryBloomItems.Set(float64(i.itemCount))
	summaryBloomDesignItems.Set(float64(i.targetItems))
	summaryBloomBitsGauge.Set(float64(i.bitCount))
	summaryBloomHashesGauge.Set(float64(i.hashes))
	summaryBloomFPR.Set(bloomFalsePositiveRate(i.itemCount, i.bitCount, i.hashes))
	summaryBloomOccupancy.Set(float64(i.setBits) / float64(i.bitCount))
	if i.itemCount > i.targetItems {
		summaryBloomSaturated.Set(1)
	} else {
		summaryBloomSaturated.Set(0)
	}
}

func bloomFalsePositiveRate(items int, bitCount uint64, hashes uint8) float64 {
	if items <= 0 || bitCount == 0 || hashes == 0 {
		return 0
	}
	set := -math.Expm1(-float64(hashes) * float64(items) / float64(bitCount))
	return math.Pow(set, float64(hashes))
}

func bloomParams(items int) (uint64, uint8) {
	m := uint64(math.Ceil(-float64(items) * math.Log(summaryTargetFPR) / (math.Ln2 * math.Ln2)))
	if m < 8 {
		m = 8
	}
	m = (m + 7) &^ 7
	k := uint8(math.Round(float64(m) / math.Max(float64(items), 1) * math.Ln2))
	if k == 0 {
		k = 1
	}
	if k > 32 {
		k = 32
	}
	return m, k
}

func (s *cacheSummary) hashes(key string, fn func(uint64)) {
	bloomHashes(key, s.MBits, s.Hashes, fn)
}

func bloomHashes(key string, bits uint64, hashes uint8, fn func(uint64)) {
	d := sha256.Sum256([]byte(key))
	h1, h2 := binary.LittleEndian.Uint64(d[:8]), binary.LittleEndian.Uint64(d[8:16])
	for i := uint8(0); i < hashes; i++ {
		fn((h1 + uint64(i)*h2) % bits)
	}
}

func (s *cacheSummary) Contains(key string) bool {
	if !IsValidCacheKey(key) || s.MBits == 0 || len(s.Bits) != int(s.MBits/8) {
		return false
	}
	ok := true
	s.hashes(key, func(bit uint64) {
		if s.Bits[bit/8]&(1<<(bit%8)) == 0 {
			ok = false
		}
	})
	return ok
}
func (s *cacheSummary) MarshalBinary() ([]byte, error) {
	var value any
	switch s.Version {
	case legacySummaryFormatVersion:
		value = legacyCacheSummaryOutput{
			Version: s.Version, Layout: s.Layout, CreatedNS: s.CreatedNS, ExpiresNS: s.ExpiresNS,
			MBits: s.MBits, Hashes: s.Hashes, Bits: s.Bits,
		}
	case dynamicSummaryFormatVersion:
		value = dynamicCacheSummaryOutput{
			Version: s.Version, Layout: s.Layout, CreatedNS: s.CreatedNS, ExpiresNS: s.ExpiresNS,
			ItemCount: s.ItemCount, DesignItems: s.DesignItems, MBits: s.MBits, Hashes: s.Hashes, Bits: s.Bits,
		}
	default:
		return nil, errors.New("incompatible summary")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(b) > maxSummaryBodyBytes {
		return nil, errors.New("summary exceeds wire limit")
	}
	return b, nil
}

func summaryFromIncrementalBits(bitCount uint64, hashes uint8, bits []byte, now time.Time, ttl time.Duration) *cacheSummary {
	return &cacheSummary{Version: legacySummaryFormatVersion, Layout: cacheLayoutVersion, CreatedNS: now.UnixNano(), ExpiresNS: now.Add(ttl).UnixNano(), MBits: bitCount, Hashes: hashes, Bits: bits}
}

func newIncrementalCacheSummary(bits []byte, now time.Time, ttl time.Duration) (*cacheSummary, error) {
	if ttl <= 0 || len(bits) != int(summaryBloomBits/8) {
		return nil, errors.New("invalid incremental summary inputs")
	}
	return summaryFromIncrementalBits(summaryBloomBits, summaryBloomHashes, bits, now, ttl), nil
}

func newDynamicCacheSummary(items int, capacity bloomCapacity, bits []byte, now time.Time, ttl time.Duration) (*cacheSummary, error) {
	if ttl <= 0 || items < 0 || items > int(cacheMetadataEntryLimit) || !validSummaryBloomCapacity(capacity) || len(bits) != int(capacity.BitCount/8) {
		return nil, errors.New("invalid dynamic summary inputs")
	}
	return &cacheSummary{
		Version: dynamicSummaryFormatVersion, Layout: cacheLayoutVersion,
		CreatedNS: now.UnixNano(), ExpiresNS: now.Add(ttl).UnixNano(),
		ItemCount: items, DesignItems: int(capacity.DesignEntries),
		MBits: capacity.BitCount, Hashes: capacity.Hashes, Bits: bits,
	}, nil
}

func parseCacheSummary(body []byte, now time.Time) (*cacheSummary, error) {
	if len(body) == 0 || len(body) > maxSummaryBodyBytes {
		return nil, errors.New("invalid summary body size")
	}
	// Preflight scalar metadata without materializing b. This validates the
	// claimed layout before the strict decoder allocates its bounded RawMessage
	// copy and before decodeSummaryBits allocates the raw Bloom bitset.
	var metadata cacheSummaryMetadataWire
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, errors.New("invalid summary encoding")
	}
	if !validSummaryTimestamps(metadata.CreatedNS, metadata.ExpiresNS, now) {
		return nil, errors.New("invalid summary timestamps")
	}
	var s cacheSummary
	switch metadata.Version {
	case legacySummaryFormatVersion:
		if metadata.Layout != cacheLayoutVersion || metadata.MBits != summaryBloomBits || metadata.Hashes != summaryBloomHashes {
			return nil, errors.New("invalid bloom parameters")
		}
		var wire legacyCacheSummaryWire
		if err := decodeStrictSummaryWire(body, &wire); err != nil {
			return nil, err
		}
		bits, err := decodeSummaryBits(wire.Bits, wire.MBits)
		if err != nil {
			return nil, err
		}
		s = cacheSummary{
			Version: wire.Version, Layout: wire.Layout, CreatedNS: wire.CreatedNS, ExpiresNS: wire.ExpiresNS,
			DesignItems: summaryBloomTargetItems, MBits: wire.MBits, Hashes: wire.Hashes, Bits: bits,
		}
	case dynamicSummaryFormatVersion:
		if metadata.Layout != cacheLayoutVersion || metadata.ItemCount == nil || metadata.DesignItems == nil || *metadata.ItemCount < 0 || *metadata.ItemCount > int(cacheMetadataEntryLimit) {
			return nil, errors.New("invalid dynamic summary dimensions")
		}
		capacity := bloomCapacity{DesignEntries: int64(*metadata.DesignItems), BitCount: metadata.MBits, Hashes: metadata.Hashes}
		if !validSummaryBloomCapacity(capacity) {
			return nil, errors.New("invalid bloom parameters")
		}
		var wire dynamicCacheSummaryWire
		if err := decodeStrictSummaryWire(body, &wire); err != nil {
			return nil, err
		}
		bits, err := decodeSummaryBits(wire.Bits, wire.MBits)
		if err != nil {
			return nil, err
		}
		if *wire.ItemCount > 0 && !hasSetBloomBit(bits) {
			return nil, errors.New("invalid empty bloom state")
		}
		s = cacheSummary{
			Version: wire.Version, Layout: wire.Layout, CreatedNS: wire.CreatedNS, ExpiresNS: wire.ExpiresNS,
			ItemCount: *wire.ItemCount, DesignItems: *wire.DesignItems,
			MBits: wire.MBits, Hashes: wire.Hashes, Bits: bits,
		}
	default:
		return nil, errors.New("incompatible summary")
	}
	return &s, nil
}

func validSummaryTimestamps(createdNS, expiresNS int64, now time.Time) bool {
	created, expires := time.Unix(0, createdNS), time.Unix(0, expiresNS)
	return expires.After(now) && expires.After(created) && !created.After(now.Add(2*time.Minute)) && expires.Sub(created) <= 2*defaultSummaryTTL
}

func hasSetBloomBit(bits []byte) bool {
	for _, value := range bits {
		if value != 0 {
			return true
		}
	}
	return false
}

func decodeStrictSummaryWire(body []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid summary encoding")
	}
	return nil
}

func decodeSummaryBits(encoded json.RawMessage, bitCount uint64) ([]byte, error) {
	if bitCount < 8 || bitCount%8 != 0 {
		return nil, errors.New("invalid bloom parameters")
	}
	expectedBytes := bitCount / 8
	if expectedBytes > uint64(maxAcceptedSummaryBloomCapacity().BitCount/8) {
		return nil, errors.New("invalid bloom parameters")
	}
	expectedEncodedBytes := base64.StdEncoding.EncodedLen(int(expectedBytes))
	if len(encoded) != expectedEncodedBytes+2 || len(encoded) < 2 || encoded[0] != '"' || encoded[len(encoded)-1] != '"' {
		return nil, errors.New("invalid bloom bit length")
	}
	bits := make([]byte, int(expectedBytes))
	n, err := base64.StdEncoding.Decode(bits, encoded[1:len(encoded)-1])
	if err != nil || n != len(bits) {
		return nil, errors.New("invalid bloom bits")
	}
	return bits, nil
}

type summaryRecord struct {
	summary *cacheSummary
	bytes   int
	etag    string
}

func boundedSummaryETag(etag string) string {
	// ETags are optional synchronization metadata. Never let a peer-controlled
	// response header become unbounded resident state.
	if len(etag) < 2 || len(etag) > 128 || etag[0] != '"' || etag[len(etag)-1] != '"' || strings.ContainsAny(etag, "\r\n") {
		return ""
	}
	return etag
}

// summaryStore owns bounded peer hints. The small lock is only held to replace
// pointers and copy candidates; Bloom membership tests happen after release.
type summaryStore struct {
	mu      sync.RWMutex
	records map[string]summaryRecord
	bytes   int
}

func (st *summaryStore) receive(peer string, body []byte, etag string, now time.Time, member func(string) bool, remoteBudget int) error {
	if !member(peer) {
		return errors.New("unknown summary sender")
	}
	s, err := parseCacheSummary(body, now)
	if err != nil {
		return err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	residentBytes := len(s.Bits)
	if residentBytes > remoteBudget {
		return errors.New("summary exceeds remote memory budget")
	}
	oldBytes := 0
	if old, ok := st.records[peer]; ok {
		oldBytes = old.bytes
	}
	used := st.bytes - oldBytes + residentBytes
	if used > remoteBudget {
		return errors.New("summary remote memory budget exhausted")
	}
	st.records[peer] = summaryRecord{summary: s, bytes: residentBytes, etag: boundedSummaryETag(etag)}
	st.bytes = used
	return nil
}

func (st *summaryStore) etag(peer string, now time.Time) string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	rec, ok := st.records[peer]
	if !ok || !time.Unix(0, rec.summary.ExpiresNS).After(now) {
		return ""
	}
	return rec.etag
}

func (st *summaryStore) retainPeers(peers map[string]struct{}, now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for peer, rec := range st.records {
		_, retained := peers[peer]
		if !retained || !time.Unix(0, rec.summary.ExpiresNS).After(now) {
			delete(st.records, peer)
			st.bytes -= rec.bytes
		}
	}
}

func (st *summaryStore) removePeer(peer string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if rec, ok := st.records[peer]; ok {
		delete(st.records, peer)
		st.bytes -= rec.bytes
	}
}

func (st *summaryStore) candidates(key string, members []string, now time.Time) (positive, uncovered []string) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var candidates []string
	for _, peer := range members {
		rec, covered := st.records[peer]
		if !covered || !time.Unix(0, rec.summary.ExpiresNS).After(now) {
			uncovered = append(uncovered, peer)
			continue
		}
		if rec.summary.Contains(key) {
			summaryAgeSeconds.Observe(now.Sub(time.Unix(0, rec.summary.CreatedNS)).Seconds())
			candidates = append(candidates, peer)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := rankPeer(key, candidates[i]), rankPeer(key, candidates[j])
		return bytes.Compare(a[:], b[:]) < 0
	})
	return candidates, uncovered
}

func rankPeer(key, addr string) [sha256.Size]byte {
	return sha256.Sum256([]byte(key + "\x00" + addr))
}
