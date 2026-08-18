package main

import (
	"bytes"
	"crypto/sha256"
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
	summaryFormatVersion   = 1
	cacheLayoutVersion     = 1
	defaultSummaryTTL      = 45 * time.Second
	defaultSummaryInterval = 20 * time.Second
	summaryTargetFPR       = 0.01
	// A 1m-entry filter at 1% false-positive rate is ~1.15 MiB before JSON
	// base64 encoding. Two MiB is a hard wire cap with modest headroom.
	maxSummaryBodyBytes     = 2 << 20
	maxSummaryItems         = 1_000_000
	maxSummaryPeers         = 256
	maxSummaryPushes        = 4
	maxSummaryReceives      = 4
	maxSummaryResidentBytes = 512 << 20
	summaryPushTimeout      = 200 * time.Millisecond
)

var (
	summaryPushesTotal   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_pushes_total", Help: "Summary publications by outcome"}, []string{"outcome"})
	summaryReceiptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_receipts_total", Help: "Peer summary receipts by outcome"}, []string{"outcome"})
	summaryResidentCount = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_resident_count", Help: "Current valid peer summaries"})
	summaryResidentBytes = promauto.NewGauge(prometheus.GaugeOpts{Name: "cache_proxy_summary_resident_bytes", Help: "Current valid peer summary bytes"})
	summaryAgeSeconds    = promauto.NewHistogram(prometheus.HistogramOpts{Name: "cache_proxy_summary_age_seconds", Help: "Age of a summary used during lookup", Buckets: prometheus.ExponentialBuckets(0.1, 2, 10)})
	summaryLookupTotal   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_summary_lookups_total", Help: "Summary lookup decisions"}, []string{"outcome"})
	peerDirectGetsTotal  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "cache_proxy_peer_direct_gets_total", Help: "Direct peer GET attempts in summary mode"}, []string{"outcome"})
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
	Version    int    `json:"v"`
	Layout     int    `json:"l"`
	Sender     string `json:"s"`
	Generation uint64 `json:"g"`
	CreatedNS  int64  `json:"c"`
	ExpiresNS  int64  `json:"e"`
	ItemCount  int    `json:"n"`
	MBits      uint64 `json:"m"`
	Hashes     uint8  `json:"k"`
	Bits       []byte `json:"b"`
}

func newCacheSummary(sender string, generation uint64, keys []string, now time.Time, ttl time.Duration) (*cacheSummary, error) {
	if sender == "" || generation == 0 || ttl <= 0 || len(keys) > maxSummaryItems {
		return nil, errors.New("invalid summary inputs")
	}
	m, k := bloomParams(len(keys))
	if m/8 > maxSummaryBodyBytes {
		return nil, fmt.Errorf("summary bloom exceeds %d-byte limit", maxSummaryBodyBytes)
	}
	s := &cacheSummary{Version: summaryFormatVersion, Layout: cacheLayoutVersion, Sender: sender, Generation: generation, CreatedNS: now.UnixNano(), ExpiresNS: now.Add(ttl).UnixNano(), ItemCount: len(keys), MBits: m, Hashes: k, Bits: make([]byte, m/8)}
	for _, key := range keys {
		if !IsValidCacheKey(key) {
			return nil, errors.New("invalid cache key in summary snapshot")
		}
		s.add(key)
	}
	return s, nil
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
	d := sha256.Sum256([]byte(key))
	h1, h2 := binary.LittleEndian.Uint64(d[:8]), binary.LittleEndian.Uint64(d[8:16])
	for i := uint8(0); i < s.Hashes; i++ {
		fn((h1 + uint64(i)*h2) % s.MBits)
	}
}

func (s *cacheSummary) add(key string) {
	s.hashes(key, func(bit uint64) { s.Bits[bit/8] |= 1 << (bit % 8) })
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
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if len(b) > maxSummaryBodyBytes {
		return nil, errors.New("summary exceeds wire limit")
	}
	return b, nil
}
func parseCacheSummary(body []byte, now time.Time) (*cacheSummary, error) {
	if len(body) == 0 || len(body) > maxSummaryBodyBytes {
		return nil, errors.New("invalid summary body size")
	}
	var s cacheSummary
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&s); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("invalid summary encoding")
	}
	if s.Version != summaryFormatVersion || s.Layout != cacheLayoutVersion || s.Sender == "" || len(s.Sender) > 253 || strings.ContainsAny(s.Sender, "\r\n") || s.Generation == 0 {
		return nil, errors.New("incompatible summary")
	}
	if s.ItemCount < 0 || s.ItemCount > maxSummaryItems {
		return nil, errors.New("invalid bloom parameters")
	}
	wantBits, wantHashes := bloomParams(s.ItemCount)
	if s.MBits != wantBits || s.Hashes != wantHashes || s.MBits/8 > maxSummaryBodyBytes || len(s.Bits) != int(s.MBits/8) {
		return nil, errors.New("invalid bloom parameters")
	}
	created, expires := time.Unix(0, s.CreatedNS), time.Unix(0, s.ExpiresNS)
	if !expires.After(now) || !expires.After(created) || created.After(now.Add(2*time.Minute)) || expires.Sub(created) > 2*defaultSummaryTTL {
		return nil, errors.New("invalid summary timestamps")
	}
	return &s, nil
}

type summaryRecord struct {
	summary *cacheSummary
	bytes   int
}

// summaryStore owns bounded peer hints. The small lock is only held to replace
// pointers and copy candidates; Bloom membership tests happen after release.
type summaryStore struct {
	mu      sync.RWMutex
	records map[string]summaryRecord
	bytes   int
}

func (st *summaryStore) receive(peer string, body []byte, now time.Time, member func(string) bool) error {
	s, err := parseCacheSummary(body, now)
	if err != nil {
		return err
	}
	if !member(peer) {
		return errors.New("unknown summary sender")
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if old, ok := st.records[peer]; ok && old.summary.Generation >= s.Generation {
		return errors.New("stale summary generation")
	}
	if len(st.records) >= maxSummaryPeers {
		if _, exists := st.records[peer]; !exists {
			return errors.New("summary peer limit")
		}
	}
	residentBytes := len(s.Bits)
	oldBytes := 0
	if old, ok := st.records[peer]; ok {
		oldBytes = old.bytes
	}
	if st.bytes-oldBytes+residentBytes > maxSummaryResidentBytes {
		return errors.New("summary resident byte limit")
	}
	if oldBytes > 0 {
		st.bytes -= oldBytes
	}
	st.records[peer] = summaryRecord{summary: s, bytes: residentBytes}
	st.bytes += residentBytes
	return nil
}
func (st *summaryStore) removeNonMembers(member func(string) bool, now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for peer, rec := range st.records {
		if !member(peer) || time.Unix(0, rec.summary.ExpiresNS).Before(now) {
			delete(st.records, peer)
			st.bytes -= rec.bytes
		}
	}
}
func (st *summaryStore) candidates(key string, members []string, now time.Time) (positive, uncovered []string) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	type candidate struct{ addr, identity string }
	var cs []candidate
	for _, peer := range members {
		rec, covered := st.records[peer]
		if !covered || !time.Unix(0, rec.summary.ExpiresNS).After(now) {
			uncovered = append(uncovered, peer)
			continue
		}
		if rec.summary.Contains(key) {
			summaryAgeSeconds.Observe(now.Sub(time.Unix(0, rec.summary.CreatedNS)).Seconds())
			cs = append(cs, candidate{peer, rec.summary.Sender})
		}
	}
	sort.Slice(cs, func(i, j int) bool {
		a, b := rankPeer(key, cs[i].identity, cs[i].addr), rankPeer(key, cs[j].identity, cs[j].addr)
		return bytes.Compare(a[:], b[:]) < 0
	})
	positive = make([]string, len(cs))
	for i := range cs {
		positive[i] = cs[i].addr
	}
	return positive, uncovered
}

func rankPeer(key, identity, addr string) [sha256.Size]byte {
	return sha256.Sum256([]byte(key + "\x00" + identity + "\x00" + addr))
}
