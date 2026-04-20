package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// CacheProxy handles S3 requests from DuckDB, caching responses on local NVMe
// and fetching from peers or S3 on cache miss.
type CacheProxy struct {
	store    *DiskCache
	peers    *PeerManager
	s3Client *s3.Client
	flights  singleFlight
}

// singleFlight deduplicates concurrent fetches for the same cache key.
type singleFlight struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func (sf *singleFlight) Do(key string, fn func() ([]byte, error)) ([]byte, error) {
	sf.mu.Lock()
	if sf.m == nil {
		sf.m = make(map[string]*call)
	}
	if c, ok := sf.m[key]; ok {
		sf.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call{}
	c.wg.Add(1)
	sf.m[key] = c
	sf.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	sf.mu.Lock()
	delete(sf.m, key)
	sf.mu.Unlock()

	return c.val, c.err
}

func NewCacheProxy(store *DiskCache, peers *PeerManager) *CacheProxy {
	// Initialize AWS S3 client using default credential chain
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("Failed to load AWS config for S3 client.", "error", err)
	}

	var s3Client *s3.Client
	if err == nil {
		s3Client = s3.NewFromConfig(cfg)
	}

	return &CacheProxy{
		store:    store,
		peers:    peers,
		s3Client: s3Client,
	}
}

// HandleS3Request handles S3 GET requests from DuckDB.
// DuckDB sends requests like: GET /bucket/key with Range header.
func (p *CacheProxy) HandleS3Request(w http.ResponseWriter, r *http.Request) {
	if r.Method == "HEAD" {
		p.handleHeadRequest(w, r)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "only GET and HEAD supported", http.StatusMethodNotAllowed)
		return
	}

	// Parse bucket and key from path: /bucket/key/path
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) < 2 {
		http.Error(w, "invalid path: expected /bucket/key", http.StatusBadRequest)
		return
	}
	bucket := parts[0]
	key := parts[1]
	rangeHeader := r.Header.Get("Range")

	cacheKey := CacheKey(bucket, key, rangeHeader)

	// 1. Check local cache
	if data, ok := p.store.Get(cacheKey); ok {
		cacheBytesServed.WithLabelValues("local").Add(float64(len(data)))
		if rangeHeader != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %s/%d", strings.TrimPrefix(rangeHeader, "bytes="), len(data)))
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write(data)
		return
	}
	cacheMissesTotal.Inc()

	// 2 + 3. Fetch (deduplicated): try peers, then S3
	data, err := p.flights.Do(cacheKey, func() ([]byte, error) {
		// Try peers
		if p.peers != nil {
			if peerData, peerAddr, ok := p.peers.FetchFromPeers(cacheKey); ok {
				slog.Debug("Cache peer hit.", "key", cacheKey[:16], "peer", peerAddr)
				cacheBytesServed.WithLabelValues("peer").Add(float64(len(peerData)))
				return peerData, nil
			}
		}

		// Fetch from S3
		slog.Debug("Cache miss, fetching from S3.", "bucket", bucket, "key", key, "range", rangeHeader)
		data, err := p.fetchFromS3(r.Context(), bucket, key, rangeHeader)
		if err != nil {
			return nil, err
		}
		cacheBytesServed.WithLabelValues("s3").Add(float64(len(data)))
		return data, nil
	})

	if err != nil {
		slog.Error("Failed to fetch.", "bucket", bucket, "key", key, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// Cache locally
	if err := p.store.Put(cacheKey, data); err != nil {
		slog.Warn("Failed to cache.", "key", cacheKey[:16], "error", err)
	}

	if rangeHeader != "" {
		w.WriteHeader(http.StatusPartialContent)
	}
	w.Write(data)
}

func (p *CacheProxy) handleHeadRequest(w http.ResponseWriter, r *http.Request) {
	// Forward HEAD requests directly to S3 (metadata, not cached)
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) < 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	if p.s3Client == nil {
		http.Error(w, "S3 client not configured", http.StatusServiceUnavailable)
		return
	}

	out, err := p.s3Client.HeadObject(r.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	if out.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *out.ContentLength))
	}
	if out.ContentType != nil {
		w.Header().Set("Content-Type", *out.ContentType)
	}
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}
	w.WriteHeader(http.StatusOK)
}

func (p *CacheProxy) fetchFromS3(ctx context.Context, bucket, key, rangeHeader string) ([]byte, error) {
	if p.s3Client == nil {
		return nil, fmt.Errorf("S3 client not configured")
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	out, err := p.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 response: %w", err)
	}

	return data, nil
}

// HandlePeerHas responds to "do you have this cache key?" from peers.
func (p *CacheProxy) HandlePeerHas(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if p.store.Has(key) {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// HandlePeerGet returns cached data to a peer.
func (p *CacheProxy) HandlePeerGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	reader, size, ok := p.store.Open(key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	io.Copy(w, reader)
}
