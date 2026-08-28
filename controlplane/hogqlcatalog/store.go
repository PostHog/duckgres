package hogqlcatalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrPhysicalRefreshLeaseLost = errors.New("HogQL physical catalog refresh lease lost")

type Reader interface {
	Latest(ctx context.Context, catalog PhysicalIdentifier) (*HogQLSemanticCatalogSnapshot, error)
	Generation(ctx context.Context, catalog PhysicalIdentifier, generation int64) (*HogQLSemanticCatalogSnapshot, error)
}

type Publisher interface {
	Publish(ctx context.Context, snapshot *HogQLSemanticCatalogSnapshot) error
}

type PhysicalRefreshStore interface {
	Reader
	AcquirePhysicalRefresh(ctx context.Context, catalog PhysicalIdentifier, ttl time.Duration, force bool) (*PhysicalRefreshLease, bool, error)
	PublishPhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, metadata *PhysicalCatalogMetadata, languageVersion string, refreshAfter time.Duration) (*HogQLSemanticCatalogSnapshot, bool, error)
	ReleasePhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, retryAfter time.Duration) error
}

type PhysicalRefreshLease struct {
	catalog PhysicalIdentifier
	epoch   int64
	token   string
}

type MemoryStore struct {
	mu       sync.RWMutex
	catalogs map[PhysicalIdentifier]*catalogHistory
	leases   map[PhysicalIdentifier]memoryPhysicalRefreshLease
	now      func() time.Time
}

type memoryPhysicalRefreshLease struct {
	epoch         int64
	token         string
	expiresAt     time.Time
	nextRefreshAt time.Time
}

type catalogHistory struct {
	latest      int64
	generations map[int64]*HogQLSemanticCatalogSnapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		catalogs: make(map[PhysicalIdentifier]*catalogHistory),
		leases:   make(map[PhysicalIdentifier]memoryPhysicalRefreshLease),
		now:      time.Now,
	}
}

func (s *MemoryStore) AcquirePhysicalRefresh(ctx context.Context, catalog PhysicalIdentifier, ttl time.Duration, force bool) (*PhysicalRefreshLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if ttl <= 0 {
		return nil, false, fmt.Errorf("physical refresh lease TTL must be positive")
	}
	normalized, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.leases[normalized]
	now := s.now()
	if current.token != "" && current.expiresAt.After(now) {
		return nil, false, nil
	}
	if !force && current.nextRefreshAt.After(now) {
		return nil, false, nil
	}
	current.epoch++
	current.token = uuid.NewString()
	current.expiresAt = now.Add(ttl)
	s.leases[normalized] = current
	return &PhysicalRefreshLease{catalog: normalized, epoch: current.epoch, token: current.token}, true, nil
}

func (s *MemoryStore) PublishPhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, metadata *PhysicalCatalogMetadata, languageVersion string, refreshAfter time.Duration) (*HogQLSemanticCatalogSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if lease == nil {
		return nil, false, ErrPhysicalRefreshLeaseLost
	}
	if refreshAfter <= 0 {
		return nil, false, fmt.Errorf("physical refresh interval must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.physicalRefreshLeaseMatches(lease) {
		return nil, false, ErrPhysicalRefreshLeaseLost
	}
	history := s.catalogs[lease.catalog]
	var latest *HogQLSemanticCatalogSnapshot
	generation := int64(1)
	if history != nil {
		latest = history.generations[history.latest]
		generation = history.latest + 1
	}
	merged, err := mergePhysicalCatalog(ctx, metadata, latest, lease.catalog, languageVersion, generation)
	if err != nil {
		return nil, false, err
	}
	if latest != nil && snapshotContentsEqual(latest, merged) {
		s.releasePhysicalRefresh(lease, s.now().Add(refreshAfter))
		return cloneSnapshot(latest), false, nil
	}
	if history == nil {
		history = &catalogHistory{generations: make(map[int64]*HogQLSemanticCatalogSnapshot)}
		s.catalogs[lease.catalog] = history
	}
	history.generations[generation] = merged
	history.latest = generation
	s.releasePhysicalRefresh(lease, s.now().Add(refreshAfter))
	return cloneSnapshot(merged), true, nil
}

func (s *MemoryStore) ReleasePhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, retryAfter time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease == nil {
		return ErrPhysicalRefreshLeaseLost
	}
	if retryAfter < 0 {
		return fmt.Errorf("physical refresh retry interval cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.leases[lease.catalog]
	if current.epoch != lease.epoch {
		return ErrPhysicalRefreshLeaseLost
	}
	if current.token == "" {
		return nil
	}
	if current.token != lease.token {
		return ErrPhysicalRefreshLeaseLost
	}
	s.releasePhysicalRefresh(lease, s.now().Add(retryAfter))
	return nil
}

func (s *MemoryStore) physicalRefreshLeaseMatches(lease *PhysicalRefreshLease) bool {
	current := s.leases[lease.catalog]
	return current.epoch == lease.epoch && current.token == lease.token && lease.token != "" && current.expiresAt.After(s.now())
}

func (s *MemoryStore) releasePhysicalRefresh(lease *PhysicalRefreshLease, nextRefreshAt time.Time) {
	current := s.leases[lease.catalog]
	if current.epoch == lease.epoch && current.token == lease.token {
		current.token = ""
		current.expiresAt = time.Time{}
		current.nextRefreshAt = nextRefreshAt
		s.leases[lease.catalog] = current
	}
}

func (s *MemoryStore) Publish(ctx context.Context, snapshot *HogQLSemanticCatalogSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeAndValidateSnapshot(snapshot)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	history := s.catalogs[normalized.Catalog]
	if history == nil {
		history = &catalogHistory{generations: make(map[int64]*HogQLSemanticCatalogSnapshot)}
		s.catalogs[normalized.Catalog] = history
	}
	if normalized.Generation < history.latest {
		return fmt.Errorf("%w: latest=%d received=%d", ErrGenerationRegression, history.latest, normalized.Generation)
	}
	if normalized.Generation == history.latest && history.latest != 0 {
		if reflect.DeepEqual(history.generations[history.latest], normalized) {
			return nil
		}
		return fmt.Errorf("%w: generation=%d", ErrGenerationConflict, normalized.Generation)
	}
	history.generations[normalized.Generation] = normalized
	history.latest = normalized.Generation
	return nil
}

func (s *MemoryStore) Latest(ctx context.Context, catalog PhysicalIdentifier) (*HogQLSemanticCatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalizedCatalog, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := s.catalogs[normalizedCatalog]
	if history == nil {
		return nil, ErrCatalogNotFound
	}
	return cloneSnapshot(history.generations[history.latest]), nil
}

func (s *MemoryStore) Generation(ctx context.Context, catalog PhysicalIdentifier, generation int64) (*HogQLSemanticCatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if generation <= 0 {
		return nil, ErrGenerationNotFound
	}
	normalizedCatalog, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := s.catalogs[normalizedCatalog]
	if history == nil {
		return nil, ErrCatalogNotFound
	}
	snapshot := history.generations[generation]
	if snapshot == nil {
		return nil, ErrGenerationNotFound
	}
	return cloneSnapshot(snapshot), nil
}

func normalizedCatalog(catalog PhysicalIdentifier) (PhysicalIdentifier, error) {
	if err := normalizePhysicalIdentifier(&catalog); err != nil {
		return PhysicalIdentifier{}, err
	}
	return catalog, nil
}
