package hogqlcatalog

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type Reader interface {
	Latest(ctx context.Context, catalog PhysicalIdentifier) (*HogQLSemanticCatalogSnapshot, error)
	Generation(ctx context.Context, catalog PhysicalIdentifier, generation int64) (*HogQLSemanticCatalogSnapshot, error)
}

type Publisher interface {
	Publish(ctx context.Context, snapshot *HogQLSemanticCatalogSnapshot) error
}

type MemoryStore struct {
	mu       sync.RWMutex
	catalogs map[PhysicalIdentifier]*catalogHistory
}

type catalogHistory struct {
	latest      int64
	generations map[int64]*HogQLSemanticCatalogSnapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{catalogs: make(map[PhysicalIdentifier]*catalogHistory)}
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
