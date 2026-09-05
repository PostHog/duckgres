package hogqlcatalog

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPhysicalRefreshPreservesSemanticDefinitionsAndLogicalNames(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	semantic := testSnapshot(7)
	semantic.LogicalTables[0].Fields[0].Name = "event_key"
	semantic.LogicalTables[0].Properties[0].SourceField = "event_key"
	semantic.LogicalTables[0].Relationships[0].JoinKeys[0].SourceField = "event_key"
	alias := semantic.LogicalTables[0]
	alias.Name = "event_stream"
	semantic.LogicalTables = append(semantic.LogicalTables, alias)
	semantic.Functions = completeSemanticSnapshot(1).Functions
	if err := store.Publish(ctx, semantic); err != nil {
		t.Fatalf("publish semantic snapshot: %v", err)
	}

	lease, acquired, err := store.AcquirePhysicalRefresh(ctx, testCatalog(), time.Minute, true)
	if err != nil || !acquired {
		t.Fatalf("acquire physical refresh = (%#v, %t, %v)", lease, acquired, err)
	}
	refreshed, published, err := store.PublishPhysicalRefresh(ctx, lease, physicalCatalog("bigint", true), "1.0.0", time.Hour)
	if err != nil {
		t.Fatalf("publish physical refresh: %v", err)
	}
	if !published || refreshed.Generation != 8 {
		t.Fatalf("physical refresh = generation %d published %t, want generation 8 published", refreshed.Generation, published)
	}

	if len(refreshed.LogicalTables) != 3 || refreshed.LogicalTables[0].Name != "events" || refreshed.LogicalTables[1].Name != "event_stream" {
		t.Fatalf("logical projections sharing one physical table were not preserved: %#v", refreshed.LogicalTables)
	}
	events := refreshed.LogicalTables[0]
	if events.Fields[0].Name != "event_key" || events.Fields[0].PhysicalColumn.Value != "id" {
		t.Fatalf("logical field identity was not preserved: %#v", events.Fields[0])
	}
	if events.Fields[0].TrinoTypeSignature != "bigint" || events.Fields[0].LogicalType != LogicalTypeInteger || !events.Fields[0].Nullable {
		t.Fatalf("physical field metadata was not refreshed: %#v", events.Fields[0])
	}
	if !reflect.DeepEqual(events.Properties, semantic.LogicalTables[0].Properties) || !reflect.DeepEqual(events.Relationships, semantic.LogicalTables[0].Relationships) {
		t.Fatalf("table semantic definitions changed: %#v", events)
	}
	if !reflect.DeepEqual(refreshed.Functions, semantic.Functions) {
		t.Fatalf("root semantic definitions changed: %#v", refreshed.Functions)
	}
	if got := events.Fields[1]; got.Name != "created_at" || got.TrinoTypeSignature != "timestamp(6)" {
		t.Fatalf("new physical column was not added: %#v", got)
	}
}

func TestMemoryStorePhysicalRefreshIsIdempotentAndMonotonic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	firstLease := acquirePhysicalRefresh(t, store)
	first, published, err := store.PublishPhysicalRefresh(ctx, firstLease, physicalCatalog("varchar", false), "1.0.0", time.Hour)
	if err != nil || !published || first.Generation != 1 {
		t.Fatalf("first refresh = (%#v, %t, %v), want published generation 1", first, published, err)
	}

	retryLease := acquirePhysicalRefresh(t, store)
	retry, published, err := store.PublishPhysicalRefresh(ctx, retryLease, physicalCatalog("varchar", false), "1.0.0", time.Hour)
	if err != nil || published || retry.Generation != 1 {
		t.Fatalf("identical refresh = (%#v, %t, %v), want unchanged generation 1", retry, published, err)
	}

	changedLease := acquirePhysicalRefresh(t, store)
	changed, published, err := store.PublishPhysicalRefresh(ctx, changedLease, physicalCatalog("bigint", false), "1.0.0", time.Hour)
	if err != nil || !published || changed.Generation != 2 {
		t.Fatalf("changed refresh = (%#v, %t, %v), want published generation 2", changed, published, err)
	}
}

func TestPhysicalRefreshMergesSemanticPublicationAfterLeaseAcquisition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	lease := acquirePhysicalRefresh(t, store)

	semantic := testSnapshot(1)
	semantic.LogicalTables[0].Fields[0].Name = "event_key"
	semantic.LogicalTables[0].Properties[0].SourceField = "event_key"
	semantic.LogicalTables[0].Relationships[0].JoinKeys[0].SourceField = "event_key"
	if err := store.Publish(ctx, semantic); err != nil {
		t.Fatalf("publish semantic snapshot during physical fetch: %v", err)
	}

	refreshed, published, err := store.PublishPhysicalRefresh(ctx, lease, physicalCatalog("bigint", false), "1.0.0", time.Hour)
	if err != nil || !published {
		t.Fatalf("publish physical refresh = (%#v, %t, %v)", refreshed, published, err)
	}
	if refreshed.Generation != 2 {
		t.Fatalf("physical refresh generation = %d, want 2", refreshed.Generation)
	}
	if refreshed.LogicalTables[0].Fields[0].Name != "event_key" || refreshed.LogicalTables[0].Fields[0].TrinoTypeSignature != "bigint" {
		t.Fatalf("physical refresh did not merge the latest semantic generation: %#v", refreshed.LogicalTables[0].Fields[0])
	}
	if !reflect.DeepEqual(refreshed.LogicalTables[0].Properties, semantic.LogicalTables[0].Properties) ||
		!reflect.DeepEqual(refreshed.LogicalTables[0].Relationships, semantic.LogicalTables[0].Relationships) {
		t.Fatalf("physical refresh replaced concurrently published semantics: %#v", refreshed.LogicalTables[0])
	}
}

func TestMemoryStoreRejectsStalePhysicalRefreshLease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	stale := acquirePhysicalRefresh(t, store)
	now = now.Add(2 * time.Minute)
	current := acquirePhysicalRefresh(t, store)
	latest, published, err := store.PublishPhysicalRefresh(ctx, current, physicalCatalog("bigint", false), "1.0.0", time.Hour)
	if err != nil || !published || latest.Generation != 1 {
		t.Fatalf("current refresh = (%#v, %t, %v)", latest, published, err)
	}
	if _, _, err := store.PublishPhysicalRefresh(ctx, stale, physicalCatalog("varchar", false), "1.0.0", time.Hour); !errors.Is(err, ErrPhysicalRefreshLeaseLost) {
		t.Fatalf("stale refresh error = %v, want ErrPhysicalRefreshLeaseLost", err)
	}

	retained, err := store.Latest(ctx, testCatalog())
	if err != nil || retained.Generation != 1 || retained.LogicalTables[0].Fields[0].TrinoTypeSignature != "bigint" {
		t.Fatalf("latest after stale refresh = (%#v, %v)", retained, err)
	}
}

func TestMemoryStoreRetainsLastGoodGenerationWhenPhysicalMergeFails(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.Publish(ctx, testSnapshot(4)); err != nil {
		t.Fatalf("publish semantic snapshot: %v", err)
	}
	lease := acquirePhysicalRefresh(t, store)
	invalid := physicalCatalog("varchar", false)
	invalid.Tables = invalid.Tables[:1]
	if _, _, err := store.PublishPhysicalRefresh(ctx, lease, invalid, "1.0.0", time.Hour); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("invalid physical refresh error = %v, want ErrInvalidSnapshot", err)
	}
	if err := store.ReleasePhysicalRefresh(ctx, lease, 0); err != nil {
		t.Fatalf("release failed refresh: %v", err)
	}

	retained, err := store.Latest(ctx, testCatalog())
	if err != nil || retained.Generation != 4 {
		t.Fatalf("latest after failed refresh = (%#v, %v), want generation 4", retained, err)
	}
}

func acquirePhysicalRefresh(t *testing.T, store *MemoryStore) *PhysicalRefreshLease {
	t.Helper()
	lease, acquired, err := store.AcquirePhysicalRefresh(context.Background(), testCatalog(), time.Minute, true)
	if err != nil || !acquired {
		t.Fatalf("acquire physical refresh = (%#v, %t, %v)", lease, acquired, err)
	}
	return lease
}

func physicalCatalog(eventIDType string, addCreatedAt bool) *PhysicalCatalogMetadata {
	eventColumns := []PhysicalColumnMetadata{{
		Name:               PhysicalIdentifier{Value: "id"},
		Ordinal:            1,
		TrinoTypeSignature: eventIDType,
		Nullability:        ColumnNullable,
		StarVisibility:     ColumnStarVisible,
	}}
	if addCreatedAt {
		eventColumns = append(eventColumns, PhysicalColumnMetadata{
			Name:               PhysicalIdentifier{Value: "created_at"},
			Ordinal:            2,
			TrinoTypeSignature: "timestamp(6)",
			Nullability:        ColumnNotNull,
			StarVisibility:     ColumnStarVisible,
		})
	}
	return &PhysicalCatalogMetadata{
		Catalog: testCatalog(),
		Tables: []PhysicalTableMetadata{
			{
				Schema:  PhysicalIdentifier{Value: "default"},
				Table:   PhysicalIdentifier{Value: "events"},
				Columns: eventColumns,
			},
			{
				Schema: PhysicalIdentifier{Value: "default"},
				Table:  PhysicalIdentifier{Value: "persons"},
				Columns: []PhysicalColumnMetadata{{
					Name:               PhysicalIdentifier{Value: "id"},
					Ordinal:            1,
					TrinoTypeSignature: "varchar",
					Nullability:        ColumnNotNull,
					StarVisibility:     ColumnStarVisible,
				}},
			},
		},
	}
}
