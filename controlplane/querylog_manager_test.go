//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

type fakeTenantQueryLogStore struct {
	mu        sync.Mutex
	stopCount int
}

func (s *fakeTenantQueryLogStore) Log(server.QueryLogEntry) {}

func (s *fakeTenantQueryLogStore) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopCount++
}

func (s *fakeTenantQueryLogStore) stops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopCount
}

func enabledTenantQueryLogConfig() configstore.QueryLogConfig {
	return configstore.QueryLogConfig{
		Enabled:              true,
		FlushIntervalS:       1,
		BatchSize:            1,
		CompactIntervalS:     60,
		DataInliningRowLimit: 1000,
	}
}

func TestTenantQueryLogProviderResolvesCacheMissInBackground(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	store := &fakeTenantQueryLogStore{}
	var startOnce sync.Once

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(ctx context.Context, orgID string) (tenantQueryLogRuntime, error) {
			if orgID != "org-a" {
				t.Fatalf("expected org-a, got %q", orgID)
			}
			startOnce.Do(func() { close(started) })
			select {
			case <-release:
				return tenantQueryLogRuntime{
					DuckLake: server.DuckLakeConfig{MetadataStore: "postgres:dbname=querylog"},
				}, nil
			case <-ctx.Done():
				return tenantQueryLogRuntime{}, ctx.Err()
			}
		},
	)
	m.newLog = func(server.Config, server.DuckLakeConfig) (tenantQueryLogStore, error) {
		return store, nil
	}

	done := make(chan server.QueryLogSink, 1)
	go func() {
		done <- m.QueryLogSinkForOrg("org-a")
	}()

	select {
	case sink := <-done:
		if sink != nil {
			t.Fatalf("expected cache miss to return nil before background resolve, got %#v", sink)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("query log provider blocked on tenant logger resolution")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected background resolver to start")
	}
	close(release)

	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == store
	})
	m.Stop()
}

func TestTenantQueryLogProviderBacksOffResolverFailures(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	calls := 0
	resolved := make(chan struct{})

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(context.Context, string) (tenantQueryLogRuntime, error) {
			calls++
			close(resolved)
			return tenantQueryLogRuntime{}, errors.New("resolver down")
		},
	)
	m.now = func() time.Time { return now }

	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected resolver miss to return nil, got %#v", sink)
	}
	select {
	case <-resolved:
	case <-time.After(time.Second):
		t.Fatal("expected resolver to run")
	}
	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected resolver failure backoff to return nil, got %#v", sink)
	}
	if calls != 1 {
		t.Fatalf("expected resolver to be called once during backoff, got %d", calls)
	}
}

func TestTenantQueryLogProviderRefreshesExpiringCredentials(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	oldStore := &fakeTenantQueryLogStore{}
	newStore := &fakeTenantQueryLogStore{}
	resolves := 0

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(context.Context, string) (tenantQueryLogRuntime, error) {
			resolves++
			if resolves == 1 {
				return tenantQueryLogRuntime{
					DuckLake: server.DuckLakeConfig{
						MetadataStore:  "postgres:dbname=querylog",
						S3SessionToken: "old-token",
					},
					ExpiresAt: ptrTime(now.Add(credentialRefreshLookahead / 2)),
				}, nil
			}
			return tenantQueryLogRuntime{
				DuckLake: server.DuckLakeConfig{
					MetadataStore:  "postgres:dbname=querylog",
					S3SessionToken: "new-token",
				},
				ExpiresAt: ptrTime(now.Add(time.Hour)),
			}, nil
		},
	)
	m.now = func() time.Time { return now }
	m.newLog = func(_ server.Config, dl server.DuckLakeConfig) (tenantQueryLogStore, error) {
		switch dl.S3SessionToken {
		case "old-token":
			return oldStore, nil
		case "new-token":
			return newStore, nil
		default:
			t.Fatalf("unexpected token %q", dl.S3SessionToken)
			return nil, nil
		}
	}

	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected cold cache miss, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == oldStore
	})

	if sink := m.QueryLogSinkForOrg("org-a"); sink != oldStore {
		t.Fatalf("expected expiring credentials to keep serving old logger during refresh, got %#v", sink)
	}

	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == newStore
	})
	if resolves < 2 {
		t.Fatalf("expected expiring credentials to force a second resolve, got %d resolves", resolves)
	}
	if oldStore.stops() != 1 {
		t.Fatalf("expected old logger to stop after refresh, got %d stops", oldStore.stops())
	}
}

func TestTenantQueryLogProviderStopsExpiredLogger(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	oldStore := &fakeTenantQueryLogStore{}

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(context.Context, string) (tenantQueryLogRuntime, error) {
			return tenantQueryLogRuntime{
				DuckLake:  server.DuckLakeConfig{MetadataStore: "postgres:dbname=querylog"},
				ExpiresAt: ptrTime(now.Add(time.Hour)),
			}, nil
		},
	)
	m.now = func() time.Time { return now }
	m.newLog = func(server.Config, server.DuckLakeConfig) (tenantQueryLogStore, error) {
		return oldStore, nil
	}

	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected cold cache miss, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == oldStore
	})

	now = now.Add(time.Hour)
	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected expired logger to be removed before refresh, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return oldStore.stops() == 1
	})
}

func TestTenantQueryLogProviderStopsSharedLoggerAfterLastOrgRotates(t *testing.T) {
	baseTime := time.Unix(1700000000, 0).UTC()
	now := baseTime
	oldStore := &fakeTenantQueryLogStore{}
	newStoreA := &fakeTenantQueryLogStore{}
	newStoreB := &fakeTenantQueryLogStore{}
	resolveCounts := make(map[string]int)
	var countsMu sync.Mutex

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(_ context.Context, orgID string) (tenantQueryLogRuntime, error) {
			countsMu.Lock()
			resolveCounts[orgID]++
			call := resolveCounts[orgID]
			countsMu.Unlock()
			if call == 1 {
				return tenantQueryLogRuntime{
					DuckLake: server.DuckLakeConfig{
						MetadataStore:  "postgres:dbname=querylog",
						S3SessionToken: "shared-token",
					},
					ExpiresAt: ptrTime(baseTime.Add(time.Hour)),
				}, nil
			}
			return tenantQueryLogRuntime{
				DuckLake: server.DuckLakeConfig{
					MetadataStore:  "postgres:dbname=querylog",
					S3SessionToken: "new-token-" + orgID,
				},
				ExpiresAt: ptrTime(baseTime.Add(2 * time.Hour)),
			}, nil
		},
	)
	m.now = func() time.Time { return now }
	m.newLog = func(_ server.Config, dl server.DuckLakeConfig) (tenantQueryLogStore, error) {
		switch dl.S3SessionToken {
		case "shared-token":
			return oldStore, nil
		case "new-token-org-a":
			return newStoreA, nil
		case "new-token-org-b":
			return newStoreB, nil
		default:
			t.Fatalf("unexpected token %q", dl.S3SessionToken)
			return nil, nil
		}
	}

	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected cold cache miss for org-a, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == oldStore
	})
	if sink := m.QueryLogSinkForOrg("org-b"); sink != nil {
		t.Fatalf("expected cold cache miss for org-b, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-b") == oldStore
	})

	now = baseTime.Add(credentialRefreshLookahead + time.Minute)
	if sink := m.QueryLogSinkForOrg("org-a"); sink != oldStore {
		t.Fatalf("expected org-a to keep using shared logger while refreshing, got %#v", sink)
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == newStoreA
	})
	if oldStore.stops() != 0 {
		t.Fatalf("expected shared logger to stay alive for org-b, got %d stops", oldStore.stops())
	}
	if sink := m.QueryLogSinkForOrg("org-b"); sink != oldStore {
		t.Fatalf("expected org-b to still use shared logger before its refresh, got %#v", sink)
	}

	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-b") == newStoreB
	})
	if oldStore.stops() != 1 {
		t.Fatalf("expected shared logger to stop after last org rotates, got %d stops", oldStore.stops())
	}
}

func TestTenantQueryLogProviderRetriesAfterLoggerCreationTimeout(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	startedFirst := make(chan struct{})
	startedSecond := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	var releaseOnce sync.Once
	newLogCalls := 0
	lateStore := &fakeTenantQueryLogStore{}
	secondStore := &fakeTenantQueryLogStore{}

	m := newTenantQueryLogManager(
		server.Config{},
		enabledTenantQueryLogConfig,
		func(context.Context, string) (tenantQueryLogRuntime, error) {
			return tenantQueryLogRuntime{
				DuckLake: server.DuckLakeConfig{MetadataStore: "postgres:dbname=querylog"},
			}, nil
		},
	)
	m.now = func() time.Time { return now }
	m.resolveTimeout = 20 * time.Millisecond
	m.retryInterval = 20 * time.Millisecond
	m.newLog = func(server.Config, server.DuckLakeConfig) (tenantQueryLogStore, error) {
		newLogCalls++
		if newLogCalls == 1 {
			firstOnce.Do(func() { close(startedFirst) })
			<-releaseFirst
			return lateStore, nil
		}
		secondOnce.Do(func() { close(startedSecond) })
		return secondStore, nil
	}
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	t.Cleanup(release)

	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected cold cache miss, got %#v", sink)
	}
	select {
	case <-startedFirst:
	case <-time.After(time.Second):
		t.Fatal("expected first logger creation to start")
	}

	time.Sleep(50 * time.Millisecond)
	now = now.Add(time.Minute)
	if sink := m.QueryLogSinkForOrg("org-a"); sink != nil {
		t.Fatalf("expected timed-out logger creation retry to return nil before second creation, got %#v", sink)
	}
	select {
	case <-startedSecond:
	case <-time.After(time.Second):
		t.Fatal("expected second logger creation to start after timeout and retry interval")
	}
	waitUntil(t, time.Second, func() bool {
		return m.QueryLogSinkForOrg("org-a") == secondStore
	})

	release()
	waitUntil(t, time.Second, func() bool {
		return lateStore.stops() == 1
	})
	if sink := m.QueryLogSinkForOrg("org-a"); sink != secondStore {
		t.Fatalf("expected late first logger not to replace second logger, got %#v", sink)
	}
}

func TestQueryLogRuntimeKeyIncludesAttachmentAndCredentialFields(t *testing.T) {
	base := server.DuckLakeConfig{
		MetadataStore:  "postgres:dbname=querylog",
		ObjectStore:    "s3://bucket/path/",
		S3Endpoint:     "s3.us-east-1.example",
		S3AccessKey:    "access-key",
		S3SecretKey:    "secret-key",
		S3SessionToken: "session-token",
		S3Region:       "us-east-1",
		S3UseSSL:       true,
		S3URLStyle:     "path",
		SpecVersion:    "0.3",
	}
	baseKey := queryLogRuntimeKey(base)

	mutated := base
	mutated.S3Region = "us-west-2"
	if queryLogRuntimeKey(mutated) == baseKey {
		t.Fatal("expected S3 region change to invalidate query log runtime key")
	}

	mutated = base
	mutated.S3SecretKey = "rotated-secret-key"
	if queryLogRuntimeKey(mutated) == baseKey {
		t.Fatal("expected S3 secret change to invalidate query log runtime key")
	}

	mutated = base
	mutated.S3SessionToken = "rotated-session-token"
	if queryLogRuntimeKey(mutated) == baseKey {
		t.Fatal("expected S3 session token change to invalidate query log runtime key")
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
