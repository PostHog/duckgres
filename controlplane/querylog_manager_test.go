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
	stopCount int
}

func (s *fakeTenantQueryLogStore) Log(server.QueryLogEntry) {}

func (s *fakeTenantQueryLogStore) Stop() {
	s.stopCount++
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
