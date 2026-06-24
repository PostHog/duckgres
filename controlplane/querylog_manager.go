//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

const (
	queryLogManagerResolveTimeout = 30 * time.Second
	queryLogResolveRetryInterval  = 30 * time.Second
)

type tenantQueryLogRuntime struct {
	DuckLake  server.DuckLakeConfig
	ExpiresAt *time.Time
}

type tenantQueryLogManager struct {
	base    server.Config
	config  func() configstore.QueryLogConfig
	resolve func(context.Context, string) (tenantQueryLogRuntime, error)
	newLog  func(server.Config, server.DuckLakeConfig) (tenantQueryLogStore, error)
	now     func() time.Time

	loggers     map[string]*tenantQueryLogState
	orgKeys     map[string]string
	keyRefs     map[string]map[string]struct{}
	resolving   map[string]struct{}
	nextResolve map[string]time.Time
	configSig   string

	resolveTimeout time.Duration
	retryInterval  time.Duration
	stopped        bool
	stopOnce       sync.Once
	mu             sync.Mutex
}

type tenantQueryLogState struct {
	key       string
	logger    tenantQueryLogStore
	expiresAt *time.Time
}

type tenantQueryLogStore interface {
	server.QueryLogSink
	Stop()
}

func newTenantQueryLogManager(
	base server.Config,
	config func() configstore.QueryLogConfig,
	resolve func(context.Context, string) (tenantQueryLogRuntime, error),
) *tenantQueryLogManager {
	return &tenantQueryLogManager{
		base:    base,
		config:  config,
		resolve: resolve,
		newLog: func(cfg server.Config, dlCfg server.DuckLakeConfig) (tenantQueryLogStore, error) {
			return server.NewQueryLoggerForDuckLake(cfg, dlCfg)
		},
		now:         time.Now,
		loggers:     make(map[string]*tenantQueryLogState),
		orgKeys:     make(map[string]string),
		keyRefs:     make(map[string]map[string]struct{}),
		resolving:   make(map[string]struct{}),
		nextResolve: make(map[string]time.Time),

		resolveTimeout: queryLogManagerResolveTimeout,
		retryInterval:  queryLogResolveRetryInterval,
	}
}

func (m *tenantQueryLogManager) QueryLogSinkForOrg(orgID string) server.QueryLogSink {
	logger, shouldResolve, forceResolve, stale := m.cachedLoggerForOrg(orgID)
	if len(stale) > 0 {
		go stopTenantQueryLogStores(stale)
	}
	if shouldResolve {
		m.resolveLoggerInBackground(orgID, forceResolve)
	}
	return logger
}

func (m *tenantQueryLogManager) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopped = true
		stale := m.clearLoggersLocked()
		m.mu.Unlock()
		stopTenantQueryLogStores(stale)
	})
}

func (m *tenantQueryLogManager) loggerForOrg(ctx context.Context, orgID string, forceResolve bool) (tenantQueryLogStore, error) {
	if m == nil || orgID == "" {
		return nil, nil
	}
	cfg := m.currentConfig()
	sig := queryLogConfigSignature(cfg)
	if !cfg.Enabled {
		m.mu.Lock()
		stale := m.clearLoggersLocked()
		m.configSig = sig
		m.mu.Unlock()
		stopTenantQueryLogStores(stale)
		return nil, nil
	}

	now := m.now()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil, nil
	}
	if sig != m.configSig {
		stale := m.clearLoggersLocked()
		m.configSig = sig
		m.nextResolve = make(map[string]time.Time)
		m.mu.Unlock()
		stopTenantQueryLogStores(stale)
		m.mu.Lock()
		if m.stopped {
			m.mu.Unlock()
			return nil, nil
		}
	}
	if key := m.orgKeys[orgID]; key != "" && !forceResolve {
		if state := m.loggers[key]; state != nil {
			if queryLogCredentialsExpired(state.expiresAt, now) {
				stale := m.releaseOrgLocked(orgID)
				m.mu.Unlock()
				stopTenantQueryLogStores(stale)
			} else {
				logger := state.logger
				m.mu.Unlock()
				return logger, nil
			}
		} else {
			delete(m.orgKeys, orgID)
			m.mu.Unlock()
		}
	} else {
		m.mu.Unlock()
	}

	if m.resolve == nil {
		return nil, fmt.Errorf("tenant query log resolver is not configured")
	}
	resolveCtx, cancel := context.WithTimeout(ctx, m.currentResolveTimeout())
	defer cancel()
	runtime, err := m.resolve(resolveCtx, orgID)
	if err != nil {
		return nil, err
	}
	if runtime.DuckLake.MetadataStore == "" {
		m.mu.Lock()
		stale := m.releaseOrgLocked(orgID)
		m.mu.Unlock()
		stopTenantQueryLogStores(stale)
		return nil, nil
	}
	key := queryLogRuntimeKey(runtime.DuckLake)

	m.mu.Lock()
	if state := m.loggers[key]; state != nil && !queryLogCredentialsExpired(state.expiresAt, now) {
		stale := m.assignOrgKeyLocked(orgID, key)
		logger := state.logger
		m.mu.Unlock()
		stopTenantQueryLogStores(stale)
		return logger, nil
	}
	m.mu.Unlock()

	base := m.base
	base.QueryLog = serverQueryLogConfig(base.QueryLog, cfg)
	base.DuckLake = runtime.DuckLake
	logger, err := m.createLogger(resolveCtx, base, runtime.DuckLake)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		return nil, nil
	}

	currentCfg := m.currentConfig()
	if !currentCfg.Enabled || queryLogConfigSignature(currentCfg) != sig {
		logger.Stop()
		return nil, nil
	}

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		logger.Stop()
		return nil, nil
	}
	if state := m.loggers[key]; state != nil && !queryLogCredentialsExpired(state.expiresAt, m.now()) {
		stale := m.assignOrgKeyLocked(orgID, key)
		existing := state.logger
		m.mu.Unlock()
		logger.Stop()
		stopTenantQueryLogStores(stale)
		return existing, nil
	}
	stale := m.installLoggerLocked(orgID, key, logger, runtime.ExpiresAt)
	m.mu.Unlock()
	stopTenantQueryLogStores(stale)
	slog.Info("querylog: tenant logger enabled.", "org", orgID, "logger_key", key, "expires_at", runtime.ExpiresAt)
	return logger, nil
}

func (m *tenantQueryLogManager) cachedLoggerForOrg(orgID string) (tenantQueryLogStore, bool, bool, []tenantQueryLogStore) {
	if m == nil || orgID == "" {
		return nil, false, false, nil
	}
	cfg := m.currentConfig()
	sig := queryLogConfigSignature(cfg)
	now := m.now()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return nil, false, false, nil
	}

	var stale []tenantQueryLogStore
	if !cfg.Enabled {
		stale = m.clearLoggersLocked()
		m.configSig = sig
		return nil, false, false, stale
	}
	if sig != m.configSig {
		stale = m.clearLoggersLocked()
		m.configSig = sig
		m.nextResolve = make(map[string]time.Time)
	}
	if key := m.orgKeys[orgID]; key != "" {
		if state := m.loggers[key]; state != nil {
			if queryLogCredentialsExpired(state.expiresAt, now) {
				stale = append(stale, m.releaseOrgLocked(orgID)...)
			} else {
				resolve := false
				forceResolve := false
				if queryLogCredentialsExpiring(state.expiresAt, now) {
					resolve = m.markResolvingLocked(orgID, now)
					forceResolve = resolve
				}
				return state.logger, resolve, forceResolve, stale
			}
		} else {
			stale = append(stale, m.releaseOrgLocked(orgID)...)
		}
	}
	return nil, m.markResolvingLocked(orgID, now), false, stale
}

func (m *tenantQueryLogManager) markResolvingLocked(orgID string, now time.Time) bool {
	if _, ok := m.resolving[orgID]; ok {
		return false
	}
	if next := m.nextResolve[orgID]; !next.IsZero() && now.Before(next) {
		return false
	}
	m.resolving[orgID] = struct{}{}
	return true
}

func (m *tenantQueryLogManager) resolveLoggerInBackground(orgID string, forceResolve bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), m.currentResolveTimeout())
		logger, err := m.loggerForOrg(ctx, orgID, forceResolve)
		cancel()

		m.mu.Lock()
		delete(m.resolving, orgID)
		if err != nil || logger == nil {
			m.nextResolve[orgID] = m.now().Add(m.currentRetryInterval())
		} else {
			delete(m.nextResolve, orgID)
		}
		m.mu.Unlock()

		if err != nil {
			slog.Warn("querylog: tenant logger unavailable.", "org", orgID, "error", err)
		}
	}()
}

type tenantQueryLogCreateResult struct {
	logger tenantQueryLogStore
	err    error
}

func (m *tenantQueryLogManager) createLogger(ctx context.Context, cfg server.Config, dlCfg server.DuckLakeConfig) (tenantQueryLogStore, error) {
	resultCh := make(chan tenantQueryLogCreateResult)
	timedOut := make(chan struct{})
	go func() {
		logger, err := m.newLog(cfg, dlCfg)
		result := tenantQueryLogCreateResult{logger: logger, err: err}
		select {
		case resultCh <- result:
		case <-timedOut:
			if result.logger != nil {
				result.logger.Stop()
			}
			if result.err != nil {
				slog.Warn("querylog: late tenant logger creation failed.", "error", result.err)
			}
		}
	}()

	select {
	case result := <-resultCh:
		return result.logger, result.err
	case <-ctx.Done():
		close(timedOut)
		return nil, ctx.Err()
	}
}

func (m *tenantQueryLogManager) clearLoggersLocked() []tenantQueryLogStore {
	loggers := make([]tenantQueryLogStore, 0, len(m.loggers))
	for _, state := range m.loggers {
		if state != nil && state.logger != nil {
			loggers = append(loggers, state.logger)
		}
	}
	m.loggers = make(map[string]*tenantQueryLogState)
	m.orgKeys = make(map[string]string)
	m.keyRefs = make(map[string]map[string]struct{})
	return loggers
}

func (m *tenantQueryLogManager) installLoggerLocked(orgID, key string, logger tenantQueryLogStore, expiresAt *time.Time) []tenantQueryLogStore {
	var stale []tenantQueryLogStore
	if old := m.loggers[key]; old != nil && old.logger != nil && old.logger != logger {
		stale = append(stale, old.logger)
	}
	m.loggers[key] = &tenantQueryLogState{key: key, logger: logger, expiresAt: expiresAt}
	stale = append(stale, m.assignOrgKeyLocked(orgID, key)...)
	return stale
}

func (m *tenantQueryLogManager) assignOrgKeyLocked(orgID, key string) []tenantQueryLogStore {
	var stale []tenantQueryLogStore
	if oldKey := m.orgKeys[orgID]; oldKey != "" && oldKey != key {
		stale = append(stale, m.releaseOrgKeyLocked(orgID, oldKey)...)
	}
	m.orgKeys[orgID] = key
	if m.keyRefs[key] == nil {
		m.keyRefs[key] = make(map[string]struct{})
	}
	m.keyRefs[key][orgID] = struct{}{}
	return stale
}

func (m *tenantQueryLogManager) releaseOrgLocked(orgID string) []tenantQueryLogStore {
	key := m.orgKeys[orgID]
	if key == "" {
		return nil
	}
	return m.releaseOrgKeyLocked(orgID, key)
}

func (m *tenantQueryLogManager) releaseOrgKeyLocked(orgID, key string) []tenantQueryLogStore {
	if m.orgKeys[orgID] == key {
		delete(m.orgKeys, orgID)
	}
	refs := m.keyRefs[key]
	if refs != nil {
		delete(refs, orgID)
		if len(refs) > 0 {
			return nil
		}
		delete(m.keyRefs, key)
	}
	state := m.loggers[key]
	delete(m.loggers, key)
	if state == nil || state.logger == nil {
		return nil
	}
	return []tenantQueryLogStore{state.logger}
}

func stopTenantQueryLogStores(loggers []tenantQueryLogStore) {
	for _, logger := range loggers {
		if logger != nil {
			logger.Stop()
		}
	}
}

func (m *tenantQueryLogManager) currentConfig() configstore.QueryLogConfig {
	if m.config == nil {
		return configstore.QueryLogConfig{}
	}
	return m.config()
}

func (m *tenantQueryLogManager) currentResolveTimeout() time.Duration {
	if m.resolveTimeout > 0 {
		return m.resolveTimeout
	}
	return queryLogManagerResolveTimeout
}

func (m *tenantQueryLogManager) currentRetryInterval() time.Duration {
	if m.retryInterval > 0 {
		return m.retryInterval
	}
	return queryLogResolveRetryInterval
}

func serverQueryLogConfig(base server.QueryLogConfig, cfg configstore.QueryLogConfig) server.QueryLogConfig {
	out := base
	out.Enabled = cfg.Enabled
	if cfg.FlushIntervalS > 0 {
		out.FlushInterval = time.Duration(cfg.FlushIntervalS) * time.Second
	}
	if cfg.BatchSize > 0 {
		out.BatchSize = cfg.BatchSize
	}
	if cfg.CompactIntervalS > 0 {
		out.CompactInterval = time.Duration(cfg.CompactIntervalS) * time.Second
	}
	if cfg.DataInliningRowLimit > 0 {
		out.DataInliningRowLimit = cfg.DataInliningRowLimit
	}
	if out.FlushInterval <= 0 {
		out.FlushInterval = 5 * time.Second
	}
	if out.BatchSize <= 0 {
		out.BatchSize = 1000
	}
	if out.CompactInterval <= 0 {
		out.CompactInterval = 10 * time.Minute
	}
	if out.DataInliningRowLimit <= 0 {
		out.DataInliningRowLimit = 1000
	}
	return out
}

func queryLogConfigSignature(cfg configstore.QueryLogConfig) string {
	return fmt.Sprintf("%t/%d/%d/%d/%d",
		cfg.Enabled,
		cfg.FlushIntervalS,
		cfg.BatchSize,
		cfg.CompactIntervalS,
		cfg.DataInliningRowLimit,
	)
}

func queryLogRuntimeKey(dl server.DuckLakeConfig) string {
	data, err := json.Marshal(dl)
	if err != nil {
		data = []byte(fmt.Sprintf("%#v", dl))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:12])
}

func queryLogCredentialsExpiring(expiresAt *time.Time, now time.Time) bool {
	return expiresAt != nil && !now.Before(expiresAt.Add(-credentialRefreshLookahead))
}

func queryLogCredentialsExpired(expiresAt *time.Time, now time.Time) bool {
	return expiresAt != nil && !now.Before(*expiresAt)
}
