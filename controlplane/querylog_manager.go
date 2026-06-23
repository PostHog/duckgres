//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

const (
	defaultTenantQueryLogRetentionInterval = time.Hour
	queryLogManagerResolveTimeout          = 30 * time.Second
	queryLogResolveRetryInterval           = 30 * time.Second
	queryLogRetentionOrgTimeout            = 30 * time.Second
)

type tenantQueryLogRuntime struct {
	DuckLake  server.DuckLakeConfig
	ExpiresAt *time.Time
}

type tenantQueryLogManager struct {
	base    server.Config
	config  func() configstore.QueryLogConfig
	orgIDs  func() []string
	resolve func(context.Context, string) (tenantQueryLogRuntime, error)
	newLog  func(server.Config, server.DuckLakeConfig) (tenantQueryLogStore, error)
	now     func() time.Time

	loggers     map[string]*tenantQueryLogState
	orgKeys     map[string]string
	resolving   map[string]struct{}
	nextResolve map[string]time.Time
	configSig   string

	lastRetention      time.Time
	retentionInProcess bool
	stopped            bool
	stopOnce           sync.Once
	mu                 sync.Mutex
}

type tenantQueryLogState struct {
	key       string
	logger    tenantQueryLogStore
	expiresAt *time.Time
}

type tenantQueryLogStore interface {
	server.QueryLogSink
	DeleteBefore(time.Time, string) (int64, error)
	Compact()
	Stop()
}

func newTenantQueryLogManager(
	base server.Config,
	config func() configstore.QueryLogConfig,
	orgIDs func() []string,
	resolve func(context.Context, string) (tenantQueryLogRuntime, error),
) *tenantQueryLogManager {
	return &tenantQueryLogManager{
		base:    base,
		config:  config,
		orgIDs:  orgIDs,
		resolve: resolve,
		newLog: func(cfg server.Config, dlCfg server.DuckLakeConfig) (tenantQueryLogStore, error) {
			return server.NewQueryLoggerForDuckLake(cfg, dlCfg)
		},
		now:         time.Now,
		loggers:     make(map[string]*tenantQueryLogState),
		orgKeys:     make(map[string]string),
		resolving:   make(map[string]struct{}),
		nextResolve: make(map[string]time.Time),
	}
}

func (m *tenantQueryLogManager) QueryLogSinkForOrg(orgID string) server.QueryLogSink {
	logger, shouldResolve, stale := m.cachedLoggerForOrg(orgID)
	if len(stale) > 0 {
		go stopTenantQueryLogStores(stale)
	}
	if shouldResolve {
		m.resolveLoggerInBackground(orgID)
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

func (m *tenantQueryLogManager) RunRetention(ctx context.Context) {
	if m == nil {
		return
	}
	cfg := m.currentConfig()
	if !cfg.Enabled || cfg.RetentionPeriodS <= 0 {
		return
	}

	interval := time.Duration(cfg.RetentionIntervalS) * time.Second
	if interval <= 0 {
		interval = defaultTenantQueryLogRetentionInterval
	}
	now := m.now()

	m.mu.Lock()
	if m.stopped || m.retentionInProcess || (!m.lastRetention.IsZero() && now.Sub(m.lastRetention) < interval) {
		m.mu.Unlock()
		return
	}
	m.retentionInProcess = true
	m.mu.Unlock()

	hadError := false
	completed := false
	defer func() {
		m.mu.Lock()
		m.retentionInProcess = false
		if completed && !hadError {
			m.lastRetention = now
		}
		m.mu.Unlock()
	}()

	cutoff := now.Add(-time.Duration(cfg.RetentionPeriodS) * time.Second)
	orgIDs := m.currentOrgIDs()
	for _, orgID := range orgIDs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		orgCtx, cancel := context.WithTimeout(ctx, queryLogRetentionOrgTimeout)
		logger, err := m.loggerForOrg(orgCtx, orgID)
		if err != nil {
			cancel()
			hadError = true
			slog.Warn("querylog: retention skipped for org; logger unavailable.", "org", orgID, "error", err)
			continue
		}
		if logger == nil {
			cancel()
			hadError = true
			slog.Warn("querylog: retention skipped for org; logger unavailable.", "org", orgID)
			continue
		}
		deleted, err := logger.DeleteBefore(cutoff, orgID)
		if err != nil {
			cancel()
			hadError = true
			slog.Warn("querylog: retention delete failed.", "org", orgID, "error", err)
			continue
		}
		cancel()
		if deleted > 0 {
			logger.Compact()
			slog.Info("querylog: retention deleted old rows.", "org", orgID, "rows", deleted, "cutoff", cutoff)
		}
	}
	if ctx.Err() != nil {
		return
	}
	completed = true
}

func (m *tenantQueryLogManager) loggerForOrg(ctx context.Context, orgID string) (tenantQueryLogStore, error) {
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
	if key := m.orgKeys[orgID]; key != "" {
		if state := m.loggers[key]; state != nil && !queryLogCredentialsExpired(state.expiresAt, now) {
			logger := state.logger
			m.mu.Unlock()
			return logger, nil
		}
	}
	m.mu.Unlock()

	if m.resolve == nil {
		return nil, fmt.Errorf("tenant query log resolver is not configured")
	}
	resolveCtx, cancel := context.WithTimeout(ctx, queryLogManagerResolveTimeout)
	runtime, err := m.resolve(resolveCtx, orgID)
	cancel()
	if err != nil {
		return nil, err
	}
	if runtime.DuckLake.MetadataStore == "" {
		return nil, nil
	}
	key := queryLogRuntimeKey(runtime.DuckLake)

	m.mu.Lock()
	if state := m.loggers[key]; state != nil && !queryLogCredentialsExpired(state.expiresAt, now) {
		m.orgKeys[orgID] = key
		logger := state.logger
		m.mu.Unlock()
		return logger, nil
	}
	m.mu.Unlock()

	base := m.base
	base.QueryLog = serverQueryLogConfig(base.QueryLog, cfg)
	base.DuckLake = runtime.DuckLake
	logger, err := m.newLog(base, runtime.DuckLake)
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
	defer m.mu.Unlock()
	if m.stopped {
		logger.Stop()
		return nil, nil
	}
	if state := m.loggers[key]; state != nil && !queryLogCredentialsExpired(state.expiresAt, m.now()) {
		logger.Stop()
		m.orgKeys[orgID] = key
		return state.logger, nil
	}
	if old := m.loggers[key]; old != nil {
		old.logger.Stop()
	}
	m.loggers[key] = &tenantQueryLogState{key: key, logger: logger, expiresAt: runtime.ExpiresAt}
	m.orgKeys[orgID] = key
	slog.Info("querylog: tenant logger enabled.", "org", orgID, "logger_key", key, "expires_at", runtime.ExpiresAt)
	return logger, nil
}

func (m *tenantQueryLogManager) cachedLoggerForOrg(orgID string) (tenantQueryLogStore, bool, []tenantQueryLogStore) {
	if m == nil || orgID == "" {
		return nil, false, nil
	}
	cfg := m.currentConfig()
	sig := queryLogConfigSignature(cfg)
	now := m.now()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return nil, false, nil
	}

	var stale []tenantQueryLogStore
	if !cfg.Enabled {
		stale = m.clearLoggersLocked()
		m.configSig = sig
		return nil, false, stale
	}
	if sig != m.configSig {
		stale = m.clearLoggersLocked()
		m.configSig = sig
		m.nextResolve = make(map[string]time.Time)
	}
	if key := m.orgKeys[orgID]; key != "" {
		if state := m.loggers[key]; state != nil {
			if queryLogCredentialsExpired(state.expiresAt, now) {
				delete(m.orgKeys, orgID)
			} else {
				resolve := false
				if queryLogCredentialsExpiring(state.expiresAt, now) {
					resolve = m.markResolvingLocked(orgID, now)
				}
				return state.logger, resolve, stale
			}
		}
	}
	return nil, m.markResolvingLocked(orgID, now), stale
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

func (m *tenantQueryLogManager) resolveLoggerInBackground(orgID string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), queryLogManagerResolveTimeout)
		logger, err := m.loggerForOrg(ctx, orgID)
		cancel()

		m.mu.Lock()
		delete(m.resolving, orgID)
		if err != nil || logger == nil {
			m.nextResolve[orgID] = m.now().Add(queryLogResolveRetryInterval)
		} else {
			delete(m.nextResolve, orgID)
		}
		m.mu.Unlock()

		if err != nil {
			slog.Warn("querylog: tenant logger unavailable.", "org", orgID, "error", err)
		}
	}()
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
	return loggers
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

func (m *tenantQueryLogManager) currentOrgIDs() []string {
	if m.orgIDs == nil {
		return nil
	}
	orgIDs := m.orgIDs()
	sort.Strings(orgIDs)
	return orgIDs
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
