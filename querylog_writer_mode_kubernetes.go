//go:build kubernetes

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/controlplane"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/internal/cliboot"
	"github.com/posthog/duckgres/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type staticQueryLogDuckLakeConfigResolver struct {
	cfg server.Config
}

var queryLogWriterBootstrapBundledExtensions = server.BootstrapBundledExtensions

func (r staticQueryLogDuckLakeConfigResolver) ResolveQueryLogDuckLakeConfig(context.Context, string) (server.QueryLogDuckLakeResolvedConfig, error) {
	if r.cfg.DuckLake.MetadataStore == "" {
		return server.QueryLogDuckLakeResolvedConfig{}, errors.New("querylog: static DuckLake metadata store is required")
	}
	cfg := r.cfg
	cfg.QueryLog.Enabled = true
	cfg.QueryLog.Sink = server.QueryLogSinkDuckLake
	return server.QueryLogDuckLakeResolvedConfig{Config: cfg}, nil
}

type configStoreQueryLogDuckLakeConfigResolver struct {
	base                  server.Config
	store                 *configstore.ConfigStore
	clientset             kubernetes.Interface
	namespace             string
	stsBroker             *controlplane.STSBroker
	defaultSpecVersion    string
	resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)
}

const queryLogWriterEmptyMetadataSecretRefError = "metadata store credentials: secret ref requires name and key"

func (r *configStoreQueryLogDuckLakeConfigResolver) ResolveQueryLogDuckLakeConfig(ctx context.Context, orgID string) (server.QueryLogDuckLakeResolvedConfig, error) {
	snap := r.store.Snapshot()
	if snap == nil {
		return server.QueryLogDuckLakeResolvedConfig{}, errors.New("querylog: config snapshot unavailable")
	}
	org, ok := snap.Orgs[orgID]
	if !ok || org == nil {
		return server.QueryLogDuckLakeResolvedConfig{}, fmt.Errorf("%w: org %q not found in config snapshot", server.ErrQueryLogNoDuckLakeTarget, orgID)
	}
	payload, err := controlplane.BuildTenantActivationPayloadWithResolver(
		ctx,
		r.clientset,
		r.namespace,
		org,
		r.stsBroker,
		r.defaultSpecVersion,
		r.resolveDucklingStatus,
	)
	if err != nil {
		if queryLogWriterActivationErrorIsNoDuckLakeTarget(err) {
			return server.QueryLogDuckLakeResolvedConfig{}, fmt.Errorf("%w: %v", server.ErrQueryLogNoDuckLakeTarget, err)
		}
		return server.QueryLogDuckLakeResolvedConfig{}, err
	}
	cfg := r.base
	cfg.DuckLake = payload.DuckLake
	cfg.QueryLog.Enabled = true
	cfg.QueryLog.Sink = server.QueryLogSinkDuckLake
	return server.QueryLogDuckLakeResolvedConfig{
		Config:               cfg,
		CredentialsExpiresAt: payload.S3CredentialsExpiresAt,
	}, nil
}

func queryLogWriterActivationErrorIsNoDuckLakeTarget(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), queryLogWriterEmptyMetadataSecretRefError)
}

func runQueryLogWriter(ctx context.Context, cfg server.Config, resolved configresolve.Resolved) error {
	if !cfg.QueryLog.Enabled {
		return errors.New("querylog: query log is disabled")
	}
	if err := queryLogWriterBootstrapBundledExtensions(cfg.DataDir); err != nil {
		return fmt.Errorf("querylog: bootstrap bundled DuckDB extensions: %w", err)
	}

	metricsSrv := cliboot.InitMetrics()
	defer shutdownQueryLogWriterMetrics(metricsSrv)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer, err := server.NewKafkaQueryLogConsumer(cfg.QueryLog)
	if err != nil {
		return err
	}
	defer func() { _ = consumer.Close() }()

	cfgResolver, err := newQueryLogDuckLakeConfigResolver(ctx, cfg, resolved)
	if err != nil {
		return err
	}
	writerResolver, err := server.NewQueryLogDuckLakeEntryWriterResolver(cfgResolver)
	if err != nil {
		return err
	}
	defer func() { _ = writerResolver.Close() }()
	writerResolver.StartIdlePruner(ctx)

	writer, err := server.NewQueryLogKafkaWriter(consumer, writerResolver)
	if err != nil {
		return err
	}
	writer.ConfigureBatching(cfg.QueryLog.BatchSize, cfg.QueryLog.FlushInterval)

	slog.Info("Starting query-log Kafka writer.",
		"topic", cfg.QueryLog.Kafka.Topic,
		"brokers", len(cfg.QueryLog.Kafka.Brokers),
		"group_id", cfg.QueryLog.Kafka.GroupID,
		"flush_interval", cfg.QueryLog.FlushInterval,
		"batch_size", cfg.QueryLog.BatchSize,
	)
	if err := writer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func newQueryLogDuckLakeConfigResolver(ctx context.Context, cfg server.Config, resolved configresolve.Resolved) (server.QueryLogDuckLakeConfigResolver, error) {
	if strings.TrimSpace(resolved.ConfigStoreConn) == "" {
		return staticQueryLogDuckLakeConfigResolver{cfg: cfg}, nil
	}

	store, err := configstore.NewConfigStore(resolved.ConfigStoreConn, resolved.ConfigPollInterval)
	if err != nil {
		return nil, err
	}
	store.Start(ctx)
	go func() {
		<-ctx.Done()
		sqlDB, err := store.DB().DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	}()

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	namespace, err := queryLogWriterNamespace(resolved.K8sWorkerNamespace)
	if err != nil {
		return nil, err
	}

	var stsBroker *controlplane.STSBroker
	if resolved.AWSRegion != "" {
		stsBroker, err = controlplane.NewSTSBroker(ctx, resolved.AWSRegion)
		if err != nil {
			slog.Warn("STS broker unavailable for query-log writer; STS-backed orgs will fail until it recovers.", "error", err)
		}
	}

	var resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)
	ducklingClient, err := provisioner.NewDucklingClient()
	if err != nil {
		slog.Warn("Duckling client unavailable for query-log writer; using config store runtime only.", "error", err)
	} else {
		resolveDucklingStatus = func(ctx context.Context, orgID string) (*provisioner.DucklingStatus, error) {
			// The CR name is the warehouse row's duckling_name (authoritative,
			// never derived). The column is NOT NULL and backfilled; the org-ID
			// fallback only covers legacy in-flight rows with an empty value.
			warehouse, err := store.GetManagedWarehouse(orgID)
			if err != nil {
				return nil, fmt.Errorf("resolve duckling name for org %q: %w", orgID, err)
			}
			name := warehouse.DucklingName
			if name == "" {
				name = orgID
			}
			return ducklingClient.Get(ctx, name)
		}
	}

	return &configStoreQueryLogDuckLakeConfigResolver{
		base:                  cfg,
		store:                 store,
		clientset:             clientset,
		namespace:             namespace,
		stsBroker:             stsBroker,
		defaultSpecVersion:    resolved.DuckLakeDefaultSpecVersion,
		resolveDucklingStatus: resolveDucklingStatus,
	}, nil
}

func queryLogWriterNamespace(configured string) (string, error) {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured), nil
	}
	ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("querylog: k8s worker namespace is required when service account namespace cannot be read: %w", err)
	}
	return strings.TrimSpace(string(ns)), nil
}

func shutdownQueryLogWriterMetrics(srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
