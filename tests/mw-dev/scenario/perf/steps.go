package perf

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/mw-dev/scenario/sql"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
	pgdriver "github.com/posthog/duckgres/tests/perf/drivers/pgwire"
	trinodriver "github.com/posthog/duckgres/tests/perf/drivers/trino"
)

const StepTypePerfQueries = "perf_queries"

type DriverFactory interface {
	NewPGWire(connection scenariosql.PGWireConnection) (perfcore.ProtocolDriver, error)
	NewTrino(ctx context.Context, connection trinodriver.ConnectionConfig) (perfcore.ProtocolDriver, error)
}

type ExecutorConfig struct {
	ProvisionState *provision.State
	Connection     scenariosql.ConnectionConfig
	OutputDir      string
	DriverFactory  DriverFactory
	State          *State
	Now            func() time.Time
}

type Executor struct {
	provisionState *provision.State
	connection     scenariosql.ConnectionConfig
	outputDir      string
	driverFactory  DriverFactory
	state          *State
	now            func() time.Time
}

type State struct {
	mu      sync.Mutex
	results map[string]StepResult
}

type StepResult struct {
	StepID    string
	OutputDir string
	Summary   perfcore.RunSummary
}

type stepSpec struct {
	OrgID             string
	Username          string
	Password          string
	CatalogFile       string
	Targets           []perfcore.Protocol
	RunID             string
	DatasetVersion    string
	Database          string
	OutputSubdir      string
	ReadOnly          bool
	FailOnQueryErrors bool
	TrinoSchema       string
	TrinoCACertFile   string
	TrinoStartup      trinodriver.StartupOptions
}

type defaultDriverFactory struct{}

func NewExecutor(cfg ExecutorConfig) *Executor {
	factory := cfg.DriverFactory
	if factory == nil {
		factory = defaultDriverFactory{}
	}
	state := cfg.State
	if state == nil {
		state = NewState()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Executor{
		provisionState: cfg.ProvisionState,
		connection:     cfg.Connection,
		outputDir:      cfg.OutputDir,
		driverFactory:  factory,
		state:          state,
		now:            now,
	}
}

func NewState() *State {
	return &State{results: make(map[string]StepResult)}
}

func (e *Executor) State() *State {
	return e.state
}

func (e *Executor) OutputDir() string {
	return e.outputDir
}

func (s *State) StoreResult(result StepResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result.StepID] = result
}

func (s *State) Result(stepID string) (StepResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.results[stepID]
	return result, ok
}

func (e *Executor) ExecuteStep(ctx context.Context, step core.Step) error {
	if step.Type != StepTypePerfQueries {
		return classified(ErrorClassUnsupportedStep, fmt.Errorf("unsupported perf step type %q", step.Type))
	}
	spec, err := e.parseStep(step)
	if err != nil {
		return err
	}
	catalog, err := perfcore.LoadCatalog(spec.CatalogFile)
	if err != nil {
		return classified(ErrorClassConfig, err)
	}
	catalog, err = restrictCatalogTargets(catalog, spec.Targets)
	if err != nil {
		return classified(ErrorClassConfig, err)
	}
	if spec.ReadOnly {
		if err := perfcore.ValidateReadOnlyCatalog(catalog); err != nil {
			return classified(ErrorClassConfig, fmt.Errorf("read-only perf catalog validation failed: %w", err))
		}
	}

	drivers, err := e.driversForCatalog(ctx, catalog, spec)
	if err != nil {
		return err
	}
	defer closeDrivers(drivers)

	perfDir := filepath.Join(e.outputDir, spec.OutputSubdir)
	sink, err := perfcore.NewArtifactSink(perfDir)
	if err != nil {
		return classified(ErrorClassConfig, err)
	}
	sinkClosed := false
	closeSink := func(summary perfcore.RunSummary, metrics string) error {
		if sinkClosed {
			return nil
		}
		sinkClosed = true
		return sink.Close(summary, metrics)
	}
	runner := perfcore.NewQueryRunner(perfcore.RunnerConfig{
		RunID:          spec.RunID,
		Catalog:        catalog,
		DatasetVersion: spec.DatasetVersion,
		Drivers:        drivers,
		Sink:           closingSink{sink: sink, closeFunc: closeSink},
		Now:            e.now,
	})
	summary, err := runner.Run(ctx)
	if err != nil {
		_ = closeSink(summary, "")
		return classified(ErrorClassPerf, err)
	}
	result := StepResult{
		StepID:    step.ID,
		OutputDir: perfDir,
		Summary:   summary,
	}
	e.state.StoreResult(result)
	if spec.FailOnQueryErrors && summary.TotalErrors > 0 {
		return classified(ErrorClassPerf, fmt.Errorf("perf step %s recorded %d query error(s)", step.ID, summary.TotalErrors))
	}
	return nil
}

type closingSink struct {
	sink      perfcore.ResultSink
	closeFunc func(perfcore.RunSummary, string) error
}

func (s closingSink) Record(result perfcore.QueryResult) error {
	return s.sink.Record(result)
}

func (s closingSink) Close(summary perfcore.RunSummary, serverMetrics string) error {
	return s.closeFunc(summary, serverMetrics)
}

func (e *Executor) parseStep(step core.Step) (stepSpec, error) {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return stepSpec{}, err
	}
	catalogFile, err := requiredString(step, "catalog_file")
	if err != nil {
		return stepSpec{}, err
	}
	runID, err := requiredString(step, "run_id")
	if err != nil {
		return stepSpec{}, err
	}
	if e.outputDir == "" {
		return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("perf output dir is required"))
	}
	targets, err := targetsFromWith(step)
	if err != nil {
		return stepSpec{}, err
	}
	trinoStartupTimeout, err := durationFromWith(step, "trino_startup_timeout")
	if err != nil {
		return stepSpec{}, err
	}
	trinoStartupPollInterval, err := durationFromWith(step, "trino_startup_poll_interval")
	if err != nil {
		return stepSpec{}, err
	}

	username := stringFromWith(step, "username", "root")
	password := stringFromWith(step, "password", "")
	if password == "" {
		if e.provisionState == nil {
			return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("provision state is required when with.password is omitted"))
		}
		resp, ok := e.provisionState.ProvisionResponse(orgID)
		if !ok {
			return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("no provision response found for org %q", orgID))
		}
		if resp.Username != "" {
			username = resp.Username
		}
		password = resp.Password
	}

	return stepSpec{
		OrgID:             orgID,
		Username:          username,
		Password:          password,
		CatalogFile:       catalogFile,
		Targets:           targets,
		RunID:             runID,
		DatasetVersion:    stringFromWith(step, "dataset_version", ""),
		Database:          stringFromWith(step, "catalog", "ducklake"),
		OutputSubdir:      stringFromWith(step, "output_subdir", "perf"),
		ReadOnly:          boolFromWith(step, "read_only", true),
		FailOnQueryErrors: boolFromWith(step, "fail_on_query_errors", true),
		TrinoSchema:       stringFromWith(step, "trino_schema", "posthog"),
		TrinoCACertFile:   stringFromWith(step, "trino_ca_cert_file", ""),
		TrinoStartup: trinodriver.StartupOptions{
			Timeout:      trinoStartupTimeout,
			PollInterval: trinoStartupPollInterval,
		},
	}, nil
}

func targetsFromWith(step core.Step) ([]perfcore.Protocol, error) {
	raw, ok := step.With["targets"]
	if !ok {
		return nil, nil
	}
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.targets must be a non-empty list", step.ID))
	}

	targets := make([]perfcore.Protocol, 0, len(values))
	seen := make(map[perfcore.Protocol]struct{}, len(values))
	for i, rawTarget := range values {
		value, ok := rawTarget.(string)
		if !ok || value == "" {
			return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.targets[%d] must be a non-empty string", step.ID, i))
		}
		target := perfcore.Protocol(value)
		switch target {
		case perfcore.ProtocolPGWire, perfcore.ProtocolTrino:
		default:
			return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.targets[%d] has unsupported perf protocol %q", step.ID, i, target))
		}
		if _, exists := seen[target]; exists {
			return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.targets contains duplicate protocol %q", step.ID, target))
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

func restrictCatalogTargets(catalog perfcore.Catalog, targets []perfcore.Protocol) (perfcore.Catalog, error) {
	if targets == nil {
		return catalog, nil
	}

	available := make(map[perfcore.Protocol]struct{}, len(catalog.Targets))
	for _, target := range catalog.Targets {
		available[target] = struct{}{}
	}
	for _, target := range targets {
		if _, ok := available[target]; !ok {
			return perfcore.Catalog{}, fmt.Errorf("perf target %q is not present in perf catalog", target)
		}
	}
	catalog.Targets = append([]perfcore.Protocol(nil), targets...)
	return catalog, nil
}

func (e *Executor) driversForCatalog(ctx context.Context, catalog perfcore.Catalog, spec stepSpec) (map[perfcore.Protocol]perfcore.ProtocolDriver, error) {
	drivers := make(map[perfcore.Protocol]perfcore.ProtocolDriver, len(catalog.Targets))
	var success bool
	defer func() {
		if !success {
			closeDrivers(drivers)
		}
	}()
	for _, target := range catalog.Targets {
		if _, ok := drivers[target]; ok {
			continue
		}
		switch target {
		case perfcore.ProtocolPGWire:
			connection, err := e.pgwireConnection(spec)
			if err != nil {
				return nil, err
			}
			driver, err := e.driverFactory.NewPGWire(connection)
			if err != nil {
				return nil, classified(ErrorClassConfig, fmt.Errorf("create pgwire perf driver: %w", err))
			}
			drivers[target] = driver
		case perfcore.ProtocolTrino:
			connection, err := e.trinoConnection(spec)
			if err != nil {
				return nil, err
			}
			driver, err := e.driverFactory.NewTrino(ctx, connection)
			if err != nil {
				return nil, classified(ErrorClassConfig, fmt.Errorf("create Trino perf driver: %w", err))
			}
			drivers[target] = driver
		default:
			return nil, classified(ErrorClassConfig, fmt.Errorf("unsupported perf target protocol %q", target))
		}
	}
	success = true
	return drivers, nil
}

func (e *Executor) trinoConnection(spec stepSpec) (trinodriver.ConnectionConfig, error) {
	if e.provisionState == nil {
		return trinodriver.ConnectionConfig{}, classified(ErrorClassConfig, fmt.Errorf("provision state is required for Trino perf target"))
	}
	status, ok := e.provisionState.TrinoStatus(spec.OrgID)
	if !ok {
		return trinodriver.ConnectionConfig{}, classified(ErrorClassConfig, fmt.Errorf("no Trino readiness state found for org %q; run wait_trino_ready before perf_queries", spec.OrgID))
	}
	if !status.Enabled || !status.Available || status.Status == nil ||
		status.Status.State != provision.WarehouseStateReady ||
		status.Cell.ID == "" || status.Status.Cell != status.Cell.ID {
		return trinodriver.ConnectionConfig{}, classified(ErrorClassConfig, fmt.Errorf("trino readiness state for org %q is incomplete; run wait_trino_ready before perf_queries", spec.OrgID))
	}
	if status.Cell.CoordinatorURL == "" {
		return trinodriver.ConnectionConfig{}, classified(ErrorClassConfig, fmt.Errorf("trino readiness state for org %q has no coordinator URL", spec.OrgID))
	}
	if status.Status.Principal == "" || status.Status.Catalog == "" {
		return trinodriver.ConnectionConfig{}, classified(ErrorClassConfig, fmt.Errorf("trino readiness state for org %q has no principal or catalog", spec.OrgID))
	}
	return trinodriver.ConnectionConfig{
		ServerURL:  status.Cell.CoordinatorURL,
		Username:   status.Status.Principal,
		Password:   spec.Password,
		Catalog:    status.Status.Catalog,
		Schema:     spec.TrinoSchema,
		Source:     "duckgres-perf",
		CACertFile: spec.TrinoCACertFile,
		Startup:    spec.TrinoStartup,
	}, nil
}

func (e *Executor) pgwireConnection(spec stepSpec) (scenariosql.PGWireConnection, error) {
	cfg := e.connection
	cfg.OrgID = spec.OrgID
	cfg.Database = spec.Database
	cfg.Username = spec.Username
	cfg.Password = spec.Password
	connection, err := cfg.PGWire()
	if err != nil {
		return scenariosql.PGWireConnection{}, classified(ErrorClassConfig, err)
	}
	return connection, nil
}

func closeDrivers(drivers map[perfcore.Protocol]perfcore.ProtocolDriver) {
	for _, driver := range drivers {
		_ = driver.Close()
	}
}

func (defaultDriverFactory) NewPGWire(connection scenariosql.PGWireConnection) (perfcore.ProtocolDriver, error) {
	db, err := connection.OpenDB()
	if err != nil {
		return nil, err
	}
	return pgdriver.NewWithDB(db), nil
}

func (defaultDriverFactory) NewTrino(ctx context.Context, connection trinodriver.ConnectionConfig) (perfcore.ProtocolDriver, error) {
	return trinodriver.New(ctx, connection)
}

func requiredString(step core.Step, key string) (string, error) {
	value, ok := step.With[key].(string)
	if !ok || value == "" {
		return "", classified(ErrorClassConfig, fmt.Errorf("step %s with.%s must be a non-empty string", step.ID, key))
	}
	return value, nil
}

func stringFromWith(step core.Step, key, fallback string) string {
	value, ok := step.With[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func boolFromWith(step core.Step, key string, fallback bool) bool {
	raw, ok := step.With[key]
	if !ok {
		return fallback
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func durationFromWith(step core.Step, key string) (time.Duration, error) {
	raw, ok := step.With[key]
	if !ok {
		return 0, nil
	}
	value, ok := raw.(string)
	if !ok || value == "" {
		return 0, classified(ErrorClassConfig, fmt.Errorf("step %s with.%s must be a positive duration string", step.ID, key))
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, classified(ErrorClassConfig, fmt.Errorf("step %s with.%s must be a positive duration string", step.ID, key))
	}
	return duration, nil
}
