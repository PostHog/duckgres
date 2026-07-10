package sql

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/provision"
	"gopkg.in/yaml.v3"
)

const (
	StepTypeSQL        = "sql"
	StepTypeSQLCatalog = "sql_catalog"
)

type RetryConfig struct {
	MaxAttempts  int
	RetryBackoff time.Duration
	Sleep        func(context.Context, time.Duration) error
}

type ExecutorConfig struct {
	ProvisionState *provision.State
	Connection     ConnectionConfig
	Driver         Driver
	Retry          RetryConfig
	State          *State
}

type Executor struct {
	provisionState *provision.State
	connection     ConnectionConfig
	driver         Driver
	retry          RetryConfig
	state          *State
}

type State struct {
	mu      sync.Mutex
	results map[string]StepResult
}

type StepResult struct {
	StepID   string
	QueryID  string
	Rows     int64
	Attempts int
	Duration time.Duration
}

type querySpec struct {
	ID      string
	SQL     string
	Catalog string
}

type catalogFile struct {
	Name        string             `yaml:"name"`
	Description string             `yaml:"description"`
	Queries     []catalogFileQuery `yaml:"queries"`
}

type catalogFileQuery struct {
	ID      string `yaml:"id"`
	SQL     string `yaml:"sql"`
	Catalog string `yaml:"catalog"`
}

func NewExecutor(cfg ExecutorConfig) *Executor {
	driver := cfg.Driver
	if driver == nil {
		driver = NewDatabaseDriver()
	}
	state := cfg.State
	if state == nil {
		state = NewState()
	}
	retry := cfg.Retry
	if retry.MaxAttempts == 0 {
		retry.MaxAttempts = 12
	}
	if retry.RetryBackoff == 0 {
		retry.RetryBackoff = 10 * time.Second
	}
	if retry.Sleep == nil {
		retry.Sleep = sleepContext
	}
	return &Executor{
		provisionState: cfg.ProvisionState,
		connection:     cfg.Connection,
		driver:         driver,
		retry:          retry,
		state:          state,
	}
}

func NewState() *State {
	return &State{results: make(map[string]StepResult)}
}

func (e *Executor) State() *State {
	return e.state
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
	switch step.Type {
	case StepTypeSQL:
		spec, err := parseSQLStep(step)
		if err != nil {
			return err
		}
		return e.executeQuery(ctx, step, spec, step.ID)
	case StepTypeSQLCatalog:
		specs, err := parseCatalogStep(step)
		if err != nil {
			return err
		}
		for _, spec := range specs {
			resultID := step.ID + "/" + spec.ID
			if err := e.executeQuery(ctx, step, spec, resultID); err != nil {
				return err
			}
		}
		return nil
	default:
		return classified(ErrorClassUnsupportedStep, fmt.Errorf("unsupported SQL step type %q", step.Type))
	}
}

func (e *Executor) executeQuery(ctx context.Context, step core.Step, spec querySpec, resultID string) error {
	req, err := e.queryRequest(step, spec, resultID)
	if err != nil {
		return err
	}
	retry, err := retryConfigForStep(step, e.retry)
	if err != nil {
		return err
	}

	var attempts int
	var lastErr error
	for attempts = 1; attempts <= retry.MaxAttempts; attempts++ {
		result, err := e.driver.Execute(ctx, req)
		if err == nil {
			if minRows, ok, err := intFromWith(step, "min_rows"); err != nil {
				return err
			} else if ok && result.Rows < int64(minRows) {
				return classified(ErrorClassSQL, fmt.Errorf("execute SQL step %s query %s returned %d rows, want at least %d", step.ID, spec.ID, result.Rows, minRows))
			}
			e.state.StoreResult(StepResult{
				StepID:   resultID,
				QueryID:  spec.ID,
				Rows:     result.Rows,
				Attempts: attempts,
				Duration: result.Duration,
			})
			return nil
		}
		lastErr = err
		if !IsTransientStartupError(err) {
			return classified(ErrorClassSQL, fmt.Errorf("execute SQL step %s query %s: %w", step.ID, spec.ID, err))
		}
		if attempts == retry.MaxAttempts {
			break
		}
		if sleepErr := retry.Sleep(ctx, retry.RetryBackoff); sleepErr != nil {
			return sleepErr
		}
	}
	return classified(ErrorClassTransientTimeout, fmt.Errorf("%w for SQL step %s query %s after %d attempts: %w", ErrTransientRetriesExhausted, step.ID, spec.ID, attempts, lastErr))
}

func (e *Executor) queryRequest(step core.Step, spec querySpec, resultID string) (QueryRequest, error) {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return QueryRequest{}, err
	}
	username := stringFromWith(step, "username", "root")
	password := stringFromWith(step, "password", "")
	if password == "" {
		if e.provisionState == nil {
			return QueryRequest{}, invalidStep(step.ID, "provision state is required when with.password is omitted")
		}
		resp, ok := e.provisionState.ProvisionResponse(orgID)
		if !ok {
			return QueryRequest{}, invalidStep(step.ID, "no provision response found for org %q", orgID)
		}
		if resp.Username != "" {
			username = resp.Username
		}
		password = resp.Password
	}

	cfg := e.connection
	cfg.OrgID = orgID
	cfg.Database = spec.Catalog
	cfg.Username = username
	cfg.Password = password
	connection, err := cfg.PGWire()
	if err != nil {
		return QueryRequest{}, err
	}
	return QueryRequest{
		StepID:  resultID,
		QueryID: spec.ID,
		OrgID:   orgID,
		Catalog: spec.Catalog,
		SQL:     spec.SQL,
		PGWire:  connection,
	}, nil
}

func parseSQLStep(step core.Step) (querySpec, error) {
	sqlText, err := sqlFromStep(step)
	if err != nil {
		return querySpec{}, err
	}
	return querySpec{
		ID:      stringFromWith(step, "query_id", step.ID),
		SQL:     sqlText,
		Catalog: stringFromWith(step, "catalog", "ducklake"),
	}, nil
}

func parseCatalogStep(step core.Step) ([]querySpec, error) {
	file := stringFromWith(step, "file", "")
	if file != "" {
		if _, ok := step.With["queries"]; ok {
			return nil, invalidStep(step.ID, "with.file and with.queries are mutually exclusive")
		}
		return parseCatalogFile(step, file)
	}
	raw, ok := step.With["queries"]
	if !ok {
		return nil, invalidStep(step.ID, "with.queries or with.file is required")
	}
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, invalidStep(step.ID, "with.queries must be a non-empty list")
	}
	specs := make([]querySpec, 0, len(items))
	for i, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, invalidStep(step.ID, "with.queries[%d] must be a map", i)
		}
		id, ok := m["id"].(string)
		if !ok || id == "" {
			return nil, invalidStep(step.ID, "with.queries[%d].id must be a non-empty string", i)
		}
		sqlText, ok := m["sql"].(string)
		if !ok || strings.TrimSpace(sqlText) == "" {
			return nil, invalidStep(step.ID, "with.queries[%d].sql must be a non-empty string", i)
		}
		catalog := stringFromMap(m, "catalog", stringFromWith(step, "catalog", "ducklake"))
		specs = append(specs, querySpec{ID: id, SQL: sqlText, Catalog: catalog})
	}
	return specs, nil
}

func parseCatalogFile(step core.Step, file string) ([]querySpec, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, invalidStep(step.ID, "read catalog file %s: %v", file, err)
	}
	var catalog catalogFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&catalog); err != nil {
		return nil, invalidStep(step.ID, "parse catalog file %s: %v", file, err)
	}
	if len(catalog.Queries) == 0 {
		return nil, invalidStep(step.ID, "catalog file %s must contain at least one query", file)
	}
	defaultCatalog := stringFromWith(step, "catalog", "ducklake")
	specs := make([]querySpec, 0, len(catalog.Queries))
	for i, item := range catalog.Queries {
		item.ID = strings.TrimSpace(item.ID)
		item.SQL = strings.TrimSpace(item.SQL)
		item.Catalog = strings.TrimSpace(item.Catalog)
		if item.ID == "" {
			return nil, invalidStep(step.ID, "catalog file %s queries[%d].id must be non-empty", file, i)
		}
		if item.SQL == "" {
			return nil, invalidStep(step.ID, "catalog file %s queries[%d].sql must be non-empty", file, i)
		}
		catalogName := item.Catalog
		if catalogName == "" {
			catalogName = defaultCatalog
		}
		specs = append(specs, querySpec{ID: item.ID, SQL: item.SQL, Catalog: catalogName})
	}
	return specs, nil
}

func sqlFromStep(step core.Step) (string, error) {
	if sqlText := stringFromWith(step, "sql", ""); strings.TrimSpace(sqlText) != "" {
		resolved, err := core.ResolveEnvTemplates(sqlText)
		if err != nil {
			return "", invalidStep(step.ID, "%v", err)
		}
		return resolved, nil
	}
	file := stringFromWith(step, "file", "")
	if file == "" {
		return "", invalidStep(step.ID, "with.sql or with.file is required")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", invalidStep(step.ID, "read SQL file %s: %v", file, err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return "", invalidStep(step.ID, "SQL file %s is empty", file)
	}
	resolved, err := core.ResolveEnvTemplates(string(raw))
	if err != nil {
		return "", invalidStep(step.ID, "%v", err)
	}
	return resolved, nil
}

func retryConfigForStep(step core.Step, base RetryConfig) (RetryConfig, error) {
	if maxAttempts, ok, err := intFromWith(step, "max_attempts"); err != nil {
		return RetryConfig{}, err
	} else if ok {
		base.MaxAttempts = maxAttempts
	}
	if retryInterval, ok, err := durationFromWith(step, "retry_interval"); err != nil {
		return RetryConfig{}, err
	} else if ok {
		base.RetryBackoff = retryInterval
	}
	if base.MaxAttempts <= 0 {
		return RetryConfig{}, invalidStep(step.ID, "max_attempts must be greater than zero")
	}
	return base, nil
}

func IsTransientStartupError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"capacity exhausted",
		"no duckgres worker",
		"still provisioning",
		"failed to initialize session",
		"timed out waiting for an available worker",
		"failed to start",
		"spawn sized worker",
		"failed to detect attached catalogs",
		"eof",
		"connection reset by peer",
		"i/o timeout",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func requiredString(step core.Step, key string) (string, error) {
	value, ok := step.With[key]
	if !ok {
		return "", invalidStep(step.ID, "with.%s is required", key)
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return "", invalidStep(step.ID, "with.%s must be a non-empty string", key)
	}
	return text, nil
}

func stringFromWith(step core.Step, key, fallback string) string {
	value, ok := step.With[key]
	if !ok {
		return fallback
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return fallback
	}
	return text
}

func stringFromMap(m map[string]any, key, fallback string) string {
	value, ok := m[key]
	if !ok {
		return fallback
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return fallback
	}
	return text
}

func durationFromWith(step core.Step, key string) (time.Duration, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	text, ok := value.(string)
	if !ok {
		return 0, false, invalidStep(step.ID, "with.%s must be a Go duration string", key)
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return 0, false, invalidStep(step.ID, "with.%s must be a Go duration: %v", key, err)
	}
	if parsed < 0 {
		return 0, false, invalidStep(step.ID, "with.%s must not be negative", key)
	}
	return parsed, true, nil
}

func intFromWith(step core.Step, key string) (int, bool, error) {
	value, ok := step.With[key]
	if !ok {
		return 0, false, nil
	}
	var parsed int
	switch typed := value.(type) {
	case int:
		parsed = typed
	case int64:
		parsed = int(typed)
	case float64:
		if typed != float64(int(typed)) {
			return 0, false, invalidStep(step.ID, "with.%s must be an integer", key)
		}
		parsed = int(typed)
	case string:
		var err error
		parsed, err = strconv.Atoi(typed)
		if err != nil {
			return 0, false, invalidStep(step.ID, "with.%s must be an integer: %v", key, err)
		}
	default:
		return 0, false, invalidStep(step.ID, "with.%s must be an integer", key)
	}
	if parsed < 0 {
		return 0, false, invalidStep(step.ID, "with.%s must not be negative", key)
	}
	return parsed, true, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
