//go:build kubernetes

package controlplane

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// Shared-pool configuration, resolved from the SAME registry file the existing
// fixed blue/green cells come from. A cell opts in with `mode: "shared-pool"`
// plus a `pool` block; everything without it keeps today's behavior byte for
// byte.
//
// The pool deliberately inherits the cell's identity — its routing group, its
// namespace and the catalog store's `cell_id` — because replacing compute must
// not rewrite which warehouse lives where.
const (
	envTrinoPoolEnabled         = "DUCKGRES_TRINO_POOL_ENABLED"
	envTrinoPoolOperatorEnabled = "DUCKGRES_TRINO_POOL_OPERATOR_ENABLED"
	envTrinoPoolGatewayURL      = "DUCKGRES_TRINO_POOL_GATEWAY_URL"

	trinoPoolModeFixed  = "fixed"
	trinoPoolModeShared = "shared-pool"

	maxBlueprintFileBytes = 1 << 20
)

// trinoRegisteredPool is the `pool` block of a registered cell.
type trinoRegisteredPool struct {
	DesiredInstances       int    `json:"desired_instances"`
	MinServing             int    `json:"min_serving"`
	MaxSurge               int    `json:"max_surge"`
	MaxRepair              int    `json:"max_repair"`
	BlueprintFile          string `json:"blueprint_file"`
	CoordinatorServicePort int32  `json:"coordinator_service_port"`
	NodeEnvironment        string `json:"node_environment"`
	// TenantAdmission turns on the Gateway's pooled admission restriction for
	// this pool. It is off by default because the restriction is deny-only: with
	// it on, a tenant whose principals have not been published yet cannot
	// dispatch work, which is correct but must be a deliberate choice.
	TenantAdmission bool `json:"tenant_admission,omitempty"`
}

// trinoPoolConfig is one resolved shared pool.
type trinoPoolConfig struct {
	PoolID       string
	PublicID     string
	RoutingGroup string
	Namespace    string
	Spec         configstore.TrinoPoolSpec
	Blueprint    *trinopool.Blueprint
	Pool         trinoRegisteredPool

	// Frozen marks a pool whose desired configuration could not be resolved.
	// Reconciliation then holds the last-good state: no creates, no drains, no
	// deletes. It is NOT a desired count of zero, and it is NOT a startup
	// failure — a bad ConfigMap must not take the control plane down.
	Frozen       bool
	FrozenReason string
}

func trinoPoolEnabled() bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolEnabled)))
	return err == nil && enabled
}

// trinoPoolOperatorEnabled reports whether this process may create, admit or
// delete instances. With the pool enabled but the operator off, the durable
// state is readable and nothing in Kubernetes or the Gateway is touched.
func trinoPoolOperatorEnabled() bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(envTrinoPoolOperatorEnabled)))
	return err == nil && enabled
}

// resolveTrinoPoolConfigs returns the shared pools declared in the registry.
// Cells without a pool block are ignored here and continue through the existing
// fixed-cell path untouched.
func resolveTrinoPoolConfigs() ([]trinoPoolConfig, error) {
	path := strings.TrimSpace(os.Getenv(envTrinoCellsFile))
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Trino registry: %w", err)
	}
	cells, err := parseTrinoCellRegistry(data)
	if err != nil {
		return nil, err
	}

	var configs []trinoPoolConfig
	for _, cell := range cells {
		mode := strings.TrimSpace(cell.Mode)
		if mode == "" || mode == trinoPoolModeFixed {
			continue
		}
		if mode != trinoPoolModeShared {
			return nil, fmt.Errorf("Trino cell %s has an unsupported mode %q", cell.ID, mode)
		}
		// Asking for a pool while the feature is off must fail loudly. Falling
		// back to fixed blue/green would give the operator a deployment shape
		// they did not ask for and would not be told about.
		if !trinoPoolEnabled() {
			return nil, fmt.Errorf("Trino cell %s declares shared-pool mode but %s is not enabled", cell.ID, envTrinoPoolEnabled)
		}
		config, err := resolveTrinoPoolConfig(cell)
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}
	return configs, nil
}

func resolveTrinoPoolConfig(cell trinoRegisteredCell) (trinoPoolConfig, error) {
	pool := cell.Pool
	if pool == nil {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s is in shared-pool mode but declares no pool block", cell.ID)
	}
	if len(cell.Backends) != 0 {
		// A pooled cell's members are created by the operator and recorded in
		// the config store. A static backend list would be a second, silently
		// competing source of truth for the same routing group.
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s is in shared-pool mode and must not declare static backends", cell.ID)
	}
	if pool.DesiredInstances < 1 {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s must declare at least one desired instance", cell.ID)
	}
	if pool.MinServing < 1 || pool.MinServing > pool.DesiredInstances {
		// A floor above the desired count can never be satisfied, so every
		// planned drain would be refused forever.
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s must keep its serving floor between one and the desired instance count", cell.ID)
	}
	if pool.MaxSurge < 0 || pool.MaxRepair < 0 {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s declares a negative budget", cell.ID)
	}
	if pool.CoordinatorServicePort < 1 || pool.CoordinatorServicePort > 65535 {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s declares an invalid coordinator service port", cell.ID)
	}
	if strings.TrimSpace(pool.NodeEnvironment) == "" {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s declares no node environment", cell.ID)
	}
	if strings.TrimSpace(pool.BlueprintFile) == "" {
		return trinoPoolConfig{}, fmt.Errorf("Trino cell %s declares no blueprint file", cell.ID)
	}

	config := trinoPoolConfig{
		PoolID:       registeredTrinoCellPrefix + cell.ID,
		PublicID:     cell.ID,
		RoutingGroup: cell.RoutingGroup,
		Namespace:    cell.Namespace,
		Pool:         *pool,
		Spec: configstore.TrinoPoolSpec{
			PoolID:           registeredTrinoCellPrefix + cell.ID,
			PublicID:         cell.ID,
			APIMode:          configstore.TrinoPoolAPIModeShared,
			DesiredInstances: pool.DesiredInstances,
			MinServing:       pool.MinServing,
			MaxSurge:         pool.MaxSurge,
			MaxRepair:        pool.MaxRepair,
		},
	}

	blueprint, err := loadTrinoPoolBlueprint(pool.BlueprintFile, cell.Namespace)
	if err != nil {
		// Freeze rather than fail: the last-good desired state is preserved,
		// the operator is told why, and nothing is created or deleted while the
		// configuration is unreadable.
		config.Frozen, config.FrozenReason = true, err.Error()
		return config, nil
	}
	config.Blueprint = blueprint
	config.Spec.DesiredReleaseID = blueprint.ReleaseID
	config.Spec.DesiredBlueprintDigest = blueprint.Digest()
	return config, nil
}

func loadTrinoPoolBlueprint(path, namespace string) (*trinopool.Blueprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("blueprint is unreadable: %w", err)
	}
	if info.Size() > maxBlueprintFileBytes {
		return nil, errors.New("blueprint exceeds the size limit")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("blueprint is unreadable: %w", err)
	}
	blueprint, err := trinopool.ParseBlueprint(data)
	if err != nil {
		return nil, fmt.Errorf("blueprint is invalid: %w", err)
	}
	if blueprint.Namespace != namespace {
		// Instances would land outside the namespace that holds the pool's
		// shared Secrets, service accounts and network policy.
		return nil, fmt.Errorf("blueprint targets namespace %q, the cell is in %q", blueprint.Namespace, namespace)
	}
	return blueprint, nil
}
