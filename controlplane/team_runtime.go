package controlplane

import (
	"fmt"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const teamRuntimeConfigPath = "/etc/duckgres/runtime/duckgres.yaml"

// TeamRuntime is the resolved per-team runtime view derived from config-store data.
// It is copy-based so callers can reason about runtime-relevant changes without
// depending on mutable config-store models.
type TeamRuntime struct {
	TeamName string

	WorkerIdentity configstore.ManagedWarehouseWorkerIdentity

	WarehouseDatabase configstore.ManagedWarehouseDatabase
	MetadataStore     configstore.ManagedWarehouseMetadataStore
	S3                configstore.ManagedWarehouseS3

	WarehouseDatabaseCredentials configstore.SecretRef
	MetadataStoreCredentials     configstore.SecretRef
	S3Credentials                configstore.SecretRef
	RuntimeConfig                configstore.SecretRef
}

// ResolveTeamRuntime converts a TeamConfig into a resolved runtime view.
// Teams without a managed warehouse contract do not currently have a runtime
// slice to resolve, so the function returns (nil, false).
func ResolveTeamRuntime(team *configstore.TeamConfig) (*TeamRuntime, bool) {
	if team == nil || team.Warehouse == nil || team.Name == "" {
		return nil, false
	}

	warehouse := team.Warehouse
	return &TeamRuntime{
		TeamName: team.Name,

		WorkerIdentity: warehouse.WorkerIdentity,

		WarehouseDatabase: warehouse.WarehouseDatabase,
		MetadataStore:     warehouse.MetadataStore,
		S3:                warehouse.S3,

		WarehouseDatabaseCredentials: warehouse.WarehouseDatabaseCredentials,
		MetadataStoreCredentials:     warehouse.MetadataStoreCredentials,
		S3Credentials:                warehouse.S3Credentials,
		RuntimeConfig:                warehouse.RuntimeConfig,
	}, true
}

// ApplyTeamRuntimeToPoolConfig overlays runtime-specific worker settings onto
// the base pool configuration.
func ApplyTeamRuntimeToPoolConfig(base K8sWorkerPoolConfig, runtime *TeamRuntime) (K8sWorkerPoolConfig, error) {
	if runtime == nil {
		return base, nil
	}

	cfg := base
	namespace := cfg.Namespace
	if runtime.WorkerIdentity.Namespace != "" {
		namespace = runtime.WorkerIdentity.Namespace
	}
	if runtime.WorkerIdentity.ServiceAccountName != "" {
		cfg.ServiceAccount = runtime.WorkerIdentity.ServiceAccountName
	}

	if runtime.RuntimeConfig.Name != "" {
		if runtime.RuntimeConfig.Key == "" {
			return K8sWorkerPoolConfig{}, fmt.Errorf("team %s runtime config secret %s missing key", runtime.TeamName, runtime.RuntimeConfig.Name)
		}
		if runtime.RuntimeConfig.Namespace != "" {
			if namespace != "" && runtime.RuntimeConfig.Namespace != namespace {
				return K8sWorkerPoolConfig{}, fmt.Errorf(
					"team %s runtime config namespace %q does not match worker namespace %q",
					runtime.TeamName,
					runtime.RuntimeConfig.Namespace,
					namespace,
				)
			}
			namespace = runtime.RuntimeConfig.Namespace
		}

		cfg.ConfigMap = ""
		cfg.ConfigSecretName = runtime.RuntimeConfig.Name
		cfg.ConfigSecretKey = runtime.RuntimeConfig.Key
		cfg.ConfigPath = teamRuntimeConfigPath
	}

	if namespace != "" {
		cfg.Namespace = namespace
	}

	return cfg, nil
}
