package configresolve

import (
	"net/http"

	"github.com/posthog/duckgres/controlplane"
	"github.com/posthog/duckgres/server"
)

// Control-plane config assembly.
//
// This is the SINGLE place a resolved config becomes a
// controlplane.ControlPlaneConfig. Both control-plane entry points call it: the
// all-in-one `duckgres --mode control-plane` (main.go) and the production
// `cmd/duckgres-controlplane` (Dockerfile.controlplane, its own CD pipeline).
//
// It is one function on purpose. The two binaries previously each carried a
// hand-maintained 56-field literal with nothing forcing them to agree, and both
// DUCKGRES_USER_SECRET_KEY and every DUCKGRES_TRINO_BENCHMARK_* variable had
// drifted out of the production one — resolved into memory, then dropped on the
// floor, so the features they configure were dead in production while the
// mw-dev scenario (which runs the all-in-one binary) passed. Adding a knob to
// Resolved and wiring it here now reaches both binaries, and
// configresolve/controlplane_test.go fails if either half of that contract
// breaks again.

// ControlPlaneOverrides carries the few values that are NOT part of the
// resolved config because each binary owns them: its own (already
// TLS/ACME-adjusted) server config, the flags it parsed, and the metrics server
// it already started.
type ControlPlaneOverrides struct {
	// Server is the binary's server.Config AFTER it has applied its own TLS /
	// ACME adjustments — not resolved.Server.
	Server server.Config
	// SocketDir and ConfigPath come from the binary's own flags.
	SocketDir  string
	ConfigPath string
	// MetricsServer is the already-running metrics server, shut down during a
	// handover. Nil when the binary runs none.
	MetricsServer *http.Server
}

// ControlPlaneConfig assembles the control-plane config both binaries boot
// from. Every field is either derived from resolved or taken from overrides;
// there is no third source.
func ControlPlaneConfig(resolved Resolved, overrides ControlPlaneOverrides) controlplane.ControlPlaneConfig {
	return controlplane.ControlPlaneConfig{
		Config: overrides.Server,
		Process: controlplane.ProcessConfig{
			MinWorkers: resolved.ProcessMinWorkers,
			MaxWorkers: resolved.ProcessMaxWorkers,
		},
		SocketDir:                  overrides.SocketDir,
		ConfigPath:                 overrides.ConfigPath,
		MetricsServer:              overrides.MetricsServer,
		WorkerQueueTimeout:         resolved.WorkerQueueTimeout,
		WorkerIdleTimeout:          resolved.WorkerIdleTimeout,
		RetireOnSessionEnd:         resolved.ProcessRetireOnSessionEnd,
		HandoverDrainTimeout:       resolved.HandoverDrainTimeout,
		WorkerBackend:              resolved.WorkerBackend,
		ConfigStoreConn:            resolved.ConfigStoreConn,
		ConfigPollInterval:         resolved.ConfigPollInterval,
		InternalSecret:             resolved.InternalSecret,
		InternalSecretFallbacks:    resolved.InternalSecretFallbacks,
		ReadOnlySecret:             resolved.ReadOnlySecret,
		ReadOnlySecretFallbacks:    resolved.ReadOnlySecretFallbacks,
		UserSecretKey:              resolved.UserSecretKey,
		SNIRoutingMode:             resolved.SNIRoutingMode,
		ManagedHostnameSuffixes:    resolved.ManagedHostnameSuffixes,
		MetadataHostnameSuffixes:   resolved.MetadataHostnameSuffixes,
		MetadataProxyMaxConns:      resolved.MetadataProxyMaxConns,
		DucklingBucketSuffix:       resolved.DucklingBucketSuffix,
		DuckLakeDefaultSpecVersion: resolved.DuckLakeDefaultSpecVersion,

		AdmissionReclaimerMaxReservations: resolved.AdmissionReclaimerMaxReservations,

		K8s: controlplane.K8sConfig{
			WorkerImage:                  resolved.K8sWorkerImage,
			WorkerNamespace:              resolved.K8sWorkerNamespace,
			ControlPlaneID:               resolved.K8sControlPlaneID,
			WorkerPort:                   resolved.K8sWorkerPort,
			WorkerSecret:                 resolved.K8sWorkerSecret,
			WorkerConfigMap:              resolved.K8sWorkerConfigMap,
			ImagePullPolicy:              resolved.K8sWorkerImagePullPolicy,
			ServiceAccount:               resolved.K8sWorkerServiceAccount,
			WorkerCPURequest:             resolved.K8sWorkerCPURequest,
			WorkerMemoryRequest:          resolved.K8sWorkerMemoryRequest,
			WorkerNodeSelector:           resolved.K8sWorkerNodeSelector,
			WorkerTolerationKey:          resolved.K8sWorkerTolerationKey,
			WorkerTolerationValue:        resolved.K8sWorkerTolerationValue,
			AllowClientWorkerProfile:     resolved.K8sAllowClientWorkerProfile,
			WorkerPriorityClassName:      resolved.K8sWorkerPriorityClassName,
			PlaceholderImage:             resolved.K8sPlaceholderImage,
			PlaceholderPriorityClassName: resolved.K8sPlaceholderPriorityClassName,
			WorkerProfileMinCPU:          resolved.K8sWorkerProfileMinCPU,
			WorkerProfileMaxCPU:          resolved.K8sWorkerProfileMaxCPU,
			WorkerProfileMinMemory:       resolved.K8sWorkerProfileMinMemory,
			WorkerProfileMaxMemory:       resolved.K8sWorkerProfileMaxMemory,
			WorkerMaxTTL:                 resolved.K8sWorkerMaxTTL,
			WorkerDefaultTTL:             resolved.K8sWorkerDefaultTTL,
			ExploratoryTierEnabled:       resolved.K8sExploratoryTierEnabled,
			ExploratoryWorkerCPU:         resolved.K8sExploratoryWorkerCPU,
			ExploratoryWorkerMemory:      resolved.K8sExploratoryWorkerMemory,
			ExploratoryWorkerTTL:         resolved.K8sExploratoryWorkerTTL,
			ReshardPodCPU:                resolved.K8sReshardPodCPU,
			ReshardPodMemory:             resolved.K8sReshardPodMemory,
			TrinoBenchmark:               resolved.TrinoBenchmark,
			AWSRegion:                    resolved.AWSRegion,
		},
	}
}
