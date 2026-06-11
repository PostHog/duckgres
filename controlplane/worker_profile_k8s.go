//go:build kubernetes

package controlplane

import "github.com/posthog/duckgres/controlplane/configstore"

func resolveOrgDefaultWorkerProfile(k K8sConfig, org *configstore.OrgConfig) (*WorkerProfile, []string, error) {
	if org == nil {
		return nil, nil, nil
	}
	profile, warns, _, err := resolveWorkerProfileWithConfig(k, nil, orgWorkerProfileDefaults{
		CPU:    org.DefaultWorkerCPU,
		Memory: org.DefaultWorkerMemory,
		TTL:    org.DefaultWorkerTTL,
	})
	return profile, warns, err
}
