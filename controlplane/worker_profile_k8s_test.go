//go:build kubernetes

package controlplane

import "testing"

func TestRequestedWorkerMemoryBytesMatchesWorkerPodRequest(t *testing.T) {
	tests := []struct {
		name             string
		profile          *WorkerProfile
		deploymentMemory string
	}{
		{name: "profile", profile: &WorkerProfile{Memory: "60Gi"}, deploymentMemory: "120Gi"},
		{name: "deployment default", deploymentMemory: "120Gi"},
		{name: "built-in default"},
		{name: "Kubernetes sub-byte rounding", deploymentMemory: "500m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := &K8sWorkerPool{workerMemoryRequest: tt.deploymentMemory}
			podProfile := WorkerProfile{}
			if tt.profile != nil {
				podProfile = *tt.profile
			}
			podResources := pool.workerResourcesForProfile(podProfile)
			podMemory := podResources.Requests.Memory().Value()

			admissionMemory, err := requestedWorkerMemoryBytes(tt.profile, tt.deploymentMemory)
			if err != nil {
				t.Fatalf("requestedWorkerMemoryBytes: %v", err)
			}
			if admissionMemory != podMemory {
				t.Fatalf("admission memory = %d, pod request = %d", admissionMemory, podMemory)
			}
		})
	}
}
