//go:build kubernetes

package controlplane

import "testing"

// The spawn-connect budget must absorb the worker's full warmup (the health
// handler blocks on warmupDone through extension load + DuckLake ATTACH), so it
// has to be comfortably larger than a bare TCP dial. 90s proved too tight under
// concurrent cold spawns; guard against regressing it back.
func TestWorkerSpawnConnectTimeoutCoversWarmup(t *testing.T) {
	if workerSpawnConnectTimeout <= 90_000_000_000 { // 90s in ns
		t.Errorf("workerSpawnConnectTimeout=%s must exceed the old 90s budget that reaped still-warming workers", workerSpawnConnectTimeout)
	}
}

// The detached spawn+activate budget must dominate the sum of the phases it
// supervises (pod-ready + connect + activate); otherwise the outer ctx can
// expire mid-phase and thrash. Defining it as that sum keeps the three in sync.
func TestWorkerSpawnActivateTimeoutDominatesPhases(t *testing.T) {
	phases := workerPodReadyTimeout + workerSpawnConnectTimeout + defaultActivatingTimeout
	if workerSpawnActivateTimeout < phases {
		t.Errorf("workerSpawnActivateTimeout=%s must be >= pod-ready+connect+activate=%s", workerSpawnActivateTimeout, phases)
	}
}
