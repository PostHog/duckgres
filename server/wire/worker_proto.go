package wire

import "github.com/posthog/duckgres/server/ducklake"

// WorkerControlMetadata identifies the logical worker owner on a control
// plane → worker request.
type WorkerControlMetadata struct {
	WorkerID     int    `json:"worker_id"`
	OwnerEpoch   int64  `json:"owner_epoch"`
	CPInstanceID string `json:"cp_instance_id,omitempty"`
}

// WorkerActivationPayload is the tenant runtime material delivered to a
// shared warm worker over the control plane RPC path.
type WorkerActivationPayload struct {
	WorkerControlMetadata
	OrgID    string          `json:"org_id"`
	DuckLake ducklake.Config `json:"ducklake"`
}

// WorkerCreateSessionPayload is the control plane request body for creating
// a worker-local session.
type WorkerCreateSessionPayload struct {
	WorkerControlMetadata
	Username    string `json:"username"`
	MemoryLimit string `json:"memory_limit"`
	Threads     int    `json:"threads"`
}

// WorkerDestroySessionPayload is the control plane request body for
// destroying a worker-local session.
type WorkerDestroySessionPayload struct {
	WorkerControlMetadata
	SessionToken string `json:"session_token"`
}

// WorkerHealthCheckPayload is the control plane request body for
// health-checking a worker.
type WorkerHealthCheckPayload struct {
	WorkerControlMetadata
}
