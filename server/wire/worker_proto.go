package wire

import (
	"github.com/posthog/duckgres/server/ducklake"
	"github.com/posthog/duckgres/server/iceberg"
)

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
	// Iceberg is the per-tenant Iceberg catalog (AWS S3 Tables) config.
	// Empty when the tenant has not opted in or hasn't been provisioned yet.
	Iceberg iceberg.Config `json:"iceberg"`
}

// WorkerCreateSessionPayload is the control plane request body for creating
// a worker-local session.
type WorkerCreateSessionPayload struct {
	WorkerControlMetadata
	Username    string `json:"username"`
	MemoryLimit string `json:"memory_limit"`
	Threads     int    `json:"threads"`
	// SearchPath, when set, is the client's connect-time search_path (parsed
	// from the startup `options` parameter, e.g. `-c search_path=iceberg.public`).
	// Empty means use the worker's default. Sanitized control-plane side.
	SearchPath string `json:"search_path,omitempty"`
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
