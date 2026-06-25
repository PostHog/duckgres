package wire

import (
	"github.com/posthog/duckgres/server/ducklake"
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
}

// WorkerCreateSessionPayload is the control plane request body for creating
// a worker-local session.
type WorkerCreateSessionPayload struct {
	WorkerControlMetadata
	Username    string `json:"username"`
	MemoryLimit string `json:"memory_limit"`
	Threads     int    `json:"threads"`
	// SecretStatements are the user's persistent CREATE SECRET statements
	// (decrypted by the control plane from the config store) to replay on the
	// worker before the session is handed to the client. Worker pods are
	// ephemeral, so this is the only way a user secret survives across
	// sessions. Carries credential material: never log this payload.
	SecretStatements []string `json:"secret_statements,omitempty"`
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

// WorkerWaitSessionIdlePayload asks a worker to acknowledge that the session's
// current Flight SQL operation has released its worker-side lifecycle state.
type WorkerWaitSessionIdlePayload struct {
	WorkerControlMetadata
}

// WorkerReleaseQueryHandlePayload asks a worker to release a statement query
// handle that was created by GetFlightInfo but abandoned before DoGet could
// consume it.
type WorkerReleaseQueryHandlePayload struct {
	WorkerControlMetadata
	Ticket []byte `json:"ticket"`
}
