package server

// WorkerControlMetadata identifies the logical worker owner on a control-plane
// to worker request.
type WorkerControlMetadata struct {
	WorkerID     int    `json:"worker_id"`
	OwnerEpoch   int64  `json:"owner_epoch"`
	CPInstanceID string `json:"cp_instance_id,omitempty"`
}

// WorkerCreateSessionPayload is the control-plane request body for creating a
// worker-local session.
type WorkerCreateSessionPayload struct {
	WorkerControlMetadata
	Username    string `json:"username"`
	MemoryLimit string `json:"memory_limit"`
	Threads     int    `json:"threads"`
}

// WorkerDestroySessionPayload is the control-plane request body for destroying
// a worker-local session.
type WorkerDestroySessionPayload struct {
	WorkerControlMetadata
	SessionToken string `json:"session_token"`
}

// WorkerHealthCheckPayload is the control-plane request body for health-checking
// a worker.
type WorkerHealthCheckPayload struct {
	WorkerControlMetadata
}

// WorkerResetPayload is the control-plane request body for returning a shared
// warm worker to its neutral idle runtime.
type WorkerResetPayload struct {
	WorkerControlMetadata
}
