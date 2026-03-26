package server

import "time"

// WorkerActivationPayload is the tenant runtime material delivered to a shared
// warm worker over the control-plane RPC path.
type WorkerActivationPayload struct {
	WorkerControlMetadata
	OrgID          string         `json:"org_id"`
	LeaseExpiresAt time.Time      `json:"lease_expires_at"`
	DuckLake       DuckLakeConfig `json:"ducklake"`
}
