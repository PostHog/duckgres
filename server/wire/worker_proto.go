package wire

import (
	"time"

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

// WorkerSetS3CachePayload asks a worker to route the tenant's S3 traffic
// through the node-local cache proxy (Enabled=true, the default transport) or
// to bypass it (Enabled=false: the S3 secret is rebuilt with the org's native
// HTTPS transport, so the proxy blind-tunnels via CONNECT and never serves or
// fills its cache). Session-scoped in effect because remote workers run one
// session at a time; the worker restores the proxy transport before the next
// session starts. Carries the `duckgres.s3_cache` session GUC.
type WorkerSetS3CachePayload struct {
	WorkerControlMetadata
	Enabled bool `json:"enabled"`
	// Mode is one of on, off, or passthrough. Empty retains the legacy Enabled
	// behavior for requests from older control planes.
	Mode string `json:"mode,omitempty"`
}

// WorkerReleaseQueryHandlePayload asks a worker to release a statement query
// handle that was created by GetFlightInfo but abandoned before DoGet could
// consume it.
type WorkerReleaseQueryHandlePayload struct {
	WorkerControlMetadata
	Ticket []byte `json:"ticket"`
}

// QueryLogEntry represents a completed client query event that the control
// plane asks the activated worker to persist in tenant query-log storage.
type QueryLogEntry struct {
	// QueryID is the per-statement UUIDv7 the control plane mints when the
	// query arrives. Every event for one statement carries the same value, so
	// a QueryStart row and its terminal row join on it.
	QueryID string
	// ParentQueryID links a statement to the inbound protocol message it came
	// from. A batched simple query ("SELECT 1; SELECT 2") runs N statements
	// under one Query message: each gets its own QueryID and shares the
	// message's ID here. Empty when the statement is the whole message.
	ParentQueryID string
	// StatementIndex is the statement's zero-based position within a batched
	// simple query (ClickHouse's script_query_number).
	StatementIndex int
	// EventTime is the statement's START time on every event type, including
	// terminal ones. This deliberately diverges from ClickHouse, where
	// event_time is when the event was logged: pinning both rows of a
	// start/terminal pair to the same instant keeps them in one monthly
	// partition and lets them join without a window function. A terminal row's
	// finish time is EventTime + QueryDurationMs.
	EventTime             time.Time
	QueryDurationMs       int64
	Type                  string
	Query                 string
	TranspiledQuery       *string
	QueryKind             string
	NormalizedHash        int64
	ResultRows            int64
	WrittenRows           int64
	ExceptionCode         string
	Exception             string
	UserName              string
	OrgID                 string
	CurrentDatabase       string
	ClientAddress         string
	ClientPort            int
	ApplicationName       string
	PID                   int32
	WorkerID              int
	IsTranspiled          bool
	Protocol              string
	TraceID               string
	SpanID                string
	PostgresScanMs        int64
	CPUTimeSeconds        float64
	PeakBufferMemoryBytes int64

	// QueryMetadata is the JSON-encoded querymeta.Metadata for the statement:
	// what it reads, what it writes, which columns and functions it touches,
	// and the access class each of those implies. Recorded for audit today and
	// intended as the input a future authorization policy is evaluated against,
	// which is why it carries its own completeness flag rather than letting an
	// empty list stand for "nothing touched".
	QueryMetadata string
	// AccessKinds is the statement's access classes, comma-separated, lifted
	// out of QueryMetadata so the common "show me every write" filter is a
	// plain column scan instead of JSON extraction.
	AccessKinds string
	// MetadataComplete is false when extraction could not see the whole
	// statement. A consumer that gates on QueryMetadata must treat false as
	// "unknown", never as "nothing touched".
	MetadataComplete bool

	// WorkerTier is which worker tier executed the statement: "exploratory"
	// (the small warm worker) or "standard". Recorded at statement start; a
	// statement that triggered escalation logs the tier it ULTIMATELY ran on.
	WorkerTier string
}

// WorkerQueryLogPayload carries a batch of completed query-log entries to the
// activated worker that owns the tenant runtime.
type WorkerQueryLogPayload struct {
	WorkerControlMetadata
	Entries []QueryLogEntry `json:"entries"`
}
