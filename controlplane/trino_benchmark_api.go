package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
)

// The Trino benchmark API is the ONLY way a scenario run creates or deletes a
// benchmark Trino cluster. Everything credential-shaped stays on the control
// plane: the scenario runner authenticates with the internal secret, names the
// org, and receives back nothing but a cluster ID, a lifecycle state, the
// non-secret in-cluster endpoint, worker counts, and the pinned image
// reference. The metadata reader password and the read-only S3 identity are
// resolved from charts-created resources inside the control plane and never
// cross this boundary — see TrinoReaderIdentity.
//
// The feature is fail-closed: with no configured lifecycle (the default) every
// route answers 503, so a Duckgres deployment without the companion charts
// reader resources simply cannot start a benchmark cluster.

// TrinoBenchmarkState is the closed set of lifecycle states the API reports.
// pending and failed are distinguishable so a poller can treat one as "keep
// waiting" and the other as terminal.
type TrinoBenchmarkState string

const (
	// TrinoBenchmarkStatePending means the cluster exists but the coordinator
	// or some requested worker replica is not ready yet. Poll again.
	TrinoBenchmarkStatePending TrinoBenchmarkState = "pending"
	// TrinoBenchmarkStateReady means the coordinator is ready AND every
	// requested worker replica is ready. Only then is Endpoint usable.
	TrinoBenchmarkStateReady TrinoBenchmarkState = "ready"
	// TrinoBenchmarkStateFailed is terminal: the cluster cannot become ready
	// without operator action. A poller must stop, not keep waiting.
	TrinoBenchmarkStateFailed TrinoBenchmarkState = "failed"
)

// maxTrinoBenchmarkWorkers bounds what a caller may request, independent of the
// deployment's configured default. A benchmark cluster is a disposable
// side-by-side comparison, not a capacity-planning tool.
const maxTrinoBenchmarkWorkers = 16

// TrinoBenchmarkSettings is the deployment configuration for the benchmark
// lifecycle. Like the other pod-shape knobs it is env-only (see
// configresolve/resolve.go) and every value has a documented default:
//
//	DUCKGRES_TRINO_BENCHMARK_ENABLED             false  (fail-closed)
//	DUCKGRES_TRINO_BENCHMARK_IMAGE               ""     (required when enabled)
//	DUCKGRES_TRINO_BENCHMARK_IMAGE_PULL_POLICY   IfNotPresent
//	DUCKGRES_TRINO_BENCHMARK_SERVICE_ACCOUNT     ""     (pod default SA)
//	DUCKGRES_TRINO_BENCHMARK_WORKERS             4
//	DUCKGRES_TRINO_BENCHMARK_COORDINATOR_CPU     2
//	DUCKGRES_TRINO_BENCHMARK_COORDINATOR_MEMORY  8Gi
//	DUCKGRES_TRINO_BENCHMARK_WORKER_CPU          2
//	DUCKGRES_TRINO_BENCHMARK_WORKER_MEMORY       8Gi
//
// Enabled alone is not sufficient: without a pinned image and a resolvable
// charts-created reader identity the lifecycle refuses to start.
type TrinoBenchmarkSettings struct {
	Enabled           bool
	Image             string
	ImagePullPolicy   string
	ServiceAccount    string
	Workers           int
	CoordinatorCPU    string
	CoordinatorMemory string
	WorkerCPU         string
	WorkerMemory      string
}

// TrinoBenchmarkCluster is the complete, credential-free response body. Every
// field here is safe to write into scenario state, HTTP responses, logs, and
// benchmark artifacts.
type TrinoBenchmarkCluster struct {
	ID    string              `json:"id"`
	State TrinoBenchmarkState `json:"state"`
	// Endpoint is the in-cluster coordinator URL. Populated once ready.
	Endpoint string `json:"endpoint,omitempty"`
	// RequestedWorkers / ReadyWorkers make the readiness rule auditable from
	// the artifact: a run is only comparable if they match.
	RequestedWorkers int `json:"requested_workers,omitempty"`
	ReadyWorkers     int `json:"ready_workers"`
	// Image is the pinned Trino+Brikk image reference (digest where the
	// deployment pins one). It is the authoritative record of which engine and
	// connector build produced the numbers.
	Image string `json:"image,omitempty"`
}

// TrinoBenchmarkRequest is the provision body. It deliberately cannot carry an
// image, a namespace, credentials, or any other infrastructure knob: those come
// from control-plane configuration only. Unknown fields are rejected so a
// caller cannot believe it configured something it did not.
type TrinoBenchmarkRequest struct {
	// Workers is the requested worker replica count. 0 means "use the
	// control plane's configured default" (4).
	Workers int `json:"workers,omitempty"`
	// RunID is an opaque, non-secret scenario run identifier recorded as a
	// label so a leftover cluster can be traced back to its run.
	RunID string `json:"run_id,omitempty"`
}

// TrinoBenchmarkProvisionResult distinguishes a fresh provision from an
// idempotent no-op so the API can answer 202 vs 200 truthfully.
type TrinoBenchmarkProvisionResult struct {
	Cluster TrinoBenchmarkCluster
	// Created is true only when this call actually created resources.
	Created bool
}

// TrinoBenchmarkLifecycle owns the short-lived benchmark cluster associated
// with one managed warehouse. The Kubernetes implementation is
// trinoBenchmarkManager (kubernetes build tag).
type TrinoBenchmarkLifecycle interface {
	// ProvisionTrinoBenchmark is idempotent for an identical request and
	// returns ErrTrinoBenchmarkConflict when a cluster of the same name exists
	// with different ownership or configuration.
	ProvisionTrinoBenchmark(ctx context.Context, orgID string, request TrinoBenchmarkRequest) (TrinoBenchmarkProvisionResult, error)
	// TrinoBenchmarkStatus returns ErrTrinoBenchmarkNotFound for an unknown
	// cluster.
	TrinoBenchmarkStatus(ctx context.Context, clusterID string) (TrinoBenchmarkCluster, error)
	// DeprovisionTrinoBenchmark is idempotent and safe after a partial
	// provision; it deletes only resources owned by clusterID.
	DeprovisionTrinoBenchmark(ctx context.Context, clusterID string) error
}

// Lifecycle error sentinels. Handlers map these to status codes and NEVER
// forward the wrapped error text to the client — an infrastructure error can
// contain a connection string or a Secret value.
var (
	// ErrTrinoBenchmarkNotFound: no cluster with that ID.
	ErrTrinoBenchmarkNotFound = errors.New("trino benchmark cluster not found")
	// ErrTrinoBenchmarkConflict: a same-named cluster exists with different
	// ownership or configuration. Never silently adopted.
	ErrTrinoBenchmarkConflict = errors.New("trino benchmark cluster conflict")
	// ErrTrinoBenchmarkDisabled: the feature is switched off in this
	// deployment.
	ErrTrinoBenchmarkDisabled = errors.New("trino benchmark lifecycle is disabled")
	// ErrTrinoBenchmarkConfig: the deployment is missing required
	// configuration — most importantly the charts-created reader identity.
	// This is the fail-closed path: it never degrades to writer credentials.
	ErrTrinoBenchmarkConfig = errors.New("trino benchmark configuration is incomplete")
	// ErrTrinoBenchmarkInvalidRequest: caller-supplied input is unusable.
	ErrTrinoBenchmarkInvalidRequest = errors.New("invalid trino benchmark request")
)

// trinoBenchmarkNameRe constrains both the org ID and the cluster ID to
// characters that are safe in a Kubernetes object name and a label value. It is
// deliberately stricter than the API needs so a malformed caller can never
// steer resource naming.
var trinoBenchmarkNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// registerTrinoBenchmarkAPI mounts the lifecycle routes on an
// admin-authenticated route group. requireAdmin is passed in (rather than
// assumed) so the caller decides the gate; the scenario runner authenticates
// with the internal secret, which resolves to admin.
//
// lifecycle may be nil: the deployment then answers 503 everywhere, which is
// the intended state until the companion charts reader resources exist.
func registerTrinoBenchmarkAPI(r gin.IRouter, lifecycle TrinoBenchmarkLifecycle, requireAdmin gin.HandlerFunc) {
	h := trinoBenchmarkHandler{lifecycle: lifecycle}
	r.POST("/trino-benchmarks/orgs/:org_id/provision", requireAdmin, h.provision)
	r.GET("/trino-benchmarks/status/:cluster_id", requireAdmin, h.status)
	r.POST("/trino-benchmarks/deprovision/:cluster_id", requireAdmin, h.deprovision)
}

type trinoBenchmarkHandler struct{ lifecycle TrinoBenchmarkLifecycle }

// available reports whether a lifecycle is wired, answering 503 when not.
func (h trinoBenchmarkHandler) available(c *gin.Context) bool {
	if h.lifecycle == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Trino benchmark lifecycle is not configured"})
		return false
	}
	return true
}

func (h trinoBenchmarkHandler) provision(c *gin.Context) {
	if !h.available(c) {
		return
	}
	orgID := c.Param("org_id")
	if !trinoBenchmarkNameRe.MatchString(orgID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid org id"})
		return
	}
	request, err := decodeTrinoBenchmarkRequest(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result, err := h.lifecycle.ProvisionTrinoBenchmark(c.Request.Context(), orgID, request)
	if err != nil {
		writeTrinoBenchmarkError(c, "provision", orgID, err)
		return
	}
	// 202 on a real create (resources are converging), 200 on an idempotent
	// repeat so a retrying caller can tell the two apart.
	status := http.StatusOK
	if result.Created {
		status = http.StatusAccepted
	}
	c.JSON(status, result.Cluster)
}

func (h trinoBenchmarkHandler) status(c *gin.Context) {
	if !h.available(c) {
		return
	}
	clusterID := c.Param("cluster_id")
	if !trinoBenchmarkNameRe.MatchString(clusterID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cluster id"})
		return
	}
	cluster, err := h.lifecycle.TrinoBenchmarkStatus(c.Request.Context(), clusterID)
	if err != nil {
		writeTrinoBenchmarkError(c, "status", clusterID, err)
		return
	}
	c.JSON(http.StatusOK, cluster)
}

func (h trinoBenchmarkHandler) deprovision(c *gin.Context) {
	if !h.available(c) {
		return
	}
	clusterID := c.Param("cluster_id")
	if !trinoBenchmarkNameRe.MatchString(clusterID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cluster id"})
		return
	}
	err := h.lifecycle.DeprovisionTrinoBenchmark(c.Request.Context(), clusterID)
	// Teardown is always_run in the scenario DAG, so "already gone" is the
	// success case, not an error to escalate.
	if err != nil && !errors.Is(err, ErrTrinoBenchmarkNotFound) {
		writeTrinoBenchmarkError(c, "deprovision", clusterID, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// decodeTrinoBenchmarkRequest parses an optional JSON body. An empty body is
// valid and means "all defaults"; unknown fields are rejected so a caller
// cannot think it passed an image, a namespace, or a credential.
func decodeTrinoBenchmarkRequest(body io.Reader) (TrinoBenchmarkRequest, error) {
	var request TrinoBenchmarkRequest
	if body == nil {
		return request, nil
	}
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&request); err != nil {
		if errors.Is(err, io.EOF) {
			return TrinoBenchmarkRequest{}, nil
		}
		// The body is caller-supplied, but the decoder echoes offending field
		// names, so keep the message generic.
		return TrinoBenchmarkRequest{}, fmt.Errorf("request body must be a Trino benchmark request object")
	}
	if request.Workers < 0 || request.Workers > maxTrinoBenchmarkWorkers {
		return TrinoBenchmarkRequest{}, fmt.Errorf("workers must be between 0 and %d", maxTrinoBenchmarkWorkers)
	}
	if request.RunID != "" && !trinoBenchmarkNameRe.MatchString(request.RunID) {
		return TrinoBenchmarkRequest{}, fmt.Errorf("run_id must be a DNS-1123 label")
	}
	return request, nil
}

// writeTrinoBenchmarkError maps a lifecycle error to a status code and a fixed,
// sanitized message. The underlying error is logged (server-side, where the
// operator can see it) but never returned: infrastructure errors routinely
// contain connection strings and Secret references.
func writeTrinoBenchmarkError(c *gin.Context, operation, subject string, err error) {
	status := http.StatusInternalServerError
	message := "Trino benchmark " + operation + " failed"
	switch {
	case errors.Is(err, ErrTrinoBenchmarkNotFound):
		status, message = http.StatusNotFound, "Trino benchmark cluster not found"
	case errors.Is(err, ErrTrinoBenchmarkConflict):
		status, message = http.StatusConflict, "Trino benchmark cluster exists with different ownership or configuration"
	case errors.Is(err, ErrTrinoBenchmarkDisabled):
		status, message = http.StatusServiceUnavailable, "Trino benchmark lifecycle is disabled"
	case errors.Is(err, ErrTrinoBenchmarkConfig):
		status, message = http.StatusServiceUnavailable, "Trino benchmark reader identity or image is not configured"
	case errors.Is(err, ErrTrinoBenchmarkInvalidRequest):
		status, message = http.StatusBadRequest, "invalid Trino benchmark request"
	}
	if status >= http.StatusInternalServerError {
		slog.Error("Trino benchmark lifecycle operation failed.",
			"operation", operation, "subject", subject, "error", err)
	}
	c.JSON(status, gin.H{"error": message})
}
