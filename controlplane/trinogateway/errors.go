// Package trinogateway is the client for the Gateway's pooled member lifecycle
// protocol (v1). The Gateway owns member eligibility, admission, obligation
// accounting, the minimum-serving floor, the publication barrier and the
// irreversible retirement claim; duckgres owns Kubernetes effects and the
// durable operations that drive them. There is no distributed transaction
// between the two, so this client is built around one rule: a lost response is
// UNKNOWN, and unknown is resolved by reading the same operation back.
package trinogateway

import (
	"errors"
	"fmt"
	"net/http"
)

// Typed conflicts, mapped from the Gateway's X-Trino-Gateway-Error header.
// Each one is terminal for the step: retrying a stale epoch, a changed intent
// or an exhausted budget cannot succeed, and hot-retrying would bury a real
// fault under a retry loop.
var (
	ErrPoolDisabled       = errors.New("gateway pool protocol is disabled")
	ErrStaleEpoch         = errors.New("gateway rejected a stale controller epoch")
	ErrIntentChanged      = errors.New("gateway recorded a different payload for this operation step")
	ErrStaleGeneration    = errors.New("gateway rejected a stale generation")
	ErrPhase              = errors.New("gateway refused the transition from the member's current phase")
	ErrIrreversible       = errors.New("gateway refused to resume or reuse a retiring identity")
	ErrServingFloor       = errors.New("gateway refused a drain that would breach the serving floor")
	ErrSurgeBudget        = errors.New("gateway refused an activation: surge or repair budget exhausted")
	ErrNotCertified       = errors.New("gateway refused an activation: missing, stale or mismatched certificate")
	ErrPublicationBarrier = errors.New("gateway refused a join without a receipt at the open publication revision")
	ErrMembershipChanged  = errors.New("gateway membership generation moved during the publication")
	ErrReceiptsIncomplete = errors.New("gateway refused a publication commit: a receipt is missing")
	ErrEvidenceRequired   = errors.New("gateway refused a loss claim without termination evidence")
	ErrTenantNotAdmitted  = errors.New("gateway tenant admission gate is not open")
	ErrUnavailable        = errors.New("gateway is unavailable")

	// The Gateway rejects a malformed request body with POOL_VALIDATION. For
	// this client that is a bug in the caller, never something to retry.
	ErrValidation = errors.New("gateway rejected the request body")
	ErrNotFound   = errors.New("gateway does not know this pool, member or receipt")
	// ErrIdentityConflict covers a member registered against a backend that
	// belongs to another routing group, or an endpoint that does not match the
	// Gateway's own backend registration.
	ErrIdentityConflict = errors.New("gateway refused a conflicting member identity")
	ErrAPIMode          = errors.New("gateway refused the call for this pool's api mode")
	// ErrNotDrained is the Gateway refusing to seal a member that still has
	// obligations. It is the authoritative answer to "is the drain finished".
	ErrNotDrained   = errors.New("gateway refused to seal a member that is not drained")
	ErrRepairBudget = errors.New("gateway refused an activation: repair budget exhausted")
	// ErrPrincipalConflict means a published principal already belongs to
	// another tenant in this pool. That is an ambiguity the gate must never
	// resolve by guessing, so the publication is refused whole.
	ErrPrincipalConflict = errors.New("gateway refused a principal already bound to another tenant")
)

// Error carries the Gateway's response code alongside the mapped sentinel.
type Error struct {
	Code   string
	Status int
	Body   string
	err    error
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("gateway returned HTTP %d", e.Status)
	}
	return fmt.Sprintf("gateway returned %s (HTTP %d)", e.Code, e.Status)
}

func (e *Error) Unwrap() error { return e.err }

// codeSentinels is the whole mapping. A code the Gateway adds later is
// deliberately NOT retryable: an unrecognized refusal is treated as terminal
// for the step, so an unknown rejection cannot become an infinite retry.
var codeSentinels = map[string]error{
	"POOL_DISABLED":              ErrPoolDisabled,
	"POOL_STALE_EPOCH":           ErrStaleEpoch,
	"POOL_INTENT_CHANGED":        ErrIntentChanged,
	"POOL_STALE_GENERATION":      ErrStaleGeneration,
	"POOL_PHASE":                 ErrPhase,
	"POOL_IRREVERSIBLE":          ErrIrreversible,
	"POOL_SERVING_FLOOR":         ErrServingFloor,
	"POOL_SURGE_BUDGET":          ErrSurgeBudget,
	"POOL_NOT_CERTIFIED":         ErrNotCertified,
	"POOL_PUBLICATION_BARRIER":   ErrPublicationBarrier,
	"POOL_MEMBERSHIP_CHANGED":    ErrMembershipChanged,
	"POOL_RECEIPTS_INCOMPLETE":   ErrReceiptsIncomplete,
	"POOL_EVIDENCE_REQUIRED":     ErrEvidenceRequired,
	"POOL_VALIDATION":            ErrValidation,
	"POOL_NOT_FOUND":             ErrNotFound,
	"POOL_IDENTITY_CONFLICT":     ErrIdentityConflict,
	"POOL_APIMODE":               ErrAPIMode,
	"POOL_NOT_DRAINED":           ErrNotDrained,
	"POOL_REPAIR_BUDGET":         ErrRepairBudget,
	"POOL_PRINCIPAL_CONFLICT":    ErrPrincipalConflict,
	"TENANT_NOT_ADMITTED":        ErrTenantNotAdmitted,
	"ROUTING_STATE_UNAVAILABLE":  ErrUnavailable,
	"TENANT_IDENTITY_UNVERIFIED": ErrTenantNotAdmitted,
}

func newGatewayError(status int, code, body string) error {
	mapped := codeSentinels[code]
	if mapped == nil {
		switch {
		case status == http.StatusServiceUnavailable, status == http.StatusTooManyRequests, status >= 500:
			mapped = ErrUnavailable
		default:
			mapped = errors.New("gateway refused the request")
		}
	}
	return &Error{Code: code, Status: status, Body: body, err: mapped}
}

// Retryable reports whether an error is a transient condition rather than a
// verdict about the operation. Only availability failures qualify: every
// conflict above means the Gateway made a decision that a retry cannot change.
//
// Note what this does NOT say: a transport error has no verdict at all. The
// caller must resolve those through GetOperation before retrying, because the
// mutation may well have been applied.
func Retryable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}
