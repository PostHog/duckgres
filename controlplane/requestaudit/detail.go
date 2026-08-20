// Package requestaudit carries non-sensitive handler context into the control
// plane's audit middleware without coupling provisioning handlers to the admin
// package.
package requestaudit

import "github.com/gin-gonic/gin"

const (
	detailKey  = "duckgres_audit_detail"
	outcomeKey = "duckgres_audit_outcome"
)

// Outcome is a machine-readable, non-sensitive result established by a
// handler. It complements the HTTP status when a durable mutation succeeds but
// later response-path work fails.
type Outcome string

const (
	// OutcomeCredentialMinted means the service-grant row was durably created.
	// It carries no credential ID or secret.
	OutcomeCredentialMinted Outcome = "credential_minted"
)

// SetDetail records a human-readable, non-sensitive summary for the current
// request. The audit middleware persists it verbatim, so callers must never
// include credentials or raw secret DDL.
func SetDetail(c *gin.Context, detail string) {
	if detail != "" {
		c.Set(detailKey, detail)
	}
}

// Detail returns the handler-provided audit summary, if any.
func Detail(c *gin.Context) string {
	return c.GetString(detailKey)
}

// SetOutcome records a machine-readable durable result for AuditMiddleware.
func SetOutcome(c *gin.Context, outcome Outcome) {
	if outcome != "" {
		c.Set(outcomeKey, string(outcome))
	}
}

// GetOutcome returns the handler-provided durable outcome, if any.
func GetOutcome(c *gin.Context) string {
	return c.GetString(outcomeKey)
}
