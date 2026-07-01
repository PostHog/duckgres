//go:build kubernetes

package admin

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/server/usersecrets"
)

// QueryResult is the result of an impersonated query.
type QueryResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
}

// Impersonator opens a session as an arbitrary org+user and runs SQL on that
// org's worker. Implemented by the controlplane adapter (it holds the org
// router). The implementation MUST destroy the session when the query returns.
type Impersonator interface {
	Impersonate(c *gin.Context, org, username, sql string, allowWrite bool) (*QueryResult, error)
}

type impersonateRequest struct {
	Username   string `json:"username"`
	SQL        string `json:"sql"`
	AllowWrite bool   `json:"allow_write"`
}

// registerImpersonateAPI wires POST /orgs/:id/impersonate/query. RoleGate
// already restricts POST to admins; the handler re-checks and audits.
func registerImpersonateAPI(r *gin.RouterGroup, imp Impersonator, audit *AuditStore) {
	if imp == nil {
		return
	}
	r.POST("/orgs/:id/impersonate/query", func(c *gin.Context) {
		id := IdentityFromContext(c)
		if id == nil || id.Role != RoleAdmin {
			c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
			return
		}
		org := c.Param("id")
		var req impersonateRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		if strings.TrimSpace(req.Username) == "" || strings.TrimSpace(req.SQL) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "username and sql are required"})
			return
		}

		// recordAudit writes one rich impersonation audit row (admin actor +
		// target user + redacted SQL) on EVERY exit path — success, engine
		// failure, AND the write-without-allow_write rejection — and marks the
		// request handled so the generic AuditMiddleware does not also record a
		// mislabeled "config.create" row. A failed audit write is logged loudly
		// but never discards an already-executed query (it cannot be un-run).
		recordAudit := func(status int) {
			entry := &AdminAuditEntry{
				Action:      "impersonate.query",
				Method:      c.Request.Method,
				Path:        c.FullPath(),
				Org:         org,
				TargetUser:  req.Username,
				SQLRedacted: usersecrets.RedactForLog(req.SQL),
				RemoteAddr:  c.ClientIP(),
				Status:      status,
			}
			if id != nil {
				entry.Actor, entry.Role, entry.Source = id.Email, string(id.Role), id.Source
			}
			if audit != nil {
				if err := audit.Record(entry); err != nil {
					slog.Error("admin: FAILED to audit impersonation",
						"actor", entry.Actor, "org", org, "target_user", req.Username, "error", err)
				}
			}
			c.Set(ctxAuditHandledKey, true)
		}

		// Defense in depth: a write statement must be explicitly opted into
		// (the UI also forces a confirm). The classifier is conservative — it
		// treats WITH/CTEs, EXPLAIN ANALYZE of a writer, and anything not
		// obviously read-only as a write, so false positives only cost an extra
		// confirm.
		if !req.AllowWrite && !isReadOnlySQL(req.SQL) {
			recordAudit(http.StatusBadRequest)
			c.JSON(http.StatusBadRequest, gin.H{"error": "statement may write; resend with allow_write=true"})
			return
		}

		result, runErr := imp.Impersonate(c, org, req.Username, req.SQL, req.AllowWrite)
		if runErr != nil {
			recordAudit(http.StatusBadGateway)
			// The engine echoes the offending SQL in errors (LINE 1: ... SECRET
			// '...'), so redact before returning it to the client / proxy logs.
			c.JSON(http.StatusBadGateway, gin.H{"error": usersecrets.RedactErrorForLog(req.SQL, runErr.Error())})
			return
		}
		recordAudit(http.StatusOK)
		c.JSON(http.StatusOK, result)
	})
}

// readOnlyLeadingKeywords are statement prefixes we consider non-mutating.
// WITH is deliberately EXCLUDED: a writable CTE (WITH x AS (INSERT ...)) is a
// mutation, so WITH always requires allow_write.
var readOnlyLeadingKeywords = []string{
	"select", "explain", "show", "describe", "desc", "pragma", "values", "table",
}

// isReadOnlySQL reports whether the (single) statement looks read-only. Heuristic
// leading-keyword check, mirroring the conservative stance in server/conn.go's
// DML detection: err toward classifying as a write.
func isReadOnlySQL(sql string) bool {
	s := strings.TrimSpace(strings.ToLower(sql))
	// Strip a leading line comment / block comment cheaply.
	s = strings.TrimSpace(s)
	for _, kw := range readOnlyLeadingKeywords {
		if s == kw || strings.HasPrefix(s, kw+" ") || strings.HasPrefix(s, kw+"\n") || strings.HasPrefix(s, kw+"\t") || strings.HasPrefix(s, kw+"(") {
			// Reject multi-statement batches (a read-only prefix followed by a
			// second statement that could mutate).
			if strings.Contains(strings.TrimRight(s, "; \n\t"), ";") {
				return false
			}
			// EXPLAIN ANALYZE executes the inner statement to profile it, so
			// EXPLAIN ANALYZE <DML/DDL> is a WRITE despite the read-only-looking
			// EXPLAIN prefix. Treat it as a write so allow_write is required.
			if kw == "explain" && explainExecutes(s) {
				return false
			}
			return true
		}
	}
	return false
}

// explainExecutes reports whether an EXPLAIN statement actually runs its inner
// query (EXPLAIN ANALYZE, or EXPLAIN (ANALYZE[, ...])) — and can therefore
// mutate through it. Plain EXPLAIN only plans and is safe.
func explainExecutes(s string) bool {
	rest := strings.TrimSpace(strings.TrimPrefix(s, "explain"))
	if strings.HasPrefix(rest, "analyze") {
		return true
	}
	// Parenthesized option list, e.g. EXPLAIN (ANALYZE, VERBOSE) INSERT ...
	if strings.HasPrefix(rest, "(") {
		if end := strings.IndexByte(rest, ')'); end > 0 {
			for _, tok := range strings.FieldsFunc(rest[1:end], func(r rune) bool {
				return r == ' ' || r == ',' || r == '\t' || r == '\n'
			}) {
				if tok == "analyze" {
					return true
				}
			}
		}
	}
	return false
}
