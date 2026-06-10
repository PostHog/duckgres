package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/posthog/duckgres/server/usersecrets"
	"github.com/posthog/duckgres/server/wire"
)

// UserSecretManager persists per-user CREATE PERSISTENT SECRET statements so
// they survive across sessions and worker pods. Implemented by the
// multitenant control plane (backed by the config store); nil in standalone
// and process-worker modes, where secret DDL passes through untouched.
type UserSecretManager interface {
	// Ready reports whether secrets can be persisted (e.g. the encryption
	// key is configured). A non-nil error is shown to the client.
	Ready() error
	// PutSecret stores one statement. With ifNotExists set, an already-stored
	// name is left untouched (mirroring DuckDB's IF NOT EXISTS no-op on the
	// live session); otherwise any prior statement with the same name is
	// replaced. Called only after the statement executed successfully on the
	// live session.
	PutSecret(ctx context.Context, orgID, username, secretName, statement string, ifNotExists bool) error
	// DeleteSecret removes one stored secret, reporting whether it existed.
	DeleteSecret(ctx context.Context, orgID, username, secretName string) (existed bool, err error)
}

// maxUserSecretStatementLen bounds a single stored CREATE SECRET statement.
const maxUserSecretStatementLen = 8 * 1024

// isSecretNotFoundError matches DuckDB's error for dropping a secret that
// does not exist ("Invalid Input Error: Failed to remove non-existent secret
// with name '...'"). The DROP store-fallback is gated on exactly this error;
// see execUserSecretDDL.
func isSecretNotFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "non-existent secret")
}

// secretDDLError is a client-facing error from the secret DDL path.
type secretDDLError struct {
	code string
	msg  string
}

// execUserSecretDDL is the shared (simple + extended protocol) handler for
// persistent-secret DDL. It returns handled=false when the statement is not
// secret DDL this feature owns (temporary secrets, no manager configured,
// unclassifiable text) — the caller then proceeds down the normal execution
// path. When handled=true the statement was executed (and the store updated);
// tag is the CommandComplete tag, or secErr the error to send.
//
// Semantics:
//   - CREATE [OR REPLACE] PERSISTENT SECRET <name>: execute on the session
//     first (DuckDB validates), then persist the statement (encrypted) in the
//     config store for replay on the user's future sessions.
//   - DROP [PERSISTENT] SECRET <name>: execute on the session, then delete
//     from the store so it doesn't reappear next session. If the session-side
//     DROP fails with DuckDB's not-found error (and only that error) but the
//     store had the secret — e.g. its replay failed because an extension is
//     missing — the store row is still deleted and the DROP reported
//     successful, otherwise a broken stored secret could never be removed.
//
// DuckDB secrets are not transactional, so the store write is immediate even
// inside an explicit transaction block; a ROLLBACK does not undo it.
func (c *clientConn) execUserSecretDDL(query string) (handled bool, tag string, secErr *secretDDLError) {
	mgr := c.server.cfg.UserSecrets
	if mgr == nil {
		return false, "", nil
	}
	st := usersecrets.Classify(query)
	if st.Kind == usersecrets.KindNone || st.Temporary {
		return false, "", nil
	}
	if st.Kind == usersecrets.KindCreate && !st.Persistent {
		return false, "", nil // plain CREATE SECRET stays session-scoped
	}
	if st.Kind == usersecrets.KindDrop && st.Name == "" {
		return false, "", nil // let DuckDB produce its own parse error
	}

	// Persistence side effects must map 1:1 to a statement. Letting a
	// persistent variant fall through inside a multi-statement batch would be
	// worse than rejecting: the statement executes, never persists, and the
	// next session's hygiene wipe silently deletes it.
	if st.MultiStatement {
		if st.Persistent {
			return true, "", &secretDDLError{"0A000", "persistent secret DDL must be sent as a single statement; split it out of the multi-statement batch"}
		}
		return false, "", nil // plain DROP SECRET in a batch keeps passthrough behavior
	}

	// The stored statement is replayed verbatim on future sessions, so bound
	// parameters can never be resolved again — and the executor would run
	// them unbound here anyway. Require literals.
	if hasParameterPlaceholders(query) {
		return true, "", &secretDDLError{"0A000", "persistent secret statements must use literal values, not parameters ($1, ...), so they can be replayed on future sessions"}
	}

	if st.Kind == usersecrets.KindCreate {
		if err := mgr.Ready(); err != nil {
			return true, "", &secretDDLError{"0A000", fmt.Sprintf("persistent secrets are not enabled on this deployment: %v", err)}
		}
		if st.Name == "" {
			return true, "", &secretDDLError{"0A000", "persistent secrets must be named (CREATE PERSISTENT SECRET <name> (...)) so they can be managed across sessions"}
		}
		if usersecrets.IsReservedName(st.Name) {
			return true, "", &secretDDLError{"42939", fmt.Sprintf("secret name %q is reserved for system use", st.Name)}
		}
		if len(query) > maxUserSecretStatementLen {
			return true, "", &secretDDLError{"54000", fmt.Sprintf("CREATE SECRET statement exceeds %d bytes", maxUserSecretStatementLen)}
		}
	}

	ctx, cleanup := c.queryContext()
	defer cleanup()

	// Lifecycle log pair, like the normal exec paths. logQueryStarted/
	// Finished/Error redact secret DDL internally (see usersecrets.
	// RedactForLog) — they own redaction, callers pass raw text.
	queryStart := time.Now()
	var queryFinalErr error
	c.logQueryStarted(query)
	defer func() {
		c.logQueryFinished(query, queryStart, 0, queryFinalErr)
	}()

	upperQuery := strings.ToUpper(query)
	cmdType := c.getCommandType(upperQuery)

	_, execErr := c.executor.ExecContext(ctx, query)
	if execErr != nil {
		queryFinalErr = execErr
		if st.Kind == usersecrets.KindDrop && isSecretNotFoundError(execErr) {
			// The stored secret may exist even though the session-side one
			// doesn't (its replay failed, e.g. a missing extension). Deleting
			// the row is the only way for the user to get rid of it, so
			// treat the DROP as successful when the store had it. Gated on
			// DuckDB's not-found error specifically: any other failure
			// (cancellation, RPC error, aborted transaction, the
			// multiple-storages ambiguity error) means the session-side
			// secret may still exist, and reporting a successful DROP while
			// deleting only the stored copy would hand the user a false
			// confirmation — fatal for a credential revocation.
			if existed, delErr := mgr.DeleteSecret(ctx, c.orgID, c.username, st.Name); delErr == nil && existed {
				queryFinalErr = nil
				c.updateTxStatus(cmdType)
				return true, c.buildCommandTag(cmdType, nil), nil
			}
		}
		errMsg := friendlyExecError(execErr)
		if c.isCallerCancellation(execErr) {
			errMsg = "canceling statement due to user request"
		} else {
			c.logQueryError(query, execErr)
		}
		c.setTxError()
		return true, "", &secretDDLError{classifyErrorCode(execErr), errMsg}
	}

	// Session-side DDL succeeded; now make it durable. Failures here must be
	// surfaced as errors — the client believing a secret persisted when it
	// didn't is the one outcome this feature exists to prevent.
	var storeErr error
	switch st.Kind {
	case usersecrets.KindCreate:
		storeErr = mgr.PutSecret(ctx, c.orgID, c.username, st.Name, query, st.IfNotExists)
		if storeErr != nil {
			return true, "", &secretDDLError{"58000", fmt.Sprintf(
				"secret %q was applied to the current session but could not be persisted (it will NOT survive this session): %v; retry the statement",
				st.Name, storeErr)}
		}
	case usersecrets.KindDrop:
		if _, storeErr = mgr.DeleteSecret(ctx, c.orgID, c.username, st.Name); storeErr != nil {
			return true, "", &secretDDLError{"58000", fmt.Sprintf(
				"secret %q was dropped from the current session but is still persisted and will reappear next session: %v; retry the statement",
				st.Name, storeErr)}
		}
	}

	c.updateTxStatus(cmdType)
	return true, c.buildCommandTag(cmdType, nil), nil
}

// handleUserSecretDDLSimple intercepts persistent-secret DDL on the simple
// query protocol. Returns true when fully handled (responses written,
// including ReadyForQuery).
func (c *clientConn) handleUserSecretDDLSimple(query string) bool {
	handled, tag, secErr := c.execUserSecretDDL(query)
	if !handled {
		return false
	}
	if secErr != nil {
		c.sendError("ERROR", secErr.code, secErr.msg)
	} else {
		_ = wire.WriteCommandComplete(c.writer, tag)
	}
	_ = wire.WriteReadyForQuery(c.writer, c.txStatus)
	_ = c.writer.Flush()
	return true
}

// handleUserSecretDDLExtended intercepts persistent-secret DDL at Execute
// time on the extended query protocol (ReadyForQuery is sent by Sync).
// Returns true when fully handled.
func (c *clientConn) handleUserSecretDDLExtended(query string) bool {
	handled, tag, secErr := c.execUserSecretDDL(query)
	if !handled {
		return false
	}
	if secErr != nil {
		c.sendError("ERROR", secErr.code, secErr.msg)
	} else {
		_ = wire.WriteCommandComplete(c.writer, tag)
	}
	return true
}
