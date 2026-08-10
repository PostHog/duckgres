package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/posthog/duckgres/transpiler/transform"
)

// ClientIdleTimeoutGUCName is the connect-time option a client may use to
// request a longer idle timeout, for example:
//
//	PGOPTIONS='-c duckgres.idle_timeout=15m' psql ...
//
// It is deliberately a Duckgres-specific option: PostgreSQL's
// idle_session_timeout has different semantics and is currently ignored for
// compatibility. This option is accepted only when the operator sets a
// positive Config.ClientIdleTimeoutMax.
const ClientIdleTimeoutGUCName = "duckgres.idle_timeout"

// ValidateClientIdleTimeoutOption parses a client-requested idle timeout and
// enforces the operator-configured maximum. A non-positive maximum disables
// the feature. Client values must be positive: clients cannot disable idle
// reaping and retain a worker indefinitely.
func ValidateClientIdleTimeoutOption(raw string, max time.Duration) (time.Duration, error) {
	if max <= 0 {
		return 0, invalidClientIdleTimeout("client idle-timeout overrides are disabled")
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || d <= 0 {
		return 0, invalidClientIdleTimeout("duckgres.idle_timeout must be a positive Go duration")
	}
	if d > max {
		return 0, invalidClientIdleTimeout(fmt.Sprintf("duckgres.idle_timeout must not exceed %s", max))
	}
	return d, nil
}

func invalidClientIdleTimeout(message string) error {
	return &transform.CodedError{Code: "22023", Message: message}
}

func (c *clientConn) applyStartupIdleTimeout(raw string) error {
	d, err := ValidateClientIdleTimeoutOption(raw, c.server.cfg.ClientIdleTimeoutMax)
	if err != nil {
		return err
	}
	c.idleTimeout = d
	return nil
}

func (c *clientConn) effectiveIdleTimeout() time.Duration {
	if c.idleTimeout > 0 {
		return c.idleTimeout
	}
	return c.server.cfg.IdleTimeout
}
