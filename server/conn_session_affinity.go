package server

import (
	"fmt"
	"strings"
)

// sessionAffinityGUCName is the fully-qualified name of the duckgres-namespaced
// startup option that pins a connection to a single worker for its whole life.
// A truthy value keeps the connection off the exploratory small-worker tier, so
// the control plane never destroys and re-creates its session on another worker
// (see controlplane/control.go and server/conn_tier.go). Ad-hoc `ATTACH ...
// (TYPE postgres)` catalogs and exported transaction snapshots — worker-side
// session state that the tier does not replay onto a new worker — then survive
// for the whole session.
const sessionAffinityGUCName = "duckgres.session_affinity"

// SessionAffinityGUCName is the startup-option / GUC name, exported for the
// control plane's startup-option parsing.
const SessionAffinityGUCName = sessionAffinityGUCName

// ParseSessionAffinityOption reads a `-c duckgres.session_affinity=...` startup
// option. It reports whether the client asked to pin the session, and returns
// an error for a value that is not a boolean so the control plane can reject
// the connection (FATAL 22023) rather than silently ignore the request. An
// absent option is not affinity and not an error.
func ParseSessionAffinityOption(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return false, nil
	case "true", "on", "yes", "1":
		return true, nil
	case "false", "off", "no", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid %s startup option %q: want a boolean", sessionAffinityGUCName, raw)
	}
}
