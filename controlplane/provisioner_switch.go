//go:build kubernetes

package controlplane

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// envProvisionerEnabled turns the provisioning controller loop off. It exists
// for the hand-over to hogtower: once hogtower drives Duckling lifecycle and
// the Trino projections, a second writer here would fight it. Unset or
// unparsable keeps the loop on, so no existing deployment changes behavior.
const envProvisionerEnabled = "DUCKGRES_PROVISIONER_ENABLED"

func provisionerControllerEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(envProvisionerEnabled))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("Ignoring unparsable "+envProvisionerEnabled+"; the provisioning controller stays on.", "value", raw)
		return true
	}
	return enabled
}
