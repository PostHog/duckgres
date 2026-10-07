//go:build kubernetes

package controlplane

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// envProvisionerEnabled turns the provisioning controller loop off. It exists
// for the hand-over to hogtower: once hogtower drives Duckling lifecycle and
// the Trino projections, a second writer here would fight it. Unset or
// unparsable keeps the loop on, so no existing deployment changes behavior.
const envProvisionerEnabled = "DUCKGRES_PROVISIONER_ENABLED"

// controlHandedOver is set at startup when the config store records that
// another control plane took over provisioning (see applyControlHandover).
var controlHandedOver bool

func provisionerControllerEnabled() bool {
	if controlHandedOver {
		return false
	}
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

// controlHandoverTable records which control plane owns provisioning when it
// is not this one. duckgres only reads it; the control plane taking over
// (hogtower) creates the table and writes the row, and deleting the row hands
// control back on the next restart. A missing table means no hand-over.
const controlHandoverTable = "duckgres_control_handover"

// handoverOverrides are the gates a hand-over forces off. They are applied
// to the process environment because every gate reads it, so one place
// covers the controller, the pool operator, the catalog writer, node
// disruption and default placement (which requires the operator).
var handoverOverrides = map[string]string{
	envProvisionerEnabled:             "false",
	envTrinoPoolOperatorEnabled:       "false",
	envTrinoPoolCatalogWriter:         "false",
	envTrinoPoolNodeDisruptionEnabled: "false",
}

// readControlHandoverOwner returns the control plane recorded as the owner of
// provisioning, or "" when there is none. A missing table means no hand-over.
func readControlHandoverOwner(db *gorm.DB) (string, error) {
	var owners []string
	err := db.Raw("SELECT owner FROM " + controlHandoverTable + " WHERE component = 'provisioning'").Scan(&owners).Error
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
			return "", nil
		}
		return "", fmt.Errorf("read control hand-over: %w", err)
	}
	if len(owners) == 0 {
		return "", nil
	}
	return owners[0], nil
}

// applyControlHandover checks the config store for a provisioning hand-over
// and, when one is recorded, turns every duckgres writer of Duckling and
// Trino pool state off for this process. It is read once at startup:
// toggling it takes a restart, which is also what keeps a running replica
// from flipping mid-reconcile. A read error fails startup rather than
// guessing, because guessing wrong means two writers. It returns the
// recorded owner ("" when there is no hand-over) so the management API's
// read-only gate starts from the same answer (see management_read_only.go).
func applyControlHandover(db *gorm.DB) (string, error) {
	owner, err := readControlHandoverOwner(db)
	if err != nil || owner == "" {
		return "", err
	}
	controlHandedOver = true
	for key, value := range handoverOverrides {
		if err := os.Setenv(key, value); err != nil {
			return "", err
		}
	}
	if err := os.Unsetenv(envTrinoDefaultCell); err != nil {
		return "", err
	}
	slog.Warn("Provisioning is handed over to another control plane; Duckling lifecycle, Trino projections and the Trino pool operator are off in this process.",
		"owner", owner, "table", controlHandoverTable)
	return owner, nil
}
