//go:build kubernetes

package controlplane

import (
	"errors"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func validateTrinoBackendAvailability(backend configstore.TrinoBackend) error {
	if backend != configstore.TrinoBackendHoglake {
		return nil
	}
	config, err := trinoManagedHoglakeConfig()
	if err != nil {
		return err
	}
	if config == nil {
		return errors.New("managed Hoglake provisioning is not configured")
	}
	return nil
}
