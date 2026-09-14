//go:build kubernetes

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type rolloutCanaryStore interface {
	TrinoRolloutCanaryEligible(context.Context, string, string, string) (bool, error)
}

func buildTrinoRolloutReadiness(fleet trinoFleet, store rolloutCanaryStore) (*trinoRolloutReadinessHandler, error) {
	tokenPath := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE"))
	canaryPath := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_ROLLOUT_CANARIES_FILE"))
	if tokenPath == "" && canaryPath == "" {
		return nil, nil
	}
	if tokenPath == "" || canaryPath == "" || len(fleet) == 0 {
		return nil, errors.New("rollout readiness requires token, canaries, and registered cells")
	}
	tokenBytes, err := readRolloutSecretFile(tokenPath, 4096)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(tokenBytes))
	if len(token) < 32 || !validRolloutSecret(token) {
		return nil, errors.New("invalid rollout capability")
	}
	canaryBytes, err := readRolloutSecretFile(canaryPath, 64<<10)
	if err != nil {
		return nil, err
	}
	var config struct {
		Canaries []rolloutCanaryCredential `json:"canaries"`
	}
	decoder := json.NewDecoder(bytes.NewReader(canaryBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || len(config.Canaries) == 0 || len(config.Canaries) > 16 {
		return nil, errors.New("invalid rollout canary configuration")
	}
	canaries := make(map[string]rolloutCanaryCredential)
	for _, canary := range config.Canaries {
		if _, duplicate := canaries[canary.Cell]; duplicate || canary.Cell == "" || canary.OrgID == "" || len(canary.OrgID) > 255 || configstore.ValidateDatabaseName(canary.Principal) != nil || !validRolloutSecret(canary.Password) {
			return nil, errors.New("invalid rollout canary entry")
		}
		canaries[canary.Cell] = canary
	}
	handler := &trinoRolloutReadinessHandler{token: token, slots: make(map[string]rolloutReadinessSlot), limit: make(chan struct{}, 4), timeout: 10 * time.Second}
	for _, wire := range fleet {
		if wire.Cell.PublicID == "" {
			continue
		}
		canary, exists := canaries[wire.Cell.PublicID]
		if !exists || wire.Cell.RoutingGroup == "" || len(wire.Cell.Backends) != 2 || wire.Kubernetes == nil {
			return nil, errors.New("registered rollout cell requires one canary and two slots")
		}
		delete(canaries, wire.Cell.PublicID)
		handler.kube = wire.Kubernetes
		for _, backend := range wire.Cell.Backends {
			if backend.ID != "blue" && backend.ID != "green" {
				return nil, errors.New("unsupported rollout slot")
			}
			key := wire.Cell.RoutingGroup + "/" + backend.ID
			if _, duplicate := handler.slots[key]; duplicate {
				return nil, errors.New("duplicate rollout slot")
			}
			handler.slots[key] = rolloutReadinessSlot{cell: wire.Cell.PublicID, routingGroup: wire.Cell.RoutingGroup, color: backend.ID, namespace: wire.Cell.Namespace, backendName: wire.Cell.RoutingGroup + "-" + backend.ID, coordinatorURL: backend.CoordinatorURL, tlsServerName: backend.TLSServerName, observer: func() (string, string) { return wire.Provisioner.ObserverCredential() }, client: newRolloutHTTPClient(backend.TLSServerName), canary: canary}
		}
	}
	if len(canaries) != 0 || len(handler.slots) == 0 {
		return nil, errors.New("canary references an unknown registered cell")
	}
	handler.probe = newRolloutProbe(func(ctx context.Context, canary rolloutCanaryCredential, cellID string) (bool, error) {
		return store.TrinoRolloutCanaryEligible(ctx, canary.OrgID, cellID, canary.Principal)
	})
	return handler, nil
}

func readRolloutSecretFile(name string, limit int64) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, errors.New("rollout secret file unavailable")
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit || !utf8.Valid(data) {
		return nil, errors.New("invalid rollout secret file")
	}
	return data, nil
}

func validRolloutSecret(value string) bool {
	return value != "" && len(value) <= 4096 && utf8.ValidString(value) && strings.IndexFunc(value, unicode.IsControl) == -1
}
