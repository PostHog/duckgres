//go:build !kubernetes

package controlplane

import (
	"fmt"
	"time"
)

// ReshardRunnerModeConfig mirrors the kubernetes-tagged definition so main.go
// compiles without the tag.
type ReshardRunnerModeConfig struct {
	ConfigStoreConn    string
	ConfigPollInterval time.Duration
	AWSRegion          string
}

// RunReshardRunnerMode requires the kubernetes build tag (the runner patches
// Duckling CRs over the in-cluster API).
func RunReshardRunnerMode(ReshardRunnerModeConfig) error {
	return fmt.Errorf("reshard-runner mode requires a binary built with -tags kubernetes")
}
