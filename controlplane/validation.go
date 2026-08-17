package controlplane

import (
	"fmt"
	"strings"
)

func validateTLSFiles(certFile, keyFile string) error {
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("TLS certificate and key are required")
	}
	return nil
}

func validateUsers(users map[string]string) error {
	if len(users) == 0 {
		return fmt.Errorf("at least one user is required")
	}

	for username, password := range users {
		if strings.TrimSpace(username) == "" {
			return fmt.Errorf("username must not be empty")
		}
		if password == "" {
			return fmt.Errorf("password for user %q must not be empty", username)
		}
	}

	return nil
}

func validateControlPlaneSecurity(cfg ControlPlaneConfig) error {
	if err := validateTLSFiles(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
		return err
	}
	if err := validateUsers(cfg.Users); err != nil {
		return err
	}
	return nil
}

func validateWorkerBackendConfig(cfg ControlPlaneConfig) error {
	if cfg.WorkerBackend == "remote" && strings.TrimSpace(cfg.ConfigStoreConn) == "" {
		return fmt.Errorf("worker_backend=remote requires config_store for multitenant K8s mode")
	}
	if cfg.WorkerBackend != "remote" && strings.TrimSpace(cfg.ConfigStoreConn) != "" {
		return fmt.Errorf("config_store requires worker_backend=remote")
	}
	return nil
}
