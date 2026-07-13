//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

// Reshard-runner run mode (`--mode reshard-runner`): the entrypoint of the
// dedicated per-operation pod duckgres-reshard-op-<id> (spawned by the CP —
// see reshard_pod.go). The process claims exactly ONE operation
// (DUCKGRES_RESHARD_OP_ID), executes the reshard step machine
// (provisioner.ReshardRunner) to a terminal state, and exits.
//
// Exit semantics: the operation ROW is the source of truth for the outcome —
// a failed/cancelled/rolled-back op still exits 0. Only infrastructure
// problems (config store unreachable, claim not winnable, fenced away by a
// takeover) exit nonzero.

// ReshardRunnerModeConfig carries the resolved configuration the mode needs.
type ReshardRunnerModeConfig struct {
	ConfigStoreConn    string        // config-store DSN (DUCKGRES_CONFIG_STORE via the resolver)
	ConfigPollInterval time.Duration // the CP fleet's snapshot poll (post-block propagation wait)
	AWSRegion          string        // STS region for the pre-flip backup's AssumeRole ("" → backup nil-degrades)
}

// RunReshardRunnerMode executes the single operation named by
// DUCKGRES_RESHARD_OP_ID and returns when it reached a terminal state (or an
// infrastructure error occurred).
func RunReshardRunnerMode(cfg ReshardRunnerModeConfig) error {
	opID, err := strconv.ParseInt(os.Getenv("DUCKGRES_RESHARD_OP_ID"), 10, 64)
	if err != nil {
		return fmt.Errorf("reshard-runner mode requires DUCKGRES_RESHARD_OP_ID (got %q): %w", os.Getenv("DUCKGRES_RESHARD_OP_ID"), err)
	}
	if cfg.ConfigStoreConn == "" {
		return fmt.Errorf("reshard-runner mode requires a config store (DUCKGRES_CONFIG_STORE)")
	}
	if cfg.ConfigPollInterval <= 0 {
		cfg.ConfigPollInterval = 30 * time.Second
	}

	// SIGTERM (pod deletion) cancels the run context; the step machine treats
	// that as a failure at the current step and rolls back within the pod's
	// termination grace.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, cfg.ConfigPollInterval)
	if err != nil {
		return fmt.Errorf("connect config store: %w", err)
	}

	ducklingClient, err := provisioner.NewDucklingClient()
	if err != nil {
		return fmt.Errorf("duckling client (in-cluster kubernetes API): %w", err)
	}

	// Pre-flip catalog backuper: same nil-degrade contract as before — without
	// STS the runner skips the best-effort backup on non-destructive directions
	// and hard-fails the destructive cnpg→external direction.
	var backuper provisioner.CatalogBackuper
	if cfg.AWSRegion != "" {
		if stsBroker, err := NewSTSBroker(ctx, cfg.AWSRegion); err != nil {
			slog.Warn("STS broker unavailable; pre-flip catalog backup disabled.", "error", err)
		} else {
			backuper = provisioner.NewPGCatalogBackuper(func(ctx context.Context, roleARN string) (string, string, string, error) {
				creds, err := stsBroker.AssumeRole(ctx, roleARN)
				if err != nil {
					return "", "", "", err
				}
				return creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, nil
			})
		}
	}

	runnerID := os.Getenv("POD_NAME")
	if runnerID == "" {
		runnerID, _ = os.Hostname()
	}
	if runnerID == "" {
		runnerID = "reshard-runner"
	}

	runner := provisioner.NewReshardRunner(store, ducklingClient, runnerID, cfg.ConfigPollInterval, backuper)

	// Ephemeral external-target password handoff: pull it from the creating CP
	// replica's in-memory stash over the internal-secret-authed admin endpoint.
	// A failed pull is NOT an infra error — the runner proceeds without the
	// stash and the step machine fails the op with the clear "password is not
	// available … cancel and re-run" message (and rolls back), which is exactly
	// the contract when the stashing replica is gone.
	if url := os.Getenv("DUCKGRES_RESHARD_PASSWORD_URL"); url != "" {
		if password, err := fetchReshardPassword(ctx, url, os.Getenv("DUCKGRES_INTERNAL_SECRET")); err != nil {
			slog.Warn("Fetching the ephemeral external-target password failed; the operation will fail with a clear re-run message if it needs it.",
				"op", opID, "error", err)
		} else {
			runner.StashExternalPassword(opID, password)
			slog.Info("Ephemeral external-target password fetched from the creating control-plane replica.", "op", opID)
		}
	}

	slog.Info("Reshard runner pod starting.", "op", opID, "runner", runnerID)
	return runner.RunSingleOperation(ctx, opID)
}

// reshardPasswordFetchTimeout bounds each password pull attempt.
const reshardPasswordFetchTimeout = 10 * time.Second

// fetchReshardPassword GETs the ephemeral password from the creating CP
// replica (internal-secret auth). Retries transient failures briefly; a 404
// (stash gone / op terminal) is terminal immediately. The password is never
// logged.
func fetchReshardPassword(ctx context.Context, url, internalSecret string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		password, retryable, err := fetchReshardPasswordOnce(ctx, url, internalSecret)
		if err == nil {
			return password, nil
		}
		lastErr = err
		if !retryable {
			return "", err
		}
	}
	return "", lastErr
}

func fetchReshardPasswordOnce(ctx context.Context, url, internalSecret string) (password string, retryable bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, reshardPasswordFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("X-Duckgres-Internal-Secret", internalSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", true, fmt.Errorf("password fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		var body struct {
			Password string `json:"password"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
			return "", false, fmt.Errorf("password fetch: decode response: %w", err)
		}
		if body.Password == "" {
			return "", false, fmt.Errorf("password fetch: empty password in response")
		}
		return body.Password, false, nil
	case http.StatusNotFound:
		return "", false, fmt.Errorf("password fetch: 404 — the creating control-plane replica no longer has the stash (replica gone or operation terminal)")
	default:
		return "", true, fmt.Errorf("password fetch: HTTP %d", resp.StatusCode)
	}
}
