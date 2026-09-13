package perf

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

const StepTypeSetupHoglake = "setup_hoglake"

func (e *Executor) setupHoglake(ctx context.Context, step core.Step) error {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return err
	}
	uri, err := requiredString(step, "uri")
	if err != nil {
		return err
	}
	source := stringFromWith(step, "source", "")
	plan := stringFromWith(step, "properties_plan", "")
	if (source == "") == (plan == "") {
		return fmt.Errorf("setup_hoglake requires exactly one of source or properties_plan")
	}
	inputFlag, input := "--source", source
	if plan != "" {
		inputFlag, input = "--properties-plan", plan
	}
	script, err := requiredString(step, "file")
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "python3", script, "--uri", uri, inputFlag, input, "--catalog", orgID)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("register Hoglake frozen fixtures: %w", err)
	}
	return nil
}
