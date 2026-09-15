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
	propertiesSource := stringFromWith(step, "properties_source", "")
	if (source == "") == (propertiesSource == "") {
		return fmt.Errorf("setup_hoglake requires exactly one of source or properties_source")
	}
	inputFlag, input := "--source", source
	if propertiesSource != "" {
		inputFlag, input = "--properties-source", propertiesSource
	}
	script, err := requiredString(step, "file")
	if err != nil {
		return err
	}
	catalog := stringFromWith(step, "hoglake_catalog", orgID)
	args := []string{script, "--uri", uri, inputFlag, input, "--catalog", catalog}
	if propertiesSource != "" {
		representation, err := requiredString(step, "representation")
		if err != nil {
			return err
		}
		if representation != "json" && representation != "variant" {
			return fmt.Errorf("properties representation must be json or variant")
		}
		args = append(args, "--properties-representation", representation)
	}
	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("register Hoglake frozen fixtures: %w", err)
	}
	return nil
}
