# AGENTS

## Work Style
- Prefer small, incremental PRs aligned to deliverables.
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When following a plan file, mark tasks upon completion
- When creating new branch from origin/main, do not track origin/main.
- Always run `just lint` before committing.
- Parallelize using subagents when possible.
- Prefer correctness, maintainability, robustness over shortcut implementations.

## Command Runner
- Use `just` recipes instead of raw commands (run `just` to see all recipes).
- `just ci` runs the full local CI pipeline (lint + unit + integration + controlplane tests).
- `just lint` runs `golangci-lint` (not `go vet` — CI uses golangci-lint).
