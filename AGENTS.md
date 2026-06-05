# AGENTS

## Work Style
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When following a plan file, mark tasks upon completion
- Always run `just lint` before committing.
- Parallelize using subagents when possible.
- Prefer correctness, maintainability, robustness over shortcut implementations.

## Command Runner
- Use `just` recipes instead of raw commands (run `just` to see all recipes).
- `just ci` runs the full local CI pipeline (lint + unit + integration + controlplane tests).
- `just lint` runs `golangci-lint` (not `go vet` — CI uses golangci-lint).

## Security / Data Handling
- **This repo is public.** Never expose customer or internal data in anything that lands here — PR titles/bodies, commit messages, code, comments, or test fixtures.
- This includes customer/org IDs and UUIDs, customer names, internal hostnames/cluster names/endpoints, secrets, and internal-only identifiers.
- When a diagnostic detail is needed for context, redact it (`<org-id>`, `org A`/`org B`, "the prod cluster") or keep it in the local conversation only — not in the published artifact.
