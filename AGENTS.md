# AGENTS

## Work Style
- Prefer small, incremental PRs aligned to deliverables.
- TDD with red-green cycle is required: write/adjust tests first and run them to confirm they fail (red), then implement the minimum code to make them pass (green).
- Keep configs and flags explicit; document defaults in README.
- Provide runbooks for local dev and failure recovery.
- When following a plan file, mark tasks upon completion
- When creating new branch from origin/main, do not track origin/main. 
- Always run lint before committing.
- Parallelize using subagents when possible.