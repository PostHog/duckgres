#!/usr/bin/env python3
"""Generate a deterministic test-impact summary for a git diff.

The goal is PR review visibility, not a merge gate. The classifier is deliberately
rule-based so reviewers can audit why a warning appeared.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


TEST_CASE_RE = re.compile(r"^\s*(func\s+Test[A-Za-z0-9_]*\s*\(|def\s+test_[A-Za-z0-9_]*\s*\()")
ASSERTION_RE = re.compile(
    r"(require\.|assert\.|t\.Fatalf?\(|t\.Errorf?\(|\bfail\s+\"|\|\|\s*fail\b)"
)
SKIP_RE = re.compile(
    r"(t\.Skipf?\(|pytest\.mark\.skip|\.skip\(|\bSkip\s*:|knownFailures)",
    re.IGNORECASE,
)
TEST_COMMAND_RE = re.compile(r"\b(go test|just test[-A-Za-z0-9_]*)\b")
WORKFLOW_JOB_RE = re.compile(r"^\s{2}[A-Za-z0-9_-]+:\s*(#.*)?$")
RETRY_RE = re.compile(r"\b(retry|retrying|attempt)\b", re.IGNORECASE)


@dataclass
class FileDeltas:
    test_added: int = 0
    test_changed: int = 0
    test_deleted: int = 0
    e2e_added: int = 0
    e2e_changed: int = 0
    e2e_deleted: int = 0
    workflow_added: int = 0
    workflow_changed: int = 0
    workflow_deleted: int = 0
    runner_changed: int = 0


@dataclass
class Warning:
    title: str
    severity: str
    details: list[str] = field(default_factory=list)


@dataclass
class TestImpactPlan:
    file_deltas: FileDeltas = field(default_factory=FileDeltas)
    changed_test_files: list[str] = field(default_factory=list)
    deleted_test_files: list[str] = field(default_factory=list)
    changed_e2e_files: list[str] = field(default_factory=list)
    changed_workflow_files: list[str] = field(default_factory=list)
    test_cases_added: int = 0
    test_cases_removed: int = 0
    assertions_added: int = 0
    assertions_removed: int = 0
    skips_added: int = 0
    continue_on_error_added: int = 0
    path_filters_added: int = 0
    retry_lines_added: int = 0
    workflow_jobs_removed: int = 0
    test_commands_removed: int = 0
    warnings: list[Warning] = field(default_factory=list)

    @property
    def coverage_risk(self) -> str:
        if any(w.severity == "likely_reduced" for w in self.warnings):
            return "likely reduced"
        if self.warnings:
            return "needs review"
        return "neutral or increased"


def is_test_file(path: str) -> bool:
    base = os.path.basename(path)
    return (
        path.startswith("tests/")
        or base.endswith("_test.go")
        or base.endswith("_test.py")
        or (
            path.startswith("scripts/client-compat/")
            and base.startswith("test_")
            and base.endswith(".py")
        )
    )


def is_e2e_or_journey_file(path: str) -> bool:
    return path.startswith("tests/e2e-mw-dev/") or path.startswith("tests/journeys/")


def is_workflow_file(path: str) -> bool:
    return path.startswith(".github/workflows/")


def is_runner_file(path: str) -> bool:
    return path == "justfile" or is_workflow_file(path)


def parse_name_status(line: str) -> tuple[str, str] | None:
    if not line.strip():
        return None
    parts = line.split("\t")
    status = parts[0]
    if status.startswith("R") or status.startswith("C"):
        if len(parts) < 3:
            return None
        return status[0], parts[2]
    if len(parts) < 2:
        return None
    return status[0], parts[1]


def add_file_delta(plan: TestImpactPlan, status: str, path: str) -> None:
    if is_test_file(path):
        plan.changed_test_files.append(path)
        if status == "A":
            plan.file_deltas.test_added += 1
        elif status == "D":
            plan.file_deltas.test_deleted += 1
            plan.deleted_test_files.append(path)
        else:
            plan.file_deltas.test_changed += 1

    if is_e2e_or_journey_file(path):
        plan.changed_e2e_files.append(path)
        if status == "A":
            plan.file_deltas.e2e_added += 1
        elif status == "D":
            plan.file_deltas.e2e_deleted += 1
        else:
            plan.file_deltas.e2e_changed += 1

    if is_workflow_file(path):
        plan.changed_workflow_files.append(path)
        if status == "A":
            plan.file_deltas.workflow_added += 1
        elif status == "D":
            plan.file_deltas.workflow_deleted += 1
        else:
            plan.file_deltas.workflow_changed += 1

    if is_runner_file(path) and status != "A":
        plan.file_deltas.runner_changed += 1


def strip_diff_marker(line: str) -> str:
    return line[1:] if line[:1] in {"+", "-"} else line


def analyze_patch_line(plan: TestImpactPlan, path: str, line: str) -> None:
    if not line or line.startswith("+++") or line.startswith("---"):
        return
    if line[0] not in {"+", "-"}:
        return

    added = line[0] == "+"
    content = strip_diff_marker(line)
    if content.startswith(("+", "-")):
        return

    if TEST_CASE_RE.search(content):
        if added:
            plan.test_cases_added += 1
        else:
            plan.test_cases_removed += 1

    if ASSERTION_RE.search(content):
        if added:
            plan.assertions_added += 1
        else:
            plan.assertions_removed += 1

    if added and is_test_file(path) and SKIP_RE.search(content):
        plan.skips_added += 1

    if added and is_workflow_file(path) and "continue-on-error:" in content and "true" in content.lower():
        plan.continue_on_error_added += 1

    if added and is_workflow_file(path) and re.match(r"^\s*paths\s*:", content):
        plan.path_filters_added += 1

    if added and is_e2e_or_journey_file(path) and RETRY_RE.search(content):
        plan.retry_lines_added += 1

    if not added and is_workflow_file(path) and WORKFLOW_JOB_RE.match(content):
        plan.workflow_jobs_removed += 1

    if not added and path == "justfile" and TEST_COMMAND_RE.search(content):
        plan.test_commands_removed += 1


def analyze_diff(name_status_lines: Iterable[str], patch_lines: Iterable[str]) -> TestImpactPlan:
    plan = TestImpactPlan()

    for line in name_status_lines:
        parsed = parse_name_status(line)
        if parsed is None:
            continue
        status, path = parsed
        add_file_delta(plan, status, path)

    current_path = ""
    for line in patch_lines:
        if line.startswith("diff --git "):
            parts = line.split()
            if len(parts) >= 4:
                current_path = parts[3][2:] if parts[3].startswith("b/") else parts[3]
            continue
        analyze_patch_line(plan, current_path, line)

    add_warnings(plan)
    return plan


def add_warnings(plan: TestImpactPlan) -> None:
    if plan.deleted_test_files:
        plan.warnings.append(
            Warning(
                title="Test files deleted",
                severity="likely_reduced",
                details=plan.deleted_test_files,
            )
        )

    if plan.test_cases_removed > plan.test_cases_added:
        plan.warnings.append(
            Warning(
                title="Test cases removed",
                severity="likely_reduced",
                details=[
                    f"{plan.test_cases_removed} removed vs {plan.test_cases_added} added"
                ],
            )
        )

    if plan.assertions_removed > plan.assertions_added:
        plan.warnings.append(
            Warning(
                title="Assertions removed",
                severity="needs_review",
                details=[
                    f"{plan.assertions_removed} removed vs {plan.assertions_added} added"
                ],
            )
        )

    if plan.skips_added:
        plan.warnings.append(
            Warning(
                title="New skips or known failures",
                severity="likely_reduced",
                details=[f"{plan.skips_added} skip/allowlist line(s) added"],
            )
        )

    ci_details = []
    if plan.file_deltas.workflow_deleted:
        ci_details.append(f"{plan.file_deltas.workflow_deleted} workflow file(s) deleted")
    if plan.workflow_jobs_removed:
        ci_details.append(f"{plan.workflow_jobs_removed} possible workflow job line(s) removed")
    if plan.continue_on_error_added:
        ci_details.append(f"{plan.continue_on_error_added} continue-on-error line(s) added")
    if plan.path_filters_added:
        ci_details.append(f"{plan.path_filters_added} workflow path filter line(s) added")
    if ci_details:
        plan.warnings.append(
            Warning(
                title="CI/test runner behavior changed",
                severity="likely_reduced",
                details=ci_details,
            )
        )

    if plan.test_commands_removed:
        plan.warnings.append(
            Warning(
                title="Test command removed",
                severity="likely_reduced",
                details=[f"{plan.test_commands_removed} test command line(s) removed from justfile"],
            )
        )

    if plan.retry_lines_added:
        plan.warnings.append(
            Warning(
                title="E2E or journey retry behavior changed",
                severity="needs_review",
                details=[f"{plan.retry_lines_added} retry/attempt line(s) added"],
            )
        )

    if plan.changed_e2e_files and not plan.retry_lines_added:
        plan.warnings.append(
            Warning(
                title="E2E or journey files changed",
                severity="needs_review",
                details=sorted(set(plan.changed_e2e_files)),
            )
        )


def bullet_list(items: Iterable[str], limit: int = 8) -> list[str]:
    values = list(items)
    visible = values[:limit]
    bullets = [f"  - `{item}`" for item in visible]
    if len(values) > limit:
        bullets.append(f"  - ... and {len(values) - limit} more")
    return bullets


def render_markdown(plan: TestImpactPlan) -> str:
    lines = [
        "## Test Impact Plan",
        "",
        "Deterministic summary of how this PR changes tests, CI runners, and coverage-risk signals.",
        "",
        "### Summary",
        "",
        "| Area | Added | Changed | Deleted |",
        "|---|---:|---:|---:|",
        f"| Test files | {plan.file_deltas.test_added} | {plan.file_deltas.test_changed} | {plan.file_deltas.test_deleted} |",
        f"| E2E/journey files | {plan.file_deltas.e2e_added} | {plan.file_deltas.e2e_changed} | {plan.file_deltas.e2e_deleted} |",
        f"| Workflow files | {plan.file_deltas.workflow_added} | {plan.file_deltas.workflow_changed} | {plan.file_deltas.workflow_deleted} |",
        "",
        "### Signals",
        "",
        f"- Test cases: +{plan.test_cases_added} / -{plan.test_cases_removed}",
        f"- Assertions: +{plan.assertions_added} / -{plan.assertions_removed}",
        f"- Skips or known failures added: {plan.skips_added}",
        f"- Workflow `continue-on-error` added: {plan.continue_on_error_added}",
        f"- Workflow path filters added: {plan.path_filters_added}",
        f"- Test commands removed from `justfile`: {plan.test_commands_removed}",
        f"- E2E/journey retry lines added: {plan.retry_lines_added}",
        "",
        f"**Coverage risk: {plan.coverage_risk}**",
    ]

    if plan.warnings:
        lines.extend(["", "### Warnings", ""])
        for warning in plan.warnings:
            lines.append(f"- **{warning.title}** ({warning.severity.replace('_', ' ')})")
            lines.extend(bullet_list(warning.details))
    else:
        lines.extend(["", "No coverage-reduction warnings detected."])

    return "\n".join(lines) + "\n"


def run_git(repo: Path, args: list[str]) -> list[str]:
    out = subprocess.check_output(["git", "-C", str(repo), *args], text=True)
    return out.splitlines()


def write_text(path: str | None, content: str) -> None:
    if path:
        Path(path).write_text(content)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository path")
    parser.add_argument("--base", default="origin/main", help="base ref")
    parser.add_argument("--head", default="HEAD", help="head ref")
    parser.add_argument("--markdown-output", help="write Markdown report here")
    parser.add_argument("--json-output", help="write JSON report here")
    parser.add_argument(
        "--json",
        action="store_true",
        help="print JSON instead of Markdown to stdout",
    )
    args = parser.parse_args(argv)

    repo = Path(args.repo)
    diff_range = f"{args.base}...{args.head}"
    name_status = run_git(repo, ["diff", "--name-status", diff_range])
    patch = run_git(repo, ["diff", "--unified=0", diff_range])
    plan = analyze_diff(name_status, patch)

    markdown = render_markdown(plan)
    payload = json.dumps(asdict(plan), indent=2, sort_keys=True)
    write_text(args.markdown_output, markdown)
    write_text(args.json_output, payload + "\n")

    if args.json:
        sys.stdout.write(payload + "\n")
    else:
        sys.stdout.write(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
