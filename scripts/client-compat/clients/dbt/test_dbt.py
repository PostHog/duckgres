#!/usr/bin/env python3
"""
dbt-postgres compatibility test for duckgres.

Runs dbt debug, run, and test against duckgres, reporting each step.
Also runs the shared query catalog like every other client.
"""

import json
import os
import shutil
import subprocess
import sys
import time

import yaml

from report_client import ReportClient

PROJECT_DIR = "/dbt_project"
QUERIES_PATH = "/queries.yaml"

host = os.environ.get("PGHOST", "duckgres")
port = os.environ.get("PGPORT", "5432")
user = os.environ.get("PGUSER", "postgres")
password = os.environ.get("PGPASSWORD", "postgres")


class Results:
    def __init__(self, rc: ReportClient):
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.rc = rc
        self.current_suite = ""

    def set_suite(self, suite: str):
        self.current_suite = suite

    def ok(self, name, detail=""):
        self.passed += 1
        suffix = f" ({detail})" if detail else ""
        print(f"  PASS  {name}{suffix}", flush=True)
        self.rc.report(self.current_suite, name, "pass", detail)

    def fail(self, name, reason):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  FAIL  {name}: {reason}", flush=True)
        self.rc.report(self.current_suite, name, "fail", reason)


def wait_for_duckgres():
    """Poll duckgres until it's ready."""
    print(f"Waiting for duckgres at {host}:{port}...", flush=True)
    for attempt in range(30):
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password,
                sslmode="require",
            )
            conn.cursor().execute("SELECT 1")
            conn.close()
            print("  duckgres is ready.", flush=True)
            return
        except Exception:
            time.sleep(1)
    print("FATAL: duckgres not reachable after 30 attempts", flush=True)
    sys.exit(1)


def run_dbt(*args, capture=True):
    """Run a dbt command and return (returncode, stdout, stderr)."""
    cmd = [
        "dbt", *args,
        "--project-dir", PROJECT_DIR,
        "--profiles-dir", PROJECT_DIR,
    ]
    print(f"  $ {' '.join(cmd)}", flush=True)
    result = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        cwd=PROJECT_DIR,
    )
    if capture:
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                print(f"    {line}", flush=True)
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                print(f"    [stderr] {line}", flush=True)
    return result.returncode, result.stdout or "", result.stderr or ""


def test_shared_queries(r):
    """Run the shared query catalog via psycopg2."""
    import psycopg2

    r.set_suite("")  # suite comes from YAML
    with open(QUERIES_PATH) as f:
        queries = yaml.safe_load(f)

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password,
        sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    for entry in queries:
        suite = entry["suite"]
        name = entry["name"]
        sql = entry["sql"]
        r.current_suite = suite
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            r.ok(name, f"{len(rows)} rows")
        except Exception as e:
            r.fail(name, str(e).split("\n")[0])
            # Reconnect after error
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password,
                sslmode="require",
            )
            conn.autocommit = True
            cur = conn.cursor()

    cur.close()
    conn.close()


def test_dbt_debug(r):
    """Run dbt debug — tests connection and catalog introspection."""
    r.set_suite("dbt_lifecycle")
    rc, stdout, stderr = run_dbt("debug")
    if rc == 0:
        r.ok("dbt_debug", "connection ok")
    else:
        # Extract the failure reason from output
        detail = ""
        for line in (stdout + stderr).split("\n"):
            if "ERROR" in line or "error" in line.lower():
                detail = line.strip()[:200]
                break
        r.fail("dbt_debug", detail or f"exit code {rc}")


def test_dbt_run(r):
    """Run dbt run — materializes models (view + table)."""
    r.set_suite("dbt_lifecycle")
    rc, stdout, stderr = run_dbt("run")
    if rc == 0:
        r.ok("dbt_run", "models materialized")
    else:
        detail = ""
        for line in (stdout + stderr).split("\n"):
            if "ERROR" in line or "Compilation Error" in line or "Database Error" in line:
                detail = line.strip()[:200]
                break
        r.fail("dbt_run", detail or f"exit code {rc}")


def test_dbt_test(r):
    """Run dbt test — schema tests (not_null, unique)."""
    r.set_suite("dbt_lifecycle")
    rc, stdout, stderr = run_dbt("test")
    if rc == 0:
        r.ok("dbt_test", "schema tests passed")
    else:
        detail = ""
        for line in (stdout + stderr).split("\n"):
            if "FAIL" in line or "ERROR" in line:
                detail = line.strip()[:200]
                break
        r.fail("dbt_test", detail or f"exit code {rc}")


def test_dbt_catalog(r):
    """Run dbt docs generate — heavy catalog introspection."""
    r.set_suite("dbt_lifecycle")
    rc, stdout, stderr = run_dbt("docs", "generate")
    if rc == 0:
        # Check that catalog.json was produced
        catalog_path = os.path.join(PROJECT_DIR, "target", "catalog.json")
        if os.path.exists(catalog_path):
            with open(catalog_path) as f:
                catalog = json.load(f)
            node_count = len(catalog.get("nodes", {}))
            r.ok("dbt_docs_generate", f"catalog: {node_count} nodes")
        else:
            r.ok("dbt_docs_generate", "completed but no catalog.json")
    else:
        detail = ""
        for line in (stdout + stderr).split("\n"):
            if "ERROR" in line:
                detail = line.strip()[:200]
                break
        r.fail("dbt_docs_generate", detail or f"exit code {rc}")


def main():
    wait_for_duckgres()

    rc = ReportClient("dbt")
    r = Results(rc)

    print("\n=== Shared Queries ===", flush=True)
    test_shared_queries(r)

    print("\n=== dbt debug ===", flush=True)
    test_dbt_debug(r)

    print("\n=== dbt run ===", flush=True)
    test_dbt_run(r)

    print("\n=== dbt test ===", flush=True)
    test_dbt_test(r)

    print("\n=== dbt docs generate ===", flush=True)
    test_dbt_catalog(r)

    # Summary
    print(f"\n{'='*50}", flush=True)
    print(f"dbt: {r.passed} passed, {r.failed} failed", flush=True)
    if r.errors:
        print("Failures:", flush=True)
        for name, reason in r.errors:
            print(f"  {name}: {reason}", flush=True)

    rc.done()
    sys.exit(1 if r.failed else 0)


if __name__ == "__main__":
    main()
