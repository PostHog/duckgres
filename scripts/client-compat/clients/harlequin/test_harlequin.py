#!/usr/bin/env python3
"""
Runs the exact catalog introspection queries that harlequin-postgres sends
on connect, verifying duckgres compatibility. Optionally launches harlequin
in a pseudo-terminal to verify it doesn't crash on startup.

Expects PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE env vars.
"""

import os
import subprocess
import sys
import time

import psycopg2
import yaml

from report_client import ReportClient


def load_queries(path="/queries.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "duckgres"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        sslmode="require",
    )


def run_query(cur, rc, name, sql, params=None):
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        print(f"  PASS  {name} ({len(rows)} rows)")
        rc.report("catalog", name, "pass", f"{len(rows)} rows")
        return rows
    except Exception as e:
        print(f"  FAIL  {name}: {e}")
        rc.report("catalog", name, "fail", str(e))
        return None


def test_catalog_queries(rc):
    """Run the same introspection queries harlequin-postgres sends on connect."""
    print("=== Harlequin catalog introspection queries ===")
    conn = get_conn()
    cur = conn.cursor()
    failures = 0

    # 1. Database listing
    rows = run_query(
        cur, rc,
        "pg_database listing",
        """SELECT datname FROM pg_database
           WHERE datistemplate IS false AND datallowconn IS true
           ORDER BY datname ASC""",
    )
    if rows is None:
        failures += 1

    # 2. Schema discovery
    rows = run_query(
        cur, rc,
        "information_schema.schemata",
        """SELECT schema_name FROM information_schema.schemata
           WHERE schema_name != 'information_schema'
             AND schema_name NOT LIKE 'pg_%%'
           ORDER BY schema_name ASC""",
    )
    if rows is None:
        failures += 1

    # 3. Table/view listing
    rows = run_query(
        cur, rc,
        "information_schema.tables",
        """SELECT table_name, table_type FROM information_schema.tables
           WHERE table_schema = 'public'
           ORDER BY table_name ASC""",
    )
    if rows is None:
        failures += 1

    # 4. Materialized views
    rows = run_query(
        cur, rc,
        "pg_matviews",
        """SELECT matviewname FROM pg_matviews
           WHERE schemaname = 'public'
           ORDER BY matviewname ASC""",
    )
    if rows is None:
        failures += 1

    # 5. Column listing (for each table found)
    if rows is not None:
        # Get tables first
        cur.execute(
            """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
               ORDER BY table_name ASC LIMIT 5"""
        )
        tables = cur.fetchall()
        for (table_name,) in tables:
            r = run_query(
                cur, rc,
                f"columns({table_name})",
                """SELECT column_name, data_type FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = %s
                   ORDER BY ordinal_position ASC""",
                (table_name,),
            )
            if r is None:
                failures += 1

    # 6. Materialized view columns (via pg_attribute)
    rows = run_query(
        cur, rc,
        "pg_attribute + format_type",
        """SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
           FROM pg_attribute a
           JOIN pg_class t ON a.attrelid = t.oid
           JOIN pg_namespace s ON t.relnamespace = s.oid
           WHERE a.attnum > 0 AND NOT a.attisdropped
             AND s.nspname = 'public'
           ORDER BY a.attnum
           LIMIT 20""",
    )
    if rows is None:
        failures += 1

    cur.close()
    conn.close()
    return failures


def test_shared_queries(rc):
    """Run all queries from the shared queries.yaml file."""
    print("\n=== Shared catalog queries ===")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    failures = 0

    for q in load_queries():
        suite = q["suite"]
        name = q["name"]
        sql = q["sql"]
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            print(f"  PASS  {name} ({len(rows)} rows)")
            rc.report(suite, name, "pass", f"{len(rows)} rows")
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            rc.report(suite, name, "fail", str(e))
            failures += 1

    cur.close()
    conn.close()
    return failures


def test_harlequin_startup(rc):
    """Launch harlequin with a pseudo-terminal and verify it connects without crashing."""
    print("\n=== Harlequin TUI startup test ===")

    dsn = "postgres://{user}:{password}@{host}:{port}/{dbname}?sslmode=require".format(
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        host=os.environ.get("PGHOST", "duckgres"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
    )

    # Use `script` to provide a pseudo-terminal for the TUI.
    # Harlequin needs a terminal to render; without one it fails immediately.
    # We let it run for a few seconds (enough to connect and load schema),
    # then kill it. If it crashes during connect, `script` captures the error.
    output_file = "/tmp/harlequin_output.txt"
    proc = subprocess.Popen(
        [
            "script",
            "-qfc",
            f"uv run --with 'harlequin[postgres]' harlequin -a postgres '{dsn}'",
            output_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "TERM": "xterm-256color", "COLUMNS": "120", "LINES": "40"},
    )

    # Let harlequin connect and render for a few seconds
    time.sleep(8)
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()

    # Check output for errors
    try:
        with open(output_file) as f:
            output = f.read()
    except FileNotFoundError:
        output = ""

    # Harlequin prints connection errors to the terminal
    error_indicators = [
        "connection refused",
        "could not connect",
        "OperationalError",
        "psycopg2.Error",
        "Traceback",
        "FATAL",
    ]

    for indicator in error_indicators:
        if indicator.lower() in output.lower():
            print(f"  FAIL  Harlequin startup: found '{indicator}' in output")
            print(f"  Output (last 500 chars): {output[-500:]}")
            rc.report("tui_startup", "harlequin_connect", "fail", f"found '{indicator}'")
            return 1

    print("  PASS  Harlequin connected and rendered without errors")
    rc.report("tui_startup", "harlequin_connect", "pass")
    return 0


def main():
    # Wait for duckgres to be ready
    print("Waiting for duckgres to accept connections...")
    for attempt in range(30):
        try:
            conn = get_conn()
            conn.close()
            print(f"Connected after {attempt + 1} attempts.")
            break
        except Exception:
            time.sleep(1)
    else:
        print("FAIL: Could not connect to duckgres after 30 seconds")
        sys.exit(1)

    rc = ReportClient("harlequin")
    failures = 0
    failures += test_shared_queries(rc)
    failures += test_catalog_queries(rc)
    failures += test_harlequin_startup(rc)

    print(f"\n{'='*40}")
    if failures > 0:
        print(f"FAILED: {failures} test(s) failed")
    else:
        print("ALL TESTS PASSED")

    rc.done()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
