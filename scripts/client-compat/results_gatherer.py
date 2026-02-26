#!/usr/bin/env python3
"""
Results gatherer for client compatibility tests.

Runs an HTTP server that accepts test results from client containers,
stores them in an in-memory DuckDB database, and produces a summary
report when all clients have reported done.

Endpoints:
    POST /result  — {"client", "suite", "test_name", "status", "detail"}
    POST /done    — {"client"}
    GET  /report  — current results as text

Environment:
    EXPECTED_CLIENTS  — comma-separated list of clients to wait for (e.g. "psycopg,harlequin")
    PORT              — listen port (default 8080)
"""

import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import duckdb
import yaml


QUERIES_PATH = os.environ.get("QUERIES_PATH", "/queries.yaml")

db = duckdb.connect(":memory:")
db.execute("""
    CREATE TABLE queries (
        suite VARCHAR,
        name VARCHAR,
        sql VARCHAR
    )
""")
db.execute("""
    CREATE TABLE results (
        client VARCHAR,
        suite VARCHAR,
        test_name VARCHAR,
        status VARCHAR,
        detail VARCHAR,
        ts TIMESTAMP DEFAULT current_timestamp
    )
""")


db.execute("""
    CREATE VIEW coverage AS
    SELECT q.suite, q.name, q.sql, r.client, r.status, r.detail, r.ts
    FROM queries q
    LEFT JOIN results r ON q.suite = r.suite AND q.name = r.test_name
""")


def load_queries():
    """Load query definitions from YAML into the queries table."""
    with open(QUERIES_PATH) as f:
        entries = yaml.safe_load(f)
    for entry in entries:
        db.execute(
            "INSERT INTO queries VALUES (?, ?, ?)",
            [entry["suite"], entry["name"], entry["sql"]],
        )
    count = db.execute("SELECT count(*) FROM queries").fetchone()[0]
    print(f"Loaded {count} query definitions from {QUERIES_PATH}", flush=True)


load_queries()

lock = threading.Lock()
done_clients: set[str] = set()
expected_clients = set(
    c.strip() for c in os.environ.get("EXPECTED_CLIENTS", "").split(",") if c.strip()
)
has_failures = False
shutdown_event = threading.Event()


def generate_report() -> tuple[str, bool]:
    """Generate the summary report. Returns (report_text, any_failures)."""
    lines = []
    lines.append("")
    lines.append("=" * 70)
    lines.append("  CLIENT COMPATIBILITY REPORT")
    lines.append("=" * 70)

    # Summary by client + suite
    rows = db.execute("""
        SELECT client, suite,
               count(*) FILTER (WHERE status = 'pass') AS pass,
               count(*) FILTER (WHERE status = 'fail') AS fail
        FROM results
        GROUP BY client, suite
        ORDER BY client, suite
    """).fetchall()

    if rows:
        # Column widths
        cw = max(len(r[0]) for r in rows)
        sw = max(len(r[1]) for r in rows)
        cw = max(cw, 8)
        sw = max(sw, 8)

        header = f"  {'Client':<{cw}}  {'Suite':<{sw}}  {'Pass':>6}  {'Fail':>6}"
        lines.append(header)
        lines.append("  " + "-" * (cw + sw + 18))
        for client, suite, passed, failed in rows:
            marker = " *" if failed > 0 else ""
            lines.append(f"  {client:<{cw}}  {suite:<{sw}}  {passed:>6}  {failed:>6}{marker}")
    else:
        lines.append("  No results recorded.")

    # Totals
    totals = db.execute("""
        SELECT count(*) FILTER (WHERE status = 'pass') AS pass,
               count(*) FILTER (WHERE status = 'fail') AS fail
        FROM results
    """).fetchone()
    total_pass, total_fail = totals
    lines.append("  " + "-" * 40)
    lines.append(f"  TOTAL: {total_pass} passed, {total_fail} failed")

    # Failures detail
    failures = db.execute("""
        SELECT client, suite, test_name, detail
        FROM results
        WHERE status = 'fail'
        ORDER BY client, suite, test_name
    """).fetchall()

    if failures:
        lines.append("")
        lines.append("  FAILURES:")
        for client, suite, test_name, detail in failures:
            detail_str = f": {detail}" if detail else ""
            lines.append(f"    {client} / {suite} / {test_name}{detail_str}")

    # Coverage: queries with no results from any client
    untested = db.execute("""
        SELECT q.suite, q.name
        FROM queries q
        LEFT JOIN results r ON q.suite = r.suite AND q.name = r.test_name
        WHERE r.test_name IS NULL
        ORDER BY q.suite, q.name
    """).fetchall()

    if untested:
        lines.append("")
        lines.append("  UNTESTED QUERIES:")
        for suite, name in untested:
            lines.append(f"    {suite} / {name}")

    lines.append("=" * 70)
    lines.append("")

    return "\n".join(lines), total_fail > 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress default request logging
        pass

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def _respond(self, code, body=""):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(body.encode())

    def do_POST(self):
        global has_failures

        if self.path == "/result":
            data = self._read_json()
            client = data.get("client", "")
            suite = data.get("suite", "")
            test_name = data.get("test_name", "")
            status = data.get("status", "")
            detail = data.get("detail", "")

            with lock:
                db.execute(
                    "INSERT INTO results VALUES (?, ?, ?, ?, ?, current_timestamp)",
                    [client, suite, test_name, status, detail],
                )
                if status == "fail":
                    has_failures = True

            self._respond(200, "ok")

        elif self.path == "/done":
            data = self._read_json()
            client = data.get("client", "")

            with lock:
                done_clients.add(client)
                all_done = expected_clients and done_clients >= expected_clients

            if all_done:
                self._respond(200, "all clients done, shutting down")
                shutdown_event.set()
            else:
                remaining = expected_clients - done_clients
                self._respond(200, f"waiting for: {', '.join(sorted(remaining))}")

        else:
            self._respond(404, "not found")

    def do_GET(self):
        if self.path == "/report":
            with lock:
                report, _ = generate_report()
            self._respond(200, report)

        elif self.path == "/health":
            self._respond(200, "ok")

        else:
            self._respond(404, "not found")


def write_results():
    """Write results JSON and DuckDB database to volume if configured."""
    results_dir = os.environ.get("RESULTS_DIR")
    if not results_dir:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # JSON export
    with lock:
        rows = db.execute("""
            SELECT client, suite, test_name, status, detail, ts::VARCHAR AS ts
            FROM results ORDER BY client, suite, test_name
        """).fetchall()
    cols = ["client", "suite", "test_name", "status", "detail", "ts"]
    records = [dict(zip(cols, row)) for row in rows]
    json_path = os.path.join(results_dir, f"results_{ts}.json")
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Results written to {json_path}", flush=True)

    # DuckDB file export (contains both queries and results tables)
    db_path = os.path.join(results_dir, f"results_{ts}.duckdb")
    with lock:
        db.execute(f"ATTACH '{db_path}' AS export_db")
        db.execute("CREATE TABLE export_db.queries AS SELECT * FROM queries")
        db.execute("CREATE TABLE export_db.results AS SELECT * FROM results")
        db.execute("""
            CREATE VIEW export_db.coverage AS
            SELECT q.suite, q.name, q.sql, r.client, r.status, r.detail, r.ts
            FROM export_db.queries q
            LEFT JOIN export_db.results r ON q.suite = r.suite AND q.name = r.test_name
        """)
        db.execute("DETACH export_db")
    print(f"DuckDB written to {db_path}", flush=True)


def main():
    port = int(os.environ.get("PORT", "8080"))

    if not expected_clients:
        print("WARNING: EXPECTED_CLIENTS not set, will never auto-shutdown", flush=True)

    print(f"Results gatherer listening on :{port}", flush=True)
    print(f"Expecting clients: {', '.join(sorted(expected_clients))}", flush=True)

    # On SIGTERM (docker compose stop), dump whatever we have and exit.
    def handle_sigterm(signum, frame):
        print("\nReceived SIGTERM, writing partial results...", flush=True)
        report, _ = generate_report()
        print(report, flush=True)
        write_results()
        sys.exit(1)

    signal.signal(signal.SIGTERM, handle_sigterm)

    server = HTTPServer(("0.0.0.0", port), Handler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    # Wait for all clients to report done
    shutdown_event.wait()

    server.shutdown()

    report, _ = generate_report()
    print(report, flush=True)
    write_results()

    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
