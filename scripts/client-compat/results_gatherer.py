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


db = duckdb.connect(":memory:")
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
    """Write results JSON to volume if configured."""
    results_dir = os.environ.get("RESULTS_DIR")
    if not results_dir:
        return
    with lock:
        rows = db.execute("""
            SELECT client, suite, test_name, status, detail, ts::VARCHAR AS ts
            FROM results ORDER BY client, suite, test_name
        """).fetchall()
    cols = ["client", "suite", "test_name", "status", "detail", "ts"]
    records = [dict(zip(cols, row)) for row in rows]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(results_dir, f"results_{ts}.json")
    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"Results written to {out_path}", flush=True)


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
