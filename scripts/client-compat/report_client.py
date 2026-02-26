"""
Lightweight client for reporting test results to the results-gatherer.

Fire-and-forget: if the gatherer is unreachable, results are silently
dropped so test scripts still work standalone.

Usage:
    from report_client import ReportClient

    rc = ReportClient("psycopg")
    rc.report("catalog", "pg_database", "pass", "4 rows")
    rc.report("catalog", "pg_settings", "fail", "Table not found")
    rc.done()
"""

import json
import os
import urllib.request


class ReportClient:
    def __init__(self, client_name: str):
        self.client = client_name
        self.url = os.environ.get("RESULTS_URL", "http://results-gatherer:8080")

    def _post(self, path: str, data: dict):
        try:
            req = urllib.request.Request(
                f"{self.url}{path}",
                data=json.dumps(data).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass

    def report(self, suite: str, test_name: str, status: str, detail: str = ""):
        self._post("/result", {
            "client": self.client,
            "suite": suite,
            "test_name": test_name,
            "status": status,
            "detail": detail,
        })

    def done(self):
        self._post("/done", {"client": self.client})
