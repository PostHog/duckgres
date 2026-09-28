#!/usr/bin/env python3
"""Test recovery using disposable Kubernetes fixtures, Postgres, and Gateway."""

import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--gateway-image", required=True)
    parser.add_argument("--postgres-image", default="postgres:17-alpine")
    parser.add_argument("--fixture-image", default="busybox:1.37")
    args = parser.parse_args()
    run_id = secrets.token_hex(8)
    namespace = "trino-recovery-e2e-" + run_id
    label = "duckgres.io/recovery-e2e"
    password, token = secrets.token_hex(24), secrets.token_hex(32)
    kubectl = ["kubectl", "--context", args.context, "--request-timeout=30s"]
    processes = []
    namespace_uid = None

    def command(*arguments, data=None):
        return subprocess.run(kubectl + list(arguments), input=data, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout

    def create(obj):
        return json.loads(command("create", "-f", "-", "-o", "json", data=json.dumps(obj)))

    def pod(name, image, container):
        return {"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": name, "namespace": namespace, "labels": {label: run_id}},
            "spec": {"automountServiceAccountToken": False, "restartPolicy": "Never",
                     "containers": [{"name": name, "image": image,
                         "resources": {"requests": {"cpu": "500m", "memory": "768Mi"},
                                       "limits": {"cpu": "500m", "memory": "768Mi"}}, **container}]}}

    def forward(name, remote_port):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        process = subprocess.Popen(kubectl + ["-n", namespace, "port-forward", "--address=127.0.0.1",
                                   "pod/" + name, f"{port}:{remote_port}"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(process)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and process.poll() is None:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return port
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("Disposable fixture port-forward did not start")

    def setup():
        create({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "postgres", "namespace": namespace},
                "stringData": {"password": password}})
        create(pod("postgres", args.postgres_image, {
            "env": [{"name": "POSTGRES_USER", "value": "recovery_test"},
                    {"name": "POSTGRES_DB", "value": "gateway"},
                    {"name": "POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "postgres", "key": "password"}}}],
            "readinessProbe": {"exec": {"command": ["pg_isready", "-U", "recovery_test", "-d", "gateway"]}, "periodSeconds": 2}}))
        command("-n", namespace, "wait", "pod/postgres", "--for=condition=Ready", "--timeout=180s")
        postgres = json.loads(command("-n", namespace, "get", "pod/postgres", "-o", "json"))
        config = {
            "serverConfig": {"node.environment": "test", "http-server.http.port": "8080"},
            "dataStore": {"jdbcUrl": f"jdbc:postgresql://{postgres['status']['podIP']}:5432/gateway",
                          "user": "recovery_test", "password": password, "driver": "org.postgresql.Driver"},
            "monitor": {"taskDelay": "1s"}, "routing": {"defaultRoutingGroup": "recovery-test"},
            "transactionAwareness": {"enabled": True, "identityKey": secrets.token_hex(32),
                                     "adminToken": token, "terminalRetentionSeconds": 3600, "pool": {"enabled": True}}}
        create({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "gateway", "namespace": namespace},
                "stringData": {"config.json": json.dumps(config)}})
        gateway = pod("gateway", args.gateway_image, {
            "command": ["java", "-Xmx384m", "-jar", "/usr/lib/trino-gateway/gateway-ha-jar-with-dependencies.jar", "/fixture/config.json"],
            "volumeMounts": [{"name": "config", "mountPath": "/fixture", "readOnly": True}],
            "readinessProbe": {"httpGet": {"path": "/trino-gateway/readyz", "port": 8080}, "periodSeconds": 2}})
        gateway["spec"]["volumes"] = [{"name": "config", "secret": {"secretName": "gateway"}}]
        create(gateway)
        command("-n", namespace, "wait", "pod/gateway", "--for=condition=Ready", "--timeout=240s")

    try:
        ns = create({"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": namespace, "labels": {label: run_id}}})
        namespace_uid = ns["metadata"]["uid"]
        print("Created disposable recovery boundary namespace", flush=True)
        setup()
        pg_port, gateway_port = forward("postgres", 5432), forward("gateway", 8080)
        env = {**os.environ, "TRINO_RECOVERY_E2E_NAMESPACE": namespace,
               "TRINO_RECOVERY_E2E_RUN_ID": run_id, "TRINO_RECOVERY_E2E_CONTEXT": args.context,
               "TRINO_RECOVERY_E2E_CONFIG_DSN": f"postgres://recovery_test:{password}@127.0.0.1:{pg_port}/postgres?sslmode=disable",
               "TRINO_RECOVERY_E2E_GATEWAY_DSN": f"postgres://recovery_test:{password}@127.0.0.1:{pg_port}/gateway?sslmode=disable",
               "TRINO_RECOVERY_E2E_GATEWAY_URL": f"http://127.0.0.1:{gateway_port}",
               "TRINO_RECOVERY_E2E_TOKEN": token, "TRINO_RECOVERY_E2E_IMAGE": args.fixture_image}
        print("Running real-store recovery assertions with synthetic retained obligations", flush=True)
        subprocess.run(["just", "test-trino-recovery-boundary"], cwd=Path(__file__).resolve().parents[3], env=env, check=True)
    finally:
        for process in processes:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if namespace_uid:
            current = json.loads(command("get", "namespace", namespace, "-o", "json"))
            if current["metadata"]["uid"] != namespace_uid or current["metadata"]["labels"].get(label) != run_id:
                raise RuntimeError("Refusing cleanup: fixture namespace identity changed")
            command("delete", "namespace", namespace, "--wait=true", "--timeout=180s")
            print("Deleted disposable recovery boundary namespace and fixtures", flush=True)


if __name__ == "__main__":
    main()
