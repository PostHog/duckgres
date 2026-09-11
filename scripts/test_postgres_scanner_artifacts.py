#!/usr/bin/env python3
"""Verify the configured postgres_scanner downloads for both Linux platforms."""

import gzip
import hashlib
from pathlib import Path
import re
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parent.parent
KEYS = (
    "DUCKDB_EXTENSION_VERSION",
    "POSTGRES_SCANNER_REPOSITORY",
    "POSTGRES_SCANNER_SHA256_AMD64",
    "POSTGRES_SCANNER_SHA256_ARM64",
)


def one(text, pattern, label):
    matches = re.findall(pattern, text, re.MULTILINE)
    if len(matches) != 1:
        raise ValueError(f"{label}: expected exactly one value, found {len(matches)}")
    return matches[0]


def configured_pins():
    configurations = {}
    for filename in (
        "Dockerfile",
        "Dockerfile.worker",
        ".github/workflows/e2e-mw-dev.yml",
        ".github/workflows/scenario-dev.yml",
    ):
        text = (ROOT / filename).read_text()
        configurations[filename] = {
            key: one(text, rf"^\s*(?:ARG )?{key}=([^\s]+)\s*$", f"{filename}: {key}")
            for key in KEYS
        }

    filename = ".github/workflows/container-image-worker-cd.yml"
    text = (ROOT / filename).read_text()
    # Only the build matrix owns download pins; the manifest matrix has versions too.
    build = text.split("\n    build:", 1)[1].split("\n    manifest:", 1)[0]
    fields = ("version", "pg_scanner_repo", "pg_scanner_sha256_amd64", "pg_scanner_sha256_arm64")
    configurations[filename] = {
        key: one(build, rf'^\s*(?:- )?{field}: "([^"\n]+)"\s*$', f"{filename}: {field}")
        for key, field in zip(KEYS, fields)
    }
    for key, field in zip(KEYS, fields):
        expected = f"{key}=${{{{ matrix.duckdb.{field} }}}}"
        if expected not in build:
            raise ValueError(f"{filename}: missing build argument {expected}")

    pins = configurations["Dockerfile"]
    for filename, actual in configurations.items():
        if actual != pins:
            raise ValueError(f"{filename}: scanner pins differ from Dockerfile defaults")
    print("Scanner version, repository, and checksums agree in all five build configurations.", flush=True)
    return pins


def verify_artifacts(pins):
    version = pins["DUCKDB_EXTENSION_VERSION"]
    repository = pins["POSTGRES_SCANNER_REPOSITORY"].rstrip("/")
    failures = []
    revisions = set()
    with tempfile.TemporaryDirectory(prefix="duckgres-scanner-") as directory:
        for arch in ("amd64", "arm64"):
            platform = f"linux_{arch}"
            url = f"{repository}/v{version}/{platform}/postgres_scanner.duckdb_extension.gz"
            destination = Path(directory) / f"{platform}.gz"
            try:
                subprocess.run(
                    ["curl", "--fail", "--silent", "--show-error", "--location",
                     "--connect-timeout", "30", "--max-time", "180", url, "-o", str(destination)],
                    check=True,
                )
                compressed = destination.read_bytes()
                actual = hashlib.sha256(compressed).hexdigest()
                expected = pins[f"POSTGRES_SCANNER_SHA256_{arch.upper()}"]
                if actual != expected:
                    raise ValueError(f"SHA256 mismatch: expected {expected}, downloaded {actual}; {url}")
                # DuckDB's extension footer has eight 32-byte metadata fields,
                # followed by its 256-byte signature. Verify after the checksum.
                artifact = gzip.decompress(compressed)
                if len(artifact) < 512:
                    raise ValueError("extension is too short to contain metadata")
                footer = artifact[-512:-256]
                fields = [footer[i:i + 32].rstrip(b"\0").decode("ascii") for i in range(0, 256, 32)]
                if fields[6] != platform or fields[5] != f"v{version}":
                    raise ValueError(f"extension metadata mismatch: platform={fields[6]}, version={fields[5]}")
                if fields[7] != "4" or fields[3] != "CPP" or not re.fullmatch(r"[0-9a-f]{7,40}", fields[4]):
                    raise ValueError(f"unexpected extension metadata: {fields!r}")
                revisions.add(fields[4])
                print(f"PASS {platform}: SHA256={actual}, DuckDB={fields[5]}, scanner={fields[4]}; {url}", flush=True)
            except (OSError, subprocess.CalledProcessError, ValueError, EOFError) as error:
                failures.append(f"{platform}: {error}")
    if len(revisions) > 1:
        failures.append(f"platforms have different scanner source revisions: {sorted(revisions)}")
    if failures:
        raise ValueError("\n".join(failures))


if __name__ == "__main__":
    try:
        verify_artifacts(configured_pins())
    except (OSError, ValueError, IndexError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
