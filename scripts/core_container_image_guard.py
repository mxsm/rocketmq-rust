#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Validate the local-only four-service container and Helm boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
CORE_KEYS = {"namesrv", "broker", "controller", "proxy"}
FORBIDDEN = ("mcp", "sre", "dashboard", "openmessaging", "brokercontainer", "dledger")


def _profile_service_keys(source: str) -> set[str]:
    in_services = False
    keys: set[str] = set()
    for line in source.splitlines():
        if line == "services:":
            in_services = True
            continue
        if in_services and line and not line.startswith(" "):
            break
        match = re.match(r"^  ([a-z][a-z0-9-]*):\s*$", line)
        if in_services and match:
            keys.add(match.group(1))
    return keys


def audit(policy_path: Path, chart: Path) -> list[str]:
    findings: list[str] = []
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    expected_services = {f"rocketmq-{key}" for key in CORE_KEYS}
    if set(policy.get("services", [])) != expected_services:
        findings.append("core policy service set must be exactly namesrv/broker/controller/proxy")
    if policy.get("publication", {}).get("default") != "local-layout-only":
        findings.append("core policy must default to local-layout-only")
    dockerfile = ROOT / policy.get("dockerfile", "")
    if not dockerfile.is_file():
        findings.append("core Dockerfile is missing")
    else:
        source = dockerfile.read_text(encoding="utf-8").lower()
        for required in ("copy --chmod=0555", "user 10001:10001", "entrypoint"):
            if required not in source:
                findings.append(f"core Dockerfile is missing: {required}")
        for forbidden in ("cargo build", "docker push", "buildx --push"):
            if forbidden in source:
                findings.append(f"core Dockerfile contains a forbidden build/publication route: {forbidden}")
    required_chart = {
        "Chart.yaml",
        "values.yaml",
        "values.schema.json",
        "templates/_helpers.tpl",
        "templates/configmaps.yaml",
        "templates/workloads.yaml",
        "templates/services.yaml",
        "templates/networkpolicies.yaml",
        *policy.get("chart", {}).get("profiles", []),
    }
    present = {path.relative_to(chart).as_posix() for path in chart.rglob("*") if path.is_file()}
    if present != required_chart:
        findings.append(
            f"core chart file set changed: missing={sorted(required_chart - present)} extra={sorted(present - required_chart)}"
        )
    schema_path = chart / "values.schema.json"
    if schema_path.is_file():
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        properties = set(schema["properties"]["services"]["properties"])
        if properties != CORE_KEYS:
            findings.append("core values schema service set changed")
    for path in sorted(chart.rglob("*")):
        if not path.is_file():
            continue
        source = path.read_text(encoding="utf-8").lower()
        if any(token in source for token in FORBIDDEN):
            findings.append(f"excluded capability leaked into core chart: {path.relative_to(chart)}")
        if path.name.startswith("values-") or path.name == "values.yaml":
            keys = _profile_service_keys(source)
            if keys and not keys.issubset(CORE_KEYS):
                findings.append(f"unknown service key in {path.name}: {sorted(keys - CORE_KEYS)}")
    helm = shutil.which("helm")
    if helm:
        lint = subprocess.run([helm, "lint", str(chart)], capture_output=True, text=True, check=False)
        if lint.returncode != 0:
            findings.append(f"helm lint failed: {lint.stderr.strip()}")
        for profile in policy.get("chart", {}).get("profiles", []):
            rendered = subprocess.run(
                [helm, "template", "rocketmq-core", str(chart), "-f", str(chart / profile)],
                capture_output=True,
                text=True,
                check=False,
            )
            if rendered.returncode != 0:
                findings.append(f"helm template failed for {profile}: {rendered.stderr.strip()}")
            elif any(token in rendered.stdout.lower() for token in FORBIDDEN):
                findings.append(f"excluded capability rendered for {profile}")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=ROOT / "docker" / "core-container-policy.json")
    parser.add_argument("--chart", type=Path, default=ROOT / "distribution" / "helm" / "rocketmq-rust-core")
    args = parser.parse_args(argv)
    try:
        findings = audit(args.policy.resolve(), args.chart.resolve())
    except (OSError, KeyError, json.JSONDecodeError) as error:
        findings = [str(error)]
    if findings:
        for finding in findings:
            print(f"CORE_CONTAINER_IMAGE_GUARD_FAILED detail={finding}", file=sys.stderr)
        return 1
    print(f"CORE_CONTAINER_IMAGE_GUARD_OK services=4 helm={'available' if shutil.which('helm') else 'unavailable'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
