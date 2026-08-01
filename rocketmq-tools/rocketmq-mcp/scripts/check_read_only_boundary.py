#!/usr/bin/env python3
"""Fail when the standalone MCP dependency closure enables mutation features."""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
FORBIDDEN = {
    "rocketmq-admin-core": {"client-adapter", "mutation-client-adapter"},
    "rocketmq-client-rust": {"admin-full", "admin-mutation"},
}
FORBIDDEN_IMPORTS = (
    "rocketmq_admin_core::client_adapter",
    "TopicMutationAdmin",
    "ConsumerMutationAdmin",
    "MessageMutationAdmin",
    "DashboardMutationAdmin",
    "MQAdminMutationExt",
    "AuthAdmin",
    "BrokerAdmin",
    "ConsumerAdmin",
    "OffsetAdmin",
    "RouteAdmin",
    "TopicAdmin",
)


def main() -> int:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
    ]
    metadata = json.loads(subprocess.check_output(command, cwd=ROOT, text=True))
    packages = {package["id"]: package["name"] for package in metadata["packages"]}
    violations: list[str] = []
    for node in metadata["resolve"]["nodes"]:
        package = packages.get(node["id"])
        forbidden = FORBIDDEN.get(package)
        if not forbidden:
            continue
        enabled = set(node.get("features", []))
        overlap = sorted(enabled & forbidden)
        if overlap:
            violations.append(f"{package}: forbidden features enabled: {', '.join(overlap)}")

    for path in sorted((ROOT / "src").rglob("*.rs")):
        text = path.read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_IMPORTS:
            if forbidden in text:
                violations.append(f"{path.relative_to(ROOT)} imports forbidden capability `{forbidden}`")

    if violations:
        print("MCP read-only boundary violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    print("MCP read-only dependency boundary passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
