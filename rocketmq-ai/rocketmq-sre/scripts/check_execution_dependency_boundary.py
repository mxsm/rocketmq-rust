# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Enforce the isolated Executor and target-writing Agent dependency boundary."""

from __future__ import annotations

import json
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any


EXECUTOR = "rocketmq-sre-executor"
AGENT = "rocketmq-sre-execution-agent"
EXECUTOR_FORBIDDEN = {
    AGENT,
    "k8s-openapi",
    "kube",
    "kube-client",
    "kube-core",
    "rocketmq-admin-core",
    "rocketmq-mcp",
    "rocketmq-sre-model-gateway",
}
AGENT_REQUIRED = {"k8s-openapi", "kube", "rocketmq-admin-core"}
MUTATION_FEATURE = "mutation-client-adapter"


def load_metadata(workspace: Path) -> dict[str, Any]:
    command = [
        "cargo",
        "+1.95.0",
        "metadata",
        "--manifest-path",
        str(workspace / "Cargo.toml"),
        "--format-version",
        "1",
        "--locked",
    ]
    completed = subprocess.run(
        command,
        cwd=workspace,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        print(completed.stderr, file=sys.stderr)
        raise RuntimeError("cargo metadata failed")
    return json.loads(completed.stdout)


def package_id(metadata: dict[str, Any], name: str) -> str:
    matches = [package["id"] for package in metadata["packages"] if package["name"] == name]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one package named {name}, found {len(matches)}")
    return matches[0]


def normal_closure(metadata: dict[str, Any], root_name: str) -> set[str]:
    resolve = metadata.get("resolve")
    if not resolve:
        raise RuntimeError("cargo metadata did not contain a resolved dependency graph")
    names = {package["id"]: package["name"] for package in metadata["packages"]}
    edges: dict[str, list[str]] = {}
    for node in resolve["nodes"]:
        normal = []
        for dependency in node["deps"]:
            kinds = dependency.get("dep_kinds", [])
            if not kinds or any(kind.get("kind") is None for kind in kinds):
                normal.append(dependency["pkg"])
        edges[node["id"]] = normal

    seen: set[str] = set()
    pending = deque([package_id(metadata, root_name)])
    while pending:
        current = pending.popleft()
        if current in seen:
            continue
        seen.add(current)
        pending.extend(edges.get(current, []))
    return {names[identifier] for identifier in seen}


def direct_dependency_features(metadata: dict[str, Any], package_name: str, dependency_name: str) -> set[str]:
    package = next(package for package in metadata["packages"] if package["name"] == package_name)
    dependencies = [
        dependency
        for dependency in package["dependencies"]
        if dependency["name"] == dependency_name and dependency.get("kind") is None
    ]
    if len(dependencies) != 1:
        raise RuntimeError(
            f"expected one normal {dependency_name} dependency in {package_name}, found {len(dependencies)}"
        )
    return set(dependencies[0].get("features", []))


def main() -> int:
    workspace = Path(__file__).resolve().parents[1]
    try:
        metadata = load_metadata(workspace)
        executor = normal_closure(metadata, EXECUTOR)
        agent = normal_closure(metadata, AGENT)
        forbidden = sorted(executor & EXECUTOR_FORBIDDEN)
        missing = sorted(AGENT_REQUIRED - agent)
        mutation_features = direct_dependency_features(metadata, AGENT, "rocketmq-admin-core")
    except (KeyError, RuntimeError, json.JSONDecodeError) as error:
        print(f"SRE_EXECUTION_DEPENDENCY_BOUNDARY_ERROR: {error}", file=sys.stderr)
        return 1

    failures = []
    if forbidden:
        failures.append(f"Executor contains forbidden normal dependencies: {', '.join(forbidden)}")
    if missing:
        failures.append(f"Execution Agent is missing required target drivers: {', '.join(missing)}")
    if MUTATION_FEATURE not in mutation_features:
        failures.append(f"Execution Agent does not explicitly enable {MUTATION_FEATURE}")
    if failures:
        for failure in failures:
            print(f"SRE_EXECUTION_DEPENDENCY_BOUNDARY_ERROR: {failure}", file=sys.stderr)
        return 1

    print(
        "SRE_EXECUTION_DEPENDENCY_BOUNDARY_OK "
        f"executor_packages={len(executor)} agent_packages={len(agent)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
