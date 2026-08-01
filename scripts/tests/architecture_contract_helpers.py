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

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tomllib
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def load_json(relative: str) -> dict[str, Any]:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def load_toml(relative: str) -> dict[str, Any]:
    return tomllib.loads((ROOT / relative).read_text(encoding="utf-8"))


def source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def workspace_packages() -> dict[str, str]:
    workspace = load_toml("Cargo.toml")["workspace"]
    packages = {}
    for member in workspace["members"]:
        manifest = load_toml(f"{member}/Cargo.toml")
        packages[manifest["package"]["name"]] = member
    return packages


def governed_packages() -> dict[str, str]:
    packages = workspace_packages()
    policy = load_json("scripts/architecture-dependency-policy.json")
    governed = set(policy["target_dag"])
    for manifest_path in policy["roots"]["standalone_manifests"]:
        manifest = load_toml(manifest_path)
        name = manifest["package"]["name"]
        if name in governed:
            packages[name] = str(Path(manifest_path).parent).replace("\\", "/")
    return packages


def normal_dependencies(relative: str) -> set[str]:
    return set(load_toml(relative).get("dependencies", {}))


def run_dependency_guard(mode: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python", "scripts/architecture_dependency_guard.py", "--mode", mode],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
