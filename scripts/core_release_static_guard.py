#!/usr/bin/env python3
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

"""Run every required semantic and structural core release static guard."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class StaticRoute:
    route_id: str
    argv: tuple[str, ...]


@dataclass(frozen=True)
class StaticResult:
    route_id: str
    exit_code: int


def required_routes() -> tuple[StaticRoute, ...]:
    python = sys.executable
    scope = ("--scope", "core-release")
    return (
        StaticRoute("public-api-intent", (python, "scripts/public_api_intent_guard.py", *scope)),
        StaticRoute("telemetry-semantic", (python, "scripts/telemetry_semantic_guard.py", *scope)),
        StaticRoute(
            "rust-hygiene",
            (python, "scripts/rust_hygiene_guard.py", *scope, "--identity", "structural"),
        ),
        StaticRoute("rust-lint-debt", (python, "scripts/rust_lint_debt_guard.py", *scope)),
        StaticRoute(
            "architecture-dependency",
            (python, "scripts/architecture_dependency_guard.py", "--mode", "structural", *scope),
        ),
        StaticRoute(
            "architecture-documentation",
            (python, "scripts/architecture_documentation_guard.py", "--mode", "semantic", *scope),
        ),
        StaticRoute("architecture-debt", (python, "scripts/architecture_debt_guard.py", "--check", *scope)),
        StaticRoute("module-maintainability", (python, "scripts/module_maintainability_guard.py", *scope)),
        StaticRoute(
            "stable-surface",
            (python, "scripts/stable_surface_guard.py", *scope, "--mode", "target"),
        ),
        StaticRoute(
            "architecture-release",
            (python, "scripts/architecture_release_guard.py", *scope, "--mode", "structural"),
        ),
    )


def run_routes(root: Path, routes: Iterable[StaticRoute]) -> tuple[int, tuple[StaticResult, ...]]:
    results: list[StaticResult] = []
    for route in routes:
        print(f"CORE_RELEASE_STATIC_START route={route.route_id}")
        completed = subprocess.run(
            route.argv,
            cwd=root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.stdout:
            sys.stdout.write(completed.stdout)
        if completed.stderr:
            sys.stderr.write(completed.stderr)
        results.append(StaticResult(route.route_id, completed.returncode))
        print(f"CORE_RELEASE_STATIC_RESULT route={route.route_id} exit_code={completed.returncode}")
    failures = [result.route_id for result in results if result.exit_code != 0]
    if failures:
        print(f"CORE_RELEASE_STATIC_FAILED failures={','.join(failures)}", file=sys.stderr)
        return 1, tuple(results)
    print(f"CORE_RELEASE_STATIC_OK routes={len(results)}")
    return 0, tuple(results)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    routes = required_routes()
    if args.list:
        for route in routes:
            print(f"{route.route_id}\t{' '.join(route.argv)}")
        return 0
    status, _ = run_routes(ROOT, routes)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
