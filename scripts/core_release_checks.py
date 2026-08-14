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

"""Run the cumulative, scope-aware short checks for a release phase."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "scripts/v1-functional-test-matrix.json"


@dataclass(frozen=True)
class TestSummary:
    matched: int
    executed: int
    passed: int
    failed: int
    ignored: int
    valid: bool


def load_scope(root: Path) -> dict[str, Any]:
    return json.loads((root / "scripts/core-release-scope.json").read_text(encoding="utf-8"))


def load_matrix(root: Path) -> dict[str, Any]:
    return json.loads((root / "scripts/v1-functional-test-matrix.json").read_text(encoding="utf-8"))


def expand_generated_command(kind: str, scope: dict[str, Any]) -> list[str]:
    packages = [item["name"] for item in scope["core_packages"]]
    package_args = [argument for package in packages for argument in ("-p", package)]
    if kind == "core-format":
        return ["cargo", "fmt", "--check", *package_args]
    if kind == "core-clippy":
        return ["cargo", "clippy", "--locked", "--no-deps", "--all-targets", "--all-features", *package_args, "--", "-D", "warnings"]
    raise ValueError(f"unknown generated command kind: {kind}")


def test_summary(list_output: str, run_output: str) -> TestSummary:
    matched = sum(line.rstrip().endswith(": test") for line in list_output.splitlines())
    summaries = re.findall(
        r"test result: (?:ok|FAILED)\.\s+(\d+) passed;\s+(\d+) failed;\s+(\d+) ignored",
        run_output,
    )
    passed = sum(int(item[0]) for item in summaries)
    failed = sum(int(item[1]) for item in summaries)
    ignored = sum(int(item[2]) for item in summaries)
    executed = passed + failed
    valid = matched > 0 and executed > 0 and failed == 0 and passed + failed + ignored == matched
    return TestSummary(matched, executed, passed, failed, ignored, valid)


def _routing_command() -> list[str]:
    if sys.platform == "win32":
        return ["powershell", "-NoProfile", "-File", "scripts/check-agents-routing.ps1"]
    return ["bash", "scripts/check-agents-routing.sh"]


def _argv(route: dict[str, Any], scope: dict[str, Any], version: str) -> list[str]:
    if route["kind"] in {"core-format", "core-clippy"}:
        return expand_generated_command(route["kind"], scope)
    if route["kind"] == "platform-routing":
        return _routing_command()
    return [argument.replace("{version}", version) for argument in route["argv"]]


def _cargo_test_list_argv(argv: list[str]) -> list[str]:
    if "--" in argv:
        split = argv.index("--")
        return [*argv[: split + 1], *argv[split + 1 :], "--list"]
    return [*argv, "--", "--list"]


def _run(argv: list[str], root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, cwd=root, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)


def _safe_segment(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise ValueError(f"unsafe evidence path segment: {value}")
    return value


def run_checks(
    root: Path,
    *,
    phase: int,
    version: str,
    run_id: str,
    attempt: int,
    include_repo_global: bool,
) -> tuple[int, Path]:
    matrix = load_matrix(root)
    scope = load_scope(root)
    evidence_root = root / "target/v1-evidence" / _safe_segment(version) / _safe_segment(run_id) / f"attempt-{attempt}"
    logs = evidence_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    routes = [
        route
        for route in matrix["fixed_routes"]
        if route["phase"] <= phase and (include_repo_global or route["scope"] == "core")
    ]
    routes.extend(
        {
            "id": route["test_id"],
            "category": "capability-test",
            "phase": route["phase"],
            "scope": "core",
            "status": "active",
            "owner": route["capability_id"],
            "kind": "cargo-test",
            "argv": route["argv"],
            "blocking": True,
        }
        for route in matrix["capability_routes"]
        if route["phase"] <= phase
    )
    results = []
    blocking_failures = []
    for route in routes:
        started = datetime.now(timezone.utc).isoformat()
        summary = TestSummary(0, 0, 0, 0, 0, True)
        if route["status"] != "active":
            exit_code = 1
            output = f"route is {route['status']} and cannot run before its owning phase is implemented\n"
        else:
            argv = _argv(route, scope, version)
            list_output = ""
            if argv[:2] == ["cargo", "test"]:
                listed = _run(_cargo_test_list_argv(argv), root)
                list_output = listed.stdout + listed.stderr
                if listed.returncode != 0:
                    completed = listed
                else:
                    completed = _run(argv, root)
            else:
                completed = _run(argv, root)
            output = list_output + completed.stdout + completed.stderr
            exit_code = completed.returncode
            if argv[:2] == ["cargo", "test"]:
                summary = test_summary(list_output, completed.stdout + completed.stderr)
                if not summary.valid:
                    exit_code = exit_code or 1
        ended = datetime.now(timezone.utc).isoformat()
        log_path = logs / f"{route['id']}.log"
        log_path.write_text(output, encoding="utf-8", newline="\n")
        result = {
            "route_id": route["id"],
            "category": route["category"],
            "scope": route["scope"],
            "status": route["status"],
            "blocking": route["blocking"],
            "command": _argv(route, scope, version) if route["status"] == "active" else route["argv"],
            "started_at": started,
            "ended_at": ended,
            "exit_code": exit_code,
            **asdict(summary),
            "log_path": log_path.relative_to(root).as_posix(),
        }
        results.append(result)
        if exit_code != 0 and route["blocking"] and route["scope"] == "core":
            blocking_failures.append(route["id"])
    report = {
        "schema_version": 1,
        "version": version,
        "run_id": run_id,
        "attempt": attempt,
        "phase": phase,
        "status": "failed" if blocking_failures else "passed",
        "blocking_failures": blocking_failures,
        "results": results,
    }
    report_path = evidence_root / "short-check-results.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    return (1 if blocking_failures else 0), report_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", type=int, choices=range(0, 7), required=True)
    parser.add_argument("--version")
    parser.add_argument("--run-id", default="local")
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--include-repo-global", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    matrix = load_matrix(ROOT)
    if args.list:
        for route in matrix["fixed_routes"]:
            if route["phase"] <= args.phase:
                print(f"{route['id']}\t{route['scope']}\t{route['status']}")
        for route in matrix["capability_routes"]:
            if route["phase"] <= args.phase:
                print(f"{route['test_id']}\tcore\tactive")
        return 0
    if args.attempt < 1:
        parser.error("--attempt must be positive")
    if args.version is None:
        import set_workspace_version

        args.version = set_workspace_version.workspace_version(ROOT)
    status, report = run_checks(
        ROOT,
        phase=args.phase,
        version=args.version,
        run_id=args.run_id,
        attempt=args.attempt,
        include_repo_global=args.include_repo_global,
    )
    print(f"CORE_RELEASE_CHECKS_{'OK' if status == 0 else 'FAILED'} phase={args.phase} report={report.relative_to(ROOT)}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
