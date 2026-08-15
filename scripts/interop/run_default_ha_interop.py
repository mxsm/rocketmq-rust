#!/usr/bin/env python3
# Copyright 2023 The RocketMQ Rust Authors
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

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DIRECTIONS = {"java-master-rust-slave", "rust-master-java-slave"}
FAULTS = {"none", "reconnect-resume", "tail-truncate", "slow-replica"}
FLUSH_MODES = {"ASYNC_FLUSH", "SYNC_FLUSH"}


def load_matrix(path: Path) -> dict[str, Any]:
    matrix = json.loads(path.read_text(encoding="utf-8"))
    validate_matrix(matrix)
    return matrix


def validate_matrix(matrix: dict[str, Any]) -> None:
    if matrix.get("schemaVersion") != 1:
        raise ValueError("DefaultHA matrix schemaVersion must be 1")
    if matrix.get("haMode") != "DefaultHA":
        raise ValueError("only the DefaultHA compatibility profile is supported")
    if matrix.get("controllerMode") is not False or matrix.get("dledgerCommitLog") is not False:
        raise ValueError("DefaultHA interop must exclude Controller and DLedger CommitLog")
    wire = matrix.get("wireContract")
    if wire != {"transferHeaderBytes": 12, "slaveReportBytes": 8, "byteOrder": "big-endian"}:
        raise ValueError("DefaultHA wire contract must remain 12-byte transfer / 8-byte report / big-endian")

    scenarios = matrix.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("DefaultHA matrix must contain scenarios")
    ids: set[str] = set()
    observed_directions: set[str] = set()
    for scenario in scenarios:
        scenario_id = scenario.get("id")
        if not isinstance(scenario_id, str) or not scenario_id or scenario_id in ids:
            raise ValueError("scenario ids must be non-empty and unique")
        ids.add(scenario_id)
        direction = scenario.get("direction")
        if direction not in DIRECTIONS:
            raise ValueError(f"{scenario_id}: unsupported direction")
        observed_directions.add(direction)
        if scenario.get("flushMode") not in FLUSH_MODES:
            raise ValueError(f"{scenario_id}: unsupported flush mode")
        if scenario.get("fault") not in FAULTS:
            raise ValueError(f"{scenario_id}: unsupported fault")
        timeout = scenario.get("timeoutSeconds")
        if not isinstance(timeout, int) or not 1 <= timeout <= 600:
            raise ValueError(f"{scenario_id}: timeout must be between 1 and 600 seconds")
        for field in ("requiredAssertions", "requiredEvidence"):
            values = scenario.get(field)
            if not isinstance(values, list) or not values or len(values) != len(set(values)):
                raise ValueError(f"{scenario_id}: {field} must be a non-empty unique list")
    if observed_directions != DIRECTIONS:
        raise ValueError("DefaultHA matrix must cover both Java/Rust directions")


def validate_scenario_result(
    scenario: dict[str, Any], result: dict[str, Any], evidence_root: Path | None = None
) -> None:
    scenario_id = scenario["id"]
    if result.get("id") != scenario_id or result.get("direction") != scenario["direction"]:
        raise ValueError(f"{scenario_id}: result identity does not match the matrix")
    if result.get("status") != "passed":
        raise ValueError(f"{scenario_id}: status must be passed; skipped and partial results are failures")
    assertions = result.get("assertions")
    expected_assertions = set(scenario["requiredAssertions"])
    if not isinstance(assertions, dict) or set(assertions) != expected_assertions:
        raise ValueError(f"{scenario_id}: assertion set does not match the matrix")
    if any(value is not True for value in assertions.values()):
        raise ValueError(f"{scenario_id}: every assertion must pass")
    evidence = result.get("evidence")
    expected_evidence = set(scenario["requiredEvidence"])
    if not isinstance(evidence, dict) or set(evidence) != expected_evidence:
        raise ValueError(f"{scenario_id}: evidence set does not match the matrix")
    if any(not isinstance(value, str) or not value.strip() for value in evidence.values()):
        raise ValueError(f"{scenario_id}: evidence paths must be non-empty strings")
    if evidence_root is not None:
        root = evidence_root.resolve()
        for name, value in evidence.items():
            path = (root / value).resolve()
            if not path.is_relative_to(root):
                raise ValueError(f"{scenario_id}: {name} evidence escapes the output directory")
            if not path.is_file():
                raise ValueError(f"{scenario_id}: missing {name} evidence: {value}")


def validate_result_set(scenarios: list[dict[str, Any]], result_directory: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scenario in scenarios:
        path = result_directory / f"{scenario['id']}.json"
        if not path.is_file():
            raise ValueError(f"missing scenario result: {scenario['id']}")
        result = json.loads(path.read_text(encoding="utf-8"))
        validate_scenario_result(scenario, result, result_directory.parent)
        results.append(result)
    return results


def selected_scenarios(matrix: dict[str, Any], direction: str, scenario_id: str | None) -> list[dict[str, Any]]:
    direction_filter = {
        "Both": DIRECTIONS,
        "JavaMasterRustSlave": {"java-master-rust-slave"},
        "RustMasterJavaSlave": {"rust-master-java-slave"},
    }[direction]
    selected = [item for item in matrix["scenarios"] if item["direction"] in direction_filter]
    if scenario_id is not None:
        selected = [item for item in selected if item["id"] == scenario_id]
        if not selected:
            raise ValueError(f"unknown or direction-mismatched scenario: {scenario_id}")
    return selected


def driver_command(driver: Path, scenario: dict[str, Any], args: argparse.Namespace, result_path: Path) -> list[str]:
    prefix = [sys.executable, str(driver)] if driver.suffix.lower() == ".py" else [str(driver)]
    return prefix + [
        "--scenario",
        scenario["id"],
        "--duration-seconds",
        str(args.duration_seconds),
        "--java-root",
        str(args.java_root),
        "--rust-root",
        str(args.rust_root),
        "--result",
        str(result_path),
    ]


def execute(args: argparse.Namespace) -> int:
    matrix = load_matrix(args.matrix)
    scenarios = selected_scenarios(matrix, args.direction, args.scenario)
    args.output.mkdir(parents=True, exist_ok=True)
    plan = {
        "schemaVersion": 1,
        "status": "planned" if args.dry_run else "running",
        "haMode": "DefaultHA",
        "durationSeconds": args.duration_seconds,
        "scenarioIds": [item["id"] for item in scenarios],
        "remotePublication": "not-executed",
    }
    (args.output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    if args.dry_run:
        print(f"validated {len(scenarios)} DefaultHA scenarios; no processes executed")
        return 0
    if args.case_driver is None or not args.case_driver.is_file():
        raise ValueError("--case-driver must identify an executable DefaultHA process harness")
    if not args.java_root.is_dir() or not args.rust_root.is_dir():
        raise ValueError("Java and Rust repository roots must exist")

    result_directory = args.output / "scenarios"
    result_directory.mkdir(parents=True, exist_ok=True)
    try:
        for scenario in scenarios:
            result_path = result_directory / f"{scenario['id']}.json"
            timeout = min(args.duration_seconds, scenario["timeoutSeconds"])
            completed = subprocess.run(
                driver_command(args.case_driver, scenario, args, result_path),
                cwd=args.rust_root,
                timeout=timeout,
                check=False,
            )
            if completed.returncode != 0:
                raise ValueError(f"{scenario['id']}: case driver exited with {completed.returncode}")
            if not result_path.is_file():
                raise ValueError(f"missing scenario result: {scenario['id']}")
            validate_scenario_result(scenario, json.loads(result_path.read_text(encoding="utf-8")))
        results = validate_result_set(scenarios, result_directory)
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        failure = {**plan, "status": "failed", "error": str(error)}
        (args.output / "run.json").write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8")
        raise

    run = {
        **plan,
        "status": "passed",
        "completedAt": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }
    (args.output / "run.json").write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")
    print(f"passed {len(results)} DefaultHA scenarios")
    return 0


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[2]
    command = argparse.ArgumentParser(description="Run bounded Java/Rust DefaultHA interoperability scenarios")
    command.add_argument("--direction", choices=("Both", "JavaMasterRustSlave", "RustMasterJavaSlave"), default="Both")
    command.add_argument("--duration-seconds", type=int, default=90)
    command.add_argument("--scenario")
    command.add_argument("--matrix", type=Path, default=Path(__file__).with_name("default-ha-matrix.json"))
    command.add_argument("--java-root", type=Path, default=Path(r"D:\Github\Java\rocketmq"))
    command.add_argument("--rust-root", type=Path, default=root)
    command.add_argument("--output", type=Path, default=root / "target" / "default-ha-interop")
    command.add_argument("--case-driver", type=Path)
    command.add_argument("--dry-run", action="store_true")
    return command


def main() -> int:
    args = parser().parse_args()
    if not 1 <= args.duration_seconds <= 600:
        raise ValueError("--duration-seconds must be between 1 and 600")
    return execute(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"DefaultHA interoperability failed: {error}", file=sys.stderr)
        raise SystemExit(1)
