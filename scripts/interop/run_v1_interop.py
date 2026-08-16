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

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESULT_IDS = ("I01", "I02", "I03", "I04", "I05")
EXCLUDED_MODES = {"Java Controller", "Java AutoSwitchHA", "DLedger CommitLog"}
JAVA_VERSION = "5.5.0"
MAX_TIMEOUT_SECONDS = 600
REQUIRED_CAPABILITIES = {
    "I01": {"send", "pull", "pop", "transaction", "timer", "query"},
    "I02": {"send", "pull", "pop", "transaction", "timer", "query", "error-code"},
    "I03": {"active-route", "auth", "opaque", "response"},
    "I04": {"settings", "fifo", "lite", "retry", "renewal"},
    "I05": {"java-master-rust-slave", "rust-master-java-slave", "replication", "reconnect", "tail-truncate"},
}
REQUIRED_NEGATIVE_CASES = {
    "I01": {"unknown-code", "invalid-expression", "permission-denied", "timeout"},
    "I02": {"unknown-code", "invalid-expression", "permission-denied", "timeout"},
    "I03": {"unknown-code", "permission-denied", "timeout"},
    "I04": {"invalid-expression", "permission-denied", "timeout"},
    "I05": {"timeout", "malformed-frame"},
}


class InteropError(ValueError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_matrix(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InteropError(f"unable to read interop matrix: {error}") from error
    validate_matrix(value)
    return value


def _unique_non_empty_strings(scenario_id: str, field: str, value: Any) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item.strip() for item in value)
        or len(value) != len(set(value))
    ):
        raise InteropError(f"{scenario_id}: {field} must be a non-empty unique string list")
    return value


def validate_matrix(matrix: dict[str, Any]) -> None:
    if matrix.get("schemaVersion") != 1:
        raise InteropError("v1 interop matrix schemaVersion must be 1")
    if matrix.get("javaVersion") != JAVA_VERSION:
        raise InteropError(f"v1 interop matrix must use Java {JAVA_VERSION}")
    if matrix.get("resultIds") != list(RESULT_IDS):
        raise InteropError("v1 interop matrix resultIds must be I01 through I05 in order")
    if set(matrix.get("excludedModes", [])) != EXCLUDED_MODES:
        raise InteropError("v1 interop matrix must exclude Java Controller, AutoSwitchHA, and DLedger CommitLog")
    if matrix.get("remotePublication") != "not-executed":
        raise InteropError("v1 interop matrix cannot perform remote publication")

    scenarios = matrix.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != len(RESULT_IDS):
        raise InteropError("v1 interop matrix must contain exactly five scenarios")
    ids = [scenario.get("id") for scenario in scenarios if isinstance(scenario, dict)]
    if len(ids) != len(scenarios) or len(ids) != len(set(ids)):
        raise InteropError("scenario ids must be non-empty and unique")
    if ids != list(RESULT_IDS):
        raise InteropError("scenario order and identities must match resultIds")

    for scenario in scenarios:
        scenario_id = scenario["id"]
        for field in ("client", "server"):
            if not isinstance(scenario.get(field), str) or not scenario[field].strip():
                raise InteropError(f"{scenario_id}: {field} must be a non-empty string")
        for field in ("capabilities", "negativeCases", "requiredAssertions", "requiredEvidence"):
            _unique_non_empty_strings(scenario_id, field, scenario.get(field))
        missing_capabilities = REQUIRED_CAPABILITIES[scenario_id].difference(scenario["capabilities"])
        if missing_capabilities:
            raise InteropError(
                f"{scenario_id}: missing required capabilities: {', '.join(sorted(missing_capabilities))}"
            )
        missing_negative_cases = REQUIRED_NEGATIVE_CASES[scenario_id].difference(scenario["negativeCases"])
        if missing_negative_cases:
            raise InteropError(
                f"{scenario_id}: missing required negative cases: {', '.join(sorted(missing_negative_cases))}"
            )

    i05 = scenarios[-1]
    required_ha_capabilities = {"java-master-rust-slave", "rust-master-java-slave"}
    if not required_ha_capabilities.issubset(i05["capabilities"]):
        raise InteropError("I05 must cover both DefaultHA directions")
    forbidden = " ".join(i05["capabilities"] + [i05["client"], i05["server"]]).lower()
    if any(value in forbidden for value in ("controller", "autoswitch", "dledger")):
        raise InteropError("I05 supports DefaultHA only")


def load_candidate(path: Path) -> dict[str, Any]:
    try:
        candidate = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InteropError(f"unable to read candidate manifest: {error}") from error
    if candidate.get("schema_version") != 1:
        raise InteropError("candidate manifest schema_version must be 1")
    for field in ("candidate_id", "version", "run_id", "candidate_root"):
        if not isinstance(candidate.get(field), str) or not candidate[field].strip():
            raise InteropError(f"candidate manifest {field} must be a non-empty string")
    if not isinstance(candidate.get("attempt"), int) or candidate["attempt"] < 1:
        raise InteropError("candidate manifest attempt must be a positive integer")
    candidate_root = Path(candidate["candidate_root"]).resolve()
    if candidate_root != path.resolve().parent:
        raise InteropError("candidate_root must be the directory containing CANDIDATE_RUN.json")
    return candidate


def select_scenarios(
    matrix: dict[str, Any], scenario_id: str | None, run_all: bool
) -> list[dict[str, Any]]:
    if run_all == (scenario_id is not None):
        raise InteropError("select exactly one --scenario or --all")
    if run_all:
        return list(matrix["scenarios"])
    selected = [scenario for scenario in matrix["scenarios"] if scenario["id"] == scenario_id]
    if not selected:
        raise InteropError(f"unknown interop scenario: {scenario_id}")
    return selected


def _driver_command(
    driver: Path,
    contract: Path,
    candidate_manifest: Path,
    result_path: Path,
    work_dir: Path,
) -> list[str]:
    prefix = [sys.executable, str(driver)] if driver.suffix.lower() == ".py" else [str(driver)]
    return prefix + [
        "--contract",
        str(contract),
        "--candidate-manifest",
        str(candidate_manifest),
        "--result",
        str(result_path),
        "--work-dir",
        str(work_dir),
    ]


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


def _run_child(command: list[str], work_dir: Path, timeout_seconds: int) -> tuple[int, str, str]:
    options: dict[str, Any] = {
        "cwd": work_dir,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
    }
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    process = subprocess.Popen(command, **options)
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        _terminate_process_tree(process)
        stdout, stderr = process.communicate()
        error.stdout = stdout
        error.stderr = stderr
        raise
    return process.returncode, stdout, stderr


def validate_result(
    scenario: dict[str, Any],
    result: dict[str, Any],
    candidate: dict[str, Any],
    scenario_root: Path,
) -> None:
    scenario_id = scenario["id"]
    expected_identity = {
        "schemaVersion": 1,
        "resultId": scenario_id,
        "candidateId": candidate["candidate_id"],
        "version": candidate["version"],
        "runId": candidate["run_id"],
        "attempt": candidate["attempt"],
        "javaVersion": JAVA_VERSION,
        "remotePublication": "not-executed",
    }
    for field, expected in expected_identity.items():
        if result.get(field) != expected:
            raise InteropError(f"{scenario_id}: result {field} does not match the candidate contract")
    if result.get("status") != "passed":
        raise InteropError(f"{scenario_id}: status must be passed; skipped and partial results fail")

    assertions = result.get("assertions")
    if not isinstance(assertions, dict) or set(assertions) != set(scenario["requiredAssertions"]):
        raise InteropError(f"{scenario_id}: assertion set does not match the matrix")
    if any(value is not True for value in assertions.values()):
        raise InteropError(f"{scenario_id}: every assertion must pass")

    evidence = result.get("evidence")
    if not isinstance(evidence, dict) or set(evidence) != set(scenario["requiredEvidence"]):
        raise InteropError(f"{scenario_id}: evidence set does not match the matrix")
    root = scenario_root.resolve()
    for name, value in evidence.items():
        if not isinstance(value, str) or not value.strip():
            raise InteropError(f"{scenario_id}: {name} evidence path must be a non-empty string")
        path = (root / value).resolve()
        if not path.is_relative_to(root):
            raise InteropError(f"{scenario_id}: {name} evidence escapes the scenario directory")
        if not path.is_file():
            raise InteropError(f"{scenario_id}: missing {name} evidence: {value}")


def run_scenario(
    scenario: dict[str, Any],
    candidate: dict[str, Any],
    candidate_manifest: Path,
    driver: Path,
    output_root: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    scenario_id = scenario["id"]
    scenario_root = output_root / "scenarios" / scenario_id
    if scenario_root.exists():
        raise InteropError(f"{scenario_id}: scenario output already exists")
    work_dir = scenario_root / "work"
    work_dir.mkdir(parents=True)
    contract = scenario_root / "contract.json"
    result_path = scenario_root / "result.json"
    atomic_write_json(contract, scenario)
    try:
        return_code, stdout, stderr = _run_child(
            _driver_command(driver, contract, candidate_manifest.resolve(), result_path, work_dir),
            work_dir,
            timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        (scenario_root / "stdout.log").write_text(error.stdout or "", encoding="utf-8")
        (scenario_root / "stderr.log").write_text(error.stderr or "", encoding="utf-8")
        raise InteropError(f"{scenario_id}: timed out after {timeout_seconds} seconds") from error
    (scenario_root / "stdout.log").write_text(stdout, encoding="utf-8")
    (scenario_root / "stderr.log").write_text(stderr, encoding="utf-8")
    if return_code != 0:
        raise InteropError(f"{scenario_id}: case driver exited with {return_code}")
    if not result_path.is_file():
        raise InteropError(f"{scenario_id}: missing result")
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise InteropError(f"{scenario_id}: result is not valid JSON: {error}") from error
    validate_result(scenario, result, candidate, scenario_root)
    return result


def execute(args: argparse.Namespace) -> int:
    matrix = load_matrix(args.matrix.resolve())
    candidate_manifest = args.candidate_manifest.resolve()
    candidate = load_candidate(candidate_manifest)
    scenarios = select_scenarios(matrix, args.scenario, args.run_all)
    driver_value = args.case_driver or os.environ.get("ROCKETMQ_V1_INTEROP_CASE_DRIVER")
    if not driver_value:
        raise InteropError("--case-driver or ROCKETMQ_V1_INTEROP_CASE_DRIVER is required")
    driver = Path(driver_value).resolve()
    if not driver.is_file():
        raise InteropError(f"interop case driver does not exist: {driver}")
    if not 1 <= args.timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise InteropError(f"--timeout-seconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    if not 1 <= args.max_workers <= len(RESULT_IDS):
        raise InteropError(f"--max-workers must be between 1 and {len(RESULT_IDS)}")

    output_root = (
        args.output.resolve()
        if args.output is not None
        else Path(candidate["candidate_root"]).resolve() / "evidence" / "v1-interop"
    )
    if (output_root / "run.json").exists():
        raise InteropError("interop run output already contains run.json")
    output_root.mkdir(parents=True, exist_ok=True)
    run = {
        "schemaVersion": 1,
        "candidateId": candidate["candidate_id"],
        "version": candidate["version"],
        "runId": candidate["run_id"],
        "attempt": candidate["attempt"],
        "javaVersion": JAVA_VERSION,
        "scenarioIds": [scenario["id"] for scenario in scenarios],
        "status": "running",
        "startedAt": utc_now(),
        "remotePublication": "not-executed",
    }
    atomic_write_json(output_root / "run.json", run)

    try:
        results_by_id: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=min(args.max_workers, len(scenarios))) as executor:
            futures = {
                executor.submit(
                    run_scenario,
                    scenario,
                    candidate,
                    candidate_manifest,
                    driver,
                    output_root,
                    args.timeout_seconds,
                ): scenario["id"]
                for scenario in scenarios
            }
            for future in as_completed(futures):
                results_by_id[futures[future]] = future.result()
        results = [results_by_id[scenario["id"]] for scenario in scenarios]
    except (InteropError, OSError, subprocess.SubprocessError) as error:
        atomic_write_json(
            output_root / "run.json",
            {**run, "status": "failed", "completedAt": utc_now(), "error": str(error)},
        )
        print(f"v1 interoperability failed: {error}", file=sys.stderr)
        return 1

    atomic_write_json(
        output_root / "run.json",
        {**run, "status": "passed", "completedAt": utc_now(), "results": results},
    )
    print(f"passed {len(results)} v1 interoperability scenarios")
    return 0


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[2]
    command = argparse.ArgumentParser(description="Run bounded Java 5.5/Rust 1.0 interoperability scenarios")
    command.add_argument("--candidate-manifest", type=Path, required=True)
    command.add_argument("--matrix", type=Path, default=Path(__file__).with_name("v1-interop-matrix.json"))
    selection = command.add_mutually_exclusive_group(required=True)
    selection.add_argument("--scenario", choices=RESULT_IDS)
    selection.add_argument("--all", dest="run_all", action="store_true")
    command.add_argument("--case-driver")
    command.add_argument("--output", type=Path)
    command.add_argument("--timeout-seconds", type=int, default=MAX_TIMEOUT_SECONDS)
    command.add_argument("--max-workers", type=int, default=2)
    command.set_defaults(repository_root=root)
    return command


def main(argv: list[str] | None = None) -> int:
    try:
        return execute(parser().parse_args(argv))
    except (InteropError, OSError, json.JSONDecodeError) as error:
        print(f"v1 interoperability failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
