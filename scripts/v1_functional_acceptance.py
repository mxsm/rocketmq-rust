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

"""Execute short, candidate-scoped RocketMQ Rust functional acceptance routes."""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for module_root in (ROOT / "distribution", ROOT / "scripts"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from release_archive_common import resolve_candidate_path
from release_state import atomic_write_json, read_json, resolve_existing_file, validate_candidate
import verify_release_archive

MAX_TIMEOUT_SECONDS = 600
REQUIRED_RESULT_IDS = (
    *(f"P{number:02d}" for number in range(1, 13)),
    *(f"I{number:02d}" for number in range(1, 6)),
    "M01",
    "L01",
    "A01",
    "U01-LF",
    "U01-MP",
    "U01-RDB",
    "U01-CMP",
    "U01-POP",
    "U01-TMR",
    "U01-TRD",
    "U01-CTL",
    "U01-UPG",
    "U01",
    "S01",
    "S02",
    "S03",
)
FORBIDDEN_EXECUTABLES = {"cargo", "cargo.exe", "rustc", "rustc.exe"}
PUBLISHING_ENVIRONMENT = {
    "CARGO_REGISTRY_TOKEN",
    "CRATES_IO_TOKEN",
    "DOCKER_PASSWORD",
    "GHCR_TOKEN",
    "HELM_REGISTRY_PASSWORD",
}
EVIDENCE_FIELDS = set(
    "schema_version candidate_id version run_id attempt phase gate_stage result_id result_kind status "
    "command exit_code matched_test_count executed_test_count passed_test_count failed_test_count "
    "ignored_test_count capability_ids result_path".split()
)


class AcceptanceError(RuntimeError):
    """Raised when functional evidence cannot be attributed to one candidate."""

@dataclass(frozen=True)
class Candidate:
    manifest: Path
    value: dict[str, Any]
    root: Path
    artifact_index: dict[str, Any]


@dataclass(frozen=True)
class Matrix:
    routes: tuple[dict[str, Any], ...]
    timeout_seconds: int
    max_workers: int
    java_workers: int

    def route(self, result_id: str) -> dict[str, Any]:
        for route in self.routes:
            if route["id"] == result_id:
                return route
        raise AcceptanceError(f"unknown scenario: {result_id}")


@dataclass(frozen=True)
class ResultIdentity:
    candidate_id: str
    version: str
    run_id: str
    attempt: int
    result_id: str


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _safe_id(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise AcceptanceError(f"unsafe {label}: {value}")
    return value


def load_candidate(path: Path) -> Candidate:
    manifest = resolve_existing_file(path, "candidate_manifest")
    value = read_json(manifest)
    validate_candidate(value)
    root = Path(value["candidate_root"]).resolve()
    if manifest.parent.resolve() != root:
        raise AcceptanceError("candidate manifest must live at the candidate root")
    if value["sealed"]:
        raise AcceptanceError("sealed candidates cannot produce new functional evidence")
    relative_index = value.get("artifact_index")
    if not isinstance(relative_index, str):
        raise AcceptanceError("candidate has no registered artifact index")
    index_path = resolve_candidate_path(root, relative_index, "candidate artifact index")
    index = read_json(resolve_existing_file(index_path, "candidate artifact index"))
    if _identity(index) != _identity(value) or index.get("remote_publication") != "not-executed":
        raise AcceptanceError("candidate artifact index identity or publication state mismatch")
    return Candidate(manifest, value, root, index)


def load_matrix(path: Path) -> Matrix:
    value = read_json(resolve_existing_file(path, "functional acceptance matrix"))
    functional = value.get("functional_acceptance")
    if not isinstance(functional, dict) or functional.get("schema_version") != 1:
        raise AcceptanceError("functional acceptance matrix schema is missing or unsupported")
    routes = functional.get("routes")
    if not isinstance(routes, list) or any(not isinstance(route, dict) for route in routes):
        raise AcceptanceError("functional acceptance routes must be a list")
    identifiers = tuple(route.get("id") for route in routes)
    if identifiers != REQUIRED_RESULT_IDS or len(set(identifiers)) != len(identifiers):
        raise AcceptanceError("functional acceptance result denominator is incomplete or reordered")
    known = set(identifiers)
    for route in routes:
        if route.get("kind") not in {"profile", "scenario", "aggregate", "install"}:
            raise AcceptanceError(f"functional route has invalid kind: {route.get('id')}")
        dependencies = route.get("depends_on", [])
        if not isinstance(dependencies, list) or not set(dependencies).issubset(known):
            raise AcceptanceError(f"functional route dependencies are invalid: {route.get('id')}")
        if route["id"] in dependencies:
            raise AcceptanceError(f"functional route depends on itself: {route['id']}")
        if route["kind"] == "aggregate" and not dependencies:
            raise AcceptanceError("aggregate routes require a closed dependency denominator")
        if route["kind"] != "aggregate" and not isinstance(route.get("driver_env"), str):
            raise AcceptanceError(f"functional route has no registered driver contract: {route['id']}")
    timeout_seconds = functional.get("scenario_timeout_seconds")
    max_workers = functional.get("max_workers")
    java_workers = functional.get("java_workers")
    if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise AcceptanceError("scenario timeout must be between 1 and 600 seconds")
    if not isinstance(max_workers, int) or not 1 <= max_workers <= 4:
        raise AcceptanceError("functional worker count must be between 1 and 4")
    if not isinstance(java_workers, int) or not 1 <= java_workers <= 2:
        raise AcceptanceError("Java interop worker count must be between 1 and 2")
    return Matrix(tuple(routes), timeout_seconds, max_workers, java_workers)


def select_routes(
    matrix: Matrix,
    *,
    profile: str | None = None,
    scenario: str | None = None,
    all_scenarios: bool = False,
) -> tuple[str, ...]:
    if sum((profile is not None, scenario is not None, all_scenarios)) != 1:
        raise AcceptanceError("select exactly one Profile, Scenario, or AllScenarios mode")
    if all_scenarios:
        return REQUIRED_RESULT_IDS
    selected = profile or scenario
    assert selected is not None
    try:
        route = matrix.route(selected)
    except AcceptanceError as error:
        if profile is not None:
            raise AcceptanceError(f"unknown profile: {profile}") from error
        raise
    if profile is not None and route["kind"] != "profile":
        raise AcceptanceError(f"unknown profile: {profile}")
    return (selected,)


def archive_record(candidate: dict[str, Any], index: dict[str, Any], target: str) -> dict[str, Any]:
    if _identity(candidate) != _identity(index) or index.get("remote_publication") != "not-executed":
        raise AcceptanceError("candidate artifact index identity or publication state mismatch")
    matches = [
        record
        for record in index.get("artifacts", [])
        if isinstance(record, dict) and record.get("kind") == "release-archive" and record.get("target") == target
    ]
    if len(matches) != 1:
        raise AcceptanceError(f"expected exactly one registered release archive for {target}, found {len(matches)}")
    return matches[0]


def host_target() -> str:
    machine = platform.machine().lower()
    if machine not in {"amd64", "x86_64"}:
        raise AcceptanceError(f"unsupported functional acceptance host architecture: {machine}")
    if sys.platform == "win32":
        return "x86_64-pc-windows-msvc"
    if sys.platform == "darwin":
        return "x86_64-apple-darwin"
    if sys.platform.startswith("linux"):
        return "x86_64-unknown-linux-gnu"
    raise AcceptanceError(f"unsupported functional acceptance host: {sys.platform}")


def _binary_paths(package_root: Path, archive_manifest: dict[str, Any]) -> dict[str, Path]:
    layout = verify_release_archive.load_layout()
    suffix = verify_release_archive.target_layout(layout, archive_manifest["target"])["executable_suffix"]
    return {
        binary["id"]: package_root / "bin" / f"{binary.get('archive_binary', binary['binary'])}{suffix}"
        for binary in layout["binaries"]
    }


def _require_features(route: dict[str, Any], archive_manifest: dict[str, Any]) -> None:
    records = {record.get("component"): record for record in archive_manifest.get("binaries", [])}
    for component in route.get("required_binaries", []):
        if component not in records:
            raise AcceptanceError(f"{route['id']} requires missing archive binary {component}")
    for component, features in route.get("required_features", {}).items():
        actual = set(records.get(component, {}).get("effective_features", []))
        missing = sorted(set(features) - actual)
        if missing:
            raise AcceptanceError(f"{route['id']} archive feature closure is missing {component}:{','.join(missing)}")


def expand_driver_command(
    route: dict[str, Any],
    package_root: Path,
    binaries: dict[str, Path],
    work_root: Path,
    result_path: Path,
    ports: list[int],
    *,
    driver: str | None = None,
    candidate_manifest: Path | None = None,
) -> list[str]:
    command = route.get("command")
    if command is None:
        if not driver:
            raise AcceptanceError(f"scenario driver is not configured: {route['id']} ({route.get('driver_env')})")
        driver_path = Path(driver).resolve()
        if not driver_path.is_file():
            raise AcceptanceError(f"scenario driver does not exist: {driver_path}")
        command = ["{python}", str(driver_path)] if driver_path.suffix.lower() == ".py" else [str(driver_path)]
        command += ["--contract", "{contract}", "--result", "{result}"]
    if not isinstance(command, list) or not command or any(not isinstance(value, str) for value in command):
        raise AcceptanceError(f"scenario command is invalid: {route['id']}")
    if Path(command[0]).name.lower() in FORBIDDEN_EXECUTABLES:
        raise AcceptanceError(f"source build tool is forbidden in functional acceptance: {command[0]}")
    replacements = {
        "python": sys.executable,
        "candidate_manifest": str(candidate_manifest or ""),
        "archive_root": str(package_root),
        "config_root": str(work_root / "config"),
        "data_root": str(work_root / "data"),
        "log_root": str(work_root / "logs"),
        "work_root": str(work_root),
        "result": str(result_path),
        "contract": str(work_root / "contract.json"),
        "result_id": route["id"],
        "ports": ",".join(str(port) for port in ports),
        **{name: str(path) for name, path in binaries.items()},
    }
    expanded = []
    for value in command:
        try:
            expanded.append(value.format_map(replacements))
        except KeyError as error:
            raise AcceptanceError(f"scenario command references unknown archive input: {error.args[0]}") from error
    executable = Path(expanded[0])
    if executable.name.lower() in FORBIDDEN_EXECUTABLES:
        raise AcceptanceError(f"source build tool is forbidden in functional acceptance: {expanded[0]}")
    return expanded


def reserve_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            lease = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lease.bind(("127.0.0.1", 0))
            sockets.append(lease)
        return [lease.getsockname()[1] for lease in sockets]
    finally:
        for lease in sockets:
            lease.close()


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
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
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_bounded_process(
    command: list[str],
    work_root: Path,
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
    *,
    extra_environment: dict[str, str] | None = None,
) -> int:
    environment = os.environ.copy()
    for name in list(environment):
        if name in PUBLISHING_ENVIRONMENT or (name.startswith("CARGO_REGISTRIES_") and name.endswith("_TOKEN")):
            environment.pop(name, None)
    environment.update(extra_environment or {})
    options: dict[str, Any] = {
        "cwd": work_root,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
        "env": environment,
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
        stdout_path.write_text(stdout or "", encoding="utf-8")
        stderr_path.write_text(stderr or "", encoding="utf-8")
        raise subprocess.TimeoutExpired(command, timeout_seconds, stdout, stderr) from error
    finally:
        if process.poll() is None:
            _terminate_process_tree(process)
    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    return process.returncode


def validate_driver_result(value: dict[str, Any], expected: ResultIdentity) -> None:
    identity = ResultIdentity(
        value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt"), value.get("result_id")
    )
    if identity != expected:
        raise AcceptanceError("functional result identity does not match the selected candidate")
    if value.get("status") != "passed" or value.get("remote_publication") != "not-executed":
        raise AcceptanceError("functional result did not pass locally")
    counts = tuple(value.get(name) for name in (
        "matched_test_count", "executed_test_count", "passed_test_count", "failed_test_count", "ignored_test_count"
    ))
    if any(not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts):
        raise AcceptanceError("functional result test counts are invalid")
    matched, executed, passed, failed, ignored = counts
    if matched == 0 or executed == 0:
        raise AcceptanceError("functional result executed zero tests")
    if failed != 0 or passed != executed or matched != executed + ignored:
        raise AcceptanceError("functional result test counts are inconsistent or failed")
    if ignored:
        raise AcceptanceError("functional result contains ignored tests")
    if not isinstance(value.get("readiness_check_count"), int) or value["readiness_check_count"] < 1:
        raise AcceptanceError("functional result has no readiness checks")
    if value.get("teardown_completed") is not True:
        raise AcceptanceError("functional result did not complete teardown")


def validate_evidence_result(value: dict[str, Any], expected: ResultIdentity) -> None:
    if set(value) != EVIDENCE_FIELDS:
        raise AcceptanceError("functional evidence fields are not closed")
    identity = ResultIdentity(
        value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt"), value.get("result_id")
    )
    if identity != expected:
        raise AcceptanceError("functional evidence identity does not match the selected candidate")
    if value.get("phase") != 6 or value.get("gate_stage") != "full-matrix":
        raise AcceptanceError("functional evidence has the wrong gate identity")
    if value.get("result_kind") != "test" or value.get("status") != "passed" or value.get("exit_code") != 0:
        raise AcceptanceError("functional evidence did not pass")
    if value.get("schema_version") != 1 or value.get("result_path") != f"{expected.result_id}.json":
        raise AcceptanceError("functional evidence schema or result path is invalid")
    capability_ids = value.get("capability_ids")
    if not isinstance(capability_ids, list) or len(capability_ids) != len(set(capability_ids)):
        raise AcceptanceError("functional evidence capability IDs are invalid")
    matched = value.get("matched_test_count")
    executed = value.get("executed_test_count")
    passed = value.get("passed_test_count")
    failed = value.get("failed_test_count")
    ignored = value.get("ignored_test_count")
    if not all(isinstance(count, int) and not isinstance(count, bool) for count in (matched, executed, passed, failed, ignored)):
        raise AcceptanceError("functional evidence test counts are invalid")
    if matched == 0 or executed == 0 or failed != 0 or ignored != 0 or passed != executed or matched != executed:
        raise AcceptanceError("functional evidence test counts are incomplete")


def _evidence_record(
    identity: ResultIdentity,
    *,
    status: str,
    command: list[str],
    exit_code: int,
    matched: int,
    executed: int,
    passed: int,
    failed: int,
    ignored: int,
    capability_ids: list[str],
    result_path: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "candidate_id": identity.candidate_id,
        "version": identity.version,
        "run_id": identity.run_id,
        "attempt": identity.attempt,
        "phase": 6,
        "gate_stage": "full-matrix",
        "result_id": identity.result_id,
        "result_kind": "test",
        "status": status,
        "command": command,
        "exit_code": exit_code,
        "matched_test_count": matched,
        "executed_test_count": executed,
        "passed_test_count": passed,
        "failed_test_count": failed,
        "ignored_test_count": ignored,
        "capability_ids": capability_ids,
        "result_path": result_path,
    }


def _port_is_released(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _result_identity(candidate: Candidate, result_id: str) -> ResultIdentity:
    value = candidate.value
    return ResultIdentity(value["candidate_id"], value["version"], value["run_id"], value["attempt"], result_id)


def _write_failure(
    output: Path,
    identity: ResultIdentity,
    error: Exception,
    command: list[str] | None = None,
    capability_ids: list[str] | None = None,
) -> None:
    exit_code = 124 if isinstance(error, subprocess.TimeoutExpired) else 1
    atomic_write_json(
        output,
        _evidence_record(
            identity,
            status="failed",
            command=command or [],
            exit_code=exit_code,
            matched=1,
            executed=1,
            passed=0,
            failed=1,
            ignored=0,
            capability_ids=capability_ids or [],
            result_path=output.name,
        ),
    )
    atomic_write_json(
        output.with_suffix(".diagnostics.json"),
        {
            "schema_version": 1,
            **identity.__dict__,
            "error": str(error),
            "exit_code": exit_code,
            "remote_publication": "not-executed",
        },
    )


def _aggregate(candidate: Candidate, route: dict[str, Any], result_root: Path) -> Path:
    identity = _result_identity(candidate, route["id"])
    dependencies = route["depends_on"]
    for result_id in dependencies:
        result = read_json(resolve_existing_file(result_root / f"{result_id}.json", f"{result_id} result"))
        validate_evidence_result(result, _result_identity(candidate, result_id))
    output = result_root / f"{route['id']}.json"
    atomic_write_json(
        output,
        _evidence_record(
            identity,
            status="passed",
            command=["aggregate", *dependencies],
            exit_code=0,
            matched=len(dependencies),
            executed=len(dependencies),
            passed=len(dependencies),
            failed=0,
            ignored=0,
            capability_ids=[],
            result_path=output.name,
        ),
    )
    return output


def execute_one(candidate: Candidate, matrix: Matrix, result_id: str, target: str) -> Path:
    route = matrix.route(result_id)
    result_root = candidate.root / "results"
    result_root.mkdir(parents=True, exist_ok=True)
    output = result_root / f"{result_id}.json"
    if output.exists():
        raise AcceptanceError(f"functional result already exists: {result_id}")
    if route["kind"] == "aggregate":
        try:
            return _aggregate(candidate, route, result_root)
        except (AcceptanceError, OSError) as error:
            _write_failure(output, _result_identity(candidate, result_id), error)
            raise
    identity = _result_identity(candidate, result_id)
    command: list[str] | None = None
    try:
        route_target = route.get("target")
        if route_target is not None and route_target != target:
            raise AcceptanceError(f"{result_id} requires target {route_target}, not {target}")
        record = archive_record(candidate.value, candidate.artifact_index, target)
        archive = resolve_candidate_path(candidate.root, record["path"], "registered release archive")
        run_root = candidate.root / "work" / "functional" / _safe_id(result_id, "result ID")
        if run_root.exists():
            raise AcceptanceError(f"functional work root already exists: {result_id}")
        run_root.mkdir(parents=True)
        _manifest_path, archive_manifest, _probe_results = verify_release_archive.inspect_archive(
            candidate.value, candidate.root, archive, smoke=True
        )
        _require_features(route, archive_manifest)
        archive_root = run_root / "archive"
        archive_root.mkdir()
        verify_release_archive._extract(archive, archive_root)
        package_roots = [path for path in archive_root.iterdir() if path.is_dir()]
        if len(package_roots) != 1:
            raise AcceptanceError("release archive must contain exactly one package root")
        package_root = package_roots[0]
        for name in ("config", "data", "logs"):
            (run_root / name).mkdir()
        ports = reserve_ports(route.get("port_count", 4))
        binaries = _binary_paths(package_root, archive_manifest)
        contract = {
            "schema_version": 1,
            **identity.__dict__,
            "archive_artifact_id": archive_manifest["artifact_id"],
            "package_root": str(package_root),
            "binary_paths": {name: str(path) for name, path in binaries.items()},
            "ports": ports,
            "timeout_seconds": matrix.timeout_seconds,
            "roots": {name: str(run_root / name) for name in ("config", "data", "logs")},
            "required_capability_ids": route.get("capability_ids", []),
            "remote_publication": "not-executed",
        }
        atomic_write_json(run_root / "contract.json", contract)
        raw_result = run_root / "driver-result.json"
        command = expand_driver_command(
            route,
            package_root,
            binaries,
            run_root,
            raw_result,
            ports,
            driver=os.environ.get(route["driver_env"]),
            candidate_manifest=candidate.manifest,
        )
        return_code = run_bounded_process(
            command,
            run_root,
            run_root / "stdout.log",
            run_root / "stderr.log",
            matrix.timeout_seconds,
            extra_environment={
                "ROCKETMQ_FUNCTIONAL_ARCHIVE_ONLY": "true",
                "ROCKETMQ_FUNCTIONAL_CONTRACT": str(run_root / "contract.json"),
            },
        )
        if return_code != 0:
            raise AcceptanceError(f"functional scenario exited with {return_code}")
        raw = read_json(resolve_existing_file(raw_result, "functional driver result"))
        validate_driver_result(raw, identity)
        if not all(_port_is_released(port) for port in ports):
            raise AcceptanceError("functional teardown did not release every leased port")
        atomic_write_json(
            output,
            _evidence_record(
                identity,
                status="passed",
                command=command,
                exit_code=0,
                matched=raw["matched_test_count"],
                executed=raw["executed_test_count"],
                passed=raw["passed_test_count"],
                failed=raw["failed_test_count"],
                ignored=raw["ignored_test_count"],
                capability_ids=route.get("capability_ids", []),
                result_path=output.name,
            ),
        )
        return output
    except (AcceptanceError, OSError, subprocess.SubprocessError) as error:
        _write_failure(output, identity, error, command, route.get("capability_ids", []))
        raise


def _context(candidate: Candidate, worker_id: str) -> Path:
    context_root = candidate.root / "contexts" / "functional"
    context_root.mkdir(parents=True, exist_ok=True)
    path = context_root / f"{worker_id}.json"
    if path.exists():
        return path
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/capture_candidate_execution_context.py"),
            "--candidate-manifest",
            str(candidate.manifest),
            "--worker-id",
            worker_id,
            "--output-root",
            str(context_root),
        ],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode != 0:
        raise AcceptanceError(f"unable to capture execution context for {worker_id}")
    return path


def _wrapped_one(candidate: Candidate, matrix_path: Path, result_id: str, target: str) -> int:
    worker_id = f"functional-{result_id.lower()}"
    context = _context(candidate, worker_id)
    event_root = candidate.root / "events" / "functional" / worker_id
    event_root.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(ROOT / "scripts/release_candidate_command.py"),
        "run",
        "--candidate-manifest",
        str(candidate.manifest),
        "--route-id",
        result_id,
        "--worker-id",
        worker_id,
        "--context",
        str(context),
        "--event-root",
        str(event_root),
        "--",
        sys.executable,
        str(Path(__file__).resolve()),
        "--candidate-manifest",
        str(candidate.manifest),
        "--matrix",
        str(matrix_path),
        "--target",
        target,
        "--execute-one",
        result_id,
    ]
    return subprocess.run(command, cwd=ROOT, check=False).returncode


def run_selected(candidate: Candidate, matrix: Matrix, matrix_path: Path, selected: tuple[str, ...], target: str) -> int:
    selected_set = set(selected)
    completed: set[str] = set()
    result_root = candidate.root / "results"
    for result_id in selected:
        existing = result_root / f"{result_id}.json"
        if existing.is_file():
            validate_evidence_result(read_json(existing), _result_identity(candidate, result_id))
            completed.add(result_id)
    failed = False
    pending = [result_id for result_id in selected if result_id not in completed]
    while pending and not failed:
        ready = [
            result_id
            for result_id in pending
            if matrix.route(result_id).get("target", target) == target
            and (
                set(matrix.route(result_id).get("depends_on", [])) <= completed
                or not (set(matrix.route(result_id).get("depends_on", [])) & selected_set)
            )
        ]
        if not ready:
            raise AcceptanceError(
                "functional matrix is cyclic, incomplete, or still requires collected cross-target results"
            )
        batch: list[str] = []
        java_count = 0
        for result_id in ready:
            is_java = matrix.route(result_id).get("lane") == "java"
            if is_java and java_count >= matrix.java_workers:
                continue
            batch.append(result_id)
            java_count += int(is_java)
            if len(batch) == matrix.max_workers:
                break
        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {
                executor.submit(_wrapped_one, candidate, matrix_path, result_id, target): result_id
                for result_id in batch
            }
            done, _ = wait(futures, return_when=FIRST_COMPLETED) if len(batch) > 1 else wait(futures)
            for future in done:
                result_id = futures[future]
                code = future.result()
                pending.remove(result_id)
                if code == 0:
                    completed.add(result_id)
                else:
                    failed = True
            for future, result_id in futures.items():
                if future not in done:
                    code = future.result()
                    pending.remove(result_id)
                    if code == 0:
                        completed.add(result_id)
                    else:
                        failed = True
    return 1 if failed else 0


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--candidate-manifest", type=Path, required=True)
    command.add_argument("--matrix", type=Path, default=ROOT / "scripts/v1-functional-test-matrix.json")
    command.add_argument("--target", default=host_target())
    selection = command.add_mutually_exclusive_group(required=True)
    selection.add_argument("--profile")
    selection.add_argument("--scenario")
    selection.add_argument("--all-scenarios", action="store_true")
    selection.add_argument("--execute-one", help=argparse.SUPPRESS)
    return command


def main(argv: list[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        candidate = load_candidate(args.candidate_manifest)
        matrix_path = resolve_existing_file(args.matrix, "functional acceptance matrix")
        matrix = load_matrix(matrix_path)
        if args.execute_one:
            execute_one(candidate, matrix, args.execute_one, args.target)
            return 0
        selected = select_routes(
            matrix, profile=args.profile, scenario=args.scenario, all_scenarios=args.all_scenarios
        )
        return run_selected(candidate, matrix, matrix_path, selected, args.target)
    except (AcceptanceError, OSError, json.JSONDecodeError) as error:
        print(f"V1_FUNCTIONAL_ACCEPTANCE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
