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
import platform
import signal
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any
import zipfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

import verify_release_archive as archive_verifier


TARGET_RESULTS = {
    "x86_64-unknown-linux-gnu": "S01",
    "x86_64-pc-windows-msvc": "S02",
    "x86_64-apple-darwin": "S03",
}
REQUIRED_PROFILES = ("single", "controller-3")
MAX_TIMEOUT_SECONDS = 600
REQUIRED_COMPONENTS = {"namesrv", "broker", "controller", "proxy", "admin", "store-inspect"}
REQUIRED_ASSERTIONS = (
    "artifact-identity",
    "version-metadata",
    "namesrv-start",
    "broker-register",
    "proxy-start",
    "send",
    "pull",
    "query",
    "admin-topic-crud",
    "admin-group-crud",
    "broker-restart",
    "message-persistence",
    "offset-persistence",
    "controller-3-start",
    "controller-role-change",
    "process-cleanup",
    "excluded-surfaces-absent",
)
REQUIRED_EVIDENCE = ("driver-log", "operations", "process-cleanup")
EXCLUDED_SURFACES = ("MCP", "Dashboard", "SRE", "OpenMessaging", "BrokerContainer", "DLedger")
NEGATIVE_MARKERS = (
    "excluded",
    "unsupported",
    "not supported",
    "not included",
    "not part",
    "does not",
    "false",
)


class InstallSmokeError(ValueError):
    pass


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def result_id_for_target(target: str) -> str:
    try:
        return TARGET_RESULTS[target]
    except KeyError as error:
        raise InstallSmokeError(f"unsupported release target: {target}") from error


def host_target() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if machine not in {"x86_64", "amd64"}:
        raise InstallSmokeError(f"unsupported host architecture: {machine}")
    if system == "linux":
        return "x86_64-unknown-linux-gnu"
    if system == "windows":
        return "x86_64-pc-windows-msvc"
    if system == "darwin":
        return "x86_64-apple-darwin"
    raise InstallSmokeError(f"unsupported host operating system: {system}")


def parse_profiles(value: str) -> tuple[str, ...]:
    profiles = tuple(part.strip() for part in value.split(",") if part.strip())
    if profiles != REQUIRED_PROFILES:
        raise InstallSmokeError("profiles must contain exactly single,controller-3 in order")
    return profiles


def load_candidate(path: Path) -> tuple[dict[str, Any], Path]:
    try:
        candidate = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InstallSmokeError(f"unable to read candidate manifest: {error}") from error
    if candidate.get("schema_version") != 1:
        raise InstallSmokeError("candidate manifest schema_version must be 1")
    for field in ("candidate_id", "version", "run_id", "candidate_root"):
        if not isinstance(candidate.get(field), str) or not candidate[field].strip():
            raise InstallSmokeError(f"candidate manifest {field} must be a non-empty string")
    if not isinstance(candidate.get("attempt"), int) or candidate["attempt"] < 1:
        raise InstallSmokeError("candidate manifest attempt must be a positive integer")
    root = Path(candidate["candidate_root"]).resolve()
    if root != path.resolve().parent:
        raise InstallSmokeError("candidate_root must contain CANDIDATE_RUN.json")
    return candidate, root


def archive_for_target(candidate: dict[str, Any], root: Path, target: str) -> Path:
    extension = ".zip" if target == "x86_64-pc-windows-msvc" else ".tar.gz"
    archive = root / "archives" / f"rocketmq-rust-{candidate['version']}-{target}{extension}"
    if not archive.is_file():
        raise InstallSmokeError(f"candidate archive is missing: {archive.name}")
    return archive


def _json_excluded_surface(value: Any, path: tuple[str, ...] = ()) -> str | None:
    if isinstance(value, dict):
        for key, item in value.items():
            found = _json_excluded_surface(item, path + (str(key).lower(),))
            if found is not None:
                return found
        return None
    if isinstance(value, list):
        for item in value:
            found = _json_excluded_surface(item, path)
            if found is not None:
                return found
        return None
    if not isinstance(value, str):
        return None
    lowered_path = " ".join(path)
    for surface in EXCLUDED_SURFACES:
        if surface.lower() in value.lower() and not any(marker in lowered_path for marker in ("excluded", "unsupported")):
            return surface
    return None


def validate_installed_surface(package_root: Path) -> None:
    text_suffixes = {".md", ".txt", ".toml", ".yaml", ".yml"}
    for path in package_root.rglob("*"):
        if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
            continue
        if path.suffix.lower() == ".json":
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InstallSmokeError(f"installed JSON is invalid: {path.relative_to(package_root)}: {error}") from error
            surface = _json_excluded_surface(value)
            if surface is not None:
                raise InstallSmokeError(
                    f"excluded support surface advertised by {path.relative_to(package_root)}: {surface}"
                )
            continue
        if path.suffix.lower() not in text_suffixes:
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(lines, 1):
            lowered = line.lower()
            for surface in EXCLUDED_SURFACES:
                if surface.lower() in lowered and not any(marker in lowered for marker in NEGATIVE_MARKERS):
                    raise InstallSmokeError(
                        "excluded support surface advertised by "
                        f"{path.relative_to(package_root)}:{line_number}: {surface}"
                    )


def validate_host_smoke(path: Path, candidate: dict[str, Any], target: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InstallSmokeError(f"unable to read archive host smoke evidence: {error}") from error
    if value.get("status") != "passed" or value.get("candidate_id") != candidate["candidate_id"]:
        raise InstallSmokeError("archive host smoke does not match the candidate")
    if value.get("target") != target:
        raise InstallSmokeError("archive host smoke target does not match the selected host")
    results = value.get("results")
    if not isinstance(results, list) or {result.get("component") for result in results} != REQUIRED_COMPONENTS:
        raise InstallSmokeError("archive host smoke must contain all four services and two tools")
    if any(result.get("exit_code") != 0 for result in results):
        raise InstallSmokeError("archive host smoke contains a failed version command")
    return value


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


def _driver_command(
    driver: Path,
    contract: Path,
    candidate_manifest: Path,
    package_root: Path,
    result_path: Path,
    work_dir: Path,
) -> list[str]:
    prefix = [sys.executable, str(driver)] if driver.suffix.lower() == ".py" else [str(driver)]
    return prefix + [
        "--contract",
        str(contract),
        "--candidate-manifest",
        str(candidate_manifest),
        "--package-root",
        str(package_root),
        "--result",
        str(result_path),
        "--work-dir",
        str(work_dir),
    ]


def validate_driver_result(
    value: dict[str, Any],
    candidate: dict[str, Any],
    target: str,
    result_id: str,
    profiles: tuple[str, ...],
    run_root: Path,
) -> None:
    expected = {
        "schemaVersion": 1,
        "resultId": result_id,
        "candidateId": candidate["candidate_id"],
        "version": candidate["version"],
        "runId": candidate["run_id"],
        "attempt": candidate["attempt"],
        "target": target,
        "profiles": list(profiles),
        "remotePublication": "not-executed",
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise InstallSmokeError(f"install smoke result {field} does not match the candidate contract")
    if value.get("status") != "passed":
        raise InstallSmokeError("install smoke status must be passed; skipped and partial results fail")
    assertions = value.get("assertions")
    if not isinstance(assertions, dict) or set(assertions) != set(REQUIRED_ASSERTIONS):
        raise InstallSmokeError("install smoke assertion set does not match the contract")
    if any(result is not True for result in assertions.values()):
        raise InstallSmokeError("every assertion must pass")
    evidence = value.get("evidence")
    if not isinstance(evidence, dict) or set(evidence) != set(REQUIRED_EVIDENCE):
        raise InstallSmokeError("install smoke evidence set does not match the contract")
    resolved_root = run_root.resolve()
    for name, relative in evidence.items():
        if not isinstance(relative, str) or not relative.strip():
            raise InstallSmokeError(f"{name} evidence path must be a non-empty string")
        path = (resolved_root / relative).resolve()
        if not path.is_relative_to(resolved_root):
            raise InstallSmokeError(f"{name} evidence escapes the install smoke directory")
        if not path.is_file():
            raise InstallSmokeError(f"missing {name} evidence: {relative}")


def execute(args: argparse.Namespace) -> int:
    candidate_manifest = args.candidate_manifest.resolve()
    candidate, candidate_root = load_candidate(candidate_manifest)
    target = args.archive_id
    expected_result_id = result_id_for_target(target)
    if host_target() != target:
        raise InstallSmokeError(f"archive target {target} does not match this host")
    if args.result_id != expected_result_id:
        raise InstallSmokeError(f"target {target} requires result ID {expected_result_id}")
    profiles = parse_profiles(args.profiles)
    if not 1 <= args.timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise InstallSmokeError(f"--timeout-seconds must be between 1 and {MAX_TIMEOUT_SECONDS}")
    driver_value = args.case_driver or os.environ.get("ROCKETMQ_RELEASE_INSTALL_SMOKE_DRIVER")
    if not driver_value:
        raise InstallSmokeError("--case-driver or ROCKETMQ_RELEASE_INSTALL_SMOKE_DRIVER is required")
    driver = Path(driver_value).resolve()
    if not driver.is_file():
        raise InstallSmokeError(f"install smoke case driver does not exist: {driver}")
    archive = archive_for_target(candidate, candidate_root, target)
    output = (
        args.output.resolve()
        if args.output is not None
        else candidate_root / "evidence" / "install-smoke" / f"{args.result_id}.json"
    )
    run_root = output.with_name(f"{output.stem}-work")
    if output.exists() or run_root.exists():
        raise InstallSmokeError("install smoke output already exists")
    run_root.mkdir(parents=True)
    running = {
        "schemaVersion": 1,
        "resultId": args.result_id,
        "candidateId": candidate["candidate_id"],
        "version": candidate["version"],
        "runId": candidate["run_id"],
        "attempt": candidate["attempt"],
        "target": target,
        "profiles": list(profiles),
        "status": "running",
        "archive": archive.relative_to(candidate_root).as_posix(),
        "remotePublication": "not-executed",
    }
    atomic_write_json(output, running)

    try:
        host_smoke_path = archive_verifier.verify_archive(candidate_manifest, archive, smoke=True)
        if host_smoke_path is None:
            raise InstallSmokeError("archive verifier did not produce host smoke evidence")
        host_smoke = validate_host_smoke(host_smoke_path, candidate, target)
        with tempfile.TemporaryDirectory() as temporary:
            extracted = Path(temporary)
            archive_verifier._extract(archive, extracted)
            package_roots = [path for path in extracted.iterdir() if path.is_dir()]
            if len(package_roots) != 1:
                raise InstallSmokeError("release archive must have exactly one root directory")
            package_root = package_roots[0]
            validate_installed_surface(package_root)
            contract = run_root / "contract.json"
            raw_result = run_root / "driver-result.json"
            work_dir = run_root / "work"
            work_dir.mkdir()
            atomic_write_json(
                contract,
                {
                    "schemaVersion": 1,
                    "resultId": args.result_id,
                    "target": target,
                    "profiles": list(profiles),
                    "requiredAssertions": list(REQUIRED_ASSERTIONS),
                    "requiredEvidence": list(REQUIRED_EVIDENCE),
                    "remotePublication": "not-executed",
                },
            )
            try:
                return_code, stdout, stderr = _run_child(
                    _driver_command(driver, contract, candidate_manifest, package_root, raw_result, work_dir),
                    work_dir,
                    args.timeout_seconds,
                )
            except subprocess.TimeoutExpired as error:
                (run_root / "stdout.log").write_text(error.stdout or "", encoding="utf-8")
                (run_root / "stderr.log").write_text(error.stderr or "", encoding="utf-8")
                raise InstallSmokeError(f"install smoke timed out after {args.timeout_seconds} seconds") from error
            (run_root / "stdout.log").write_text(stdout, encoding="utf-8")
            (run_root / "stderr.log").write_text(stderr, encoding="utf-8")
            if return_code != 0:
                raise InstallSmokeError(f"install smoke case driver exited with {return_code}")
            if not raw_result.is_file():
                raise InstallSmokeError("install smoke case driver produced a missing result")
            try:
                result = json.loads(raw_result.read_text(encoding="utf-8"))
            except json.JSONDecodeError as error:
                raise InstallSmokeError(f"install smoke result is invalid JSON: {error}") from error
            validate_driver_result(result, candidate, target, args.result_id, profiles, run_root)
        result.update(
            {
                "status": "passed",
                "archive": archive.relative_to(candidate_root).as_posix(),
                "archiveManifest": host_smoke["archive_manifest"],
                "hostSmoke": host_smoke_path.relative_to(candidate_root).as_posix(),
                "evidence": {
                    name: (Path(run_root.name) / relative).as_posix()
                    for name, relative in result["evidence"].items()
                },
            }
        )
        atomic_write_json(output, result)
        print(f"RELEASE_INSTALL_SMOKE_OK result_id={args.result_id} output={output}")
        return 0
    except (
        InstallSmokeError,
        archive_verifier.ArchiveError,
        OSError,
        subprocess.SubprocessError,
        tarfile.TarError,
        zipfile.BadZipFile,
        json.JSONDecodeError,
    ) as error:
        atomic_write_json(output, {**running, "status": "failed", "error": str(error)})
        print(f"RELEASE_INSTALL_SMOKE_FAILED detail={error}", file=sys.stderr)
        return 1


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Run host-targeted RocketMQ Rust release archive smoke")
    command.add_argument("--candidate-manifest", type=Path, required=True)
    command.add_argument("--archive-id", choices=tuple(TARGET_RESULTS), required=True)
    command.add_argument("--profiles", required=True)
    command.add_argument("--result-id", choices=tuple(TARGET_RESULTS.values()), required=True)
    command.add_argument("--case-driver")
    command.add_argument("--output", type=Path)
    command.add_argument("--timeout-seconds", type=int, default=MAX_TIMEOUT_SECONDS)
    return command


def main(argv: list[str] | None = None) -> int:
    try:
        return execute(parser().parse_args(argv))
    except (InstallSmokeError, OSError, json.JSONDecodeError) as error:
        print(f"RELEASE_INSTALL_SMOKE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
