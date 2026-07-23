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

"""Collect hash-bound M10 measurements on approved target hardware."""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
import platform
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import architecture_performance_guard as guard


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILES = ROOT / "scripts" / "architecture-performance-profiles.json"
DEFAULT_AUDIT_ROOT = ROOT / "target" / "architecture-refactor" / "M10"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$")
MAX_TIMEOUT_SECONDS = 86_400
MANIFEST_ENVIRONMENT_FIELDS = {
    "hardware_id",
    "filesystem",
    "build_profile",
    "features",
    "measurement_method",
}
PLACEHOLDER_MARKERS = ("replace_with", "placeholder", "synthetic", "fixture", "mock")


class SidecarError(RuntimeError):
    """Raised when authentic performance collection cannot continue safely."""


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    started_at: str
    finished_at: str
    duration_ms: int
    timed_out: bool = False


CommandExecutor = Callable[[list[str], Path, int], CommandResult]
GitSnapshotReader = Callable[[Path], tuple[str, bool]]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute_command(command: list[str], cwd: Path, timeout_seconds: int) -> CommandResult:
    started_at = utc_now()
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            start_new_session=os.name != "nt",
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP
                if os.name == "nt"
                else 0
            ),
        )
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
            exit_code = process.returncode
            timed_out = False
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                try:
                    subprocess.run(
                        ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                        capture_output=True,
                        check=False,
                        shell=False,
                        timeout=30,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    if process.poll() is None:
                        process.kill()
                if process.poll() is None:
                    process.kill()
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            stdout, stderr = process.communicate()
            exit_code = 124
            timed_out = True
    except OSError as error:
        exit_code = 127
        stdout = ""
        stderr = str(error)
        timed_out = False
    return CommandResult(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        started_at=started_at,
        finished_at=utc_now(),
        duration_ms=max(0, round((time.monotonic() - started) * 1000)),
        timed_out=timed_out,
    )


def indexed_items(items: Any, context: str, findings: list[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        findings.append(f"{context} must be a list")
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            findings.append(f"{context} entries must be objects")
            continue
        identity = item.get("id")
        if not isinstance(identity, str) or not identity:
            findings.append(f"{context} entries require a non-empty id")
            continue
        if identity in indexed:
            findings.append(f"{context} contains duplicate id: {identity}")
        indexed[identity] = item
    return indexed


def validate_command_spec(spec: dict[str, Any], context: str, findings: list[str]) -> None:
    expected_keys = {"id", "command", "timeout_seconds"}
    if set(spec) != expected_keys:
        findings.append(f"{context} must contain exactly {sorted(expected_keys)}")
    command = spec.get("command")
    if (
        not isinstance(command, list)
        or not command
        or any(not isinstance(argument, str) or not argument for argument in command)
    ):
        findings.append(f"{context}.command must be a non-empty argument list")
    else:
        normalized = " ".join(command).lower()
        if any(marker in normalized for marker in PLACEHOLDER_MARKERS):
            findings.append(f"{context}.command still contains a placeholder or fixture marker")
    timeout = spec.get("timeout_seconds")
    if (
        not isinstance(timeout, int)
        or isinstance(timeout, bool)
        or not 1 <= timeout <= MAX_TIMEOUT_SECONDS
    ):
        findings.append(
            f"{context}.timeout_seconds must be an integer between 1 and {MAX_TIMEOUT_SECONDS}"
        )


def validate_manifest(manifest: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_top_level = {
        "schema_version",
        "environment",
        "correctness_checks",
        "profiles",
    }
    if set(manifest) != expected_top_level:
        findings.append(f"manifest must contain exactly {sorted(expected_top_level)}")
    if manifest.get("schema_version") != 1:
        findings.append("manifest schema_version must be 1")

    environment = manifest.get("environment")
    if not isinstance(environment, dict):
        findings.append("manifest.environment must be an object")
        environment = {}
    elif set(environment) != MANIFEST_ENVIRONMENT_FIELDS:
        findings.append(
            "manifest.environment must contain exactly "
            f"{sorted(MANIFEST_ENVIRONMENT_FIELDS)}"
        )
    for field in ("hardware_id", "filesystem", "measurement_method"):
        value = environment.get(field)
        if not isinstance(value, str) or not value.strip():
            findings.append(f"manifest.environment.{field} must be a non-empty string")
        elif any(marker in value.lower() for marker in PLACEHOLDER_MARKERS):
            findings.append(f"manifest.environment.{field} still contains a placeholder or fixture marker")
    if environment.get("build_profile") != "release":
        findings.append("manifest.environment.build_profile must be release")
    features = environment.get("features")
    if (
        not isinstance(features, list)
        or not features
        or any(not isinstance(feature, str) or not feature for feature in features)
        or features != sorted(set(features))
    ):
        findings.append("manifest.environment.features must be a sorted unique non-empty string list")
    elif any(
        marker in feature.lower()
        for feature in features
        for marker in PLACEHOLDER_MARKERS
    ):
        findings.append("manifest.environment.features still contains a placeholder or fixture marker")

    correctness = indexed_items(
        manifest.get("correctness_checks"),
        "manifest.correctness_checks",
        findings,
    )
    expected_checks = set(policy.get("required_correctness_checks", []))
    if set(correctness) != expected_checks:
        findings.append(
            "manifest correctness inventory mismatch: "
            f"expected={sorted(expected_checks)}, actual={sorted(correctness)}"
        )
    for check_id, spec in correctness.items():
        validate_command_spec(spec, f"manifest.correctness_checks/{check_id}", findings)

    profiles = indexed_items(manifest.get("profiles"), "manifest.profiles", findings)
    expected_profiles = guard.policy_maps(policy)
    if set(profiles) != set(expected_profiles):
        findings.append(
            "manifest profile inventory mismatch: "
            f"expected={sorted(expected_profiles)}, actual={sorted(profiles)}"
        )
    for profile_id, policy_profile in expected_profiles.items():
        profile = profiles.get(profile_id)
        if profile is None:
            continue
        if set(profile) != {"id", "variants"}:
            findings.append(f"manifest.profiles/{profile_id} must contain exactly id and variants")
        variants = indexed_items(
            profile.get("variants"),
            f"manifest.profiles/{profile_id}.variants",
            findings,
        )
        expected_variants = {variant["id"] for variant in policy_profile["variants"]}
        if set(variants) != expected_variants:
            findings.append(
                f"manifest variant inventory mismatch for {profile_id}: "
                f"expected={sorted(expected_variants)}, actual={sorted(variants)}"
            )
        for variant_id, spec in variants.items():
            validate_command_spec(
                spec,
                f"manifest.profiles/{profile_id}/{variant_id}",
                findings,
            )
    return findings


def validate_environment_metadata(
    environment: dict[str, Any],
    policy: dict[str, Any],
) -> list[str]:
    findings: list[str] = []
    required_fields = set(policy.get("required_environment_fields", []))
    if set(environment) != required_fields:
        findings.append(
            "environment inventory mismatch: "
            f"expected={sorted(required_fields)}, actual={sorted(environment)}"
        )
    for field in (
        "hardware_id",
        "os",
        "kernel",
        "architecture",
        "cpu_model",
        "filesystem",
        "rustc",
        "cargo",
        "build_profile",
        "measurement_method",
    ):
        value = environment.get(field)
        if not isinstance(value, str) or not value:
            findings.append(f"environment.{field} must be a non-empty string")
    for field in ("logical_cpus", "memory_bytes"):
        value = environment.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            findings.append(f"environment.{field} must be a positive integer")
    features = environment.get("features")
    if (
        not isinstance(features, list)
        or not features
        or any(not isinstance(feature, str) or not feature for feature in features)
        or features != sorted(set(features))
    ):
        findings.append("environment.features must be a sorted unique non-empty string list")
    return findings


def manifest_template(policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "environment": {
            "hardware_id": "REPLACE_WITH_STABLE_TARGET_HARDWARE_ID",
            "filesystem": "REPLACE_WITH_TARGET_FILESYSTEM_AND_MOUNT",
            "build_profile": "release",
            "features": ["REPLACE_WITH_SORTED_FEATURE_SET"],
            "measurement_method": "REPLACE_WITH_TARGET_HARDWARE_MEASUREMENT_METHOD",
        },
        "correctness_checks": [
            {
                "id": check_id,
                "command": [
                    "REPLACE_WITH_TARGET_RUNNER",
                    "correctness",
                    check_id,
                ],
                "timeout_seconds": 3_600,
            }
            for check_id in policy["required_correctness_checks"]
        ],
        "profiles": [
            {
                "id": profile["id"],
                "variants": [
                    {
                        "id": variant["id"],
                        "command": [
                            "REPLACE_WITH_TARGET_RUNNER",
                            "measurement",
                            profile["id"],
                            variant["id"],
                        ],
                        "timeout_seconds": 7_200,
                    }
                    for variant in profile["variants"]
                ],
            }
            for profile in policy["profiles"]
        ],
    }


def is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_local_audit_path(path: Path, audit_root: Path, context: str) -> Path:
    resolved = path.resolve()
    if not is_within(resolved, audit_root):
        raise SidecarError(f"{context} must stay under ignored audit root {audit_root}")
    return resolved


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def command_transcript(
    *,
    kind: str,
    identity: dict[str, str],
    spec: dict[str, Any],
    result: CommandResult,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": kind,
        "identity": identity,
        "command": spec["command"],
        "timeout_seconds": spec["timeout_seconds"],
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "duration_ms": result.duration_ms,
        "exit_code": result.exit_code,
        "timed_out": result.timed_out,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def parse_measurement_stdout(
    stdout: str,
    profile_id: str,
    variant_id: str,
    metric_ids: set[str],
) -> dict[str, Any]:
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise SidecarError(
            f"{profile_id}/{variant_id} runner stdout is not valid JSON: {error}"
        ) from error
    expected_keys = {"schema_version", "profile", "variant", "metrics"}
    if not isinstance(result, dict) or set(result) != expected_keys:
        raise SidecarError(
            f"{profile_id}/{variant_id} runner output must contain exactly {sorted(expected_keys)}"
        )
    if result.get("schema_version") != 1:
        raise SidecarError(f"{profile_id}/{variant_id} runner schema_version must be 1")
    if result.get("profile") != profile_id or result.get("variant") != variant_id:
        raise SidecarError(f"{profile_id}/{variant_id} runner identity does not match its manifest entry")
    metrics = result.get("metrics")
    if not isinstance(metrics, dict) or set(metrics) != metric_ids:
        actual = sorted(metrics) if isinstance(metrics, dict) else []
        raise SidecarError(
            f"{profile_id}/{variant_id} metric inventory mismatch: "
            f"expected={sorted(metric_ids)}, actual={actual}"
        )
    for metric_id, measurement in metrics.items():
        if not isinstance(measurement, dict) or set(measurement) != {"samples"}:
            raise SidecarError(
                f"{profile_id}/{variant_id}/{metric_id} must contain exactly samples"
            )
    return metrics


def physical_memory_bytes() -> int:
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(MemoryStatus)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise SidecarError("cannot query physical memory with GlobalMemoryStatusEx")
        return int(status.total_physical)
    page_size = os.sysconf("SC_PAGE_SIZE")
    pages = os.sysconf("SC_PHYS_PAGES")
    memory = int(page_size) * int(pages)
    if memory <= 0:
        raise SidecarError("cannot query positive physical memory size")
    return memory


def cpu_model() -> str:
    candidates = [
        platform.processor(),
        os.environ.get("PROCESSOR_IDENTIFIER", ""),
    ]
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.is_file():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name") and ":" in line:
                candidates.append(line.split(":", 1)[1].strip())
                break
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip()
    raise SidecarError("cannot determine a non-empty CPU model")


def version_output(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        shell=False,
        timeout=30,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        raise SidecarError(f"cannot collect tool version from {' '.join(command)}")
    return completed.stdout.strip()


def collect_environment(manifest: dict[str, Any], repository_root: Path) -> dict[str, Any]:
    configured = manifest["environment"]
    logical_cpus = os.cpu_count()
    if logical_cpus is None or logical_cpus <= 0:
        raise SidecarError("cannot determine a positive logical CPU count")
    return {
        "hardware_id": configured["hardware_id"],
        "os": f"{platform.system()} {platform.version()}".strip(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "logical_cpus": logical_cpus,
        "memory_bytes": physical_memory_bytes(),
        "filesystem": configured["filesystem"],
        "rustc": version_output(["rustc", "--version", "--verbose"], repository_root),
        "cargo": version_output(["cargo", "--version", "--verbose"], repository_root),
        "build_profile": configured["build_profile"],
        "features": configured["features"],
        "measurement_method": configured["measurement_method"],
    }


def git_snapshot(repository_root: Path) -> tuple[str, bool]:
    commit = version_output(["git", "rev-parse", "HEAD"], repository_root)
    if not guard.COMMIT_RE.fullmatch(commit):
        raise SidecarError("git rev-parse HEAD did not return a full lowercase SHA-1")
    completed = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        shell=False,
        timeout=30,
    )
    if completed.returncode != 0:
        raise SidecarError("cannot inspect Git worktree state")
    return commit, bool(completed.stdout.strip())


def prepare_output_directory(output_dir: Path, audit_root: Path) -> Path:
    resolved = require_local_audit_path(output_dir, audit_root, "output directory")
    if resolved.exists():
        raise SidecarError(f"output directory already exists and will not be reused: {resolved}")
    resolved.mkdir(parents=True)
    return resolved


def collect_report(
    *,
    policy: dict[str, Any],
    manifest: dict[str, Any],
    run_id: str,
    output_dir: Path,
    audit_root: Path,
    repository_root: Path,
    git_commit: str,
    git_dirty: bool,
    environment: dict[str, Any],
    executor: CommandExecutor = execute_command,
    post_collection_git_snapshot: GitSnapshotReader | None = None,
) -> tuple[dict[str, Any], Path]:
    findings = validate_manifest(manifest, policy)
    if findings:
        raise SidecarError("invalid runner manifest:\n- " + "\n- ".join(findings))
    if not RUN_ID_RE.fullmatch(run_id):
        raise SidecarError("run_id must be a stable 3-128 character identifier")
    if not guard.COMMIT_RE.fullmatch(git_commit):
        raise SidecarError("git_commit must be a full lowercase SHA-1")
    if git_dirty:
        raise SidecarError("measurement collection requires a clean Git tree")
    environment_findings = validate_environment_metadata(environment, policy)
    if environment_findings:
        raise SidecarError(
            "invalid environment metadata:\n- "
            + "\n- ".join(environment_findings)
        )
    resolved_output = prepare_output_directory(output_dir, audit_root)

    correctness_specs = {item["id"]: item for item in manifest["correctness_checks"]}
    correctness_checks: list[dict[str, Any]] = []
    for check_id in policy["required_correctness_checks"]:
        spec = correctness_specs[check_id]
        result = executor(spec["command"], repository_root, spec["timeout_seconds"])
        transcript_path = resolved_output / "raw" / "correctness" / f"{check_id}.json"
        write_json_atomic(
            transcript_path,
            command_transcript(
                kind="correctness",
                identity={"check": check_id},
                spec=spec,
                result=result,
            ),
        )
        if result.exit_code != 0:
            reason = "timed out" if result.timed_out else f"exited with {result.exit_code}"
            raise SidecarError(f"correctness-first command {check_id} {reason}")
        correctness_checks.append(
            {
                "id": check_id,
                "status": "pass",
                "evidence_sha256": guard.file_sha256(transcript_path),
            }
        )

    manifest_profiles = {item["id"]: item for item in manifest["profiles"]}
    report_profiles: list[dict[str, Any]] = []
    for policy_profile in policy["profiles"]:
        profile_id = policy_profile["id"]
        manifest_variants = {
            item["id"]: item
            for item in manifest_profiles[profile_id]["variants"]
        }
        metric_ids = {
            metric["id"]
            for metric in guard.expanded_metrics(policy, policy_profile)
        }
        report_variants: list[dict[str, Any]] = []
        for policy_variant in policy_profile["variants"]:
            variant_id = policy_variant["id"]
            spec = manifest_variants[variant_id]
            result = executor(spec["command"], repository_root, spec["timeout_seconds"])
            transcript_path = (
                resolved_output
                / "raw"
                / "profiles"
                / profile_id
                / f"{variant_id}.json"
            )
            write_json_atomic(
                transcript_path,
                command_transcript(
                    kind="measurement",
                    identity={"profile": profile_id, "variant": variant_id},
                    spec=spec,
                    result=result,
                ),
            )
            if result.exit_code != 0:
                reason = "timed out" if result.timed_out else f"exited with {result.exit_code}"
                raise SidecarError(f"measurement command {profile_id}/{variant_id} {reason}")
            metrics = parse_measurement_stdout(
                result.stdout,
                profile_id,
                variant_id,
                metric_ids,
            )
            report_variants.append(
                {
                    "id": variant_id,
                    "parameters": policy_variant["parameters"],
                    "raw_data": {
                        "path": transcript_path.relative_to(repository_root).as_posix(),
                        "sha256": guard.file_sha256(transcript_path),
                    },
                    "metrics": metrics,
                }
            )
        report_profiles.append(
            {
                "id": profile_id,
                "measurement_status": "measured",
                "variants": report_variants,
            }
        )

    if post_collection_git_snapshot is not None:
        final_commit, final_dirty = post_collection_git_snapshot(repository_root)
        if final_commit != git_commit or final_dirty:
            raise SidecarError(
                "Git commit or worktree state changed during measurement collection"
            )

    report = {
        "schema_version": 1,
        "run_id": run_id,
        "report_kind": "measurement",
        "profile_schema_sha256": guard.canonical_sha256(policy),
        "generated_at": utc_now(),
        "git": {"commit": git_commit, "dirty": False},
        "environment": environment,
        "correctness_checks": correctness_checks,
        "profiles": report_profiles,
    }
    report_findings = guard.validate_report(
        report,
        policy,
        "sidecar report",
        allow_fixture=False,
    )
    if report_findings:
        raise SidecarError("generated report failed validation:\n- " + "\n- ".join(report_findings))
    report_path = resolved_output / "measurement-report.json"
    write_json_atomic(report_path, report)
    return report, report_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", type=Path, default=DEFAULT_PROFILES)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate-manifest", type=Path)
    mode.add_argument("--manifest", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        policy = guard.load_json(args.profiles)
        policy_findings = guard.validate_profile_policy(policy)
        if policy_findings:
            raise SidecarError("invalid performance policy:\n- " + "\n- ".join(policy_findings))
        if args.generate_manifest is not None:
            manifest_path = require_local_audit_path(
                args.generate_manifest,
                DEFAULT_AUDIT_ROOT,
                "generated manifest",
            )
            if manifest_path.exists():
                raise SidecarError(f"generated manifest already exists: {manifest_path}")
            write_json_atomic(manifest_path, manifest_template(policy))
            print(f"ARCHITECTURE_PERFORMANCE_SIDECAR_TEMPLATE_OK path={manifest_path}")
            return 0
        if args.output_dir is None or args.run_id is None:
            raise SidecarError("--output-dir and --run-id are required with --manifest")
        manifest = guard.load_json(args.manifest)
        manifest_findings = validate_manifest(manifest, policy)
        if manifest_findings:
            raise SidecarError("invalid runner manifest:\n- " + "\n- ".join(manifest_findings))
        commit, dirty = git_snapshot(ROOT)
        environment = collect_environment(manifest, ROOT)
        report, report_path = collect_report(
            policy=policy,
            manifest=manifest,
            run_id=args.run_id,
            output_dir=args.output_dir,
            audit_root=DEFAULT_AUDIT_ROOT,
            repository_root=ROOT,
            git_commit=commit,
            git_dirty=dirty,
            environment=environment,
            post_collection_git_snapshot=git_snapshot,
        )
        variant_count = sum(len(profile["variants"]) for profile in report["profiles"])
        print(
            "ARCHITECTURE_PERFORMANCE_SIDECAR_OK "
            f"correctness={len(report['correctness_checks'])} variants={variant_count} "
            f"report={report_path}"
        )
        return 0
    except (OSError, ValueError, SidecarError) as error:
        print(f"ARCHITECTURE_PERFORMANCE_SIDECAR_FAILED {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
