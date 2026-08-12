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

"""Measure, compare, and qualify fail-closed message-path releases."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "scripts" / "message-path-qualification-policy.json"
DEFAULT_OUTPUT_ROOT = ROOT / "target" / "message-path-qualification"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$")
TOPIC_RE = re.compile(r"^[A-Za-z0-9_%.-]{1,127}$")
SCENARIOS = {"sync", "async", "batch", "lite-pull"}
EXTERNAL_EVIDENCE = {"performance_comparison", "fault_matrix", "rpo", "soak"}
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class QualificationError(RuntimeError):
    """Raised when qualification cannot safely continue."""


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int


CommandExecutor = Callable[[list[str], Path, int], CommandResult]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(f"cannot load JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise QualificationError(f"JSON document must be an object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def canonical_sha256(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def execute_command(command: list[str], cwd: Path, timeout_seconds: int) -> CommandResult:
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            shell=False,
            check=False,
        )
        return CommandResult(result.returncode, result.stdout, result.stderr, round((time.monotonic() - started) * 1000))
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout.decode("utf-8", errors="replace") if isinstance(error.stdout, bytes) else error.stdout or ""
        stderr = error.stderr.decode("utf-8", errors="replace") if isinstance(error.stderr, bytes) else error.stderr or ""
        return CommandResult(124, stdout, stderr + "\ncommand timed out", round((time.monotonic() - started) * 1000))
    except OSError as error:
        return CommandResult(127, "", str(error), round((time.monotonic() - started) * 1000))


def positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_policy(policy: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected = {"schema_version", "artifact_kind", "comparison_thresholds", "stability_thresholds", "modes"}
    if set(policy) != expected:
        findings.append(f"policy keys must be exactly {sorted(expected)}")
    if policy.get("schema_version") != 1:
        findings.append("policy schema_version must be 1")
    if policy.get("artifact_kind") != "rocketmq_message_path_qualification_policy":
        findings.append("policy artifact_kind is invalid")

    thresholds = policy.get("comparison_thresholds")
    expected_thresholds = {
        "maximum_throughput_regression_percent",
        "maximum_p99_latency_regression_percent",
    }
    if not isinstance(thresholds, dict) or set(thresholds) != expected_thresholds:
        findings.append("comparison_thresholds has an invalid shape")
    else:
        for key in expected_thresholds:
            value = thresholds.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 < float(value) <= 100:
                findings.append(f"comparison_thresholds.{key} must be >0 and <=100")

    stability = policy.get("stability_thresholds")
    expected_stability = {
        "maximum_throughput_normalized_mad_percent",
        "maximum_p99_latency_normalized_mad_percent",
    }
    if not isinstance(stability, dict) or set(stability) != expected_stability:
        findings.append("stability_thresholds has an invalid shape")
    else:
        for key in expected_stability:
            value = stability.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 < float(value) <= 100:
                findings.append(f"stability_thresholds.{key} must be >0 and <=100")

    modes = policy.get("modes")
    if not isinstance(modes, dict) or set(modes) != {"smoke", "release"}:
        findings.append("policy modes must be exactly smoke and release")
        return findings

    expected_mode_keys = {
        "minimum_repetitions",
        "warmup_runs",
        "require_clean_git",
        "required_external_evidence",
        "workloads",
    }
    for mode_name, mode in modes.items():
        if not isinstance(mode, dict):
            findings.append(f"modes.{mode_name} must be an object")
            continue
        allowed_keys = expected_mode_keys | ({"minimum_soak_seconds"} if mode_name == "release" else set())
        if set(mode) != allowed_keys:
            findings.append(f"modes.{mode_name} keys must be exactly {sorted(allowed_keys)}")
        if not positive_int(mode.get("minimum_repetitions")):
            findings.append(f"modes.{mode_name}.minimum_repetitions must be positive")
        if not isinstance(mode.get("warmup_runs"), int) or isinstance(mode.get("warmup_runs"), bool) or mode["warmup_runs"] < 0:
            findings.append(f"modes.{mode_name}.warmup_runs must be non-negative")
        if not isinstance(mode.get("require_clean_git"), bool):
            findings.append(f"modes.{mode_name}.require_clean_git must be boolean")
        required = mode.get("required_external_evidence")
        if not isinstance(required, list) or len(required) != len(set(required or [])):
            findings.append(f"modes.{mode_name}.required_external_evidence must be a unique list")
        elif not set(required).issubset(EXTERNAL_EVIDENCE):
            findings.append(f"modes.{mode_name}.required_external_evidence contains an unknown kind")
        if mode_name == "smoke" and required != []:
            findings.append("smoke must not require external release evidence")
        if mode_name == "release":
            if mode.get("minimum_repetitions", 0) < 5:
                findings.append("release requires at least five repetitions")
            if set(required or []) != EXTERNAL_EVIDENCE:
                findings.append("release must require performance, fault, RPO, and soak evidence")
            if mode.get("minimum_soak_seconds", 0) < 21_600:
                findings.append("release minimum soak must be at least six hours")

        workloads = mode.get("workloads")
        if not isinstance(workloads, list) or not workloads:
            findings.append(f"modes.{mode_name}.workloads must be a non-empty list")
            continue
        seen: set[str] = set()
        workload_keys = {"id", "scenario", "message_count", "message_size_bytes", "batch_size", "timeout_ms"}
        for index, workload in enumerate(workloads):
            context = f"modes.{mode_name}.workloads[{index}]"
            if not isinstance(workload, dict) or set(workload) != workload_keys:
                findings.append(f"{context} has an invalid shape")
                continue
            identity = workload.get("id")
            if not isinstance(identity, str) or not RUN_ID_RE.fullmatch(identity):
                findings.append(f"{context}.id is invalid")
            elif identity in seen:
                findings.append(f"{context}.id is duplicated")
            else:
                seen.add(identity)
            if workload.get("scenario") not in SCENARIOS:
                findings.append(f"{context}.scenario is invalid")
            for key in ("message_count", "message_size_bytes", "batch_size", "timeout_ms"):
                if not positive_int(workload.get(key)):
                    findings.append(f"{context}.{key} must be positive")
    return findings


def git_snapshot(root: Path) -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, encoding="utf-8", check=False
    )
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, encoding="utf-8", check=False
    )
    if commit.returncode != 0 or status.returncode != 0:
        raise QualificationError("cannot bind qualification to the current Git snapshot")
    return commit.stdout.strip(), bool(status.stdout.strip())


def tool_version(command: list[str]) -> str:
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", check=False)
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def environment_record() -> dict[str, Any]:
    identity = "|".join(
        [platform.node(), platform.machine(), platform.processor(), str(os.cpu_count() or 0), platform.platform()]
    )
    return {
        "hardware_id": f"sha256:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}",
        "os": platform.system(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "logical_cpus": os.cpu_count() or 0,
        "rustc": tool_version(["rustc", "--version"]),
        "cargo": tool_version(["cargo", "--version"]),
    }


def validate_target(namesrv: str, confirmation: str, topic: str, durability_contract: str) -> None:
    if not namesrv.strip() or namesrv != confirmation:
        raise QualificationError("--confirm-target must exactly match the non-empty --namesrv value")
    if not TOPIC_RE.fullmatch(topic):
        raise QualificationError("--topic must contain only RocketMQ-safe topic characters")
    if not durability_contract.strip():
        raise QualificationError("--durability-contract must be explicit and non-empty")


def benchmark_command(
    workload: dict[str, Any], namesrv: str, topic: str, run_id: str, output_json: Path
) -> list[str]:
    return [
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-client-rust",
        "--example",
        "client-production-benchmark",
        "--",
        "--namesrv",
        namesrv,
        "--topic",
        topic,
        "--scenario",
        str(workload["scenario"]),
        "--message-count",
        str(workload["message_count"]),
        "--message-size",
        str(workload["message_size_bytes"]),
        "--batch-size",
        str(workload["batch_size"]),
        "--timeout-ms",
        str(workload["timeout_ms"]),
        "--run-id",
        run_id,
        "--output-json",
        str(output_json),
    ]


def build_plan(
    policy: dict[str, Any], mode: str, namesrv: str, topic: str, run_id: str, repetitions: int
) -> dict[str, Any]:
    mode_policy = policy["modes"][mode]
    commands: list[list[str]] = []
    for workload in mode_policy["workloads"]:
        for repetition in range(mode_policy["warmup_runs"] + repetitions):
            sample_id = f"{run_id}-{workload['id']}-{repetition + 1}"
            commands.append(benchmark_command(workload, namesrv, topic, sample_id, Path("<run-dir>") / f"{sample_id}.json"))
    return {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_qualification_plan",
        "mode": mode,
        "run_id": run_id,
        "target": {"namesrv_addr": namesrv, "topic": topic},
        "warmup_runs_per_workload": mode_policy["warmup_runs"],
        "repetitions_per_workload": repetitions,
        "commands": commands,
        "required_external_evidence": mode_policy["required_external_evidence"],
    }


def validate_measurement(
    measurement: dict[str, Any], workload: dict[str, Any], namesrv: str, topic: str, sample_id: str
) -> list[str]:
    findings: list[str] = []
    if measurement.get("schema_version") != 1:
        findings.append("measurement schema_version must be 1")
    if measurement.get("artifact_kind") != "rocketmq_message_path_measurement":
        findings.append("measurement artifact_kind is invalid")
    if measurement.get("run_id") != sample_id:
        findings.append("measurement run_id does not match the requested sample")
    if measurement.get("scenario") != workload["scenario"]:
        findings.append("measurement scenario does not match the workload")
    target = measurement.get("target")
    if target != {"namesrv_addr": namesrv, "topic": topic}:
        findings.append("measurement target does not match the confirmed target")
    expected_workload = {
        "message_count": workload["message_count"],
        "message_size_bytes": workload["message_size_bytes"],
        "batch_size": workload["batch_size"],
    }
    if measurement.get("workload") != expected_workload:
        findings.append("measurement workload does not match policy")
    result = measurement.get("result")
    if not isinstance(result, dict):
        findings.append("measurement result must be an object")
        return findings
    success = result.get("success_count")
    send_failed = result.get("send_failed_count")
    response_failed = result.get("response_failed_count")
    if success != workload["message_count"] or send_failed != 0 or response_failed != 0:
        findings.append("measurement must complete every message without send or response failures")
    for metric in ("duration_us", "throughput_messages_per_second", "payload_mib_per_second"):
        value = result.get(metric)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value)) or value <= 0:
            findings.append(f"measurement result.{metric} must be finite and positive")
    latency = result.get("latency_us")
    if not isinstance(latency, dict) or latency.get("samples") != workload["message_count"]:
        findings.append("measurement latency sample count must equal message_count")
    elif not positive_int(latency.get("p99")):
        findings.append("measurement latency p99 must be positive")
    return findings


def record_artifact(run_dir: Path, path: Path) -> dict[str, str]:
    return {"path": path.relative_to(run_dir).as_posix(), "sha256": sha256_file(path)}


def copy_external(run_dir: Path, source: Path, name: str) -> dict[str, str]:
    target = run_dir / "external" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return record_artifact(run_dir, target)


def bound_file(path: Path | None, label: str, *, required: bool) -> tuple[str | None, list[str]]:
    if path is None:
        return None, [f"release measurement requires --{label}"] if required else []
    resolved = path.resolve()
    if not resolved.is_file():
        return None, [f"--{label} is not a file: {resolved}"]
    return f"sha256:{sha256_file(resolved)}", []


def measurement_identity(
    args: argparse.Namespace, commit: str, dirty: bool, *, release: bool
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    findings: list[str] = []
    subject_commit = getattr(args, "subject_commit", None) or commit
    if not GIT_SHA_RE.fullmatch(subject_commit):
        findings.append("subject commit must be a full lowercase Git SHA")
    if subject_commit != commit:
        findings.append("subject commit must equal the checked-out commit")
    role = getattr(args, "subject_role", None) or ("candidate" if release else "smoke")
    if role not in {"baseline", "candidate", "smoke"}:
        findings.append("subject role must be baseline, candidate, or smoke")
    artifact_hash, artifact_findings = bound_file(
        getattr(args, "artifact_manifest", None), "artifact-manifest", required=release
    )
    config_hash, config_findings = bound_file(
        getattr(args, "effective_config", None), "effective-config", required=release
    )
    findings.extend(artifact_findings)
    findings.extend(config_findings)
    deployment_digest = getattr(args, "deployment_digest", None)
    if release and not isinstance(deployment_digest, str):
        findings.append("release measurement requires --deployment-digest")
    elif deployment_digest is not None and not DIGEST_RE.fullmatch(deployment_digest):
        findings.append("deployment digest must use sha256:<64 lowercase hex>")
    target_id = getattr(args, "target_id", None)
    cluster_uid = getattr(args, "cluster_uid", None)
    if release and not target_id:
        findings.append("release measurement requires --target-id")
    if release and not cluster_uid:
        findings.append("release measurement requires --cluster-uid")
    if release and dirty:
        findings.append("release measurement requires a clean Git worktree")
    subject = {
        "role": role,
        "commit": subject_commit,
        "artifact_manifest_sha256": artifact_hash,
        "deployment_digest": deployment_digest,
    }
    target = {
        "target_id": target_id or "local-smoke",
        "cluster_uid": cluster_uid or "local-smoke",
        "namesrv_addr": args.namesrv,
        "topic": args.topic,
        "effective_config_sha256": config_hash,
    }
    return subject, target, findings


def aggregate_samples(samples: list[dict[str, Any]]) -> dict[str, float]:
    throughputs = [float(item["result"]["throughput_messages_per_second"]) for item in samples]
    payload_rates = [float(item["result"]["payload_mib_per_second"]) for item in samples]
    result = {
        "throughput_messages_per_second_median": statistics.median(throughputs),
        "payload_mib_per_second_median": statistics.median(payload_rates),
    }
    for percentile in ("p50", "p95", "p99", "p999", "average"):
        values = [float(item["result"]["latency_us"][percentile]) for item in samples]
        result[f"{percentile}_latency_us_median"] = statistics.median(values)
    return result


def run_qualification(
    policy: dict[str, Any], args: argparse.Namespace, executor: CommandExecutor = execute_command
) -> tuple[dict[str, Any], Path]:
    findings = validate_policy(policy)
    if findings:
        raise QualificationError("invalid qualification policy: " + "; ".join(findings))
    validate_target(args.namesrv, args.confirm_target, args.topic, args.durability_contract)
    if args.mode not in policy["modes"]:
        raise QualificationError(f"unknown qualification mode: {args.mode}")
    mode_policy = policy["modes"][args.mode]
    repetitions = args.repetitions or mode_policy["minimum_repetitions"]
    if repetitions < mode_policy["minimum_repetitions"]:
        raise QualificationError(f"{args.mode} requires at least {mode_policy['minimum_repetitions']} repetitions")
    if not RUN_ID_RE.fullmatch(args.run_id):
        raise QualificationError("--run-id must contain 3-128 safe characters")

    commit, dirty = git_snapshot(ROOT)
    if mode_policy["require_clean_git"] and dirty:
        raise QualificationError("release qualification requires a clean Git worktree")
    run_dir = args.output_dir.resolve() / args.run_id
    if run_dir.exists():
        raise QualificationError(f"qualification output already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    artifacts: list[dict[str, str]] = []
    subject, target, failures = measurement_identity(args, commit, dirty, release=args.mode == "release")
    workload_reports: list[dict[str, Any]] = []

    if not failures:
        for workload in mode_policy["workloads"]:
            measured: list[dict[str, Any]] = []
            sample_records: list[dict[str, Any]] = []
            total_runs = mode_policy["warmup_runs"] + repetitions
            for run_index in range(total_runs):
                warmup = run_index < mode_policy["warmup_runs"]
                ordinal = run_index + 1 if warmup else run_index - mode_policy["warmup_runs"] + 1
                phase = "warmup" if warmup else "sample"
                sample_id = f"{args.run_id}-{workload['id']}-{phase}-{ordinal}"
                raw_path = run_dir / "raw" / workload["id"] / f"{phase}-{ordinal}.json"
                stdout_path = raw_path.with_suffix(".stdout.log")
                stderr_path = raw_path.with_suffix(".stderr.log")
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                command = benchmark_command(workload, args.namesrv, args.topic, sample_id, raw_path)
                result = executor(command, ROOT, args.command_timeout_seconds)
                stdout_path.write_text(result.stdout, encoding="utf-8")
                stderr_path.write_text(result.stderr, encoding="utf-8")
                artifacts.extend([record_artifact(run_dir, stdout_path), record_artifact(run_dir, stderr_path)])
                if result.exit_code != 0:
                    failures.append(f"{sample_id} failed with exit code {result.exit_code}")
                    break
                if not raw_path.is_file():
                    failures.append(f"{sample_id} did not produce its JSON measurement")
                    break
                measurement = load_json(raw_path)
                sample_findings = validate_measurement(measurement, workload, args.namesrv, args.topic, sample_id)
                artifacts.append(record_artifact(run_dir, raw_path))
                if sample_findings:
                    failures.extend(f"{sample_id}: {finding}" for finding in sample_findings)
                    break
                sample_record = {
                    "phase": phase,
                    "ordinal": ordinal,
                    "duration_ms": result.duration_ms,
                    "measurement": measurement,
                    "raw_data": record_artifact(run_dir, raw_path),
                }
                sample_records.append(sample_record)
                if not warmup:
                    measured.append(measurement)
            workload_report: dict[str, Any] = {
                "id": workload["id"],
                "parameters": workload,
                "runs": sample_records,
            }
            if len(measured) == repetitions:
                workload_report["aggregate"] = aggregate_samples(measured)
            workload_reports.append(workload_report)
            if failures:
                break

    report = {
        "schema_version": 2,
        "artifact_kind": "rocketmq_message_path_measurement_set",
        "run_id": args.run_id,
        "generated_at": utc_now(),
        "mode": args.mode,
        "status": "fail" if failures else "pass",
        "measurement_qualified": not failures,
        "business_contract": "java-equivalent-message-semantics",
        "implementation_strategy": "rust-native",
        "durability_contract": args.durability_contract,
        "policy_sha256": canonical_sha256(policy),
        "git": {"commit": commit, "dirty": dirty},
        "subject": subject,
        "environment": environment_record(),
        "target": target,
        "repetitions_per_workload": repetitions,
        "workloads": workload_reports,
        "failures": failures,
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
    }
    report_path = run_dir / "measurement-set.json"
    write_json(report_path, report)
    return report, report_path


def regression_percent(direction: str, baseline: float, candidate: float) -> float:
    if baseline <= 0:
        raise QualificationError("baseline comparison metric must be positive")
    if direction == "higher":
        return (baseline - candidate) / baseline * 100.0
    return (candidate - baseline) / baseline * 100.0


def measurement_binding(report: dict[str, Any], report_sha256: str | None) -> dict[str, Any]:
    subject = report.get("subject", {})
    target = report.get("target", {})
    return {
        "report_sha256": report_sha256,
        "commit": subject.get("commit"),
        "artifact_manifest_sha256": subject.get("artifact_manifest_sha256"),
        "deployment_digest": subject.get("deployment_digest"),
        "target_id": target.get("target_id"),
        "cluster_uid": target.get("cluster_uid"),
        "effective_config_sha256": target.get("effective_config_sha256"),
    }


def validate_measurement_set(
    policy: dict[str, Any], report: dict[str, Any], label: str, *, expected_role: str | None = None
) -> list[str]:
    failures: list[str] = []
    if report.get("schema_version") != 2 or report.get("artifact_kind") != "rocketmq_message_path_measurement_set":
        failures.append(f"{label} measurement contract is invalid")
    if report.get("status") != "pass" or report.get("measurement_qualified") is not True:
        failures.append(f"{label} measurement must be qualified and pass")
    if report.get("policy_sha256") != canonical_sha256(policy):
        failures.append(f"{label} measurement policy hash differs")
    subject = report.get("subject")
    if not isinstance(subject, dict):
        failures.append(f"{label} subject binding is missing")
    else:
        if expected_role is not None and subject.get("role") != expected_role:
            failures.append(f"{label} subject role must be {expected_role}")
        if not isinstance(subject.get("commit"), str) or not GIT_SHA_RE.fullmatch(subject["commit"]):
            failures.append(f"{label} subject commit is invalid")
        for key in ("artifact_manifest_sha256", "deployment_digest"):
            value = subject.get(key)
            if report.get("mode") == "release" and (not isinstance(value, str) or not DIGEST_RE.fullmatch(value)):
                failures.append(f"{label} subject {key} is invalid")
    target = report.get("target")
    if not isinstance(target, dict):
        failures.append(f"{label} target binding is missing")
    elif report.get("mode") == "release":
        for key in ("target_id", "cluster_uid"):
            if not isinstance(target.get(key), str) or not target[key].strip():
                failures.append(f"{label} target {key} is missing")
        value = target.get("effective_config_sha256")
        if not isinstance(value, str) or not DIGEST_RE.fullmatch(value):
            failures.append(f"{label} target effective_config_sha256 is invalid")
    if report.get("mode") == "release":
        minimum = policy["modes"]["release"]["minimum_repetitions"]
        if report.get("repetitions_per_workload", 0) < minimum:
            failures.append(f"{label} measurement has fewer than {minimum} repetitions")
    return failures


def compare_reports(
    policy: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    baseline_sha256: str | None = None,
    candidate_sha256: str | None = None,
) -> dict[str, Any]:
    failures = validate_policy(policy)
    failures.extend(validate_measurement_set(policy, baseline, "baseline", expected_role="baseline"))
    failures.extend(validate_measurement_set(policy, candidate, "candidate", expected_role="candidate"))
    if failures:
        return {
            "schema_version": 2,
            "artifact_kind": "rocketmq_message_path_comparison",
            "status": "fail",
            "release_comparison_qualified": False,
            "failures": failures,
            "comparisons": [],
        }
    if baseline.get("mode") != candidate.get("mode"):
        failures.append("baseline and candidate modes differ")
    if baseline.get("subject", {}).get("commit") == candidate.get("subject", {}).get("commit"):
        failures.append("baseline and candidate commits must differ")
    if baseline.get("durability_contract") != candidate.get("durability_contract"):
        failures.append("baseline and candidate durability contracts differ")
    if baseline.get("business_contract") != candidate.get("business_contract"):
        failures.append("baseline and candidate business contracts differ")
    if baseline.get("environment", {}).get("hardware_id") != candidate.get("environment", {}).get("hardware_id"):
        failures.append("baseline and candidate hardware identities differ")
    for key in ("target_id", "cluster_uid", "effective_config_sha256"):
        if baseline.get("target", {}).get(key) != candidate.get("target", {}).get(key):
            failures.append(f"baseline and candidate {key} bindings differ")

    baseline_workloads = {item["id"]: item for item in baseline.get("workloads", []) if isinstance(item, dict) and "id" in item}
    candidate_workloads = {item["id"]: item for item in candidate.get("workloads", []) if isinstance(item, dict) and "id" in item}
    if set(baseline_workloads) != set(candidate_workloads):
        failures.append("baseline and candidate workload sets differ")
    comparisons: list[dict[str, Any]] = []
    thresholds = policy["comparison_thresholds"]
    for workload_id in sorted(set(baseline_workloads) & set(candidate_workloads)):
        baseline_item = baseline_workloads[workload_id]
        candidate_item = candidate_workloads[workload_id]
        if baseline_item.get("parameters") != candidate_item.get("parameters"):
            failures.append(f"{workload_id}: workload parameters differ")
            continue
        baseline_aggregate = baseline_item.get("aggregate", {})
        candidate_aggregate = candidate_item.get("aggregate", {})
        throughput_regression = regression_percent(
            "higher",
            float(baseline_aggregate.get("throughput_messages_per_second_median", 0)),
            float(candidate_aggregate.get("throughput_messages_per_second_median", 0)),
        )
        p99_regression = regression_percent(
            "lower",
            float(baseline_aggregate.get("p99_latency_us_median", 0)),
            float(candidate_aggregate.get("p99_latency_us_median", 0)),
        )
        status = "pass"
        if throughput_regression > thresholds["maximum_throughput_regression_percent"]:
            failures.append(f"{workload_id}: throughput regression exceeds the policy threshold")
            status = "fail"
        if p99_regression > thresholds["maximum_p99_latency_regression_percent"]:
            failures.append(f"{workload_id}: p99 latency regression exceeds the policy threshold")
            status = "fail"
        comparisons.append(
            {
                "workload": workload_id,
                "status": status,
                "throughput_regression_percent": round(throughput_regression, 6),
                "p99_latency_regression_percent": round(p99_regression, 6),
            }
        )
    return {
        "schema_version": 2,
        "artifact_kind": "rocketmq_message_path_comparison",
        "generated_at": utc_now(),
        "status": "fail" if failures else "pass",
        "release_comparison_qualified": not failures and baseline.get("mode") == "release",
        "policy_sha256": canonical_sha256(policy),
        "durability_contract": candidate.get("durability_contract"),
        "hardware_id": candidate.get("environment", {}).get("hardware_id"),
        "target": candidate.get("target"),
        "baseline": measurement_binding(baseline, baseline_sha256),
        "candidate": measurement_binding(candidate, candidate_sha256),
        "failures": failures,
        "comparisons": comparisons,
    }


def evidence_path(path: Path, name: str) -> Path:
    resolved = path.resolve()
    if resolved.is_dir():
        resolved = resolved / name
    if not resolved.is_file():
        raise QualificationError(f"evidence file does not exist: {resolved}")
    return resolved


def validate_final_evidence(
    policy: dict[str, Any], args: argparse.Namespace
) -> tuple[list[str], dict[str, Path], dict[str, dict[str, Any]]]:
    paths = {
        "candidate_measurement": evidence_path(args.candidate_measurement, "measurement-set.json"),
        "performance_comparison": evidence_path(args.performance_comparison, "performance-comparison.json"),
        "fault_matrix": evidence_path(args.fault_evidence, "run.json"),
        "rpo": evidence_path(args.rpo_evidence, "ack-failover-run.json"),
        "soak": evidence_path(args.soak_report, "soak-report.json"),
    }
    documents = {key: load_json(path) for key, path in paths.items()}
    candidate = documents["candidate_measurement"]
    comparison = documents["performance_comparison"]
    fault = documents["fault_matrix"]
    rpo = documents["rpo"]
    soak = documents["soak"]
    findings = validate_measurement_set(policy, candidate, "candidate", expected_role="candidate")
    subject = candidate.get("subject", {})
    target = candidate.get("target", {})
    expected_commit = subject.get("commit")
    expected_deployment = subject.get("deployment_digest")
    expected_target = target.get("target_id")
    expected_cluster = target.get("cluster_uid")
    expected_config = target.get("effective_config_sha256")
    expected_durability = candidate.get("durability_contract")
    candidate_hash = f"sha256:{sha256_file(paths['candidate_measurement'])}"
    if args.candidate_commit != expected_commit:
        findings.append("--candidate-commit differs from the candidate measurement")

    if comparison.get("schema_version") != 2 or comparison.get("artifact_kind") != "rocketmq_message_path_comparison":
        findings.append("performance comparison contract is invalid")
    if comparison.get("status") != "pass" or comparison.get("release_comparison_qualified") is not True:
        findings.append("performance comparison is not release-qualified")
    comparison_candidate = comparison.get("candidate", {})
    for key, expected in (
        ("report_sha256", candidate_hash),
        ("commit", expected_commit),
        ("deployment_digest", expected_deployment),
        ("target_id", expected_target),
        ("cluster_uid", expected_cluster),
        ("effective_config_sha256", expected_config),
    ):
        if comparison_candidate.get(key) != expected:
            findings.append(f"performance comparison candidate {key} differs")
    if comparison.get("durability_contract") != expected_durability:
        findings.append("performance comparison durability differs")

    fault_identity = fault.get("release_identity", {})
    if fault.get("candidate_commit") != expected_commit or fault_identity.get("deployment_digest") != expected_deployment:
        findings.append("fault evidence candidate identity differs")
    if fault.get("dynamic_execution") is not True or fault.get("fixture") is not False:
        findings.append("fault evidence must be a dynamic non-fixture run")
    if fault_identity.get("target_id") != expected_target or fault_identity.get("cluster_uid") != expected_cluster:
        findings.append("fault evidence target differs")
    if fault_identity.get("effective_config_sha256") != expected_config:
        findings.append("fault evidence effective configuration differs")
    if fault_identity.get("durability_contract") != expected_durability:
        findings.append("fault evidence durability differs")

    if rpo.get("schema_version") != 1 or rpo.get("artifact_kind") != "controller_failover_qualification_evidence":
        findings.append("RPO evidence contract is invalid")
    if rpo.get("status") != "pass" or rpo.get("strict_qualification_passed") is not True:
        findings.append("RPO evidence is not strict-qualified")
    for key, expected in (
        ("candidate_commit", expected_commit),
        ("deployment_digest", expected_deployment),
        ("target_id", expected_target),
        ("cluster_uid", expected_cluster),
        ("effective_config_sha256", expected_config),
        ("durability_contract", expected_durability),
    ):
        if rpo.get(key) != expected:
            findings.append(f"RPO evidence {key} differs")

    if soak.get("schema_version") != 1 or soak.get("artifact_kind") != "rocketmq_message_path_soak_report":
        findings.append("soak report contract is invalid")
    if soak.get("status") != "pass" or soak.get("monotonic_growth_detected") is not False:
        findings.append("soak report must pass without monotonic resource growth")
    if soak.get("duration_seconds", 0) < policy["modes"]["release"]["minimum_soak_seconds"]:
        findings.append("soak duration is below the release minimum")
    soak_identity = soak.get("release_identity", {})
    for key, expected in (
        ("commit", expected_commit),
        ("deployment_digest", expected_deployment),
        ("target_id", expected_target),
        ("cluster_uid", expected_cluster),
        ("effective_config_sha256", expected_config),
        ("durability_contract", expected_durability),
    ):
        if soak_identity.get(key) != expected:
            findings.append(f"soak release identity {key} differs")
    sampling = soak.get("sampling", {})
    if sampling.get("coverage_percent", 0) < 99 or sampling.get("max_gap_seconds", math.inf) > 90:
        findings.append("soak sampling coverage or maximum gap is outside the release contract")
    if not isinstance(soak.get("pods"), list) or not soak["pods"]:
        findings.append("soak report must include pod identity and restart evidence")
    elif any(pod.get("restarts") != 0 or pod.get("oom_killed") is not False for pod in soak["pods"]):
        findings.append("soak report contains a restarted or OOM-killed pod")
    if not isinstance(soak.get("series"), list) or not soak["series"]:
        findings.append("soak report must include analyzed resource series")
    elif any(item.get("status") != "pass" or not DIGEST_RE.fullmatch(str(item.get("raw_artifact_sha256", ""))) for item in soak["series"]):
        findings.append("soak resource series must pass and bind raw artifact hashes")
    return findings, paths, documents


def run_final_qualification(policy: dict[str, Any], args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    if not GIT_SHA_RE.fullmatch(args.candidate_commit):
        raise QualificationError("--candidate-commit must be a full lowercase Git SHA")
    if not RUN_ID_RE.fullmatch(args.run_id):
        raise QualificationError("--run-id must contain 3-128 safe characters")
    run_dir = args.output_dir.resolve() / args.run_id
    if run_dir.exists():
        raise QualificationError(f"qualification output already exists: {run_dir}")
    findings, source_paths, documents = validate_final_evidence(policy, args)
    run_dir.mkdir(parents=True)
    evidence: dict[str, Any] = {}
    artifacts: list[dict[str, str]] = []
    for key, source in source_paths.items():
        artifact = copy_external(run_dir, source, source.name)
        evidence[key] = artifact
        artifacts.append(artifact)
    candidate = documents["candidate_measurement"]
    report = {
        "schema_version": 2,
        "artifact_kind": "rocketmq_message_path_qualification_report",
        "run_id": args.run_id,
        "generated_at": utc_now(),
        "status": "fail" if findings else "pass",
        "release_qualified": not findings,
        "candidate_commit": args.candidate_commit,
        "subject": candidate.get("subject"),
        "target": candidate.get("target"),
        "durability_contract": candidate.get("durability_contract"),
        "policy_sha256": canonical_sha256(policy),
        "evidence": evidence,
        "failures": findings,
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
    }
    report_path = run_dir / "qualification-report.json"
    write_json(report_path, report)
    return report, report_path


def add_target_arguments(parser: argparse.ArgumentParser, *, measurement: bool = False) -> None:
    parser.add_argument("--mode", choices=("smoke", "release"), required=True)
    parser.add_argument("--namesrv", required=True)
    parser.add_argument("--confirm-target", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--durability-contract", required=True)
    parser.add_argument("--run-id", default=f"message-path-{int(time.time())}")
    parser.add_argument("--repetitions", type=int)
    if measurement:
        parser.add_argument("--subject-role", choices=("baseline", "candidate", "smoke"))
        parser.add_argument("--subject-commit")
        parser.add_argument("--artifact-manifest", type=Path)
        parser.add_argument("--deployment-digest")
        parser.add_argument("--target-id")
        parser.add_argument("--cluster-uid")
        parser.add_argument("--effective-config", type=Path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-policy")

    plan_parser = subparsers.add_parser("plan")
    add_target_arguments(plan_parser)

    for command in ("measure", "run"):
        measure_parser = subparsers.add_parser(command)
        add_target_arguments(measure_parser, measurement=True)
        measure_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
        measure_parser.add_argument("--command-timeout-seconds", type=int, default=1800)

    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("--baseline", type=Path, required=True)
    compare_parser.add_argument("--candidate", type=Path, required=True)
    compare_parser.add_argument("--output", type=Path, required=True)

    qualify_parser = subparsers.add_parser("qualify")
    qualify_parser.add_argument("--candidate-commit", required=True)
    qualify_parser.add_argument("--candidate-measurement", type=Path, required=True)
    qualify_parser.add_argument("--performance-comparison", type=Path, required=True)
    qualify_parser.add_argument("--fault-evidence", type=Path, required=True)
    qualify_parser.add_argument("--rpo-evidence", type=Path, required=True)
    qualify_parser.add_argument("--soak-report", type=Path, required=True)
    qualify_parser.add_argument("--run-id", default=f"message-path-release-{int(time.time())}")
    qualify_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)

    args = parser.parse_args()
    try:
        policy = load_json(args.policy)
        findings = validate_policy(policy)
        if args.command == "validate-policy":
            if findings:
                raise QualificationError("; ".join(findings))
            print(f"message-path qualification policy passed: sha256={canonical_sha256(policy)}")
            return 0
        if findings:
            raise QualificationError("invalid qualification policy: " + "; ".join(findings))
        if args.command == "compare":
            comparison = compare_reports(
                policy,
                load_json(args.baseline),
                load_json(args.candidate),
                baseline_sha256=f"sha256:{sha256_file(args.baseline)}",
                candidate_sha256=f"sha256:{sha256_file(args.candidate)}",
            )
            write_json(args.output, comparison)
            print(f"message-path comparison {comparison['status']}: {args.output}")
            return 0 if comparison["status"] == "pass" else 1
        if args.command == "qualify":
            report, report_path = run_final_qualification(policy, args)
            print(f"message-path qualification {report['status']}: {report_path}")
            return 0 if report["status"] == "pass" else 1

        validate_target(args.namesrv, args.confirm_target, args.topic, args.durability_contract)
        repetitions = args.repetitions or policy["modes"][args.mode]["minimum_repetitions"]
        if repetitions <= 0:
            raise QualificationError("--repetitions must be positive")
        if args.command == "plan":
            print(json.dumps(build_plan(policy, args.mode, args.namesrv, args.topic, args.run_id, repetitions), indent=2))
            return 0
        report, report_path = run_qualification(policy, args)
        print(f"message-path measurement {report['status']}: {report_path}")
        return 0 if report["status"] == "pass" else 1
    except QualificationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
