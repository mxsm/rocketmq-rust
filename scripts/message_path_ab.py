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

"""Plan, execute, and assemble paired same-target message-path A/B evidence."""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import statistics
import sys
from pathlib import Path
from typing import Any

import message_path_qualification as qualification


ROOT = Path(__file__).resolve().parents[1]
SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")
IMAGE_REFERENCE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


class AbError(RuntimeError):
    """Raised when paired A/B evidence cannot safely continue."""


def digest(path: Path) -> str:
    return f"sha256:{qualification.sha256_file(path)}"


def load_image_map(path: Path, label: str) -> dict[str, str]:
    document = qualification.load_json(path)
    if set(document) != set(SERVICES):
        raise AbError(f"{label} image map must contain exactly {', '.join(SERVICES)}")
    result = {service: str(document[service]) for service in SERVICES}
    for service, reference in result.items():
        if not IMAGE_REFERENCE.fullmatch(reference):
            raise AbError(f"{label} {service} image must use a registry manifest digest")
    return result


def subject_binding(
    provenance: dict[str, Any], role: str, commit: str, image_map_path: Path
) -> dict[str, Any]:
    subject = provenance.get(role)
    if not isinstance(subject, dict) or subject.get("commit") != commit:
        raise AbError(f"{role} provenance commit differs from the requested commit")
    map_hash = digest(image_map_path)
    if subject.get("image_map_sha256") != map_hash:
        raise AbError(f"{role} image map hash differs from provenance")
    deployment_digest = subject.get("deployment_digest")
    if not isinstance(deployment_digest, str) or not qualification.DIGEST_RE.fullmatch(deployment_digest):
        raise AbError(f"{role} deployment digest is invalid")
    return {
        "commit": commit,
        "artifact_manifest_sha256": map_hash,
        "deployment_digest": deployment_digest,
        "image_map_sha256": map_hash,
    }


def alternating_arms(repetitions: int, seed: int, warmup_runs: int) -> list[dict[str, Any]]:
    if repetitions < 5:
        raise AbError("paired release A/B requires at least five repetitions")
    if warmup_runs < 1:
        raise AbError("paired release A/B requires at least one warmup per subject")
    generator = random.Random(seed)
    arms: list[dict[str, Any]] = []
    for phase, count in (("warmup", warmup_runs), ("sample", repetitions)):
        for ordinal in range(1, count + 1):
            pair = ["baseline", "candidate"]
            generator.shuffle(pair)
            for role in pair:
                arms.append(
                    {
                        "index": len(arms),
                        "role": role,
                        "phase": phase,
                        "ordinal": ordinal,
                    }
                )
    return arms


def create_plan(args: argparse.Namespace) -> dict[str, Any]:
    policy = qualification.load_json(args.policy)
    findings = qualification.validate_policy(policy)
    if findings:
        raise AbError("invalid qualification policy: " + "; ".join(findings))
    if not qualification.RUN_ID_RE.fullmatch(args.run_id):
        raise AbError("run ID must contain 3-128 safe characters")
    if not qualification.GIT_SHA_RE.fullmatch(args.baseline_commit):
        raise AbError("baseline commit must be a full lowercase Git SHA")
    if not qualification.GIT_SHA_RE.fullmatch(args.candidate_commit):
        raise AbError("candidate commit must be a full lowercase Git SHA")
    if args.baseline_commit == args.candidate_commit:
        raise AbError("baseline and candidate commits must differ")
    if not qualification.GIT_SHA_RE.fullmatch(args.driver_commit):
        raise AbError("benchmark driver commit must be a full lowercase Git SHA")
    qualification.validate_target(args.namesrv, args.namesrv, args.topic_prefix, args.durability_contract)
    if not args.target_id.strip() or not args.cluster_uid.strip():
        raise AbError("target ID and cluster UID must be explicit")
    if not args.effective_config.is_file():
        raise AbError("effective configuration artifact is missing")

    load_image_map(args.baseline_image_map, "baseline")
    load_image_map(args.candidate_image_map, "candidate")
    provenance = qualification.load_json(args.image_provenance)
    if provenance.get("schema_version") != 1 or provenance.get(
        "artifact_kind"
    ) != "rocketmq_local_evidence_image_provenance":
        raise AbError("image provenance contract is invalid")
    release = policy["modes"]["release"]
    repetitions = args.repetitions or release["minimum_repetitions"]
    subjects = {
        "baseline": subject_binding(provenance, "baseline", args.baseline_commit, args.baseline_image_map),
        "candidate": subject_binding(provenance, "candidate", args.candidate_commit, args.candidate_image_map),
    }
    return {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_ab_plan",
        "run_id": args.run_id,
        "generated_at": qualification.utc_now(),
        "mode": "release",
        "seed": args.seed,
        "warmup_runs_per_subject": release["warmup_runs"],
        "repetitions_per_subject": repetitions,
        "policy_sha256": f"sha256:{qualification.canonical_sha256(policy)}",
        "durability_contract": args.durability_contract,
        "driver": {"commit": args.driver_commit},
        "target": {
            "target_id": args.target_id,
            "cluster_uid": args.cluster_uid,
            "namesrv_addr": args.namesrv,
            "topic_prefix": args.topic_prefix,
            "effective_config_sha256": digest(args.effective_config),
        },
        "subjects": subjects,
        "arms": alternating_arms(repetitions, args.seed, release["warmup_runs"]),
    }


def validate_plan(plan: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if plan.get("schema_version") != 1 or plan.get("artifact_kind") != "rocketmq_message_path_ab_plan":
        failures.append("A/B plan contract is invalid")
        return failures
    if plan.get("policy_sha256") != f"sha256:{qualification.canonical_sha256(policy)}":
        failures.append("A/B plan policy hash differs")
    repetitions = plan.get("repetitions_per_subject")
    warmups = plan.get("warmup_runs_per_subject")
    if not isinstance(repetitions, int) or repetitions < 5:
        failures.append("A/B plan requires at least five repetitions per subject")
    if not isinstance(warmups, int) or warmups < 1:
        failures.append("A/B plan requires at least one warmup per subject")
    arms = plan.get("arms")
    if not isinstance(arms, list):
        failures.append("A/B plan arms are missing")
        return failures
    expected = {(role, "warmup", ordinal) for role in ("baseline", "candidate") for ordinal in range(1, warmups + 1)}
    expected |= {(role, "sample", ordinal) for role in ("baseline", "candidate") for ordinal in range(1, repetitions + 1)}
    actual: set[tuple[Any, Any, Any]] = set()
    for index, arm in enumerate(arms):
        if not isinstance(arm, dict) or arm.get("index") != index:
            failures.append("A/B plan arm indexes must be contiguous")
            continue
        actual.add((arm.get("role"), arm.get("phase"), arm.get("ordinal")))
    if actual != expected or len(arms) != len(expected):
        failures.append("A/B plan does not contain every required arm exactly once")
    if plan.get("subjects", {}).get("baseline", {}).get("commit") == plan.get("subjects", {}).get(
        "candidate", {}
    ).get("commit"):
        failures.append("A/B plan subject commits must differ")
    return failures


def arm_directory(output_root: Path, arm: dict[str, Any]) -> Path:
    return output_root / "arms" / f"{arm['index']:03d}-{arm['role']}-{arm['phase']}-{arm['ordinal']}"


def execute_arm(
    plan: dict[str, Any], plan_path: Path, arm_index: int, output_root: Path, timeout_seconds: int
) -> tuple[dict[str, Any], Path]:
    policy = qualification.load_json(qualification.DEFAULT_POLICY)
    findings = validate_plan(plan, policy)
    if findings:
        raise AbError("; ".join(findings))
    arms = plan["arms"]
    if arm_index < 0 or arm_index >= len(arms):
        raise AbError("arm index is outside the execution plan")
    arm = arms[arm_index]
    run_dir = arm_directory(output_root, arm)
    report_path = run_dir / "arm-report.json"
    if report_path.exists():
        raise AbError(f"arm was already executed and cannot be overwritten: {report_path}")
    run_dir.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    records: list[dict[str, Any]] = []
    topic_prefix = plan["target"]["topic_prefix"]
    for workload in policy["modes"]["release"]["workloads"]:
        topic = f"{topic_prefix}_{arm['role'][0]}{arm['phase'][0]}{arm['ordinal']}_{workload['id'].replace('-', '_')}"
        sample_id = f"{plan['run_id']}-{arm['role']}-{arm['phase']}-{arm['ordinal']}-{workload['id']}"
        raw_path = run_dir / "raw" / f"{workload['id']}.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        command = qualification.benchmark_command(
            workload, plan["target"]["namesrv_addr"], topic, sample_id, raw_path
        )
        result = qualification.execute_command(command, ROOT, timeout_seconds)
        stdout_path = raw_path.with_suffix(".stdout.log")
        stderr_path = raw_path.with_suffix(".stderr.log")
        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")
        record: dict[str, Any] = {
            "workload": workload["id"],
            "command": command,
            "exit_code": result.exit_code,
            "duration_ms": result.duration_ms,
            "stdout_sha256": digest(stdout_path),
            "stderr_sha256": digest(stderr_path),
        }
        if result.exit_code != 0 or not raw_path.is_file():
            failures.append(f"{sample_id} failed with exit code {result.exit_code}")
            records.append(record)
            break
        measurement = qualification.load_json(raw_path)
        sample_findings = qualification.validate_measurement(
            measurement, workload, plan["target"]["namesrv_addr"], topic, sample_id
        )
        record["measurement"] = measurement
        record["raw_sha256"] = digest(raw_path)
        records.append(record)
        if sample_findings:
            failures.extend(f"{sample_id}: {finding}" for finding in sample_findings)
            break
    report = {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_ab_arm",
        "generated_at": qualification.utc_now(),
        "plan_sha256": digest(plan_path),
        "arm": arm,
        "status": "fail" if failures else "pass",
        "failures": failures,
        "records": records,
    }
    qualification.write_json(report_path, report)
    return report, report_path


def normalized_mad_percent(values: list[float]) -> float:
    median = statistics.median(values)
    if median <= 0:
        raise AbError("stability values must have a positive median")
    return statistics.median(abs(value - median) for value in values) / median * 100.0


def subject_artifacts(plan: dict[str, Any], output_root: Path, role: str) -> list[dict[str, str]]:
    artifacts: list[dict[str, str]] = []
    for arm in plan["arms"]:
        if arm["role"] != role:
            continue
        directory = arm_directory(output_root, arm)
        for path in sorted(item for item in directory.rglob("*") if item.is_file()):
            artifacts.append({"path": path.relative_to(output_root).as_posix(), "sha256": digest(path)})
    return artifacts


def paired_bootstrap(baseline: list[float], candidate: list[float], seed: int) -> dict[str, float]:
    if len(baseline) != len(candidate) or len(baseline) < 5:
        raise AbError("paired bootstrap requires at least five matched samples")
    differences = [candidate[index] - baseline[index] for index in range(len(baseline))]
    generator = random.Random(seed)
    estimates = sorted(
        statistics.median(generator.choice(differences) for _ in differences) for _ in range(10_000)
    )
    lower = estimates[math.floor(0.025 * (len(estimates) - 1))]
    upper = estimates[math.floor(0.975 * (len(estimates) - 1))]
    return {
        "median_difference": statistics.median(differences),
        "confidence_95_lower": lower,
        "confidence_95_upper": upper,
    }


def assemble(plan: dict[str, Any], plan_path: Path, output_root: Path) -> tuple[dict[str, Any], Path]:
    policy = qualification.load_json(qualification.DEFAULT_POLICY)
    failures = validate_plan(plan, policy)
    reports: dict[tuple[str, str, int], tuple[dict[str, Any], Path]] = {}
    for arm in plan.get("arms", []):
        path = arm_directory(output_root, arm) / "arm-report.json"
        if not path.is_file():
            failures.append(f"missing arm report: {arm['index']}")
            continue
        report = qualification.load_json(path)
        key = (arm["role"], arm["phase"], arm["ordinal"])
        if report.get("plan_sha256") != digest(plan_path) or report.get("arm") != arm:
            failures.append(f"arm {arm['index']} binding differs from the plan")
        if report.get("status") != "pass" or report.get("failures"):
            failures.append(f"arm {arm['index']} did not pass")
        reports[key] = (report, path)
    if failures:
        raise AbError("; ".join(failures))

    environment = qualification.environment_record()
    measurement_paths: dict[str, Path] = {}
    measurements: dict[str, dict[str, Any]] = {}
    thresholds = policy["stability_thresholds"]
    for role in ("baseline", "candidate"):
        workload_reports: list[dict[str, Any]] = []
        for workload in policy["modes"]["release"]["workloads"]:
            runs: list[dict[str, Any]] = []
            samples: list[dict[str, Any]] = []
            for phase, count in (
                ("warmup", plan["warmup_runs_per_subject"]),
                ("sample", plan["repetitions_per_subject"]),
            ):
                for ordinal in range(1, count + 1):
                    arm_report, arm_path = reports[(role, phase, ordinal)]
                    record = next((item for item in arm_report["records"] if item["workload"] == workload["id"]), None)
                    if not isinstance(record, dict) or "measurement" not in record:
                        raise AbError(f"{role} {phase} {ordinal} is missing {workload['id']}")
                    runs.append(
                        {
                            "phase": phase,
                            "ordinal": ordinal,
                            "measurement": record["measurement"],
                            "arm_report_sha256": digest(arm_path),
                        }
                    )
                    if phase == "sample":
                        samples.append(record["measurement"])
            throughputs = [float(item["result"]["throughput_messages_per_second"]) for item in samples]
            p99s = [float(item["result"]["latency_us"]["p99"]) for item in samples]
            stability = {
                "throughput_normalized_mad_percent": normalized_mad_percent(throughputs),
                "p99_normalized_mad_percent": normalized_mad_percent(p99s),
            }
            stability["status"] = (
                "pass"
                if stability["throughput_normalized_mad_percent"]
                <= thresholds["maximum_throughput_normalized_mad_percent"]
                and stability["p99_normalized_mad_percent"]
                <= thresholds["maximum_p99_latency_normalized_mad_percent"]
                else "fail"
            )
            if stability["status"] != "pass":
                raise AbError(f"{role} {workload['id']} sample stability exceeds the A/B contract")
            workload_reports.append(
                {
                    "id": workload["id"],
                    "parameters": workload,
                    "runs": runs,
                    "aggregate": qualification.aggregate_samples(samples),
                    "stability": stability,
                }
            )
        subject = {"role": role, **plan["subjects"][role]}
        report = {
            "schema_version": 2,
            "artifact_kind": "rocketmq_message_path_measurement_set",
            "run_id": f"{plan['run_id']}-{role}",
            "generated_at": qualification.utc_now(),
            "mode": "release",
            "status": "pass",
            "measurement_qualified": True,
            "business_contract": "java-equivalent-message-semantics",
            "implementation_strategy": "rust-native",
            "durability_contract": plan["durability_contract"],
            "policy_sha256": qualification.canonical_sha256(policy),
            "git": {"commit": plan["driver"]["commit"], "dirty": False},
            "subject": subject,
            "environment": environment,
            "target": {
                "target_id": plan["target"]["target_id"],
                "cluster_uid": plan["target"]["cluster_uid"],
                "namesrv_addr": plan["target"]["namesrv_addr"],
                "topic": plan["target"]["topic_prefix"],
                "effective_config_sha256": plan["target"]["effective_config_sha256"],
            },
            "paired_ab": {"plan_sha256": digest(plan_path), "seed": plan["seed"]},
            "repetitions_per_workload": plan["repetitions_per_subject"],
            "workloads": workload_reports,
            "failures": [],
            "artifacts": subject_artifacts(plan, output_root, role),
        }
        path = output_root / role / "measurement-set.json"
        qualification.write_json(path, report)
        measurement_paths[role] = path
        measurements[role] = report

    comparison = qualification.compare_reports(
        policy,
        measurements["baseline"],
        measurements["candidate"],
        baseline_sha256=digest(measurement_paths["baseline"]),
        candidate_sha256=digest(measurement_paths["candidate"]),
    )
    for item in comparison.get("comparisons", []):
        workload_id = item["workload"]
        baseline_item = next(entry for entry in measurements["baseline"]["workloads"] if entry["id"] == workload_id)
        candidate_item = next(entry for entry in measurements["candidate"]["workloads"] if entry["id"] == workload_id)
        baseline_runs = [entry["measurement"] for entry in baseline_item["runs"] if entry["phase"] == "sample"]
        candidate_runs = [entry["measurement"] for entry in candidate_item["runs"] if entry["phase"] == "sample"]
        item["paired_bootstrap_95_ci"] = {
            "throughput_messages_per_second": paired_bootstrap(
                [float(entry["result"]["throughput_messages_per_second"]) for entry in baseline_runs],
                [float(entry["result"]["throughput_messages_per_second"]) for entry in candidate_runs],
                plan["seed"],
            ),
            "p99_latency_us": paired_bootstrap(
                [float(entry["result"]["latency_us"]["p99"]) for entry in baseline_runs],
                [float(entry["result"]["latency_us"]["p99"]) for entry in candidate_runs],
                plan["seed"] + 1,
            ),
        }
    comparison["ab_plan_sha256"] = digest(plan_path)
    comparison_path = output_root / "performance-comparison.json"
    qualification.write_json(comparison_path, comparison)
    return comparison, comparison_path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--policy", type=Path, default=qualification.DEFAULT_POLICY)
    plan.add_argument("--run-id", required=True)
    plan.add_argument("--baseline-commit", required=True)
    plan.add_argument("--candidate-commit", required=True)
    plan.add_argument("--driver-commit", required=True)
    plan.add_argument("--baseline-image-map", type=Path, required=True)
    plan.add_argument("--candidate-image-map", type=Path, required=True)
    plan.add_argument("--image-provenance", type=Path, required=True)
    plan.add_argument("--effective-config", type=Path, required=True)
    plan.add_argument("--target-id", required=True)
    plan.add_argument("--cluster-uid", required=True)
    plan.add_argument("--namesrv", required=True)
    plan.add_argument("--topic-prefix", required=True)
    plan.add_argument("--durability-contract", required=True)
    plan.add_argument("--repetitions", type=int, default=5)
    plan.add_argument("--seed", type=int, default=20260812)
    plan.add_argument("--output", type=Path, required=True)

    execute = subparsers.add_parser("execute-arm")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--arm-index", type=int, required=True)
    execute.add_argument("--output-root", type=Path, required=True)
    execute.add_argument("--command-timeout-seconds", type=int, default=1800)

    assemble_parser = subparsers.add_parser("assemble")
    assemble_parser.add_argument("--plan", type=Path, required=True)
    assemble_parser.add_argument("--output-root", type=Path, required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--plan", type=Path, required=True)
    validate.add_argument("--output-root", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "plan":
            plan = create_plan(args)
            qualification.write_json(args.output, plan)
            print(f"message-path A/B plan ready: {args.output}")
            return 0
        plan = qualification.load_json(args.plan)
        if args.command == "execute-arm":
            report, path = execute_arm(plan, args.plan, args.arm_index, args.output_root, args.command_timeout_seconds)
            print(f"message-path A/B arm {report['status']}: {path}")
            return 0 if report["status"] == "pass" else 1
        if args.command == "assemble":
            comparison, path = assemble(plan, args.plan, args.output_root)
            print(f"message-path A/B comparison {comparison['status']}: {path}")
            return 0 if comparison["status"] == "pass" else 1
        findings = validate_plan(plan, qualification.load_json(qualification.DEFAULT_POLICY))
        if findings:
            raise AbError("; ".join(findings))
        if args.output_root:
            comparison = qualification.load_json(args.output_root / "performance-comparison.json")
            if comparison.get("status") != "pass" or comparison.get("ab_plan_sha256") != digest(args.plan):
                raise AbError("A/B comparison is not a passing result bound to this plan")
        print("MESSAGE_PATH_AB_VALID")
        return 0
    except (AbError, qualification.QualificationError, OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
