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

"""Validate M10 benchmark reports and enforce the architecture performance gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILES = ROOT / "scripts" / "architecture-performance-profiles.json"
DEFAULT_EXCEPTIONS = ROOT / "scripts" / "architecture-performance-exceptions.json"
REQUIRED_PROFILE_IDS = {
    "local-append",
    "sync-flush",
    "local-pull",
    "rocks-pull",
    "tiered-append",
    "tiered-pull",
    "connection-soak",
    "overload",
}
REQUIRED_CORE_METRICS = {
    "throughput_per_second",
    "p99_latency_us",
    "peak_rss_bytes",
    "allocations_per_operation",
    "io_amplification_ratio",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_timestamp(value: Any, field: str, findings: list[str]) -> datetime | None:
    if not isinstance(value, str) or not value:
        findings.append(f"{field} must be a non-empty RFC3339 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        findings.append(f"{field} is not a valid RFC3339 timestamp: {value}")
        return None
    if parsed.tzinfo is None:
        findings.append(f"{field} must include a timezone: {value}")
        return None
    return parsed.astimezone(timezone.utc)


def is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def expanded_metrics(policy: dict[str, Any], profile: dict[str, Any]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    sets = policy.get("metric_sets", {})
    for set_name in profile.get("metric_sets", []):
        metric_set = sets.get(set_name, [])
        if isinstance(metric_set, list):
            metrics.extend(deepcopy(metric_set))
    own_metrics = profile.get("metrics", [])
    if isinstance(own_metrics, list):
        metrics.extend(deepcopy(own_metrics))
    return metrics


def validate_metric(metric: Any, context: str, policy: dict[str, Any], findings: list[str]) -> None:
    if not isinstance(metric, dict):
        findings.append(f"{context} metric must be an object")
        return
    metric_id = metric.get("id")
    if not isinstance(metric_id, str) or not metric_id:
        findings.append(f"{context} metric id must be non-empty")
    if metric.get("direction") not in {"higher", "lower"}:
        findings.append(f"{context}/{metric_id} direction must be higher or lower")
    if not isinstance(metric.get("unit"), str) or not metric.get("unit"):
        findings.append(f"{context}/{metric_id} unit must be non-empty")
    comparison = metric.get("comparison")
    if comparison not in {"relative", "absolute-observation"}:
        findings.append(f"{context}/{metric_id} comparison is invalid: {comparison}")
    if comparison == "relative":
        budget = metric.get("max_regression_percent", policy.get("default_max_regression_percent"))
        if not is_finite_number(budget) or not 0 <= float(budget) <= float(
            policy.get("default_max_regression_percent", -1)
        ):
            findings.append(f"{context}/{metric_id} regression budget must be within the approved default")


def validate_hypothesis(
    hypothesis: Any,
    metric_ids: set[str],
    variant_ids: set[str],
    context: str,
    findings: list[str],
) -> None:
    if not isinstance(hypothesis, dict):
        findings.append(f"{context} hypothesis must be an object")
        return
    metric = hypothesis.get("metric")
    if metric not in metric_ids:
        findings.append(f"{context} hypothesis references unknown metric: {metric}")
    if hypothesis.get("operator") not in {"max", "min", "min_improvement_percent"}:
        findings.append(f"{context}/{metric} hypothesis operator is invalid")
    if not is_finite_number(hypothesis.get("value")):
        findings.append(f"{context}/{metric} hypothesis value must be finite")
    if hypothesis.get("gate") is not False:
        findings.append(f"{context}/{metric} hypothesis must declare gate=false")
    selected_variants = hypothesis.get("variants")
    if selected_variants is not None:
        if (
            not isinstance(selected_variants, list)
            or not selected_variants
            or len(selected_variants) != len(set(selected_variants))
            or not set(selected_variants).issubset(variant_ids)
        ):
            findings.append(f"{context}/{metric} hypothesis variants are invalid")


def validate_profile_policy(policy: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if policy.get("schema_version") != 1 or policy.get("milestone") != "M10-05":
        findings.append("performance profile schema or milestone is invalid")
    default_budget = policy.get("default_max_regression_percent")
    if not is_finite_number(default_budget) or float(default_budget) != 5.0:
        findings.append("default performance regression budget must remain exactly 5%")
    minimum_samples = policy.get("minimum_samples")
    if not isinstance(minimum_samples, int) or isinstance(minimum_samples, bool) or minimum_samples < 5:
        findings.append("minimum_samples must be an integer of at least five")
    max_noise = policy.get("max_relative_mad_percent")
    if not is_finite_number(max_noise) or float(max_noise) <= 0:
        findings.append("max_relative_mad_percent must be positive")
    max_deviation = policy.get("max_sample_deviation_percent")
    noise_value = float(max_noise) if is_finite_number(max_noise) else 0.0
    if not is_finite_number(max_deviation) or float(max_deviation) <= noise_value:
        findings.append("max_sample_deviation_percent must exceed the relative MAD limit")

    environment_fields = policy.get("required_environment_fields")
    if not isinstance(environment_fields, list) or len(environment_fields) != len(set(environment_fields)):
        findings.append("required_environment_fields must be a unique list")
    correctness_checks = policy.get("required_correctness_checks")
    if not isinstance(correctness_checks, list) or len(correctness_checks) < 4:
        findings.append("at least four correctness-first checks are required")

    metric_sets = policy.get("metric_sets")
    if not isinstance(metric_sets, dict) or not isinstance(metric_sets.get("core"), list):
        findings.append("metric_sets.core is required")
        core_metrics: list[dict[str, Any]] = []
    else:
        core_metrics = metric_sets["core"]
    core_ids = {metric.get("id") for metric in core_metrics if isinstance(metric, dict)}
    if core_ids != REQUIRED_CORE_METRICS:
        findings.append(f"core metric set mismatch: expected={sorted(REQUIRED_CORE_METRICS)}, actual={sorted(core_ids)}")
    for metric in core_metrics:
        validate_metric(metric, "metric_sets/core", policy, findings)

    profiles = policy.get("profiles")
    if not isinstance(profiles, list):
        findings.append("profiles must be a list")
        return findings
    profile_ids = [profile.get("id") for profile in profiles if isinstance(profile, dict)]
    if set(profile_ids) != REQUIRED_PROFILE_IDS or len(profile_ids) != len(REQUIRED_PROFILE_IDS):
        findings.append(f"profile inventory mismatch: expected={sorted(REQUIRED_PROFILE_IDS)}, actual={sorted(profile_ids)}")
    for profile in profiles:
        if not isinstance(profile, dict):
            findings.append("profile entry must be an object")
            continue
        profile_id = profile.get("id", "<missing>")
        context = f"profile/{profile_id}"
        if not isinstance(profile.get("description"), str) or not profile.get("description"):
            findings.append(f"{context} description must be non-empty")
        collection = profile.get("collection")
        if not isinstance(collection, dict):
            findings.append(f"{context} collection must be an object")
        else:
            if collection.get("driver") != "target-hardware-sidecar":
                findings.append(f"{context} collection driver must be target-hardware-sidecar")
            if collection.get("status") != "requires-target-hardware-run":
                findings.append(f"{context} collection status must preserve the target-hardware requirement")
            reference_commands = collection.get("reference_commands")
            if not isinstance(reference_commands, list) or any(
                not isinstance(command, str) or not command for command in reference_commands
            ):
                findings.append(f"{context} collection reference_commands must be a string list")
            required_outputs = collection.get("required_outputs")
            expected_outputs = {
                "environment_metadata",
                "correctness_evidence",
                "raw_samples",
                "profile_report",
            }
            if not isinstance(required_outputs, list) or set(required_outputs) != expected_outputs:
                findings.append(f"{context} collection required_outputs must preserve the evidence contract")
        metrics = expanded_metrics(policy, profile)
        metric_ids = [metric.get("id") for metric in metrics if isinstance(metric, dict)]
        if len(metric_ids) != len(set(metric_ids)):
            findings.append(f"{context} metric ids must be unique")
        if not REQUIRED_CORE_METRICS.issubset(metric_ids):
            findings.append(f"{context} must include every core metric")
        for metric in profile.get("metrics", []):
            validate_metric(metric, context, policy, findings)
        variants = profile.get("variants")
        variant_ids: list[str] = []
        if not isinstance(variants, list) or not variants:
            findings.append(f"{context} variants must be a non-empty list")
        else:
            variant_ids = [variant.get("id") for variant in variants if isinstance(variant, dict)]
            if len(variant_ids) != len(set(variant_ids)) or any(not value for value in variant_ids):
                findings.append(f"{context} variant ids must be unique and non-empty")
            for variant in variants:
                if not isinstance(variant, dict) or not isinstance(variant.get("parameters"), dict):
                    findings.append(f"{context} variant parameters must be an object")
        for hypothesis in profile.get("hypotheses", []):
            validate_hypothesis(hypothesis, set(metric_ids), set(variant_ids), context, findings)
    return findings


def policy_maps(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {profile["id"]: profile for profile in policy["profiles"]}


def list_map(items: Any, key: str, context: str, findings: list[str]) -> dict[str, Any]:
    if not isinstance(items, list):
        findings.append(f"{context} must be a list")
        return {}
    result: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get(key), str) or not item.get(key):
            findings.append(f"{context} entries require a non-empty {key}")
            continue
        identity = item[key]
        if identity in result:
            findings.append(f"{context} contains duplicate {key}: {identity}")
        result[identity] = item
    return result


def validate_report(report: dict[str, Any], policy: dict[str, Any], name: str, allow_fixture: bool) -> list[str]:
    findings: list[str] = []
    if report.get("schema_version") != 1:
        findings.append(f"{name} schema_version must be 1")
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,127}", run_id):
        findings.append(f"{name}.run_id must be a stable 3-128 character identifier")
    report_kind = report.get("report_kind")
    if report_kind not in {"measurement", "fixture"}:
        findings.append(f"{name} report_kind must be measurement or fixture")
    if report_kind == "fixture" and not allow_fixture:
        findings.append(f"{name} fixture reports require --allow-fixture")
    expected_policy_hash = canonical_sha256(policy)
    if report.get("profile_schema_sha256") != expected_policy_hash:
        findings.append(f"{name} profile_schema_sha256 does not match the selected profile policy")
    parse_timestamp(report.get("generated_at"), f"{name}.generated_at", findings)

    git = report.get("git")
    if not isinstance(git, dict) or not COMMIT_RE.fullmatch(str(git.get("commit", ""))):
        findings.append(f"{name}.git.commit must be a full lowercase SHA-1")
    if not isinstance(git, dict) or not isinstance(git.get("dirty"), bool):
        findings.append(f"{name}.git.dirty must be boolean")
    elif report_kind == "measurement" and git["dirty"]:
        findings.append(f"{name} measurement must be captured from a clean Git tree")

    environment = report.get("environment")
    if not isinstance(environment, dict):
        findings.append(f"{name}.environment must be an object")
        environment = {}
    required_environment_fields = policy.get("required_environment_fields", [])
    for field in required_environment_fields:
        value = environment.get(field)
        if value is None or value == "" or value == []:
            findings.append(f"{name}.environment.{field} is required")
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
        if field in required_environment_fields and (
            not isinstance(environment.get(field), str) or not environment.get(field)
        ):
            findings.append(f"{name}.environment.{field} must be a non-empty string")
    for field in ("logical_cpus", "memory_bytes"):
        value = environment.get(field)
        if field in required_environment_fields and (
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
        ):
            findings.append(f"{name}.environment.{field} must be a positive integer")
    features = environment.get("features")
    if not isinstance(features, list) or not features or any(not isinstance(item, str) or not item for item in features):
        findings.append(f"{name}.environment.features must be a non-empty string list")
    elif features != sorted(set(features)):
        findings.append(f"{name}.environment.features must be sorted and unique")

    required_checks = set(policy.get("required_correctness_checks", []))
    checks = list_map(report.get("correctness_checks"), "id", f"{name}.correctness_checks", findings)
    if set(checks) != required_checks:
        findings.append(
            f"{name} correctness check inventory mismatch: expected={sorted(required_checks)}, actual={sorted(checks)}"
        )
    for check_id, check in checks.items():
        if check.get("status") != "pass":
            findings.append(f"{name} correctness-first check did not pass: {check_id}")
        if not SHA256_RE.fullmatch(str(check.get("evidence_sha256", ""))):
            findings.append(f"{name} correctness check lacks an evidence SHA-256: {check_id}")

    expected_profiles = policy_maps(policy)
    profiles = list_map(report.get("profiles"), "id", f"{name}.profiles", findings)
    if set(profiles) != set(expected_profiles):
        findings.append(
            f"{name} profile inventory mismatch: expected={sorted(expected_profiles)}, actual={sorted(profiles)}"
        )
    minimum_samples = policy.get("minimum_samples", 5)
    for profile_id, policy_profile in expected_profiles.items():
        report_profile = profiles.get(profile_id)
        if not isinstance(report_profile, dict):
            continue
        if report_profile.get("measurement_status") != "measured":
            findings.append(f"{name}/{profile_id} must be measured")
        variants = list_map(report_profile.get("variants"), "id", f"{name}/{profile_id}.variants", findings)
        expected_variants = {variant["id"]: variant for variant in policy_profile["variants"]}
        if set(variants) != set(expected_variants):
            findings.append(
                f"{name}/{profile_id} variant inventory mismatch: expected={sorted(expected_variants)}, actual={sorted(variants)}"
            )
        metric_contracts = {metric["id"]: metric for metric in expanded_metrics(policy, policy_profile)}
        for variant_id, policy_variant in expected_variants.items():
            variant = variants.get(variant_id)
            if not isinstance(variant, dict):
                continue
            if variant.get("parameters") != policy_variant.get("parameters"):
                findings.append(f"{name}/{profile_id}/{variant_id} parameters differ from the frozen profile")
            raw_data = variant.get("raw_data")
            if not isinstance(raw_data, dict) or not isinstance(raw_data.get("path"), str) or not raw_data.get("path"):
                findings.append(f"{name}/{profile_id}/{variant_id} raw_data.path is required")
            if not isinstance(raw_data, dict) or not SHA256_RE.fullmatch(str(raw_data.get("sha256", ""))):
                findings.append(f"{name}/{profile_id}/{variant_id} raw_data.sha256 is invalid")
            metrics = variant.get("metrics")
            if not isinstance(metrics, dict):
                findings.append(f"{name}/{profile_id}/{variant_id}.metrics must be an object")
                continue
            if set(metrics) != set(metric_contracts):
                findings.append(
                    f"{name}/{profile_id}/{variant_id} metric inventory mismatch: "
                    f"expected={sorted(metric_contracts)}, actual={sorted(metrics)}"
                )
            for metric_id, contract in metric_contracts.items():
                measurement = metrics.get(metric_id)
                samples = measurement.get("samples") if isinstance(measurement, dict) else None
                if not isinstance(samples, list) or len(samples) < minimum_samples:
                    findings.append(
                        f"{name}/{profile_id}/{variant_id}/{metric_id} requires at least {minimum_samples} samples"
                    )
                    continue
                if any(not is_finite_number(sample) for sample in samples):
                    findings.append(f"{name}/{profile_id}/{variant_id}/{metric_id} contains non-finite samples")
                    continue
                numeric = [float(sample) for sample in samples]
                if any(sample < 0 for sample in numeric):
                    findings.append(f"{name}/{profile_id}/{variant_id}/{metric_id} contains negative samples")
                if contract.get("comparison") == "relative" and any(sample <= 0 for sample in numeric):
                    findings.append(f"{name}/{profile_id}/{variant_id}/{metric_id} relative samples must be positive")
    return findings


def environment_fingerprint(report: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    environment = report["environment"]
    return {field: environment[field] for field in policy["required_environment_fields"]}


def sample_stats(samples: list[Any]) -> dict[str, float]:
    numeric = [float(value) for value in samples]
    median = statistics.median(numeric)
    mad = statistics.median(abs(value - median) for value in numeric)
    relative_mad = 0.0 if median == 0 and mad == 0 else (math.inf if median == 0 else mad / abs(median) * 100.0)
    max_deviation = (
        0.0
        if median == 0 and all(value == 0 for value in numeric)
        else (
            math.inf
            if median == 0
            else max(abs(value - median) for value in numeric) / abs(median) * 100.0
        )
    )
    return {
        "median": round(median, 8),
        "mad": round(mad, 8),
        "relative_mad_percent": round(relative_mad, 8),
        "max_sample_deviation_percent": round(max_deviation, 8),
        "minimum": round(min(numeric), 8),
        "maximum": round(max(numeric), 8),
        "samples": len(numeric),
    }


def regression_percent(direction: str, baseline: float, candidate: float) -> float:
    if direction == "lower":
        return (candidate - baseline) / baseline * 100.0
    return (baseline - candidate) / baseline * 100.0


def active_exception_map(
    exception_document: dict[str, Any],
    now: datetime,
    findings: list[str],
) -> dict[tuple[str, str, str], dict[str, Any]]:
    if exception_document.get("schema_version") != 1:
        findings.append("performance exception schema_version must be 1")
    items = exception_document.get("exceptions")
    if not isinstance(items, list):
        findings.append("performance exceptions must be a list")
        return {}
    result: dict[tuple[str, str, str], dict[str, Any]] = {}
    for index, item in enumerate(items):
        context = f"exception[{index}]"
        if not isinstance(item, dict):
            findings.append(f"{context} must be an object")
            continue
        key = (str(item.get("profile", "")), str(item.get("variant", "")), str(item.get("metric", "")))
        if not all(key):
            findings.append(f"{context} requires profile, variant, and metric")
            continue
        for field in ("owner", "reason", "approved_by", "rollback_config"):
            if not isinstance(item.get(field), str) or not item.get(field):
                findings.append(f"{context}.{field} must be non-empty")
        allowed = item.get("allowed_regression_percent")
        if not is_finite_number(allowed) or not 5.0 < float(allowed) <= 100.0:
            findings.append(f"{context}.allowed_regression_percent must be >5 and <=100")
        expires = parse_timestamp(item.get("expires_at"), f"{context}.expires_at", findings)
        if expires is not None and expires <= now:
            findings.append(f"{context} expired at {item.get('expires_at')}")
        if key in result:
            findings.append(f"duplicate performance exception: {'/'.join(key)}")
        result[key] = item
    return result


def validate_exception_contracts(
    policy: dict[str, Any],
    exceptions: dict[tuple[str, str, str], dict[str, Any]],
    findings: list[str],
) -> None:
    profile_contracts = policy_maps(policy)
    for profile_id, variant_id, metric_id in exceptions:
        profile = profile_contracts.get(profile_id)
        if profile is None:
            findings.append(f"performance exception references unknown profile: {profile_id}")
            continue
        variants = {variant["id"] for variant in profile["variants"]}
        contracts = {metric["id"]: metric for metric in expanded_metrics(policy, profile)}
        if variant_id not in variants or metric_id not in contracts:
            findings.append(
                f"performance exception references unknown contract: {profile_id}/{variant_id}/{metric_id}"
            )
            continue
        if contracts[metric_id]["comparison"] != "relative":
            findings.append(
                f"performance exception cannot target a non-regression observation: "
                f"{profile_id}/{variant_id}/{metric_id}"
            )


def hypothesis_result(hypothesis: dict[str, Any], candidate: float, regression: float | None) -> dict[str, Any]:
    operator = hypothesis["operator"]
    target = float(hypothesis["value"])
    if operator == "max":
        observed = candidate
        met = candidate <= target
    elif operator == "min":
        observed = candidate
        met = candidate >= target
    else:
        observed = -(regression or 0.0)
        met = observed >= target
    return {
        "metric": hypothesis["metric"],
        "operator": operator,
        "target": target,
        "observed": round(observed, 8),
        "status": "met" if met else "missed",
        "gate": False,
    }


def evaluate_documents(
    policy: dict[str, Any],
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    exception_document: dict[str, Any],
    *,
    allow_fixture: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    findings = validate_profile_policy(policy)
    findings.extend(validate_report(baseline, policy, "baseline", allow_fixture))
    findings.extend(validate_report(candidate, policy, "candidate", allow_fixture))
    effective_now = now or datetime.now(timezone.utc)
    exceptions = active_exception_map(exception_document, effective_now, findings)
    validate_exception_contracts(policy, exceptions, findings)
    if findings:
        return {"schema_version": 1, "status": "fail", "failures": findings, "comparisons": [], "hypotheses": []}

    baseline_environment = environment_fingerprint(baseline, policy)
    candidate_environment = environment_fingerprint(candidate, policy)
    if baseline_environment != candidate_environment:
        return {
            "schema_version": 1,
            "status": "fail",
            "failures": ["baseline and candidate environments differ from the frozen profile metadata"],
            "comparisons": [],
            "hypotheses": [],
        }
    baseline_time = datetime.fromisoformat(baseline["generated_at"].replace("Z", "+00:00"))
    candidate_time = datetime.fromisoformat(candidate["generated_at"].replace("Z", "+00:00"))
    if candidate_time < baseline_time:
        return {
            "schema_version": 1,
            "status": "fail",
            "failures": ["candidate measurement predates the baseline measurement"],
            "comparisons": [],
            "hypotheses": [],
        }
    if (
        baseline.get("report_kind") == "measurement"
        and candidate.get("report_kind") == "measurement"
        and baseline["git"]["commit"] == candidate["git"]["commit"]
    ):
        return {
            "schema_version": 1,
            "status": "fail",
            "failures": ["baseline and candidate measurements must bind different Git commits"],
            "comparisons": [],
            "hypotheses": [],
        }

    failures: list[str] = []
    warnings: list[str] = []
    comparisons: list[dict[str, Any]] = []
    hypotheses: list[dict[str, Any]] = []
    used_exceptions: list[dict[str, Any]] = []
    baseline_profiles = {profile["id"]: profile for profile in baseline["profiles"]}
    candidate_profiles = {profile["id"]: profile for profile in candidate["profiles"]}
    for policy_profile in policy["profiles"]:
        profile_id = policy_profile["id"]
        baseline_variants = {variant["id"]: variant for variant in baseline_profiles[profile_id]["variants"]}
        candidate_variants = {variant["id"]: variant for variant in candidate_profiles[profile_id]["variants"]}
        contracts = {metric["id"]: metric for metric in expanded_metrics(policy, policy_profile)}
        for policy_variant in policy_profile["variants"]:
            variant_id = policy_variant["id"]
            metric_results: dict[str, dict[str, Any]] = {}
            for metric_id, contract in contracts.items():
                baseline_stats = sample_stats(baseline_variants[variant_id]["metrics"][metric_id]["samples"])
                candidate_stats = sample_stats(candidate_variants[variant_id]["metrics"][metric_id]["samples"])
                max_noise = float(policy["max_relative_mad_percent"])
                max_deviation = float(policy["max_sample_deviation_percent"])
                noisy = (
                    baseline_stats["relative_mad_percent"] > max_noise
                    or candidate_stats["relative_mad_percent"] > max_noise
                    or baseline_stats["max_sample_deviation_percent"] > max_deviation
                    or candidate_stats["max_sample_deviation_percent"] > max_deviation
                )
                comparison: dict[str, Any] = {
                    "profile": profile_id,
                    "variant": variant_id,
                    "metric": metric_id,
                    "unit": contract["unit"],
                    "direction": contract["direction"],
                    "comparison": contract["comparison"],
                    "baseline": baseline_stats,
                    "candidate": candidate_stats,
                }
                regression: float | None = None
                if noisy:
                    comparison["status"] = "fail-noise"
                    failures.append(
                        f"{profile_id}/{variant_id}/{metric_id} exceeds the noise stability limits "
                        f"(MAD {max_noise}%, sample deviation {max_deviation}%)"
                    )
                elif contract["comparison"] == "relative":
                    regression = regression_percent(
                        contract["direction"], baseline_stats["median"], candidate_stats["median"]
                    )
                    regression = round(regression, 8)
                    budget = float(contract.get("max_regression_percent", policy["default_max_regression_percent"]))
                    comparison["regression_percent"] = regression
                    comparison["budget_percent"] = budget
                    exception = exceptions.get((profile_id, variant_id, metric_id))
                    if regression > budget:
                        if exception is not None and regression <= float(exception["allowed_regression_percent"]):
                            comparison["status"] = "approved-exception"
                            used_exceptions.append(exception)
                        else:
                            comparison["status"] = "fail-regression"
                            failures.append(
                                f"{profile_id}/{variant_id}/{metric_id} regressed {regression:.4f}% "
                                f"beyond the {budget:.4f}% budget"
                            )
                    elif regression < 0:
                        comparison["status"] = "improved"
                    else:
                        comparison["status"] = "pass"
                else:
                    comparison["status"] = "observed"
                comparisons.append(comparison)
                metric_results[metric_id] = {"candidate": candidate_stats["median"], "regression": regression}

            for hypothesis in policy_profile.get("hypotheses", []):
                selected_variants = hypothesis.get("variants")
                if selected_variants is not None and variant_id not in selected_variants:
                    continue
                measured = metric_results[hypothesis["metric"]]
                result = hypothesis_result(hypothesis, measured["candidate"], measured["regression"])
                result.update({"profile": profile_id, "variant": variant_id})
                hypotheses.append(result)
                if result["status"] == "missed":
                    warnings.append(
                        f"hypothesis missed (non-gating): {profile_id}/{variant_id}/{hypothesis['metric']}"
                    )

    unused_exceptions = [exception for exception in exceptions.values() if exception not in used_exceptions]
    for exception in unused_exceptions:
        warnings.append(
            "unused performance exception: "
            f"{exception['profile']}/{exception['variant']}/{exception['metric']}"
        )

    return {
        "schema_version": 1,
        "status": "fail" if failures else "pass",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "policy_sha256": canonical_sha256(policy),
        "environment_sha256": canonical_sha256(baseline_environment),
        "failures": failures,
        "warnings": warnings,
        "comparisons": comparisons,
        "hypotheses": hypotheses,
        "approved_exceptions": used_exceptions,
    }


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", type=Path, default=DEFAULT_PROFILES)
    parser.add_argument("--exceptions", type=Path, default=DEFAULT_EXCEPTIONS)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--validate-profiles", action="store_true")
    parser.add_argument(
        "--allow-fixture",
        action="store_true",
        help="allow synthetic fixture reports; never use for production acceptance",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        policy = load_json(args.profiles)
        exception_document = load_json(args.exceptions)
    except ValueError as error:
        print(f"ARCHITECTURE_PERFORMANCE_GUARD_FAILED {error}", file=sys.stderr)
        return 1

    if args.validate_profiles:
        findings = validate_profile_policy(policy)
        exceptions = active_exception_map(exception_document, datetime.now(timezone.utc), findings)
        if not findings:
            validate_exception_contracts(policy, exceptions, findings)
        if findings:
            for finding in findings:
                print(f"ARCHITECTURE_PERFORMANCE_PROFILE_FINDING {finding}", file=sys.stderr)
            return 1
        variant_count = sum(len(profile["variants"]) for profile in policy["profiles"])
        metric_count = sum(len(expanded_metrics(policy, profile)) for profile in policy["profiles"])
        print(
            "ARCHITECTURE_PERFORMANCE_PROFILES_OK "
            f"profiles={len(policy['profiles'])} variants={variant_count} metric_contracts={metric_count}"
        )
        return 0

    if args.baseline is None or args.candidate is None:
        print("ARCHITECTURE_PERFORMANCE_GUARD_FAILED --baseline and --candidate are required", file=sys.stderr)
        return 1
    try:
        baseline = load_json(args.baseline)
        candidate = load_json(args.candidate)
    except ValueError as error:
        print(f"ARCHITECTURE_PERFORMANCE_GUARD_FAILED {error}", file=sys.stderr)
        return 1
    result = evaluate_documents(
        policy,
        baseline,
        candidate,
        exception_document,
        allow_fixture=args.allow_fixture,
    )
    result["baseline_sha256"] = file_sha256(args.baseline)
    result["candidate_sha256"] = file_sha256(args.candidate)
    if args.output is not None:
        write_json(args.output, result)
    print(
        "ARCHITECTURE_PERFORMANCE_GUARD_"
        f"{result['status'].upper()} comparisons={len(result['comparisons'])} "
        f"failures={len(result['failures'])} hypotheses={len(result['hypotheses'])}"
    )
    for failure in result["failures"]:
        print(f"PERFORMANCE_FINDING {failure}", file=sys.stderr)
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
