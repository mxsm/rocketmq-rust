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

"""Analyze fail-closed message-path resource-soak evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "scripts" / "message-path-soak-policy.json"
HEX64 = set("0123456789abcdef")


class SoakError(RuntimeError):
    """Raised when evidence is incomplete or violates its contract."""


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SoakError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise SoakError(f"JSON root must be an object: {path}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def is_sha256(value: object) -> bool:
    if not isinstance(value, str):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(char in HEX64 for char in digest)


def finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def validate_policy(policy: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if policy.get("schema_version") != 1 or policy.get("artifact_kind") != "rocketmq_message_path_soak_policy":
        findings.append("policy schema_version/artifact_kind is invalid")
    profiles = policy.get("profiles")
    if not isinstance(profiles, dict) or set(profiles) != {"smoke", "full"}:
        findings.append("policy profiles must contain exactly smoke and full")
    else:
        for name, profile in profiles.items():
            if not isinstance(profile, dict):
                findings.append(f"profile {name} must be an object")
                continue
            for key in ("observation_seconds", "sample_interval_seconds", "maximum_gap_seconds"):
                if not isinstance(profile.get(key), int) or profile[key] <= 0:
                    findings.append(f"profile {name}.{key} must be a positive integer")
            for key in ("warmup_seconds", "cooldown_seconds"):
                if not isinstance(profile.get(key), int) or profile[key] < 0:
                    findings.append(f"profile {name}.{key} must be a non-negative integer")
            coverage = profile.get("minimum_coverage_percent")
            if not finite_number(coverage) or not 0 < float(coverage) <= 100:
                findings.append(f"profile {name}.minimum_coverage_percent must be in (0,100]")
    if isinstance(profiles, dict) and isinstance(profiles.get("full"), dict):
        full = profiles["full"]
        if full.get("warmup_seconds", 0) < 1800 or full.get("observation_seconds", 0) < 21600:
            findings.append("full profile must retain at least 30m warmup and 6h observation")
        if full.get("cooldown_seconds", 0) < 900:
            findings.append("full profile must retain at least 15m cooldown")
    required = policy.get("required_series")
    if not isinstance(required, list) or not required:
        findings.append("required_series must be a non-empty array")
    else:
        names: set[str] = set()
        for rule in required:
            if not isinstance(rule, dict) or not isinstance(rule.get("metric"), str):
                findings.append("every required_series rule needs a metric")
                continue
            if rule["metric"] in names:
                findings.append(f"required metric is duplicated: {rule['metric']}")
            names.add(rule["metric"])
            if rule.get("kind") not in {"rss", "task", "fd", "queue", "cache", "lag", "renewal"}:
                findings.append(f"required metric {rule['metric']} has an unknown kind")
            if not isinstance(rule.get("minimum_scopes"), int) or rule["minimum_scopes"] <= 0:
                findings.append(f"required metric {rule['metric']} minimum_scopes must be positive")
    return findings


def load_samples(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    samples: list[dict[str, Any]] = []
    pod_observations: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise SoakError(f"cannot read sample file {path}: {error}") from error
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise SoakError(f"invalid NDJSON at {path}:{line_number}: {error}") from error
        if not isinstance(record, dict) or not finite_number(record.get("timestamp")):
            raise SoakError(f"sample {line_number} must contain a finite timestamp")
        if "pod" in record:
            pod = record["pod"]
            if not isinstance(pod, dict):
                raise SoakError(f"pod sample {line_number} must contain an object")
            pod_observations.append({"timestamp": float(record["timestamp"]), **pod})
            continue
        metric = record.get("metric")
        scope = record.get("scope")
        value = record.get("value")
        if not isinstance(metric, str) or not metric or not isinstance(scope, str) or not scope:
            raise SoakError(f"metric sample {line_number} needs non-empty metric and scope")
        if not finite_number(value) or float(value) < 0:
            raise SoakError(f"metric sample {line_number} value must be finite and non-negative")
        samples.append(
            {
                "timestamp": float(record["timestamp"]),
                "metric": metric,
                "scope": scope,
                "value": float(value),
            }
        )
    if not samples:
        raise SoakError("sample file contains no metric samples")
    return samples, pod_observations


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        raise SoakError("cannot calculate a percentile over no values")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(ratio * len(ordered)) - 1))
    return ordered[index]


def theil_sen_slope_per_hour(points: list[tuple[float, float]]) -> float:
    slopes: list[float] = []
    for index, (left_time, left_value) in enumerate(points):
        for right_time, right_value in points[index + 1 :]:
            delta = right_time - left_time
            if delta > 0:
                slopes.append((right_value - left_value) / delta * 3600.0)
    return statistics.median(slopes) if slopes else 0.0


def kendall_tau(points: list[tuple[float, float]]) -> float:
    concordant = 0
    discordant = 0
    for index, (_, left_value) in enumerate(points):
        for _, right_value in points[index + 1 :]:
            if right_value > left_value:
                concordant += 1
            elif right_value < left_value:
                discordant += 1
    pairs = concordant + discordant
    return (concordant - discordant) / pairs if pairs else 0.0


def maximum_gap(points: list[tuple[float, float]]) -> float:
    ordered = sorted(timestamp for timestamp, _ in points)
    return max((right - left for left, right in zip(ordered, ordered[1:])), default=0.0)


def longest_above(points: list[tuple[float, float]], threshold: float) -> float:
    started: float | None = None
    longest = 0.0
    previous = 0.0
    for timestamp, value in points:
        if value > threshold:
            started = timestamp if started is None else started
            previous = timestamp
        elif started is not None:
            longest = max(longest, previous - started)
            started = None
    if started is not None:
        longest = max(longest, previous - started)
    return longest


def pair_capacity_metric(rule: dict[str, Any], groups: dict[tuple[str, str], list[tuple[float, float]]], scope: str) -> float | None:
    capacity_metric = rule.get("capacity_metric")
    if not capacity_metric:
        return None
    points = groups.get((capacity_metric, scope), [])
    if not points:
        return None
    capacities = {value for _, value in points}
    if len(capacities) != 1:
        raise SoakError(f"capacity changed during the run: {capacity_metric}/{scope}")
    return capacities.pop()


def analyze_series(
    metric: str,
    scope: str,
    kind: str,
    points: list[tuple[float, float]],
    capacity: float | None,
    observation_start: float,
    observation_end: float,
    cooldown_end: float,
    comparison_seconds: int,
    thresholds: dict[str, Any],
    raw_digest: str,
) -> dict[str, Any]:
    observed = [(timestamp, value) for timestamp, value in sorted(points) if observation_start <= timestamp <= observation_end]
    if not observed:
        raise SoakError(f"series has no observation samples: {metric}/{scope}")
    first_values = [value for timestamp, value in observed if timestamp <= observation_start + comparison_seconds]
    last_values = [value for timestamp, value in observed if timestamp >= observation_end - comparison_seconds]
    if not first_values or not last_values:
        raise SoakError(f"series lacks first/last comparison windows: {metric}/{scope}")
    values = [value for _, value in observed]
    first_median = statistics.median(first_values)
    last_median = statistics.median(last_values)
    delta = last_median - first_median
    slope = theil_sen_slope_per_hour(observed)
    tau = kendall_tau(observed)
    failures: list[str] = []
    leak = False

    if kind == "rss":
        limit = capacity
        config = thresholds[kind]
        if limit is None or limit <= 0:
            failures.append("memory limit is missing")
        elif max(values) >= limit * float(config["maximum_limit_ratio"]):
            failures.append("RSS reached the configured memory-limit ceiling")
        allowed_delta = max(
            first_median * float(config["maximum_delta_ratio"]),
            float(config["maximum_delta_bytes"]),
        )
        if delta > allowed_delta:
            failures.append("RSS endpoint delta exceeds the stability allowance")
        leak = (
            tau >= float(config["minimum_kendall_tau"])
            and slope > float(config["minimum_leak_slope_bytes_per_hour"])
            and delta > allowed_delta
        )
    elif kind in {"task", "fd"}:
        config = thresholds[kind]
        allowed_delta = max(
            first_median * float(config["maximum_delta_ratio"]),
            float(config["maximum_delta_absolute"]),
        )
        if delta > allowed_delta:
            failures.append(f"{kind} endpoint delta exceeds the stability allowance")
        leak = (
            tau >= float(config["minimum_kendall_tau"])
            and slope > float(config["minimum_leak_slope_per_hour"])
            and delta > allowed_delta
        )
    elif kind == "queue":
        config = thresholds[kind]
        if capacity is None or capacity <= 0:
            failures.append("queue capacity is missing")
        else:
            if max(values) > capacity:
                failures.append("queue exceeded its hard capacity")
            if percentile(values, 0.99) > capacity * float(config["maximum_p99_capacity_ratio"]):
                failures.append("queue p99 occupancy exceeds the stability allowance")
            cooldown = [value for timestamp, value in points if observation_end < timestamp <= cooldown_end]
            if not cooldown:
                failures.append("queue cooldown samples are missing")
            elif statistics.median(cooldown) > capacity * float(config["maximum_cooldown_ratio"]):
                failures.append("queue did not drain during cooldown")
    elif kind == "cache":
        config = thresholds[kind]
        if capacity is None or capacity <= 0:
            failures.append("cache budget is missing")
        else:
            if max(values) > capacity * float(config["maximum_budget_ratio"]):
                failures.append("cache exceeded its hard budget")
            allowed_delta = max(
                capacity * float(config["maximum_delta_ratio"]),
                float(config["maximum_delta_bytes"]),
            )
            leak = (
                tau >= float(config["minimum_kendall_tau"])
                and slope > capacity * float(config["minimum_leak_slope_budget_ratio_per_hour"])
                and delta > allowed_delta
            )
            if leak:
                failures.append("cache shows sustained monotonic growth instead of a plateau")
    elif kind == "lag":
        config = thresholds[kind]
        if longest_above(observed, float(config["maximum_sustained_bytes"])) > float(
            config["maximum_sustained_seconds"]
        ):
            failures.append("lag remained above the sustained threshold")
    elif kind == "renewal":
        if max(values) > float(thresholds[kind]["maximum_due_lag_micros"]):
            failures.append("receipt renewal due-lag exceeded its deadline allowance")

    if leak and not failures:
        failures.append("monotonic growth detected")
    return {
        "metric": metric,
        "scope": scope,
        "kind": kind,
        "status": "pass" if not failures else "fail",
        "sample_count": len(observed),
        "first_window_median": first_median,
        "last_window_median": last_median,
        "delta": delta,
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
        "capacity": capacity,
        "theil_sen_slope_per_hour": slope,
        "kendall_tau": tau,
        "monotonic_growth": leak,
        "failures": failures,
        "raw_artifact_sha256": "sha256:" + raw_digest,
    }


def summarize_pods(observations: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in observations:
        name = item.get("name")
        if isinstance(name, str) and name:
            by_name[name].append(item)
    findings: list[str] = []
    pods: list[dict[str, Any]] = []
    for name, items in sorted(by_name.items()):
        uids = {str(item.get("uid", "")) for item in items}
        restarts = max((int(item.get("restarts", 0)) for item in items), default=0)
        oom_killed = any(bool(item.get("oom_killed", False)) for item in items)
        if len(uids) != 1 or "" in uids:
            findings.append(f"pod identity changed or is missing: {name}")
        if restarts != 0 or oom_killed:
            findings.append(f"pod restarted or was OOM-killed: {name}")
        pods.append({"name": name, "uid": sorted(uids)[0] if uids else "", "restarts": restarts, "oom_killed": oom_killed})
    if not pods:
        findings.append("pod identity/restart observations are missing")
    return pods, findings


def analyze(
    policy: dict[str, Any],
    profile_name: str,
    samples_path: Path,
    identity_path: Path,
    output_path: Path,
    workload_path: Path | None,
    policy_digest: str | None = None,
) -> dict[str, Any]:
    findings = validate_policy(policy)
    if findings:
        raise SoakError("invalid soak policy: " + "; ".join(findings))
    profile = policy["profiles"].get(profile_name)
    if profile is None:
        raise SoakError(f"unknown soak profile: {profile_name}")
    identity = read_json(identity_path)
    required_identity = {
        "commit",
        "deployment_digest",
        "target_id",
        "cluster_uid",
        "effective_config_sha256",
        "durability_contract",
    }
    missing_identity = sorted(required_identity - identity.keys())
    if missing_identity:
        raise SoakError("release identity is missing: " + ", ".join(missing_identity))
    samples, pod_observations = load_samples(samples_path)
    timestamps = sorted({sample["timestamp"] for sample in samples})
    run_start = timestamps[0]
    observation_start = run_start + profile["warmup_seconds"]
    observation_end = observation_start + profile["observation_seconds"]
    cooldown_end = observation_end + profile["cooldown_seconds"]
    if timestamps[-1] < cooldown_end:
        raise SoakError("sample timeline does not cover warmup, observation, and cooldown")

    groups: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    for sample in samples:
        groups[(sample["metric"], sample["scope"])].append((sample["timestamp"], sample["value"]))
    raw_digest = sha256_file(samples_path)
    expected = profile["observation_seconds"] // profile["sample_interval_seconds"] + 1
    observed_counts: list[int] = []
    max_gap_seconds = 0.0
    series: list[dict[str, Any]] = []
    failures: list[str] = []
    for rule in policy["required_series"]:
        matching = sorted((scope, points) for (metric, scope), points in groups.items() if metric == rule["metric"])
        if len(matching) < rule["minimum_scopes"]:
            failures.append(
                f"required metric {rule['metric']} has {len(matching)} scopes; {rule['minimum_scopes']} required"
            )
            continue
        for scope, points in matching:
            observed_points = [point for point in points if observation_start <= point[0] <= observation_end]
            observed_counts.append(len(observed_points))
            max_gap_seconds = max(max_gap_seconds, maximum_gap(observed_points))
            capacity = pair_capacity_metric(rule, groups, scope)
            result = analyze_series(
                rule["metric"],
                scope,
                rule["kind"],
                points,
                capacity,
                observation_start,
                observation_end,
                cooldown_end,
                min(policy["windows"]["comparison_seconds"], max(1, profile["observation_seconds"] // 2)),
                policy["thresholds"],
                raw_digest,
            )
            series.append(result)
            failures.extend(f"{result['metric']}/{scope}: {message}" for message in result["failures"])

    observed = min(observed_counts, default=0)
    coverage = observed / expected * 100.0 if expected else 0.0
    if coverage < float(profile["minimum_coverage_percent"]):
        failures.append("sampling coverage is below the profile minimum")
    if max_gap_seconds > float(profile["maximum_gap_seconds"]):
        failures.append("sampling maximum gap exceeds the profile limit")
    pods, pod_findings = summarize_pods(pod_observations)
    failures.extend(pod_findings)

    workload: dict[str, Any] | None = None
    artifacts = [
        {"path": samples_path.name, "sha256": raw_digest},
        {"path": identity_path.name, "sha256": sha256_file(identity_path)},
    ]
    if workload_path is not None:
        workload = read_json(workload_path)
        workload_digest = sha256_file(workload_path)
        artifacts.append({"path": workload_path.name, "sha256": workload_digest})
        attempted = workload.get("attempted")
        put_ok = workload.get("put_ok")
        consumed = workload.get("consumed")
        if not isinstance(attempted, int) or attempted <= 0 or put_ok != attempted or consumed != put_ok:
            failures.append("workload did not consume every PutOk message")
        for key in ("send_failures", "consume_failures", "missing", "duplicates", "corrupt"):
            if workload.get(key) != 0:
                failures.append(f"workload {key} must be zero")
    elif profile_name == "full":
        failures.append("full soak requires a workload summary")

    growth = any(item["monotonic_growth"] for item in series)
    report = {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_soak_report",
        "profile": profile_name,
        "status": "pass" if not failures else "fail",
        "monotonic_growth_detected": growth,
        "started_at": dt.datetime.fromtimestamp(observation_start, dt.timezone.utc).isoformat(),
        "finished_at": dt.datetime.fromtimestamp(observation_end, dt.timezone.utc).isoformat(),
        "duration_seconds": int(profile["observation_seconds"]),
        "release_identity": identity,
        "sampling": {
            "interval_seconds": profile["sample_interval_seconds"],
            "expected": expected,
            "observed": observed,
            "coverage_percent": coverage,
            "max_gap_seconds": max_gap_seconds,
        },
        "workload": workload,
        "pods": pods,
        "series": series,
        "failures": failures,
        "policy_sha256": policy_digest
        or hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
        "artifacts": artifacts,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def validate_report(policy: dict[str, Any], report_path: Path) -> list[str]:
    findings = validate_policy(policy)
    report = read_json(report_path)
    if report.get("schema_version") != 1 or report.get("artifact_kind") != "rocketmq_message_path_soak_report":
        findings.append("report schema_version/artifact_kind is invalid")
    profile_name = report.get("profile")
    if profile_name not in policy.get("profiles", {}):
        findings.append("report profile is invalid")
        return findings
    profile = policy["profiles"][profile_name]
    if report.get("duration_seconds", 0) < profile["observation_seconds"]:
        findings.append("report duration is below the selected profile")
    sampling = report.get("sampling")
    if not isinstance(sampling, dict):
        findings.append("report sampling is missing")
    else:
        if sampling.get("coverage_percent", 0) < profile["minimum_coverage_percent"]:
            findings.append("report coverage is below policy")
        if sampling.get("max_gap_seconds", math.inf) > profile["maximum_gap_seconds"]:
            findings.append("report maximum gap is above policy")
    identity = report.get("release_identity")
    if not isinstance(identity, dict):
        findings.append("report release_identity is missing")
    pods = report.get("pods")
    if not isinstance(pods, list) or not pods:
        findings.append("report pod evidence is missing")
    elif any(pod.get("restarts") != 0 or pod.get("oom_killed") is not False for pod in pods):
        findings.append("report contains restarted or OOM-killed pods")
    series = report.get("series")
    if not isinstance(series, list) or not series:
        findings.append("report series is missing")
    elif any(item.get("status") != "pass" or not is_sha256(item.get("raw_artifact_sha256")) for item in series):
        findings.append("report contains failed or unbound resource series")
    artifacts = report.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        findings.append("report artifact inventory is missing")
    else:
        for artifact in artifacts:
            if not isinstance(artifact, dict) or not is_sha256(artifact.get("sha256")):
                findings.append("report artifact inventory contains an invalid digest")
                continue
            path = report_path.parent / str(artifact.get("path", ""))
            if not path.is_file() or sha256_file(path) != artifact["sha256"]:
                findings.append(f"report artifact is missing or tampered: {artifact.get('path')}")
    if report.get("status") != "pass" or report.get("monotonic_growth_detected") is not False:
        findings.append("report did not pass resource stability qualification")
    return findings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("validate-policy")
    analyze_command = commands.add_parser("analyze")
    analyze_command.add_argument("--profile", choices=("smoke", "full"), required=True)
    analyze_command.add_argument("--samples", type=Path, required=True)
    analyze_command.add_argument("--identity", type=Path, required=True)
    analyze_command.add_argument("--workload-summary", type=Path)
    analyze_command.add_argument("--output", type=Path, required=True)
    report_command = commands.add_parser("validate-report")
    report_command.add_argument("--report", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        policy = read_json(args.policy)
        if args.command == "validate-policy":
            findings = validate_policy(policy)
            if findings:
                raise SoakError("; ".join(findings))
            print("MESSAGE_PATH_SOAK_POLICY_OK")
            return 0
        if args.command == "analyze":
            report = analyze(
                policy,
                args.profile,
                args.samples,
                args.identity,
                args.output,
                args.workload_summary,
                sha256_file(args.policy),
            )
            print(json.dumps({"status": report["status"], "report": str(args.output.resolve())}, sort_keys=True))
            return 0 if report["status"] == "pass" else 2
        findings = validate_report(policy, args.report)
        if findings:
            raise SoakError("; ".join(findings))
        print("MESSAGE_PATH_SOAK_REPORT_OK")
        return 0
    except SoakError as error:
        print(f"MESSAGE_PATH_SOAK_ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
