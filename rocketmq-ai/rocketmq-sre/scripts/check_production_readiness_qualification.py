#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validate sustained-operation and scale qualification evidence."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "production-readiness.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.production-readiness-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.production-readiness-qualification-report.v1"
EXPECTED_FAULTS = {
    "mcp_connector_pod_replacement",
    "control_plane_pod_replacement",
    "execution_agent_pod_replacement",
    "model_mock_outage",
    "postgres_pod_replacement",
    "collector_outage",
    "broker_pod_replacement",
}
EXPECTED_EVIDENCE = {
    "qualification_runner",
    "soak_runner",
    "scale_runner",
    "postgres_ha_runner",
    "disaster_recovery_runner",
    "service_image_contract",
    "handoff_checklist",
}
EXPECTED_SOURCES = {"soak", "scale", "policy", "precheck", "postgres_ha", "disaster_recovery", "service_images"}
REVISION = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8-sig") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for child in value for text in all_strings(child)]
    if isinstance(value, dict):
        return [text for key, child in value.items() for text in (*all_strings(key), *all_strings(child))]
    return []


def repository_file(raw_path: Any, location: str, findings: list[str]) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path:
        findings.append(f"{location} must be a non-empty repository path")
        return None
    path = PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts:
        findings.append(f"{location} must stay inside the repository")
        return None
    resolved = REPOSITORY_ROOT / Path(*path.parts)
    if not resolved.is_file():
        findings.append(f"{location} does not exist: {raw_path}")
        return None
    return resolved


def positive_integer(value: Any, location: str, findings: list[str]) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        findings.append(f"{location} must be a positive integer")


def positive_number(value: Any, location: str, findings: list[str]) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        findings.append(f"{location} must be a positive number")


def nonnegative_number(value: Any, location: str, findings: list[str]) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
        findings.append(f"{location} must be a non-negative number")


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_values = {
        "schema_version": MANIFEST_SCHEMA,
        "environment": "disposable_kind",
        "operating_mode": "rules_only",
        "production_certified": False,
        "model_provider_network_calls": False,
        "unattended_autonomous_execution": False,
        "live_mode_ceiling": "supervised",
    }
    for field, expected in expected_values.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")

    minimums = manifest.get("minimums")
    expected_minimums = {
        "soak_duration_seconds": 21_600,
        "soak_samples": 300,
        "sampled_availability_ratio": 0.99,
        "logical_clusters": 100,
        "topic_assets": 10_000,
        "consumer_group_assets": 10_000,
        "asset_page_samples": 40,
        "evidence_query_samples": 100,
        "policy_evaluation_samples": 10_000,
        "precheck_samples": 1_000,
        "operational_measurement_samples": 1_000,
    }
    if not isinstance(minimums, dict):
        findings.append("minimums must be an object")
    else:
        for field, expected in expected_minimums.items():
            if minimums.get(field) != expected:
                findings.append(f"minimums.{field} must remain {expected!r}")

    limits = manifest.get("latency_limits_millis")
    expected_limits = {"evidence_query_p95": 500, "policy_evaluation_p99": 50, "precheck_p95": 50}
    if not isinstance(limits, dict):
        findings.append("latency_limits_millis must be an object")
    else:
        for field, expected in expected_limits.items():
            if limits.get(field) != expected:
                findings.append(f"latency_limits_millis.{field} must remain {expected}")

    operational_limits = manifest.get("operational_limits")
    if operational_limits != {"error_rate": 0.01, "execution_queue_depth": 256}:
        findings.append("operational_limits must remain bounded")

    if set(manifest.get("required_faults", [])) != EXPECTED_FAULTS:
        findings.append("required_faults must contain the complete bounded outage matrix")

    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict) or set(evidence) != EXPECTED_EVIDENCE:
        findings.append("repository_evidence is incomplete")
    else:
        for name, path in evidence.items():
            repository_file(path, f"repository_evidence.{name}", findings)

    report = manifest.get("live_report")
    if not isinstance(report, dict):
        findings.append("live_report must be an object")
    else:
        if report.get("schema_version") != REPORT_SCHEMA:
            findings.append("live_report schema is unsupported")
        if report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if set(report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
        if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
            findings.append("live reports must exclude secrets and message bodies")
        if report.get("external_operator_signoff_required_for_production") is not True:
            findings.append("production certification must require external operator signoff")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains credential-like material")
    return findings


def parse_timestamp(value: Any, location: str, findings: list[str]) -> datetime | None:
    if not isinstance(value, str):
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None


def validate_measurement(
    measurements: Any,
    name: str,
    minimum_samples: int,
    percentile: str,
    maximum_millis: float,
    findings: list[str],
) -> None:
    if not isinstance(measurements, dict):
        findings.append("measurements must be an object")
        return
    measurement = measurements.get(name)
    if not isinstance(measurement, dict):
        findings.append(f"measurements.{name} must be an object")
        return
    samples = measurement.get("samples")
    if not isinstance(samples, int) or isinstance(samples, bool) or samples < minimum_samples:
        findings.append(f"measurements.{name}.samples is below {minimum_samples}")
    value = measurement.get(percentile)
    positive_number(value, f"measurements.{name}.{percentile}", findings)
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > maximum_millis:
        findings.append(f"measurements.{name}.{percentile} exceeds {maximum_millis} ms")
    if measurement.get("unit") != "milliseconds":
        findings.append(f"measurements.{name}.unit must be milliseconds")


def validate_report(report: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if report.get("schema_version") != REPORT_SCHEMA:
        findings.append(f"report schema_version must be {REPORT_SCHEMA}")
    if report.get("status") != "passed" or report.get("environment") != "disposable_kind":
        findings.append("report must be a passed disposable Kind qualification")
    if not isinstance(report.get("revision"), str) or not REVISION.fullmatch(report["revision"]):
        findings.append("report revision must be a full lowercase Git SHA")
    if report.get("source_clean") is not True:
        findings.append("report source must be clean")
    started = parse_timestamp(report.get("started_at"), "started_at", findings)
    finished = parse_timestamp(report.get("finished_at"), "finished_at", findings)
    if started and finished and finished < started:
        findings.append("finished_at must not precede started_at")
    expected_values = {
        "production_certified": False,
        "model_provider_network_calls": 0,
        "unattended_autonomous_execution": False,
        "live_mode_ceiling": "supervised",
        "secrets_recorded": False,
        "message_bodies_recorded": False,
    }
    for field, expected in expected_values.items():
        if report.get(field) != expected:
            findings.append(f"report {field} must remain {expected!r}")

    sources = report.get("sources")
    if not isinstance(sources, list):
        findings.append("sources must be an array")
    else:
        observed_sources: set[str] = set()
        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                findings.append(f"sources[{index}] must be an object")
                continue
            source_id = source.get("id")
            if isinstance(source_id, str):
                observed_sources.add(source_id)
            if source.get("status") != "passed":
                findings.append(f"sources[{index}] must be passed")
            if not isinstance(source.get("schema_version"), str) or not source["schema_version"]:
                findings.append(f"sources[{index}].schema_version must be non-empty")
            if not isinstance(source.get("sha256"), str) or not DIGEST.fullmatch(source["sha256"]):
                findings.append(f"sources[{index}].sha256 must be canonical")
            if source.get("revision") != report.get("revision"):
                findings.append(f"sources[{index}].revision must match the report revision")
        if observed_sources != EXPECTED_SOURCES or len(sources) != len(EXPECTED_SOURCES):
            findings.append(f"source evidence set drifted: {sorted(observed_sources)}")

    minimums = manifest["minimums"]
    soak = report.get("soak")
    if not isinstance(soak, dict):
        findings.append("soak must be an object")
    else:
        for field in ("planned_duration_seconds", "observed_duration_seconds"):
            if not isinstance(soak.get(field), (int, float)) or soak[field] < minimums["soak_duration_seconds"]:
                findings.append(f"soak.{field} is below the full-duration minimum")
        if not isinstance(soak.get("samples_observed"), int) or soak["samples_observed"] < minimums["soak_samples"]:
            findings.append("soak.samples_observed is below the minimum")
        availability = soak.get("sampled_availability_ratio")
        if not isinstance(availability, (int, float)) or availability < minimums["sampled_availability_ratio"]:
            findings.append("soak sampled availability is below the minimum")
        if soak.get("full_duration_qualification") is not True or soak.get("final_all_ready") is not True:
            findings.append("soak must be a complete full-duration run with final readiness")
        if soak.get("data_plane_independent") is not True:
            findings.append("soak must prove RocketMQ data-plane independence")
        if soak.get("unresolved_faults") != []:
            findings.append("soak contains unresolved faults")
        faults = soak.get("faults")
        if not isinstance(faults, list):
            findings.append("soak faults must be an array")
        else:
            ids = {fault.get("id") for fault in faults if isinstance(fault, dict)}
            if ids != EXPECTED_FAULTS or len(faults) != len(EXPECTED_FAULTS):
                findings.append(f"soak fault set drifted: {sorted(str(value) for value in ids)}")
            for fault in faults:
                if not isinstance(fault, dict):
                    continue
                fault_id = fault.get("id")
                positive_number(fault.get("recovery_seconds"), f"soak fault {fault_id}.recovery_seconds", findings)
                if fault.get("recovered") is not True or fault.get("data_plane_recovery_verified") is not True:
                    findings.append(f"soak fault {fault_id} did not verify recovery")
                if fault_id == "broker_pod_replacement":
                    if (
                        fault.get("data_plane_probe_phase") != "after_recovery"
                        or fault.get("data_plane_remained_ready") is not None
                    ):
                        findings.append("broker replacement must report post-recovery probing without a continuity claim")
                elif (
                    fault.get("data_plane_probe_phase") != "during_outage"
                    or fault.get("data_plane_remained_ready") is not True
                ):
                    findings.append(f"soak fault {fault_id} did not preserve the RocketMQ data plane during outage")
                probe = fault.get("bounded_data_plane_probe")
                if not isinstance(probe, dict) or any(
                    probe.get(field) != 10
                    for field in ("sent_messages", "received_messages", "acknowledged_messages")
                ):
                    findings.append(f"soak fault {fault.get('id')} is missing its bounded data-plane probe")
                elif probe.get("message_bodies_recorded") is not False:
                    findings.append(f"soak fault {fault.get('id')} recorded message bodies")
        resources = soak.get("resource_summary")
        if not isinstance(resources, dict) or resources.get("samples", 0) <= 0:
            findings.append("soak resource_summary must contain measured samples")
        else:
            nonnegative_number(resources.get("cpu_percent_max"), "soak.resource_summary.cpu_percent_max", findings)
            positive_number(resources.get("memory_bytes_max"), "soak.resource_summary.memory_bytes_max", findings)

    scale = report.get("scale")
    if not isinstance(scale, dict):
        findings.append("scale must be an object")
    else:
        required_scale = {
            "logical_clusters": minimums["logical_clusters"],
            "topic_assets": minimums["topic_assets"],
            "consumer_group_assets": minimums["consumer_group_assets"],
        }
        for field, minimum in required_scale.items():
            if not isinstance(scale.get(field), int) or scale[field] < minimum:
                findings.append(f"scale.{field} is below {minimum}")
        if scale.get("page_limit") != 500 or scale.get("oversized_page_rejected") is not True:
            findings.append("scale pagination must remain bounded at 500 rows")
        page_samples = scale.get("page_samples")
        if not isinstance(page_samples, int) or page_samples < minimums["asset_page_samples"]:
            findings.append("scale.page_samples is below the asset pagination minimum")
        if scale.get("quota_backpressure_verified") is not True or scale.get("cleanup_verified") is not True:
            findings.append("scale quota/backpressure and cleanup must be verified")

    limits = manifest["latency_limits_millis"]
    measurements = report.get("measurements")
    validate_measurement(
        measurements,
        "evidence_query",
        minimums["evidence_query_samples"],
        "p95_millis",
        limits["evidence_query_p95"],
        findings,
    )

    operational = report.get("operational_measurements")
    if not isinstance(operational, dict):
        findings.append("operational_measurements must be an object")
    else:
        samples = operational.get("samples")
        if (
            not isinstance(samples, int)
            or isinstance(samples, bool)
            or samples < minimums["operational_measurement_samples"]
        ):
            findings.append("operational_measurements.samples is below the minimum")
        error_rate = operational.get("error_rate")
        nonnegative_number(error_rate, "operational_measurements.error_rate", findings)
        if (
            isinstance(error_rate, (int, float))
            and not isinstance(error_rate, bool)
            and error_rate > manifest["operational_limits"]["error_rate"]
        ):
            findings.append("operational_measurements.error_rate exceeds the limit")
        queue_depth = operational.get("execution_queue_depth_max")
        nonnegative_number(queue_depth, "operational_measurements.execution_queue_depth_max", findings)
        if (
            isinstance(queue_depth, (int, float))
            and not isinstance(queue_depth, bool)
            and queue_depth > manifest["operational_limits"]["execution_queue_depth"]
        ):
            findings.append("operational_measurements.execution_queue_depth_max exceeds the limit")
        error_count = operational.get("error_count")
        if not isinstance(error_count, int) or isinstance(error_count, bool) or error_count < 0:
            findings.append("operational_measurements.error_count must be a non-negative integer")
        queue_samples = operational.get("execution_queue_depth_samples")
        if not isinstance(queue_samples, int) or isinstance(queue_samples, bool) or queue_samples < minimums[
            "operational_measurement_samples"
        ]:
            findings.append("operational_measurements.execution_queue_depth_samples is below the minimum")
    validate_measurement(
        measurements,
        "policy_evaluation",
        minimums["policy_evaluation_samples"],
        "p99_millis",
        limits["policy_evaluation_p99"],
        findings,
    )
    validate_measurement(
        measurements,
        "execution_precheck",
        minimums["precheck_samples"],
        "p95_millis",
        limits["precheck_p95"],
        findings,
    )

    handoff = report.get("handoff")
    if not isinstance(handoff, dict):
        findings.append("handoff must be an object")
    else:
        if handoff.get("checklist_validated") is not True or handoff.get("command_paths_validated") is not True:
            findings.append("handoff checklist and commands must be validated")
        if handoff.get("independent_operator_signoff") is not False:
            findings.append("disposable qualification must not fabricate independent operator signoff")
        if handoff.get("required_for_production") is not True:
            findings.append("external operator signoff must remain required for production")

    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict) or cleanup.get("status") != "passed":
        findings.append("cleanup must be passed")
    elif any(cleanup.get(field) is not True for field in ("disposable_kind_destroyed", "owned_containers_removed", "owned_artifacts_removed")):
        findings.append("cleanup must remove every qualification-owned resource")
    if any(SENSITIVE.search(value) for value in all_strings(report)):
        findings.append("report contains credential-like material")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = load_json(args.manifest)
    findings = validate_manifest(manifest)
    if args.report is not None:
        findings.extend(validate_report(load_json(args.report), manifest))
    if findings:
        for finding in findings:
            print(f"production-readiness qualification: {finding}")
        return 1
    print("production-readiness qualification: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
