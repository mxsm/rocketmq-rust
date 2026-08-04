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
"""Validate the committed R1 catalog and optional machine-local live report."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "r1-actions.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.r1-action-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.r1-action-qualification-report.v1"
EXPECTED_ACTIONS = {
    "observability.logger_level_ttl.v1",
    "proxy.scale_out_one.v1",
    "proxy.restart_one.v1",
    "telemetry.collector.restart_one.v1",
}
EXPECTED_OUTCOMES = {
    "live_verified_success",
    "precheck_denied",
    "duplicate_idempotent",
    "stale_epoch_denied",
    "unknown_effect_reconciled",
    "verification_failure",
    "compensation_or_safe_stop",
    "rollback_failure_or_manual_takeover_quarantined",
    "scope_denied",
    "kill_switch_denied",
}
EXPECTED_SHARED = {
    "duplicate_idempotent",
    "stale_epoch_denied",
    "unknown_effect_reconciled",
    "scope_denied",
    "kill_switch_denied",
}
EXPECTED_DENIALS = {
    "disabled_action",
    "wrong_tenant",
    "wrong_cluster",
    "expired_grant",
    "unknown_action",
    "r2_without_approval",
    "r3_action",
    "raw_request",
    "shell_command",
    "arbitrary_kubernetes_patch",
}
REVISION = re.compile(r"^[0-9a-f]{40}$")
IDENTIFIER = re.compile(r"^[0-9a-f]{8}-[0-9a-f-]{27,}$", re.IGNORECASE)
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
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


def repository_evidence(value: Any, location: str, findings: list[str]) -> Path | None:
    if not isinstance(value, dict):
        findings.append(f"{location} must be an object")
        return None
    raw_path = value.get("path")
    test = value.get("test")
    if not isinstance(raw_path, str) or not raw_path:
        findings.append(f"{location}.path must be non-empty")
        return None
    path = PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts or "docs" in path.parts:
        findings.append(f"{location}.path must be repository implementation evidence")
        return None
    resolved = REPOSITORY_ROOT / Path(*path.parts)
    if not resolved.is_file():
        findings.append(f"{location}.path does not exist: {raw_path}")
        return None
    if not isinstance(test, str) or not test or test not in resolved.read_text(encoding="utf-8"):
        findings.append(f"{location}.test is absent from {raw_path}: {test}")
    return resolved


def yaml_scalar(path: Path, key: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf"(?m)^{re.escape(key)}:\s*([^#\s]+)", text)
    return match.group(1).strip("\"'") if match else None


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        findings.append(f"schema_version must be {MANIFEST_SCHEMA}")
    if manifest.get("operating_mode") != "rules_only":
        findings.append("R1 qualification must remain rules-only")
    if manifest.get("environment") != "disposable_kind":
        findings.append("R1 live qualification must identify disposable Kind")
    if manifest.get("model_provider_network_calls") is not False:
        findings.append("model-provider network calls must remain disabled")
    if manifest.get("production_certified") is not False:
        findings.append("disposable qualification must not claim production certification")
    if set(manifest.get("required_outcomes", [])) != EXPECTED_OUTCOMES:
        findings.append("required outcome matrix is incomplete")
    if set(manifest.get("hard_denials", [])) != EXPECTED_DENIALS:
        findings.append("hard-denial matrix is incomplete")

    shared = manifest.get("shared_boundary_evidence")
    if not isinstance(shared, dict) or set(shared) != EXPECTED_SHARED:
        findings.append("shared boundary evidence is incomplete")
    else:
        for outcome, evidence in shared.items():
            repository_evidence(evidence, f"shared_boundary_evidence.{outcome}", findings)

    actions = manifest.get("actions")
    if not isinstance(actions, list):
        findings.append("actions must be an array")
        actions = []
    observed: set[str] = set()
    switches: set[str] = set()
    for index, action in enumerate(actions):
        location = f"actions[{index}]"
        if not isinstance(action, dict):
            findings.append(f"{location} must be an object")
            continue
        action_id = action.get("id")
        if not isinstance(action_id, str) or action_id not in EXPECTED_ACTIONS:
            findings.append(f"{location}.id is not a registered R1 action")
            continue
        if action_id in observed:
            findings.append(f"duplicate R1 action: {action_id}")
        observed.add(action_id)
        if action.get("risk") != "r1" or action.get("qualification") != "disposable_cluster_smoke_passed":
            findings.append(f"{location} must be R1 and disposable-cluster qualified")
        if not isinstance(action.get("owner"), str) or not action["owner"].strip():
            findings.append(f"{location}.owner must be non-empty")
        enable_switch = action.get("enable_switch")
        if not isinstance(enable_switch, str) or not enable_switch:
            findings.append(f"{location}.enable_switch must be non-empty")
        elif enable_switch in switches:
            findings.append(f"{location}.enable_switch must be independent")
        else:
            switches.add(enable_switch)
        descriptor_raw = action.get("descriptor")
        descriptor = REPOSITORY_ROOT / Path(*PurePosixPath(str(descriptor_raw)).parts)
        if not descriptor.is_file():
            findings.append(f"{location}.descriptor does not exist")
        else:
            if yaml_scalar(descriptor, "id") != action_id:
                findings.append(f"{location}.descriptor id drifted")
            if yaml_scalar(descriptor, "risk") != "r1" or yaml_scalar(descriptor, "execution_supported") != "true":
                findings.append(f"{location}.descriptor is not an executable R1 action")
            if yaml_scalar(descriptor, "owner") != action.get("owner"):
                findings.append(f"{location}.owner drifted from its descriptor")
        for field in ("precheck_evidence", "live_success_evidence", "recovery_matrix_evidence"):
            repository_evidence(action.get(field), f"{location}.{field}", findings)
    if observed != EXPECTED_ACTIONS:
        findings.append(f"R1 action set drifted: {sorted(observed)}")

    report = manifest.get("live_report")
    if not isinstance(report, dict) or report.get("schema_version") != REPORT_SCHEMA:
        findings.append("live report contract is missing or unsupported")
    else:
        if report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
            findings.append("live report must exclude secrets and message bodies")
        if set(report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains a credential-like value")
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


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if report.get("schema_version") != REPORT_SCHEMA:
        findings.append(f"report schema_version must be {REPORT_SCHEMA}")
    if report.get("status") != "passed" or report.get("environment") != "disposable_kind":
        findings.append("report must be a passed disposable Kind run")
    if not isinstance(report.get("revision"), str) or not REVISION.fullmatch(report["revision"]):
        findings.append("report revision must be a full lowercase Git SHA")
    if report.get("source_clean") is not True:
        findings.append("report source must be clean")
    started = parse_timestamp(report.get("started_at"), "started_at", findings)
    finished = parse_timestamp(report.get("finished_at"), "finished_at", findings)
    if started and finished and finished < started:
        findings.append("finished_at must not precede started_at")
    if report.get("model_provider_network_calls") != 0:
        findings.append("report must prove zero model-provider network calls")
    if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
        findings.append("report must exclude secrets and message bodies")

    actions = report.get("actions")
    if not isinstance(actions, list):
        findings.append("report actions must be an array")
        actions = []
    observed: set[str] = set()
    for index, action in enumerate(actions):
        location = f"report.actions[{index}]"
        if not isinstance(action, dict):
            findings.append(f"{location} must be an object")
            continue
        action_id = action.get("id")
        if not isinstance(action_id, str) or action_id not in EXPECTED_ACTIONS:
            findings.append(f"{location}.id is not a registered R1 action")
            continue
        observed.add(action_id)
        outcomes = action.get("outcomes")
        if not isinstance(outcomes, dict) or set(outcomes) != EXPECTED_OUTCOMES:
            findings.append(f"{location}.outcomes is incomplete")
        elif any(value != "passed" for value in outcomes.values()):
            findings.append(f"{location}.outcomes contains a non-passing result")
        live = action.get("live")
        if not isinstance(live, dict):
            findings.append(f"{location}.live must be an object")
        else:
            if live.get("state") != "succeeded":
                findings.append(f"{location}.live state must be succeeded")
            for field in ("execution_id", "correlation_id"):
                if not isinstance(live.get(field), str) or not IDENTIFIER.fullmatch(live[field]):
                    findings.append(f"{location}.live.{field} must be a redacted identifier")
            for field in (
                "approval_events",
                "intent_records",
                "result_records",
                "confirmed_agent_effects",
                "successful_verifications",
                "verification_evidence_records",
            ):
                if not isinstance(live.get(field), int) or live[field] < 1:
                    findings.append(f"{location}.live.{field} must be positive")
            if live.get("target_mutations") != 1:
                findings.append(f"{location}.live.target_mutations must equal one")
        recovery = action.get("recovery")
        if not isinstance(recovery, dict):
            findings.append(f"{location}.recovery must be an object")
        else:
            if recovery.get("verified_success") != "succeeded":
                findings.append(f"{location}.recovery verified success is missing")
            automatic = action_id in {
                "observability.logger_level_ttl.v1",
                "proxy.scale_out_one.v1",
            }
            expected_mode = "automatic_compensation" if automatic else "manual_takeover_safe_stop"
            expected_failure = "rolled_back" if automatic else "escalated"
            expected_rollback_failure = "escalated" if automatic else "not_applicable_manual_takeover"
            if recovery.get("recovery_mode") != expected_mode:
                findings.append(f"{location}.recovery mode is incorrect")
            if recovery.get("verification_failure") != expected_failure:
                findings.append(f"{location}.recovery verification failure has an unsafe terminal state")
            if recovery.get("rollback_failure") != expected_rollback_failure:
                findings.append(f"{location}.recovery rollback-failure handling is incorrect")
            if recovery.get("compensation_intents", 0) < 1 or recovery.get("active_quarantines", 0) < 1:
                findings.append(f"{location}.recovery lacks compensation or quarantine evidence")
    if observed != EXPECTED_ACTIONS or len(actions) != len(EXPECTED_ACTIONS):
        findings.append("report must contain each R1 action exactly once")

    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict) or cleanup.get("status") != "passed":
        findings.append("cleanup must pass")
    elif any(
        cleanup.get(field) is not True
        for field in (
            "proxy_replicas_restored",
            "logger_ttl_restored",
            "proxy_ready",
            "collector_ready",
            "owned_resources_removed",
        )
    ):
        findings.append("cleanup proof is incomplete")
    if any(SENSITIVE.search(value) for value in all_strings(report)):
        findings.append("report contains a credential-like value")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        findings = validate_manifest(load_json(args.manifest))
        if args.report is not None:
            findings.extend(validate_report(load_json(args.report)))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"R1_ACTION_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"R1_ACTION_QUALIFICATION_FINDING {finding}")
        print(f"R1_ACTION_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"R1_ACTION_QUALIFICATION_OK actions=4 outcomes=10 model_network_calls=false{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
