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
"""Validate bounded-autonomy qualification for the approved R1 actions."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "autonomy-actions.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.autonomy-action-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.autonomy-action-qualification-report.v1"
EXPECTED_ACTIONS = {
    "observability.logger_level_ttl.v1",
    "proxy.scale_out_one.v1",
    "proxy.restart_one.v1",
    "telemetry.collector.restart_one.v1",
}
EXPECTED_OUTCOMES = {
    "disabled_by_default",
    "shadow_cohort_persisted",
    "insufficient_shadow_denied",
    "shadow_window_qualified",
    "supervised_transition_persisted",
    "heterogeneous_critic_bound",
    "same_family_critic_denied",
    "insufficient_supervised_denied",
    "supervised_successes_persisted",
    "stale_or_partial_evidence_denied",
    "freeze_or_budget_denied",
    "kill_switch_denied",
    "execution_failure_paused",
    "expected_deny_not_paused",
    "live_supervised_action_verified",
    "recovery_and_cleanup",
}
EXPECTED_SHARED = {
    "lifecycle_matrix",
    "same_family_critic_denied",
    "stale_or_partial_evidence_denied",
    "freeze_or_budget_denied",
    "kill_switch_denied",
    "failure_pause_reconciled",
}
REVISION = re.compile(r"^[0-9a-f]{40}$")
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


def repository_file(raw_path: Any, location: str, findings: list[str]) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path:
        findings.append(f"{location} must be a non-empty repository path")
        return None
    path = PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts or "docs" in path.parts:
        findings.append(f"{location} must be repository implementation evidence")
        return None
    resolved = REPOSITORY_ROOT / Path(*path.parts)
    if not resolved.is_file():
        findings.append(f"{location} does not exist: {raw_path}")
        return None
    return resolved


def repository_evidence(value: Any, location: str, findings: list[str]) -> None:
    if not isinstance(value, dict):
        findings.append(f"{location} must be an object")
        return
    resolved = repository_file(value.get("path"), f"{location}.path", findings)
    test = value.get("test")
    if resolved is not None and (
        not isinstance(test, str) or not test or test not in resolved.read_text(encoding="utf-8")
    ):
        findings.append(f"{location}.test is absent from {value.get('path')}: {test}")


def yaml_scalar(path: Path, key: str) -> str | None:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*([^#\s]+)", path.read_text(encoding="utf-8"))
    return match.group(1).strip("\"'") if match else None


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_values = {
        "schema_version": MANIFEST_SCHEMA,
        "operating_mode": "rules_only",
        "environment": "disposable_kind",
        "live_mode_ceiling": "supervised",
        "unattended_autonomous_execution": False,
        "model_provider_network_calls": False,
        "production_certified": False,
    }
    for field, expected in expected_values.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")
    if set(manifest.get("required_outcomes", [])) != EXPECTED_OUTCOMES:
        findings.append("required outcome matrix is incomplete")

    shared = manifest.get("shared_boundary_evidence")
    if not isinstance(shared, dict) or set(shared) != EXPECTED_SHARED:
        findings.append("shared boundary evidence is incomplete")
    else:
        for name, evidence in shared.items():
            repository_evidence(evidence, f"shared_boundary_evidence.{name}", findings)

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
            findings.append(f"{location}.id is not an approved autonomy action")
            continue
        if action_id in observed:
            findings.append(f"duplicate autonomy action: {action_id}")
        observed.add(action_id)
        if action.get("risk") != "r1" or action.get("qualification") != "disposable_cluster_smoke_passed":
            findings.append(f"{location} must be R1 and disposable-cluster qualified")
        if action.get("shadow_samples") != 20 or action.get("supervised_successes") != 5:
            findings.append(f"{location} qualification sample thresholds drifted")
        if action.get("observation_window_days") != 7:
            findings.append(f"{location} observation window must be seven days")
        if not isinstance(action.get("owner"), str) or not action["owner"].strip():
            findings.append(f"{location}.owner must be non-empty")
        switch = action.get("enable_switch")
        if not isinstance(switch, str) or not switch:
            findings.append(f"{location}.enable_switch must be non-empty")
        elif switch in switches:
            findings.append(f"{location}.enable_switch must be independent")
        else:
            switches.add(switch)
        descriptor = repository_file(action.get("descriptor"), f"{location}.descriptor", findings)
        policy = repository_file(action.get("policy"), f"{location}.policy", findings)
        if descriptor is not None:
            if yaml_scalar(descriptor, "id") != action_id or yaml_scalar(descriptor, "risk") != "r1":
                findings.append(f"{location}.descriptor identity or risk drifted")
            if yaml_scalar(descriptor, "owner") != action.get("owner"):
                findings.append(f"{location}.owner drifted from its descriptor")
        if policy is not None and yaml_scalar(policy, "action_id") != action_id:
            findings.append(f"{location}.policy action drifted")
    if observed != EXPECTED_ACTIONS or len(actions) != len(EXPECTED_ACTIONS):
        findings.append(f"autonomy action set drifted: {sorted(observed)}")

    fixture = manifest.get("offline_critic_fixture")
    if not isinstance(fixture, dict):
        findings.append("offline Critic fixture is missing")
    else:
        primary = fixture.get("primary_model_family")
        critic = fixture.get("critic_model_family")
        if fixture.get("transport") != "offline_scripted":
            findings.append("Critic transport must remain offline_scripted")
        if not isinstance(primary, str) or not isinstance(critic, str) or primary == critic:
            findings.append("offline Critic fixture must use heterogeneous model families")
        if fixture.get("heterogeneous_required") is not True:
            findings.append("heterogeneous Critic enforcement must remain required")

    report = manifest.get("live_report")
    if not isinstance(report, dict) or report.get("schema_version") != REPORT_SCHEMA:
        findings.append("live report contract is missing or unsupported")
    else:
        if report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if set(report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
        if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
            findings.append("live report must exclude secrets and message bodies")
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
    expected_values = {
        "live_mode_ceiling": "supervised",
        "unattended_autonomous_execution": False,
        "model_provider_network_calls": 0,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
    }
    for field, expected in expected_values.items():
        if report.get(field) != expected:
            findings.append(f"report {field} must remain {expected!r}")

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
            findings.append(f"{location}.id is not an approved autonomy action")
            continue
        observed.add(action_id)
        outcomes = action.get("outcomes")
        if not isinstance(outcomes, dict) or set(outcomes) != EXPECTED_OUTCOMES:
            findings.append(f"{location}.outcomes is incomplete")
        elif any(value != "passed" for value in outcomes.values()):
            findings.append(f"{location}.outcomes contains a non-passing result")
        lifecycle = action.get("lifecycle")
        if not isinstance(lifecycle, dict):
            findings.append(f"{location}.lifecycle must be an object")
        else:
            if lifecycle.get("initial_mode") != "disabled" or lifecycle.get("final_mode") != "supervised":
                findings.append(f"{location}.lifecycle must remain Disabled-to-Supervised")
            if lifecycle.get("shadow_samples") != 20 or lifecycle.get("supervised_successes") != 5:
                findings.append(f"{location}.lifecycle sample counts drifted")
            if lifecycle.get("observation_window_days") != 7:
                findings.append(f"{location}.lifecycle observation window drifted")
            if lifecycle.get("shadow_cohorts") != 1 or lifecycle.get("supervised_cohorts") != 1:
                findings.append(f"{location}.lifecycle cohort persistence drifted")
            if lifecycle.get("same_family_critic_denied") is not True:
                findings.append(f"{location}.lifecycle must deny a same-family Critic")
            if lifecycle.get("autonomous_transition_executed") is not False:
                findings.append(f"{location}.lifecycle must not execute an Autonomous transition")
            if lifecycle.get("expected_deny_paused") is not False:
                findings.append(f"{location}.lifecycle ExpectedDeny must not pause")
            if lifecycle.get("execution_failure_paused") is not True:
                findings.append(f"{location}.lifecycle failure must pause")
            primary = lifecycle.get("primary_model_family")
            critic = lifecycle.get("critic_model_family")
            if lifecycle.get("critic_transport") != "offline_scripted" or primary == critic:
                findings.append(f"{location}.lifecycle lacks a heterogeneous offline Critic")
        live = action.get("live")
        if not isinstance(live, dict) or live.get("state") != "succeeded" or live.get("target_mutations") != 1:
            findings.append(f"{location}.live must prove one successful supervised target mutation")
        recovery = action.get("recovery")
        if not isinstance(recovery, dict) or recovery.get("verified_success") != "succeeded":
            findings.append(f"{location}.recovery must prove verified recovery")
    if observed != EXPECTED_ACTIONS or len(actions) != len(EXPECTED_ACTIONS):
        findings.append("report must contain each autonomy action exactly once")

    cleanup = report.get("cleanup")
    required_cleanup = (
        "disposable_kind_destroyed",
        "owned_runtime_artifacts_removed",
        "qualification_fragments_removed",
        "target_state_restored",
    )
    if not isinstance(cleanup, dict) or cleanup.get("status") != "passed":
        findings.append("cleanup must pass")
    elif any(cleanup.get(field) is not True for field in required_cleanup):
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
        print(f"AUTONOMY_ACTION_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"AUTONOMY_ACTION_QUALIFICATION_FINDING {finding}")
        print(f"AUTONOMY_ACTION_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"AUTONOMY_ACTION_QUALIFICATION_OK actions=4 outcomes=16 model_network_calls=false{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
