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
"""Validate the committed R2 catalog and an optional machine-local live report."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "r2-actions.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.r2-action-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.r2-action-qualification-report.v1"
EXPECTED_ACTIONS = {
    "broker.config.patch_allowlisted.v1",
    "topic.config.patch_allowlisted.v1",
    "subscription_group.patch_allowlisted.v1",
    "proxy.rollout_image_canary.v1",
    "security.credential_rotate_overlap.v1",
}
EXPECTED_OUTCOMES = {
    "live_verified_success",
    "critic_required_and_heterogeneous",
    "same_family_critic_denied",
    "self_approval_denied",
    "duplicate_idempotent",
    "stale_generation_or_epoch_denied",
    "unknown_effect_reconciled",
    "verification_failure",
    "automatic_compensation",
    "rollback_failure_quarantined",
    "scope_denied",
    "kill_switch_denied",
}
EXPECTED_SHARED = EXPECTED_OUTCOMES - {
    "live_verified_success",
    "verification_failure",
    "automatic_compensation",
    "rollback_failure_quarantined",
}
EXPECTED_DENIALS = {
    "disabled_action",
    "wrong_tenant",
    "wrong_cluster",
    "expired_grant",
    "unknown_action",
    "critic_missing",
    "critic_same_family",
    "self_approval",
    "stale_plan_hash",
    "stale_precondition",
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


def yaml_scalar(text: str, key: str) -> str | None:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*([^#\s]+)", text)
    return match.group(1).strip("\"'") if match else None


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if manifest.get("schema_version") != MANIFEST_SCHEMA:
        findings.append(f"schema_version must be {MANIFEST_SCHEMA}")
    if manifest.get("operating_mode") != "rules_only":
        findings.append("R2 qualification must remain rules-only")
    if manifest.get("environment") != "disposable_kind":
        findings.append("R2 live qualification must identify disposable Kind")
    if manifest.get("critic_transport") != "offline_scripted":
        findings.append("R2 qualification must use the offline scripted Critic transport")
    if manifest.get("model_provider_network_calls") is not False:
        findings.append("model-provider network calls must remain disabled")
    if manifest.get("unattended_execution") is not False:
        findings.append("R2 qualification must not enable unattended execution")
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
            findings.append(f"{location}.id is not an approved R2 action")
            continue
        if action_id in observed:
            findings.append(f"duplicate R2 action: {action_id}")
        observed.add(action_id)
        if action.get("risk") != "r2" or action.get("qualification") != "disposable_cluster_smoke_passed":
            findings.append(f"{location} must be R2 and disposable-cluster qualified")
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
            descriptor_text = descriptor.read_text(encoding="utf-8")
            if yaml_scalar(descriptor_text, "id") != action_id:
                findings.append(f"{location}.descriptor id drifted")
            if yaml_scalar(descriptor_text, "risk") != "r2" or yaml_scalar(descriptor_text, "execution_supported") != "true":
                findings.append(f"{location}.descriptor is not an executable R2 action")
            if yaml_scalar(descriptor_text, "owner") != action.get("owner"):
                findings.append(f"{location}.owner drifted from its descriptor")
            if "valid_heterogeneous_critic" not in descriptor_text:
                findings.append(f"{location}.descriptor does not require a heterogeneous Critic")
            if not re.search(r"(?ms)^compensation:\s*\n\s+mode:\s*automatic\b", descriptor_text):
                findings.append(f"{location}.descriptor lacks automatic compensation")
            invariants = action.get("required_live_invariants")
            if not isinstance(invariants, list) or not invariants:
                findings.append(f"{location}.required_live_invariants must be non-empty")
            elif any(not isinstance(value, str) or value not in descriptor_text for value in invariants):
                findings.append(f"{location}.required_live_invariants drifted from the descriptor")
        for field in (
            "precheck_evidence",
            "live_success_evidence",
            "generation_evidence",
            "recovery_matrix_evidence",
        ):
            repository_evidence(action.get(field), f"{location}.{field}", findings)
    if observed != EXPECTED_ACTIONS or len(actions) != len(EXPECTED_ACTIONS):
        findings.append(f"R2 action set drifted: {sorted(observed)}")

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


def action_invariants(manifest: dict[str, Any]) -> dict[str, set[str]]:
    return {
        action["id"]: set(action["required_live_invariants"])
        for action in manifest.get("actions", [])
        if isinstance(action, dict)
        and isinstance(action.get("id"), str)
        and isinstance(action.get("required_live_invariants"), list)
    }


def validate_report(report: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
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
    if report.get("critic_transport") != "offline_scripted":
        findings.append("report must prove the offline scripted Critic transport")
    if report.get("model_provider_network_calls") != 0:
        findings.append("report must prove zero model-provider network calls")
    if report.get("unattended_execution") is not False:
        findings.append("report must prove unattended execution stayed disabled")
    if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
        findings.append("report must exclude secrets and message bodies")

    required_invariants = action_invariants(manifest)
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
            findings.append(f"{location}.id is not an approved R2 action")
            continue
        if action_id in observed:
            findings.append(f"duplicate report action: {action_id}")
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
                "critic_reviews",
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
            if live.get("actor_separation") is not True:
                findings.append(f"{location}.live must prove operator/approver separation")
            if live.get("critic_transport") != "offline_scripted":
                findings.append(f"{location}.live must use the offline scripted Critic")
            primary = live.get("primary_model_family")
            critic = live.get("critic_model_family")
            if (
                not isinstance(primary, str)
                or not primary.strip()
                or not isinstance(critic, str)
                or not critic.strip()
                or primary.strip().casefold() == critic.strip().casefold()
            ):
                findings.append(f"{location}.live does not prove a heterogeneous Critic")
            invariants = live.get("safety_invariants")
            if not isinstance(invariants, dict) or set(invariants) != required_invariants.get(action_id, set()):
                findings.append(f"{location}.live safety invariants are incomplete")
            elif any(value is not True for value in invariants.values()):
                findings.append(f"{location}.live contains a failed safety invariant")

        recovery = action.get("recovery")
        if not isinstance(recovery, dict):
            findings.append(f"{location}.recovery must be an object")
        else:
            expected = {
                "recovery_mode": "automatic_compensation",
                "verified_success": "succeeded",
                "verification_failure": "rolled_back",
                "rollback_failure": "escalated",
            }
            for field, value in expected.items():
                if recovery.get(field) != value:
                    findings.append(f"{location}.recovery.{field} must be {value}")
            if recovery.get("compensation_intents", 0) < 2:
                findings.append(f"{location}.recovery lacks both compensation attempts")
            if recovery.get("active_quarantines", 0) < 1:
                findings.append(f"{location}.recovery lacks rollback-failure quarantine evidence")
    if observed != EXPECTED_ACTIONS or len(actions) != len(EXPECTED_ACTIONS):
        findings.append("report must contain each R2 action exactly once")

    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict) or cleanup.get("status") != "passed":
        findings.append("cleanup must pass")
    elif any(
        cleanup.get(field) is not True
        for field in (
            "disposable_kind_destroyed",
            "proxy_canary_removed",
            "credential_fixtures_removed",
            "admin_bootstrap_removed",
            "owned_runtime_artifacts_removed",
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
        manifest = load_json(args.manifest)
        findings = validate_manifest(manifest)
        if args.report is not None:
            findings.extend(validate_report(load_json(args.report), manifest))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"R2_ACTION_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"R2_ACTION_QUALIFICATION_FINDING {finding}")
        print(f"R2_ACTION_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"R2_ACTION_QUALIFICATION_OK actions=5 outcomes=12 model_network_calls=false{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
