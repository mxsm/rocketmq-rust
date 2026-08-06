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
"""Check the bounded execution, compatibility, regional and DR qualification catalog."""

from __future__ import annotations

import json
import re
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "execution-recovery.v1.json"
EXPECTED_SCHEMA = "rocketmq-sre.execution-recovery-qualification.v1"
EXPECTED_ACTIONS = {
    "observability.logger_level_ttl.v1": "r1",
    "proxy.scale_out_one.v1": "r1",
    "proxy.restart_one.v1": "r1",
    "telemetry.collector.restart_one.v1": "r1",
    "broker.config.patch_allowlisted.v1": "r2",
    "topic.config.patch_allowlisted.v1": "r2",
    "subscription_group.patch_allowlisted.v1": "r2",
    "proxy.rollout_image_canary.v1": "r2",
    "security.credential_rotate_overlap.v1": "r2",
}
EXPECTED_AUTONOMY = {
    "observability.logger_level_ttl.v1",
    "proxy.scale_out_one.v1",
    "proxy.restart_one.v1",
    "telemetry.collector.restart_one.v1",
}
EXPECTED_DENIALS = {
    "r2_without_approval",
    "r3_action",
    "unknown_action",
    "raw_request",
    "shell_command",
    "arbitrary_kubernetes_patch",
}
EXPECTED_COMPONENTS = {"control_plane", "connector", "mcp", "execution_agent"}
EXPECTED_PROTOCOL_CASES = {
    "additive_optional_field",
    "unknown_major",
    "missing_required_feature",
    "schema_digest_drift",
    "tool_surface_drift",
}
EXPECTED_DR_EXERCISES = {
    "postgresql_failover",
    "kubernetes_node_loss",
    "broker_commitlog_replication",
    "controller_quorum_recovery",
    "object_metadata_content_restore",
    "control_plane_backup_restore",
}
SENSITIVE_VALUE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)


def _load(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("manifest root must be an object")
    return value


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for child in value for text in _all_strings(child)]
    if isinstance(value, dict):
        return [text for key, child in value.items() for text in (*_all_strings(key), *_all_strings(child))]
    return []


def _repository_path(value: Any, findings: list[str], location: str) -> Path | None:
    if not isinstance(value, str) or not value:
        findings.append(f"{location} must be a non-empty repository-relative path")
        return None
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "docs" in path.parts:
        findings.append(f"{location} must be implementation evidence: {value}")
        return None
    resolved = REPOSITORY_ROOT / Path(*path.parts)
    if not resolved.exists():
        findings.append(f"{location} does not exist: {value}")
        return None
    return resolved


def _yaml_scalar(text: str, key: str) -> str | None:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*([^#\s]+)", text)
    return match.group(1).strip('"\'') if match else None


def _enabled_action_descriptors() -> dict[str, str]:
    enabled: dict[str, str] = {}
    for path in sorted((SRE_ROOT / "config" / "actions").glob("*.yaml")):
        text = path.read_text(encoding="utf-8")
        if _yaml_scalar(text, "execution_supported") != "true":
            continue
        action_id = _yaml_scalar(text, "id")
        risk = _yaml_scalar(text, "risk")
        if action_id and risk:
            enabled[action_id] = risk
    return enabled


def _autonomy_descriptors() -> set[str]:
    result: set[str] = set()
    for path in sorted((SRE_ROOT / "config" / "autonomy" / "actions").glob("*.yaml")):
        action_id = _yaml_scalar(path.read_text(encoding="utf-8"), "action_id")
        if action_id:
            result.add(action_id)
    return result


def validate(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if manifest.get("schema_version") != EXPECTED_SCHEMA:
        findings.append(f"schema_version must be {EXPECTED_SCHEMA}")
    if manifest.get("operating_mode") != "rules_only" or manifest.get("model_provider_network_calls") is not False:
        findings.append("qualification must remain rules-only without model-provider network calls")
    for value in _all_strings(manifest):
        if SENSITIVE_VALUE.search(value):
            findings.append("manifest contains a credential-like value")

    enabled_descriptors = _enabled_action_descriptors()
    if enabled_descriptors != EXPECTED_ACTIONS:
        findings.append(f"enabled descriptor set drifted: {enabled_descriptors}")

    actions = manifest.get("controlled_actions")
    if not isinstance(actions, list):
        findings.append("controlled_actions must be an array")
        actions = []
    catalog_actions: dict[str, str] = {}
    seen_switches: set[str] = set()
    agent_config = (SRE_ROOT / "crates" / "rocketmq-sre-execution-agent" / "src" / "config.rs").read_text(
        encoding="utf-8"
    )
    for index, action in enumerate(actions):
        location = f"controlled_actions[{index}]"
        if not isinstance(action, dict):
            findings.append(f"{location} must be an object")
            continue
        action_id = action.get("id")
        risk = action.get("risk")
        if isinstance(action_id, str) and isinstance(risk, str):
            if action_id in catalog_actions:
                findings.append(f"duplicate controlled action: {action_id}")
            catalog_actions[action_id] = risk
        owners = action.get("owners")
        if not isinstance(owners, dict) or any(
            not isinstance(owners.get(role), str) or not owners[role].strip()
            for role in ("component", "sre", "security")
        ):
            findings.append(f"{location} requires component, SRE and security owners")
        enable_switch = action.get("enable_switch")
        if not isinstance(enable_switch, str) or enable_switch not in agent_config:
            findings.append(f"{location}.enable_switch is not implemented by Execution Agent")
        elif enable_switch in seen_switches:
            findings.append(f"enable switch is not independent: {enable_switch}")
        else:
            seen_switches.add(enable_switch)
        for field in ("descriptor", "precheck_test", "disposable_cluster_smoke"):
            _repository_path(action.get(field), findings, f"{location}.{field}")
        if action.get("qualification") not in {"contract_tested", "disposable_cluster_smoke_passed", "production_certified"}:
            findings.append(f"{location}.qualification is not recognized")
    if catalog_actions != EXPECTED_ACTIONS:
        findings.append(f"controlled action catalog drifted: {catalog_actions}")
    for action in actions:
        if not isinstance(action, dict):
            continue
        if action.get("qualification") != "disposable_cluster_smoke_passed":
            findings.append(
                f"controlled action {action.get('id')} must be qualified as disposable_cluster_smoke_passed"
            )

    r1_live = manifest.get("r1_live_qualification")
    if not isinstance(r1_live, dict):
        findings.append("R1 live qualification contract is missing")
    else:
        if r1_live.get("actions") != 4 or r1_live.get("required_outcomes_per_action") != 10:
            findings.append("R1 live qualification must cover four actions by ten outcomes")
        if r1_live.get("model_provider_network_calls") is not False:
            findings.append("R1 live qualification must not call model providers")
        if r1_live.get("production_certified") is not False:
            findings.append("R1 disposable qualification must not claim production certification")
        for field in ("manifest", "script", "checker"):
            _repository_path(r1_live.get(field), findings, f"r1_live_qualification.{field}")

    r2_live = manifest.get("r2_live_qualification")
    if not isinstance(r2_live, dict):
        findings.append("R2 live qualification contract is missing")
    else:
        if r2_live.get("actions") != 5 or r2_live.get("required_outcomes_per_action") != 12:
            findings.append("R2 live qualification must cover five actions by twelve outcomes")
        for field in ("model_provider_network_calls", "unattended_execution", "production_certified"):
            if r2_live.get(field) is not False:
                findings.append(f"R2 live qualification {field} must remain false")
        for field in ("manifest", "script", "checker"):
            _repository_path(r2_live.get(field), findings, f"r2_live_qualification.{field}")

    autonomy_live = manifest.get("autonomy_live_qualification")
    if not isinstance(autonomy_live, dict):
        findings.append("autonomy live qualification contract is missing")
    else:
        if autonomy_live.get("actions") != 4 or autonomy_live.get("required_outcomes_per_action") != 16:
            findings.append("autonomy live qualification must cover four actions by sixteen outcomes")
        if autonomy_live.get("live_mode_ceiling") != "supervised":
            findings.append("autonomy live qualification must stop at Supervised")
        for field in (
            "unattended_autonomous_execution",
            "model_provider_network_calls",
            "production_certified",
        ):
            if autonomy_live.get(field) is not False:
                findings.append(f"autonomy live qualification {field} must remain false")
        for field in ("manifest", "script", "checker"):
            _repository_path(autonomy_live.get(field), findings, f"autonomy_live_qualification.{field}")

    common = manifest.get("common_execution_safety_evidence")
    required_safety = {
        "successful_verification",
        "duplicate_request",
        "unknown_result",
        "verification_failure",
        "rollback_or_safe_stop",
        "stale_epoch_and_non_terminal_effect",
        "rollback_failure_quarantine",
    }
    if not isinstance(common, dict) or set(common) != required_safety:
        findings.append("common execution safety evidence is incomplete")
    else:
        for name, evidence in common.items():
            if not isinstance(evidence, dict):
                findings.append(f"common evidence {name} must be an object")
                continue
            path = _repository_path(evidence.get("path"), findings, f"common evidence {name}")
            test_name = evidence.get("test")
            if path and (not isinstance(test_name, str) or test_name not in path.read_text(encoding="utf-8")):
                findings.append(f"common evidence test is absent: {name}")

    asymmetric = manifest.get("asymmetric_partition_live_qualification")
    if not isinstance(asymmetric, dict):
        findings.append("asymmetric Executor partition qualification is missing")
    else:
        expected_asymmetric = {
            "stale_effect_rows": 0,
            "stale_target_writes": 0,
            "fresh_target_writes": 1,
            "model_provider_network_calls": False,
            "production_certified": False,
        }
        for field, expected in expected_asymmetric.items():
            if asymmetric.get(field) != expected:
                findings.append(f"asymmetric partition {field} must remain {expected!r}")
        for field in ("manifest", "script", "checker"):
            _repository_path(asymmetric.get(field), findings, f"asymmetric partition {field}")
        test_path = _repository_path(asymmetric.get("test_path"), findings, "asymmetric partition test_path")
        test_name = asymmetric.get("test")
        if test_path and (
            not isinstance(test_name, str) or test_name not in test_path.read_text(encoding="utf-8")
        ):
            findings.append("asymmetric partition test is absent from test_path")

    r2 = manifest.get("r2_authorization")
    if not isinstance(r2, dict) or any(
        r2.get(field) is not True
        for field in (
            "heterogeneous_critic_required",
            "human_approval_required",
            "short_lived_grant_required",
            "separation_of_duties_required",
        )
    ):
        findings.append("R2 authorization controls are incomplete")
    elif not isinstance(r2.get("evidence"), list) or len(r2["evidence"]) < 2:
        findings.append("R2 authorization requires critic and approval evidence")
    else:
        for index, evidence in enumerate(r2["evidence"]):
            path = _repository_path(evidence.get("path"), findings, f"r2 evidence[{index}]")
            test_name = evidence.get("test")
            if path and (not isinstance(test_name, str) or test_name not in path.read_text(encoding="utf-8")):
                findings.append(f"R2 evidence test is absent: {test_name}")

    autonomy = manifest.get("autonomous_actions")
    if not isinstance(autonomy, list):
        findings.append("autonomous_actions must be an array")
        autonomy = []
    autonomy_ids = {item.get("id") for item in autonomy if isinstance(item, dict)}
    if autonomy_ids != EXPECTED_AUTONOMY or _autonomy_descriptors() != EXPECTED_AUTONOMY:
        findings.append("autonomy catalog must contain exactly the four qualified R1 actions")
    for index, item in enumerate(autonomy):
        if isinstance(item, dict):
            _repository_path(item.get("policy"), findings, f"autonomous_actions[{index}].policy")
            _repository_path(item.get("smoke"), findings, f"autonomous_actions[{index}].smoke")

    controls = manifest.get("dynamic_controls")
    if not isinstance(controls, dict) or controls.get("default_mode") != "disabled":
        findings.append("autonomy must remain disabled by default")
    elif set(controls.get("freeze_scopes", [])) != {"organization", "tenant", "cluster", "action"}:
        findings.append("dynamic controls must cover organization, tenant, cluster and action scopes")
    else:
        _repository_path(controls.get("evidence"), findings, "dynamic_controls.evidence")

    if set(manifest.get("hard_denials", [])) != EXPECTED_DENIALS:
        findings.append("hard-denial catalog is incomplete")

    compatibility = manifest.get("compatibility")
    if not isinstance(compatibility, dict) or compatibility.get("artifact_mode") != "native_versioned_binary":
        findings.append("compatibility must use native versioned binaries")
    else:
        if set(compatibility.get("components", [])) != EXPECTED_COMPONENTS:
            findings.append("compatibility component matrix is incomplete")
        if set(compatibility.get("protocol_cases", [])) != EXPECTED_PROTOCOL_CASES:
            findings.append("compatibility protocol cases are incomplete")
        matrices = compatibility.get("matrices")
        expected_matrices = [
            {
                "control_plane": "current",
                "connector": "n_minus_one",
                "mcp": "n_minus_one",
                "execution_agent": "n_minus_one",
            },
            {
                "control_plane": "n_minus_one",
                "connector": "current",
                "mcp": "current",
                "execution_agent": "current",
            },
        ]
        if matrices != expected_matrices:
            findings.append("both current/N-1 binary directions are required")
        _repository_path(compatibility.get("qualification_script"), findings, "compatibility.qualification_script")

    regional = manifest.get("regional_isolation")
    if not isinstance(regional, dict) or regional.get("minimum_independent_regions", 0) < 2:
        findings.append("regional isolation requires two independent regions")
    else:
        if set(regional.get("required_components_per_region", [])) != {"connector", "execution_agent"}:
            findings.append("each region requires a Connector and Execution Agent")
        if regional.get("cross_region_writes") != "forbidden":
            findings.append("cross-region writes must be forbidden")
        for field in ("healthy_region_remains_available", "recovery_requires_rehandshake", "bounded_backlog_required"):
            if regional.get(field) is not True:
                findings.append(f"regional isolation requires {field}")
        _repository_path(regional.get("qualification_script"), findings, "regional_isolation.qualification_script")

    disaster_recovery = manifest.get("disaster_recovery")
    if not isinstance(disaster_recovery, dict) or disaster_recovery.get("approval_audit_step_intent_rpo") != 0:
        findings.append("Approval/Audit/StepIntent recovery must target RPO=0")
    else:
        if set(disaster_recovery.get("exercises", [])) != EXPECTED_DR_EXERCISES:
            findings.append("disaster-recovery exercise catalog is incomplete")
        _repository_path(disaster_recovery.get("qualification_script"), findings, "disaster_recovery.qualification_script")

    return findings


def main() -> int:
    try:
        manifest = _load(DEFAULT_MANIFEST)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"EXECUTION_RECOVERY_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    findings = validate(manifest)
    if findings:
        for finding in findings:
            print(f"EXECUTION_RECOVERY_QUALIFICATION_FINDING {finding}")
        print(f"EXECUTION_RECOVERY_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    print(
        "EXECUTION_RECOVERY_QUALIFICATION_OK "
        f"controlled_actions={len(EXPECTED_ACTIONS)} autonomous_actions={len(EXPECTED_AUTONOMY)} "
        "regions=2 dr_exercises=6 asymmetric_partition=true model_network_calls=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
