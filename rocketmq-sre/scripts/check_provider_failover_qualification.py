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
"""Validate the supervised read-only provider-failover qualification."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "provider-failover.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.provider-failover-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.provider-failover-qualification-report.v1"
ENVIRONMENT = "docker_postgresql_local_primary_deepseek_secondary"
REVISION = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
ABSOLUTE_LOCAL_PATH = re.compile(r"(?:\b[A-Za-z]:\\|/home/|/Users/)")
FORBIDDEN_REPORT_FIELDS = {
    "api_key",
    "credential",
    "secret",
    "model_prompt",
    "prompt_body",
    "provider_response",
    "response_body",
    "message_body",
    "debug_path",
    "endpoint_url",
}


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


def all_keys(value: Any) -> list[str]:
    if isinstance(value, list):
        return [key for child in value for key in all_keys(child)]
    if isinstance(value, dict):
        return [str(key) for key, child in value.items()] + [key for child in value.values() for key in all_keys(child)]
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


def parse_timestamp(value: Any, location: str, findings: list[str]) -> datetime | None:
    if not isinstance(value, str):
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected = {
        "schema_version": MANIFEST_SCHEMA,
        "environment": ENVIRONMENT,
        "operating_mode": "supervised_read_only",
        "production_certified": False,
        "unattended_autonomous_execution": False,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            findings.append(f"{field} must remain {value!r}")
    providers = manifest.get("providers")
    expected_providers = {
        "primary": {
            "kind": "loopback_fault_fixture",
            "network": "loopback_only",
            "credential": "ephemeral_process_fixture",
        },
        "secondary": {
            "provider": "deepseek",
            "protocol": "responses_api",
            "model": "deepseek-v4-flash",
            "network": "real_https",
        },
        "descriptors_only": ["zhipu-glm", "kimi-moonshot"],
    }
    if providers != expected_providers:
        findings.append("provider qualification and descriptor-only boundaries drifted")
    scenarios = manifest.get("required_scenarios")
    expected_scenarios = {
        "transient_primary_to_live_secondary": {
            "primary_error": "service_unavailable",
            "secondary_result": "model_assisted",
            "authorized_evidence_citations": True,
            "minimum_secondary_attempts": 1,
            "maximum_secondary_attempts": 2,
            "maximum_schema_repairs": 1,
            "maximum_diagnosis_attempts": 1,
        },
        "policy_denial_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
        "unsupported_capability_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
        "invalid_schema_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
        "invalid_citation_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
        "all_unavailable_rules_only": {"result": "rules_only", "execution_eligible": False},
    }
    if scenarios != expected_scenarios:
        findings.append("required provider-failover scenario matrix drifted")
    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict):
        findings.append("repository_evidence must be an object")
    else:
        live_test_path = repository_file(
            evidence.get("live_test_path"), "repository_evidence.live_test_path", findings
        )
        for field in ("runner", "checker"):
            repository_file(evidence.get(field), f"repository_evidence.{field}", findings)
        test_name = evidence.get("live_test")
        if live_test_path is not None and (
            not isinstance(test_name, str) or test_name not in live_test_path.read_text(encoding="utf-8")
        ):
            findings.append("repository_evidence.live_test is absent from its live_test_path")
    live_report = manifest.get("live_report")
    if not isinstance(live_report, dict) or live_report.get("schema_version") != REPORT_SCHEMA:
        findings.append("live_report contract is missing or unsupported")
    else:
        if live_report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if set(live_report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
        for field in ("secrets_recorded", "prompts_recorded", "responses_recorded", "message_bodies_recorded"):
            if live_report.get(field) is not False:
                findings.append(f"live_report.{field} must be false")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains a credential-like value")
    return findings


def _scenario(report: dict[str, Any], name: str, findings: list[str]) -> dict[str, Any]:
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, dict) or not isinstance(scenarios.get(name), dict):
        findings.append(f"scenarios.{name} must be an object")
        return {}
    return scenarios[name]


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "environment": ENVIRONMENT,
    }
    for field, value in expected.items():
        if report.get(field) != value:
            findings.append(f"report.{field} must be {value!r}")
    revision = report.get("candidate_commit")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        findings.append("candidate_commit must be a full lowercase Git SHA")
    if report.get("source_clean") is not True:
        findings.append("qualification source must be clean")
    started = parse_timestamp(report.get("started_at"), "started_at", findings)
    finished = parse_timestamp(report.get("finished_at"), "finished_at", findings)
    if started and finished and finished < started:
        findings.append("finished_at must not precede started_at")

    live = _scenario(report, "transient_primary_to_live_secondary", findings)
    expected_live = {
        "result": "model_assisted",
        "primary_attempts": 1,
        "primary_error": "service_unavailable",
        "diagnosis_attempts": 1,
        "rules_only_attempts": 0,
        "actual_provider": "deepseek",
        "actual_model": "deepseek-v4-flash",
        "fallback_chain": ["qualification-primary-transient"],
        "authorized_evidence_citations": True,
        "cited_evidence_count": 1,
        "invocation_persisted": True,
    }
    if any(live.get(field) != value for field, value in expected_live.items()):
        findings.append("live secondary fallback proof drifted")
    secondary_attempts = live.get("secondary_attempts")
    primary_attempts = live.get("primary_attempts")
    diagnosis_attempts = live.get("diagnosis_attempts")
    rules_only_attempts = live.get("rules_only_attempts")
    secondary_failed_attempts = live.get("secondary_failed_attempts")
    schema_repairs = live.get("schema_repairs")
    if (
        not isinstance(secondary_attempts, int)
        or isinstance(secondary_attempts, bool)
        or secondary_attempts not in {1, 2}
    ):
        findings.append("live secondary attempts must be one initial request plus at most one repair")
    if diagnosis_attempts != 1:
        findings.append("live fallback must succeed within one diagnosis attempt")
    if not isinstance(primary_attempts, int) or isinstance(primary_attempts, bool):
        findings.append("live primary attempts must be an integer")
    elif primary_attempts != diagnosis_attempts:
        findings.append("each live diagnosis attempt must record exactly one transient primary attempt")
    if rules_only_attempts != 0:
        findings.append("live fallback must not rely on a later direct-provider retry")
    if not isinstance(secondary_failed_attempts, int) or isinstance(secondary_failed_attempts, bool):
        findings.append("live secondary failed attempts must be an integer")
    elif secondary_failed_attempts < 0:
        findings.append("live secondary failed attempts cannot be negative")
    elif isinstance(secondary_attempts, int) and secondary_attempts != secondary_failed_attempts + 1:
        findings.append("live secondary attempts must contain exactly one successful invocation")
    if not isinstance(schema_repairs, int) or isinstance(schema_repairs, bool) or schema_repairs not in {0, 1}:
        findings.append("live secondary schema repairs must be zero or one")
    elif secondary_failed_attempts != schema_repairs:
        findings.append("the only permitted failed secondary attempt is the bounded schema repair parent")
    if set(live) != set(expected_live) | {
        "secondary_attempts",
        "secondary_failed_attempts",
        "schema_repairs",
    }:
        findings.append("live secondary fallback report fields drifted")

    policy = _scenario(report, "policy_denial_stops_fallback", findings)
    if policy != {
        "result": "rules_only",
        "primary_attempts": 1,
        "primary_error": "policy_denied",
        "secondary_attempts": 0,
    }:
        findings.append("policy denial must stop provider fallback")

    capability = _scenario(report, "unsupported_capability_stops_fallback", findings)
    if capability != {
        "result": "rules_only",
        "primary_attempts": 1,
        "primary_error": "capability_unsupported",
        "secondary_attempts": 0,
    }:
        findings.append("unsupported capability must stop provider fallback")

    schema = _scenario(report, "invalid_schema_stops_fallback", findings)
    if schema != {
        "result": "rules_only",
        "primary_attempts": 2,
        "primary_error": "schema_validation_failed",
        "secondary_attempts": 0,
        "schema_repairs": 1,
    }:
        findings.append("invalid structured output must stop provider fallback after one bounded repair")

    citation = _scenario(report, "invalid_citation_stops_fallback", findings)
    if citation != {
        "result": "rules_only",
        "primary_attempts": 2,
        "primary_error": "schema_validation_failed",
        "secondary_attempts": 0,
        "schema_repairs": 1,
    }:
        findings.append("unauthorized citation must stop provider fallback after one bounded repair")

    unavailable = _scenario(report, "all_unavailable_rules_only", findings)
    if unavailable != {
        "result": "rules_only",
        "primary_attempts": 1,
        "primary_error": "service_unavailable",
        "secondary_attempts": 0,
        "execution_eligible": False,
    }:
        findings.append("complete provider outage must remain non-executable rules-only")

    if report.get("provider_certification") != {
        "deepseek": "live_smoke_passed",
        "zhipu_glm": "descriptor_only",
        "kimi_moonshot": "descriptor_only",
    }:
        findings.append("provider certification status is overstated or incomplete")
    if report.get("safety") != {
        "effective_access": "read_only",
        "production_certified": False,
        "unattended_autonomous_execution": False,
        "mutation_calls": 0,
        "executor_calls": 0,
        "execution_agent_calls": 0,
        "secrets_recorded": False,
        "prompts_recorded": False,
        "responses_recorded": False,
        "message_bodies_recorded": False,
    }:
        findings.append("safety proof drifted from the supervised read-only boundary")
    if report.get("cleanup") != {
        "postgres_container_removed": True,
        "database_url_cleared": True,
        "api_key_environment_cleared": True,
        "loopback_credential_environment_cleared": True,
    }:
        findings.append("cleanup proof is incomplete")
    if FORBIDDEN_REPORT_FIELDS.intersection(key.lower() for key in all_keys(report)):
        findings.append("report contains a forbidden sensitive payload field")
    strings = all_strings(report)
    if any(SENSITIVE.search(value) for value in strings):
        findings.append("report contains a credential-like value")
    if any(ABSOLUTE_LOCAL_PATH.search(value) for value in strings):
        findings.append("report contains an absolute local path")
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
        print(f"PROVIDER_FAILOVER_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"PROVIDER_FAILOVER_QUALIFICATION_FINDING {finding}")
        print(f"PROVIDER_FAILOVER_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"PROVIDER_FAILOVER_QUALIFICATION_OK scenarios=6{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
