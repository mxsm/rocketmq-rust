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
"""Validate the DeepSeek Responses API AI SRE diagnosis qualification."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "deepseek-diagnosis.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.deepseek-diagnosis-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.deepseek-diagnosis-qualification-report.v1"
ENVIRONMENT = "docker_postgresql_deepseek_responses"
REVISION = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
EXPECTED_ASSERTIONS: dict[str, bool | int | str] = {
    "model_assisted_diagnosis": True,
    "authorized_evidence_citations": True,
    "input_tokens_present": True,
    "output_tokens_present": True,
    "invocation_persisted": True,
    "maximum_schema_repairs": 1,
    "maximum_model_network_calls": 7,
    "maximum_diagnosis_attempts": 2,
    "maximum_rules_only_fallbacks": 1,
    "stream_sessions": 2,
    "completed_semantic_streams": 1,
    "stream_terminal_verified": True,
    "stream_cancellation_verified": True,
    "read_only_tool_selections": 1,
    "tool_selection_protocol": "openai_chat_completions",
    "tool_execution_calls": 0,
    "mutation_calls": 0,
    "execution_eligible": False,
}
FORBIDDEN_REPORT_FIELDS = {
    "api_key",
    "credential",
    "model_prompt",
    "prompt_body",
    "provider_response",
    "response_body",
    "message_body",
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
        "provider": "deepseek",
        "protocol": "responses_api",
        "model": "deepseek-v4-flash",
        "model_provider_network_calls": True,
        "production_certified": False,
        "unattended_autonomous_execution": False,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            findings.append(f"{field} must remain {value!r}")
    if manifest.get("required_assertions") != EXPECTED_ASSERTIONS:
        findings.append("required DeepSeek diagnosis assertions drifted")

    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict):
        findings.append("repository_evidence must be an object")
    else:
        live_test_path = repository_file(
            evidence.get("live_test_path"), "repository_evidence.live_test_path", findings
        )
        for field in ("adapter", "contract_fixture", "contract_test", "runner", "checker"):
            repository_file(evidence.get(field), f"repository_evidence.{field}", findings)
        test_name = evidence.get("live_test")
        if live_test_path is not None and (
            not isinstance(test_name, str) or test_name not in live_test_path.read_text(encoding="utf-8")
        ):
            findings.append("repository_evidence.live_test is absent from its live_test_path")

    report = manifest.get("live_report")
    if not isinstance(report, dict) or report.get("schema_version") != REPORT_SCHEMA:
        findings.append("live_report contract is missing or unsupported")
    else:
        if report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if set(report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
        for field in ("secrets_recorded", "prompts_recorded", "responses_recorded", "message_bodies_recorded"):
            if report.get(field) is not False:
                findings.append(f"live_report.{field} must be false")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains a credential-like value")
    return findings


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "environment": ENVIRONMENT,
        "provider": "deepseek",
        "protocol": "responses_api",
        "model": "deepseek-v4-flash",
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

    diagnosis = report.get("diagnosis")
    if not isinstance(diagnosis, dict):
        findings.append("diagnosis proof must be an object")
    else:
        exact = {
            "mode": "model_assisted",
            "authorized_evidence_citations": True,
            "input_tokens_present": True,
            "output_tokens_present": True,
            "invocation_persisted": True,
            "stream_sessions": 2,
            "completed_semantic_streams": 1,
            "stream_terminal_verified": True,
            "stream_cancellation_verified": True,
            "read_only_tool_selections": 1,
            "tool_selection_protocol": "openai_chat_completions",
            "tool_execution_calls": 0,
            "mutation_calls": 0,
            "execution_eligible": False,
        }
        for field, value in exact.items():
            if diagnosis.get(field) != value:
                findings.append(f"diagnosis.{field} must be {value!r}")
        citation_count = diagnosis.get("cited_evidence_count")
        if not isinstance(citation_count, int) or isinstance(citation_count, bool) or citation_count < 1:
            findings.append("diagnosis.cited_evidence_count must be at least one")
        repairs = diagnosis.get("schema_repairs")
        if not isinstance(repairs, int) or isinstance(repairs, bool) or not 0 <= repairs <= 1:
            findings.append("diagnosis.schema_repairs must be between zero and one")
        calls = diagnosis.get("model_network_calls")
        if not isinstance(calls, int) or isinstance(calls, bool) or not 4 <= calls <= 7:
            findings.append("diagnosis.model_network_calls must be between four and seven")
        attempts = diagnosis.get("diagnosis_attempts")
        if not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= 2:
            findings.append("diagnosis.diagnosis_attempts must be one or two")
        fallbacks = diagnosis.get("rules_only_fallbacks")
        if not isinstance(fallbacks, int) or isinstance(fallbacks, bool) or not 0 <= fallbacks <= 1:
            findings.append("diagnosis.rules_only_fallbacks must be zero or one")
        event_count = diagnosis.get("stream_event_count")
        if not isinstance(event_count, int) or isinstance(event_count, bool) or event_count < 4 or event_count > 128:
            findings.append("diagnosis.stream_event_count must be between four and 128")

    safety = report.get("safety")
    expected_safety = {
        "production_certified": False,
        "unattended_autonomous_execution": False,
        "secrets_recorded": False,
        "prompts_recorded": False,
        "responses_recorded": False,
        "message_bodies_recorded": False,
    }
    if not isinstance(safety, dict) or safety != expected_safety:
        findings.append("safety proof drifted from the supervised read-only boundary")
    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict) or cleanup != {
        "postgres_container_removed": True,
        "database_url_cleared": True,
        "api_key_environment_cleared": True,
    }:
        findings.append("cleanup proof is incomplete")
    if FORBIDDEN_REPORT_FIELDS.intersection(key.lower() for key in all_keys(report)):
        findings.append("report contains a forbidden sensitive payload field")
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
        print(f"DEEPSEEK_DIAGNOSIS_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"DEEPSEEK_DIAGNOSIS_QUALIFICATION_FINDING {finding}")
        print(f"DEEPSEEK_DIAGNOSIS_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"DEEPSEEK_DIAGNOSIS_QUALIFICATION_OK model=deepseek-v4-flash{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
