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
"""Validate live RocketMQ evidence through streamed SRE conversations."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "live-conversations.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.live-conversation-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.live-conversation-qualification-report.v1"
ENVIRONMENT = "kind_rocketmq_mcp_connector_postgresql"
PROVIDER = "isolated_local_openai_compatible_fixture"
STREAM_SCHEMA = "rocketmq-sre.conversation-stream-event.v1"
REVISION = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
FORBIDDEN_REPORT_FIELDS = {
    "api_key",
    "credential",
    "model_prompt",
    "prompt_body",
    "provider_response",
    "response_body",
    "message_body",
    "access_token",
}
EXPECTED_ASSERTIONS: dict[str, bool | int] = {
    "stream_sessions": 2,
    "consumer_lag_positive": True,
    "broker_runtime_active": True,
    "model_assisted_answers": 2,
    "authorized_evidence_citations": 2,
    "persisted_turns": 2,
    "contiguous_sequences": True,
    "unique_terminal_events": True,
    "provisional_answer_deltas": True,
    "disconnect_cancellation_contract_tested": True,
    "mutation_calls": 0,
    "executor_calls": 0,
    "execution_agent_calls": 0,
}


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


def all_keys(value: Any) -> list[str]:
    if isinstance(value, list):
        return [key for child in value for key in all_keys(child)]
    if isinstance(value, dict):
        return [str(key) for key, child in value.items()] + [key for child in value.values() for key in all_keys(child)]
    return []


def repository_file(raw_path: Any, location: str, findings: list[str]) -> None:
    if not isinstance(raw_path, str) or not raw_path:
        findings.append(f"{location} must be a non-empty repository path")
        return
    path = PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts or "docs" in path.parts:
        findings.append(f"{location} must be repository implementation evidence")
        return
    if not (REPOSITORY_ROOT / Path(*path.parts)).is_file():
        findings.append(f"{location} does not exist: {raw_path}")


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
        "model_provider": PROVIDER,
        "online_provider_calls": 0,
        "production_certified": False,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            findings.append(f"{field} must remain {value!r}")
    if manifest.get("required_assertions") != EXPECTED_ASSERTIONS:
        findings.append("required live Conversation assertions drifted")
    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict):
        findings.append("repository_evidence must be an object")
    else:
        for field in (
            "live_runner",
            "kind_runner",
            "smoke",
            "conversation_service",
            "stream_contract",
            "model_fixture",
            "checker",
        ):
            repository_file(evidence.get(field), f"repository_evidence.{field}", findings)
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


def require_exact(section: Any, expected: dict[str, Any], name: str, findings: list[str]) -> dict[str, Any]:
    if not isinstance(section, dict):
        findings.append(f"{name} proof must be an object")
        return {}
    for field, value in expected.items():
        if section.get(field) != value:
            findings.append(f"{name}.{field} must be {value!r}")
    return section


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    for field, value in {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "environment": ENVIRONMENT,
    }.items():
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

    require_exact(
        report.get("model"),
        {"provider": PROVIDER, "online_provider_calls": 0, "production_certified": False},
        "model",
        findings,
    )
    stream = require_exact(
        report.get("stream"),
        {
            "schema_version": STREAM_SCHEMA,
            "session_count": 2,
            "accepted_count": 2,
            "terminal_count": 2,
            "sequence_verified": True,
            "terminal_unique": True,
            "disconnect_cancellation_contract_tested": True,
            "max_response_bytes": 1024 * 1024,
        },
        "stream",
        findings,
    )
    for field in ("provisional_delta_count", "event_count"):
        value = stream.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 2:
            findings.append(f"stream.{field} must be at least two")
    max_frame_bytes = stream.get("max_frame_bytes")
    if not isinstance(max_frame_bytes, int) or isinstance(max_frame_bytes, bool) or not 1 <= max_frame_bytes <= 256 * 1024:
        findings.append("stream.max_frame_bytes must be between one and 262144")

    consumer = require_exact(
        report.get("consumer_lag"),
        {
            "source": "rocketmq-mcp",
            "citation_authorized": True,
            "persisted": True,
            "diagnostic_pack": "consumer-lag.v2",
        },
        "consumer_lag",
        findings,
    )
    if not isinstance(consumer.get("resource"), str) or not consumer["resource"].startswith("consumer-lag/"):
        findings.append("consumer_lag.resource must be a Consumer Lag resource")
    lag = consumer.get("total_lag")
    if not isinstance(lag, int) or isinstance(lag, bool) or lag < 1:
        findings.append("consumer_lag.total_lag must be positive")

    broker = require_exact(
        report.get("broker_runtime"),
        {
            "source": "rocketmq-mcp",
            "resource": "broker-runtime/rocketmq-dev-broker",
            "broker_up": True,
            "citation_authorized": True,
            "persisted": True,
            "diagnostic_pack": "broker-health.v1",
        },
        "broker_runtime",
        findings,
    )
    for field in ("broker_rows", "active_broker_rows"):
        value = broker.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            findings.append(f"broker_runtime.{field} must be positive")

    require_exact(
        report.get("safety"),
        {
            "mutation_calls": 0,
            "executor_calls": 0,
            "execution_agent_calls": 0,
            "effective_access": "read_only",
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
        "safety",
        findings,
    )
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
        print(f"LIVE_CONVERSATION_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"LIVE_CONVERSATION_QUALIFICATION_FINDING {finding}")
        print(f"LIVE_CONVERSATION_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"LIVE_CONVERSATION_QUALIFICATION_OK streams=2 read_only=true{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
