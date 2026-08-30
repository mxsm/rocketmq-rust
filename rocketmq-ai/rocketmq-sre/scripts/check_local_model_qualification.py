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
"""Validate the credential-free local-model qualification contract and report."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "local-model.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.local-model-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.local-model-qualification-report.v1"
ENVIRONMENT = "disposable_docker_loopback_ollama"
IMAGE = "ollama/ollama:0.13.3"
MODEL = "qwen2.5:0.5b"
REVISION = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
FORBIDDEN_REPORT_FIELDS = {
    "api_key",
    "credential",
    "credential_value",
    "endpoint",
    "endpoint_url",
    "local_path",
    "model_prompt",
    "prompt",
    "prompt_body",
    "provider_response",
    "response",
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
        "production_certified": False,
        "unattended_autonomous_execution": False,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            findings.append(f"{field} must remain {value!r}")

    runtime = manifest.get("runtime")
    expected_runtime = {
        "provider": "ollama",
        "protocol": "openai_compatible_chat_completions",
        "image": IMAGE,
        "endpoint_scope": "loopback_only",
        "credential_required": False,
    }
    if runtime != expected_runtime:
        findings.append("runtime contract drifted from pinned credential-free loopback Ollama")
    if manifest.get("model") != {"id": MODEL, "maximum_artifact_bytes": 450_000_000}:
        findings.append("model contract drifted from the bounded qwen2.5:0.5b artifact")
    if manifest.get("limits") != {
        "model_calls": 1,
        "request_timeout_seconds": 120,
        "maximum_response_bytes": 65_536,
        "maximum_content_bytes": 4_096,
    }:
        findings.append("local-model invocation bounds drifted")
    if manifest.get("safety") != {
        "external_model_provider_calls": 0,
        "target_mutations": 0,
        "executor_calls": 0,
        "execution_agent_calls": 0,
        "secrets_recorded": False,
        "prompts_recorded": False,
        "responses_recorded": False,
        "message_bodies_recorded": False,
    }:
        findings.append("local-model safety boundary drifted")

    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict):
        findings.append("repository_evidence must be an object")
    else:
        live_test_path = repository_file(
            evidence.get("live_test_path"), "repository_evidence.live_test_path", findings
        )
        for field in ("profile", "runner", "checker"):
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
        findings.append("manifest contains credential-like material")
    return findings


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected = {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "environment": ENVIRONMENT,
        "operating_mode": "supervised_read_only",
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

    runtime = report.get("runtime")
    if not isinstance(runtime, dict):
        findings.append("runtime proof must be an object")
    else:
        exact = {
            "provider": "ollama",
            "protocol": "openai_compatible_chat_completions",
            "image": IMAGE,
            "model": MODEL,
            "endpoint_scope": "loopback_only",
            "model_calls": 1,
            "response_non_empty": True,
            "tool_calls": 0,
            "credential_present": False,
            "artifact_download_network": True,
        }
        for field, value in exact.items():
            if runtime.get(field) != value:
                findings.append(f"runtime.{field} must be {value!r}")
        for field in ("image_id", "model_digest"):
            value = runtime.get(field)
            if not isinstance(value, str) or not DIGEST.fullmatch(value):
                findings.append(f"runtime.{field} must be a sha256 digest")
        size = runtime.get("model_size_bytes")
        if not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= 450_000_000:
            findings.append("runtime.model_size_bytes must remain within the artifact bound")
        response_bytes = runtime.get("response_bytes")
        if not isinstance(response_bytes, int) or isinstance(response_bytes, bool) or not 0 < response_bytes <= 4_096:
            findings.append("runtime.response_bytes must be between one and 4096")
        for field in ("input_tokens", "output_tokens"):
            value = runtime.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                findings.append(f"runtime.{field} must be a positive token count")

    expected_safety = {
        "production_certified": False,
        "unattended_autonomous_execution": False,
        "external_model_provider_calls": 0,
        "target_mutations": 0,
        "executor_calls": 0,
        "execution_agent_calls": 0,
        "secrets_recorded": False,
        "prompts_recorded": False,
        "responses_recorded": False,
        "message_bodies_recorded": False,
    }
    if report.get("safety") != expected_safety:
        findings.append("safety proof drifted from the supervised local-only boundary")
    cleanup = report.get("cleanup")
    expected_cleanup = {
        "container_removed": True,
        "volume_removed": True,
        "endpoint_environment_cleared": True,
        "model_environment_cleared": True,
        "image_state_restored": True,
    }
    if not isinstance(cleanup, dict) or any(cleanup.get(field) != value for field, value in expected_cleanup.items()):
        findings.append("cleanup proof is incomplete")
    elif not isinstance(cleanup.get("image_preexisting_before"), bool):
        findings.append("cleanup.image_preexisting_before must be boolean")
    if FORBIDDEN_REPORT_FIELDS.intersection(key.lower() for key in all_keys(report)):
        findings.append("report contains a forbidden payload or machine-local path field")
    if any(SENSITIVE.search(value) for value in all_strings(report)):
        findings.append("report contains credential-like material")
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
        print(f"LOCAL_MODEL_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"LOCAL_MODEL_QUALIFICATION_FINDING {finding}")
        print(f"LOCAL_MODEL_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(f"LOCAL_MODEL_QUALIFICATION_OK provider=ollama model={MODEL}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
