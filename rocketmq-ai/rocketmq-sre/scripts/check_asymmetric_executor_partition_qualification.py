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
"""Validate the asymmetric Executor partition qualification contract and report."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "asymmetric-executor-partition.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.asymmetric-executor-partition-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.asymmetric-executor-partition-qualification-report.v1"
ENVIRONMENT = "docker_postgresql_http_partition"
REVISION = re.compile(r"^[0-9a-f]{40}$")
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
EXPECTED_ASSERTIONS: dict[str, bool | int] = {
    "old_executor_authority_reachable_after_partition": False,
    "old_executor_agent_reachable_after_partition": True,
    "agent_authority_reachable_during_takeover": True,
    "new_epoch_greater_than_old_epoch": True,
    "stale_dispatch_rejected": True,
    "stale_effect_rows": 0,
    "stale_target_writes": 0,
    "fresh_target_writes": 1,
    "minimum_fence_rejections": 1,
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
        "operating_mode": "rules_only",
        "production_certified": False,
        "model_provider_network_calls": False,
        "unattended_autonomous_execution": False,
    }
    for field, value in expected.items():
        if manifest.get(field) != value:
            findings.append(f"{field} must remain {value!r}")
    if manifest.get("required_assertions") != EXPECTED_ASSERTIONS:
        findings.append("required asymmetric-partition assertions drifted")

    evidence = manifest.get("repository_evidence")
    if not isinstance(evidence, dict):
        findings.append("repository_evidence must be an object")
    else:
        test_path = repository_file(evidence.get("test_path"), "repository_evidence.test_path", findings)
        for field in ("runner", "checker"):
            repository_file(evidence.get(field), f"repository_evidence.{field}", findings)
        test_name = evidence.get("test")
        if test_path is not None and (
            not isinstance(test_name, str) or test_name not in test_path.read_text(encoding="utf-8")
        ):
            findings.append("repository_evidence.test is absent from its test_path")

    report = manifest.get("live_report")
    if not isinstance(report, dict) or report.get("schema_version") != REPORT_SCHEMA:
        findings.append("live_report contract is missing or unsupported")
    else:
        if report.get("machine_local_only") is not True:
            findings.append("live reports must remain machine-local")
        if set(report.get("allowed_roots", [])) != {r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"}:
            findings.append("live report roots must be restricted to D: or F:")
        if report.get("secrets_recorded") is not False or report.get("message_bodies_recorded") is not False:
            findings.append("live reports must exclude secrets and message bodies")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains a credential-like value")
    return findings


def validate_report(report: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if report.get("schema_version") != REPORT_SCHEMA:
        findings.append(f"report schema_version must be {REPORT_SCHEMA}")
    if report.get("status") != "passed" or report.get("environment") != ENVIRONMENT:
        findings.append("report must be a passed Docker PostgreSQL HTTP partition run")
    revision = report.get("candidate_commit")
    if not isinstance(revision, str) or not REVISION.fullmatch(revision):
        findings.append("candidate_commit must be a full lowercase Git SHA")
    if report.get("source_clean") is not True:
        findings.append("qualification source must be clean")
    started = parse_timestamp(report.get("started_at"), "started_at", findings)
    finished = parse_timestamp(report.get("finished_at"), "finished_at", findings)
    if started and finished and finished < started:
        findings.append("finished_at must not precede started_at")

    connectivity = report.get("connectivity")
    expected_connectivity = {
        "old_executor_authority_reachable_after_partition": False,
        "old_executor_agent_reachable_after_partition": True,
        "agent_authority_reachable_during_takeover": True,
    }
    if not isinstance(connectivity, dict) or connectivity != expected_connectivity:
        findings.append("asymmetric connectivity proof is incomplete")

    fencing = report.get("fencing")
    if not isinstance(fencing, dict):
        findings.append("fencing proof must be an object")
    else:
        old_epoch = fencing.get("old_epoch")
        active_epoch = fencing.get("active_epoch")
        if (
            not isinstance(old_epoch, int)
            or isinstance(old_epoch, bool)
            or not isinstance(active_epoch, int)
            or isinstance(active_epoch, bool)
            or active_epoch <= old_epoch
        ):
            findings.append("active_epoch must be an integer greater than old_epoch")
        expected_fencing = {
            "stale_dispatch_rejected": True,
            "stale_effect_rows": 0,
            "stale_target_writes": 0,
            "fresh_target_writes": 1,
        }
        for field, expected in expected_fencing.items():
            if fencing.get(field) != expected:
                findings.append(f"fencing.{field} must be {expected!r}")
        rejections = fencing.get("fence_rejections")
        if not isinstance(rejections, int) or isinstance(rejections, bool) or rejections < 1:
            findings.append("fencing.fence_rejections must be at least one")

    safety = report.get("safety")
    expected_safety = {
        "model_provider_network_calls": 0,
        "production_certified": False,
        "unattended_autonomous_execution": False,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
    }
    if not isinstance(safety, dict) or safety != expected_safety:
        findings.append("safety proof drifted from the rules-only non-production boundary")
    cleanup = report.get("cleanup")
    if not isinstance(cleanup, dict) or cleanup != {
        "postgres_container_removed": True,
        "database_url_cleared": True,
    }:
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
        print(f"ASYMMETRIC_EXECUTOR_PARTITION_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"ASYMMETRIC_EXECUTOR_PARTITION_QUALIFICATION_FINDING {finding}")
        print(f"ASYMMETRIC_EXECUTOR_PARTITION_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    suffix = " report=passed" if args.report is not None else ""
    print(
        "ASYMMETRIC_EXECUTOR_PARTITION_QUALIFICATION_OK "
        f"stale_effect_rows=0 stale_target_writes=0 fresh_target_writes=1{suffix}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
