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
"""Validate independent operator handoff qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-ai" / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "independent-operator-handoff.v1.json"
DEFAULT_CHECKLIST = SRE_ROOT / "docs" / "phase05-handoff-checklist.md"
MANIFEST_SCHEMA = "rocketmq-sre.independent-operator-handoff.v1"
REPORT_SCHEMA = "rocketmq-sre.independent-operator-handoff-report.v1"
ALLOWED_REPORT_ROOTS = (r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence")
MAXIMUM_REPORT_AGE_HOURS = 24
MAXIMUM_VALIDITY_DAYS = 30
MAXIMUM_EXERCISE_DURATION_SECONDS = 3600
EXPECTED_COMMAND_MARKERS = (
    "phase05-enterprise-smoke.ps1",
    "phase05-control-plane-restore.ps1",
    "phase05-test-cluster-dr.ps1",
)
EXPECTED_EXERCISES = (
    ("environment_startup", "read_only_target"),
    ("read_only_onboarding", "read_only_target"),
    ("evidence_cited_diagnosis", "read_only_target"),
    ("supervised_action_safety", "isolated_qualification"),
    ("enterprise_integration_inspection", "read_only_target"),
    ("control_plane_restore", "isolated_qualification"),
    ("test_cluster_disaster_recovery", "isolated_qualification"),
    ("cleanup", "isolated_qualification"),
)
EXPECTED_SUMMARY = {
    "expected_exercises": 8,
    "passed_exercises": 8,
    "independent_operator_signoff": True,
    "production_target_mutations": 0,
    "qualification_environment_only": True,
    "cleanup_completed": True,
    "model_provider_calls": 0,
    "credentials_recorded": False,
    "message_bodies_recorded": False,
    "raw_logs_recorded": False,
}
EXPECTED_SAFETY = {
    key: value
    for key, value in EXPECTED_SUMMARY.items()
    if key
    not in {
        "expected_exercises",
        "passed_exercises",
        "independent_operator_signoff",
        "cleanup_completed",
    }
}
REF_PATTERNS = {
    "handoff_ref": re.compile(r"^handoff://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "environment_ref": re.compile(r"^environment://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "incoming_operator_ref": re.compile(r"^operator://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "outgoing_operator_ref": re.compile(r"^operator://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "approval_ref": re.compile(r"^approval://[a-z0-9][a-z0-9._/-]{2,127}$"),
}
REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
EMAIL_PATTERN = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
SENSITIVE_PATTERN = re.compile(
    r"(?i)(bearer\s+\S+|api[_-]?key|client[_-]?secret|access[_-]?key|password|private\s+key|"
    r"-----begin|token[=:]\s*\S+|secret[=:]\s*\S+)"
)
ENDPOINT_PATTERN = re.compile(r"(?i)\b(?:https?|postgres(?:ql)?|amqps?|s3)://")
MACHINE_PATH_PATTERN = re.compile(r"(?i)(?:\b[A-Z]:\\|/(?:home|tmp|var|etc)/)")
REPORT_FIELDS = {
    "schema_version",
    "handoff_ref",
    "environment_ref",
    "source_revision",
    "checklist_digest",
    "observed_at",
    "valid_until",
    "handoff_qualified",
    "production_certified",
    "grants_execution_authority",
    "unattended_autonomous_execution",
    "attestation",
    "exercise_results",
    "summary",
}
ATTESTATION_FIELDS = {
    "incoming_operator_ref",
    "outgoing_operator_ref",
    "approval_ref",
    "incoming_was_not_contributor",
    "incoming_confirmed_limitations",
    "outgoing_disclosed_limitations",
    "signed_at",
}
EXERCISE_RESULT_FIELDS = {
    "exercise_id",
    "environment_scope",
    "status",
    "performed_by",
    "source_revision",
    "started_at",
    "completed_at",
    "evidence_digest",
    "independently_executed",
    "unresolved_deviations",
}


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def file_digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def validate_manifest(manifest: dict[str, Any], checklist: Path = DEFAULT_CHECKLIST) -> list[str]:
    findings: list[str] = []
    expected_header = {
        "schema_version": MANIFEST_SCHEMA,
        "operating_mode": "read_only_with_isolated_qualification",
        "production_certified": False,
        "grants_execution_authority": False,
        "unattended_autonomous_execution": False,
    }
    for field, expected in expected_header.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")

    source_of_truth = manifest.get("source_of_truth")
    if source_of_truth != {
        "handoff_checklist": "rocketmq-ai/rocketmq-sre/docs/phase05-handoff-checklist.md",
        "required_command_markers": list(EXPECTED_COMMAND_MARKERS),
    }:
        findings.append("handoff source_of_truth contract drifted")
    if not checklist.is_file():
        findings.append("handoff checklist is missing")
    else:
        checklist_text = checklist.read_text(encoding="utf-8")
        for marker in EXPECTED_COMMAND_MARKERS:
            if marker not in checklist_text:
                findings.append(f"handoff checklist is missing command marker {marker}")

    exercises = manifest.get("required_exercises")
    actual_exercises = ()
    if isinstance(exercises, list):
        actual_exercises = tuple(
            (exercise.get("id"), exercise.get("environment_scope"))
            for exercise in exercises
            if isinstance(exercise, dict)
        )
    if actual_exercises != EXPECTED_EXERCISES:
        findings.append("required handoff exercise matrix drifted")

    if manifest.get("report_contract") != {
        "schema_version": REPORT_SCHEMA,
        "machine_local_only": True,
        "allowed_roots": list(ALLOWED_REPORT_ROOTS),
        "maximum_report_age_hours": MAXIMUM_REPORT_AGE_HOURS,
        "maximum_validity_days": MAXIMUM_VALIDITY_DAYS,
        "maximum_exercise_duration_seconds": MAXIMUM_EXERCISE_DURATION_SECONDS,
        "requires_distinct_operators": True,
        "requires_every_exercise_exactly_once": True,
        "requires_current_checklist_digest": True,
    }:
        findings.append("handoff report_contract drifted")
    if manifest.get("safety") != EXPECTED_SAFETY:
        findings.append("handoff safety contract drifted")
    return findings


def parse_timestamp(value: Any, field: str, findings: list[str]) -> datetime | None:
    if not isinstance(value, str):
        findings.append(f"{field} must be an RFC 3339 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        findings.append(f"{field} must be an RFC 3339 timestamp")
        return None
    if parsed.tzinfo is None:
        findings.append(f"{field} must include a timezone")
        return None
    return parsed.astimezone(timezone.utc)


def iter_strings(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from iter_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_strings(child)


def validate_exact_fields(value: dict[str, Any], expected: set[str], location: str, findings: list[str]) -> None:
    if set(value) != expected:
        findings.append(f"{location} fields do not match the bounded report schema")


def report_path_is_allowed(report_path: str) -> bool:
    normalized = report_path.replace("/", "\\")
    parts = normalized.split("\\")
    if ".." in parts or "." in parts or not re.match(r"^[A-Za-z]:\\", normalized):
        return False
    folded = normalized.casefold()
    for root in ALLOWED_REPORT_ROOTS:
        root_folded = root.rstrip("\\").casefold()
        if folded == root_folded or folded.startswith(f"{root_folded}\\"):
            return True
    return False


def validate_report(
    report: dict[str, Any],
    manifest: dict[str, Any],
    *,
    report_path: str,
    now: datetime | None = None,
    checklist: Path = DEFAULT_CHECKLIST,
) -> list[str]:
    findings = validate_manifest(manifest, checklist)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    validate_exact_fields(report, REPORT_FIELDS, "report", findings)
    if not report_path_is_allowed(report_path):
        findings.append("handoff report must remain under an allowed machine-local root")
    if report.get("schema_version") != REPORT_SCHEMA:
        findings.append(f"schema_version must be {REPORT_SCHEMA}")
    for field in ("handoff_ref", "environment_ref"):
        if not REF_PATTERNS[field].fullmatch(str(report.get(field, ""))):
            findings.append(f"{field} must be an opaque {field.removesuffix('_ref')}:// reference")

    source_revision = report.get("source_revision")
    if not REVISION_PATTERN.fullmatch(str(source_revision or "")):
        findings.append("source_revision must be a 40- or 64-character lowercase hexadecimal revision")
    if checklist.is_file() and report.get("checklist_digest") != file_digest(checklist):
        findings.append("checklist_digest does not match the current handoff checklist")
    if report.get("handoff_qualified") is not True:
        findings.append("handoff_qualified must be true for an accepted report")
    for field in ("production_certified", "grants_execution_authority", "unattended_autonomous_execution"):
        if report.get(field) is not False:
            findings.append(f"{field} must remain false")

    observed_at = parse_timestamp(report.get("observed_at"), "observed_at", findings)
    valid_until = parse_timestamp(report.get("valid_until"), "valid_until", findings)
    if observed_at is not None and valid_until is not None:
        if valid_until <= observed_at or valid_until - observed_at > timedelta(days=MAXIMUM_VALIDITY_DAYS):
            findings.append("handoff validity window is invalid or exceeds the contract limit")
        if (
            now < observed_at
            or now - observed_at > timedelta(hours=MAXIMUM_REPORT_AGE_HOURS)
            or now >= valid_until
        ):
            findings.append("handoff report is expired or not currently valid")

    strings = list(iter_strings(report))
    if any(EMAIL_PATTERN.search(value) for value in strings):
        findings.append("handoff report must not contain a personal identifier")
    if any(SENSITIVE_PATTERN.search(value) for value in strings):
        findings.append("handoff report must not contain sensitive material")
    if any(ENDPOINT_PATTERN.search(value) for value in strings):
        findings.append("handoff report must not contain a raw endpoint")
    if any(MACHINE_PATH_PATTERN.search(value) for value in strings):
        findings.append("handoff report must not contain a machine-local path")

    attestation = report.get("attestation")
    incoming = ""
    outgoing = ""
    if not isinstance(attestation, dict):
        findings.append("attestation must be an object")
    else:
        validate_exact_fields(attestation, ATTESTATION_FIELDS, "attestation", findings)
        for field in ("incoming_operator_ref", "outgoing_operator_ref", "approval_ref"):
            if not REF_PATTERNS[field].fullmatch(str(attestation.get(field, ""))):
                findings.append(f"attestation.{field} must be an opaque reference")
        incoming = str(attestation.get("incoming_operator_ref", ""))
        outgoing = str(attestation.get("outgoing_operator_ref", ""))
        if incoming == outgoing:
            findings.append("incoming and outgoing operators must be distinct")
        for field in (
            "incoming_was_not_contributor",
            "incoming_confirmed_limitations",
            "outgoing_disclosed_limitations",
        ):
            if attestation.get(field) is not True:
                findings.append(f"attestation.{field} must be true")
        signed_at = parse_timestamp(attestation.get("signed_at"), "attestation.signed_at", findings)
        if signed_at is not None and observed_at is not None:
            if signed_at > observed_at or observed_at - signed_at > timedelta(hours=1):
                findings.append("attestation signing time must be within one hour of report observation")

    expected_exercises = dict(EXPECTED_EXERCISES)
    results = report.get("exercise_results")
    if not isinstance(results, list):
        findings.append("exercise_results must be an array")
        results = []
    actual_ids: list[str] = []
    for index, result in enumerate(results):
        location = f"exercise_results[{index}]"
        if not isinstance(result, dict):
            findings.append(f"{location} must be an object")
            continue
        validate_exact_fields(result, EXERCISE_RESULT_FIELDS, location, findings)
        exercise_id = str(result.get("exercise_id", ""))
        actual_ids.append(exercise_id)
        if exercise_id not in expected_exercises:
            findings.append(f"{location} is an unexpected exercise result")
        elif result.get("environment_scope") != expected_exercises[exercise_id]:
            findings.append(f"{location}.environment_scope does not match the exercise contract")
        if result.get("status") != "passed":
            findings.append(f"{location} must pass")
        if result.get("performed_by") != incoming:
            findings.append(f"{location}.performed_by must be the incoming operator")
        if result.get("source_revision") != source_revision:
            findings.append(f"{location}.source_revision must match the handoff revision")
        if result.get("independently_executed") is not True:
            findings.append(f"{location}.independently_executed must be true")
        if result.get("unresolved_deviations") != 0:
            findings.append(f"{location}.unresolved_deviations must be zero")
        if not DIGEST_PATTERN.fullmatch(str(result.get("evidence_digest", ""))):
            findings.append(f"{location}.evidence_digest must be a lowercase SHA-256 digest")
        started_at = parse_timestamp(result.get("started_at"), f"{location}.started_at", findings)
        completed_at = parse_timestamp(result.get("completed_at"), f"{location}.completed_at", findings)
        if started_at is not None and completed_at is not None:
            if (
                completed_at <= started_at
                or completed_at - started_at > timedelta(seconds=MAXIMUM_EXERCISE_DURATION_SECONDS)
                or (observed_at is not None and completed_at > observed_at)
            ):
                findings.append(f"{location} has an invalid exercise time window")

    for exercise_id, count in sorted(Counter(actual_ids).items()):
        if count > 1:
            findings.append(f"duplicate exercise result: {exercise_id}")
    for exercise_id in sorted(set(expected_exercises) - set(actual_ids)):
        findings.append(f"missing required exercise: {exercise_id}")
    if report.get("summary") != EXPECTED_SUMMARY:
        findings.append("handoff safety summary does not match the required independent result")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--checklist", type=Path, default=DEFAULT_CHECKLIST)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    manifest = load_json(arguments.manifest)
    if arguments.report is None:
        findings = validate_manifest(manifest, arguments.checklist)
    else:
        findings = validate_report(
            load_json(arguments.report),
            manifest,
            report_path=str(arguments.report),
            checklist=arguments.checklist,
        )
    if findings:
        for finding in findings:
            print(f"ERROR: {finding}")
        return 1
    print(
        "INDEPENDENT_OPERATOR_HANDOFF_QUALIFICATION_OK "
        f"exercises={len(EXPECTED_EXERCISES)} "
        f"report={'accepted' if arguments.report else 'not_provided'} "
        "production_certified=false grants_execution_authority=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
