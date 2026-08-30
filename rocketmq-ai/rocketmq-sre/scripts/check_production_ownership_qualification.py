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
"""Validate machine-local target-organization ownership attestations."""

from __future__ import annotations

import argparse
import json
import re
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from typing import Any


SRE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "production-ownership.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.production-ownership-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.production-ownership-qualification-report.v1"
EXPECTED_TARGETS = {
    ("infrastructure", "postgresql"): {
        "service_owner",
        "access_owner",
        "backup_owner",
        "recovery_owner",
    },
    ("infrastructure", "evidence_object_store"): {
        "service_owner",
        "access_owner",
        "backup_owner",
        "recovery_owner",
    },
    ("infrastructure", "oidc"): {
        "service_owner",
        "access_owner",
        "security_owner",
        "escalation_owner",
    },
    ("infrastructure", "vault_kms"): {
        "service_owner",
        "access_owner",
        "security_owner",
        "recovery_owner",
    },
    ("infrastructure", "observability"): {
        "service_owner",
        "access_owner",
        "data_owner",
        "escalation_owner",
    },
    ("action", "observability.logger_level_ttl.v1"): {
        "component_owner",
        "sre_owner",
        "security_owner",
        "escalation_owner",
    },
    ("action", "proxy.scale_out_one.v1"): {
        "component_owner",
        "sre_owner",
        "security_owner",
        "escalation_owner",
    },
    ("action", "proxy.restart_one.v1"): {
        "component_owner",
        "sre_owner",
        "security_owner",
        "escalation_owner",
    },
    ("action", "telemetry.collector.restart_one.v1"): {
        "component_owner",
        "sre_owner",
        "security_owner",
        "escalation_owner",
    },
}
REVISION = re.compile(r"^[0-9a-f]{40}$")
OPAQUE_REFS = {
    "owner_ref": re.compile(r"^owner://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "approval_ref": re.compile(r"^approval://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "on_call_ref": re.compile(r"^oncall://[a-z0-9][a-z0-9._/-]{2,127}$"),
}
ENVIRONMENT_REF = re.compile(r"^environment://[a-z0-9][a-z0-9._/-]{2,127}$")
PLACEHOLDER = re.compile(r"(?:^|[._/-])(todo|tbd|placeholder|example|unknown|unassigned|changeme|dummy)(?:$|[._/-])", re.IGNORECASE)
EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)


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


def expected_bindings(manifest: dict[str, Any]) -> set[tuple[str, str, str]]:
    return {
        (target["kind"], target["id"], responsibility)
        for target in manifest["required_targets"]
        for responsibility in target["responsibilities"]
    }


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_values = {
        "schema_version": MANIFEST_SCHEMA,
        "environment": "target_organization",
        "production_certified": False,
        "grants_execution_authority": False,
        "model_provider_network_calls": False,
        "unattended_autonomous_execution": False,
        "live_mode_ceiling": "supervised",
        "max_validity_days": 90,
    }
    for field, expected in expected_values.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")

    observed_targets: dict[tuple[str, str], set[str]] = {}
    targets = manifest.get("required_targets")
    if not isinstance(targets, list):
        findings.append("required_targets must be an array")
    else:
        for index, target in enumerate(targets):
            if not isinstance(target, dict):
                findings.append(f"required_targets[{index}] must be an object")
                continue
            key = (target.get("kind"), target.get("id"))
            responsibilities = target.get("responsibilities")
            if (
                not all(isinstance(part, str) for part in key)
                or not isinstance(responsibilities, list)
                or not responsibilities
                or not all(isinstance(responsibility, str) for responsibility in responsibilities)
            ):
                findings.append(f"required_targets[{index}] is malformed")
                continue
            if key in observed_targets:
                findings.append(f"duplicate required target: {key[0]}/{key[1]}")
                continue
            if len(set(responsibilities)) != len(responsibilities):
                findings.append(f"required_targets[{index}] contains duplicate responsibilities")
            observed_targets[key] = set(responsibilities)
    if observed_targets != EXPECTED_TARGETS:
        findings.append("required target ownership matrix drifted")

    contract = manifest.get("report_contract")
    expected_contract = {
        "schema_version": REPORT_SCHEMA,
        "machine_local_only": True,
        "allowed_roots": [r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"],
        "owner_reference_scheme": "owner://",
        "approval_reference_scheme": "approval://",
        "on_call_reference_scheme": "oncall://",
        "personal_data_recorded": False,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
    }
    if contract != expected_contract:
        findings.append("report_contract drifted from the machine-local privacy boundary")
    if any(SENSITIVE.search(value) for value in all_strings(manifest)):
        findings.append("manifest contains credential-like material")
    return findings


def parse_timestamp(value: Any, location: str, findings: list[str]) -> datetime | None:
    if not isinstance(value, str):
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        findings.append(f"{location} must be an RFC 3339 timestamp")
        return None
    if parsed.tzinfo is None:
        findings.append(f"{location} must include a timezone")
        return None
    return parsed


def validate_report(
    report: dict[str, Any],
    manifest: dict[str, Any],
    *,
    now: datetime | None = None,
) -> list[str]:
    findings: list[str] = []
    for field, expected in {
        "schema_version": REPORT_SCHEMA,
        "status": "passed",
        "ownership_qualified": True,
        "production_certified": False,
        "grants_execution_authority": False,
        "personal_data_recorded": False,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
    }.items():
        if report.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")

    if not REVISION.fullmatch(str(report.get("source_revision", ""))):
        findings.append("source_revision must be a full Git revision")
    if not ENVIRONMENT_REF.fullmatch(str(report.get("environment_ref", ""))):
        findings.append("environment_ref must be an opaque environment:// reference")

    observed_at = parse_timestamp(report.get("observed_at"), "observed_at", findings)
    expires_at = parse_timestamp(report.get("expires_at"), "expires_at", findings)
    current = now or datetime.now(UTC)
    if observed_at is not None and expires_at is not None:
        if expires_at <= observed_at:
            findings.append("expires_at must be after observed_at")
        validity_days = (expires_at - observed_at).total_seconds() / 86_400
        if validity_days > manifest.get("max_validity_days", 0):
            findings.append("attestation validity exceeds the configured maximum")
        if expires_at <= current:
            findings.append("ownership attestation is expired")

    expected = expected_bindings(manifest)
    seen: set[tuple[str, str, str]] = set()
    assignments = report.get("assignments")
    if not isinstance(assignments, list):
        findings.append("assignments must be an array")
    else:
        for index, assignment in enumerate(assignments):
            location = f"assignments[{index}]"
            if not isinstance(assignment, dict):
                findings.append(f"{location} must be an object")
                continue
            binding = (
                assignment.get("target_kind"),
                assignment.get("target_id"),
                assignment.get("responsibility"),
            )
            if not all(isinstance(part, str) for part in binding):
                findings.append(f"{location} has a malformed ownership binding")
                continue
            if binding in seen:
                findings.append(f"duplicate ownership binding: {'/'.join(binding)}")
            seen.add(binding)
            if binding not in expected:
                findings.append(f"unexpected ownership binding: {'/'.join(binding)}")
            for field, pattern in OPAQUE_REFS.items():
                value = assignment.get(field)
                if not isinstance(value, str) or not pattern.fullmatch(value):
                    findings.append(f"{location}.{field} must be an opaque reference")
            if assignment.get("verified") is not True:
                findings.append(f"{location}.verified must be true")

    missing = expected - seen
    if missing:
        findings.append(f"missing required ownership bindings: {len(missing)}")

    strings = all_strings(report)
    if any(PLACEHOLDER.search(value) for value in strings):
        findings.append("report contains placeholder ownership values")
    if any(EMAIL.search(value) for value in strings):
        findings.append("report contains direct personal-data values instead of opaque references")
    if any(SENSITIVE.search(value) for value in strings):
        findings.append("report contains credential-like material")
    return findings


def validate_report_path(raw_path: str) -> list[str]:
    path = PureWindowsPath(raw_path)
    if path.drive.upper() not in {"D:", "F:"} or not path.is_absolute():
        return ["report path must be absolute and restricted to the D: or F: evidence root"]
    if ".." in path.parts:
        return ["report path must not escape the dedicated evidence root"]
    if len(path.parts) < 2 or path.parts[1].lower() != "rocketmq-sre-evidence":
        return ["report path must stay under the dedicated rocketmq-sre-evidence root"]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report")
    args = parser.parse_args()
    try:
        manifest = load_json(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"PRODUCTION_OWNERSHIP_QUALIFICATION_FAILED unable_to_load_manifest={error}")
        return 1
    findings = validate_manifest(manifest)
    report_validated = False
    if args.report:
        findings.extend(validate_report_path(args.report))
        if not findings:
            try:
                report = load_json(Path(args.report))
            except (OSError, ValueError, json.JSONDecodeError) as error:
                print(f"PRODUCTION_OWNERSHIP_QUALIFICATION_FAILED unable_to_load_report={error}")
                return 1
            findings.extend(validate_report(report, manifest))
            report_validated = True
    if findings:
        for finding in findings:
            print(f"PRODUCTION_OWNERSHIP_QUALIFICATION_FINDING {finding}")
        print(f"PRODUCTION_OWNERSHIP_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    bindings = len(expected_bindings(manifest))
    report_status = "validated" if report_validated else "not_provided"
    print(
        "PRODUCTION_OWNERSHIP_QUALIFICATION_OK "
        f"targets={len(EXPECTED_TARGETS)} bindings={bindings} report={report_status} "
        "production_certified=false grants_execution_authority=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
