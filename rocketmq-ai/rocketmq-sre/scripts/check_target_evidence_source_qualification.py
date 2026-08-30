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
"""Validate target-environment Evidence source qualification."""

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
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "target-evidence-sources.v1.json"
DEFAULT_PACK_MANIFEST = SRE_ROOT / "config" / "qualification" / "diagnostic-packs.v1.json"
MANIFEST_SCHEMA = "rocketmq-sre.target-evidence-source-qualification.v1"
REPORT_SCHEMA = "rocketmq-sre.target-evidence-source-qualification-report.v1"
PACK_SCHEMA = "rocketmq-sre.diagnostic-pack-qualification.v1"
EXPECTED_SOURCE_LIMITS = (
    ("admin-query", 300),
    ("kubernetes", 600),
    ("prometheus", 120),
    ("rocketmq-mcp", 300),
    ("runtime", 60),
    ("topology", 600),
)
ALLOWED_REPORT_ROOTS = (r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence")
MAXIMUM_REPORT_AGE_HOURS = 24
MAXIMUM_VALIDITY_DAYS = 90
MINIMUM_SAMPLES_PER_ROUTE = 3
REQUIRED_ROUTE_ASSERTIONS = (
    "query_executed",
    "canonical_schema_valid",
    "tenant_scope_enforced",
    "cluster_scope_enforced",
    "freshness_enforced",
    "row_bound_enforced",
    "byte_bound_enforced",
    "redaction_verified",
    "missing_semantics_verified",
)
REQUIRED_BINDING_ASSERTIONS = (
    "production_backend",
    "tls_verified",
    "workload_identity_verified",
    "tenant_scope_verified",
    "cluster_scope_verified",
)
READ_ONLY_SUMMARY = {
    "expected_routes": 32,
    "passed_routes": 32,
    "source_types": 6,
    "target_mutations": 0,
    "executor_calls": 0,
    "execution_agent_calls": 0,
    "model_provider_calls": 0,
    "credentials_recorded": False,
    "message_bodies_recorded": False,
    "evidence_payloads_recorded": False,
}
REF_PATTERNS = {
    "environment_ref": re.compile(r"^environment://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "integration_ref": re.compile(r"^integration://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "identity_ref": re.compile(r"^identity://[a-z0-9][a-z0-9._/-]{2,127}$"),
    "owner_ref": re.compile(r"^owner://[a-z0-9][a-z0-9._/-]{2,127}$"),
}
REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
EMAIL_PATTERN = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
SENSITIVE_PATTERN = re.compile(
    r"(?i)(bearer\s+\S+|api[_-]?key|client[_-]?secret|access[_-]?key|password|private\s+key|"
    r"-----begin|token[=:]\s*\S+|secret[=:]\s*\S+)"
)


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def canonical_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def required_routes(pack_manifest: dict[str, Any]) -> tuple[tuple[str, str, str, str], ...]:
    routes: list[tuple[str, str, str, str]] = []
    packs = pack_manifest.get("packs")
    if not isinstance(packs, list):
        raise ValueError("DiagnosticPack manifest packs must be an array")
    for pack in packs:
        if not isinstance(pack, dict) or not isinstance(pack.get("id"), str):
            raise ValueError("DiagnosticPack entries must contain an id")
        evidence = pack.get("required_evidence")
        if not isinstance(evidence, list):
            raise ValueError(f"DiagnosticPack {pack['id']} required_evidence must be an array")
        for requirement in evidence:
            if not isinstance(requirement, dict):
                raise ValueError(f"DiagnosticPack {pack['id']} required Evidence must be an object")
            fields = (pack["id"], requirement.get("key"), requirement.get("source"), requirement.get("resource_prefix"))
            if not all(isinstance(field, str) and field for field in fields):
                raise ValueError(f"DiagnosticPack {pack['id']} has an incomplete required Evidence route")
            routes.append(fields)  # type: ignore[arg-type]
    return tuple(routes)


def validate_manifest(manifest: dict[str, Any], pack_manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_header = {
        "schema_version": MANIFEST_SCHEMA,
        "operating_mode": "read_only",
        "production_certified": False,
        "grants_execution_authority": False,
        "unattended_autonomous_execution": False,
    }
    for field, expected in expected_header.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")

    try:
        routes = required_routes(pack_manifest)
    except ValueError as error:
        findings.append(str(error))
        routes = ()
    packs = pack_manifest.get("packs")
    if pack_manifest.get("schema_version") != PACK_SCHEMA:
        findings.append("DiagnosticPack source schema drifted")
    if pack_manifest.get("pack_count") != 32 or not isinstance(packs, list) or len(packs) != 32:
        findings.append("DiagnosticPack source must contain exactly 32 packs")
    if len(routes) != 32 or len(set(routes)) != len(routes):
        findings.append("DiagnosticPack source must contain 32 unique required Evidence routes")

    source_of_truth = manifest.get("source_of_truth")
    if source_of_truth != {
        "path": "rocketmq-ai/rocketmq-sre/config/qualification/diagnostic-packs.v1.json",
        "schema_version": PACK_SCHEMA,
        "required_pack_count": 32,
        "required_route_count": 32,
    }:
        findings.append("source_of_truth contract drifted")
    source_limits = manifest.get("required_source_types")
    actual_limits = ()
    if isinstance(source_limits, list):
        actual_limits = tuple(
            (entry.get("source"), entry.get("maximum_freshness_seconds"))
            for entry in source_limits
            if isinstance(entry, dict)
        )
    if actual_limits != EXPECTED_SOURCE_LIMITS:
        findings.append("required source freshness limits drifted")
    if {route[2] for route in routes} != {source for source, _ in EXPECTED_SOURCE_LIMITS}:
        findings.append("required source types no longer match the DiagnosticPack routes")

    if manifest.get("report_contract") != {
        "schema_version": REPORT_SCHEMA,
        "machine_local_only": True,
        "allowed_roots": list(ALLOWED_REPORT_ROOTS),
        "maximum_report_age_hours": MAXIMUM_REPORT_AGE_HOURS,
        "maximum_validity_days": MAXIMUM_VALIDITY_DAYS,
        "minimum_samples_per_route": MINIMUM_SAMPLES_PER_ROUTE,
        "requires_every_route_exactly_once": True,
        "requires_opaque_environment_refs": True,
        "requires_opaque_identity_refs": True,
    }:
        findings.append("report_contract drifted")
    if manifest.get("required_route_assertions") != list(REQUIRED_ROUTE_ASSERTIONS):
        findings.append("required route assertions drifted")
    summary_fields = {"expected_routes", "passed_routes", "source_types"}
    expected_safety = {key: value for key, value in READ_ONLY_SUMMARY.items() if key not in summary_fields}
    if manifest.get("safety") != expected_safety:
        findings.append("read-only safety contract drifted")
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


def report_path_is_allowed(report_path: str, allowed_roots: list[str]) -> bool:
    normalized = report_path.replace("/", "\\")
    parts = normalized.split("\\")
    if ".." in parts or "." in parts or not re.match(r"^[A-Za-z]:\\", normalized):
        return False
    folded = normalized.casefold()
    for root in allowed_roots:
        root_folded = root.rstrip("\\").casefold()
        if folded == root_folded or folded.startswith(f"{root_folded}\\"):
            return True
    return False


def validate_report(
    report: dict[str, Any],
    manifest: dict[str, Any],
    pack_manifest: dict[str, Any],
    *,
    report_path: str,
    now: datetime | None = None,
) -> list[str]:
    findings = validate_manifest(manifest, pack_manifest)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if not report_path_is_allowed(report_path, list(ALLOWED_REPORT_ROOTS)):
        findings.append("qualification report must remain under an allowed machine-local root")

    if report.get("schema_version") != REPORT_SCHEMA:
        findings.append(f"schema_version must be {REPORT_SCHEMA}")
    for field, pattern in REF_PATTERNS.items():
        if field == "environment_ref" and not pattern.fullmatch(str(report.get(field, ""))):
            findings.append(f"{field} must be an opaque {field.removesuffix('_ref')}:// reference")
    if not REVISION_PATTERN.fullmatch(str(report.get("source_revision", ""))):
        findings.append("source_revision must be a 40- or 64-character lowercase hexadecimal revision")
    if report.get("catalog_digest") != canonical_digest(pack_manifest):
        findings.append("catalog_digest does not match the current DiagnosticPack source of truth")
    for field in ("production_certified", "grants_execution_authority", "unattended_autonomous_execution"):
        if report.get(field) is not False:
            findings.append(f"{field} must remain false")
    if report.get("evidence_sources_qualified") is not True:
        findings.append("evidence_sources_qualified must be true for an accepted target report")

    observed_at = parse_timestamp(report.get("observed_at"), "observed_at", findings)
    valid_until = parse_timestamp(report.get("valid_until"), "valid_until", findings)
    if observed_at is not None and valid_until is not None:
        maximum_validity = timedelta(days=MAXIMUM_VALIDITY_DAYS)
        if valid_until <= observed_at or valid_until - observed_at > maximum_validity:
            findings.append("qualification validity window is invalid or exceeds the contract limit")
        maximum_age = timedelta(hours=MAXIMUM_REPORT_AGE_HOURS)
        if now < observed_at or now - observed_at > maximum_age or now >= valid_until:
            findings.append("qualification report is expired or not currently valid")

    strings = list(iter_strings(report))
    if any(EMAIL_PATTERN.search(value) for value in strings):
        findings.append("qualification report must not contain a personal identifier")
    if any(SENSITIVE_PATTERN.search(value) for value in strings):
        findings.append("qualification report must not contain sensitive material")

    expected_sources = {source for source, _ in EXPECTED_SOURCE_LIMITS}
    bindings = report.get("source_bindings")
    actual_sources: list[str] = []
    if not isinstance(bindings, list):
        findings.append("source_bindings must be an array")
        bindings = []
    for index, binding in enumerate(bindings):
        location = f"source_bindings[{index}]"
        if not isinstance(binding, dict):
            findings.append(f"{location} must be an object")
            continue
        source = binding.get("source")
        if isinstance(source, str):
            actual_sources.append(source)
        for field in ("integration_ref", "identity_ref", "owner_ref"):
            if not REF_PATTERNS[field].fullmatch(str(binding.get(field, ""))):
                findings.append(f"{location}.{field} must be an opaque {field.removesuffix('_ref')}:// reference")
        for field in REQUIRED_BINDING_ASSERTIONS:
            if binding.get(field) is not True:
                findings.append(f"{location}.{field} must be true")
    if set(actual_sources) != expected_sources or len(actual_sources) != len(expected_sources):
        findings.append("source binding set must cover every required source exactly once")

    try:
        expected_routes = set(required_routes(pack_manifest))
    except ValueError:
        expected_routes = set()
    route_results = report.get("route_results")
    if not isinstance(route_results, list):
        findings.append("route_results must be an array")
        route_results = []
    actual_routes: list[tuple[str, str, str, str]] = []
    freshness_limits = dict(EXPECTED_SOURCE_LIMITS)
    minimum_samples = MINIMUM_SAMPLES_PER_ROUTE
    for index, result in enumerate(route_results):
        location = f"route_results[{index}]"
        if not isinstance(result, dict):
            findings.append(f"{location} must be an object")
            continue
        route = tuple(str(result.get(field, "")) for field in ("pack_id", "evidence_key", "source", "resource_prefix"))
        actual_routes.append(route)  # type: ignore[arg-type]
        if route not in expected_routes:
            findings.append(f"{location} is an unexpected route result")
        if result.get("status") != "passed":
            findings.append(f"{location} must pass")
        sample_count = result.get("sample_count")
        if not isinstance(sample_count, int) or isinstance(sample_count, bool) or sample_count < minimum_samples:
            findings.append(f"{location}.sample_count is below the required minimum")
        if not DIGEST_PATTERN.fullmatch(str(result.get("evidence_digest", ""))):
            findings.append(f"{location}.evidence_digest must be a lowercase SHA-256 digest")
        for field in REQUIRED_ROUTE_ASSERTIONS:
            if result.get(field) is not True:
                findings.append(f"{location}.{field} must be true")
        route_observed = parse_timestamp(result.get("observed_at"), f"{location}.observed_at", findings)
        source = route[2]
        if observed_at is not None and route_observed is not None and source in freshness_limits:
            age = observed_at - route_observed
            if age < timedelta(0) or age > timedelta(seconds=freshness_limits[source]):
                findings.append(f"{location} is outside its source freshness window")

    duplicate_routes = {route for route, count in Counter(actual_routes).items() if count > 1}
    for route in sorted(duplicate_routes):
        findings.append(f"duplicate route result: {'/'.join(route)}")
    for route in sorted(expected_routes - set(actual_routes)):
        findings.append(f"missing required route: {'/'.join(route)}")

    if report.get("summary") != READ_ONLY_SUMMARY:
        findings.append("read-only summary does not match the required 32-route qualification result")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--pack-manifest", type=Path, default=DEFAULT_PACK_MANIFEST)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    manifest = load_json(arguments.manifest)
    pack_manifest = load_json(arguments.pack_manifest)
    if arguments.report is None:
        findings = validate_manifest(manifest, pack_manifest)
    else:
        report = load_json(arguments.report)
        findings = validate_report(
            report,
            manifest,
            pack_manifest,
            report_path=str(arguments.report),
        )
    if findings:
        for finding in findings:
            print(f"ERROR: {finding}")
        return 1
    routes = required_routes(pack_manifest)
    print(
        "TARGET_EVIDENCE_SOURCE_QUALIFICATION_OK "
        f"routes={len(routes)} sources={len({route[2] for route in routes})} "
        f"report={'accepted' if arguments.report else 'not_provided'} "
        "production_certified=false grants_execution_authority=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
