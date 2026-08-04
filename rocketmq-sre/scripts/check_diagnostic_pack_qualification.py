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
"""Validate the complete rules-only diagnostic-pack qualification contract."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "diagnostic-packs.v1.json"
EXPECTED_SCHEMA = "rocketmq-sre.diagnostic-pack-qualification.v1"
EXPECTED_REPORT_SCHEMA = "rocketmq-sre.diagnostic-pack-qualification-report.v1"
EXPECTED_SCENARIOS = {"normal", "fault", "missing"}
EXPECTED_TEMPLATES = {"cluster_health", "consumer", "broker", "telemetry", "producer_consumer"}
PACK_COUNT = 32
PACK_SCENARIO_COUNT = 96
MAX_CITED_EVIDENCE = 200
SENSITIVE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)


def load_object(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} root must be an object")
    return value


def all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [text for child in value for text in all_strings(child)]
    if isinstance(value, dict):
        return [text for key, child in value.items() for text in (*all_strings(key), *all_strings(child))]
    return []


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if manifest.get("schema_version") != EXPECTED_SCHEMA:
        findings.append(f"schema_version must be {EXPECTED_SCHEMA}")
    if manifest.get("operating_mode") != "rules_only":
        findings.append("operating_mode must be rules_only")
    if manifest.get("model_provider_network_calls") is not False:
        findings.append("model-provider network calls must be disabled")
    if manifest.get("target_mutation_calls") != 0 or manifest.get("execution_eligible") is not False:
        findings.append("qualification must remain mutation-zero and execution-ineligible")
    if manifest.get("pack_count") != PACK_COUNT or manifest.get("scenario_count") != 3:
        findings.append("manifest cardinality must be 32 packs by 3 scenarios")
    if manifest.get("pack_scenario_count") != PACK_SCENARIO_COUNT:
        findings.append("pack_scenario_count must be 96")
    if set(manifest.get("inspection_templates", [])) != EXPECTED_TEMPLATES:
        findings.append("inspection templates must expose the complete operational catalog")

    packs = manifest.get("packs")
    if not isinstance(packs, list) or len(packs) != PACK_COUNT:
        findings.append("packs must contain exactly 32 entries")
        packs = []
    seen: set[str] = set()
    combinations: set[tuple[str, str]] = set()
    for index, pack in enumerate(packs):
        location = f"packs[{index}]"
        if not isinstance(pack, dict):
            findings.append(f"{location} must be an object")
            continue
        pack_id = pack.get("id")
        if not isinstance(pack_id, str) or not re.fullmatch(r"[a-z0-9-]+\.v[1-9][0-9]*", pack_id):
            findings.append(f"{location}.id must be a major-qualified pack ID")
            continue
        if pack_id in seen:
            findings.append(f"duplicate pack ID: {pack_id}")
        seen.add(pack_id)
        if pack.get("inspection_template") not in EXPECTED_TEMPLATES:
            findings.append(f"{pack_id} must select one declared inspection template")
        requirements = pack.get("required_evidence")
        if not isinstance(requirements, list) or not requirements:
            findings.append(f"{pack_id} requires at least one Evidence contract")
        else:
            for requirement in requirements:
                if not isinstance(requirement, dict) or any(
                    not isinstance(requirement.get(field), str) or not requirement[field]
                    for field in ("key", "source", "resource_prefix")
                ):
                    findings.append(f"{pack_id} contains an incomplete Evidence requirement")
        scenarios = pack.get("scenarios")
        if not isinstance(scenarios, list) or {item.get("scenario") for item in scenarios if isinstance(item, dict)} != EXPECTED_SCENARIOS:
            findings.append(f"{pack_id} must contain normal, fault, and missing scenarios")
            continue
        for scenario in scenarios:
            name = scenario.get("scenario")
            combination = (pack_id, name)
            if combination in combinations:
                findings.append(f"duplicate pack/scenario: {pack_id}/{name}")
            combinations.add(combination)
            if scenario.get("execution_eligible") is not False:
                findings.append(f"{pack_id}/{name} must be execution-ineligible")
            if name == "missing" and (scenario.get("expected_status") != "inconclusive" or scenario.get("partial") is not True):
                findings.append(f"{pack_id}/missing must be partial and inconclusive")
    if len(combinations) != PACK_SCENARIO_COUNT:
        findings.append("manifest does not contain 96 unique pack/scenario combinations")

    assets = manifest.get("fixture_assets")
    if not isinstance(assets, list) or not assets:
        findings.append("fixture_assets must be a non-empty array")
    else:
        for value in assets:
            if not isinstance(value, str):
                findings.append("fixture asset path must be a string")
                continue
            path = PurePosixPath(value)
            if path.is_absolute() or ".." in path.parts or "docs" in path.parts:
                findings.append(f"fixture asset must be repository-relative implementation evidence: {value}")
            elif not (SRE_ROOT / Path(*path.parts)).is_file():
                findings.append(f"fixture asset does not exist: {value}")
    for value in all_strings(manifest):
        if SENSITIVE.search(value):
            findings.append("manifest contains credential-like material")
            break
    return findings


def validate_report(report: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if report.get("schema_version") != EXPECTED_REPORT_SCHEMA or report.get("status") != "passed":
        findings.append("live report schema or status is invalid")
    for field in ("revision", "environment", "database", "started_at", "finished_at"):
        if not isinstance(report.get(field), str) or not report[field]:
            findings.append(f"live report requires {field}")
    if report.get("operating_mode") != "rules_only":
        findings.append("live report must remain rules_only")
    for field in ("model_provider_network_calls", "target_mutation_calls", "execution_records"):
        if report.get(field) != 0:
            findings.append(f"live report {field} must be zero")
    if report.get("cross_cluster_access_rejected") is not True or report.get("schema_drift_rejected") is not True:
        findings.append("live report must prove cross-cluster and schema-drift rejection")
    if report.get("pack_count") != PACK_COUNT or report.get("pack_scenario_count") != PACK_SCENARIO_COUNT:
        findings.append("live report cardinality must be 32 packs and 96 pack/scenarios")

    expected = {
        (pack["id"], scenario["scenario"]): scenario
        for pack in manifest["packs"]
        for scenario in pack["scenarios"]
    }
    results = report.get("results")
    if not isinstance(results, list) or len(results) != PACK_SCENARIO_COUNT:
        findings.append("live report must contain exactly 96 results")
        results = []
    actual: set[tuple[str, str]] = set()
    for result in results:
        if not isinstance(result, dict):
            findings.append("live report result must be an object")
            continue
        key = (result.get("pack_id"), result.get("scenario"))
        if key in actual:
            findings.append(f"live report duplicates {key}")
        actual.add(key)
        contract = expected.get(key)
        if contract is None:
            findings.append(f"live report contains unknown result {key}")
            continue
        if result.get("status") != contract["expected_status"]:
            findings.append(f"live status drifted for {key}")
        if result.get("reason_codes") != contract["expected_reason_codes"]:
            findings.append(f"live reason codes drifted for {key}")
        if result.get("partial") != contract["partial"] or result.get("execution_eligible") is not False:
            findings.append(f"live safety result drifted for {key}")
        if not isinstance(result.get("persisted_run_count"), int) or result["persisted_run_count"] < 1:
            findings.append(f"live result was not persisted for {key}")
        citation_count = result.get("cited_evidence_count")
        if not isinstance(citation_count, int) or not 0 <= citation_count <= MAX_CITED_EVIDENCE:
            findings.append(f"live citation count exceeded its bound for {key}")
    if actual != set(expected):
        findings.append("live report result surface differs from the manifest")
    for value in all_strings(report):
        if SENSITIVE.search(value):
            findings.append("live report contains credential-like material")
            break
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    try:
        manifest = load_object(arguments.manifest)
        findings = validate_manifest(manifest)
        if arguments.report is not None:
            findings.extend(validate_report(load_object(arguments.report), manifest))
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"DIAGNOSTIC_PACK_QUALIFICATION_FAILED unable_to_load={error}")
        return 1
    if findings:
        for finding in findings:
            print(f"DIAGNOSTIC_PACK_QUALIFICATION_FINDING {finding}")
        print(f"DIAGNOSTIC_PACK_QUALIFICATION_FAILED findings={len(findings)}")
        return 1
    print(
        "DIAGNOSTIC_PACK_QUALIFICATION_OK "
        f"packs={PACK_COUNT} pack_scenarios={PACK_SCENARIO_COUNT} "
        f"live_report={str(arguments.report is not None).lower()} model_network_calls=false target_mutations=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
